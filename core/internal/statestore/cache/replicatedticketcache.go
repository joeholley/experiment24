// Tries to use the guidelines at https://cloud.google.com/apis/design for the gRPC API where possible.
// TODO: permissive deadlines for all RPC calls
package cache

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	_ "net/http"       // Debug
	_ "net/http/pprof" // Debug

	pb "open-match.dev/pkg/pb/v2"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	store "open-match.dev/core/internal/statestore/datatypes"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "cache",
	})
)

// cache.UpdateRequest is basically a wrapper around a StateUpdate (in
// statestorage/datatypes/store.go) that adds context and a channel where
// results should go. The state storage layer shouldn't need to understand the
// underlying context, or where the update request originated (which is where
// it will return). These are necessary for the gRPC server in om-core,
// however!
type UpdateRequest struct {
	// Context, so this request can be cancelled
	Ctx context.Context
	// The update itself.
	Update store.StateUpdate
	// Return channel to confirm the write by sending back the assigned ticket ID
	ResultsChan chan *store.StateResponse
}

// The server instantiates a replicatedTicketCache on startup, and all
// ticket-reading functionality reads from this local cache. The cache also has
// the necessary data structures for replicating ticket cache changes that come
// in to this instance by it handling gRPC calls. This data structure contains
// sync.Map members, so it should not be copied after instantiation (see
// https://pkg.go.dev/sync#Map)
type ReplicatedTicketCache struct {
	// Local copies of all the state data.
	Tickets     sync.Map
	InactiveSet sync.Map
	Assignments sync.Map

	// How this replicatedTicketCache is replicated.
	Replicator store.StateReplicator
	// The queue of cache updates
	UpRequests chan *UpdateRequest

	// Application config
	Cfg *viper.Viper
}

// outgoingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server.  It processes incoming repliation events that are
// produced by gRPC handlers, and sends those events to the configured state
// storage. The updates aren't applied to the local copy of the ticket cache
// yet at this point; once the event has been successfully replicated and
// received in the incomingReplicationQueue goroutine, the update is applied to
// the local cache.
func (tc *ReplicatedTicketCache) OutgoingReplicationQueue(ctx context.Context) {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "outgoing",
	})

	logger.Debug(" Listening for replication requests")
	exec := false
	pipelineRequests := make([]*UpdateRequest, 0)
	pipeline := make([]*store.StateUpdate, 0)

	for {
		// initialize variables for this loop
		exec = false
		pipelineRequests = pipelineRequests[:0]
		pipeline = pipeline[:0]
		timeout := time.After(time.Millisecond * time.Duration(tc.Cfg.GetInt("OM_CACHE_OUT_WAIT_TIMEOUT_MS")))

		// collect currently pending requests to write to state storage using a single command (e.g. Redis Pipelining)
		for exec != true {
			select {
			case req := <-tc.UpRequests:
				pipelineRequests = append(pipelineRequests, req)
				pipeline = append(pipeline, &req.Update)

				//logger.Debugf(" %v requests queued for current batch", len(pipelineRequests))
				if len(pipelineRequests) >= tc.Cfg.GetInt("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD") {
					// Maximum batch size reached
					logger.Debug("OM_CACHE_OUT_MAX_QUEUE_THRESHOLD reached")
					exec = true
				}

			case <-timeout:
				// Timeout reached, don't wait for the batch to be full.
				logger.Debug("OM_CACHE_OUT_WAIT_TIMEOUT_MS reached")
				exec = true
			}
		}

		// If the redis update pipeline batch job has commands to run, execute
		if len(pipelineRequests) > 0 {
			// TODO: some kind of defered-goroutine-something that handles context cancellation
			logger.Debug("executing batch")
			results := tc.Replicator.SendUpdates(pipeline)
			logger.Debug(" got results")

			for index, result := range results {
				// send back this result to it's unique return channel
				pipelineRequests[index].ResultsChan <- result
			}
		}
	}
}

// incomingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server. It reads all incoming replication events from the
// configured state storage and applies them to the local ticket cache.  In
// practice, this does almost all the work for every om-core gRPC handler
// /except/ InvokeMatchMakingFunction.
func (tc *ReplicatedTicketCache) IncomingReplicationQueue(ctx context.Context) {

	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "incoming",
	})

	// Listen to the replication streams in Redis asynchronously,
	// and add updates to the channel to be processed as they come in
	replStream := make(chan store.StateUpdate, tc.Cfg.GetInt("OM_CACHE_IN_MAX_UPDATES_PER_POLL"))
	go func() {
		for {
			// The getUpdates() function blocks if there are no updates, but it respects
			// the timeout defined in the config environment variable OM_CACHE_IN_WAIT_TIMEOUT_MS
			results := tc.Replicator.GetUpdates()
			for _, curUpdate := range results {
				replStream <- *curUpdate
			}
		}
	}()

	// Check the channel for updates, and apply them
	for {
		// Force sleep time between applying replication updates into the local cache
		// to avoid tight looping and high cpu usage.
		time.Sleep(time.Millisecond * time.Duration(tc.Cfg.GetInt("OM_CACHE_IN_WAIT_TIMEOUT_MS")))
		done := false

		for !done {
			// Maximum length of time we can process updates access to the
			// ticket cache is locked during updates, so we need a hard limit here.
			updateTimeout := time.After(time.Millisecond * 500)

			// Process all incoming updates (up to the maximum defined in the
			// config var OM_CACHE_IN_WAIT_TIMEOUT_MS), up until there
			// are none left or the lock timeout is reached.
			select {
			case curUpdate := <-replStream:
				// Still updates to process.
				switch curUpdate.Cmd {
				case store.Ticket:
					// Convert the update value back to a protobuf message for
					// storage.
					//
					// https://protobuf.dev/programming-guides/api/#use-different-messages
					// states that this is not a preferred pattern, but om-core
					// meets the criteria to be an exception:
					// "If all of the following are true:
					//
					// - your service is the storage system
					// - your system doesn't make decisions based on your
					//   clients' structured data
					// - your system simply stores, loads, and perhaps provides
					//   queries at your client's request"
					ticketPb := &pb.Ticket{}
					proto.Unmarshal([]byte(curUpdate.Value), ticketPb)

					// Set TicketID. Must do it post-replication since
					// TicketID /is/ the Replication ID
					// (e.g. redis stream entry id)
					// This guarantees that the client can never get an
					// invalid ticketID that was not successfully stored/replicated
					ticketPb.Id = curUpdate.Key

					// All tickets begin inactive
					tc.InactiveSet.Store(curUpdate.Key, true)
					tc.Tickets.Store(curUpdate.Key, ticketPb)
					logger.Debugf("ticket replication received: %v", curUpdate.Key)

				case store.Activate:
					tc.InactiveSet.Delete(curUpdate.Key)
					logger.Debugf("activation replication received: %v", curUpdate.Key)

				case store.Deactivate:
					tc.InactiveSet.Store(curUpdate.Key, true)
					logger.Debugf("deactivate replication received: %v", curUpdate.Key)

				case store.Assign:
					// Convert the assignment back into a protobuf message.
					assignmentPb := &pb.Assignment{}
					proto.Unmarshal([]byte(curUpdate.Value), assignmentPb)
					tc.Assignments.Store(curUpdate.Key, assignmentPb)
					logger.Debugf("**DEPRECATED** assign replication received %v:%v", curUpdate.Key, assignmentPb.GetConnection())
				}
			case <-updateTimeout:
				// Lock hold timeout exceeded
				logger.Debug("lock hold timeout")
				done = true
			default:
				// Nothing left to process; exit immediately
				logger.Debug("Incoming update queue empty")
				done = true
			}
		}

		// Expiration closure, contains all code that removes data from the
		// replicated ticket cache.
		//
		// No need for this to be it's own function yet as the performance is
		// satisfactory running it immediately after every cache update.
		//
		// Removal logic is as follows:
		// * ticket ids expired from the inactive list MUST also have their
		//   tickets removed from the ticket cache! Any ticket that exists and
		//   doesn't have it's id on the inactive list is considered active and
		//   will appear in ticket pools for invoked MMFs!
		// * tickets with user-specified expiration times sooner than the
		//   default MUST be removed from the cache at the user-specified time.
		//   Inactive list is not affected by this as inactive list entries for
		//   tickets that don't exist have no effect (except briefly taking up a
		//   few bytes of memory). Dangling inactive list entries will be cleaned
		//   up in expirations cycles after the configured OM ticket TTL anyway.
		// * assignments are expired after the configured OM ticket TTL AND the
		//   configured OM assignment TTL have elapsed. This is to handle cases
		//   where tickets expire after they were passed to invoked MMFs but
		//   before they are in sessions. Such tickets are still allowed to be
		//   assigned  and their assigments will be retained until the
		//   expiration time described above. **DEPRECATED**
		//
		// TODO: measure the impact of expiration operations with a timer
		// metric under extreme load, and if it's problematic,
		// revisit / possibly do it asynchronously
		//
		// Ticket IDs are assigned by Redis in the format of
		// <unix_timestamp>-<index> where the index increments every time a
		// ticket is created during the same second. We are only
		// interested in the ticket creation time (the unix timestamp) here.
		//
		// The in-memory replication module just follows the redis entry ID
		// convention so this works fine, but retrieving the creation time
		// would need to be abstracted into a method of the stateReplicator
		// interface if we ever support a different replication layer (for
		// example, pub/sub).
		{
			// Separate logrus instance with its own metadata to aid troubleshooting
			exLogger := logrus.WithFields(logrus.Fields{
				"app":       "open_match",
				"component": "replicatedTicketCache",
				"operation": "expiration",
			})

			// Metrics are tallied in local variables. The values get copied to
			// the module global values that the metrics actually sample after
			// th tally is complete. This way the mid-tally values never get
			// accidentally reported to the metrics sidecar (which could happen
			// if we used the module global values to compute the tally)
			var (
				numInactive            int64
				numInactiveDeletions   int64
				numTickets             int64
				numTicketDeletions     int64
				numAssignments         int64
				numAssignmentDeletions int64
			)
			startTime := time.Now()

			// cull expired tickets from the local cache inactive ticket set
			tc.InactiveSet.Range(func(id, _ any) bool {
				numInactive++
				// Get creation timestamp from ID
				ticketCreationTime, err := strconv.ParseInt(strings.Split(id.(string), "-")[0], 10, 64)
				if err != nil {
					exLogger.Error("Unable to parse ticket ID into an unix timestamp when trying to expire old assignments")
				}
				if (time.Now().Unix() - ticketCreationTime) > int64(tc.Cfg.GetInt("OM_CACHE_TICKET_TTL_SECS")) {
					// Ensure that when expiring a ticket from the inactive set, the ticket is always deleted as well.
					_, existed := tc.Tickets.LoadAndDelete(id)
					if existed {
						numTickets++
						numTicketDeletions++
					}

					// Remove expired ticket from the inactive set.
					tc.InactiveSet.Delete(id)
					numInactiveDeletions++
				}
				return true
			})

			// cull expired tickets from local cache
			tc.Tickets.Range(func(id, ticket any) bool {
				numTickets++
				if time.Now().After(ticket.(*pb.Ticket).GetExpirationTime().AsTime()) {
					tc.Tickets.Delete(id)
					numTicketDeletions++
				}
				return true
			})

			// cull expired assignments from local cache
			tc.Assignments.Range(func(id, _ any) bool {
				numAssignments++
				// Get creation timestamp from ID
				ticketCreationTime, err := strconv.ParseInt(strings.Split(id.(string), "-")[0], 10, 64)
				if err != nil {
					exLogger.Error("Unable to parse ticket ID into an unix timestamp when trying to expire old assignments")
				}
				if (time.Now().Unix() - ticketCreationTime) >
					int64(tc.Cfg.GetInt("OM_CACHE_TICKET_TTL_SECS")+tc.Cfg.GetInt("OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_SECS")) {
					tc.Assignments.Delete(id)
					numAssignmentDeletions++
				}
				return true
			})

			// Log results and record metrics
			AssignmentCount = numAssignments
			TicketCount = numTickets
			InactiveCount = numInactive

			ExpirationCycleDuration.Record(ctx, float64(time.Since(startTime).Microseconds()/1000.0))

			if numAssignmentDeletions > 0 {
				exLogger.Debugf("Removed %v expired assignments from local cache", numAssignmentDeletions)
			}
			if numInactiveDeletions > 0 {
				exLogger.Debugf("%v ticket ids expired from the inactive list in local cache", numInactiveDeletions)
			}
			if numTicketDeletions > 0 {
				exLogger.Debugf("Removed %v expired tickets from local cache", numTicketDeletions)
			}
		}

	}
}
