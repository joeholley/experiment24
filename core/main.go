// Tries to use the guidelines at https://cloud.google.com/apis/design for the gRPC API where possible.
// TODO: permissive deadlines for all RPC calls
package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"net/http"
	_ "net/http"
	_ "net/http/pprof"

	pb "open-match.dev/pkg/pb/v2"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"open-match.dev/core/internal/config"
	"open-match.dev/core/internal/filter"
	store "open-match.dev/core/internal/statestore/datatypes"
	memoryReplicator "open-match.dev/core/internal/statestore/memory"
	redisReplicator "open-match.dev/core/internal/statestore/redis"
)

// Required by protobuf compiler's golang gRPC auto-generated code.
type grpcServer struct {
	pb.UnimplementedOpenMatchServiceServer
}

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "core",
	})
	cfg *viper.Viper = nil
)

// cacheUpdateRequest is basically a wrapper around a store.StateUpdate. The state storage
// layer shouldn't need to understand the underlying context, or where the update
// request originated (which is where it will return). These are necessary for the
// gRPC server in om-core, however!
type cacheUpdateRequest struct {
	// Context, so this request can be cancelled
	ctx context.Context
	// The update itself.
	update store.StateUpdate
	// Return channel to confirm the write by sending back the assigned ticket ID
	resultsChan chan *store.StateResponse
}

// The server instantiates a replicatedTicketCache on startup, and all
// ticket-reading functionality reads from this local cache. The cache also has
// the necessary data structures for replicating ticket cache changes that come
// in to this instance by it handling gRPC calls.
type replicatedTicketCache struct {
	// Local copies of all the state data.
	tickets     sync.Map
	inactiveSet sync.Map
	assignments sync.Map

	// How this replicatedTicketCache is replicated.
	replicator store.StateReplicator
	// The queue of cache updates
	upRequests chan *cacheUpdateRequest
}

// One global instance for the local ticket cache. Everything
// reads and writes to this one instance using concurrent-safe
// data structures where necessary.
var tc replicatedTicketCache

func main() {
	go func() {
		logger.Print(http.ListenAndServe("localhost:2224", nil))
	}()

	// Read configuration env vars, and configure logging
	cfg = config.Read()
	ctx := context.Background()

	// Set up the replicated ticket cache.
	// fields using sync.Map come ready to use and don't need initialization
	tc.upRequests = make(chan *cacheUpdateRequest)
	switch cfg.GetString("OM_STATE_STORAGE_TYPE") {
	case "redis":
		// Default: use redis
		tc.replicator = redisReplicator.New(cfg)
	case "memory":
		// use local memory
		// NOT RECOMMENDED FOR PRODUCTION
		//
		// Store statage in local memory only. Every copy of om-core spun up
		// using this configuration is an island which does not send or receive
		// any updates to/from any other instances.
		// Useful for debugging, local development, etc.
		logger.Warnf("OM_STATE_STORAGE_TYPE configuration variable set to 'memory'. NOT RECOMMENDED FOR PRODUCTION")
		tc.replicator = memoryReplicator.New(cfg)
	}

	// These goroutines send and receive cache updates from state storage
	go incomingReplicationQueue(ctx, &tc)
	go outgoingReplicationQueue(ctx, &tc)

	// Start the gRPC server
	start(cfg)

	// TODO remove for RC
	// Dump the final state of the cache to the log for debugging.
	if cfg.GetBool("OM_VERBOSE") {
		spew.Dump(dump(&tc.tickets))
		spew.Dump(dump(&tc.inactiveSet))
		spew.Dump(dump(&tc.assignments))
	}
	logger.Info("Application stopped successfully.")
	logger.Infof("Final state of local cache: %v tickets, %v active, %v inactive, %v assignments", len(dump(&tc.tickets)), len(setDifference(&tc.tickets, &tc.inactiveSet)), len(dump(&tc.inactiveSet)), len(dump(&tc.assignments)))

}

// outgoingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server.  It processes incoming repliation events that are
// produced by gRPC handlers, and sends those events to the configured state
// storage. The updates aren't applied to the local copy of the ticket cache
// yet at this point; once the event has been successfully replicated and
// received in the incomingReplicationQueue goroutine, the update is applied to
// the local cache.
func outgoingReplicationQueue(ctx context.Context, tc *replicatedTicketCache) {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "outgoing",
	})

	logger.Debug(" Listening for replication requests")
	exec := false
	redisPipelineRequests := make([]*cacheUpdateRequest, 0)
	redisPipeline := make([]*store.StateUpdate, 0)

	for {
		// initialize variables for this loop
		exec = false
		redisPipelineRequests = redisPipelineRequests[:0]
		redisPipeline = redisPipeline[:0]
		timeout := time.After(time.Millisecond * time.Duration(cfg.GetInt("OM_REDIS_PIPELINE_WAIT_TIMEOUT_MS")))

		// collect currently pending requests to write to redis using a pipeline command
		for exec != true {
			select {
			case req := <-tc.upRequests:
				redisPipelineRequests = append(redisPipelineRequests, req)
				redisPipeline = append(redisPipeline, &req.update)

				//logger.Debugf(" %v requests queued for current batch", len(redisPipelineRequests))
				if len(redisPipelineRequests) >= cfg.GetInt("OM_REDIS_PIPELINE_MAX_QUEUE_THRESHOLD") {
					// Maximum batch size reached
					logger.Debug("OM_REDIS_PIPELINE_MAX_QUEUE_THRESHOLD reached")
					exec = true
				}

			case <-timeout:
				// Timeout reached, don't wait for the batch to be full.
				logger.Debug("OM_REDIS_PIPELINE_WAIT_TIMEOUT_MS reached")
				exec = true
			}
		}

		// If the redis update pipeline batch job has commands to run, execute
		if len(redisPipelineRequests) > 0 {
			// TODO: some kind of defered-goroutine-something that handles context cancellation
			logger.Debug("executing batch")
			results := tc.replicator.SendUpdates(redisPipeline)
			logger.Debug(" got results")

			for index, result := range results {
				// send back this result to it's unique return channel
				redisPipelineRequests[index].resultsChan <- result
			}
		}
	}
}

// incomingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server. It reads all incoming replication events from the
// configured state storage and applies them to the local ticket cache.  In
// practice, this does almost all the work for every om-core gRPC handler
// /except/ InvokeMatchMakingFunction.
func incomingReplicationQueue(ctx context.Context, tc *replicatedTicketCache) {

	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "incoming",
	})

	// Listen to the replication streams in Redis asynchronously,
	// and add updates to the channel to be processed as they come in
	replStream := make(chan store.StateUpdate, cfg.GetInt("OM_REDIS_REPLICATION_MAX_UPDATES_PER_POLL"))
	go func() {
		for {
			// The getUpdates() function blocks if there are no updates, but it respects
			// the timeout defined in the config environment variable OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS
			results := tc.replicator.GetUpdates()
			for _, curUpdate := range results {
				replStream <- *curUpdate
			}
		}
	}()

	// Check the channel for updates, and apply them
	for {
		// Force sleep time between applying replication updates into the local cache
		// to avoid tight looping and high cpu usage.
		time.Sleep(time.Millisecond * time.Duration(cfg.GetInt("OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS")))
		done := false

		for !done {
			// Maximum length of time we can process updates access to the
			// ticket cache is locked during updates, so we need a hard limit here.
			updateTimeout := time.After(time.Millisecond * 500)

			// Process all incoming updates (up to the maximum defined in the
			// config var OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS), up until there
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
					tc.inactiveSet.Store(curUpdate.Key, true)
					tc.tickets.Store(curUpdate.Key, ticketPb)
					logger.Debugf("ticket replication received: %v", curUpdate.Key)

				case store.Activate:
					tc.inactiveSet.Delete(curUpdate.Key)
					logger.Debugf("activation replication received: %v", curUpdate.Key)

				case store.Deactivate:
					tc.inactiveSet.Store(curUpdate.Key, true)
					logger.Debugf("deactivate replication received: %v", curUpdate.Key)

				case store.Assign:
					// Convert the assignment back into a protobuf message.
					assignmentPb := &pb.Assignment{}
					proto.Unmarshal([]byte(curUpdate.Value), assignmentPb)
					tc.assignments.Store(curUpdate.Key, assignmentPb)
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
		// The in-memory replication module just follows the redis
		// convention so this works fine, but this would need to be
		// abstracted into a method of the stateReplicator interface if
		// we ever support a different replication layer (for example,
		// pub/sub).
		{
			exLogger := logrus.WithFields(logrus.Fields{
				"app":       "open_match",
				"component": "replicatedTicketCache",
				"operation": "expiration",
			})
			numInactiveSetDeletions := 0
			numTicketDeletions := 0
			numAssignmentDeletions := 0

			// cull expired tickets from the local cache inactive ticket set
			tc.inactiveSet.Range(func(id, _ any) bool {
				// Get creation timestamp from ID
				ticketCreationTime, err := strconv.ParseInt(strings.Split(id.(string), "-")[0], 10, 64)
				if err != nil {
					exLogger.Error("Unable to parse ticket ID into an unix timestamp when trying to expire old assignments")
				}
				if (time.Now().Unix() - ticketCreationTime) > int64(cfg.GetInt("OM_TICKET_TTL_SECS")) {
					// Ensure that when expiring a ticket from the inactive set, the ticket is always deleted as well.
					_, existed := tc.tickets.LoadAndDelete(id)
					if existed {
						numTicketDeletions++
					}

					// Remove expired ticket from the inactive set.
					tc.inactiveSet.Delete(id)
					numInactiveSetDeletions++
				}
				return true
			})

			// cull expired tickets from local cache
			tc.tickets.Range(func(id, ticket any) bool {
				if time.Now().After(ticket.(*pb.Ticket).GetExpirationTime().AsTime()) {
					tc.tickets.Delete(id)
					numTicketDeletions++
				}
				return true
			})

			// cull expired assignments from local cache
			tc.assignments.Range(func(id, _ any) bool {
				// Get creation timestamp from ID
				ticketCreationTime, err := strconv.ParseInt(strings.Split(id.(string), "-")[0], 10, 64)
				if err != nil {
					exLogger.Error("Unable to parse ticket ID into an unix timestamp when trying to expire old assignments")
				}
				if (time.Now().Unix() - ticketCreationTime) >
					int64(cfg.GetInt("OM_TICKET_TTL_SECS")+cfg.GetInt("OM_ASSIGNMENT_TTL_SECS")) {
					tc.assignments.Delete(id)
					numAssignmentDeletions++
				}
				return true
			})

			if numAssignmentDeletions > 0 {
				exLogger.Infof("Removed %v expired assignments from local cache", numAssignmentDeletions)
			} else {
				exLogger.Debug("No expired assignments to remove from cache this cycle")
			}
			if numInactiveSetDeletions > 0 {
				exLogger.Infof("%v ticket ids expired from the inactive list in local cache", numInactiveSetDeletions)
			} else {
				exLogger.Debug("No ticket ids to remove from the cache inactive list this cycle")
			}
			if numTicketDeletions > 0 {
				exLogger.Infof("Removed %v expired tickets from local cache", numTicketDeletions)
			} else {
				exLogger.Debug("No expired tickets to remove from cache this cycle")
			}
		}

	}
}

// CreateTicket generates an event to update the ticket state storage, adding a
// new ticket. The ticket's id will be generated by the state storage and
// returned asynchronously.  This request hangs until it can return the ticket
// id. When a ticket is created, it starts off as inactive and must be
// activated with the ActivateTickets call. This ensures that no ticket that
// was not successfully replicated and returned to the om-core client is ever
// put in the pool.
func (s *grpcServer) CreateTicket(parentCtx context.Context, req *pb.CreateTicketRequest) (*pb.CreateTicketResponse, error) {

	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "core",
		"rpc":       "CreateTicket",
	})

	// Input validation
	if req.GetTicket() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "ticket is required")
	}
	if req.GetTicket().GetId() != "" {
		logger.Warnf("CreateTicket request included a ticketid (%v). Open Match assigns Ticket IDs, so this value will be overwritten. See documentation for more details.", req.GetTicket().GetId())
	}

	// Get timestamps from ticket in the request
	crTime := req.GetTicket().GetAttributes().GetCreationTime()
	exTime := req.GetTicket().GetExpirationTime()

	// Set creation timestamp if request didn't provide one
	if !crTime.IsValid() {
		logger.Info("CreationTime provided is invalid or nil; replacing with current time")
		crTime = timestamppb.Now()
	}

	// The default expiration time is based on the provided creation time
	// (which defaults to now) + the configured ticket TTL
	defaultExTime := crTime.AsTime().Add(time.Second * time.Duration(cfg.GetInt("OM_TICKET_TTL_SECS")))

	// Set expiration timestamp if request didn't provide a valid one
	if !exTime.IsValid() ||
		exTime.AsTime().After(defaultExTime) {
		logger.Debugf("ExpirationTime provided is invalid or nil; replacing with current time + OM_TICKET_TTL_SECS (%v)", cfg.GetInt("OM_TICKET_TTL_SECS"))
		exTime = timestamppb.New(defaultExTime)
	}

	// Update ticket with new creation/expiration time
	ticketPb, err := proto.Marshal(&pb.Ticket{
		ExpirationTime: exTime,
		Extensions:     req.GetTicket().GetExtensions(),
		Attributes: &pb.Ticket_FilterableData{
			Tags:         req.GetTicket().GetAttributes().GetTags(),
			StringArgs:   req.GetTicket().GetAttributes().GetStringArgs(),
			DoubleArgs:   req.GetTicket().GetAttributes().GetDoubleArgs(),
			CreationTime: crTime,
		},
	})

	// Marshal ticket into storage format
	if ticketPb == nil || err != nil {
		err = errors.Wrap(err, "failed to marshal the ticket protobuf")
		logger.Errorf(" CreateTicket: %v", err)
		return nil, err
	}

	// Make a results return channel
	rChan := make(chan *store.StateResponse)

	// Queue our ticket creation cache update request to be replicated to all
	// om-core instances.
	tc.upRequests <- &cacheUpdateRequest{
		// Writing this ticket to state storage (redis by default) can be
		// pipelined by the implementing module for better performance under
		// high rps. The batch writing async goroutine
		// outgoingReplicationQueue() handles writing to state storage.
		update: store.StateUpdate{
			Cmd:   store.Ticket,
			Value: string(ticketPb[:]),
		},
		resultsChan: rChan,
		ctx:         parentCtx,
	}

	// Get the results
	results := <-rChan

	return &pb.CreateTicketResponse{TicketId: results.Result}, results.Err
}

// DeactivatesTicket is a lazy deletion process: it adds the provided ticketIDs to the inactive list,
// which prevents them from appearing in player pools, and the tickets are deleted when they expire.
func (s *grpcServer) DeactivateTickets(parentCtx context.Context, req *pb.DeactivateTicketsRequest) (*pb.DeactivateTicketsResponse, error) {

	// Validate number of requested updates
	numReqUpdates := len(req.GetTicketIds())
	if numReqUpdates == 0 {
		err := status.Error(codes.InvalidArgument, "No Ticket IDs in update request")
		return nil, err
	}
	if numReqUpdates > cfg.GetInt("OM_MAX_STATE_UPDATES_PER_CALL") {
		errMsg := fmt.Sprintf("Too many ticket state updates requested in a single call (configured maximum %v, requested %v)",
			cfg.GetInt("OM_MAX_STATE_UPDATES_PER_CALL"), numReqUpdates)
		err := status.Error(codes.InvalidArgument, errMsg)
		return nil, err
	}
	// Validate input against state storage event id format
	validTicketIds, invalidTicketIds := validateTicketIds(req.GetTicketIds())

	// Send the ticket deactivation updates. Returns the last Replication ID of
	// the batch. Can be used to determine if all of these activations have
	// been applied to a given om-core instance.
	errs := updateTicketsActiveState(parentCtx, validTicketIds, store.Deactivate)

	// Add deactivation failures to the error details
	_ = invalidTicketIds
	err := addStateUpdateErrorDetails(errs)

	return &pb.DeactivateTicketsResponse{}, err
}

// ActivateTickets accepts a list of ticketids to activate, validates the
// input, and generates replication updates for each activation event.
func (s *grpcServer) ActivateTickets(parentCtx context.Context, req *pb.ActivateTicketsRequest) (*pb.ActivateTicketsResponse, error) {

	// Validate number of requested updates
	numReqUpdates := len(req.GetTicketIds())
	if numReqUpdates == 0 {
		err := status.Error(codes.InvalidArgument, "No Ticket IDs in update request")
		return nil, err
	}
	if numReqUpdates > cfg.GetInt("OM_MAX_STATE_UPDATES_PER_CALL") {
		errMsg := fmt.Sprintf("Too many ticket state updates requested in a single call (configured maximum %v, requested %v)",
			cfg.GetInt("OM_MAX_STATE_UPDATES_PER_CALL"), numReqUpdates)
		err := status.Error(codes.InvalidArgument, errMsg)
		return nil, err
	}

	// Validate input against state storage event id format
	validTicketIds, invalidTicketIds := validateTicketIds(req.GetTicketIds())

	// Send the ticket activation updates. Returns the last Replication ID of
	// the batch. Can be used to determine if all of these activations have
	// been applied to a given om-core instance.
	errs := updateTicketsActiveState(parentCtx, validTicketIds, store.Activate)

	// Add activation failures to the error details
	_ = invalidTicketIds
	err := addStateUpdateErrorDetails(errs)

	return &pb.ActivateTicketsResponse{}, err
}

// InvokeMatchmakingFunctions loops through each Pool in the provided Profile,
// applying the filters inside and adding participating tickets to those pools.
// It then attempts to connect to every matchmaking function in the provided
// list, and send that Profile with filled Pools to each. It then aggregates
// all the resulting streams into a channel on which it can return results from
// each matchmaking function asynchronously.
// TODO: audit context https://stackoverflow.com/questions/76724124/does-a-go-grpc-server-streaming-method-not-have-a-context-argument
func (s *grpcServer) InvokeMatchmakingFunctions(req *pb.MmfRequest, stream pb.OpenMatchService_InvokeMatchmakingFunctionsServer) error {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "core",
		"rpc":       "InvokeMatchmakingFunctions",
	})

	// input validation
	if req.GetProfile() == nil {
		return status.Error(codes.InvalidArgument, "profile is required")
	}
	if req.GetMmfs() == nil {
		return status.Error(codes.InvalidArgument, "list of mmfs to invoke is required")
	}

	// Apply filters from all pools specified in this profile
	// to find the participating tickets for each pool.  Start by snapshotting
	// the state of the ticket cache to a new data structure, so we can work
	// with that snapshot without incurring a bunch of additional access
	// contention on the ticket cache itself, which will continue to be updated
	// as we process. The participants of these pools won't reflect updates to
	// the ticket cache that happen after this point.

	// Copy the ticket cache, leaving out inactive tickets.
	pactiveTickets := make([]*pb.Ticket, 0)
	tc.tickets.Range(func(id, ticket any) bool {
		// not ok means an error was encountered, indicating this
		// ticket is NOT inactive (meaning it IS active)
		pactiveTickets = append(pactiveTickets, ticket.(*pb.Ticket))
		return true
	})
	inactiveTickets := make([]string, 0)
	tc.inactiveSet.Range(func(id, _ any) bool {
		// not ok means an error was encountered, indicating this
		// ticket is NOT inactive (meaning it IS active)
		inactiveTickets = append(inactiveTickets, id.(string))
		return true
	})

	activeTickets := setDifference(&tc.tickets, &tc.inactiveSet)
	unassignedTickets := setDifference(&tc.inactiveSet, &tc.assignments)

	logger.Infof(" %5d/%5d tickets active, %5d/%5d tickets inactive without assignments",
		len(activeTickets), len(pactiveTickets), len(unassignedTickets), len(inactiveTickets))
	logger.Debugf("Ticket cache contains %v active tickets to filter into pools for profile %v", len(activeTickets), req.GetProfile().GetName())

	// validate pool filters before filling them
	validPools := map[string][]*pb.Ticket{}
	for name, pool := range req.GetProfile().GetPools() {
		if valid, err := filter.ValidatePoolFilters(pool); valid {
			// Initialize a clean roster for this pool
			validPools[name] = make([]*pb.Ticket, 0)
		} else {
			logger.Error("Unable to fill pool with tickets, invalid: %w", err)
		}
	}

	// Perform filtering, and 'chunk' the pools into slightly smaller 4mb pieces to stream
	// 4mb is default max pb size.
	// 1000 for a little extra headroom, we're not trying to hyper-optimize here
	maxPbSize := 4 * 1000 * 1000
	chunkedPools := make([]map[string][]*pb.Ticket, 0)
	chunkedPools = append(chunkedPools, map[string][]*pb.Ticket{})
	var chunkCount int32
	emptyChunkSize := proto.Size(&pb.ChunkedMmfRunRequest{Profile: req.GetProfile(), NumChunks: math.MaxInt32})
	curChunkSize := emptyChunkSize
	for _, ticket := range activeTickets {
		for name, _ := range validPools {
			// All the hard work for this is in internal/filter/filter.go
			if filter.In(req.GetProfile().GetPools()[name], ticket.(*pb.Ticket)) {
				ticketSize := proto.Size(ticket.(*pb.Ticket))
				// Check if this ticket will put us over the max pb size for this chunk
				if (curChunkSize + ticketSize) >= maxPbSize {
					// Start a new chunk
					curChunkSize = emptyChunkSize
					chunkCount++
					chunkedPools = append(chunkedPools, map[string][]*pb.Ticket{})
				}
				chunkedPools[chunkCount][name] = append(chunkedPools[chunkCount][name], ticket.(*pb.Ticket))
				curChunkSize += ticketSize
				//validPools[name] = append(validPools[name], ticket.(*pb.Ticket))
			}
		}
	}

	// put final participant rosters into the pools.
	// Send the full profile in each streamed 'chunk', only the pools are broken
	// up to keep the pbs under the max size. This could probably be optimized
	// so we don't repeatedly send profile details in larger payloads, but this
	// implementation is 1) simpler and 2) could still be useful to the receiving
	// MMF if it somehow only got part of the chunked request.
	chunkedRequest := make([]*pb.ChunkedMmfRunRequest, len(chunkedPools))
	for chunkIndex, chunk := range chunkedPools {
		// Fill this request 'chunk' with the chunked pools we built above
		pools := make(map[string]*pb.Pool)
		profile := &pb.Profile{
			Name:       req.GetProfile().GetName(),
			Pools:      pools,
			Extensions: req.GetProfile().GetExtensions(),
		}
		for name, participantRoster := range chunk {
			profile.GetPools()[name] = &pb.Pool{
				Participants: &pb.Roster{
					Name:    name + "_roster",
					Tickets: participantRoster,
				},
			}
		}
		chunkedRequest[chunkIndex] = &pb.ChunkedMmfRunRequest{
			Profile:   profile,
			NumChunks: int32(len(chunkedPools)),
		}

	}

	// TODO: cache connections using a pool and re-use
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: (time.Second * 5)}))

	// Simple fan-in channel pattern implemented as an async inline goroutine.
	// Asynchronously sends matches to InvokeMatchMakingFunction() caller as
	// they come in from the concurrently-running MMFs. Exits when all
	// MMFs are complete.
	// TODO: add a configurable timeout for long-running MMFs
	matchChan := make(chan *pb.Match)
	waitChan := make(chan bool)
	var mmfwg sync.WaitGroup
	go func() {
		logger.Debug("Results Fan-in goroutine active")
		for {
			select {
			case match := <-matchChan:
				logger.Debugf("Streaming back match %v", match.GetId())
				stream.Send(&pb.StreamedMmfResponse{Match: match})
			case <-waitChan:
				logger.Debug("ALL MMFS COMPLETE")
				close(matchChan)
				return
			}
		}
	}()

	// Invoke each requested MMF, and put the matches they stream back into the match channel.
	for _, mmf := range req.GetMmfs() {
		// Add this invocation to the MMF wait group.
		mmfwg.Add(1)
		//TODO: Needs to be an errorgroup
		go func(mmf *pb.MatchmakingFunctionSpec) error {
			defer mmfwg.Done()

			var err error
			err = nil

			// Connect to gRPC server
			mmfHost := mmf.GetHost()
			if mmf.GetPort() > 0 {
				mmfHost = fmt.Sprintf("%v:%v", mmfHost, mmf.GetPort())
			}
			if mmf.GetType() == pb.MatchmakingFunctionSpec_REST {
				logger.Error("")
				err = status.Error(codes.Internal, fmt.Errorf("REST Mmf invocation NYI %v: %w", mmfHost, err).Error())
			} else { // TODO: gRPC is default for legacy OM1 reasons, need to swap enum order in pb to change

				// TODO: proper client caching/re-use
				var conn *grpc.ClientConn
				conn, err = grpc.Dial(mmfHost, opts...)
				if err != nil {
					return status.Error(codes.Internal, fmt.Errorf("Failed to make gRPC client connection for %v: %w", mmfHost, err).Error())
				}
				defer conn.Close()
				client := pb.NewMatchMakingFunctionServiceClient(conn)
				logger.Debugf("connected to %v", mmfHost)
				logger.Infof("connected to %v", mmfHost)

				// Set a timeout for this API call, but don't respect cancelling the parent context, use the empty background context instead.
				ctx, cancel := context.WithTimeout(context.Background(), (time.Duration(cfg.GetInt("OM_MMF_TIMEOUT_SECS")) * time.Second))
				//ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				//logger.Debugf("Connected to MMF '%v' at %v", mmf.GetName(), mmfHost)
				logger.Infof("Connected to MMF '%v' at %v", mmf.GetName(), mmfHost)

				// Run the MMF
				var mmfStream pb.MatchMakingFunctionService_RunClient
				mmfStream, err = client.Run(ctx)
				if err != nil {
					return status.Error(codes.Internal, fmt.Errorf("Failed to connect to MMF at %v: %w", mmfHost, err).Error())
				}
				logger.Infof("Waiting for results from '%v' at %v", mmf.GetName(), mmfHost)

				// Request itself is chunked if all the tickets returned in
				// ticket pools result in a total request size larger than the
				// default gRPC message size of 4mb.
				for index, chunk := range chunkedRequest {
					mmfStream.Send(chunk)
					logger.Infof("MMF request chunk %02d/%02d: %0.2fmb", index+1, len(chunkedRequest), float64(proto.Size(chunk))/float64(1024*1024))
				}

				var tdwg sync.WaitGroup
				for {
					// Get results from MMF
					var result *pb.StreamedMmfResponse
					result, err = mmfStream.Recv()
					if errors.Is(err, io.EOF) { // io.EOF is the error grpc sends when the server closes a stream.
						logger.Errorf("MMF EOF '%v' at %v", mmf.GetName(), mmfHost)
						break
					}
					if err != nil { // MMF has an error
						logger.Errorf("MMF Error '%v' at %v", err, mmfHost)
						return status.Error(codes.Internal, fmt.Errorf("Unable to invoke MMF '%v' at %v: %w", mmf.GetName(), mmfHost, err).Error())
					}
					// Make a copy we can send to goroutines.
					resultCopy := result.GetMatch()

					// deactivate tickets & send match back to the matchmaker
					// Note, even with the waitgroup, this implementation is
					// only waiting until the deactivations is added to the
					// local ticket cache. The distributed/eventually
					// consistent nature of om-core implementation means there
					// is no feasible way to wait for replication to every
					// instance
					tdwg.Add(1)
					go func(res *pb.Match) {
						defer tdwg.Done()

						// Get array of ticket ids to deactivate
						ticketIdsToDeactivate := []string{}
						for _, roster := range res.GetRosters() {
							for _, ticket := range roster.GetTickets() {
								ticketIdsToDeactivate = append(ticketIdsToDeactivate, ticket.GetId())
							}
						}

						// Kick off deactivation
						logger.Debugf("deactivating tickets in %v", res.GetId())
						errs := updateTicketsActiveState(ctx, ticketIdsToDeactivate, store.Deactivate)
						if len(errs) > 0 {
							logger.Errorf("Error deactivating match %v tickets: %v", res.GetId(), err)
						}
						logger.Debugf("Done deactivating tickets in %v", res.GetId())

						// Wait for deactivation to complete BEFORE returning the match.
						if cfg.GetBool("OM_MATCH_TICKET_DEACTIVATION_WAIT") {
							replComplete := false
							for !replComplete {
								time.Sleep(100 * time.Millisecond)
								// Check the deactivation has been replicated
								// to the local ticket cache, after which
								// we'll send back the match. Note: Other
								// om-core instances may not have gotten these
								// updates yet, but this is as good as we can
								// get without adding a ton of complexity and
								// slowing things down a lot by trying to
								// verify the cache state of an arbitrary
								// number of replicas.
								logger.Debugf("checking for deactivation of ticket %v", ticketIdsToDeactivate[len(ticketIdsToDeactivate)-1])
								_, replComplete = tc.inactiveSet.Load(ticketIdsToDeactivate[len(ticketIdsToDeactivate)-1])
							}
							logger.Debug("Matched tickets deactivation complete, returning match")
						}

						// Send back the match to the fan-in function.
						logger.Debugf("sending '%v' from mmf '%v' to output queue", mmf.GetName(), res.GetId())
						//logger.Infof("sending '%v' from mmf '%v' to output queue", mmf.GetName(), res.GetId())
						matchChan <- res

						// Match already returned; don't exit goroutine until
						// deactivation is complete.
						// TODO: This could be optimized by de-duplicating
						// tickets before deactivating, but we'd only expect
						// big gains if there is significant ticket collision
						// in matches. Since we expect most matchmakers to try
						// and minimize collisions, it's likely a premature
						// optimization to do this before we know how often it
						// is a significant performance gain.
						if !cfg.GetBool("OM_MATCH_TICKET_DEACTIVATION_WAIT") {
							replComplete := false
							for !replComplete {
								time.Sleep(100 * time.Millisecond)
								// Check the replication ID of the local ticket
								// cache, if it has passed the last
								// deactivcation repliation id, we can exit
								// this goroutine.
								logger.Debugf("checking for deactivation of ticket %v", ticketIdsToDeactivate[len(ticketIdsToDeactivate)-1])
								_, replComplete = tc.inactiveSet.Load(ticketIdsToDeactivate[len(ticketIdsToDeactivate)-1])
							}
							logger.Debug("Matched tickets deactivation complete for previously returned match")
						}
						return
					}(resultCopy)

				}

				// Wait for all match ticket deactivations to complete.
				tdwg.Wait()
			}
			if err != nil && !errors.Is(err, io.EOF) {
				// io.EOF indicates the MMF server closed the stream (mmf is complete)
				logger.Error(err)
			}
			//logger.Debugf("async call to mmf '%v' at %v complete", mmf.GetName(), mmfHost)
			logger.Infof("async call to mmf '%v' at %v complete", mmf.GetName(), mmfHost)
			return err
		}(mmf)
	}

	// TODO switch to an errgroup that works with contexts.
	// https://stackoverflow.com/questions/71246253/handle-goroutine-termination-and-error-handling-via-error-group
	// Wait for all mmfs to complete.
	mmfwg.Wait()
	close(waitChan)
	return nil
}

// CreateAssignments should be considered deprecated, and is only provided for
// use by developers when programming against om-core. In production, having
// the matchmaker handle this functionality doesn't make for clean failure
// domains and introduces unnecessary load on the matchmaker.
// Functionally, CreateAssignments makes replication updates for each ticket in
// the provided roster, assigning it to the server provided in the roster's
// Assignment field.
func (s *grpcServer) CreateAssignments(parentCtx context.Context, req *pb.CreateAssignmentsRequest) (*pb.CreateAssignmentsResponse, error) {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "core",
		"rpc":       "CreateAssignments",
	})

	// Input validation
	if req.GetAssignmentRoster() == nil || req.GetAssignmentRoster().GetAssignment() == nil {
		return nil, status.Error(codes.InvalidArgument, "roster with assignment is required")
	}
	assignmentPb, err := proto.Marshal(req.GetAssignmentRoster().GetAssignment())
	if assignmentPb == nil || err != nil {
		err = errors.Wrap(err, "failed to marshal the assignment protobuf")
		logger.Errorf("Error: %v", err)
		return nil, err
	}

	rChan := make(chan *store.StateResponse, len(req.GetAssignmentRoster().GetTickets()))
	numUpdates := 0
	for _, ticket := range req.GetAssignmentRoster().GetTickets() {
		if ticket.Id != "" {
			logger.Debugf("Assignment request ticket id: %v", ticket.Id)
			tc.upRequests <- &cacheUpdateRequest{
				// This command is replicated to all other om-core instances
				// using the batch writing async goroutine
				// outgoingReplicationQueue() and its effect is applied to the
				// local ticket cache in the update processing async goroutine
				// incomingReplicationQueue().
				resultsChan: rChan,
				ctx:         parentCtx,
				update: store.StateUpdate{
					Cmd:   store.Assign,
					Key:   ticket.Id,
					Value: string(assignmentPb[:]),
				},
			}
			numUpdates++
		} else {
			// Note the error but continue processing
			err = status.Error(codes.Internal, "Empty ticket ID in assignment request!")
			logger.Error(err)
		}
	}

	for i := 0; i < numUpdates; i++ {
		// The results.result field contains the redis stream id (aka the replication ID)
		// We don't actually need this for anything, so just check for an error
		results := <-rChan

		if results.Err != nil {
			// Wrap redis error and give it a gRPC internal server error status code
			err = status.Error(codes.Internal, fmt.Errorf("Unable to delete ticket: %w", results.Err).Error())
			logger.Error(err)
		}
	}

	logger.Debugf("DEPRECATED CreateAssignments: %v tickets given assignment \"%v\"", numUpdates, req.GetAssignmentRoster().GetAssignment().GetConnection())
	return &pb.CreateAssignmentsResponse{}, nil
}

// WatchAssignments should be considered deprecated, and is only provided for
// use by developers when programming against om-core. In production, having
// the matchmaker handle this functionality doesn't make for clean failure
// domains and introduces unnecessary load on the matchmaker.
// Functionally, it asynchronously watches for exactly one replication update
// containing an assignment for each of the provided ticket ids, and streams
// those assignments back to the caller.  This is rather limited functionality
// as reflected by this function's deprecated status.
func (s *grpcServer) WatchAssignments(req *pb.WatchAssignmentsRequest, stream pb.OpenMatchService_WatchAssignmentsServer) error {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "core",
		"rpc":       "WatchAssignments",
	})

	logger.Debugf("ticketids to watch: %v", req.GetTicketIds())

	// var init
	newestTicketCtime := time.Now()
	updateChan := make(chan *pb.StreamedWatchAssignmentsResponse)
	defer close(updateChan)

	for _, id := range req.GetTicketIds() {
		// Launch an asynch goroutine for each ticket ID in the request that just loops,
		// checking the sync.Map for an assignment. Once one is found, it puts that in the
		// channel, then exits.
		go func(ticketId string) {
			for {
				if value, ok := tc.assignments.Load(ticketId); ok {
					assignment := value.(*pb.Assignment)
					logger.Debugf(" watchassignments loop got assignment %v for ticketid: %v",
						assignment.Connection, ticketId)
					updateChan <- &pb.StreamedWatchAssignmentsResponse{
						Assignment: assignment,
						Id:         ticketId,
					}
					return
				}
				// TODO exp bo + jitter. Not a priority since this API is
				// marked as deprecated.
				time.Sleep(1 * time.Second)
			}
		}(id)

		x, err := strconv.ParseInt(strings.Split(id, "-")[0], 0, 64)
		if err != nil {
			logger.Error(err)
		}
		ticketCtime := time.Unix(x, 0)
		// find the newest ticket creation time.
		if ticketCtime.After(newestTicketCtime) {
			newestTicketCtime = ticketCtime
		}
	}

	// loop once per result we want to get.
	// Exit when we've gotten all results or timeout is reached.
	// From newest ticket id, get the ticket creation time and deduce the max
	// time to wait from it's creation time + configured TTL
	timeout := time.After(time.Until(newestTicketCtime.Add(time.Second * time.Duration(cfg.GetInt("OM_TICKET_TTL_SECS")))))
	for _, _ = range req.GetTicketIds() {
		select {
		case thisAssignment := <-updateChan:
			stream.Send(thisAssignment)
		case <-timeout:
			return nil
		}
	}

	return nil
}

// validateTicketIds checks each id in the input list, and sends back
// separate lists of those that are valid and those that are invalid.
func validateTicketIds(ids []string) (validIds, invalidIds []string) {
	validationRegex := regexp.MustCompile(tc.replicator.GetReplIdValidationRegex())
	for _, id := range ids {
		logger.Debugf("Validating ticket id %v", id)
		if validationRegex.MatchString(id) {
			validIds = append(validIds, id)
			logger.Debugf("ticket id %v VALID", id)
		} else {
			invalidIds = append(invalidIds, id)
			logger.Error(status.Error(codes.InvalidArgument, "Ticket ID not valid. Please pass a valid Ticket ID.").Error())
		}
	}
	return
}

// addStateUpdateErrorDetails is a convenience funtion to pass back detailed
// errors for API calls that allow the client to pass in multiple updates at once.
// gRPC API guidelines explains more about best practices using these.
// https://cloud.google.com/apis/design
//
// TODO: This is a POC implemention and needs to be expanded and finalized.
func addStateUpdateErrorDetails(errs map[string]error) error {
	st := status.New(codes.OK, "")
	if len(errs) > 0 {
		st = status.New(codes.InvalidArgument, "One or more state update arguments were invalid")

		// Generate error details
		deets := &errdetails.BadRequest{}
		for ticketId, e := range errs {
			deets.FieldViolations = append(deets.FieldViolations, &errdetails.BadRequest_FieldViolation{
				Field:       fmt.Sprintf("ticketId/%v", ticketId),
				Description: e.Error(),
			})
		}

		// Add error details
		var err error
		st, err = st.WithDetails(deets)
		if err != nil {
			logger.Errorf("Unexpected error while updateing tickets: %v", err)
		}
	}
	return st.Err()
}

// updateTicketsActiveState accepts a list of ticketids to (de-)activate, and
// generates cache updates for each.
//
// NOTE This function does no input validation, that responsibility falls on the
// function calling this one.
func updateTicketsActiveState(parentCtx context.Context, ticketIds []string, command int) map[string]error {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "core",
		"rpc":       "updateTicketsActiveState",
	})

	errs := map[string]error{}

	rChan := make(chan *store.StateResponse, len(ticketIds))
	defer close(rChan)

	numUpdates := 0
	for _, id := range ticketIds {
		logger.Debugf("update ticket status request id: %v", id)
		tc.upRequests <- &cacheUpdateRequest{
			// This command (adding/removing the id to the inactive list)
			// is replicated to all other om-core instances using the batch
			// writing async goroutine outgoingReplicationQueue() and its
			// effect is applied to the local ticket cache in the update
			// processing async goroutine incomingReplicationQueue().
			resultsChan: rChan,
			ctx:         parentCtx,
			update: store.StateUpdate{
				Cmd: command,
				Key: id,
			},
		}
		numUpdates++
	}

	// look through all results for errors
	for i := 0; i < len(ticketIds); i++ {
		results := <-rChan

		if results.Err != nil {
			// Wrap redis error and give it a gRPC internal server error status code
			// The results.result field contains the ticket id that generated the error.
			errs[results.Result] = fmt.Errorf("Unable to update ticket state: %w", results.Err)
			logger.Error(errs[results.Result])
		}
	}
	return errs
}

// dump is a simple helper function to convert a sync.Map into a standard
// golang map. Since a standard map is not concurrent-safe, use this with
// caution. Most of the places this is used are either for debugging the
// internal state of the om-core ticket cache, or in portions of the code where
// the implementation depends on taking a 'point-in-time' snapshot of the
// ticket cache because we're sending it to another process using invokeMMF()
func dump(sm *sync.Map) map[string]interface{} {
	out := map[string]interface{}{}
	sm.Range(func(key, value interface{}) bool {
		out[fmt.Sprint(key)] = value
		return true
	})
	return out
}

// setDifference is a simple helper function that performs a difference
// operation on two sets that are using sync.Map as their underlying data type.
//
// NOTE the limitation that this is taking a 'point-in-time' snapshot of the
// two sets, as they are still being updated asynchronously in a number of
// other goroutines.
func setDifference(tix *sync.Map, inactiveSet *sync.Map) (activeTickets []any) {
	inactiveTicketIds := dump(inactiveSet)
	// Copy the ticket cache, leaving out inactive tickets.
	tix.Range(func(id, ticket any) bool {
		// not ok means an error was encountered, indicating this
		// ticket is NOT inactive (meaning it IS active)
		if _, ok := inactiveTicketIds[id.(string)]; !ok {
			activeTickets = append(activeTickets, ticket)
		}
		return true
	})
	return
}
