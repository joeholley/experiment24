package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "open-match.dev/pkg/pb/v2"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"open-match.dev/core/internal/logging"
)

type openMatchService struct {
	pb.UnimplementedOpenMatchServiceServer
}

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "app.main",
	})
	signalChan chan (os.Signal) = make(chan os.Signal, 1)
	cfg        *viper.Viper     = nil
)

// The server instantiates a replicatedTicketCache on startup, and all
// ticket-reading functionality reads from this local cache. The cache also has
// the necessary data structures for replicating ticket cache changes handled
// by this server.
type replicatedTicketCache struct {
	// Local copies of all the state data.
	tickets     sync.Map
	inactiveSet sync.Map
	assignments sync.Map

	// How this replicatedTicketCache is replicated.
	replicator    rc
	upRequests    chan *cacheUpdateRequest
	replicationId string

	err error
}

// An enum for the type of commands that come in via the replication queue, since
// we switch based on the commands, which are stored as strings for human readability
// (and string comparisons are less efficient than int comparisons)
const (
	ticket = iota
	activate
	deactivate
	assign
)

// Every change to the state of the tickets in om-core is modelled as a stateUpdate.
type stateUpdate struct {
	cmd   int    // What kind of update
	key   string // The key to update
	value string // The value to associate with this key (if applicable)
}

// Results of changes to the state of the cache. This is how the clients get
// back responses to their calls.
type stateResponse struct {
	result string
	err    error
}

// The server instantiates a replicatedTicketCache on startup, and specifies
// how it wants to replicate om-core state by instantiating a stateReplicator
// that conforms to this interface. This file contains an in-memory state
// replicator for testing, that behaves similar to redis. The redis
// implementation is the reference implementation and is contained in the
// om-core redis module.
type stateReplicator interface {
	getUpdates() []*stateUpdate
	sendUpdates([]*stateUpdate) []*stateResponse
}

// cacheUpdateRequest is basically a wrapper around a stateUpdate. The state storage
// layer shouldn't need to understand the underlying context, or where the update
// request originated (which is where it will return). These are necessary for the
// gRPC server in om-core, however!
type cacheUpdateRequest struct {
	ctx         context.Context // Context, so this request can be cancelled
	update      stateUpdate
	resultsChan chan *stateResponse // Return channel to confirm the write by sending back the assigned ticket ID
}

// Local, in-memory state storage.  This mocks a tiny subset of the
// Redis Streams functionality to provide the same surface area used by om-core.
// Used for tests and local development. NOT RECOMMENDED FOR PRODUCTION
type rc struct {
	replChan  chan *stateUpdate
	replTS    time.Time
	replCount int
}

// getUpdates mocks how the om-core redis module processes a Redis Stream XRANGE command.
// https://redis.io/docs/data-types/streams/#querying-by-range-xrange-and-xrevrange
func (rc *rc) getUpdates() []*stateUpdate {
	timeout := time.After(time.Millisecond * time.Duration(cfg.GetInt("OM_REDIS_PIPELINE_WAIT_TIMEOUT_MS")))
	out := make([]*stateUpdate, 0)
	done := false
	for !done {
		select {
		case thisUpdate := <-rc.replChan:
			out = append(out, thisUpdate)
			if len(rc.replChan) == 0 {
				done = true
			}
		case <-timeout:
			done = true
		}
	}
	return out
}

// getUpdates mocks how the om-core redis module processes a Redis Stream XADD command.
// https://redis.io/docs/data-types/streams/#streams-basics
func (rc *rc) sendUpdates(updates []*stateUpdate) []*stateResponse {
	out := make([]*stateResponse, 0)
	logger.Debugf("in sendUpdates, %v updates pending", len(updates))
	for i, up := range updates {
		replId := rc.getReplId()
		if up.cmd == ticket {
			// ticket insert update commands are the only type that need
			// to track the replication ID, as it doubles as the ticket ID.
			up.key = replId
		}
		logger.Debugf(" processing update %v", i)
		rc.replChan <- up
		out = append(out, &stateResponse{result: replId, err: nil})
	}

	return out
}

// getReplId mocks how Redis Streams generate entry IDs
// https://redis.io/docs/data-types/streams/#entry-ids
func (rc *rc) getReplId() string {
	if time.Now().Unix() == rc.replTS.Unix() {
		rc.replCount += 1
	} else {
		rc.replTS = time.Now()
		rc.replCount = 0
	}
	id := fmt.Sprintf("%v-%v", strconv.FormatInt(rc.replTS.Unix(), 10), rc.replCount)
	return id
}

var tc replicatedTicketCache

func main() {

	// Context cancellation by signal
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// SIGTERM is signaled by k8s when it wants a pod to stop.
	// SIGINT is signaled when running locally and hitting Ctrl+C.
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	// Read configuration env vars, and configure logging
	cfg = readConfig()

	// Set up the replicated ticket cache.
	// fields using sync.Map come ready to use and don't need initialization
	tc.upRequests = make(chan *cacheUpdateRequest)

	// NOT RECOMMENDED FOR PRODUCTION
	if cfg.GetString("OM_STATE_STORAGE_TYPE") == "memory" {
		// Store state in local memory only. Every copy of om-core spun up
		// using this configuration is an island which does not send or receive
		// any updates to/from any other instances.
		// Useful for debugging, local development, etc.
		logger.Warnf("OM_STATE_STORAGE_TYPE configuration variable set to memory. NOT RECOMMENDED FOR PRODUCTION")
		tc.replicator = rc{
			replChan:  make(chan *stateUpdate),
			replTS:    time.Now(),
			replCount: 0,
		}
	}

	// These goroutines send and receive cache updates from state storage
	go incomingReplicationQueue(ctx, &tc)
	go outgoingReplicationQueue(ctx, &tc)

	// gRPC server startup
	logger.Infof("Open Match Server starting on port %v", cfg.GetString("PORT"))
	lis, err := net.Listen("tcp", ":"+cfg.GetString("PORT"))
	if err != nil {
		logger.Fatalf("Couldn't listen on port %v - net.Listen error: %v", cfg.GetString("PORT"), err)
	}
	logger.Infof("%v TCP listener initialized", cfg.GetString("PORT"))

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterOpenMatchServiceServer(grpcServer, &openMatchService{})
	go func() {
		if err = grpcServer.Serve(lis); err != nil {
			logger.Fatal(err)
		}
	}()
	logger.Infof("Open Match Server started")

	// Server will wait here forever for a quit signal
	<-signalChan
	grpcServer.Stop()

	// TODO remove for RC
	// Dump the final state of the cache to the log for debugging.
	if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
		dump := func(sm sync.Map) map[string]interface{} {
			out := map[string]interface{}{}
			sm.Range(func(key, value interface{}) bool {
				out[fmt.Sprint(key)] = value
				return true
			})
			return out
		}
		spew.Dump(dump(tc.tickets))
		spew.Dump(dump(tc.inactiveSet))
		spew.Dump(dump(tc.assignments))
	}
	logger.Info("Application stopped successfully.")

}

// outgoingReplicationQueue is an asynchronous goroutine that runs for the
// lifetime of the server.  It processes incoming repliation events that are
// produced by all the gRPC handlers that write to state storage, and sends
// them to the state storage selected by the configuration. It doesn't actually
// apply any of the events; once the event has been successfully replicated and
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
	redisPipeline := make([]*stateUpdate, 0)

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

				logger.Debugf(" %v requests queued for current batch", len(redisPipelineRequests))
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
			results := tc.replicator.sendUpdates(redisPipeline)
			logger.Debug(" got results")

			for index, result := range results {
				// send back this result to it's unique return channel
				redisPipelineRequests[index].resultsChan <- result
			}
		}
	}
}

// incomingReplicationQueue is an asynchronous goroutine that runs for the lifetime of the server.
// It reads all incoming replication events and applies them to the local
// ticket cache.  In practice, this does almost all the work for every gRPC
// handler /except/ InvokeMatchMakingFunction.
func incomingReplicationQueue(ctx context.Context, tc *replicatedTicketCache) {

	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "replicationQueue",
		"direction": "incoming",
	})

	// Listen to the replication streams in Redis asynchronously,
	// and add updates to the channel to be processed as they come in
	replStream := make(chan stateUpdate, cfg.GetInt("OM_REDIS_REPLICATION_MAX_UPDATES_PER_POLL"))
	go func() {
		for {
			// The getUpdates() function blocks if there are no updates, but it respects
			// the timeout defined in the config environment variable OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS
			results := tc.replicator.getUpdates()
			for _, curUpdate := range results {
				replStream <- *curUpdate
			}
		}
	}()

	// Check the channel for updates, and apply them
	for {
		// Force sleep time between processing replication updates - processing locks the
		// ticket cache so we need to make sure there's always time to read.
		time.Sleep(time.Millisecond * time.Duration(cfg.GetInt("OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS")))
		done := false

		for !done {
			// Maximum length of time we can process updates - access to the
			// ticket cache is locked during updates, so we need a hard limit here.
			lockTimeout := time.After(time.Millisecond * 500)

			// Process all incoming updates (up to the maximum defined in the
			// config var OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS), up until there
			// are none left or the lock timeout is reached.
			select {
			case curUpdate := <-replStream:
				if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
					spew.Dump(curUpdate)
				}
				// Still updates to process.
				switch curUpdate.cmd {
				case ticket:
					logger.Debug("ticket replication received")

					// Convert the ticket back to a protobuf message.
					ticketPb := &pb.Ticket{}
					proto.Unmarshal([]byte(curUpdate.value), ticketPb)
					if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
						spew.Dump(ticketPb)
					}

					// Set TicketID. Must do it post-replication since
					// TicketID is the Replication ID (e.g. redis stream id)
					ticketPb.Id = curUpdate.key

					// All tickets begin inactive
					tc.inactiveSet.Store(curUpdate.key, true)
					tc.tickets.Store(curUpdate.key, ticketPb)

				case activate:
					logger.Debug("activation replication received")
					tc.inactiveSet.Delete(curUpdate.key)

				case deactivate:
					logger.Debug("deactivate replication received")
					tc.inactiveSet.Store(curUpdate.key, true)

				case assign:
					logger.Debug("**DEPRECATED** assign replication received")

					// Convert the assignment back into a protobuf message.
					assignmentPb := &pb.Assignment{}
					proto.Unmarshal([]byte(curUpdate.value), assignmentPb)
					tc.assignments.Store(curUpdate.key, assignmentPb)
				}
			case <-lockTimeout:
				// Lock hold timeout exceeded
				logger.Debug("lock hold timeout")
				done = true
			default:
				// Nothing left to process; exit immediately
				logger.Debug("Incoming update queue empty")
				done = true
			}
		}

		// TODO: measure the impact of this with a timer metric and if it's problematic, revisit
		// and possibly do it asynchronously
		// TODO: investigate a grace period
		// cull expired tickets from local cache
		delCount := 0
		tc.tickets.Range(func(id, ticket any) bool {
			if time.Now().After(ticket.(*pb.Ticket).GetAttributes().GetExpirationTime().AsTime()) {
				tc.tickets.Delete(id)
				delCount++
			}
			return true
		})
		logger.Debugf("Removed %v expired tickets from local cache", delCount)

		// cull expired assignments from local cache
		delCount = 0
		tc.assignments.Range(func(id, _ any) bool {
			// Ticket IDs are assigned by Redis in the format of
			// <unix_timestamp>-<index> where the index increments every time a
			// ticket is created during the same second. We are only
			// interested in the ticket creation time (the unix timestamp) here.
			ticketCreationTime, err := strconv.ParseInt(strings.Split(id.(string), "-")[0], 10, 64)
			if err != nil {
				logger.Error("Unable to parse ticket ID into an unix timestamp when trying to expire old assignments")
			}
			if (time.Now().Unix() - ticketCreationTime) > int64(cfg.GetInt("OM_TICKET_TTL_SECS")) {
				tc.assignments.Delete(id)
				delCount++
			}
			return true
		})
		logger.Debugf("Removed %v expired assignments from local cache", delCount)

		// cull expired tickets from the local cache inactive ticket set
		delCount = 0
		tc.inactiveSet.Range(func(id, _ any) bool {
			// Ticket IDs are assigned by Redis in the format of
			// <unix_timestamp>-<index> where the index increments every time a
			// ticket is created during the same second. We are only
			// interested in the ticket creation time (the unix timestamp) here.
			ticketCreationTime, err := strconv.ParseInt(strings.Split(id.(string), "-")[0], 10, 64)
			if err != nil {
				logger.Error("Unable to parse ticket ID into an unix timestamp when trying to expire old assignments")
			}
			if (time.Now().Unix() - ticketCreationTime) > int64(cfg.GetInt("OM_TICKET_TTL_SECS")) {
				tc.inactiveSet.Delete(id)
				delCount++
			}
			return true
		})
		logger.Debugf("Removed %v expired assignments from local cache", delCount)

	}
}

// CreateTicket generates an event to update the ticket state storage, adding a
// new ticket. The ticket's id will be generated by the state storage and
// returned asynchronously.  This request hangs until it can return the ticket
// id. When a ticket is created, it starts off as inactive and must be
// activated with the ActivateTickets call. This ensures that no ticket that
// was not successfully replicated and returned to the om-core client is ever
// put in the pool.
func (s *openMatchService) CreateTicket(parentCtx context.Context, req *pb.CreateTicketRequest) (*pb.CreateTicketResponse, error) {

	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "openMatchService",
		"rpc":       "CreateTicket",
	})

	// Input validation
	if req.GetTicket().GetId() != "" {
		logger.Warnf("CreateTicket request included a ticketid! (%v). Open Match assigns Ticket IDs and this value will be overwritten. See documentation for more details.", req.GetTicket().GetId())
		_ = req
	}
	// Get timestamps from ticket in the request
	crtime := req.GetTicket().GetAttributes().GetCreationTime()
	extime := req.GetTicket().GetAttributes().GetExpirationTime()

	// Set creation timestamp if request didn't provide one
	if !crtime.IsValid() {
		logger.Info("CreationTime provided is invalid or nil; replacing with current time")
		crtime = timestamppb.Now()
	}

	// Set expiration timestamp if request didn't provide one
	if !extime.IsValid() {
		logger.Infof("ExpirationTime provided is invalid or nil; replacing with current time + OM_TICKET_TTL_SECS (%v)", cfg.GetInt("OM_TICKET_TTL_SECS"))
		extime = timestamppb.New(time.Now().Add(time.Second * time.Duration(cfg.GetInt("OM_TICKET_TTL_SECS"))))
	}

	// Dump ticket to STDOUT if debugging
	if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
		spew.Dump(req.GetTicket())
	}

	// Make updated ticket with new creation/expiration time
	ticketPb, err := proto.Marshal(&pb.Ticket{
		Extensions: req.GetTicket().GetExtensions(),
		Attributes: &pb.Ticket_FilterableData{
			Tags:           req.GetTicket().GetAttributes().GetTags(),
			StringArgs:     req.GetTicket().GetAttributes().GetStringArgs(),
			DoubleArgs:     req.GetTicket().GetAttributes().GetDoubleArgs(),
			CreationTime:   crtime,
			ExpirationTime: extime,
		},
	})
	if ticketPb == nil || err != nil {
		err = errors.Wrap(err, "failed to marshal the ticket protobuf")
		logger.Errorf(" CreateTicket: %v", err)
		return &pb.CreateTicketResponse{}, err
	}

	// Make a results return channel, and queue our cache update request to
	// replicate this ticket to other om-core instances.
	rChan := make(chan *stateResponse)
	tc.upRequests <- &cacheUpdateRequest{
		// Writing this ticket to redis will be pipelined for better performance under high rps
		// Here we generate the arguments for the redis call. When this cacheUpdateRequest is
		// read in the batch writing async goroutine, the redis command will actually be run.
		//
		// The batch writing async goroutine can be found in main()
		update: stateUpdate{
			cmd:   ticket,
			value: string(ticketPb[:]),
		},
		resultsChan: rChan,
		ctx:         parentCtx,
	}
	logger.Debug("waiting for results")

	// Get the results
	results := <-rChan

	logger.Debug(results.result)
	return &pb.CreateTicketResponse{TicketId: results.result}, results.err
}

// DeactivateTicket is a lazy deletion process: it adds the provided ticketID to the inactive list,
// which prevents it from appearing in player pools, and the ticket itself is deleted when it expires
// from the event stream in Redis.
func (s *openMatchService) DeactivateTicket(parentCtx context.Context, req *pb.DeactivateTicketRequest) (*pb.DeactivateTicketResponse, error) {

	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "openMatchService",
		"rpc":       "DeactivateTicket",
	})

	var err error
	err = nil

	// Input validation
	if req.GetTicketId() == "" {
		err = status.Error(codes.Internal, "No ticket ID in request!")
		logger.Error(err)
		return &pb.DeactivateTicketResponse{}, err
	}

	logger.Infof("request id: %v", req.GetTicketId())
	rChan := make(chan *stateResponse)
	tc.upRequests <- &cacheUpdateRequest{
		// This command (adding the id to the inactive list) is replicated to all other om-core instances using
		// the batch writing async goroutine outgoingReplicationQueue() and its effect is
		// applied to the local ticket cache in the update processing async goroutine incomingReplicationQueue().
		resultsChan: rChan,
		ctx:         parentCtx,
		update: stateUpdate{
			cmd: deactivate,
			key: req.GetTicketId(),
		},
	}

	// The results.result field contains the redis stream id (aka the replication ID)
	// We don't actually need this for anything, so just check for an error
	results := <-rChan
	if results.err != nil {
		// Wrap redis error and give it a gRPC internal server error status code
		err = status.Error(codes.Internal, fmt.Errorf("Unable to delete ticket: %w", results.err).Error())
	}

	return &pb.DeactivateTicketResponse{}, err
}

// ActivateTickets accepts a list of ticketids to activate, and generates replication updates for each activation event.
func (s *openMatchService) ActivateTickets(parentCtx context.Context, req *pb.ActivateTicketsRequest) (*pb.ActivateTicketsResponse, error) {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "openMatchService",
		"rpc":       "ActivateTickets",
	})

	var err error
	err = nil

	rChan := make(chan *stateResponse, len(req.TicketIds))
	numUpdates := 0
	for _, id := range req.TicketIds {
		// Input validation
		if id != "" {
			logger.Infof("Activate request id: %v", id)
			tc.upRequests <- &cacheUpdateRequest{
				// This command (adding the id to the active list) is replicated to all other om-core instances using
				// the batch writing async goroutine outgoingReplicationQueue() and its effect is
				// applied to the local ticket cache in the update processing async goroutine incomingReplicationQueue().
				resultsChan: rChan,
				ctx:         parentCtx,
				update: stateUpdate{
					cmd: activate,
					key: id,
				},
			}
			numUpdates++
		} else {
			// Note the error but continue processing
			err = status.Error(codes.Internal, "Empty ticket ID in request!")
			logger.Error(err)
		}
	}

	for i := 0; i < numUpdates; i++ {
		// The results.result field contains the redis stream id (aka the replication ID)
		// We don't actually need this for anything, so just check for an error
		results := <-rChan

		if results.err != nil {
			// Wrap redis error and give it a gRPC internal server error status code
			err = status.Error(codes.Internal, fmt.Errorf("Unable to delete ticket: %w", results.err).Error())
			logger.Error(err)
		}
	}

	return &pb.ActivateTicketsResponse{}, err

}

// CreateAssignments should be considered deprecated, and is only provided for
// use by developers when programming against om-core. In production, having
// the matchmaker handle this functionality doesn't make for clean failure
// domains and introduces load on the matchmaker that is unnecessary.
// Functionally, CreateAssignments makes replication updates for each ticket in
// the provided roster, assigning it to the server provided in the roster's
// Assignment field.
func (s *openMatchService) CreateAssignments(parentCtx context.Context, req *pb.CreateAssignmentsRequest) (*pb.CreateAssignmentsResponse, error) {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "openMatchService",
		"rpc":       "CreateAssignments",
	})

	// Input validation
	assignmentPb, err := proto.Marshal(req.GetAssignmentRoster().GetAssignment())
	if assignmentPb == nil || err != nil {
		err = errors.Wrap(err, "failed to marshal the assignment protobuf")
		logger.Errorf("Error: %v", err)
		return &pb.CreateAssignmentsResponse{}, err
	}

	rChan := make(chan *stateResponse, len(req.GetAssignmentRoster().GetTickets()))
	numUpdates := 0
	for _, ticket := range req.GetAssignmentRoster().GetTickets() {
		if ticket.Id != "" {
			logger.Infof("Assignment request ticket id: %v", ticket.Id)
			tc.upRequests <- &cacheUpdateRequest{
				// This command is replicated to all other om-core instances using
				// the batch writing async goroutine outgoingReplicationQueue() and its effect is
				// applied to the local ticket cache in the update processing async goroutine incomingReplicationQueue().
				resultsChan: rChan,
				ctx:         parentCtx,
				update: stateUpdate{
					cmd:   assign,
					key:   ticket.Id,
					value: string(assignmentPb[:]),
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

		if results.err != nil {
			// Wrap redis error and give it a gRPC internal server error status code
			err = status.Error(codes.Internal, fmt.Errorf("Unable to delete ticket: %w", results.err).Error())
			logger.Error(err)
		}
	}

	logger.Infof(" CreateAssignments: %v tickets given assignment \"%v\"", numUpdates, req.GetAssignmentRoster().GetAssignment().GetConnection())
	return &pb.CreateAssignmentsResponse{}, nil
}

// WatchAssignments should be considered deprecated, and is only provided for
// use by developers when programming against om-core. In production, having
// the matchmaker handle this functionality doesn't make for clean failure
// domains and introduces load on the matchmaker that is unnecessary.
// Functionally, it asynchronously watches for exactly one replication update
// containing an assignment for each of the provided ticket ids, and streams
// those assignments back to the caller.  This is rather limited functionality
// as reflected by this function's deprecated status.
func (s *openMatchService) WatchAssignments(req *pb.WatchAssignmentsRequest, stream pb.OpenMatchService_WatchAssignmentsServer) error {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "openMatchService",
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
				// TODO exp bo + jitter
				time.Sleep(1 * time.Second)
			}
		}(id)
		x, err := strconv.ParseInt(strings.Split(id, "-")[0], 0, 64)
		if err != nil {
			logger.Error(err)
		}
		ticketCtime := time.Unix(x, 0)
		// Take the
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

// InvokeMatchmakingFunctions loops through each Pool in the provided Profile,
// applying the filters inside and adding participating tickets to those pools.
// It then attempts to connect to every matchmaking function in the provided
// list, and send that Profile with filled Pools to each. It then aggregates
// all the resulting streams into a channel on which it can return results from
// each matchmaking function asynchronously.
func (s *openMatchService) InvokeMatchmakingFunctions(req *pb.MmfRequest, stream pb.OpenMatchService_InvokeMatchmakingFunctionsServer) error {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "openMatchService",
		"rpc":       "InvokeMatchmakingFunctions",
	})
	logger.Debugf("InvokeMMFs Request for profile name %v", req.GetProfile().GetName())

	// TODO:
	// Call each MMF, add it's ctx.Done() check to a goroutine that looks for those back on a channel
	// and exits (closes the stream) when all the downstream MMFs have completed.

	// Get tickets in this profile
	// TODO: remove mock
	allTickets := map[string]*pb.Ticket{}
	activeTickets := make([]*pb.Ticket, 0)
	tc.tickets.Range(func(id, ticket any) bool {
		allTickets[id.(string)] = ticket.(*pb.Ticket)
		//allTickets = append(allTickets, ticket.(*pb.Ticket))
		return true
	})
	tc.inactiveSet.Range(func(id, _ any) bool {
		delete(allTickets, id.(string))
		return true
	})
	for _, ticket := range allTickets {
		activeTickets = append(activeTickets, ticket)
	}

	poolsMap := req.GetProfile().GetPools()
	for name, pool := range poolsMap {
		// TODO: Get players that match this pool using filters
		req.GetProfile().GetPools()[name].Participants = &pb.Roster{
			Name:    pool.GetName() + "_roster",
			Tickets: activeTickets,
		}
	}

	// TODO: cache connections using a pool and re-use
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: (time.Second * 5)}))

	// Invoke each requested MMF, and put the matches they stream back into the match channel.
	matchChan := make(chan *pb.Match)
	eg := new(errgroup.Group)
	for _, mmf := range req.GetMmfs() {

		eg.Go(func() error {
			var err error
			err = nil
			// Connect to gRPC server
			mmfHost := mmf.GetHost()
			if mmf.GetPort() > 0 {
				mmfHost = fmt.Sprintf("%v:%v", mmfHost, mmf.GetPort())
			}
			if mmf.GetType() == pb.MmfRequest_MatchmakingFunction_REST {
				logger.Error("")
				err = status.Error(codes.Internal, fmt.Errorf("REST Mmf invocation NYI %v: %w", mmfHost, err).Error())
			} else { // TODO: gRPC is default for legacy OM1 reasons, need to swap enum order in pb to change

				var conn *grpc.ClientConn
				conn, err = grpc.Dial(mmfHost, opts...)
				if err != nil {
					err = status.Error(codes.Internal, fmt.Errorf("Failed to make gRPC client connection for %v: %w", mmfHost, err).Error())
				} else {
					defer conn.Close()

					client := pb.NewMatchMakingFunctionServiceClient(conn)
					logger.Printf("connected to %v", mmfHost)

					// Set 10 second timeout for this API call
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					var stream pb.MatchMakingFunctionService_RunClient
					stream, err = client.Run(ctx, req.GetProfile())
					if err != nil {
						err = status.Error(codes.Internal, fmt.Errorf("Failed to connect to MMF at %v: %w", mmfHost, err).Error())
					}
					for {
						// Get results from MMF
						var result *pb.Match
						result, err = stream.Recv()
						if errors.Is(err, io.EOF) { // io.EOF is the grpc signal that the handling function returned
							break
						}
						if err != nil { // MMF has an error
							err = status.Error(codes.Internal, fmt.Errorf("Unable to invoke MMF at %v: %w", mmfHost, err).Error())
						} else {
							if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
								// dump results to log for debugging
								spew.Dump(result)
							}
							// Send result back to the matchmaker
							// TODO: Deactivate tickets in the match, asynchronously so this won't block.
							matchChan <- result
						}
					}
				}
			}
			logger.Infof("async call to mmf at %v complete", mmfHost)
			if err != nil && !errors.Is(err, io.EOF) {
				logger.Error(err)
			}
			return err
		})
	}

	// asynchronously send back resulting matches from MMFs as they come in.
	// TODO: add a configurable timeout for long-running MMFs
	done := make(chan bool)
	go func() {
		for {
			select {
			case match := <-matchChan:
				stream.Send(&pb.StreamedMmfResponse{Match: match})
			case <-done:
				return
			}
		}
	}()

	// Wait for all mmfs to complete.
	// TODO switch from JustErrors errgroup example to something that works with contexts.
	err := eg.Wait()
	done <- true
	return err
}

// readConfig reads Open Match configuration from the environment.
//
// readConfig sets default config values, and configures Viper to read from the environment.
// The AutomaticEnv function of Viper reads values from env vars, but critically,
// ONLY FOR KEYS IT ALREADY HAS A DEFAULT VALUE FOR. If you've defined a new env var and you're
// trying to access it as a config value in OM, you MUST specify a default for it here, or
// Viper will NOT read the value you set in the env var!
//
// By convention, all Open Match configuration keys should be ALL CAPS and start with "OM_"
//
// If both a default and env var value exist for the same variable, the env var value wins.
func readConfig() *viper.Viper {
	cfg := viper.New()

	// Logging defaults
	cfg.SetDefault("OM_LOGGING_FORMAT", "json")
	cfg.SetDefault("OM_LOGGING_LEVEL", "info")

	// Where the OM state is stored: 'memory' should not be used in production! It makes this
	// instance into an island that does not receive/send to/from other om-core instances.
	cfg.SetDefault("OM_STATE_STORAGE_TYPE", "memory") // 'Redis' to use the redis replication feature.

	// By default, OM is configured to read and write to the same redis instance. When moving to production,
	// you may want to direct reads to a replica and writes to the Redis master, or even load-balance read
	// requests across several read replicas. See documentation for more details.
	// Redis write configuration
	cfg.SetDefault("OM_REDIS_WRITE_HOST", "192.168.1.100")
	cfg.SetDefault("OM_REDIS_WRITE_PORT", 6379)
	cfg.SetDefault("OM_REDIS_PIPELINE_MAX_QUEUE_THRESHOLD", 50) // In number of update operations
	cfg.SetDefault("OM_REDIS_PIPELINE_WAIT_TIMEOUT_MS", 500)    // In milliseconds
	cfg.SetDefault("OM_TICKET_TTL_SECS", 600)                   // In seconds

	// Redis read configuration
	cfg.SetDefault("OM_REDIS_READ_HOST", "192.168.1.100")
	cfg.SetDefault("OM_REDIS_READ_PORT", 6379)
	cfg.SetDefault("OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS", 1500)       // In milliseconds
	cfg.SetDefault("OM_REDIS_REPLICATION_MAX_UPDATES_PER_POLL", 10000) // In number of update operations

	// By default, OM tries to talk to the OTEL collector on localhost. This works when doing local
	// development, and also when running a sidecar OTEL collector in a k8s pod or serverless environment.
	cfg.SetDefault("OM_OTEL_COLLECTOR_HOST", "localhost")
	cfg.SetDefault("OM_OTEL_COLLECTOR_PORT", 4317)

	// knative env vars https://cloud.google.com/run/docs/container-contract#env-vars
	cfg.SetDefault("PORT", 8080)
	cfg.SetDefault("K_SERVICE", "open_match_core")
	cfg.SetDefault("K_REVISION", "open_match_core_rev.1")
	cfg.SetDefault("K_CONFIGURATION", "open_match_core_cfg")

	// Override default values with those from the environment variables of the same name.
	cfg.AutomaticEnv()

	// DEBUG: dump cfg vars to the log
	logging.ConfigureLogging(cfg)
	logger.Debug("configuration:")
	if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
		for key, value := range cfg.AllSettings() {
			logger.Debugf("  %v: %v", key, value)
		}
	}

	return cfg
}
