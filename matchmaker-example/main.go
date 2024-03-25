// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// NOTE: WIP, this is a testbed right now. It does not use om-core for
// assignments as this functionality is deprecated.
//
// This an example of one approach to writing a matchmaker using Open Match
// v2.x. Many approaches are viable, and we encourage you to read the
// documentation at open-match.dev for more ideas and patterns. Minimally, a
// matchmaker using open-match is expected to do something along these lines:
//
// - Have some component that receives player matchmaking requests, enriches
// the game client-provided matching attributes (e.g. ping values, game mode
// and character selections) with additional data from your own authoritative
// systems as necessary (e.g. MMR values, character progression, purchased
// unlocks), and executes the client ticket creation flow (CreateTicket() ->
// receive TicketId -> ActivateTicket(TicketId). This component is often also
// responsible for any updating your platform services layer, so as to
// statefully track the ticket ID to client ID mapping. Typically this
// component would operate asynchronously and be horizontally scalable.
// - Have some component that runs an endless loop to trigger matchmaking. A
// typical approach might loop over these actions:
//   - Look at current server fleet state.
//   - Load or derive profiles required to fill the desired sessions.
//   - Call InvokeMMFs(profile) for each profile, concurrently. If backfilling,
//   be sure the backfill profiles are 	the first profiles sent to maximize the
//   number of tickets they have access to.
//   - Get the resulting matches, and decide which should be put into game
//   hosting sessions on which servers.
//   - Re-activate any players that were matched by om-core but not put into
//   sessions last loop.
//   - Send the assignments of tickets to sessions back to game clients.
//   Om-core has deprecated assignment api functionality which you can use in
//   development and testing, but in production you are advised to have a
//   highly-available, resilient player status service.
//
// These components can be one in the same, two separate services, or have
// their duties federated across a number of platform services. Regardless of
// the pattern used, the collection of the above functionality is referred to
// by Open Match v2.x as your 'matchmaker'.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	_ "net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/functions/golang/battleroyale"
	"open-match.dev/functions/golang/soloduel"
	mmf "open-match.dev/mmf/server"
	pb "open-match.dev/pkg/pb/v2"
)

var (
	serverAddr    = flag.String("omhost", "localhost:8080", "The om-core server host:port")
	mmfServerAddr = flag.String("mmfhost", "localhost:8081", "The host:port to bind the MMF server to")
	mmCycles      = flag.Int("cycles", 20, "number of times to invoke MMFs")
	numTickets    = flag.Int("tix", 60, "number of tickets to simulate per second")
	duration      = flag.Int("dur", 60, "number seconds to simulate player clients requesting matchmaking")
	simPlayers    = flag.Bool("sim", false, "turn on simulation of player clients requesting matchmaking")
	verbose       = flag.Bool("v", false, "verbose")
	startTime     time.Time
	logger        = logrus.WithFields(logrus.Fields{
		"app": "matchmaker_example",
	})
)

// OM Core by default only lets you activate this many tickets in a single API call.
const maxActivationsPerCall = 500

func main() {

	startTime = time.Now()

	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	//opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: (time.Second * 30)}))

	// Set up structured logging
	logrus.SetFormatter(&logrus.TextFormatter{})
	logrus.SetLevel(logrus.InfoLevel)
	if *verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// The exporter embeds a default OpenTelemetry Reader and
	// implements prometheus.Collector, allowing it to be used as
	// both a Reader and Collector.
	// By default it serves on http://localhost:2223/metrics
	registerMetrics()

	// Start local test copies of the mmf servers for om-core to talk to
	// Useful in dev & testing. In prod, you'd want to run a dedicated service
	// for your mmf servers, we recommend a serverless platform like Cloud Run
	// or kNative.
	mmfFifo := &soloduel.MmfServer{}
	mmfFifoSpec := &pb.MatchmakingFunctionSpec{
		Name: "soloduel",
		Host: "localhost",
		Port: 8081,
		Type: pb.MatchmakingFunctionSpec_GRPC,
	}
	go func() { mmf.StartServer(mmfFifoSpec.Port, mmfFifo) }()
	mmfBr := &battleroyale.MmfServer{}
	mmfBrSpec := &pb.MatchmakingFunctionSpec{
		Name: "battleroyale",
		Host: "localhost",
		Port: 8082,
		Type: pb.MatchmakingFunctionSpec_GRPC,
	}
	go func() { mmf.StartServer(mmfBrSpec.Port, mmfBr) }()
	mmfSpecs := []*pb.MatchmakingFunctionSpec{
		mmfFifoSpec,
		mmfBrSpec,
	}

	// Connect to om-core server
	log.Printf("Connecting to OM Core at %v", *serverAddr)
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	//defer conn.Close() // not needed, the conn is in scope for this entire example
	client := pb.NewOpenMatchServiceClient(conn)
	log.Printf("Connected to OM Core at %v", *serverAddr)

	// Set timeout for this API call
	connDuration := 300
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(connDuration)*time.Second)
	log.Printf("Maximum connection duration %v seconds", connDuration)
	defer cancel()

	// Start mock MatchMaker frontend to simulate game client players queuing
	// for matchmaking
	if *simPlayers {
		mmfe := newMockMatchmakerFrontend(*duration, *numTickets, client)
		go mmfe.Run(ctx)
	}

	// Main Matchmaking Loop
	emptyCycles := 0
	ticketIdsToActivate := make(chan string, 10000)
	// Loop for the configured number of cycles
	for i := *mmCycles; i > 0; i-- {
		rejectedMatches := []*pb.Match{}
		matches := []*pb.Match{}

		// InvokeMmfs Example
		logger.Debugf("Invokingmmfs")
		stream, err := client.InvokeMatchmakingFunctions(ctx, &pb.MmfRequest{
			Profile: &pb.Profile{
				Name: "profile1",
				Pools: map[string]*pb.Pool{
					"pool1": &pb.Pool{
						Name: "undifferentiated_tickets",
						StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
							&pb.Pool_StringEqualsFilter{
								StringArg: "mode", Value: "casual",
							},
						},
					},
					//"pool2": &pb.Pool{
					//	Name: "pool2",
					//	StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
					//		&pb.Pool_StringEqualsFilter{
					//			StringArg: "this", Value: "that",
					//		},
					//	},
					//},
				},
			},
			Mmfs: mmfSpecs,
		})
		if err != nil {
			log.Fatalf("Invokemmf failed: %v", err)
		}

		// Loop to gather all streaming matches.
		for {
			var res *pb.StreamedMmfResponse
			res, err = stream.Recv()
			if errors.Is(err, io.EOF) { // stream complete
				logger.Infof("MMFs complete: %v", err)
				err = nil
				break
			}
			if err != nil { // server returned an error
				logger.Errorf("mmf returned err: %v", err)
				break
			}

			// We got a match result
			// DEBUG: print the match to console
			// spew.Dump(res)
			//
			// Simulate 1 in 100 matches being rejected by our matchmaker because
			// we don't like them or don't have available servers to host the sessions
			// right now.
			if rand.Intn(100) <= 1 {
				rejectedMatchesCounter.Add(ctx, 1)
				rejectedMatches = append(rejectedMatches, res.GetMatch())
			} else {
				acceptedMatchesCounter.Add(ctx, 1)
				matches = append(matches, res.GetMatch())
			}
		}

		// Print status for this cycle
		log.Printf("%02d/%02d Invokingmmfs complete, %v matches received, %v approved %v rejected, error: %v",
			*mmCycles-i, *mmCycles,
			len(matches)+len(rejectedMatches), len(matches), len(rejectedMatches), err)

		// Exit if we've seen three consequtive MMF invocations with no
		// matches; something's likely broken or the fake frontend is not
		// configured to generate any more mock player tickets.
		if (len(matches) + len(rejectedMatches)) == 0 {
			if emptyCycles >= 3 {
				break
			}
			// TODO: proper exp BO + jitter
			time.Sleep(2 * time.Second)
			emptyCycles++
		} else {
			emptyCycles = 0

			// re-activate tickets from rejected matches
			numRejectedTickets := 0
			for _, match := range rejectedMatches {
				for _, roster := range match.GetRosters() {
					for _, ticket := range roster.GetTickets() {
						// queue this ticket ID to be re-activated
						ticketIdsToActivate <- ticket.GetId()
						numRejectedTickets++
					}
				}
			}
			log.Printf("Queued %v tickets to be re-activated due to rejected matches", numRejectedTickets)
			// DEBUG: just helps distinguish from initial ticket activations in traces and logs.
			ctx := context.WithValue(ctx, "type", "re-activate")
			activateTickets(ctx, client, ticketIdsToActivate)
		}
	}

	logger.Info("Exiting...")
}

// activateTickets gathers all tickets awaiting activation, and batches them
// into calls of the size defined in maxActivationsPerCall (see
// open-match.dev/core/internal/config for limits on how many actions you can
// request in a single API call, and use that value)
func activateTickets(ctx context.Context, client pb.OpenMatchServiceClient, ticketIdsToActivate chan string) {
	// Activate all new tickets
	done := false
	ticketsAwaitingActivation := make([]string, 0)

	var activationWg sync.WaitGroup
	for len(ticketsAwaitingActivation) == maxActivationsPerCall || !done {

		// Collect tickets from the channel.
		ticketsAwaitingActivation = nil
		for !done {
			select {
			case tid := <-ticketIdsToActivate:
				ticketsAwaitingActivation = append(ticketsAwaitingActivation, tid)
				if len(ticketsAwaitingActivation) == maxActivationsPerCall {
					// maximum updates allowed per api call, go ahead and activate these,
					// then loop to grab what is left in the channel.
					done = true
				}
			default:
				done = true
			}
		}

		// We've got tickets to activate
		if len(ticketsAwaitingActivation) > 0 {
			logger.Debugf("ActivateTicket call with %v tickets: %v...", len(ticketsAwaitingActivation), ticketsAwaitingActivation[0])
			if len(ticketsAwaitingActivation) == maxActivationsPerCall {
				done = false
			}

			// Kick off activation in a goroutine in case we had to split on
			// maxActivationsPerCall, this way they are made concurrently.
			go func(ticketsAwaitingActivation []string) {

				// Track this goroutine
				activationWg.Add(1)
				defer activationWg.Done()

				// Activate tickets
				res, err := client.ActivateTickets(ctx,
					&pb.ActivateTicketsRequest{TicketIds: ticketsAwaitingActivation})
				if err != nil {
					ts, ok := ctx.Value("type").(string)
					if !ok {
						logger.Error("unable to get caller type from context")
					}
					logger.Errorf("(%v) ActivateTickets failed: %v", ts, err)
				}
				logger.Debugf("ActivateTicket complete, results: %v", res)
			}(ticketsAwaitingActivation)
		}
	}

	// After gathering all the activations and making all the necessary
	// calls, wait on the waitgroup to finish.
	activationWg.Wait()
}

// Convenience function for printing sync.Map data structures, used for debugging.
func dump(sm sync.Map) map[string]interface{} {
	out := map[string]interface{}{}
	sm.Range(func(key, value interface{}) bool {
		out[fmt.Sprint(key)] = value
		return true
	})
	return out
}

// --------------------------------------------------------------------------
// In production, the matchmaking frontend service that the game client talks
// to and that calls om-core would probably be a separate microservice from the
// matchmaking loop that processes matches above, so that tickets can be added
// asynchronously at scale. We're running them both in the same process here
// for simplicity.
// --------------------------------------------------------------------------

// generateMockClientRequest makes a fake game client matchmaking request. In
// this example, the game client sends an actual protobuf message, but in
// reality you'd probably use whatever communication protocol your game engine
// or dev kit encourages, and write your matchmaker frontend to parse it into
// the correct protobuf message format.
func generateMockClientRequest(ctx context.Context) *pb.Ticket {
	crTime := time.Now()
	s := strconv.FormatInt(crTime.UnixNano(), 10)
	return &pb.Ticket{
		Attributes: &pb.Ticket_FilterableData{
			Tags:         []string{s},
			StringArgs:   map[string]string{"mode": "casual"},
			DoubleArgs:   map[string]float64{s: float64(crTime.UnixNano())},
			CreationTime: timestamppb.New(crTime),
		},
	}
}

// Properties for our fake frontend.
type mockMatchmakerFrontend struct {
	client           pb.OpenMatchServiceClient
	duration         int // How many seconds to generate tickets
	ticketsPerSecond int // How many tickets to generate each second
}

type mmFrontEnd interface {
	Run(context.Context)
	proxyCreateTicket(func(context.Context) *pb.Ticket) string
}

// newMockMatchmakerFrontend initializes a mock frontend service that simulates
// players queuing for matchmaking every second
func newMockMatchmakerFrontend(d int, t int, c pb.OpenMatchServiceClient) *mockMatchmakerFrontend {
	return &mockMatchmakerFrontend{
		client:           c,
		duration:         d,
		ticketsPerSecond: t,
	}

}

// Actually 'Run' the mock matchmaker frontend, generating tickets and activating them.
func (mmfe *mockMatchmakerFrontend) Run(ctx context.Context) {
	// Make tickets every second.
	ticketIdsToActivate := make(chan string, 10000)
	var ticketWg sync.WaitGroup

	logger.Debugf("Adding simulated tickets to pool")

	// Generate i tickets every second for j seconds
	go func() {
		for j := mmfe.duration; j > 0; j-- {
			for i := 0; i < mmfe.ticketsPerSecond; i++ {
				ticketWg.Add(1)
				// Asynchronous ticket generation so we can make lots of
				// tickets concurrently.
				go func() {
					defer ticketWg.Done()
					ticketIdsToActivate <- mmfe.proxyCreateTicket(generateMockClientRequest(ctx))
				}()
			}

			logger.Debugf("%02d/%02d: %v simulated tickets added to pool",
				mmfe.ticketsPerSecond-j, j, mmfe.ticketsPerSecond)
			time.Sleep(1 * time.Second)
		}
	}()

	// activate pending tickets every second.
	go func() {
		for {
			ctx := context.WithValue(ctx, "type", "activate")
			activateTickets(ctx, mmfe.client, ticketIdsToActivate)
			time.Sleep(1 * time.Second)
		}
	}()
}

// proxyCreateTicket Example
// Your platform services layer should take the matchmaking request (in this
// example, we assume it is from a game client, but it could come via another
// of your game platform services as well), add attributes your platform
// services have authority over (ex: MMR, ELO, inventory, etc), then call Open
// Match core on the client's behalf to make a matchmaking ticket. In this sense,
// your platform service acts as a 'proxy' for the player's game client from
// the viewpoint of Open Match.
func (mmfe *mockMatchmakerFrontend) proxyCreateTicket(gameClientRequest *pb.Ticket) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Update metrics
	//createdTicketCounter.Add(ctx, 1)

	// Read game client matchmaking request
	ticket := gameClientRequest

	// Here is where your matchmaker would make additional calls to your
	// game platform services to add additional matchmaking attributes.
	// TODO ticket.Attributes = append(ticket.Attributes, somePlatformServicesCallThatReturnsAttributes())

	// With all matchmaking attributes collected from the client and the
	// game backend services, we're now ready to put the ticket in open match.
	logger.Debugf("ticket: %v", ticket)
	res, err := mmfe.client.CreateTicket(ctx, &pb.CreateTicketRequest{Ticket: ticket})
	if err != nil {
		logger.Errorf("CreateTicket failed: %v", err)
	}

	ticketId := res.TicketId
	logger.Debugf("CreateTicket complete, ticketID: %v, err: %v", ticketId, err)
	return ticketId
}
