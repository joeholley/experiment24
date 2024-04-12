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
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/functions/golang/battleroyale"
	"open-match.dev/functions/golang/soloduel"
	"open-match.dev/functions/golang/teamshooter"
	mmf "open-match.dev/mmf/server"
	pb "open-match.dev/pkg/pb/v2"
)

var (
	connType    = flag.String("conn", "http", "Type of connection to make to om-core: ['http' (default), 'grpc']")
	serverAddr  = flag.String("omhost", "localhost:8080", "The om-core server host:port")
	mmCycles    = flag.Int("cycles", math.MaxInt32, "number of times to invoke MMFs")
	numTickets  = flag.Int("tix", 500, "number of tickets to simulate per second")
	duration    = flag.Int("dur", math.MaxInt32, "number seconds to simulate player clients requesting matchmaking")
	simPlayers  = flag.Bool("sim", true, "turn on simulation of player clients requesting matchmaking")
	verbose     = flag.Bool("v", false, "verbose")
	ignoreEmpty = flag.Bool("ignoreempty", false, "Don't exit on three consecutive empty MMF result cycles")
	startTime   time.Time
	logger      = logrus.WithFields(logrus.Fields{
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
	mmfTeam := &teamshooter.MmfServer{}
	mmfTeamSpec := &pb.MatchmakingFunctionSpec{
		Name: "teamshooter",
		Host: "localhost",
		Port: 8083,
		Type: pb.MatchmakingFunctionSpec_GRPC,
	}
	go func() { mmf.StartServer(mmfTeamSpec.Port, mmfTeam) }()
	mmfSpecs := []*pb.MatchmakingFunctionSpec{
		mmfFifoSpec,
		mmfBrSpec,
		mmfTeamSpec,
	}

	// Connect to om-core server
	var grpcClient pb.OpenMatchServiceClient
	var httpClient *http.Client
	log.Printf("Connecting to OM Core using grpc at %v", *serverAddr)
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	//defer conn.Close() // not needed, the conn is in scope for this entire example
	grpcClient = pb.NewOpenMatchServiceClient(conn)
	log.Printf("Connected to OM Core using grpc at %v", *serverAddr)
	log.Printf("Connecting to OM Core using http at %v", *serverAddr)
	httpClient = &http.Client{}
	log.Printf("Connected to OM Core using http at %v", *serverAddr)

	// Set timeout for this API call
	//connDuration := 300
	//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(connDuration)*time.Second)
	//log.Printf("Maximum connection duration %v seconds", connDuration)
	//defer cancel()
	ctx := context.Background()

	// Start mock MatchMaker frontend to simulate game client players queuing
	// for matchmaking
	if *simPlayers {
		mmfe := newMockMatchmakerFrontend(*duration, *numTickets, grpcClient, httpClient)
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
		reqPb := &pb.MmfRequest{
			Mmfs: mmfSpecs,
			Profile: &pb.Profile{
				Name: "example_profile",
				Pools: map[string]*pb.Pool{
					// Pool of tickets where the player selected a tank class
					"tank": &pb.Pool{Name: "tank",
						StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
							&pb.Pool_StringEqualsFilter{StringArg: "class", Value: "tank"},
						},
					},
					// Pool of tickets where the player selected a dps class
					"dps": &pb.Pool{Name: "dps",
						StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
							&pb.Pool_StringEqualsFilter{StringArg: "class", Value: "dps"},
						},
					},
					// Pool of tickets where the player selected a healer class
					"healer": &pb.Pool{Name: "healer",
						StringEqualsFilters: []*pb.Pool_StringEqualsFilter{
							&pb.Pool_StringEqualsFilter{StringArg: "class", Value: "healer"},
						},
					},
				},
			},
		}

		var resPb *pb.StreamedMmfResponse
		if *connType == "grpc" {
			stream, err := grpcClient.InvokeMatchmakingFunctions(ctx, reqPb)
			if err != nil {
				log.Fatalf("Invokemmf failed: %v", err)
			}

			// Loop to gather all streaming matches.
			for {
				resPb, err = stream.Recv()
				if errors.Is(err, io.EOF) { // stream complete
					logger.Infof("MMFs complete: %v", err)
					err = nil
					break
				}
				if err != nil { // server returned an error
					logger.Errorf("MMF returned err: %v", err)
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
					rejectedMatches = append(rejectedMatches, resPb.GetMatch())
				} else {
					acceptedMatchesCounter.Add(ctx, 1)
					matches = append(matches, resPb.GetMatch())
				}
			}
		} else {
			reqJson, err := protojson.Marshal(reqPb)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"pb_message": "MmfRequest",
				}).Errorf("cannot marshal protobuf to json")
			}

			// Set up our request parameters
			req, err := http.NewRequestWithContext(
				ctx,    // Context
				"POST", // HTTP verb
				"http://localhost:58081/v2/matches:fetch", // RESTful OM2 path
				bytes.NewReader(reqJson),                  // JSON-marshalled protobuf request message
			)
			if err != nil {
				logger.Errorf("cannot create http request with context")
			}
			req.Header.Set("Content-Type", "application/json")

			// Send request
			resp, err := httpClient.Do(req)
			if err != nil {
				logger.Errorf("cannot execute http request")
			}
			// Check for a successful HTTP status code
			if resp.StatusCode != http.StatusOK {
				log.Fatalf("Request failed with status: %s", resp.Status)
			}

			// Process the stream
			for {
				// Simulate message decoding (adjust based on your actual message format)
				var msg string
				_, err := fmt.Fscanf(resp.Body, "%s\n", &msg)

				if err == io.EOF {
					resp.Body.Close()
					break // End of stream
				}
				if err != nil {
					resp.Body.Close()
					logger.Fatalf("Error reading stream: %v", err)
				}

				// NOTE: this is NOT a clean implementation.
				// grpc-gateway returns your result inside a JSON
				// container under the key 'result', so the actual text we need
				// to pass to protojson.Unmarshal needs to omit the top level
				// JSON object. Rather than marshal the text to json (which is
				// slow), we just slice off the first 10 letters
				// '{"result":'
				// and the last character '}' (which is fast, but brittle)
				trimmedMsg := msg[10:(len(msg) - 1)]
				// Unmarshal json back into protobuf
				resPb = &pb.StreamedMmfResponse{}
				err = protojson.Unmarshal([]byte(trimmedMsg), resPb)
				if err != nil {
					logger.WithFields(logrus.Fields{
						"pb_message": "StreamedMmfResponse",
					}).Errorf("cannot unmarshal http response body back into protobuf")
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
					rejectedMatches = append(rejectedMatches, resPb.GetMatch())
				} else {
					acceptedMatchesCounter.Add(ctx, 1)
					matches = append(matches, resPb.GetMatch())
				}

			}

		}

		// Print status for this cycle
		logger.Infof("%02d/%02d Invokingmmfs complete, %v matches received, %v approved %v rejected, error: %v",
			*mmCycles+1-i, *mmCycles,
			len(matches)+len(rejectedMatches), len(matches), len(rejectedMatches), err)

		// Exit if we've seen three consecutive MMF invocations with no
		// matches; something's likely broken or the fake frontend is not
		// configured to generate any more mock player tickets.
		if (len(matches) + len(rejectedMatches)) == 0 {
			if !*ignoreEmpty && emptyCycles >= 3 {
				logger.Warn("No matches returned from three consecutive MMF invocations; exiting. Turn this feature off with -ignoreempty flag")
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
			logger.Infof("Queued %v tickets to be re-activated due to rejected matches", numRejectedTickets)
			// DEBUG: just helps distinguish from initial ticket activations in traces and logs.
			ctx := context.WithValue(ctx, "type", "re-activate")
			activateTickets(ctx, grpcClient, httpClient, ticketIdsToActivate)
		}
	}

	logger.Info("Exiting...")
}

// activateTickets gathers all tickets awaiting activation, and batches them
// into calls of the size defined in maxActivationsPerCall (see
// open-match.dev/core/internal/config for limits on how many actions you can
// request in a single API call, and use that value)
func activateTickets(ctx context.Context, grpcClient pb.OpenMatchServiceClient, httpClient *http.Client, ticketIdsToActivate chan string) {
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
			// maxActivationsPerCall, this way the calls are made concurrently.
			go func(ticketsAwaitingActivation []string) {

				// Quick in-line function to grab information
				// the calling function put into the context when
				// logging errors (to aid debugging)
				callerFromContext := func() string {
					ts, ok := ctx.Value("type").(string)
					if !ok {
						logger.Error("unable to get caller type from context")
						return "undefined"
					}
					return ts
				}

				// Track this goroutine
				activationWg.Add(1)
				defer activationWg.Done()

				// Activate tickets
				reqPb := &pb.ActivateTicketsRequest{TicketIds: ticketsAwaitingActivation}
				var resPb *pb.ActivateTicketsResponse
				var err error

				if *connType == "grpc" {
					resPb, err = grpcClient.ActivateTickets(ctx, reqPb)
				} else { // RESTful HTTP using JSON (grpc-gateway)
					// Marshal request into json
					req, err := protojson.Marshal(reqPb)
					if err != nil {
						logger.WithFields(logrus.Fields{
							"caller":     callerFromContext,
							"pb_message": "ActivateTicketsRequest",
						}).Errorf("cannot marshal protobuf to json")
					}

					// RESTful version of the ActivateTickets() call
					body, err := restfulGrpcPost(ctx, httpClient, "tickets:activate", req)
					if err == nil {
						// Unmarshal response into protobuf
						resPb = &pb.ActivateTicketsResponse{}
						err = protojson.Unmarshal(body, resPb)
						if err != nil {
							logger.WithFields(logrus.Fields{
								"caller":     callerFromContext,
								"pb_message": "ActivateTicketsResponse",
							}).Errorf("cannot unmarshal HTTP JSON response body back to protobuf")
						}

					}
				}
				if err != nil {
					logger.WithFields(logrus.Fields{
						"caller": callerFromContext,
					}).Errorf("ActivateTickets failed: %w", err)
				}
				logger.Debugf("ActivateTicket complete, results: %v", resPb)
			}(ticketsAwaitingActivation)
		}
	}

	// After gathering all the activations and making all the necessary
	// calls, wait on the waitgroup to finish.
	activationWg.Wait()
}

func restfulGrpcPost(ctx context.Context, client *http.Client, urlpath string, pbReq []byte) ([]byte, error) {
	// Set up our request parameters
	req, err := http.NewRequestWithContext(
		ctx,                                  // Context
		"POST",                               // HTTP verb
		"http://localhost:58081/v2/"+urlpath, // RESTful OM2 path
		bytes.NewReader(pbReq),               // JSON-marshalled protobuf request message
	)
	if err != nil {
		logger.Errorf("cannot create http request with context")
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("cannot execute http request")
		return nil, err
	}

	// Get results
	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		logger.Errorf("cannot read bytes from http response body")
		return nil, err
	}
	return body, err
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

	// Make a fake game client request
	crTime := time.Now()
	s := strconv.FormatInt(crTime.UnixNano(), 10)
	ticket := &pb.Ticket{
		Attributes: &pb.Ticket_FilterableData{
			//StringArgs:   map[string]string{"mode": "casual"},
			Tags:         []string{s},
			DoubleArgs:   map[string]float64{s: float64(crTime.UnixNano())},
			CreationTime: timestamppb.New(crTime),
		},
	}

	// Randomly select a character archetype this fake player wants to play
	var selectedClass string
	switch rand.Intn(5) + 1 { // 1d5
	case 1:
		selectedClass = "tank"
	case 2:
		selectedClass = "healer"
	default: // 60% of players will select dps
		selectedClass = "dps"
	}
	ticket.Attributes.StringArgs = map[string]string{"class": selectedClass}

	return ticket
}

// Properties for our fake frontend.
type mockMatchmakerFrontend struct {
	httpClient       *http.Client
	grpcClient       pb.OpenMatchServiceClient
	duration         int // How many seconds to generate tickets
	ticketsPerSecond int // How many tickets to generate each second
}

type mmFrontEnd interface {
	Run(context.Context)
	proxyCreateTicket(func(context.Context) *pb.Ticket) string
}

// newMockMatchmakerFrontend initializes a mock frontend service that simulates
// players queuing for matchmaking every second
func newMockMatchmakerFrontend(d int, t int, g pb.OpenMatchServiceClient, h *http.Client) *mockMatchmakerFrontend {
	return &mockMatchmakerFrontend{
		httpClient:       h,
		grpcClient:       g,
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
			activateTickets(ctx, mmfe.grpcClient, mmfe.httpClient, ticketIdsToActivate)
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
	reqPb := &pb.CreateTicketRequest{Ticket: ticket}
	var resPb *pb.CreateTicketResponse
	var err error
	var buf []byte
	ticketId := ""

	if *connType == "grpc" {
		resPb, err = mmfe.grpcClient.CreateTicket(ctx, reqPb)
	} else { // RESTful http using grpc-gateway

		// Marshal request into json
		buf, err = protojson.Marshal(reqPb)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"pb_message": "CreateTicketsRequest",
			}).Errorf("cannot marshal proto to json")
		}

		// RESTful version of the CreateTicket() call
		buf, err = restfulGrpcPost(ctx, mmfe.httpClient, "tickets", buf)
		if err == nil {

			// Unmarshal json back into protobuf
			resPb = &pb.CreateTicketResponse{}
			err = protojson.Unmarshal(buf, resPb)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"pb_message": "CreateTicketsResponse",
				}).Errorf("cannot unmarshal http response body back into protobuf")
			}
		}
	}
	if err != nil {
		logger.Errorf("CreateTicket failed: %v", err)
	}

	if resPb != nil {
		ticketId = resPb.TicketId
	}
	logger.Debugf("CreateTicket complete, ticketID: %v, err: %v", ticketId, err)
	return ticketId
}
