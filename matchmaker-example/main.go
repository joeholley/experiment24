package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "open-match.dev/pkg/pb/v2"
)

var (
	serverAddr = flag.String("addr", "localhost:8080", "The server address in the format of host:port")
	numTickets = flag.Int("tix", 60, "number of tickets to generate")
)

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: (time.Second * 30)}))

	// Connect to gRPC server
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewOpenMatchServiceClient(conn)

	// Set timeout for this API call
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	numDeactivations := *numTickets / 2 // leave some active so we can find matches
	ids := make([]string, *numTickets)
	ch := make(chan *pb.CreateTicketResponse, *numTickets)
	daCh := make(chan *pb.DeactivateTicketResponse, numDeactivations)

	// CreateTicket Example
	{
		// An inline function that generates ticket protobuf messages to send to the CreateTickets call
		createTicketFunc := func(tags []string, sa map[string]string, da map[string]float64, crtime time.Time, extime time.Time, rchan chan *pb.CreateTicketResponse) {
			res, err := client.CreateTicket(ctx, &pb.CreateTicketRequest{Ticket: &pb.Ticket{
				//Id: id,
				Attributes: &pb.Ticket_FilterableData{
					Tags:           tags,
					StringArgs:     sa,
					DoubleArgs:     da,
					CreationTime:   timestamppb.New(crtime),
					ExpirationTime: timestamppb.New(extime),
				},
			}})
			if err != nil {
				log.Fatalf("CreateTicket failed: %v", err)
			}
			rchan <- res
		}

		// Generate some test tickets by calling the inline function above several times
		for i := 1; i < (*numTickets)+1; i++ {
			s := fmt.Sprintf("test%04d", i)
			go createTicketFunc([]string{s},
				map[string]string{s: s},
				map[string]float64{s: float64(i)},
				time.Now(),
				time.Now().Add(time.Second*time.Duration(60)),
				ch)
		}

		// Get a result for each ticket we created
		for i := 0; i < *numTickets; i++ {
			cres := <-ch
			ids[i] = cres.TicketId
			log.Printf("CreateTicket result: %v", cres)
		}
	}

	// ActivateTickets Example
	{
		_, err := client.ActivateTickets(ctx, &pb.ActivateTicketsRequest{TicketIds: ids})
		if err != nil {
			log.Printf("ActivateTickets failed: %v", err)
		}
		log.Printf("ActivateTicket complete, errors: %v", err)
	}

	// DeactivateTicket Example
	{

		// Kick off all deactivations simultaneously
		for i := 0; i < numDeactivations; i++ {
			go func(id string, rchan chan *pb.DeactivateTicketResponse) {
				res, err := client.DeactivateTicket(ctx, &pb.DeactivateTicketRequest{TicketId: id})
				if err != nil {
					log.Printf("DeactivateTicket failed: %v", err)
				}
				log.Printf("DeactivateTicket %v complete, errors: %v", id, err)
				rchan <- res
			}(ids[i], daCh)
		}

		// Block until all deactivations are complete
		for i := 0; i < numDeactivations; i++ {
			<-daCh
		}
	}

	// CreateAssignments Example
	{
		roster := &pb.Roster{
			Assignment: &pb.Assignment{Connection: "example.com:1234"},
			Tickets:    []*pb.Ticket{},
		}
		// Assign all tickets from this run to the example.com:1234 server.
		for i := 0; i < *numTickets; i++ {
			roster.Tickets = append(roster.Tickets, &pb.Ticket{Id: ids[i]})
		}
		_, err := client.CreateAssignments(ctx, &pb.CreateAssignmentsRequest{AssignmentRoster: roster})
		if err != nil {
			log.Printf("CreateAssignments failed: %v", err)
		}
		log.Printf("CreateAssignments complete, errors: %v", err)
	}

	// WatchAssignments Example
	{

		// Watch for assignments for each of the ticket ids we made.
		stream, err := client.WatchAssignments(ctx, &pb.WatchAssignmentsRequest{TicketIds: ids})
		if err != nil {
			log.Fatalf("WatchAssignments failed: %v", err)
		}
		for {
			res, err := stream.Recv()
			if err == nil {
				log.Println(res)
			} else {
				break
			}
		}
		log.Printf("CreateAssignments complete, errors: %v", err)
	}

	// InvokeMmfs Example
	{
		log.Println("Invokingmmfs")
		stream, err := client.InvokeMatchmakingFunctions(ctx, &pb.MmfRequest{
			Profile: &pb.Profile{
				Name: "profile1",
				Pools: map[string]*pb.Pool{
					"pool1": &pb.Pool{
						Name: "pool1",
					},
				},
			},
			Mmfs: []*pb.MmfRequest_MatchmakingFunction{
				&pb.MmfRequest_MatchmakingFunction{
					Host: "localhost",
					Port: 8081,
					Type: pb.MmfRequest_MatchmakingFunction_GRPC,
				},
				&pb.MmfRequest_MatchmakingFunction{
					Host: "localhost",
					Port: 8081,
					Type: pb.MmfRequest_MatchmakingFunction_GRPC,
				},
			},
		})

		if err != nil {
			log.Fatalf("Invokemmf failed: %v", err)
		}
		for {
			var res *pb.StreamedMmfResponse
			res, err = stream.Recv()
			if errors.Is(err, io.EOF) { // stream complete
				log.Println("MMFs complete")
				break
			}
			if err != nil { // server returned an error
				log.Printf("mmf returned err: %v", err)
				break
			}
			// We got a match result; print it to console
			spew.Dump(res)
		}
		log.Printf("Invokingmmfs complete, error: %v", err)
	}

}
