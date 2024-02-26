// Copyright 2019 Google LLC
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

// Package mmf provides a sample match function that uses the GRPC harness to
// set up 1v1 matches.  This sample is a reference to demonstrate the usage of
// the GRPC harness and should only be used as a starting point for your match
// function. You will need to modify the matchmaking logic in this function
// based on your game's requirements.
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "open-match.dev/pkg/pb/v2"
)

var (
	matchName = "a-simple-1v1-matchfunction"
)

// Run is this match function's implementation of the gRPC call defined in
// proto/v2/mmf.proto.  This is where your matching logic goes.
func (s *mmfServer) Run(req *pb.Profile, stream pb.MatchMakingFunctionService_RunServer) error {
	// Fetch tickets for the pools specified in the Match Profile.
	log.Printf("Generating proposals for function %v", req.GetName())

	// In this sample, just grab the first pool of tickets in the profile
	// and ignore the rest.
	tickets := []*pb.Ticket{}
	for pname, pool := range req.GetPools() {
		log.Printf("Getting tickets from pool %v", pname)
		tickets = pool.GetParticipants().GetTickets()
	}

	t := time.Now().Format("2006-01-02T15:04:05.00")

	// We'll make 1v1 sessions, so each match will contain a roster of 2 players.
	rosterPlayers := make([]*pb.Ticket, 0, 2)
	matchNum := 0

	for _, ticket := range tickets {
		log.Printf("FIFO sample, adding next ticket id %v to match", ticket.Id)
		rosterPlayers = append(rosterPlayers, ticket)

		if len(rosterPlayers) >= 2 {
			rosters := make(map[string]*pb.Roster)
			rName := fmt.Sprintf("%v_roster%04d", matchName, matchNum)

			// make a new timestamp to add to the roster extension field.
			ex := make(map[string]*anypb.Any)
			now, err := anypb.New(timestamppb.Now())
			if err != nil {
				panic(err)
			}
			ex["CreationTime"] = now

			// Populate the roster for this match.
			rosters[rName] = &pb.Roster{
				Name:       rName,
				Tickets:    rosterPlayers,
				Extensions: ex,
			}

			// Stream the generated match back to Open Match.
			id := fmt.Sprintf("profile-%s-time-%s-num-%d", matchName, t, matchNum)
			log.Printf("Streaming match '%v' back to om-core", id)
			if err := stream.Send(&pb.Match{
				Id:      id,
				Score:   100,
				Mmf:     matchName,
				Profile: req.Name,
				Rosters: rosters,
			}); err != nil {
				log.Printf("Failed to stream proposals to Open Match, got %s", err.Error())
				return err
			}

			// Re-initialize the roster variable for the next match.
			rosterPlayers = make([]*pb.Ticket, 0, 2)
			matchNum++
		}
	}

	return nil
}

type mmfServer struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
}

// Basic gRPC server for an MMF.
func main() {
	var err error
	var port int

	// Check the knative/Cloud Run auto-populated env var for our port binding
	// Default to 8081
	{
		var ok bool
		var runPort string

		if runPort, ok = os.LookupEnv("PORT"); ok {
			port, err = strconv.Atoi(runPort)
		}
		if err != nil || !ok {
			port = 8081
		}
	}

	// Create and host a new gRPC service on the configured port.
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("TCP net listener initialization failed for port %v, got %s", port, err.Error())
	}
	log.Printf(" TCP net listener initialized for port %v", port)

	var opts []grpc.ServerOption
	server := grpc.NewServer(opts...)
	pb.RegisterMatchMakingFunctionServiceServer(server, &mmfServer{})
	if err = server.Serve(ln); err != nil {
		log.Fatalf("gRPC serve failed, got %s", err.Error())
	}
	log.Printf("Open Match Server started")

	return
}
