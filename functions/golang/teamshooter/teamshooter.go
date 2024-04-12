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

// Package teamshooter provides a sample match function that uses the GRPC harness to
// set up 5v5 matches with 1 tank, 1 healer, and 3 dps.
//
// This sample is a reference to demonstrate the usage of
// the mmf harness and should only be used as a starting point for your match
// function. You will need to modify the matchmaking logic in this function
// based on your game's requirements.
package teamshooter

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	knownpb "google.golang.org/protobuf/types/known/wrapperspb"

	"open-match.dev/mmf/server"
	pb "open-match.dev/pkg/pb/v2"
)

const (
	matchName     = "team-shooter-5v5-matchfunction"
	numTeams      = 2
	maxNumMatches = 100000
)

var (
	teamComp = [...]string{"tank", "healer", "dps", "dps", "dps"}
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "matchmaking_function",
		"function":  "teamshooter",
	})
)

type MmfServer struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
}

// Run is this match function's implementation of the gRPC call defined in
// proto/v2/mmf.proto.  This is where your matching logic goes.
func (s *MmfServer) Run(stream pb.MatchMakingFunctionService_RunServer) error {
	req, err := server.GetChunkedRequest(stream)
	if err != nil {
		logger.Errorf("error getting chunked request: %v", err)
	}
	logger.Infof("Generating matches for profile %v", req.GetName())

	// DEBUG: output info about all the pools this mmf received
	for pname, pool := range req.GetPools() {
		logger.Debugf("Found %v tickets in pool %v", len(pool.GetParticipants().GetTickets()), pname)
	}

	// Initialize roster indexes.
	poolRosterIndices := make(map[string]int)
	for _, desiredTicketType := range teamComp {
		poolRosterIndices[desiredTicketType] = 0
	}

	ticketsExhausted := false
	var matchNum int
	// We'll make 5v5 sessions, so each match will contain 2 rosters of 5 players.
	// This function is meant to be easy to read and understand. It is not optimized for performance.
	for matchNum = 0; matchNum < maxNumMatches && !ticketsExhausted; matchNum++ {

		// Generate rosters
		rosters := make(map[string]*pb.Roster)
		for i := 1; i < numTeams+1; i++ {
			teamName := "team" + fmt.Sprintf("%02d", i)
			rosters[teamName] = &pb.Roster{
				Name: teamName,
			}
			rosters[teamName].Tickets = make([]*pb.Ticket, 0, len(teamComp))

			// Attempt to fill rosters. If there aren't enough of a given
			// ticket type, the MMF is finished and should exit.
			for _, desiredTicketType := range teamComp {
				if poolRosterIndices[desiredTicketType] >= len(req.GetPools()[desiredTicketType].GetParticipants().GetTickets()) {
					// set the flag to cleanly exit the MMF; not enough tickets to make more matches
					ticketsExhausted = true
				} else {
					// In a real mmf, you wouldn't just pull the next available ticket from the
					// pool; you would instead search the pool for tickets with
					// attributes/extensions that meet your match's needs.
					rosters[teamName].Tickets = append(
						rosters[teamName].Tickets,
						req.GetPools()[desiredTicketType].GetParticipants().GetTickets()[poolRosterIndices[desiredTicketType]],
					)
					poolRosterIndices[desiredTicketType]++
				}
			}
		}

		if !ticketsExhausted {
			// Create extensions for the outgoing match
			t := time.Now().Format("2006-01-02T15:04:05.00")
			id := fmt.Sprintf("profile-%s-time-%s-num-%d", matchName, t, matchNum)
			now, err := anypb.New(timestamppb.Now())
			if err != nil {
				logger.Errorf("Unable to create 'creationTime' extension for outgoing match %v", id)
			}
			score, err := anypb.New(&knownpb.Int32Value{Value: 100})
			if err != nil {
				logger.Errorf("Unable to create 'score' extension for outgoing match %v", id)
			}
			mmfName, err := anypb.New(&knownpb.StringValue{Value: matchName})
			if err != nil {
				logger.Errorf("Unable to create 'mmfName' extension for outgoing match %v", id)
			}
			profileName, err := anypb.New(&knownpb.StringValue{Value: req.Name})
			if err != nil {
				logger.Errorf("Unable to create 'profileName' extension for outgoing match %v", id)
			}

			// Stream the generated match back to Open Match.
			logger.Debugf("Streaming match '%v' back to om-core with %d rosters of %v tickets", id, numTeams, len(teamComp))
			if err := stream.Send(&pb.StreamedMmfResponse{Match: &pb.Match{
				Id:      id,
				Rosters: rosters,
				Extensions: map[string]*anypb.Any{
					"score":        score,
					"mmfName":      mmfName,
					"profileName":  profileName,
					"creationTime": now,
				},
			},
			}); err != nil {
				logger.Debugf("Failed to stream proposals to Open Match, got %s", err.Error())
				return err
			}
		}

	}
	logger.Infof("Total of %v matches returned", matchNum-1)

	return nil
}
