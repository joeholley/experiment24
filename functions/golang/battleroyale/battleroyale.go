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

package battleroyale

import (
	"fmt"
	"log"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	knownpb "google.golang.org/protobuf/types/known/wrapperspb"
	"open-match.dev/mmf/server"
	pb "open-match.dev/pkg/pb/v2"
)

const (
	playersInMatch = 100
	matchName      = "battle-royale"
	poolName       = "all"
	regionArg      = "region"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "matchmaking_function",
		"function":  "battleroyale",
	})
)

func battleRoyalRegionName(i int) string {
	return fmt.Sprintf("region_%d", i)
}

func Scenario() *BattleRoyalScenario {
	return &BattleRoyalScenario{
		regions: 20,
	}
}

type BattleRoyalScenario struct {
	regions int
}

type MmfServer struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
}

func (s *MmfServer) Run(stream pb.MatchMakingFunctionService_RunServer) error {
	req, err := server.GetChunkedRequest(stream)
	if err != nil {
		logger.Errorf("error getting chunked request: %v", err)
	}
	logger.Infof("Generating matches for profile %v", req.GetName())

	// In this sample, don't distinguish between tickets in different pools -
	// treat every ticket in every pool the same. However, since the same
	// ticket can appear in multiple pools, start by adding tickets to a map
	// (effectively creating a set), then copy the de-duplicated tickets to
	// a slice since that's what the matchmaking algo wants to work on.
	tickets := []*pb.Ticket{}
	mapTickets := map[string]pb.Ticket{}
	for pname, pool := range req.GetPools() {
		log.Printf("Found %v tickets in pool %v", len(tickets), pname)
		for _, ticket := range pool.GetParticipants().GetTickets() {
			mapTickets[ticket.GetId()] = *ticket
		}
	}

	for _, ticket := range mapTickets {
		tickets = append(tickets, &ticket)
	}

	t := time.Now().Format("2006-01-02T15:04:05.00")
	matchNum := 0
	for i := 0; i+playersInMatch <= len(tickets); i += playersInMatch {

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
			Name: rName,
			//Tickets:    rosterPlayers,
			Tickets:    tickets[i : i+playersInMatch],
			Extensions: ex,
		}

		// Stream the generated match back to Open Match.
		id := fmt.Sprintf("profile-%s-time-%s-num-%d", matchName, t, matchNum)
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
		logger.Debugf("Streaming match '%v' back to om-core with roster of %v tickets", id, playersInMatch)
		if err := stream.Send(&pb.StreamedMmfResponse{Match: &pb.Match{
			Id:      id,
			Rosters: rosters,
			Extensions: map[string]*anypb.Any{
				"score":       score,
				"mmfName":     mmfName,
				"profileName": profileName,
			},
		},
		}); err != nil {
			logger.Debugf("Failed to stream proposals to Open Match, got %s", err.Error())
			return err
		}

		//if err := stream.Send(&pb.Match{
		//	Id: fmt.Sprintf("profile-%v-time-%v-%v",
		//		p.GetName(),
		//		time.Now().Format("2006-01-02T15:04:05.00"),
		//		matchNum),
		//	Score:   1234,
		//	Mmf:     matchName,
		//	Profile: p.GetName(),
		//	Rosters: map[string]*pb.Roster{rName: &pb.Roster{
		//		Name:    rName,
		//		Tickets: tickets[i : i+playersInMatch],
		//	},
		//	},
		//}); err != nil {
		//	log.Printf("Failed to stream match to Open Match, got %s", err.Error())
		//	return err
		//}
		matchNum++
	}
	logger.Infof("Total of %v matches returned", matchNum)

	return nil
}
