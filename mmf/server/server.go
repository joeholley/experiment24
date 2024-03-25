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

// Package server contains golang implementations for common matchmaking
// function gRPC server functionality, like starting up the server, or parsing
// the chunked profile format sent by om-core.
//
//   - The MMF Server is defined in the proto/v2/mmf.proto file. If you want to
//     write your MMFs in golang, you can use the protoc-generated grpc server
//     module in pkg/pb/v2/mmf*.go. If you want to write it in a different
//     language, consult the protoc documentation for instructions on generating
//     grpc server source files in your language of choice.
//   - This package doesn't include the matchmaking function itself.  You pass
//     in a Server struct when calling Start that includes your MMF Run()
//     implementation.
//   - a typical standalone usage of this implementation can be found in
//     mmf/main.go and a useful dev iteration usage can be found in
//     matchmaker-example/main.go
package server

import (
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	pb "open-match.dev/pkg/pb/v2"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "matchmaking_function",
		"function":  "server",
	})
)

// Start creates and starts the Match Function server
func StartServer(port int32, mmfServer pb.MatchMakingFunctionServiceServer) error {
	var err error

	// Create and host a new gRPC service on the requested port.
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalf("TCP net listener initialization failed for port %v, got %s", port, err.Error())
	}

	logger.Infof("MMF gRPC Server starting on TCP port %v ", port)
	var opts []grpc.ServerOption
	server := grpc.NewServer(opts...)
	pb.RegisterMatchMakingFunctionServiceServer(server, mmfServer)
	if err = server.Serve(ln); err != nil {
		logger.Fatalf("gRPC serve failed, got %s", err.Error())
	}

	return err
}

func GetChunkedRequest(stream pb.MatchMakingFunctionService_RunServer) (*pb.Profile, error) {
	// Infinite loop that breaks on stream close (err == io.EOF) or after receiving
	// the number of chunks specified in the pb.ChunkedMmfRunRequest message
	var req *pb.Profile
	pools := make(map[string]*pb.Pool)
	for i := int32(1); i >= 0; i++ {
		in, err := stream.Recv()
		if err != nil {
			// TODO: Check if we got any portion of a valid profile, if so, attempt a run
			return nil, err
		}
		logger.Debugf("Processing chunk %02d/%02d", i, in.GetNumChunks())
		for name, pool := range in.GetProfile().GetPools() {
			logger.Debugf("concatinating pool %v", name)
			if _, ok := pools[name]; !ok {
				// First chunk containing this pool; initialize the local copy.
				pools[name] = &pb.Pool{
					Name:                    pool.GetName(),
					TagPresentFilters:       pool.GetTagPresentFilters(),
					StringEqualsFilters:     pool.GetStringEqualsFilters(),
					DoubleRangeFilters:      pool.GetDoubleRangeFilters(),
					CreationTimeRangeFilter: pool.GetCreationTimeRangeFilter(),
					Extensions:              pool.GetExtensions(),
					Participants:            pool.GetParticipants(),
				}
			} else {
				// concate pools split amoung multiple chunks
				pools[name].Participants.Tickets = append(pools[name].Participants.Tickets, pool.GetParticipants().GetTickets()...)
			}
		}
		if in.GetNumChunks() == i {
			// Read the rest of the request profile from the last chunk.
			req = &pb.Profile{
				Name:       in.GetProfile().GetName(),
				Pools:      pools,
				Extensions: in.GetProfile().GetExtensions(),
			}
			logger.Debugf("Finished receiving %v pages of MMF profile %v", in.GetProfile().GetName(), i)
			break
		}
	}
	return req, nil
}
