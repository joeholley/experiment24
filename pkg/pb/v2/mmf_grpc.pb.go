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

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.2
// source: mmf.proto

package v2

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	MatchMakingFunctionService_Run_FullMethodName = "/open_match.v2.MatchMakingFunctionService/Run"
)

// MatchMakingFunctionServiceClient is the client API for MatchMakingFunctionService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MatchMakingFunctionServiceClient interface {
	// INTERNAL USE ONLY
	// -----------------------------------------------------------------------------------------------------
	// This is the specification open match uses to call your matchmaking function on your behalf.
	Run(ctx context.Context, in *Profile, opts ...grpc.CallOption) (MatchMakingFunctionService_RunClient, error)
}

type matchMakingFunctionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMatchMakingFunctionServiceClient(cc grpc.ClientConnInterface) MatchMakingFunctionServiceClient {
	return &matchMakingFunctionServiceClient{cc}
}

func (c *matchMakingFunctionServiceClient) Run(ctx context.Context, in *Profile, opts ...grpc.CallOption) (MatchMakingFunctionService_RunClient, error) {
	stream, err := c.cc.NewStream(ctx, &MatchMakingFunctionService_ServiceDesc.Streams[0], MatchMakingFunctionService_Run_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &matchMakingFunctionServiceRunClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type MatchMakingFunctionService_RunClient interface {
	Recv() (*Match, error)
	grpc.ClientStream
}

type matchMakingFunctionServiceRunClient struct {
	grpc.ClientStream
}

func (x *matchMakingFunctionServiceRunClient) Recv() (*Match, error) {
	m := new(Match)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// MatchMakingFunctionServiceServer is the server API for MatchMakingFunctionService service.
// All implementations must embed UnimplementedMatchMakingFunctionServiceServer
// for forward compatibility
type MatchMakingFunctionServiceServer interface {
	// INTERNAL USE ONLY
	// -----------------------------------------------------------------------------------------------------
	// This is the specification open match uses to call your matchmaking function on your behalf.
	Run(*Profile, MatchMakingFunctionService_RunServer) error
	mustEmbedUnimplementedMatchMakingFunctionServiceServer()
}

// UnimplementedMatchMakingFunctionServiceServer must be embedded to have forward compatible implementations.
type UnimplementedMatchMakingFunctionServiceServer struct {
}

func (UnimplementedMatchMakingFunctionServiceServer) Run(*Profile, MatchMakingFunctionService_RunServer) error {
	return status.Errorf(codes.Unimplemented, "method Run not implemented")
}
func (UnimplementedMatchMakingFunctionServiceServer) mustEmbedUnimplementedMatchMakingFunctionServiceServer() {
}

// UnsafeMatchMakingFunctionServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MatchMakingFunctionServiceServer will
// result in compilation errors.
type UnsafeMatchMakingFunctionServiceServer interface {
	mustEmbedUnimplementedMatchMakingFunctionServiceServer()
}

func RegisterMatchMakingFunctionServiceServer(s grpc.ServiceRegistrar, srv MatchMakingFunctionServiceServer) {
	s.RegisterService(&MatchMakingFunctionService_ServiceDesc, srv)
}

func _MatchMakingFunctionService_Run_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Profile)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MatchMakingFunctionServiceServer).Run(m, &matchMakingFunctionServiceRunServer{stream})
}

type MatchMakingFunctionService_RunServer interface {
	Send(*Match) error
	grpc.ServerStream
}

type matchMakingFunctionServiceRunServer struct {
	grpc.ServerStream
}

func (x *matchMakingFunctionServiceRunServer) Send(m *Match) error {
	return x.ServerStream.SendMsg(m)
}

// MatchMakingFunctionService_ServiceDesc is the grpc.ServiceDesc for MatchMakingFunctionService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MatchMakingFunctionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "open_match.v2.MatchMakingFunctionService",
	HandlerType: (*MatchMakingFunctionServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Run",
			Handler:       _MatchMakingFunctionService_Run_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "mmf.proto",
}