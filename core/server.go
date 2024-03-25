// Tries to use the guidelines at https://cloud.google.com/apis/design for the gRPC API where possible.
package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	pb "open-match.dev/pkg/pb/v2"

	"github.com/spf13/viper"
)

func start(cfg *viper.Viper) {

	// Context cancellation by signal
	signalChan := make(chan os.Signal, 1)
	//_, cancel := signal.NotifyContext(Ctx, os.Interrupt)
	//defer cancel()

	// SIGTERM is signaled by k8s when it wants a pod to stop.
	// SIGINT is signaled when running locally and hitting Ctrl+C.
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	// gRPC server startup
	logger.Infof("Open Match Server starting on port %v", cfg.GetString("PORT"))
	lis, err := net.Listen("tcp", ":"+cfg.GetString("PORT"))
	if err != nil {
		logger.Fatalf("Couldn't listen on port %v - net.Listen error: %v", cfg.GetString("PORT"), err)
	}
	var opts []grpc.ServerOption
	omServer := grpc.NewServer(opts...)
	pb.RegisterOpenMatchServiceServer(omServer, &grpcServer{})
	go func() {
		if err = omServer.Serve(lis); err != nil {
			logger.Fatal(err)
		}
	}()
	logger.Debugf("Open Match Core Server started")

	// Server will wait here forever for a quit signal
	<-signalChan
	omServer.Stop()
}
