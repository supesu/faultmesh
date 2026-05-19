package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/supesu/faultmesh/data-plane/pkg/logging"
	"github.com/supesu/faultmesh/data-plane/pkg/signals"
	"github.com/supesu/faultmesh/data-plane/pkg/version"

	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := logging.New()

	logger.Info().
		Str("Version", version.Version).
		Str("GitCommit", version.GitCommit).
		Str("Built", version.BuildDate).
		Msg("fm-agent starting")

	controlAddr := flag.String("control-addr", "", "control plane address")
	flag.Parse()

	if *controlAddr == "" {
		logger.Error().Msg("--control-addr is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		*controlAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)

	if err != nil {
		logger.Error().Err(err).
			Str("addr", *controlAddr).
			Msg("failed to connect to control plane")
		os.Exit(1)
	}

	client := pb.NewControlServiceClient(conn)

	_ = client // stub that jawn for now

	logger.Info().
		Str("addr", *controlAddr).
		Msg("connected to control plane (stub)")

	// wait for shutdown
	ctx2, stop := signals.Context()
	defer stop()

	<-ctx2.Done()

	fmt.Println("shutdown complete")
}
