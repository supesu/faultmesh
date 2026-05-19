package main

import (
	"flag"
	"net"
	"os"
	"strconv"

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

	host, port, err := net.SplitHostPort(*controlAddr)
	if err != nil {
		logger.Error().Err(err).Str("addr", *controlAddr).Msg("--control-addr must be host:port")
		os.Exit(1)
	}
	if host == "" {
		logger.Error().Str("addr", *controlAddr).Msg("--control-addr host is empty")
		os.Exit(1)
	}
	if p, perr := strconv.Atoi(port); perr != nil || p <= 0 || p > 65535 {
		logger.Error().Str("addr", *controlAddr).Msg("--control-addr port must be in 1..65535")
		os.Exit(1)
	}

	// lazy dial — the control plane may come up after us.
	conn, err := grpc.NewClient(
		*controlAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logger.Error().Err(err).
			Str("addr", *controlAddr).
			Msg("failed to create control plane client")
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewControlServiceClient(conn)
	_ = client // stub that jawn for now

	logger.Info().
		Str("addr", *controlAddr).
		Msg("control plane client ready (lazy dial)")

	ctx, stop := signals.Context()
	defer stop()
	<-ctx.Done()

	logger.Info().Msg("shutdown complete")
}
