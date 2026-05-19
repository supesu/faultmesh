package main

import (
	"context"
	"flag"
	"net"
	"os"
	"strconv"
	"sync"

	"google.golang.org/grpc"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
	"github.com/supesu/faultmesh/data-plane/internal/collect/cgroup"
	"github.com/supesu/faultmesh/data-plane/internal/collect/proc"
	"github.com/supesu/faultmesh/data-plane/internal/encode"
	"github.com/supesu/faultmesh/data-plane/internal/grpcdial"
	"github.com/supesu/faultmesh/data-plane/internal/stream"
	"github.com/supesu/faultmesh/data-plane/pkg/logging"
	"github.com/supesu/faultmesh/data-plane/pkg/signals"
	"github.com/supesu/faultmesh/data-plane/pkg/version"
)

func main() {
	logger := logging.New()

	logger.Info().
		Str("Version", version.Version).
		Str("GitCommit", version.GitCommit).
		Str("Built", version.BuildDate).
		Msg("fm-agent starting")

	controlAddr := flag.String("control-addr", "", "control plane address")
	walDir := flag.String("wal-dir", "", "WAL directory for durable buffering (empty = in-memory)")
	tlsCert := flag.String("tls-cert", "", "client SVID cert path (enables mTLS dial)")
	tlsKey := flag.String("tls-key", "", "client SVID key path")
	tlsCA := flag.String("tls-ca", "", "trust bundle CA path")
	tlsServerName := flag.String("tls-server-name", "", "expected control-plane DNS name in server cert")
	spiffeSocket := flag.String("spiffe-socket", "", "SPIFFE workload API socket path (enables rotating SVIDs; takes precedence over --tls-cert)")
	serverSPIFFEID := flag.String("server-spiffe-id", "", "expected control-plane SPIFFE ID (e.g. spiffe://faultmesh.local/ns/faultmesh/sa/fm-control-plane)")
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

	enc := encode.New(0)

	var pollers []collect.Poller
	if p, err := proc.New(proc.Options{}); err == nil {
		pollers = append(pollers, p)
	} else {
		logger.Warn().Err(err).Msg("proc poller disabled")
	}
	if p, err := cgroup.New(cgroup.Options{}); err == nil {
		pollers = append(pollers, p)
	} else {
		logger.Warn().Err(err).Msg("cgroup poller disabled")
	}
	pollers = append(pollers, enc.SelfPoller())

	var buffer stream.AckBuffer
	var fileBuffer *stream.FileAckBuffer
	if *walDir == "" {
		mb := stream.NewMemoryAckBuffer(0)
		buffer = mb
		pollers = append(pollers, mb.SelfPoller())
	} else {
		fb, err := stream.NewFileAckBuffer(stream.FileOptions{Dir: *walDir}, logger)
		if err != nil {
			logger.Error().Err(err).Str("dir", *walDir).Msg("WAL init failed")
			os.Exit(1)
		}
		fileBuffer = fb
		buffer = fb
		pollers = append(pollers, fb.SelfPoller())
	}

	ctx, stop := signals.Context()
	defer stop()

	var dialOpts []grpc.DialOption
	var credsCloser func() error
	switch {
	case *spiffeSocket != "" && *serverSPIFFEID != "":
		creds, closer, err := grpcdial.WorkloadAPICreds(ctx, *spiffeSocket, *serverSPIFFEID)
		if err != nil {
			logger.Error().Err(err).Msg("SPIFFE workload API credential load failed")
			os.Exit(1)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
		credsCloser = closer
		logger.Info().Str("server_spiffe_id", *serverSPIFFEID).Msg("dialing control-plane via SPIFFE workload API")
	case *tlsCert != "":
		creds, err := grpcdial.LoadFileCreds(*tlsCert, *tlsKey, *tlsCA, *tlsServerName)
		if err != nil {
			logger.Error().Err(err).Msg("mTLS credential load failed")
			os.Exit(1)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
		logger.Info().Str("server_name", *tlsServerName).Msg("dialing control-plane with mTLS (file mode)")
	}

	s, err := stream.New(stream.Options{
		ControlAddr: *controlAddr,
		Encoder:     enc,
		Buffer:      buffer,
		Logger:      logger,
		DialOptions: dialOpts,
	})
	if err != nil {
		logger.Error().Err(err).Msg("stream init failed")
		os.Exit(1)
	}

	driver := &collect.Driver{Pollers: pollers, Logger: logger}

	logger.Info().
		Str("addr", *controlAddr).
		Str("wal_dir", *walDir).
		Int("pollers", len(pollers)).
		Msg("starting collect + stream loops")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := driver.Run(ctx, enc); err != nil && err != context.Canceled {
			logger.Warn().Err(err).Msg("driver loop exited")
		}
	}()
	go func() {
		defer wg.Done()
		if err := s.Run(ctx); err != nil && err != context.Canceled {
			logger.Warn().Err(err).Msg("stream loop exited")
		}
	}()

	<-ctx.Done()
	wg.Wait()

	if fileBuffer != nil {
		if err := fileBuffer.Close(); err != nil {
			logger.Warn().Err(err).Msg("WAL close error")
		}
	}
	if credsCloser != nil {
		if err := credsCloser(); err != nil {
			logger.Warn().Err(err).Msg("SPIFFE source close error")
		}
	}

	logger.Info().Msg("shutdown complete")
}
