package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/supesu/faultmesh/data-plane/internal/encode"
	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

const (
	DefaultDrainInterval = 50 * time.Millisecond
	DefaultMaxBatch      = 256
)

type Options struct {
	ControlAddr   string
	Encoder       *encode.Encoder
	Buffer        AckBuffer
	Handler       ActionHandler
	Logger        zerolog.Logger
	DialOptions   []grpc.DialOption
	DrainInterval time.Duration
	MaxBatch      int
}

type Stream struct {
	addr          string
	encoder       *encode.Encoder
	buffer        AckBuffer
	handler       ActionHandler
	logger        zerolog.Logger
	dialOptions   []grpc.DialOption
	drainInterval time.Duration
	maxBatch      int
	backoff       Backoff
}

func New(opts Options) (*Stream, error) {
	if opts.ControlAddr == "" {
		return nil, errors.New("stream: ControlAddr is required")
	}
	if opts.Encoder == nil {
		return nil, errors.New("stream: Encoder is required")
	}
	s := &Stream{
		addr:          opts.ControlAddr,
		encoder:       opts.Encoder,
		buffer:        opts.Buffer,
		handler:       opts.Handler,
		logger:        opts.Logger,
		dialOptions:   opts.DialOptions,
		drainInterval: opts.DrainInterval,
		maxBatch:      opts.MaxBatch,
	}
	if s.buffer == nil {
		s.buffer = NewMemoryAckBuffer(0)
	}
	if s.handler == nil {
		s.handler = NewNoopHandler(s.logger)
	}
	if len(s.dialOptions) == 0 {
		s.dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	if s.drainInterval <= 0 {
		s.drainInterval = DefaultDrainInterval
	}
	if s.maxBatch <= 0 {
		s.maxBatch = DefaultMaxBatch
	}
	return s, nil
}

func (s *Stream) Buffer() AckBuffer { return s.buffer }

func (s *Stream) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := s.connectAndServe(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			s.logger.Warn().Err(err).Msg("stream session ended; reconnecting")
		}
		if err := s.sleepBackoff(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *Stream) connectAndServe(ctx context.Context) error {
	conn, err := grpc.NewClient(s.addr, s.dialOptions...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewAgentControlServiceClient(conn)
	streamRPC, err := client.Stream(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	for _, frame := range s.buffer.Replay() {
		if err := streamRPC.Send(frame); err != nil {
			return fmt.Errorf("replay send (offset %d): %w", frame.GetOffset(), err)
		}
	}

	return s.runSession(ctx, streamRPC)
}

func (s *Stream) runSession(ctx context.Context, streamRPC pb.AgentControlService_StreamClient) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return s.sendLoop(gCtx, streamRPC) })
	g.Go(func() error { return s.recvLoop(gCtx, streamRPC) })
	return g.Wait()
}

func (s *Stream) sendLoop(ctx context.Context, streamRPC pb.AgentControlService_StreamClient) error {
	ticker := time.NewTicker(s.drainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		events := s.encoder.Drain(s.maxBatch)
		for _, ev := range events {
			frame := s.buffer.Append(ev)
			if err := streamRPC.Send(frame); err != nil {
				return fmt.Errorf("send (offset %d): %w", frame.GetOffset(), err)
			}
		}
	}
}

func (s *Stream) recvLoop(ctx context.Context, streamRPC pb.AgentControlService_StreamClient) error {
	for {
		msg, err := streamRPC.Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		s.backoff.Reset()

		switch p := msg.GetPayload().(type) {
		case *pb.StreamResponse_IngestAck:
			s.buffer.Truncate(p.IngestAck.GetLastAckedOffset())
		case *pb.StreamResponse_Action:
			if err := s.handler.HandleAction(ctx, p.Action); err != nil {
				s.logger.Warn().
					Err(err).
					Str("action_id", p.Action.GetActionId()).
					Msg("action handler errored")
			}
		default:
			s.logger.Warn().Msg("received StreamResponse with unknown payload; skipping")
		}
	}
}

func (s *Stream) sleepBackoff(ctx context.Context) error {
	d := s.backoff.Next()
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
