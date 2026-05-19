package stream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
	"github.com/supesu/faultmesh/data-plane/internal/encode"
	"github.com/supesu/faultmesh/data-plane/internal/stream"
	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

type testServer struct {
	pb.UnimplementedAgentControlServiceServer

	mu            sync.Mutex
	phase         int
	abortAfter    int
	ackAfter      int
	recvByOffset  map[uint64]int
	sessionsCount int
	aborted       chan struct{}
}

func newTestServer(abortAfter, ackAfter int) *testServer {
	return &testServer{
		phase:        1,
		abortAfter:   abortAfter,
		ackAfter:     ackAfter,
		recvByOffset: make(map[uint64]int),
		aborted:      make(chan struct{}),
	}
}

func (ts *testServer) flipToPhase2() {
	ts.mu.Lock()
	ts.phase = 2
	ts.mu.Unlock()
}

func (ts *testServer) Stream(srv pb.AgentControlService_StreamServer) error {
	ts.mu.Lock()
	ts.sessionsCount++
	phase := ts.phase
	ts.mu.Unlock()

	received := 0
	for {
		req, err := srv.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		offset := req.GetOffset()
		ts.mu.Lock()
		ts.recvByOffset[offset]++
		ts.mu.Unlock()
		received++

		if phase == 1 {
			if received <= ts.ackAfter {
				if err := srv.Send(&pb.StreamResponse{
					Payload: &pb.StreamResponse_IngestAck{
						IngestAck: &pb.IngestAck{LastAckedOffset: offset},
					},
				}); err != nil {
					return err
				}
			}
			if received >= ts.abortAfter {
				close(ts.aborted)
				return fmt.Errorf("simulated mid-stream abort")
			}
			continue
		}

		if err := srv.Send(&pb.StreamResponse{
			Payload: &pb.StreamResponse_IngestAck{
				IngestAck: &pb.IngestAck{LastAckedOffset: offset},
			},
		}); err != nil {
			return err
		}
	}
}

func (ts *testServer) uniqueOffsetsCount() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.recvByOffset)
}

func (ts *testServer) sessions() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.sessionsCount
}

func startTestServer(t *testing.T, ts *testServer) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterAgentControlServiceServer(srv, ts)
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func mkSamples(n int) []collect.Sample {
	out := make([]collect.Sample, n)
	now := time.Now()
	for i := range out {
		out[i] = collect.Sample{
			Name:      "test.metric",
			Value:     float64(i),
			Timestamp: now,
		}
	}
	return out
}

func TestStream_NoLossOnMidStreamAbort(t *testing.T) {
	const total = 20
	ts := newTestServer(5, 3)
	addr, stop := startTestServer(t, ts)
	defer stop()

	enc := encode.New(64)
	s, err := stream.New(stream.Options{
		ControlAddr:   addr,
		Encoder:       enc,
		Logger:        zerolog.New(io.Discard),
		DialOptions:   []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		DrainInterval: 5 * time.Millisecond,
		MaxBatch:      8,
	})
	if err != nil {
		t.Fatalf("stream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- s.Run(ctx) }()

	enc.Push("test", mkSamples(total))

	select {
	case <-ts.aborted:
	case <-time.After(2 * time.Second):
		t.Fatal("server never aborted within 2s")
	}

	ts.flipToPhase2()

	deadline := time.After(3 * time.Second)
	for {
		if ts.uniqueOffsetsCount() >= total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d unique offsets received after reconnect; want >= %d", ts.uniqueOffsetsCount(), total)
		case <-time.After(10 * time.Millisecond):
		}
	}

	if got := ts.sessions(); got < 2 {
		t.Errorf("sessions = %d, want >= 2 (abort + reconnect)", got)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled or nil", err)
		}
	case <-time.After(time.Second):
		t.Error("Run did not exit within 1s of context cancel")
	}
}

func TestStream_ContextCancelExitsCleanly(t *testing.T) {
	ts := newTestServer(1000, 1000)
	addr, stop := startTestServer(t, ts)
	defer stop()

	enc := encode.New(64)
	s, _ := stream.New(stream.Options{
		ControlAddr:   addr,
		Encoder:       enc,
		Logger:        zerolog.New(io.Discard),
		DialOptions:   []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		DrainInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled or nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not exit within 1s of cancel")
	}
}

func TestStream_NewValidation(t *testing.T) {
	enc := encode.New(0)
	cases := []struct {
		name string
		opts stream.Options
	}{
		{"missing addr", stream.Options{Encoder: enc}},
		{"missing encoder", stream.Options{ControlAddr: "x:1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := stream.New(c.opts); err == nil {
				t.Errorf("New(%+v) = nil error, want error", c.opts)
			}
		})
	}
}

func TestStream_FileBuffer_CrashAndReplayToServer(t *testing.T) {
	const total = 20
	dir := t.TempDir()
	ts := newTestServer(5, 3)
	addr, stopServer := startTestServer(t, ts)
	defer stopServer()

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	logger := zerolog.New(io.Discard)

	fb1, err := stream.NewFileAckBuffer(stream.FileOptions{Dir: dir, SyncInterval: 20 * time.Millisecond}, logger)
	if err != nil {
		t.Fatalf("fb1 open: %v", err)
	}
	for i := 1; i <= total; i++ {
		fb1.Append(&pb.Event{
			Source:  "test",
			Payload: &pb.Event_Metric{Metric: &pb.MetricPoint{Name: fmt.Sprintf("e%d", i)}},
		})
	}
	if got := fb1.Len(); got != total {
		t.Fatalf("after pre-population fb1.Len = %d, want %d", got, total)
	}

	enc1 := encode.New(64)
	s1, err := stream.New(stream.Options{
		ControlAddr:   addr,
		Encoder:       enc1,
		Buffer:        fb1,
		Logger:        logger,
		DialOptions:   dialOpts,
		DrainInterval: 5 * time.Millisecond,
		MaxBatch:      8,
	})
	if err != nil {
		t.Fatalf("stream.New #1: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Done := make(chan struct{})
	go func() { _ = s1.Run(ctx1); close(run1Done) }()

	select {
	case <-ts.aborted:
	case <-time.After(2 * time.Second):
		t.Fatal("server never aborted within 2s")
	}

	cancel1()
	<-run1Done
	if err := fb1.Close(); err != nil {
		t.Fatalf("fb1.Close: %v", err)
	}

	ts.flipToPhase2()

	fb2, err := stream.NewFileAckBuffer(stream.FileOptions{Dir: dir, SyncInterval: 20 * time.Millisecond}, logger)
	if err != nil {
		t.Fatalf("fb2 open: %v", err)
	}
	defer fb2.Close()

	if got := fb2.Len(); got == 0 {
		t.Fatal("Phase 2 WAL is empty; durability broken")
	}

	enc2 := encode.New(64)
	s2, err := stream.New(stream.Options{
		ControlAddr:   addr,
		Encoder:       enc2,
		Buffer:        fb2,
		Logger:        logger,
		DialOptions:   dialOpts,
		DrainInterval: 5 * time.Millisecond,
		MaxBatch:      8,
	})
	if err != nil {
		t.Fatalf("stream.New #2: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	go func() { _ = s2.Run(ctx2) }()

	deadline := time.After(3 * time.Second)
	for {
		if ts.uniqueOffsetsCount() >= total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d unique offsets received after crash-restart; want >= %d", ts.uniqueOffsetsCount(), total)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if got := ts.sessions(); got < 2 {
		t.Errorf("sessions = %d, want >= 2 (crash + restart)", got)
	}
}
