package collect

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type fakePoller struct {
	name    string
	samples []Sample
	err     error
}

func (f *fakePoller) Name() string                             { return f.name }
func (f *fakePoller) Poll(_ context.Context) ([]Sample, error) { return f.samples, f.err }

type sinkCall struct {
	source  string
	samples []Sample
}

type fakeSink struct {
	mu        sync.Mutex
	calls     []sinkCall
	dropEvery int
}

func (s *fakeSink) Push(source string, samples []Sample) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, sinkCall{source: source, samples: samples})
	if s.dropEvery > 0 && len(samples) >= s.dropEvery {
		return len(samples) - s.dropEvery, s.dropEvery
	}
	return len(samples), 0
}

func (s *fakeSink) snapshot() []sinkCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]sinkCall, len(s.calls))
	copy(cp, s.calls)
	return cp
}

func TestDriver_TickPushesPerPoller(t *testing.T) {
	good := &fakePoller{name: "good", samples: []Sample{{Name: "x.good", Value: 1}}}
	d := &Driver{
		Interval: 5 * time.Millisecond,
		Pollers:  []Poller{good},
		Logger:   zerolog.New(io.Discard),
	}
	sink := &fakeSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = d.Run(ctx, sink); close(done) }()

	deadline := time.After(150 * time.Millisecond)
	for {
		if calls := sink.snapshot(); len(calls) >= 1 {
			if calls[0].source != "good" || len(calls[0].samples) != 1 || calls[0].samples[0].Name != "x.good" {
				t.Fatalf("unexpected first call: %+v", calls[0])
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("no Push within 150ms")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func TestDriver_FailingPollerDoesNotStarvePeers(t *testing.T) {
	bad := &fakePoller{name: "bad", err: errors.New("boom")}
	good := &fakePoller{name: "good", samples: []Sample{{Name: "x.good", Value: 7}}}
	d := &Driver{
		Interval: 5 * time.Millisecond,
		Pollers:  []Poller{bad, good},
		Logger:   zerolog.New(io.Discard),
	}
	sink := &fakeSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _ = d.Run(ctx, sink) }()

	deadline := time.After(150 * time.Millisecond)
	for {
		calls := sink.snapshot()
		if len(calls) >= 1 {
			for _, c := range calls {
				if c.source != "good" {
					t.Fatalf("unexpected push from %q", c.source)
				}
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("no Push within 150ms")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func TestDriver_ContextCancelExits(t *testing.T) {
	d := &Driver{
		Interval: 50 * time.Millisecond,
		Pollers:  []Poller{&fakePoller{name: "n"}},
		Logger:   zerolog.New(io.Discard),
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx, &fakeSink{}) }()
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not exit within 200ms of cancel")
	}
}

func TestDriver_NoPollersIsError(t *testing.T) {
	d := &Driver{Logger: zerolog.New(io.Discard)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Run(ctx, &fakeSink{}); err == nil {
		t.Fatal("expected error for empty Pollers, got nil")
	}
}

func TestDriver_NilSinkIsError(t *testing.T) {
	d := &Driver{
		Pollers: []Poller{&fakePoller{name: "n"}},
		Logger:  zerolog.New(io.Discard),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Run(ctx, nil); err == nil {
		t.Fatal("expected error for nil Sink, got nil")
	}
}

func TestDriver_DefaultInterval(t *testing.T) {
	if DefaultInterval != 5*time.Second {
		t.Fatalf("DefaultInterval = %v, want 5s", DefaultInterval)
	}
}
