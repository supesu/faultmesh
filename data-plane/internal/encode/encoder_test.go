package encode

import (
	"context"
	"testing"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
)

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

func TestEncoder_PushAndDrain(t *testing.T) {
	e := New(8)
	accepted, dropped := e.Push("proc", mkSamples(5))
	if accepted != 5 || dropped != 0 {
		t.Fatalf("Push(5): accepted=%d dropped=%d, want 5/0", accepted, dropped)
	}
	if got := e.Len(); got != 5 {
		t.Errorf("Len = %d, want 5", got)
	}

	batch := e.Drain(10)
	if len(batch) != 5 {
		t.Fatalf("Drain(10) returned %d events, want 5", len(batch))
	}
	if got := e.Len(); got != 0 {
		t.Errorf("Len after Drain = %d, want 0", got)
	}
	for i, ev := range batch {
		if ev.GetSource() != "proc" {
			t.Errorf("event[%d].Source = %q, want %q", i, ev.GetSource(), "proc")
		}
		if ev.GetMetric() == nil {
			t.Errorf("event[%d].Metric is nil", i)
		}
	}
}

func TestEncoder_DefaultCapacity(t *testing.T) {
	e := New(0)
	if got := e.Cap(); got != DefaultCapacity {
		t.Errorf("Cap = %d, want %d", got, DefaultCapacity)
	}
	e = New(-5)
	if got := e.Cap(); got != DefaultCapacity {
		t.Errorf("Cap (negative input) = %d, want %d", got, DefaultCapacity)
	}
}

func TestEncoder_DropsCountedUnderOverload(t *testing.T) {
	const cap = 4
	e := New(cap)
	accepted, dropped := e.Push("proc", mkSamples(100))
	if accepted != cap {
		t.Errorf("accepted = %d, want %d (ring capacity)", accepted, cap)
	}
	if dropped == 0 {
		t.Errorf("dropped = 0, want > 0 under overload")
	}
	if accepted+dropped != 100 {
		t.Errorf("accepted+dropped = %d, want 100", accepted+dropped)
	}
	if got := e.Drops(); got != uint64(dropped) {
		t.Errorf("Drops() = %d, want %d", got, dropped)
	}
}

func TestEncoder_DrainEmptyReturnsEmptySlice(t *testing.T) {
	e := New(8)
	if got := e.Drain(10); len(got) != 0 {
		t.Errorf("Drain on empty ring returned %d events, want 0", len(got))
	}
}

func TestEncoder_DrainNonPositiveMax(t *testing.T) {
	e := New(8)
	e.Push("proc", mkSamples(2))
	if got := e.Drain(0); got != nil {
		t.Errorf("Drain(0) = %v, want nil", got)
	}
	if got := e.Drain(-1); got != nil {
		t.Errorf("Drain(-1) = %v, want nil", got)
	}
	if got := e.Len(); got != 2 {
		t.Errorf("Len after no-op Drain = %d, want 2", got)
	}
}

func TestSelfPoller_EmitsDropCount(t *testing.T) {
	e := New(2)
	e.Push("proc", mkSamples(10))

	sp := e.SelfPoller()
	if sp.Name() != "encoder" {
		t.Errorf("SelfPoller.Name = %q, want %q", sp.Name(), "encoder")
	}
	before := time.Now()
	samples, err := sp.Poll(context.Background())
	after := time.Now()
	if err != nil {
		t.Fatalf("SelfPoller.Poll: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("SelfPoller returned %d samples, want 1", len(samples))
	}
	s := samples[0]
	if s.Name != "encoder.drops" {
		t.Errorf("Sample.Name = %q, want %q", s.Name, "encoder.drops")
	}
	if s.Value != float64(e.Drops()) {
		t.Errorf("Sample.Value = %v, want %v (encoder Drops)", s.Value, e.Drops())
	}
	if s.Timestamp.Before(before) || s.Timestamp.After(after) {
		t.Errorf("Sample.Timestamp = %v, outside [%v, %v]", s.Timestamp, before, after)
	}
}

func TestEncoder_SelfPollerFeedsBackIntoRing(t *testing.T) {
	e := New(2)
	e.Push("proc", mkSamples(10))

	sp := e.SelfPoller()
	samples, _ := sp.Poll(context.Background())
	_ = e.Drain(2)

	accepted, dropped := e.Push("encoder", samples)
	if accepted != 1 || dropped != 0 {
		t.Fatalf("self-feedback Push: accepted=%d dropped=%d, want 1/0", accepted, dropped)
	}
	batch := e.Drain(1)
	if len(batch) != 1 {
		t.Fatalf("Drain returned %d events, want 1", len(batch))
	}
	if batch[0].GetSource() != "encoder" || batch[0].GetMetric().GetName() != "encoder.drops" {
		t.Errorf("unexpected event: source=%q name=%q",
			batch[0].GetSource(), batch[0].GetMetric().GetName())
	}
	if batch[0].GetMetric().GetValue() == 0 {
		t.Errorf("encoder.drops value = 0, want > 0")
	}
}
