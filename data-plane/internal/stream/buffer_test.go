package stream

import (
	"context"
	"testing"

	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

func mkEvent(name string) *pb.Event {
	return &pb.Event{
		Source: "test",
		Payload: &pb.Event_Metric{
			Metric: &pb.MetricPoint{Name: name},
		},
	}
}

func TestMemoryAckBuffer_AppendAssignsMonotonicOffsets(t *testing.T) {
	b := NewMemoryAckBuffer(16)
	f1 := b.Append(mkEvent("e1"))
	f2 := b.Append(mkEvent("e2"))
	f3 := b.Append(mkEvent("e3"))

	if f1.GetOffset() != 1 || f2.GetOffset() != 2 || f3.GetOffset() != 3 {
		t.Fatalf("offsets = %d, %d, %d, want 1, 2, 3",
			f1.GetOffset(), f2.GetOffset(), f3.GetOffset())
	}
	if got := b.Len(); got != 3 {
		t.Errorf("Len = %d, want 3", got)
	}
}

func TestMemoryAckBuffer_ReplayIsAscending(t *testing.T) {
	b := NewMemoryAckBuffer(16)
	for i := 0; i < 5; i++ {
		b.Append(mkEvent("e"))
	}
	frames := b.Replay()
	if len(frames) != 5 {
		t.Fatalf("Replay returned %d frames, want 5", len(frames))
	}
	for i, f := range frames {
		if f.GetOffset() != uint64(i+1) {
			t.Errorf("frame %d offset = %d, want %d", i, f.GetOffset(), i+1)
		}
	}
}

func TestMemoryAckBuffer_TruncateRemovesAckedFrames(t *testing.T) {
	b := NewMemoryAckBuffer(16)
	for i := 0; i < 10; i++ {
		b.Append(mkEvent("e"))
	}
	b.Truncate(4)

	frames := b.Replay()
	if len(frames) != 6 {
		t.Fatalf("after Truncate(4): Len = %d, want 6", len(frames))
	}
	if frames[0].GetOffset() != 5 {
		t.Errorf("first remaining offset = %d, want 5", frames[0].GetOffset())
	}
	if frames[len(frames)-1].GetOffset() != 10 {
		t.Errorf("last remaining offset = %d, want 10", frames[len(frames)-1].GetOffset())
	}
}

func TestMemoryAckBuffer_TruncateBeyondEverything(t *testing.T) {
	b := NewMemoryAckBuffer(16)
	for i := 0; i < 5; i++ {
		b.Append(mkEvent("e"))
	}
	b.Truncate(100)
	if got := b.Len(); got != 0 {
		t.Errorf("Len after Truncate(100) = %d, want 0", got)
	}
}

func TestMemoryAckBuffer_TruncateBeforeAnythingIsNoop(t *testing.T) {
	b := NewMemoryAckBuffer(16)
	for i := 0; i < 5; i++ {
		b.Append(mkEvent("e"))
	}
	b.Truncate(0)
	if got := b.Len(); got != 5 {
		t.Errorf("Len after Truncate(0) = %d, want 5", got)
	}
}

func TestMemoryAckBuffer_OverflowDropsOldest(t *testing.T) {
	const cap = 4
	b := NewMemoryAckBuffer(cap)
	for i := 0; i < 7; i++ {
		b.Append(mkEvent("e"))
	}
	if got := b.Len(); got != cap {
		t.Errorf("Len = %d, want %d", got, cap)
	}
	if got := b.DroppedOldest(); got != 3 {
		t.Errorf("DroppedOldest = %d, want 3", got)
	}
	frames := b.Replay()
	if frames[0].GetOffset() != 4 {
		t.Errorf("first remaining offset = %d, want 4 (oldest dropped)", frames[0].GetOffset())
	}
	if frames[len(frames)-1].GetOffset() != 7 {
		t.Errorf("last remaining offset = %d, want 7", frames[len(frames)-1].GetOffset())
	}
}

func TestMemoryAckBuffer_DefaultCapacity(t *testing.T) {
	b := NewMemoryAckBuffer(0)
	if got := b.capacity; got != DefaultBufferCapacity {
		t.Errorf("default capacity = %d, want %d", got, DefaultBufferCapacity)
	}
	b = NewMemoryAckBuffer(-5)
	if got := b.capacity; got != DefaultBufferCapacity {
		t.Errorf("negative capacity → %d, want %d", got, DefaultBufferCapacity)
	}
}

func TestMemoryAckBuffer_SelfPollerEmitsDropCount(t *testing.T) {
	b := NewMemoryAckBuffer(2)
	for i := 0; i < 5; i++ {
		b.Append(mkEvent("e"))
	}
	sp := b.SelfPoller()
	if got := sp.Name(); got != "stream.acks" {
		t.Errorf("SelfPoller.Name = %q, want %q", got, "stream.acks")
	}
	samples, err := sp.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("Poll returned %d samples, want 1", len(samples))
	}
	if samples[0].Name != "stream.acks.dropped" {
		t.Errorf("Sample.Name = %q, want %q", samples[0].Name, "stream.acks.dropped")
	}
	if samples[0].Value != float64(b.DroppedOldest()) {
		t.Errorf("Sample.Value = %v, want %v", samples[0].Value, b.DroppedOldest())
	}
	if samples[0].Value == 0 {
		t.Errorf("expected non-zero drops, got 0")
	}
}
