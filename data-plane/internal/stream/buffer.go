package stream

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

const DefaultBufferCapacity = 64 * 1024

type AckBuffer interface {
	Append(ev *pb.Event) *pb.StreamRequest

	Replay() []*pb.StreamRequest

	Truncate(throughOffset uint64)

	Len() int
}

type MemoryAckBuffer struct {
	mu            sync.Mutex
	frames        []*pb.StreamRequest
	nextOffset    uint64
	capacity      int
	droppedOldest atomic.Uint64
}

func NewMemoryAckBuffer(capacity int) *MemoryAckBuffer {
	if capacity <= 0 {
		capacity = DefaultBufferCapacity
	}
	return &MemoryAckBuffer{
		capacity:   capacity,
		nextOffset: 1,
	}
}

func (b *MemoryAckBuffer) Append(ev *pb.Event) *pb.StreamRequest {
	b.mu.Lock()
	defer b.mu.Unlock()

	offset := b.nextOffset
	b.nextOffset++
	frame := &pb.StreamRequest{
		Offset:  offset,
		Payload: &pb.StreamRequest_Event{Event: ev},
	}

	if len(b.frames) >= b.capacity {
		b.frames = b.frames[1:]
		b.droppedOldest.Add(1)
	}
	b.frames = append(b.frames, frame)
	return frame
}

func (b *MemoryAckBuffer) Replay() []*pb.StreamRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.frames) == 0 {
		return nil
	}
	out := make([]*pb.StreamRequest, len(b.frames))
	copy(out, b.frames)
	return out
}

func (b *MemoryAckBuffer) Truncate(throughOffset uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.frames) == 0 {
		return
	}
	idx := sort.Search(len(b.frames), func(i int) bool {
		return b.frames[i].GetOffset() > throughOffset
	})
	if idx == 0 {
		return
	}
	remaining := len(b.frames) - idx
	if remaining == 0 {
		b.frames = b.frames[:0]
		return
	}
	next := make([]*pb.StreamRequest, remaining)
	copy(next, b.frames[idx:])
	b.frames = next
}

func (b *MemoryAckBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.frames)
}

func (b *MemoryAckBuffer) DroppedOldest() uint64 {
	return b.droppedOldest.Load()
}

func (b *MemoryAckBuffer) SelfPoller() collect.Poller {
	return &ackBufferSelfPoller{b: b}
}

type ackBufferSelfPoller struct{ b *MemoryAckBuffer }

func (sp *ackBufferSelfPoller) Name() string { return "stream.acks" }

func (sp *ackBufferSelfPoller) Poll(_ context.Context) ([]collect.Sample, error) {
	return []collect.Sample{{
		Name:      "stream.acks.dropped",
		Value:     float64(sp.b.DroppedOldest()),
		Timestamp: time.Now(),
	}}, nil
}
