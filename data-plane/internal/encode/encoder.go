package encode

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
	"github.com/supesu/faultmesh/data-plane/internal/event"
	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

const DefaultCapacity = 4096

type Encoder struct {
	ring  chan *pb.Event
	drops atomic.Uint64
}

func New(capacity int) *Encoder {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Encoder{ring: make(chan *pb.Event, capacity)}
}

func (e *Encoder) Push(source string, samples []collect.Sample) (accepted, dropped int) {
	for _, s := range samples {
		pe := event.FromSample(source, s).ToProto()
		if pe == nil {
			continue
		}
		select {
		case e.ring <- pe:
			accepted++
		default:
			e.drops.Add(1)
			dropped++
		}
	}
	return accepted, dropped
}

func (e *Encoder) Drain(maxBatch int) []*pb.Event {
	if maxBatch <= 0 {
		return nil
	}
	out := make([]*pb.Event, 0, maxBatch)
	for i := 0; i < maxBatch; i++ {
		select {
		case ev := <-e.ring:
			out = append(out, ev)
		default:
			return out
		}
	}
	return out
}

func (e *Encoder) Drops() uint64 { return e.drops.Load() }

func (e *Encoder) Len() int { return len(e.ring) }

func (e *Encoder) Cap() int { return cap(e.ring) }

func (e *Encoder) SelfPoller() collect.Poller { return &selfPoller{e: e} }

type selfPoller struct{ e *Encoder }

func (sp *selfPoller) Name() string { return "encoder" }

func (sp *selfPoller) Poll(_ context.Context) ([]collect.Sample, error) {
	return []collect.Sample{{
		Name:      "encoder.drops",
		Value:     float64(sp.e.Drops()),
		Timestamp: time.Now(),
	}}, nil
}
