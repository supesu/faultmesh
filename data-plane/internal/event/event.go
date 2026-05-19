package event

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

type Kind int

const (
	KindUnknown Kind = iota
	KindMetric
)

type Event struct {
	Kind      Kind
	Timestamp time.Time
	Source    string
	Metric    MetricPoint
}

type MetricPoint struct {
	Name   string
	Value  float64
	Labels map[string]string
}

func FromSample(source string, s collect.Sample) Event {
	return Event{
		Kind:      KindMetric,
		Timestamp: s.Timestamp,
		Source:    source,
		Metric: MetricPoint{
			Name:   s.Name,
			Value:  s.Value,
			Labels: s.Labels,
		},
	}
}

func (e Event) ToProto() *pb.Event {
	out := &pb.Event{
		Timestamp: timestamppb.New(e.Timestamp),
		Source:    e.Source,
	}
	switch e.Kind {
	case KindMetric:
		out.Payload = &pb.Event_Metric{
			Metric: &pb.MetricPoint{
				Name:   e.Metric.Name,
				Value:  e.Metric.Value,
				Labels: e.Metric.Labels,
			},
		}
	default:
		return nil
	}
	return out
}
