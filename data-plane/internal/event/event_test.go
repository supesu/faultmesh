package event

import (
	"testing"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
)

func TestFromSampleAndToProto_RoundTrip(t *testing.T) {
	ts := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	s := collect.Sample{
		Name:      "proc.meminfo.mem_total",
		Value:     1_073_741_824,
		Labels:    map[string]string{"host": "node-a"},
		Timestamp: ts,
	}

	ev := FromSample("proc", s)
	if ev.Kind != KindMetric {
		t.Fatalf("Kind = %v, want KindMetric", ev.Kind)
	}
	if ev.Source != "proc" {
		t.Errorf("Source = %q, want %q", ev.Source, "proc")
	}
	if !ev.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", ev.Timestamp, ts)
	}
	if ev.Metric.Name != s.Name || ev.Metric.Value != s.Value {
		t.Errorf("Metric = %+v, want name/value to match sample", ev.Metric)
	}

	pe := ev.ToProto()
	if pe == nil {
		t.Fatal("ToProto returned nil for KindMetric")
	}
	if got := pe.GetSource(); got != "proc" {
		t.Errorf("proto Source = %q, want %q", got, "proc")
	}
	if got := pe.GetTimestamp().AsTime(); !got.Equal(ts) {
		t.Errorf("proto Timestamp = %v, want %v", got, ts)
	}
	mp := pe.GetMetric()
	if mp == nil {
		t.Fatal("proto Metric is nil")
	}
	if mp.GetName() != s.Name {
		t.Errorf("proto Metric.Name = %q, want %q", mp.GetName(), s.Name)
	}
	if mp.GetValue() != s.Value {
		t.Errorf("proto Metric.Value = %v, want %v", mp.GetValue(), s.Value)
	}
	if v, ok := mp.GetLabels()["host"]; !ok || v != "node-a" {
		t.Errorf("proto Metric.Labels[host] = %q (present=%v), want %q", v, ok, "node-a")
	}
}

func TestToProto_UnknownKindReturnsNil(t *testing.T) {
	ev := Event{Kind: KindUnknown, Source: "x", Timestamp: time.Now()}
	if got := ev.ToProto(); got != nil {
		t.Errorf("ToProto for KindUnknown = %v, want nil", got)
	}
}

func TestFromSample_PreservesNilLabels(t *testing.T) {
	s := collect.Sample{Name: "proc.loadavg.1m", Value: 0.5, Timestamp: time.Now()}
	ev := FromSample("proc", s)
	if ev.Metric.Labels != nil {
		t.Errorf("Metric.Labels = %v, want nil", ev.Metric.Labels)
	}
	pe := ev.ToProto()
	if pe.GetMetric().GetLabels() != nil && len(pe.GetMetric().GetLabels()) != 0 {
		t.Errorf("proto Metric.Labels = %v, want empty/nil", pe.GetMetric().GetLabels())
	}
}
