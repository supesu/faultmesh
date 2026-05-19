package encode

import "testing"

func BenchmarkEncoder_PushUnderOverload(b *testing.B) {
	e := New(64)
	samples := mkSamples(256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Push("bench", samples)
	}
	b.StopTimer()

	if e.Drops() == 0 {
		b.Fatalf("expected drops > 0 under overload, got %d", e.Drops())
	}
	b.ReportMetric(float64(e.Drops()), "drops")
}
