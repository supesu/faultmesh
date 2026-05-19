package stream

import (
	"testing"
	"time"
)

func TestBackoff_FirstCallInBaseRange(t *testing.T) {
	b := &Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for i := 0; i < 20; i++ {
		b.Reset()
		got := b.Next()
		if got < 0 || got >= 100*time.Millisecond {
			t.Errorf("Next() = %v, want in [0, 100ms)", got)
		}
	}
}

func TestBackoff_DoublesUpToMax(t *testing.T) {
	b := &Backoff{Base: 10 * time.Millisecond, Max: 80 * time.Millisecond}
	caps := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		80 * time.Millisecond,
		80 * time.Millisecond,
	}
	for i, c := range caps {
		got := b.Next()
		if got < 0 || got >= c {
			t.Errorf("call %d: Next() = %v, want in [0, %v)", i, got, c)
		}
	}
}

func TestBackoff_ResetReturnsToBase(t *testing.T) {
	b := &Backoff{Base: 10 * time.Millisecond, Max: 1 * time.Second}
	for i := 0; i < 10; i++ {
		_ = b.Next()
	}
	b.Reset()
	got := b.Next()
	if got >= 10*time.Millisecond {
		t.Errorf("after Reset, Next() = %v, want in [0, 10ms)", got)
	}
}

func TestBackoff_DefaultsApplied(t *testing.T) {
	b := &Backoff{}
	got := b.Next()
	if got >= defaultBackoffBase {
		t.Errorf("zero-config Next() = %v, want in [0, %v)", got, defaultBackoffBase)
	}
}
