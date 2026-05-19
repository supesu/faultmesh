package stream

import (
	"math/rand/v2"
	"time"
)

const (
	defaultBackoffBase = 100 * time.Millisecond
	defaultBackoffMax  = 30 * time.Second
)

type Backoff struct {
	Base time.Duration
	Max  time.Duration

	cur  time.Duration
	rand *rand.Rand
}

func (b *Backoff) Next() time.Duration {
	base := b.Base
	if base <= 0 {
		base = defaultBackoffBase
	}
	max := b.Max
	if max <= 0 {
		max = defaultBackoffMax
	}
	if b.cur <= 0 {
		b.cur = base
	}
	if b.cur > max {
		b.cur = max
	}

	d := b.jitter(b.cur)

	b.cur *= 2
	if b.cur > max {
		b.cur = max
	}
	return d
}

func (b *Backoff) Reset() { b.cur = 0 }

func (b *Backoff) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if b.rand != nil {
		return time.Duration(b.rand.Int64N(int64(d)))
	}
	return time.Duration(rand.Int64N(int64(d)))
}
