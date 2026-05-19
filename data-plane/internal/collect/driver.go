package collect

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

const DefaultInterval = 5 * time.Second

type Sink interface {
	Push(source string, samples []Sample) (accepted, dropped int)
}

type Driver struct {
	Interval time.Duration
	Pollers  []Poller
	Logger   zerolog.Logger
}

func (d *Driver) Run(ctx context.Context, sink Sink) error {
	if len(d.Pollers) == 0 {
		return fmt.Errorf("collect: Driver has no Pollers")
	}
	if sink == nil {
		return fmt.Errorf("collect: Driver requires a non-nil Sink")
	}
	interval := d.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			d.tick(ctx, sink)
		}
	}
}

func (d *Driver) tick(ctx context.Context, sink Sink) {
	for _, p := range d.Pollers {
		samples, err := p.Poll(ctx)
		if err != nil {
			d.Logger.Warn().
				Err(err).
				Str("poller", p.Name()).
				Msg("poller failed; skipping samples for this tick")
			continue
		}
		if len(samples) == 0 {
			continue
		}
		_, dropped := sink.Push(p.Name(), samples)
		if dropped > 0 {
			d.Logger.Warn().
				Str("poller", p.Name()).
				Int("dropped", dropped).
				Msg("sink dropped samples")
		}
	}
}
