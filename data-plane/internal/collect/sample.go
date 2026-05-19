package collect

import (
	"context"
	"time"
)

type Sample struct {
	Name      string
	Value     float64
	Labels    map[string]string
	Timestamp time.Time
}

type Poller interface {
	Name() string
	Poll(ctx context.Context) ([]Sample, error)
}
