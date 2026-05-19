package stream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/wal"
	"google.golang.org/protobuf/proto"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

const (
	defaultSegmentSize  = 4 * 1024 * 1024
	defaultMaxBytes     = 256 * 1024 * 1024
	defaultMaxAge       = 24 * time.Hour
	defaultSyncInterval = 1 * time.Second
)

type FileOptions struct {
	Dir          string
	SegmentSize  int
	MaxBytes     int64
	MaxAge       time.Duration
	SyncInterval time.Duration
}

func (o FileOptions) withDefaults() FileOptions {
	if o.SegmentSize <= 0 {
		o.SegmentSize = defaultSegmentSize
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = defaultMaxBytes
	}
	if o.MaxAge <= 0 {
		o.MaxAge = defaultMaxAge
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = defaultSyncInterval
	}
	return o
}

type FileAckBuffer struct {
	log    *wal.Log
	opts   FileOptions
	logger zerolog.Logger

	mu         sync.Mutex
	nextOffset uint64

	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	droppedRetention atomic.Uint64
}

func NewFileAckBuffer(opts FileOptions, logger zerolog.Logger) (*FileAckBuffer, error) {
	if opts.Dir == "" {
		return nil, errors.New("stream: FileOptions.Dir is required")
	}
	opts = opts.withDefaults()

	if err := os.MkdirAll(opts.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("stream: mkdir %s: %w", opts.Dir, err)
	}

	log, err := wal.Open(opts.Dir, &wal.Options{
		NoSync:           true,
		SegmentSize:      opts.SegmentSize,
		LogFormat:        wal.Binary,
		SegmentCacheSize: 2,
		AllowEmpty:       true,
		DirPerms:         0o750,
		FilePerms:        0o640,
	})
	if err != nil {
		return nil, fmt.Errorf("stream: open wal %s: %w", opts.Dir, err)
	}

	last, err := log.LastIndex()
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("stream: read wal last index: %w", err)
	}
	first, _ := log.FirstIndex()
	logger.Info().
		Str("dir", opts.Dir).
		Uint64("first_index", first).
		Uint64("last_index", last).
		Msg("WAL opened")

	ctx, cancel := context.WithCancel(context.Background())
	b := &FileAckBuffer{
		log:        log,
		opts:       opts,
		logger:     logger,
		nextOffset: last + 1,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	go b.retention()
	return b, nil
}

func (b *FileAckBuffer) Append(ev *pb.Event) *pb.StreamRequest {
	bytes, err := proto.Marshal(ev)
	if err != nil {
		b.logger.Warn().Err(err).Msg("WAL marshal failed; skipping persistence")
		return &pb.StreamRequest{
			Offset:  b.bumpOffset(),
			Payload: &pb.StreamRequest_Event{Event: ev},
		}
	}

	b.mu.Lock()
	offset := b.nextOffset
	werr := b.log.Write(offset, bytes)
	if werr == nil {
		b.nextOffset++
	}
	b.mu.Unlock()

	if werr != nil {
		b.logger.Warn().
			Err(werr).
			Uint64("offset", offset).
			Msg("WAL write failed; in-memory frame still produced")
	}
	return &pb.StreamRequest{
		Offset:  offset,
		Payload: &pb.StreamRequest_Event{Event: ev},
	}
}

func (b *FileAckBuffer) bumpOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	offset := b.nextOffset
	b.nextOffset++
	return offset
}

func (b *FileAckBuffer) Replay() []*pb.StreamRequest {
	first, err := b.log.FirstIndex()
	if err != nil || first == 0 {
		return nil
	}
	last, err := b.log.LastIndex()
	if err != nil || last < first {
		return nil
	}
	out := make([]*pb.StreamRequest, 0, last-first+1)
	for i := first; i <= last; i++ {
		data, err := b.log.Read(i)
		if err != nil {
			b.logger.Warn().Err(err).Uint64("index", i).Msg("WAL Read failed during Replay")
			continue
		}
		var ev pb.Event
		if err := proto.Unmarshal(data, &ev); err != nil {
			b.logger.Warn().Err(err).Uint64("index", i).Msg("WAL unmarshal failed during Replay")
			continue
		}
		out = append(out, &pb.StreamRequest{
			Offset:  i,
			Payload: &pb.StreamRequest_Event{Event: &ev},
		})
	}
	return out
}

func (b *FileAckBuffer) Truncate(throughOffset uint64) {
	first, err := b.log.FirstIndex()
	if err != nil || first == 0 {
		return
	}
	last, err := b.log.LastIndex()
	if err != nil || last < first {
		return
	}
	if throughOffset < first {
		return
	}
	target := throughOffset + 1
	if target > last+1 {
		target = last + 1
	}
	if target == first {
		return
	}
	if err := b.log.TruncateFront(target); err != nil {
		b.logger.Warn().Err(err).Uint64("target", target).Msg("WAL TruncateFront failed")
	}
}

func (b *FileAckBuffer) Len() int {
	first, err := b.log.FirstIndex()
	if err != nil || first == 0 {
		return 0
	}
	last, err := b.log.LastIndex()
	if err != nil || last < first {
		return 0
	}
	return int(last - first + 1)
}

func (b *FileAckBuffer) Close() error {
	b.cancel()
	<-b.done
	_ = b.log.Sync()
	err := b.log.Close()
	if err != nil && errors.Is(err, wal.ErrClosed) {
		return nil
	}
	return err
}

func (b *FileAckBuffer) DroppedRetention() uint64 {
	return b.droppedRetention.Load()
}

func (b *FileAckBuffer) SelfPoller() collect.Poller {
	return &fileBufferSelfPoller{b: b}
}

type fileBufferSelfPoller struct{ b *FileAckBuffer }

func (sp *fileBufferSelfPoller) Name() string { return "wal" }

func (sp *fileBufferSelfPoller) Poll(_ context.Context) ([]collect.Sample, error) {
	return []collect.Sample{{
		Name:      "wal.retention.dropped",
		Value:     float64(sp.b.DroppedRetention()),
		Timestamp: time.Now(),
	}}, nil
}

func (b *FileAckBuffer) retention() {
	defer close(b.done)
	ticker := time.NewTicker(b.opts.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if err := b.log.Sync(); err != nil && !errors.Is(err, wal.ErrClosed) {
				b.logger.Warn().Err(err).Msg("WAL Sync failed")
			}
			b.enforceCaps()
		}
	}
}

func (b *FileAckBuffer) enforceCaps() {
	type seg struct {
		firstIdx uint64
		size     int64
		mtime    time.Time
	}
	entries, err := os.ReadDir(b.opts.Dir)
	if err != nil {
		return
	}
	var segs []seg
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) != 20 {
			continue
		}
		idx, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		segs = append(segs, seg{firstIdx: idx, size: info.Size(), mtime: info.ModTime()})
	}
	if len(segs) <= 1 {
		return
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].firstIdx < segs[j].firstIdx })

	var total int64
	for _, s := range segs {
		total += s.size
	}

	cutoff := uint64(0)
	updateCutoff := func(c uint64) {
		if c > cutoff {
			cutoff = c
		}
	}

	if b.opts.MaxBytes > 0 && total > b.opts.MaxBytes {
		running := total
		for i := 0; i < len(segs)-1; i++ {
			if running <= b.opts.MaxBytes {
				break
			}
			running -= segs[i].size
			updateCutoff(segs[i+1].firstIdx)
		}
	}

	if b.opts.MaxAge > 0 {
		threshold := time.Now().Add(-b.opts.MaxAge)
		for i := 0; i < len(segs)-1; i++ {
			if !segs[i].mtime.Before(threshold) {
				break
			}
			updateCutoff(segs[i+1].firstIdx)
		}
	}

	if cutoff == 0 {
		return
	}
	first, err := b.log.FirstIndex()
	if err != nil || first == 0 || cutoff <= first {
		return
	}
	dropped := cutoff - first
	if err := b.log.TruncateFront(cutoff); err != nil {
		b.logger.Warn().Err(err).Uint64("cutoff", cutoff).Msg("WAL retention TruncateFront failed")
		return
	}
	b.droppedRetention.Add(dropped)
	b.logger.Info().
		Uint64("dropped", dropped).
		Uint64("new_first_index", cutoff).
		Msg("WAL retention dropped oldest entries")
}
