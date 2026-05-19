package stream

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newFileBuffer(t *testing.T, opts FileOptions) *FileAckBuffer {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 20 * time.Millisecond
	}
	b, err := NewFileAckBuffer(opts, zerolog.New(io.Discard))
	if err != nil {
		t.Fatalf("NewFileAckBuffer: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestFileAckBuffer_CrashAndRestart(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.New(io.Discard)

	b1, err := NewFileAckBuffer(FileOptions{Dir: dir}, logger)
	if err != nil {
		t.Fatalf("Phase 1 open: %v", err)
	}
	for i := 1; i <= 10; i++ {
		b1.Append(mkEvent(fmt.Sprintf("e%d", i)))
	}
	b1.Truncate(3)
	if err := b1.Close(); err != nil {
		t.Fatalf("Phase 1 Close: %v", err)
	}

	b2, err := NewFileAckBuffer(FileOptions{Dir: dir}, logger)
	if err != nil {
		t.Fatalf("Phase 2 open: %v", err)
	}
	defer b2.Close()

	frames := b2.Replay()
	if len(frames) != 7 {
		t.Fatalf("Replay = %d frames, want 7 (offsets 4..10)", len(frames))
	}
	if frames[0].GetOffset() != 4 {
		t.Errorf("first replayed offset = %d, want 4", frames[0].GetOffset())
	}
	if frames[6].GetOffset() != 10 {
		t.Errorf("last replayed offset = %d, want 10", frames[6].GetOffset())
	}
	for i, f := range frames {
		mp := f.GetEvent().GetMetric()
		if mp == nil {
			t.Errorf("frame %d has no metric payload", i)
			continue
		}
		wantName := fmt.Sprintf("e%d", int(f.GetOffset()))
		if mp.GetName() != wantName {
			t.Errorf("frame %d metric.Name = %q, want %q", i, mp.GetName(), wantName)
		}
	}

	newFrame := b2.Append(mkEvent("e11"))
	if newFrame.GetOffset() != 11 {
		t.Errorf("post-restart Append offset = %d, want 11", newFrame.GetOffset())
	}
}

func TestFileAckBuffer_AppendIsDurableWithoutClose(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.New(io.Discard)

	b1, err := NewFileAckBuffer(FileOptions{Dir: dir, SyncInterval: 10 * time.Millisecond}, logger)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 1; i <= 5; i++ {
		b1.Append(mkEvent(fmt.Sprintf("e%d", i)))
	}
	time.Sleep(50 * time.Millisecond)

	_ = b1.log.Close()
	b1.cancel()
	<-b1.done

	b2, err := NewFileAckBuffer(FileOptions{Dir: dir}, logger)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	frames := b2.Replay()
	if len(frames) != 5 {
		t.Fatalf("Replay after abrupt-close = %d frames, want 5", len(frames))
	}
}

func TestFileAckBuffer_TruncateBeyondLastEmptiesLog(t *testing.T) {
	b := newFileBuffer(t, FileOptions{})
	for i := 1; i <= 5; i++ {
		b.Append(mkEvent("e"))
	}
	b.Truncate(100)
	if got := b.Len(); got != 0 {
		t.Errorf("Len after truncate-all = %d, want 0", got)
	}
	f := b.Append(mkEvent("e6"))
	if f.GetOffset() != 6 {
		t.Errorf("post-empty Append offset = %d, want 6", f.GetOffset())
	}
}

func TestFileAckBuffer_TruncateBeforeFirstIsNoop(t *testing.T) {
	b := newFileBuffer(t, FileOptions{})
	for i := 1; i <= 5; i++ {
		b.Append(mkEvent("e"))
	}
	b.Truncate(0)
	if got := b.Len(); got != 5 {
		t.Errorf("Len after Truncate(0) = %d, want 5", got)
	}
}

func TestFileAckBuffer_RetentionDropsOldestSegmentsBySizeCap(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.New(io.Discard)
	b, err := NewFileAckBuffer(FileOptions{
		Dir:          dir,
		SegmentSize:  1024,
		MaxBytes:     2048,
		MaxAge:       time.Hour,
		SyncInterval: 20 * time.Millisecond,
	}, logger)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer b.Close()

	for i := 0; i < 2000; i++ {
		b.Append(mkEvent(fmt.Sprintf("a-fairly-long-metric-name-%d", i)))
	}
	deadline := time.After(2 * time.Second)
	for {
		if b.DroppedRetention() > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("DroppedRetention stayed 0 — retention never ran (Len=%d)", b.Len())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if got := b.Len(); got == 0 {
		t.Errorf("Len = 0 after retention; should still hold the tail segment")
	}
}

func TestFileAckBuffer_SelfPollerEmitsRetentionDrops(t *testing.T) {
	b := newFileBuffer(t, FileOptions{})
	b.droppedRetention.Store(7)

	sp := b.SelfPoller()
	if sp.Name() != "wal" {
		t.Errorf("SelfPoller.Name = %q, want %q", sp.Name(), "wal")
	}
	samples, err := sp.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("Poll returned %d samples, want 1", len(samples))
	}
	if samples[0].Name != "wal.retention.dropped" {
		t.Errorf("Sample.Name = %q, want %q", samples[0].Name, "wal.retention.dropped")
	}
	if samples[0].Value != 7 {
		t.Errorf("Sample.Value = %v, want 7", samples[0].Value)
	}
}

func TestNewFileAckBuffer_MissingDirIsError(t *testing.T) {
	if _, err := NewFileAckBuffer(FileOptions{Dir: ""}, zerolog.New(io.Discard)); err == nil {
		t.Fatal("expected error for empty Dir, got nil")
	}
}

func TestFileAckBuffer_LenMatchesReplay(t *testing.T) {
	b := newFileBuffer(t, FileOptions{})
	for i := 1; i <= 10; i++ {
		b.Append(mkEvent("e"))
	}
	b.Truncate(4)
	if got := b.Len(); got != 6 {
		t.Errorf("Len = %d, want 6", got)
	}
	if got := len(b.Replay()); got != 6 {
		t.Errorf("Replay len = %d, want 6", got)
	}
}
