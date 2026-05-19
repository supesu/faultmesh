package cgroup

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect/testutil"
)

const fixturePath = "testdata/cgroup"

func TestPoller_Poll(t *testing.T) {
	p, err := New(Options{Path: fixturePath})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	before := time.Now()
	samples, err := p.Poll(context.Background())
	after := time.Now()
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	const wantCount = 15
	if len(samples) != wantCount {
		t.Errorf("sample count = %d, want %d", len(samples), wantCount)
		for _, s := range samples {
			t.Logf("  got: %s labels=%v value=%v", s.Name, s.Labels, s.Value)
		}
	}

	for _, s := range samples {
		if s.Timestamp.Before(before) || s.Timestamp.After(after) {
			t.Errorf("sample %q timestamp %v outside [%v, %v]", s.Name, s.Timestamp, before, after)
		}
	}

	rootLbl := map[string]string{"cgroup": "/"}
	podLbl := map[string]string{"cgroup": "/kubepods.slice"}
	rootIOLbl := map[string]string{"cgroup": "/", "device": "8:0"}

	cases := []struct {
		name   string
		labels map[string]string
		value  float64
	}{
		{"cgroup.v2.memory.current", rootLbl, 1048576},
		{"cgroup.v2.pids.current", rootLbl, 42},
		{"cgroup.v2.cpu.usage_usec", rootLbl, 12345},
		{"cgroup.v2.cpu.user_usec", rootLbl, 6000},
		{"cgroup.v2.cpu.system_usec", rootLbl, 6345},
		{"cgroup.v2.io.rbytes", rootIOLbl, 1024},
		{"cgroup.v2.io.wbytes", rootIOLbl, 2048},
		{"cgroup.v2.io.rios", rootIOLbl, 3},
		{"cgroup.v2.io.wios", rootIOLbl, 4},

		{"cgroup.v2.memory.current", podLbl, 524288},
		{"cgroup.v2.memory.max", podLbl, 2097152},
		{"cgroup.v2.pids.current", podLbl, 10},
		{"cgroup.v2.cpu.usage_usec", podLbl, 5000},
		{"cgroup.v2.cpu.user_usec", podLbl, 3000},
		{"cgroup.v2.cpu.system_usec", podLbl, 2000},
	}
	for _, c := range cases {
		got, ok := testutil.Find(samples, c.name, c.labels)
		if !ok {
			t.Errorf("missing sample %q labels=%v", c.name, c.labels)
			continue
		}
		if got.Value != c.value {
			t.Errorf("%q labels=%v: got value %v, want %v", c.name, c.labels, got.Value, c.value)
		}
	}

	if _, ok := testutil.Find(samples, "cgroup.v2.memory.max", rootLbl); ok {
		t.Errorf("unbounded memory.max should be skipped at root, but was emitted")
	}
}

func TestNew_RejectsNonV2(t *testing.T) {
	tmp := t.TempDir()
	if _, err := New(Options{Path: tmp}); err == nil {
		t.Fatal("expected error for path without cgroup.controllers, got nil")
	}
}

func TestNew_RejectsMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := New(Options{Path: missing}); err == nil {
		t.Fatal("expected error for missing path, got nil")
	}
}

func TestPoller_Name(t *testing.T) {
	p, err := New(Options{Path: fixturePath})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.Name(); got != "cgroup" {
		t.Errorf("Name() = %q, want %q", got, "cgroup")
	}
}
