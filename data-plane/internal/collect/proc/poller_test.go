package proc

import (
	"context"
	"testing"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect/testutil"
)

const fixturePath = "testdata/proc"

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
	if len(samples) == 0 {
		t.Fatal("Poll returned no samples")
	}

	for _, s := range samples {
		if s.Timestamp.Before(before) || s.Timestamp.After(after) {
			t.Errorf("sample %q timestamp %v outside [%v, %v]", s.Name, s.Timestamp, before, after)
		}
	}

	want := []struct {
		name   string
		labels map[string]string
		value  float64
	}{
		{"proc.loadavg.1m", nil, 0.50},
		{"proc.loadavg.5m", nil, 0.75},
		{"proc.loadavg.15m", nil, 1.00},

		{"proc.meminfo.mem_total", nil, float64(16384000 * 1024)},
		{"proc.meminfo.mem_free", nil, float64(8192000 * 1024)},
		{"proc.meminfo.mem_available", nil, float64(12288000 * 1024)},
		{"proc.meminfo.buffers", nil, float64(524288 * 1024)},
		{"proc.meminfo.cached", nil, float64(4194304 * 1024)},
		{"proc.meminfo.swap_total", nil, float64(8388608 * 1024)},
		{"proc.meminfo.swap_free", nil, float64(8388608 * 1024)},

		{"proc.uptime.seconds_up", nil, 123456.78},
		{"proc.uptime.seconds_idle", nil, 654321.00},

		{"proc.diskstats.reads", map[string]string{"device": "sda"}, 100},
		{"proc.diskstats.writes", map[string]string{"device": "sda"}, 200},
		{"proc.diskstats.read_sectors", map[string]string{"device": "sda"}, 1600},
		{"proc.diskstats.write_sectors", map[string]string{"device": "sda"}, 3200},
		{"proc.diskstats.reads", map[string]string{"device": "sda1"}, 50},

		{"proc.net.dev.rx_bytes", map[string]string{"interface": "eth0"}, 2048},
		{"proc.net.dev.tx_bytes", map[string]string{"interface": "eth0"}, 4096},
		{"proc.net.dev.rx_packets", map[string]string{"interface": "eth0"}, 16},
		{"proc.net.dev.tx_packets", map[string]string{"interface": "eth0"}, 32},
		{"proc.net.dev.rx_errs", map[string]string{"interface": "eth0"}, 1},
		{"proc.net.dev.tx_errs", map[string]string{"interface": "eth0"}, 2},
		{"proc.net.dev.rx_bytes", map[string]string{"interface": "lo"}, 1024},
		{"proc.net.dev.tx_bytes", map[string]string{"interface": "lo"}, 1024},

		{"proc.stat.cpu.user", map[string]string{"cpu": "all"}, 1.0},
		{"proc.stat.cpu.system", map[string]string{"cpu": "all"}, 0.5},
		{"proc.stat.cpu.idle", map[string]string{"cpu": "all"}, 10.0},
		{"proc.stat.cpu.user", map[string]string{"cpu": "0"}, 0.5},
		{"proc.stat.cpu.user", map[string]string{"cpu": "1"}, 0.5},
	}

	for _, w := range want {
		got, ok := testutil.Find(samples, w.name, w.labels)
		if !ok {
			t.Errorf("missing sample %q labels=%v", w.name, w.labels)
			continue
		}
		if got.Value != w.value {
			t.Errorf("%q labels=%v: got value %v, want %v", w.name, w.labels, got.Value, w.value)
		}
	}
}

func TestNew_BadPath(t *testing.T) {
	if _, err := New(Options{Path: "testdata/does-not-exist"}); err == nil {
		t.Fatal("expected error for missing path, got nil")
	}
}

func TestPoller_Name(t *testing.T) {
	p, err := New(Options{Path: fixturePath})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.Name(); got != "proc" {
		t.Errorf("Name() = %q, want %q", got, "proc")
	}
}
