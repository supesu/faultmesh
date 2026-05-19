package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/procfs"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
)

const defaultPath = "/proc"

type Options struct {
	Path string
}

func (o Options) withDefaults() Options {
	if o.Path == "" {
		o.Path = defaultPath
	}
	return o
}

type Poller struct {
	path string
	fs   procfs.FS
}

func New(opts Options) (*Poller, error) {
	opts = opts.withDefaults()
	fs, err := procfs.NewFS(opts.Path)
	if err != nil {
		return nil, fmt.Errorf("proc: open %s: %w", opts.Path, err)
	}
	return &Poller{path: opts.Path, fs: fs}, nil
}

func (p *Poller) Name() string { return "proc" }

func (p *Poller) Poll(_ context.Context) ([]collect.Sample, error) {
	now := time.Now()
	var (
		out  []collect.Sample
		errs []error
	)
	for _, c := range []func(time.Time) ([]collect.Sample, error){
		p.statSamples,
		p.meminfoSamples,
		p.loadAvgSamples,
		p.diskstatsSamples,
		p.netDevSamples,
		p.uptimeSamples,
	} {
		s, err := c(now)
		if err != nil {
			errs = append(errs, err)
		}
		out = append(out, s...)
	}
	return out, errors.Join(errs...)
}

func (p *Poller) statSamples(ts time.Time) ([]collect.Sample, error) {
	stat, err := p.fs.Stat()
	if err != nil {
		return nil, fmt.Errorf("proc: stat: %w", err)
	}
	var samples []collect.Sample
	add := func(name string, value float64, labels map[string]string) {
		samples = append(samples, collect.Sample{
			Name: name, Value: value, Labels: labels, Timestamp: ts,
		})
	}
	emitCPU := func(c procfs.CPUStat, labels map[string]string) {
		add("proc.stat.cpu.user", c.User, labels)
		add("proc.stat.cpu.nice", c.Nice, labels)
		add("proc.stat.cpu.system", c.System, labels)
		add("proc.stat.cpu.idle", c.Idle, labels)
		add("proc.stat.cpu.iowait", c.Iowait, labels)
		add("proc.stat.cpu.irq", c.IRQ, labels)
		add("proc.stat.cpu.softirq", c.SoftIRQ, labels)
		add("proc.stat.cpu.steal", c.Steal, labels)
	}
	emitCPU(stat.CPUTotal, map[string]string{"cpu": "all"})
	for i, c := range stat.CPU {
		emitCPU(c, map[string]string{"cpu": strconv.FormatInt(i, 10)})
	}
	return samples, nil
}

func (p *Poller) meminfoSamples(ts time.Time) ([]collect.Sample, error) {
	mi, err := p.fs.Meminfo()
	if err != nil {
		return nil, fmt.Errorf("proc: meminfo: %w", err)
	}
	var samples []collect.Sample
	add := func(name string, v *uint64) {
		if v == nil {
			return
		}
		samples = append(samples, collect.Sample{
			Name: name, Value: float64(*v), Timestamp: ts,
		})
	}
	add("proc.meminfo.mem_total", mi.MemTotalBytes)
	add("proc.meminfo.mem_free", mi.MemFreeBytes)
	add("proc.meminfo.mem_available", mi.MemAvailableBytes)
	add("proc.meminfo.buffers", mi.BuffersBytes)
	add("proc.meminfo.cached", mi.CachedBytes)
	add("proc.meminfo.swap_total", mi.SwapTotalBytes)
	add("proc.meminfo.swap_free", mi.SwapFreeBytes)
	return samples, nil
}

func (p *Poller) loadAvgSamples(ts time.Time) ([]collect.Sample, error) {
	la, err := p.fs.LoadAvg()
	if err != nil {
		return nil, fmt.Errorf("proc: loadavg: %w", err)
	}
	return []collect.Sample{
		{Name: "proc.loadavg.1m", Value: la.Load1, Timestamp: ts},
		{Name: "proc.loadavg.5m", Value: la.Load5, Timestamp: ts},
		{Name: "proc.loadavg.15m", Value: la.Load15, Timestamp: ts},
	}, nil
}

func (p *Poller) netDevSamples(ts time.Time) ([]collect.Sample, error) {
	nd, err := p.fs.NetDev()
	if err != nil {
		return nil, fmt.Errorf("proc: net/dev: %w", err)
	}
	var samples []collect.Sample
	for iface, line := range nd {
		lbl := map[string]string{"interface": iface}
		samples = append(samples,
			collect.Sample{Name: "proc.net.dev.rx_bytes", Value: float64(line.RxBytes), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.net.dev.tx_bytes", Value: float64(line.TxBytes), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.net.dev.rx_packets", Value: float64(line.RxPackets), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.net.dev.tx_packets", Value: float64(line.TxPackets), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.net.dev.rx_errs", Value: float64(line.RxErrors), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.net.dev.tx_errs", Value: float64(line.TxErrors), Labels: lbl, Timestamp: ts},
		)
	}
	return samples, nil
}

func (p *Poller) diskstatsSamples(ts time.Time) ([]collect.Sample, error) {
	data, err := os.ReadFile(filepath.Join(p.path, "diskstats"))
	if err != nil {
		return nil, fmt.Errorf("proc: diskstats: %w", err)
	}
	var samples []collect.Sample
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		device := fields[2]
		reads, _ := strconv.ParseUint(fields[3], 10, 64)
		readSectors, _ := strconv.ParseUint(fields[5], 10, 64)
		writes, _ := strconv.ParseUint(fields[7], 10, 64)
		writeSectors, _ := strconv.ParseUint(fields[9], 10, 64)
		lbl := map[string]string{"device": device}
		samples = append(samples,
			collect.Sample{Name: "proc.diskstats.reads", Value: float64(reads), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.diskstats.writes", Value: float64(writes), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.diskstats.read_sectors", Value: float64(readSectors), Labels: lbl, Timestamp: ts},
			collect.Sample{Name: "proc.diskstats.write_sectors", Value: float64(writeSectors), Labels: lbl, Timestamp: ts},
		)
	}
	return samples, nil
}

func (p *Poller) uptimeSamples(ts time.Time) ([]collect.Sample, error) {
	data, err := os.ReadFile(filepath.Join(p.path, "uptime"))
	if err != nil {
		return nil, fmt.Errorf("proc: uptime: %w", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(data)))
	if len(fields) != 2 {
		return nil, fmt.Errorf("proc: uptime: expected 2 fields, got %d", len(fields))
	}
	up, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return nil, fmt.Errorf("proc: uptime up: %w", err)
	}
	idle, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, fmt.Errorf("proc: uptime idle: %w", err)
	}
	return []collect.Sample{
		{Name: "proc.uptime.seconds_up", Value: up, Timestamp: ts},
		{Name: "proc.uptime.seconds_idle", Value: idle, Timestamp: ts},
	}, nil
}
