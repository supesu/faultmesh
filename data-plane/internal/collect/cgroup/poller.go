package cgroup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/supesu/faultmesh/data-plane/internal/collect"
)

const defaultPath = "/sys/fs/cgroup"

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
	root string
}

func New(opts Options) (*Poller, error) {
	opts = opts.withDefaults()
	marker := filepath.Join(opts.Path, "cgroup.controllers")
	if _, err := os.Stat(marker); err != nil {
		return nil, fmt.Errorf("cgroup: v2 unified hierarchy required at %s: %w", opts.Path, err)
	}
	return &Poller{root: opts.Path}, nil
}

func (p *Poller) Name() string { return "cgroup" }

func (p *Poller) Poll(ctx context.Context) ([]collect.Sample, error) {
	now := time.Now()
	var (
		out  []collect.Sample
		errs []error
	)
	walkErr := filepath.WalkDir(p.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(p.root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			rel = "/"
		} else {
			rel = "/" + filepath.ToSlash(rel)
		}
		cs, cerrs := p.readGroup(now, path, rel)
		out = append(out, cs...)
		errs = append(errs, cerrs...)
		return nil
	})
	if walkErr != nil {
		errs = append(errs, walkErr)
	}
	return out, errors.Join(errs...)
}

func (p *Poller) readGroup(ts time.Time, dir, rel string) ([]collect.Sample, []error) {
	var (
		samples []collect.Sample
		errs    []error
	)
	cgLbl := map[string]string{"cgroup": rel}

	if v, err := readSingleUint(filepath.Join(dir, "memory.current")); err == nil {
		samples = append(samples, collect.Sample{
			Name: "cgroup.v2.memory.current", Value: float64(v), Labels: cgLbl, Timestamp: ts,
		})
	} else if !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("cgroup: memory.current at %s: %w", rel, err))
	}

	if v, ok, err := readMemoryMax(filepath.Join(dir, "memory.max")); err == nil {
		if ok {
			samples = append(samples, collect.Sample{
				Name: "cgroup.v2.memory.max", Value: float64(v), Labels: cgLbl, Timestamp: ts,
			})
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("cgroup: memory.max at %s: %w", rel, err))
	}

	if v, err := readSingleUint(filepath.Join(dir, "pids.current")); err == nil {
		samples = append(samples, collect.Sample{
			Name: "cgroup.v2.pids.current", Value: float64(v), Labels: cgLbl, Timestamp: ts,
		})
	} else if !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("cgroup: pids.current at %s: %w", rel, err))
	}

	if kv, err := readKeyValue(filepath.Join(dir, "cpu.stat")); err == nil {
		for _, key := range []string{"usage_usec", "user_usec", "system_usec"} {
			if v, ok := kv[key]; ok {
				samples = append(samples, collect.Sample{
					Name: "cgroup.v2.cpu." + key, Value: float64(v), Labels: cgLbl, Timestamp: ts,
				})
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("cgroup: cpu.stat at %s: %w", rel, err))
	}

	if perDev, err := readIOStat(filepath.Join(dir, "io.stat")); err == nil {
		for device, kv := range perDev {
			lbl := map[string]string{"cgroup": rel, "device": device}
			for _, key := range []string{"rbytes", "wbytes", "rios", "wios"} {
				if v, ok := kv[key]; ok {
					samples = append(samples, collect.Sample{
						Name: "cgroup.v2.io." + key, Value: float64(v), Labels: lbl, Timestamp: ts,
					})
				}
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("cgroup: io.stat at %s: %w", rel, err))
	}

	return samples, errs
}

func readSingleUint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

func readMemoryMax(path string) (value uint64, ok bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, false, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

func readKeyValue(path string) (map[string]uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]uint64)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		out[fields[0]] = v
	}
	return out, nil
}

func readIOStat(path string) (map[string]map[string]uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]uint64)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		device := fields[0]
		kv := make(map[string]uint64, len(fields)-1)
		for _, kvPair := range fields[1:] {
			eq := strings.IndexByte(kvPair, '=')
			if eq < 0 {
				continue
			}
			v, err := strconv.ParseUint(kvPair[eq+1:], 10, 64)
			if err != nil {
				continue
			}
			kv[kvPair[:eq]] = v
		}
		out[device] = kv
	}
	return out, nil
}
