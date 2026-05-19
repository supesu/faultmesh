package testutil

import "github.com/supesu/faultmesh/data-plane/internal/collect"

func LabelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func Find(samples []collect.Sample, name string, labels map[string]string) (collect.Sample, bool) {
	for _, s := range samples {
		if s.Name == name && LabelsEqual(s.Labels, labels) {
			return s, true
		}
	}
	return collect.Sample{}, false
}
