package engine

import "github.com/metricq/metricq-db-hta-go/hta"

// Committed HTA state must remain independent of newer WAL-backed live state.
func cloneManifest(m manifest) manifest {
	c := m
	c.Series = make(map[string]*hta.Series, len(m.Series))
	for name, series := range m.Series {
		if series == nil {
			continue
		}
		s := *series
		s.Levels = make(map[int64]hta.Level, len(series.Levels))
		for level, state := range series.Levels {
			s.Levels[level] = state
		}
		c.Series[name] = &s
	}
	c.Roots = make(map[string]map[int64]blob, len(m.Roots))
	for name, levels := range m.Roots {
		c.Roots[name] = make(map[int64]blob, len(levels))
		for level, root := range levels {
			c.Roots[name][level] = root
		}
	}
	c.Garbage = make(map[string]bool, len(m.Garbage))
	for key, value := range m.Garbage {
		c.Garbage[key] = value
	}
	return c
}
