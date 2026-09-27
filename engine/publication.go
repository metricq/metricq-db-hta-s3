package engine

import "github.com/metricq/metricq-db-hta-s3/hta"

// Committed HTA state must remain independent of newer WAL-backed live state.
func cloneManifest(m manifest) manifest {
	c := m
	c.Series = cloneSeries(m.Series)
	c.Roots = cloneRoots(m.Roots)
	c.Garbage = make(map[string]bool, len(m.Garbage))
	for key, value := range m.Garbage {
		c.Garbage[key] = value
	}
	return c
}

func cloneSeries(seriesMap map[string]*hta.Series) map[string]*hta.Series {
	c := make(map[string]*hta.Series, len(seriesMap))
	for name, series := range seriesMap {
		if series == nil {
			continue
		}
		s := *series
		s.Levels = make(map[int64]hta.Level, len(series.Levels))
		for level, state := range series.Levels {
			s.Levels[level] = state
		}
		c[name] = &s
	}
	return c
}

func cloneRoots(roots map[string]map[int64]blob) map[string]map[int64]blob {
	c := make(map[string]map[int64]blob, len(roots))
	for name, levels := range roots {
		c[name] = make(map[int64]blob, len(levels))
		for level, root := range levels {
			c[name][level] = root
		}
	}
	return c
}

// Committed Series and per-metric Roots are immutable. Maintenance copies the
// root directory, then clones only the metric maps it actually changes.
func cloneMaintenanceManifest(m manifest) manifest {
	c := m
	c.Roots = make(map[string]map[int64]blob, len(m.Roots))
	for name, levels := range m.Roots {
		c.Roots[name] = levels
	}
	c.Garbage = make(map[string]bool, len(m.Garbage))
	for key, v := range m.Garbage {
		c.Garbage[key] = v
	}
	return c
}
