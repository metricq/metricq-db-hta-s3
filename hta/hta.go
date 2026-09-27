// Package hta implements MetricQ's hierarchical timeline aggregation (HTA)
// for one metric: raw samples and a chain of aggregate levels
// interval_min, interval_min*factor, ... up to interval_max. Aggregation is
// right-endpoint and time-weighted, like the C++ metricq-db-hta. It performs
// no I/O and is deterministic, which WAL replay relies on.
package hta

import (
	"fmt"
	"math"
)

// Config is the aggregation configuration of one metric, in nanoseconds.
// Input selects the incoming MetricQ metric; it is not part of the layout.
type Config struct {
	Input          string `json:"input,omitempty"`
	IntervalMin    int64  `json:"interval_min"`
	IntervalMax    int64  `json:"interval_max"`
	IntervalFactor int64  `json:"interval_factor"`
}

// Defaults fills unset intervals with the legacy defaults.
func (c Config) Defaults() Config {
	if c.IntervalMin == 0 {
		c.IntervalMin = 10_000_000_000
	}
	if c.IntervalMax == 0 {
		c.IntervalMax = 31_536_000_000_000_000
	}
	if c.IntervalFactor == 0 {
		c.IntervalFactor = 10
	}
	return c
}

// Validate checks that the levels form a proper hierarchy.
func (c Config) Validate() error {
	if c.IntervalMin <= 0 || c.IntervalMax < c.IntervalMin || c.IntervalFactor < 2 || c.IntervalMin%c.IntervalFactor != 0 {
		return fmt.Errorf("invalid HTA intervals: %+v", c)
	}
	return nil
}

// Point is one sample.
type Point struct {
	Time  int64
	Value float64
}

// Aggregate summarizes an interval. Integral and ActiveTime are in value times
// nanoseconds and nanoseconds.
type Aggregate struct {
	Minimum, Maximum, Sum float64
	Count                 uint64
	Integral              float64
	ActiveTime            int64
}

// Empty returns the neutral aggregate.
func Empty() Aggregate { return Aggregate{Minimum: math.Inf(1), Maximum: math.Inf(-1)} }

// Value returns the aggregate of value v held for duration nanoseconds and
// counting count samples.
func Value(v float64, duration int64, count uint64) Aggregate {
	sum := float64(0)
	if count > 0 {
		sum = v * float64(count)
	}
	return Aggregate{v, v, sum, count, v * float64(duration), duration}
}

// Add merges b into a.
func (a *Aggregate) Add(b Aggregate) {
	a.Minimum = math.Min(a.Minimum, b.Minimum)
	a.Maximum = math.Max(a.Maximum, b.Maximum)
	a.Sum += b.Sum
	a.Count += b.Count
	a.Integral += b.Integral
	a.ActiveTime += b.ActiveTime
}

// Times returns the aggregate of n consecutive intervals equal to a.
func (a Aggregate) Times(n int64) Aggregate {
	a.Sum *= float64(n)
	a.Count *= uint64(n)
	a.Integral *= float64(n)
	a.ActiveTime *= n
	return a
}

// Record is either a raw point (Level=0), or a run of identical, consecutive
// aggregates. Runs keep storage and CPU bounded for very sparse metrics.
type Record struct {
	Time, Level, Repeat int64
	Value               float64
	Aggregate           Aggregate
}

// LastTime is the start of the last interval a record covers; for raw records
// the sample time.
func (r Record) LastTime() int64 {
	if r.Level == 0 {
		return r.Time
	}
	return r.Time + (r.Repeat-1)*r.Level
}

// Level is the open (incomplete) interval of one aggregate level.
type Level struct {
	Time      int64
	Aggregate Aggregate
}

// Series is the aggregation state of one metric: its configuration, the open
// interval of every level and the first and last accepted sample.
type Series struct {
	Config      Config
	First, Last Point
	Levels      map[int64]Level
}

// New returns an empty series.
func New(c Config) *Series { return &Series{Config: c, Levels: make(map[int64]Level)} }

// Insert adds a sample. It returns false, without changing state, for samples
// the legacy database skips (not strictly increasing, non-finite). Every record
// completed by the sample is passed to emit: the raw record first, then
// completed aggregate intervals from fine to coarse, with runs of identical
// empty intervals as one record.
func (s *Series) Insert(p Point, emit func(Record)) bool {
	if p.Time <= s.Last.Time || math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
		return false
	}
	if s.First.Time == 0 {
		s.First = p
	}
	emit(Record{Time: p.Time, Repeat: 1, Value: p.Value})
	interval := s.Config.IntervalMin
	l, ok := s.Levels[interval]
	if !ok {
		l = Level{p.Time, Empty()}
	}
	boundary := l.Time - l.Time%interval
	// Avoid overflowing the final representable timestamp bucket.
	if boundary <= math.MaxInt64-interval && p.Time >= boundary+interval {
		end := boundary + interval
		l.Aggregate.Add(Value(p.Value, end-l.Time, 0))
		s.emit(Record{Time: boundary, Level: interval, Repeat: 1, Aggregate: l.Aggregate}, emit)
		n := (p.Time - end) / interval
		if n > 0 {
			s.emit(Record{Time: end, Level: interval, Repeat: n, Aggregate: Value(p.Value, interval, 0)}, emit)
		}
		l = Level{end + n*interval, Empty()}
	}
	l.Aggregate.Add(Value(p.Value, p.Time-l.Time, 1))
	l.Time = p.Time
	s.Levels[interval] = l
	s.Last = p
	return true
}
func (s *Series) emit(r Record, emit func(Record)) {
	emit(r)
	if r.Level > s.Config.IntervalMax/s.Config.IntervalFactor {
		return
	}
	upper := r.Level * s.Config.IntervalFactor
	l, ok := s.Levels[upper]
	if !ok {
		l = Level{r.Time, Empty()}
	}
	for r.Repeat > 0 {
		remaining := (upper - l.Time%upper) / r.Level
		n := min(remaining, r.Repeat)
		l.Aggregate.Add(r.Aggregate.Times(n))
		l.Time += n * r.Level
		r.Time += n * r.Level
		r.Repeat -= n
		if l.Time%upper == 0 {
			s.emit(Record{Time: l.Time - upper, Level: upper, Repeat: 1, Aggregate: l.Aggregate}, emit)
			l.Aggregate = Empty()
			// Pass long runs through the tree without iterating every empty interval.
			full := r.Repeat / s.Config.IntervalFactor
			if full > 0 {
				s.emit(Record{Time: r.Time, Level: upper, Repeat: full, Aggregate: r.Aggregate.Times(s.Config.IntervalFactor)}, emit)
				n = full * s.Config.IntervalFactor
				r.Time += n * r.Level
				r.Repeat -= n
				l.Time = r.Time
			}
		}
	}
	s.Levels[upper] = l
}
