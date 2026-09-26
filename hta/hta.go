// Package hta implements MetricQ's right-endpoint, time-weighted aggregation.
package hta

import (
	"fmt"
	"math"
)

type Config struct {
	Input          string `json:"input,omitempty"`
	IntervalMin    int64  `json:"interval_min"`
	IntervalMax    int64  `json:"interval_max"`
	IntervalFactor int64  `json:"interval_factor"`
}

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
func (c Config) Validate() error {
	if c.IntervalMin <= 0 || c.IntervalMax < c.IntervalMin || c.IntervalFactor < 2 || c.IntervalMin%c.IntervalFactor != 0 {
		return fmt.Errorf("invalid HTA intervals: %+v", c)
	}
	return nil
}

type Point struct {
	Time  int64
	Value float64
}
type Aggregate struct {
	Minimum, Maximum, Sum float64
	Count                 uint64
	Integral              float64
	ActiveTime            int64
}

func Empty() Aggregate { return Aggregate{Minimum: math.Inf(1), Maximum: math.Inf(-1)} }
func Value(v float64, duration int64, count uint64) Aggregate {
	sum := float64(0)
	if count > 0 {
		sum = v * float64(count)
	}
	return Aggregate{v, v, sum, count, v * float64(duration), duration}
}
func (a *Aggregate) Add(b Aggregate) {
	a.Minimum = math.Min(a.Minimum, b.Minimum)
	a.Maximum = math.Max(a.Maximum, b.Maximum)
	a.Sum += b.Sum
	a.Count += b.Count
	a.Integral += b.Integral
	a.ActiveTime += b.ActiveTime
}
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

func (r Record) LastTime() int64 {
	if r.Level == 0 {
		return r.Time
	}
	return r.Time + (r.Repeat-1)*r.Level
}

type Level struct {
	Time      int64
	Aggregate Aggregate
}
type Series struct {
	Config      Config
	First, Last Point
	Levels      map[int64]Level
}

func New(c Config) *Series { return &Series{Config: c, Levels: make(map[int64]Level)} }
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
