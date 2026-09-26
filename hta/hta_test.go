package hta

import (
	"math"
	"testing"
)

func TestSparseRunsAndHierarchy(t *testing.T) {
	s := New(Config{IntervalMin: 100, IntervalMax: 100000, IntervalFactor: 10})
	rows := map[int64][]Record{}
	emit := func(r Record) { rows[r.Level] = append(rows[r.Level], r) }
	s.Insert(Point{110, 2}, emit)
	s.Insert(Point{100000000000110, 4}, emit)
	for level, rs := range rows {
		if len(rs) > 6 {
			t.Fatalf("gap expanded at level %d into %d records", level, len(rs))
		}
	}
	first := rows[100][0]
	if first.Time != 100 || first.Aggregate.Count != 1 || first.Aggregate.Integral != 360 || first.Aggregate.ActiveTime != 90 {
		t.Fatalf("wrong partial first bucket: %+v", first)
	}
	if s.Last.Time != 100000000000110 {
		t.Fatal("last point missing")
	}
}
func TestInsertFiltering(t *testing.T) {
	s := New(Config{IntervalMin: 100, IntervalMax: 1000, IntervalFactor: 10})
	emit := func(Record) {}
	for _, p := range []Point{{0, 1}, {-1, 2}, {100, math.NaN()}, {100, math.Inf(1)}} {
		if s.Insert(p, emit) {
			t.Fatalf("accepted %+v", p)
		}
	}
	if !s.Insert(Point{100, 2}, emit) || s.Insert(Point{100, 3}, emit) || s.Insert(Point{99, 3}, emit) {
		t.Fatal("monotonicity violated")
	}
}
