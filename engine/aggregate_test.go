package engine

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
)

// rawReference computes a single aggregate directly from the points with the
// engine's semantics: the window is clipped to the series, every value counts
// for the time since the previous value (or the window start), and the first
// value at or after the window end contributes its value up to the end.
func rawReference(points []hta.Point, begin, end int64) hta.Aggregate {
	a := hta.Empty()
	first, last := points[0].Time, points[len(points)-1].Time
	if end <= first || begin > last {
		return a
	}
	begin, end = max(begin, first), min(end, last)
	previous := begin
	for _, p := range points {
		if p.Time < begin {
			continue
		}
		if p.Time >= end {
			a.Add(hta.Value(p.Value, end-previous, 0))
			break
		}
		a.Add(hta.Value(p.Value, p.Time-previous, 1))
		previous = p.Time
	}
	return a
}

func closeTo(a, b float64) bool {
	return a == b || math.Abs(a-b) <= 1e-9*max(math.Abs(a), math.Abs(b))
}

// Single aggregates from the index (stored aggregates inside the window,
// border blocks read) equal the aggregate of the raw values for windows from
// one unit to the whole history, after compaction rewrote blocks and with
// values still in memory.
func TestAggregateMatchesRaw(t *testing.T) {
	ctx := context.Background()
	config := map[string]hta.Config{"x": {IntervalMin: 100, IntervalMax: 10_000_000, IntervalFactor: 10}}
	options := maintenanceOptions(t.TempDir(), true)
	options.IngestMemoryLimitBytes = 64 << 20
	e, err := Open(ctx, newStore(), options, config, NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	r := rand.New(rand.NewPCG(3, 9))
	points := make([]hta.Point, 300_000)
	now := int64(1_000)
	for i := range points {
		now += 1 + r.Int64N(30)
		if r.IntN(5000) == 0 {
			now += 50_000 // gaps longer than several levels
		}
		points[i] = hta.Point{Time: now, Value: math.Round(r.NormFloat64()*1000) / 10}
	}
	for start := 0; start < len(points); start += 1000 {
		if err := e.Ingest(ctx, "x", chunk(points[start:min(start+1000, len(points))]...)); err != nil {
			t.Fatal(err)
		}
		// Irregular checkpoints leave partial blocks for compaction.
		if r.IntN(20) == 0 {
			if err := e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Merges, rechunks and locality rewrite blocks and carry or recompute
	// their aggregates; the last values stay unflushed in memory.
	for i := 0; i < 200; i++ {
		before := metricValue(t, e.metrics.Compactions)
		if err := e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if metricValue(t, e.metrics.Compactions) == before && !e.compactionScanMore {
			break
		}
	}
	if metricValue(t, e.metrics.Compactions) == 0 {
		t.Fatal("fixture: no compaction")
	}
	checkIndexAggregates(t, e)
	tail := make([]hta.Point, 500)
	for i := range tail {
		now += 1 + r.Int64N(30)
		tail[i] = hta.Point{Time: now, Value: float64(i % 17)}
	}
	if err := e.Ingest(ctx, "x", chunk(tail...)); err != nil {
		t.Fatal(err)
	}
	points = append(points, tail...)
	span := points[len(points)-1].Time - points[0].Time
	requests := func() float64 { sum, _ := histogramSum(t, e.metrics.QueryDataRequests); return sum }
	var cold float64
	for i := 0; i < 400; i++ {
		// Cold caches: every query reads its blocks from the store.
		e.sharedNodes, e.sharedBlocks = newIndexPageCache(), newDataBlockCache()
		before := requests()
		length := int64(math.Pow(10, r.Float64()*math.Log10(float64(span))))
		begin := points[0].Time - 500 + r.Int64N(span-length+1000)
		end := begin + max(length, 1)
		resp, err := e.Query(ctx, "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_AGGREGATE, StartTime: begin, EndTime: end})
		if err != nil {
			t.Fatal(err)
		}
		cold += requests() - before
		want := rawReference(points, begin, end)
		got := resp.GetAggregate()[0]
		if got.Count != want.Count || got.ActiveTime != want.ActiveTime || (want.Count > 0 && (got.Minimum != want.Minimum || got.Maximum != want.Maximum)) || !closeTo(got.Sum, want.Sum) || !closeTo(got.Integral, want.Integral) {
			t.Fatalf("[%d,%d) length %d:\n got %+v\nwant %+v", begin, end, end-begin, got, want)
		}
	}
	t.Logf("cold data range requests per aggregate: %.2f", cold/400)
}
