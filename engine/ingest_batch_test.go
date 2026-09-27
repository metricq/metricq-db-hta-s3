package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

func walSyncs(t *testing.T, r *prometheus.Registry) uint64 {
	t.Helper()
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "metricq_db_wal_sync_seconds" {
			return f.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatal("missing WAL sync histogram")
	return 0
}

var batchConfig = map[string]hta.Config{
	"x": {IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10},
	"y": {IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10},
}

func TestIngestBatchUsesOneSyncAndReplaysLikeSingleDeliveries(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	dir := t.TempDir()
	s := newStore()
	e, err := Open(ctx, s, Options{WALDirectory: dir, BuilderHard: 64 << 20}, batchConfig, NewMetrics(registry))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := Open(ctx, newStore(), Options{WALDirectory: t.TempDir(), BuilderHard: 64 << 20}, batchConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reference.Close()
	var batch []Delivery
	for i := int64(0); i < 20; i++ {
		name := "x"
		if i%3 == 0 {
			name = "y"
		}
		c := chunk(hta.Point{Time: 100 + i*70, Value: float64(i)}, hta.Point{Time: 130 + i*70, Value: float64(-i)})
		batch = append(batch, Delivery{Metric: name, Chunk: c})
		if err = reference.Ingest(ctx, name, c); err != nil {
			t.Fatal(err)
		}
	}
	// A redelivered chunk is discarded but still counts as processed.
	batch = append(batch, batch[len(batch)-1])
	before := walSyncs(t, registry)
	n, err := e.IngestBatch(ctx, batch)
	if err != nil || n != len(batch) {
		t.Fatalf("processed %d of %d: %v", n, len(batch), err)
	}
	if got := walSyncs(t, registry) - before; got != 1 {
		t.Fatalf("%d WAL syncs for one batch", got)
	}
	if e.sequence != reference.sequence {
		t.Fatalf("sequence %d, per-delivery ingest %d", e.sequence, reference.sequence)
	}
	requests := []*metricq.HistoryRequest{
		{Type: metricq.HistoryRequest_LAST_VALUE},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 3000, IntervalMax: 50},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 3000, IntervalMax: 100},
		{Type: metricq.HistoryRequest_AGGREGATE, StartTime: 0, EndTime: 3000},
	}
	check := func(label string, e *Engine) {
		t.Helper()
		for _, name := range []string{"x", "y"} {
			for _, req := range requests {
				want, err := reference.Query(ctx, name, req)
				if err != nil {
					t.Fatal(err)
				}
				got, err := e.Query(ctx, name, req)
				if err != nil || !proto.Equal(want, got) {
					t.Fatalf("%s %s %v: %v vs %v (%v)", label, name, req.Type, got, want, err)
				}
			}
		}
	}
	check("batch", e)
	// Crash without checkpoint: replay the batch's individual frames.
	e.Close()
	replayed, err := Open(ctx, s, Options{WALDirectory: dir, BuilderHard: 64 << 20}, batchConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer replayed.Close()
	check("replay", replayed)
}

func TestIngestBatchStopsAtPressureAndInvalidDeliveries(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, newStore(), Options{WALDirectory: t.TempDir(), WALTarget: 500, WALHigh: 1000, WALHard: 1500, ObjectTarget: 100, BuilderHard: 10000}, batchConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var batch []Delivery
	for i := int64(1); i <= 100; i++ {
		batch = append(batch, Delivery{Metric: "x", Chunk: chunk(hta.Point{Time: i * 10, Value: 2})})
	}
	n, err := e.IngestBatch(ctx, batch)
	if !errors.Is(err, ErrPressure) || n == 0 || n == len(batch) {
		t.Fatalf("expected partial batch before pressure: %d %v", n, err)
	}
	if e.wal.total() > e.options.WALHard || e.sequence != uint64(n) {
		t.Fatalf("WAL %d bytes, sequence %d after %d deliveries", e.wal.total(), e.sequence, n)
	}
	// The caller flushes and retries only the remainder.
	for rest := batch[n:]; len(rest) > 0; {
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		n, err = e.IngestBatch(ctx, rest)
		if err != nil && !errors.Is(err, ErrPressure) {
			t.Fatal(err)
		}
		rest = rest[n:]
	}
	response, err := e.Query(ctx, "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 2000, IntervalMax: 1})
	if err != nil || len(response.Value) != 100 {
		t.Fatalf("lost retried deliveries: %d %v", len(response.Value), err)
	}

	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	sequence := e.sequence
	invalid := []Delivery{
		{Metric: "y", Chunk: chunk(hta.Point{Time: 10, Value: 1})},
		{Metric: "y", Chunk: &metricq.DataChunk{TimeDelta: []int64{1}}},
		{Metric: "y", Chunk: chunk(hta.Point{Time: 20, Value: 1})},
	}
	if n, err = e.IngestBatch(ctx, invalid); err == nil || n != 1 || e.sequence != sequence+1 {
		t.Fatalf("malformed delivery: processed %d, sequence +%d, %v", n, e.sequence-sequence, err)
	}
	if n, err = e.IngestBatch(ctx, []Delivery{invalid[0], {Metric: "z", Chunk: invalid[2].Chunk}}); err == nil || n != 1 {
		t.Fatalf("unknown metric: processed %d, %v", n, err)
	}
}
