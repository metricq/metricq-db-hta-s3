//go:build integration

package integration

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/engine"
	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

// Real S3 checksums, CAS, range reads and deletion with held data and watermarks
// spanning multiple metadata pages. The general legacy parity suite separately
// checks all four request types against the file-based reference implementation.
func TestHeldMetadataS3Recovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store, _ := newS3(t, ctx, fmt.Sprintf("hta-held-pages-%d", time.Now().UnixNano()))
	options := engine.Options{WALDirectory: t.TempDir(), MaintenanceEnabled: true, HoldMaxAgeSeconds: 3600, IngestMemoryLimitBytes: 64 << 20, HoldMemoryBytes: 32 << 20}
	configs := map[string]hta.Config{}
	deliveries := []engine.Delivery{}
	const base = int64(1700000000) * int64(time.Second)
	for i := 0; i < 80; i++ {
		name := fmt.Sprintf("canonical.metric.%04d", i)
		configs[name] = hta.Config{IntervalMin: int64(time.Second), IntervalMax: 100000 * int64(time.Second), IntervalFactor: 10}
		c := &metricq.DataChunk{}
		for j := 0; j < 1070; j++ {
			delta := int64(time.Second)
			if j == 0 {
				delta = base
			}
			c.TimeDelta = append(c.TimeDelta, delta)
			c.Value = append(c.Value, math.Sin(float64(j)/31)+float64(i))
		}
		deliveries = append(deliveries, engine.Delivery{Metric: name, Chunk: c})
	}
	e, err := engine.Open(ctx, store, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if n, err := e.IngestBatch(ctx, deliveries); err != nil || n != len(deliveries) {
		t.Fatalf("ingest %d: %v", n, err)
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Append an inventory entry while reusing most watermark pages.
	if err = e.Ingest(ctx, "canonical.metric.0000", &metricq.DataChunk{TimeDelta: []int64{base + 1070*int64(time.Second)}, Value: []float64{99}}); err != nil {
		t.Fatal(err)
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err = e.Reclaim(ctx); err != nil {
			t.Fatal(err)
		}
	}
	requests := []*metricq.HistoryRequest{}
	for _, kind := range []metricq.HistoryRequest_RequestType{metricq.HistoryRequest_FLEX_TIMELINE, metricq.HistoryRequest_AGGREGATE, metricq.HistoryRequest_LAST_VALUE, metricq.HistoryRequest_AGGREGATE_TIMELINE} {
		requests = append(requests, &metricq.HistoryRequest{Type: kind, StartTime: base, EndTime: base + 1100*int64(time.Second), IntervalMax: 10 * int64(time.Second)})
	}
	responses := []*metricq.HistoryResponse{}
	for _, req := range requests {
		r, err := e.Query(ctx, "canonical.metric.0000", req)
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, r)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	options.WALDirectory = t.TempDir()
	recovered, err := engine.Open(ctx, store, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for i, req := range requests {
		r, err := recovered.Query(ctx, "canonical.metric.0000", req)
		if err != nil || !proto.Equal(r, responses[i]) {
			t.Fatalf("history differs after S3-only recovery (%s): %v", req.Type, err)
		}
	}
}
