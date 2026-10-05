package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

// limitEngine stores n raw points of x at 10 ns spacing in data blocks.
func limitEngine(t *testing.T, o Options, n int) (*Engine, *countedRangeGCStore) {
	t.Helper()
	s := &countedRangeGCStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}}
	o.WALDirectory = t.TempDir()
	e, err := Open(context.Background(), s, o, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	points := make([]hta.Point, n)
	for i := range points {
		points[i] = hta.Point{Time: int64(i+1) * 10, Value: float64(i % 13)}
	}
	ingest(t, e, points...)
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.gets = 0
	return e, s
}

func TestOversizedRawQueryRejectedBeforeReading(t *testing.T) {
	// 20 000 raw points need about 260 kB; allow 100 kB.
	e, s := limitEngine(t, Options{QueryMaxResponseBytes: 100_000}, 20_000)
	_, err := e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 1 << 40})
	if err == nil || !strings.Contains(err.Error(), "would exceed query_max_response_bytes") {
		t.Fatalf("expected early rejection: %v", err)
	}
	if s.gets != 0 {
		t.Fatalf("%d data reads before rejecting", s.gets)
	}
	// A short range fits.
	r, err := e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 50_000})
	if err != nil || len(r.Value) < 4000 {
		t.Fatalf("short range: %v, %d values", err, len(r.GetValue()))
	}
	// Smoothing into few intervals fits as well, although many raw records are read.
	r, err = e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 200_000, IntervalMax: 1000})
	if err != nil || len(r.TimeDelta) == 0 {
		t.Fatalf("smoothed range: %v", err)
	}
}

func TestOversizedAggregateTimelineRejectedBeforeReading(t *testing.T) {
	e, s := limitEngine(t, Options{QueryMaxResponseBytes: 10_000}, 20_000)
	// Level 100 holds 2000 aggregates of about 53 bytes: 106 kB > 10 kB.
	_, err := e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_AGGREGATE_TIMELINE, StartTime: 0, EndTime: 1 << 40, IntervalMax: 100})
	if err == nil || !strings.Contains(err.Error(), "would exceed") {
		t.Fatalf("expected early rejection: %v", err)
	}
	if s.gets != 0 {
		t.Fatalf("%d data reads before rejecting", s.gets)
	}
}

func TestEncodedResponseSizeIsChecked(t *testing.T) {
	e, err := Open(context.Background(), newStore(), Options{WALDirectory: t.TempDir()}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// Time deltas of 2^50 ns take 8 varint bytes: about 17 bytes per point,
	// more than the 13 the estimate assumes.
	points := make([]hta.Point, 50)
	for i := range points {
		points[i] = hta.Point{Time: int64(i+1) << 50, Value: float64(i)}
	}
	ingest(t, e, points...)
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 60 << 50}
	full, err := e.Query(context.Background(), "x", req)
	if err != nil {
		t.Fatal(err)
	}
	size := proto.Size(full)
	e.options.QueryMaxResponseBytes = size
	if _, err = e.Query(context.Background(), "x", req); err != nil {
		t.Fatalf("response at the limit rejected: %v", err)
	}
	// Large time deltas exceed the typical 13 bytes per point: the estimate
	// passes, the exact check must not.
	e.options.QueryMaxResponseBytes = size - 1
	resp, err := e.Query(context.Background(), "x", req)
	if err == nil || len(resp.GetTimeDelta()) != 0 {
		t.Fatalf("oversized response returned: %v", err)
	}
}

func TestQueryMemoryBudgetRejectsAndReleases(t *testing.T) {
	e, _ := limitEngine(t, Options{QueryMemoryBytes: 1 << 20}, 20_000)
	// 20 000 records need about 5 MiB of the 1 MiB budget.
	_, err := e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 1 << 40})
	if err == nil || !strings.Contains(err.Error(), "query_memory_bytes") {
		t.Fatalf("expected memory rejection: %v", err)
	}
	if free := e.queryBudget.free; free != e.queryBudget.capacity {
		t.Fatalf("budget not released: %d of %d free", free, e.queryBudget.capacity)
	}
	if _, err = e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 10_000}); err != nil {
		t.Fatal(err)
	}
	if free := e.queryBudget.free; free != e.queryBudget.capacity {
		t.Fatalf("budget not released after success: %d of %d free", free, e.queryBudget.capacity)
	}
}

func TestQueryBudgetWaitsAndAvoidsHoldAndWait(t *testing.T) {
	b := newQueryBudget(100)
	first := &queryReservation{budget: b}
	if err := first.reserve(context.Background(), 80); err != nil {
		t.Fatal(err)
	}
	// A further reservation of a query already holding memory must not wait.
	if err := first.reserve(context.Background(), 30); !errors.Is(err, errQueryMemoryBusy) {
		t.Fatalf("holding query waited or succeeded: %v", err)
	}
	// A new query waits until memory is released.
	second := &queryReservation{budget: b}
	var wg sync.WaitGroup
	wg.Add(1)
	var waitErr error
	go func() {
		defer wg.Done()
		waitErr = second.reserve(context.Background(), 50)
	}()
	time.Sleep(20 * time.Millisecond)
	first.releaseAll()
	wg.Wait()
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	// Waiting ends with the request context.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	third := &queryReservation{budget: b}
	if err := third.reserve(ctx, 60); err == nil {
		t.Fatal("reservation beyond the free budget succeeded")
	}
	second.releaseAll()
	if b.free != 100 {
		t.Fatalf("free %d", b.free)
	}
}
