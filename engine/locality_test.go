package engine

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

func TestLocalityPacksWholeLevelAcrossTimeGapsAndPreservesSealedPrefix(t *testing.T) {
	ctx := context.Background()
	s := &countedRangeGCStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.AppendOnlyAggregates = true
	opts.HoldSeconds = 3600
	opts.Compaction.ObjectBytes = 4 << 20
	opts.Compaction.MaxJobBytes = 4 << 20
	opts.Compaction.MaxBlocks = 512
	e, err := Open(ctx, s, opts, map[string]hta.Config{"canonical.short": {IntervalMin: int64(time.Second), IntervalMax: int64(100000 * time.Second), IntervalFactor: 10}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	appendBlock := func(block int) {
		points := make([]hta.Point, maxDataBlockRecords)
		// Enormous gaps make temporal window grouping ineffective. Only level
		// order and physical pack bounds should affect raw-block locality.
		for j := range points {
			points[j] = hta.Point{Time: int64(1000000*block+j+1) * int64(time.Second), Value: math.Sin(float64(j) / 31)}
		}
		if err = e.Ingest(ctx, "canonical.short", chunk(points...)); err != nil {
			t.Fatal(err)
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		appendBlock(i)
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 10_000_000 * int64(time.Second)}
	queryCold := func() (*metricq.HistoryResponse, int64) {
		e.sharedNodes = newIndexPageCache()
		e.sharedBlocks = newDataBlockCache()
		s.gets = 0
		r, err := e.Query(ctx, "canonical.short", req)
		if err != nil {
			t.Fatal(err)
		}
		return r, s.gets
	}
	before, gets := queryCold()
	if gets != 5 || len(before.Value) != 5*maxDataBlockRecords {
		t.Fatalf("fixture: GETs=%d points=%d", gets, len(before.Value))
	}
	for i := 0; i < 32; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	after, gets := queryCold()
	if gets != 1 || !proto.Equal(before, after) {
		t.Fatalf("locality: GETs=%d, unchanged=%v", gets, proto.Equal(before, after))
	}
	var prefix []blob
	if err = e.indexRange(ctx, e.state.Roots["canonical.short"][0], 0, math.MaxInt64, &prefix); err != nil {
		t.Fatal(err)
	}
	for i := 5; i < 10; i++ {
		appendBlock(i)
	}
	for i := 0; i < 32; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var refs []blob
	if err = e.indexRange(ctx, e.state.Roots["canonical.short"][0], 0, math.MaxInt64, &refs); err != nil {
		t.Fatal(err)
	}
	for i, ref := range prefix {
		if refs[i] != ref {
			t.Fatal("appending data rewrote sealed historical prefix")
		}
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	opts.WALDirectory = t.TempDir()
	recovered, err := Open(ctx, s, opts, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	r, err := recovered.Query(ctx, "canonical.short", req)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Value) != 10*maxDataBlockRecords {
		t.Fatalf("recovery lost samples: %d", len(r.Value))
	}
}

type countedRangeGCStore struct {
	*rangeGCStore
	gets int64
}

func (s *countedRangeGCStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if len(key) >= 5 && key[:5] == "data/" {
		s.rangeGCStore.mu.Lock()
		s.gets++
		s.rangeGCStore.mu.Unlock()
	}
	return s.rangeGCStore.GetRange(ctx, key, offset, length)
}

func TestEncodedMaintenanceRangesCoalesceAndVerifyEveryBlock(t *testing.T) {
	ctx := context.Background()
	s := &countedRangeGCStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}}
	e := &Engine{store: s}
	// Use a normal engine's registry for store instrumentation.
	opened, err := Open(ctx, s, Options{WALDirectory: t.TempDir()}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	e.metrics = opened.metrics
	p, err := newPack("data")
	if err != nil {
		t.Fatal(err)
	}
	var refs []blob
	for i := 0; i < 8; i++ {
		b, err := encode(fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, p.add(b))
	}
	absent := ""
	if _, err = s.Put(ctx, p.key, p.buf.Bytes(), &absent); err != nil {
		t.Fatal(err)
	}
	// Shuffle logical order independently of physical offset.
	refs[0], refs[7] = refs[7], refs[0]
	s.gets = 0
	out, err := e.fetchEncoded(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	if s.gets != 1 || len(out) != 8 {
		t.Fatalf("ranges were not coalesced: %d", s.gets)
	}
	for i, b := range out {
		var value string
		if err = decode(b, &value); err != nil {
			t.Fatal(err)
		}
		want := i
		if i == 0 {
			want = 7
		}
		if i == 7 {
			want = 0
		}
		if value != fmt.Sprint(want) {
			t.Fatal("lost logical order")
		}
	}
	s.mu.Lock()
	s.objects[p.key][refs[3].Offset] ^= 1
	s.mu.Unlock()
	if _, err = e.fetchEncoded(ctx, refs); err == nil {
		t.Fatal("accepted corrupt block in coalesced range")
	}
}

func TestFailedLocalityJobDoesNotLoseItsSelection(t *testing.T) {
	ctx := context.Background()
	s := &prefixFailureStore{gcStore: &gcStore{memoryStore: newStore()}}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.Compaction.ObjectBytes = 4 << 20
	e, err := Open(ctx, s, opts, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < 5; i++ {
		points := make([]hta.Point, maxDataBlockRecords)
		for j := range points {
			points[j] = hta.Point{Time: int64(i*maxDataBlockRecords + j + 1), Value: float64(j)}
		}
		if err = e.Ingest(ctx, "x", chunk(points...)); err != nil {
			t.Fatal(err)
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	previousGrace := compactionAbortGrace
	compactionAbortGrace = 0
	defer func() { compactionAbortGrace = previousGrace }()
	s.prefix = "data/compact-"
	// Merges of small aggregate blocks may be selected first. Force locality
	// priority, then verify abort/recovery leaves its raw prefix selectable.
	e.compactionCompletions = 3
	if err = e.CompactOnce(ctx); err == nil {
		t.Fatal("failed upload accepted")
	}
	s.prefix = ""
	if err = e.recoverCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	refs := streamBlocks(t, e, "x", 0)
	keys := make(map[string]bool)
	for _, ref := range refs {
		keys[ref.Key] = true
	}
	if len(keys) != 1 {
		t.Fatalf("aborted selection was lost: %d raw source objects", len(keys))
	}
}
