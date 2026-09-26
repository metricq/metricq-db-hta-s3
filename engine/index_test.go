package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/metricq/metricq-db-hta-go/storage"

	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
)

func TestBulkIndexSnapshotsAndReachability(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	e := openTest(t, s, t.TempDir())
	var root blob
	var roots []blob
	var totals []int
	total := 0
	for _, count := range []int{63, 2, 4030, 65, 300} {
		p, err := newPack("index")
		if err != nil {
			t.Fatal(err)
		}
		items := make([]indexEntry, count)
		for i := range items {
			time := int64(total+i+1) * 100
			items[i] = indexEntry{First: time, Last: time + 50, Blob: blob{Key: fmt.Sprintf("data/%d", total+i)}}
		}
		root, err = e.appendIndex(ctx, root, items, p)
		if err != nil {
			t.Fatal(err)
		}
		empty := ""
		if _, err := s.Put(ctx, p.key, p.buf.Bytes(), &empty); err != nil {
			t.Fatal(err)
		}
		total += count
		roots, totals = append(roots, root), append(totals, total)
		// Every encoded page must be reachable; intermediate COW versions would
		// inflate the pack without contributing to the published index.
		var reachable int64
		var walk func(blob)
		walk = func(ptr blob) {
			if ptr.Key != p.key {
				return
			}
			reachable += ptr.Length
			n, err := e.readNode(ctx, ptr)
			if err != nil {
				t.Fatal(err)
			}
			if !n.Leaf {
				for _, edge := range n.Entries {
					walk(edge.Blob)
				}
			}
		}
		walk(root)
		if reachable != int64(p.buf.Len()) {
			t.Fatalf("unreachable index bytes: %d of %d", int64(p.buf.Len())-reachable, p.buf.Len())
		}
		for i, oldRoot := range roots {
			var refs []blob
			if err := e.indexRange(ctx, oldRoot, 0, math.MaxInt64, &refs); err != nil {
				t.Fatal(err)
			}
			if len(refs) != totals[i] {
				t.Fatalf("snapshot %d: got %d entries, want %d", i, len(refs), totals[i])
			}
			for j, ref := range refs {
				if ref.Key != fmt.Sprintf("data/%d", j) {
					t.Fatalf("snapshot %d entry %d: %s", i, j, ref.Key)
				}
			}
		}
	}
	p, _ := newPack("index")
	if _, err := e.appendIndex(ctx, root, []indexEntry{{First: 1, Last: 2}}, p); err == nil {
		t.Fatal("accepted backwards index append")
	}
}

func TestIndexTailReplacementSplits(t *testing.T) {
	for _, count := range []int{63, 64, 4096} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx := context.Background()
			s := newStore()
			e := openTest(t, s, t.TempDir())
			p, _ := newPack("index")
			items := make([]indexEntry, count)
			for i := range items {
				items[i] = indexEntry{First: int64(i) * 100, Last: int64(i)*100 + 50, Blob: blob{Key: fmt.Sprint(i)}}
			}
			root, err := e.appendIndex(ctx, blob{}, items, p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
				t.Fatal(err)
			}
			p, _ = newPack("index")
			replacement := []indexEntry{
				{First: items[count-1].First, Last: items[count-1].Last, Blob: blob{Key: "replacement"}},
				{First: int64(count) * 100, Last: int64(count)*100 + 50, Blob: blob{Key: "new"}},
			}
			updated, err := e.updateIndex(ctx, root, replacement, p, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
				t.Fatal(err)
			}
			for _, r := range []blob{root, updated} {
				var refs []blob
				if err := e.indexRange(ctx, r, 0, math.MaxInt64, &refs); err != nil {
					t.Fatal(err)
				}
				want := count
				if r == updated {
					want++
				}
				if len(refs) != want {
					t.Fatalf("got %d refs, want %d", len(refs), want)
				}
				for i, ref := range refs {
					key := fmt.Sprint(i)
					if r == updated && i == count-1 {
						key = "replacement"
					}
					if r == updated && i == count {
						key = "new"
					}
					if ref.Key != key {
						t.Fatalf("entry %d: got %s, want %s", i, ref.Key, key)
					}
				}
			}
		})
	}
}

type countedRangeStore struct {
	*memoryStore
	ranges      atomic.Int64
	dataRanges  atomic.Int64
	dataBytes   atomic.Int64
	indexRanges atomic.Int64
}

type failPackStore struct {
	*memoryStore
	failPrefix string
}

func (s *failPackStore) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	if strings.HasPrefix(key, s.failPrefix) {
		return "", fmt.Errorf("injected %s write failure", s.failPrefix)
	}
	return s.memoryStore.Put(ctx, key, b, expected)
}

func TestV2FailedPackKeepsAcknowledgedWAL(t *testing.T) {
	for _, prefix := range []string{"data/", "index/"} {
		t.Run(prefix, func(t *testing.T) {
			s := &failPackStore{memoryStore: newStore(), failPrefix: prefix}
			dir := t.TempDir()
			e := openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 100, Value: 7})
			if err := e.Flush(context.Background()); err == nil || e.wal.size == 0 {
				t.Fatalf("failed pack write discarded WAL: %v", err)
			}
			e.Close()
			s.failPrefix = "never/"
			e = openTest(t, s, dir)
			if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE}); len(got.Value) != 1 || got.Value[0] != 7 {
				t.Fatalf("ACKed value lost on replay: %v", got)
			}
			if err := e.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			e.Close()
			e = openTest(t, s, t.TempDir())
			if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 200}); len(got.Value) != 1 || got.Value[0] != 7 {
				t.Fatalf("ACKed value lost after checkpoint: %v", got)
			}
		})
	}
}

func TestV2RejectsOldManifest(t *testing.T) {
	s := newStore()
	b, err := encode(struct{ Version int }{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	empty := ""
	if _, err := s.Put(context.Background(), "manifest", b, &empty); err != nil {
		t.Fatal(err)
	}
	if e, err := Open(context.Background(), storage.Store(s), Options{WALDirectory: t.TempDir()}, testConfig, nil); err == nil {
		e.Close()
		t.Fatal("accepted an old manifest")
	}
}

func (s *countedRangeStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.ranges.Add(1)
	if strings.HasPrefix(key, "data/") {
		s.dataRanges.Add(1)
		s.dataBytes.Add(length)
	} else if strings.HasPrefix(key, "index/") {
		s.indexRanges.Add(1)
	}
	b, _, err := s.memoryStore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 || offset > int64(len(b)) || length > int64(len(b))-offset {
		return nil, fmt.Errorf("invalid range")
	}
	return b[offset : offset+length], nil
}

func TestLargeCheckpointUsesSmallDataRanges(t *testing.T) {
	const points = 70 * maxDataBlockRecords
	s := &countedRangeStore{memoryStore: newStore()}
	e, err := Open(context.Background(), s, Options{WALDirectory: t.TempDir(), BuilderHard: 64 << 20}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	chunk := &metricq.DataChunk{}
	for i := 1; i <= points; i++ {
		chunk.TimeDelta = append(chunk.TimeDelta, 100)
		chunk.Value = append(chunk.Value, float64(i))
	}
	if err := e.Ingest(context.Background(), "x", chunk); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	root, err := e.readNode(context.Background(), e.state.Roots["x"][0])
	if err != nil || root.Leaf {
		t.Fatalf("raw index did not split: %v", err)
	}
	s.ranges.Store(0)
	s.dataRanges.Store(0)
	s.dataBytes.Store(0)
	start := int64(35 * maxDataBlockRecords * 100)
	got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: start, EndTime: start + 1000})
	if len(got.Value) != 10 || got.Value[0] != float64(start/100) {
		t.Fatalf("wrong answer near middle of split index: %v", got)
	}
	if s.dataRanges.Load() > 3 || s.dataBytes.Load() > 100<<10 || s.ranges.Load() > 12 {
		t.Fatalf("small history request read too much: %d data ranges, %d bytes, %d total ranges", s.dataRanges.Load(), s.dataBytes.Load(), s.ranges.Load())
	}
	s.indexRanges.Store(0)
	// A fresh, non-overlapping time range still misses the shared data-block
	// cache, so the index pages guarding it are the only thing that must stay
	// cached.
	query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: start + 1000, EndTime: start + 2000})
	if s.indexRanges.Load() != 0 {
		t.Fatalf("immutable index pages fetched again: %d", s.indexRanges.Load())
	}
}

func TestSharedDataBlockCacheAvoidsRepeatFetches(t *testing.T) {
	const points = 3 * maxDataBlockRecords
	s := &countedRangeStore{memoryStore: newStore()}
	e, err := Open(context.Background(), s, Options{WALDirectory: t.TempDir(), BuilderHard: 64 << 20}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	chunk := &metricq.DataChunk{}
	for i := 1; i <= points; i++ {
		chunk.TimeDelta = append(chunk.TimeDelta, 100)
		chunk.Value = append(chunk.Value, float64(i))
	}
	if err := e.Ingest(context.Background(), "x", chunk); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: int64(points) * 100}
	first := query(t, e, req)
	if s.dataRanges.Load() == 0 {
		t.Fatal("first query did not read any data blocks")
	}
	s.ranges.Store(0)
	s.dataRanges.Store(0)
	// Two overlapping viewers (or an auto-refreshing dashboard) requesting the
	// same window must hit the shared block cache, not the store again.
	second := query(t, e, req)
	if s.dataRanges.Load() != 0 {
		t.Fatalf("repeat query re-fetched %d data blocks instead of using the shared cache", s.dataRanges.Load())
	}
	if len(second.Value) != len(first.Value) || second.Value[0] != first.Value[0] {
		t.Fatalf("cached query returned different data: %v vs %v", second, first)
	}
}

func TestV2IndexKeepsOldAndRecentQueriesBounded(t *testing.T) {
	ctx := context.Background()
	s := &countedRangeStore{memoryStore: newStore()}
	dir := t.TempDir()
	e := openTest(t, s, dir)
	var early *metricq.HistoryResponse
	var firstManifestBytes int
	for i := int64(1); i <= 140; i++ {
		ingest(t, e, hta.Point{Time: i * 100, Value: float64(i)})
		if err := e.Flush(ctx); err != nil {
			t.Fatalf("checkpoint %d: %v", i, err)
		}
		if i == 1 {
			firstManifestBytes = len(s.objects["manifest"])
			early = query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 200})
		}
	}
	if len(s.objects["manifest"]) > firstManifestBytes+1000 {
		t.Fatalf("root grew with checkpoints: first=%d final=%d", firstManifestBytes, len(s.objects["manifest"]))
	}
	if e.state.Version != 2 || len(e.state.Roots["x"]) == 0 {
		t.Fatal("v2 roots were not published")
	}
	e.Close()
	e = openTest(t, s, dir)
	for _, tc := range []struct {
		start, end int64
		want       float64
	}{
		{100, 200, 1},
		{7000, 7100, 70},
		{13900, 14100, 139},
	} {
		s.ranges.Store(0)
		s.dataRanges.Store(0)
		got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: tc.start, EndTime: tc.end})
		if len(got.Value) == 0 || got.Value[0] != tc.want {
			t.Fatalf("query %d: %v", tc.start, got)
		}
		if s.ranges.Load() > 20 || s.dataRanges.Load() > 4 {
			t.Fatalf("query %d scanned history: %d index/data range reads", tc.start, s.ranges.Load())
		}
	}
	if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 200}); len(got.Value) != len(early.Value) || got.Value[0] != early.Value[0] {
		t.Fatalf("old history changed after index splits: %v vs %v", got, early)
	}
}
