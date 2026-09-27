package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

// indexReadStore counts full-object and ranged index reads.
type indexReadStore struct {
	*gcStore
	mu    sync.Mutex
	reads int
}

func (s *indexReadStore) count(key string) {
	if strings.HasPrefix(key, "index/") {
		s.mu.Lock()
		s.reads++
		s.mu.Unlock()
	}
}
func (s *indexReadStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	s.count(key)
	return s.gcStore.Get(ctx, key)
}
func (s *indexReadStore) take() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.reads
	s.reads = 0
	return n
}

func TestFlushReusesPinnedIndexTails(t *testing.T) {
	ctx := context.Background()
	configs := map[string]hta.Config{}
	for i := 0; i < 100; i++ {
		configs[fmt.Sprintf("m%03d", i)] = hta.Config{IntervalMin: 100, IntervalMax: 100000, IntervalFactor: 10}
	}
	s := &indexReadStore{gcStore: &gcStore{memoryStore: newStore()}}
	options := maintenanceOptions(t.TempDir(), true)
	options.CheckpointAppendOnlyAggregates = true
	options.CompactionOptions.MergeCooldownSeconds = 0
	e, err := Open(ctx, s, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// Tiny shared cache capacity is not the mechanism under test.
	e.sharedNodes = nil
	batch := 0
	flush := func() int {
		s.take()
		for name := range configs {
			points := make([]hta.Point, 20)
			for i := range points {
				points[i] = hta.Point{Time: int64(batch*20+i+1) * 100, Value: float64(i % 7)}
			}
			if err := e.Ingest(ctx, name, chunk(points...)); err != nil {
				t.Fatal(err)
			}
		}
		batch++
		if err := e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		return s.take()
	}
	flush()
	// Enough flushes to split rightmost leaves and grow internal pages.
	for i := 0; i < 70; i++ {
		if n := flush(); n != 0 {
			t.Fatalf("flush %d read %d index pages", i+1, n)
		}
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: int64(batch*20+1) * 100, IntervalMax: 1000}
	query := func(t *testing.T, e *Engine, req *metricq.HistoryRequest) *metricq.HistoryResponse {
		t.Helper()
		response, err := e.Query(ctx, "m042", req)
		if err != nil || len(response.TimeDelta) == 0 {
			t.Fatalf("query: %v %v", response, err)
		}
		return response
	}
	want := query(t, e, req)

	// Compaction replaces roots. Only replaced paths are read once again.
	for i := 0; i < 10; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !proto.Equal(want, query(t, e, req)) {
		t.Fatal("compaction changed response")
	}
	s.take()
	if n := flush(); n == 0 {
		t.Log("no compacted root was touched by the next flush")
	}
	if n := flush(); n != 0 {
		t.Fatalf("second flush after compaction read %d index pages", n)
	}
	checkCatalog(t, e)

	// A restart has no pinned paths and remains correct.
	want = query(t, e, req)
	drain(t, e)
	e.Close()
	options.WALDirectory = t.TempDir()
	recovered, err := Open(ctx, s, options, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if !proto.Equal(want, query(t, recovered, req)) {
		t.Fatal("restart changed response")
	}
}
