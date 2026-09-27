package engine

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
)

type gcStore struct {
	*memoryStore
	deleteFail bool
	deleted    []string
}

func (s *gcStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteFail {
		return fmt.Errorf("delete unavailable")
	}
	delete(s.objects, key)
	delete(s.versions, key)
	s.deleted = append(s.deleted, key)
	return nil
}

func TestGCReferencesAcrossCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	e := openTest(t, s, t.TempDir())
	for batch := 0; batch < 80; batch++ {
		points := make([]hta.Point, 80)
		for i := range points {
			points[i] = hta.Point{Time: int64(batch*80+i+1) * 100, Value: float64(i)}
		}
		ingest(t, e, points...)
		if err := e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		expected := make(map[string]int64)
		var walk func(blob)
		walk = func(b blob) {
			expected[b.Key]++
			n, err := e.readNode(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			for _, edge := range n.Entries {
				if n.Leaf {
					expected[edge.Blob.Key]++
				} else {
					walk(edge.Blob)
				}
			}
		}
		for _, levels := range e.state.Roots {
			for _, root := range levels {
				walk(root)
			}
		}
		if len(expected) != len(e.objectRefs) {
			t.Fatalf("reference count map differs: %v vs %v", expected, e.objectRefs)
		}
		for key, count := range expected {
			if e.objectRefs[key] != count {
				t.Fatalf("%s: %d vs %d", key, count, e.objectRefs[key])
			}
		}
	}
	if len(s.deleted) == 0 {
		t.Fatal("no fully retired index objects deleted")
	}
	e.Close()
	recovered := openTest(t, s, t.TempDir())
	query(t, recovered, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 640000})
}

// A standalone aggregate pack can lose its last block; a mixed data pack
// remains live. Seed a published aggregate-only checkpoint to isolate this.
func TestGCLastDataBlockAndReaderPin(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	e := openTest(t, s, t.TempDir())
	data, _ := newPack("data")
	b, err := encode([]hta.Record{{Time: 100, Level: 100, Repeat: 1, Aggregate: hta.Aggregate{Count: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	ref := data.add(b)
	index, _ := newPack("index")
	root, err := e.appendIndex(ctx, blob{}, []indexEntry{{First: 100, Last: 100, Blob: ref, Records: 1}}, index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(ctx, data.key, data.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(ctx, index.key, index.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	e.state.Roots["x"][100] = root
	b, err = encode(e.state)
	if err != nil {
		t.Fatal(err)
	}
	e.version, err = s.Put(ctx, "manifest", b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.initializeGC(ctx); err != nil {
		t.Fatal(err)
	}
	// Pin an older reader and append a new aggregate (no new raw data).
	e.readers = 1
	e.pending.add("x", hta.Record{Time: 200, Level: 100, Repeat: 1, Aggregate: hta.Aggregate{Count: 1}})
	e.sequence++
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !e.garbage[data.key] {
		t.Fatal("last-block pack not queued")
	}
	if _, _, err = s.Get(ctx, data.key); err != nil {
		t.Fatal("reader's old pack deleted")
	}
	// Deletion failure does not fail the committed checkpoint; restart retains
	// the durable queue and blocks deletion while pinned again.
	s.deleteFail = true
	e.readers = 0
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	e.Close()
	recovered := openTest(t, s, t.TempDir())
	if !recovered.garbage[data.key] {
		t.Fatal("deletion queue lost on restart")
	}
	s.deleteFail = false
	if err = recovered.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Get(ctx, data.key); err == nil {
		t.Fatal("fully dead data pack retained")
	}
	var refs []blob
	if err = recovered.indexRange(ctx, recovered.state.Roots["x"][100], 0, math.MaxInt64, &refs); err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatal(refs)
	}
	rows, err := recovered.readBlob(ctx, refs[0])
	if err != nil {
		t.Fatal(err)
	}
	var records []hta.Record
	if err = decode(rows, &records); err != nil || len(records) != 2 {
		t.Fatalf("replacement lost records: %v %v", records, err)
	}
}

type gcQueryKey struct{}
type gatedGCStore struct {
	*gcStore
	started chan struct{}
	resume  chan struct{}
}

func (s *gatedGCStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	if ctx.Value(gcQueryKey{}) != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
		select {
		case <-s.resume:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return s.memoryStore.Get(ctx, key)
}
func TestGCConcurrentQueryPinsOldIndex(t *testing.T) {
	ctx := context.Background()
	s := &gatedGCStore{gcStore: &gcStore{memoryStore: newStore()}, started: make(chan struct{}, 1), resume: make(chan struct{})}
	e := openTest(t, s, t.TempDir())
	ingest(t, e, hta.Point{Time: 100, Value: 1}, hta.Point{Time: 200, Value: 2})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	queryCtx, cancel := context.WithTimeout(context.WithValue(ctx, gcQueryKey{}, true), 10*time.Second)
	defer cancel()
	go func() {
		_, err := e.Query(queryCtx, "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 200})
		result <- err
	}()
	select {
	case <-s.started:
	case <-queryCtx.Done():
		t.Fatal("query never reached store")
	}
	ingest(t, e, hta.Point{Time: 300, Value: 3}, hta.Point{Time: 400, Value: 4})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.garbage) == 0 {
		t.Fatal("old index not retired")
	}
	if len(s.deleted) != 0 {
		t.Fatal("snapshot objects deleted during query")
	}
	close(s.resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.garbage) != 0 {
		t.Fatal("objects remained pinned after query completed")
	}
}

func TestGCFailedPublicationPreservesOldObjects(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	e := openTest(t, s, t.TempDir())
	ingest(t, e, hta.Point{Time: 100, Value: 1}, hta.Point{Time: 200, Value: 2})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	old := e.state.Roots["x"][100]
	ingest(t, e, hta.Point{Time: 300, Value: 3}, hta.Point{Time: 400, Value: 4})
	s.fail = "manifest"
	if err := e.Flush(ctx); err == nil {
		t.Fatal("failed manifest commit succeeded")
	}
	if len(s.deleted) != 0 || len(e.garbage) != 0 || e.state.Roots["x"][100] != old {
		t.Fatal("unpublished checkpoint retired old objects")
	}
	if _, _, err := s.Get(ctx, old.Key); err != nil {
		t.Fatal(err)
	}
	s.fail = ""
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.deleted) == 0 {
		t.Fatal("successful checkpoint did not retire old index")
	}
}
