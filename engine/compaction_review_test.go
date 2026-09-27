//go:build review

package engine

// Diagnostic probes for compaction review. Logged observations are not desired
// behavior or regression assertions; keep response/error checks meaningful.
import (
	"context"
	"crypto/sha256"
	"fmt"
	"google.golang.org/protobuf/proto"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
)

func TestReviewCardinalitySelection(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	configs := map[string]hta.Config{}
	for i := 0; i < 150; i++ {
		configs[fmt.Sprintf("m%03d", i)] = hta.Config{IntervalMin: 1000000, IntervalMax: 10000000, IntervalFactor: 10}
	}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), true), configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for batch := 0; batch < 4; batch++ {
		for name := range configs {
			if err = e.Ingest(ctx, name, chunk(hta.Point{Time: int64(batch + 1), Value: 1})); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int {
		var refs []blob
		if err := e.indexRange(ctx, e.state.Roots["m000"][0], 0, math.MaxInt64, &refs); err != nil {
			t.Fatal(err)
		}
		return len(refs)
	}
	before := count()
	generation := e.state.Generation
	for i := 0; i < 20; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("150 metrics, 4 flushes, MaxBlocks=128, 20 passes: m000 blocks %d -> %d, generation %d -> %d", before, count(), generation, e.state.Generation)
}

func TestReviewAggregateCandidate(t *testing.T) {
	o := ObjectInfo{Key: "data/test", Size: 100, LiveBytes: 100, Blocks: []BlockInfo{{Metric: "x", Level: 100, Entry: indexEntry{Records: 2}}}}
	t.Logf("dense object with small aggregate block candidate=%q", candidateKey(o))
}

func reviewSynthetic(t *testing.T, n int) (*Engine, []indexEntry) {
	t.Helper()
	ctx := context.Background()
	s := newStore()
	e := openTest(t, s, t.TempDir())
	p, _ := newPack("data")
	var items []indexEntry
	for i := 0; i < n; i++ {
		stamp := int64(i+1) * 100
		b, err := encode([]hta.Record{{Time: stamp, Value: float64(i)}})
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, indexEntry{First: stamp, Last: stamp, Blob: p.add(b), Records: 1})
	}
	if _, err := s.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	idx, _ := newPack("index")
	root, err := e.appendIndex(ctx, blob{}, items, idx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(ctx, idx.key, idx.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	e.state.Roots["x"][0] = root
	e.options.Compaction.MergeSmallBlocks = true
	e.options.Compaction.BytesPerSecond = 1 << 30
	return e, items
}

func TestReviewNonadjacentBatch(t *testing.T) {
	e, items := reviewSynthetic(t, 4)
	job := CompactionJob{ID: "review"}
	for _, i := range []int{0, 1, 3} {
		job.Inputs = append(job.Inputs, BlockInfo{Metric: "x", Entry: items[i]})
	}
	repl, _, err := e.copyJob(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	drops := 0
	for _, r := range repl {
		if r.Drop {
			drops++
		}
	}
	t.Logf("selected blocks [0,1,3], adjacent [0,1] exists: merged-away blocks=%d", drops)
}

func TestReviewIndexRootCollapse(t *testing.T) {
	e, items := reviewSynthetic(t, 130)
	ctx := context.Background()
	job := CompactionJob{ID: "review"}
	for _, item := range items {
		job.Inputs = append(job.Inputs, BlockInfo{Metric: "x", Entry: item})
	}
	repl, packs, err := e.copyJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	_ = packs
	p, _ := newPack("index")
	pages := 0
	edges, err := e.replaceHistorical(ctx, e.state.Roots["x"][0], repl, p, make(map[blob]bool), &pages)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.store.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	root, err := e.readNode(ctx, edges[0].Blob)
	if err != nil {
		t.Fatal(err)
	}
	var refs []blob
	if err = e.indexRange(ctx, edges[0].Blob, 0, math.MaxInt64, &refs); err != nil {
		t.Fatal(err)
	}
	t.Logf("130 raw blocks -> %d data block(s), resulting root leaf=%v, root children=%d", len(refs), root.Leaf, len(root.Entries))
}

type reviewGateStore struct {
	*gcStore
	entered, release chan struct{}
}

func (s *reviewGateStore) Put(ctx context.Context, k string, b []byte, v *string) (string, error) {
	if strings.HasPrefix(k, "jobs/") {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.gcStore.Put(ctx, k, b, v)
}
func TestReviewJobPutBlocksIngest(t *testing.T) {
	ctx := context.Background()
	base := &gcStore{memoryStore: newStore()}
	s := &reviewGateStore{gcStore: base, entered: make(chan struct{}), release: make(chan struct{})}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), true), testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 3)
	done := make(chan error, 1)
	go func() {
		job, err := e.reserveCompaction(ctx)
		if job.ID != "" {
			e.mu.Lock()
			e.unpin(job.Generation)
			e.mu.Unlock()
		}
		done <- err
	}()
	select {
	case <-s.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no job PUT")
	}
	ack := make(chan error, 1)
	go func() { ack <- e.Ingest(ctx, "x", chunk(hta.Point{Time: 12100, Value: 1})) }()
	blocked := false
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		blocked = true
	}
	close(s.release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if blocked {
		if err = <-ack; err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("ingest blocked by jobs/ PUT for >=100ms: %v", blocked)
}

type reviewCountStore struct {
	countMu   sync.Mutex
	reads     atomic.Int64
	readBytes atomic.Int64
	*gcStore
	dataBytes, indexBytes, metaBytes int64
	puts                             int
}

func (s *reviewCountStore) Put(ctx context.Context, k string, b []byte, v *string) (string, error) {
	s.countMu.Lock()
	s.puts++
	if strings.HasPrefix(k, "data/") {
		s.dataBytes += int64(len(b))
	} else if strings.HasPrefix(k, "index/") {
		s.indexBytes += int64(len(b))
	} else {
		s.metaBytes += int64(len(b))
	}
	s.countMu.Unlock()
	return s.gcStore.Put(ctx, k, b, v)
}
func (s *reviewCountStore) GetRange(ctx context.Context, k string, offset, length int64) ([]byte, error) {
	s.reads.Add(1)
	s.readBytes.Add(length)
	b, _, err := s.Get(ctx, k)
	if err != nil {
		return nil, err
	}
	return b[offset : offset+length], nil
}

func TestReviewTailCost(t *testing.T) {
	ctx := context.Background()
	s := &reviewCountStore{gcStore: &gcStore{memoryStore: newStore()}}
	options := maintenanceOptions(t.TempDir(), true)
	options.AppendOnlyAggregates = os.Getenv("METRICQ_REVIEW_APPEND_ONLY") == "1"
	e, err := Open(ctx, s, options, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var ingestTime, flushTime time.Duration
	for batch := 0; batch < 100; batch++ {
		points := make([]hta.Point, 10)
		for j := range points {
			points[j] = hta.Point{Time: int64(batch*10+j+1) * 100, Value: float64(j)}
		}
		start := time.Now()
		ingest(t, e, points...)
		ingestTime += time.Since(start)
		start = time.Now()
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		flushTime += time.Since(start)
	}
	var refs []blob
	if err = e.indexRange(ctx, e.state.Roots["x"][100], 0, math.MaxInt64, &refs); err != nil {
		t.Fatal(err)
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 100000, IntervalMax: 100}
	e.sharedNodes = newIndexPageCache()
	e.sharedBlocks = newDataBlockCache()
	s.reads.Store(0)
	s.readBytes.Store(0)
	queryStarted := time.Now()
	response := query(t, e, req)
	queryTime := time.Since(queryStarted)
	responseBytes, _ := proto.Marshal(response)
	t.Logf("cold FLEX level100: reads=%d compressedReadBytes=%d query=%v responseSHA256=%x", s.reads.Load(), s.readBytes.Load(), queryTime, sha256.Sum256(responseBytes))
	t.Logf("100 flushes/1000 points: ingest=%v flush=%v dataWritten=%d indexWritten=%d metadataWritten=%d PUTs=%d level100blocks=%d responseTimes=%d", ingestTime, flushTime, s.dataBytes, s.indexBytes, s.metaBytes, s.puts, len(refs), len(response.TimeDelta))
	if options.AppendOnlyAggregates {
		for i := 0; i < 40; i++ {
			if err = e.CompactOnce(ctx); err != nil {
				t.Fatal(err)
			}
		}
		e.sharedNodes = newIndexPageCache()
		e.sharedBlocks = newDataBlockCache()
		s.reads.Store(0)
		s.readBytes.Store(0)
		compacted := query(t, e, req)
		if !proto.Equal(response, compacted) {
			t.Fatal("compacted response changed")
		}
		t.Logf("after consolidation: reads=%d compressedReadBytes=%d level100blocks=%d", s.reads.Load(), s.readBytes.Load(), len(streamBlocks(t, e, "x", 100)))
	}

}

func TestReviewCopyOnlyCache(t *testing.T) {
	ctx := context.Background()
	s := &reviewCountStore{gcStore: &gcStore{memoryStore: newStore()}}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), false), testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 6)
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 24000}
	want := query(t, e, req)
	s.reads.Store(0)
	query(t, e, req)
	warm := s.reads.Load()
	if err = e.CompactOnce(ctx); err != nil {
		t.Fatal(err)
	}
	s.reads.Store(0)
	got := query(t, e, req)
	if !proto.Equal(want, got) {
		t.Fatal("response changed")
	}
	t.Logf("warm raw query reads before copy-only compaction=%d, first query afterward=%d", warm, s.reads.Load())
}
