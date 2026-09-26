package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

func (s *gcStore) List(_ context.Context, prefix, token string, limit int32) ([]string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) && key > token {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > int(limit) {
		return keys[:limit], keys[limit-1], nil
	}
	return keys, "", nil
}
func maintenanceOptions(dir string, merge bool) Options {
	return Options{WALDirectory: dir, ObjectTarget: 1 << 20, BuilderHard: 32 << 20, BackgroundMaintenance: true, Compaction: CompactionOptions{Enabled: true, DeadFraction: .05, MaxJobBytes: 4 << 20, MaxBlocks: 128, ObjectBytes: 64 << 10, BytesPerSecond: 1 << 30, MergeSmallBlocks: merge}}
}
func maintenanceEngine(t *testing.T, s *gcStore, dir string, merge bool) *Engine {
	t.Helper()
	e, err := Open(context.Background(), s, maintenanceOptions(dir, merge), testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}
func fillCompaction(t *testing.T, e *Engine, n int) {
	for batch := 0; batch < n; batch++ {
		points := make([]hta.Point, 40)
		for i := range points {
			points[i] = hta.Point{Time: int64(batch*40+i+1) * 100, Value: float64(i % 7)}
		}
		ingest(t, e, points...)
		if err := e.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func drain(t *testing.T, e *Engine) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		if e.state.TrashHead == e.state.TrashComplete && e.state.TrashCleanup == "" && len(e.state.TrashCleanups) == 0 {
			return
		}
		if err := e.Reclaim(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("garbage did not drain")
}
func checkCatalog(t *testing.T, e *Engine) {
	t.Helper()
	ctx := context.Background()
	refs := make(map[blob]BlockInfo)
	var walk func(string, int64, blob)
	walk = func(metric string, level int64, ref blob) {
		n, err := e.readNode(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		refs[ref] = BlockInfo{Metric: metric, Level: level, Index: true, Entry: indexEntry{First: n.Entries[0].First, Last: n.Entries[len(n.Entries)-1].Last, Blob: ref}}
		for _, edge := range n.Entries {
			if n.Leaf {
				refs[edge.Blob] = BlockInfo{Metric: metric, Level: level, Entry: edge}
			} else {
				walk(metric, level, edge.Blob)
			}
		}
	}
	for metric, levels := range e.state.Roots {
		for level, root := range levels {
			walk(metric, level, root)
		}
	}
	var live, stored int64
	err := e.catalogWalk(ctx, e.state.Catalog, 10000, func(o ObjectInfo) bool {
		var bytes int64
		for _, b := range o.Blocks {
			expected, ok := refs[b.Entry.Blob]
			if !ok {
				t.Fatalf("catalog contains unreachable blob: %+v", b)
			}
			if b.Metric != expected.Metric || b.Level != expected.Level || b.Index != expected.Index || b.Entry.First != expected.Entry.First || b.Entry.Last != expected.Entry.Last {
				t.Fatalf("catalog descriptor differs: %+v vs %+v", b, expected)
			}
			delete(refs, b.Entry.Blob)
			bytes += b.Entry.Blob.Length
		}
		if bytes != o.LiveBytes {
			t.Fatal("live byte count differs")
		}
		live += bytes
		stored += o.Size
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 || live != e.state.LiveObjectBytes || stored != e.state.StoredObjectBytes {
		t.Fatalf("catalog accounting differs: missing=%d live=%d/%d stored=%d/%d", len(refs), live, e.state.LiveObjectBytes, stored, e.state.StoredObjectBytes)
	}
}
func TestCompactionKeepsLiveWALAndQueryResults(t *testing.T) {
	for _, merge := range []bool{false, true} {
		t.Run(fmt.Sprint(merge), func(t *testing.T) {
			ctx := context.Background()
			s := &gcStore{memoryStore: newStore()}
			dir := t.TempDir()
			e := maintenanceEngine(t, s, dir, merge)
			fillCompaction(t, e, 12)
			if len(s.deleted) != 0 {
				t.Fatal("flush performed background deletion")
			}
			ingest(t, e, hta.Point{Time: 48100, Value: 77}, hta.Point{Time: 48200, Value: 88})
			beforeSequence, head, walBytes := e.state.Sequence, e.sequence, e.wal.size
			requests := []*metricq.HistoryRequest{
				{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 48200},
				{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 150, EndTime: 47890, IntervalMax: 1000},
				{Type: metricq.HistoryRequest_AGGREGATE, StartTime: 150, EndTime: 47890},
				{Type: metricq.HistoryRequest_LAST_VALUE},
			}
			expected := make([]*metricq.HistoryResponse, len(requests))
			for i, r := range requests {
				expected[i] = query(t, e, r)
			}
			before := e.state.StoredObjectBytes - e.state.LiveObjectBytes
			if err := e.CompactOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if e.state.Sequence != beforeSequence || e.sequence != head || e.wal.size != walBytes {
				t.Fatal("compaction modified WAL checkpoint/head")
			}
			if e.state.CompactionJob.Key != "" {
				t.Fatal("job not committed")
			}
			if e.state.StoredObjectBytes-e.state.LiveObjectBytes >= before {
				t.Fatal("compaction did not reduce dead bytes")
			}
			for i, r := range requests {
				if !proto.Equal(expected[i], query(t, e, r)) {
					t.Fatalf("changed response %d", i)
				}
			}
			checkCatalog(t, e)
			drain(t, e)
			e.Close()
			recovered := maintenanceEngine(t, s, dir, merge)
			for i, r := range requests {
				if !proto.Equal(expected[i], query(t, recovered, r)) {
					t.Fatalf("restart changed response %d", i)
				}
			}
			checkCatalog(t, recovered)
		})
	}
}
func TestCompactionConcurrentFlushReplacesOnlyExactInputs(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), false)
	fillCompaction(t, e, 6)
	ctx := context.Background()
	job, err := e.reserveCompaction(ctx)
	if err != nil || job.ID == "" {
		t.Fatalf("reserve %v %+v", err, job)
	}
	defer func() { e.mu.Lock(); e.unpin(job.Generation); e.mu.Unlock() }()
	replacements, packs, err := e.copyJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	ingest(t, e, hta.Point{Time: 24100, Value: 5}, hta.Point{Time: 24200, Value: 6})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 24200}
	want := query(t, e, req)
	err = e.applyCompaction(ctx, job, replacements, packs)
	if err != nil {
		if err = e.abortCompaction(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !proto.Equal(want, query(t, e, req)) {
		t.Fatal("concurrent flush lost points")
	}
	checkCatalog(t, e)
}
func TestCompactionPinsOlderObjectsAndAllowsNewQueries(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), false)
	fillCompaction(t, e, 4)
	e.mu.Lock()
	generation := e.state.Generation
	e.pin(generation)
	e.mu.Unlock()
	if err := e.CompactOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := e.Reclaim(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// Newest retirement is protected by the old generation, despite newer queries.
	for i := 0; i < 10; i++ {
		query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
	}
	e.mu.Lock()
	e.unpin(generation)
	e.mu.Unlock()
	drain(t, e)
	checkCatalog(t, e)
}
func TestCompactionCrashRecoveryAndCleanup(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	dir := t.TempDir()
	e := maintenanceEngine(t, s, dir, false)
	fillCompaction(t, e, 4)
	ctx := context.Background()
	job, err := e.reserveCompaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, packs, err := e.copyJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	recovered := maintenanceEngine(t, s, dir, false)
	if err := recovered.recoverCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	recovered.mu.Lock()
	aborted, err := recovered.readJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	aborted.CleanupAfter = time.Now().Add(-time.Second).UnixNano()
	ref, err := recovered.writeJob(ctx, aborted)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneManifest(recovered.committed)
	next.Generation++
	next.CompactionJob = ref
	err = recovered.publishMaintenance(ctx, next)
	recovered.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.recoverCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	drain(t, recovered)
	for _, p := range packs {
		if _, _, err := s.Get(ctx, p.key); err == nil {
			t.Fatalf("orphan output remained %s", p.key)
		}
	}
	checkCatalog(t, recovered)
}

func TestCompactionCopiesCompressedBytesAndRebasesRawAppend(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), false)
	fillCompaction(t, e, 6)
	ctx := context.Background()
	job, err := e.reserveCompaction(ctx)
	if err != nil || job.ID == "" {
		t.Fatal(err)
	}
	// Restrict this persisted job to stable historical raw blocks. Appending new
	// raw blocks changes its root while retaining the exact old input addresses.
	var inputs []BlockInfo
	for _, b := range job.Inputs {
		if !b.Index && b.Level == 0 {
			inputs = append(inputs, b)
		}
	}
	if len(inputs) == 0 {
		t.Fatal("no raw inputs")
	}
	job.Inputs = inputs
	e.mu.Lock()
	ref, err := e.writeJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneManifest(e.committed)
	next.Generation++
	next.CompactionJob = ref
	err = e.publishMaintenance(ctx, next)
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	replacements, packs, err := e.copyJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range job.Inputs {
		r := replacements[input.Entry.Blob]
		if input.Entry.Blob.Hash != r.Entry.Blob.Hash {
			t.Fatal("copy-only compaction changed encoded bytes")
		}
	}
	oldRoot := e.state.Roots["x"][0]
	ingest(t, e, hta.Point{Time: 24100, Value: 99})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if e.state.Roots["x"][0] == oldRoot {
		t.Fatal("test did not change raw root")
	}
	if err := e.applyCompaction(ctx, job, replacements, packs); err != nil {
		t.Fatal("unrelated append rejected:", err)
	}
	e.mu.Lock()
	e.unpin(job.Generation)
	e.mu.Unlock()
	got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
	if got.Value[0] != 99 {
		t.Fatal("new sample lost")
	}
	checkCatalog(t, e)
}
func TestCompactionFailedPublicationKeepsOriginals(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), false)
	fillCompaction(t, e, 4)
	ctx := context.Background()
	job, err := e.reserveCompaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replacements, packs, err := e.copyJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	oldRoot := e.state.Roots["x"][0]
	sequence, walBytes := e.state.Sequence, e.wal.size
	s.fail = "manifest"
	if err := e.applyCompaction(ctx, job, replacements, packs); err == nil {
		t.Fatal("failed publication succeeded")
	}
	if e.state.Roots["x"][0] != oldRoot || e.state.Sequence != sequence || e.wal.size != walBytes {
		t.Fatal("failed publication changed checkpoint")
	}
	for _, input := range job.Inputs {
		if _, _, err := s.Get(ctx, input.Entry.Blob.Key); err != nil {
			t.Fatal("original was removed")
		}
	}
	s.fail = ""
	if err := e.abortCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.unpin(job.Generation)
	e.mu.Unlock()
	checkCatalog(t, e)
}
func TestCompactionLostManifestReplyIsReconciled(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), true)
	fillCompaction(t, e, 4)
	s.loseReply = true
	if err := e.CompactOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.loseReply = false
	checkCatalog(t, e)
	drain(t, e)
}
func TestMaintenanceGoroutineCompactsAndStops(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), false)
	fillCompaction(t, e, 4)
	e.options.Compaction.IntervalSeconds = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); e.RunMaintenance(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		compacted := e.state.CompactionJob.Key == "" && e.state.StoredObjectBytes == e.state.LiveObjectBytes
		e.mu.Unlock()
		if compacted {
			cancel()
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				t.Fatal("maintenance failed to stop")
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("background goroutine did not compact")
}

type manifestGateStore struct {
	*gcStore
	gate    bool
	started chan struct{}
	resume  chan struct{}
}

func (s *manifestGateStore) Put(ctx context.Context, key string, bytes []byte, version *string) (string, error) {
	if key == "manifest" && s.gate {
		close(s.started)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.memoryStore.Put(ctx, key, bytes, version)
}
func TestMaintenancePublicationDoesNotHoldIngestLock(t *testing.T) {
	ctx := context.Background()
	s := &manifestGateStore{gcStore: &gcStore{memoryStore: newStore()}, started: make(chan struct{}), resume: make(chan struct{})}
	options := maintenanceOptions(t.TempDir(), false)
	e, err := Open(ctx, s, options, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s.gate = true
	done := make(chan error, 1)
	go func() {
		e.mu.Lock()
		next := cloneManifest(e.committed)
		next.Generation++
		err := e.publishMaintenance(ctx, next)
		e.mu.Unlock()
		done <- err
	}()
	<-s.started
	ack := make(chan error, 1)
	go func() { ack <- e.Ingest(ctx, "x", chunk(hta.Point{Time: 200, Value: 2})) }()
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(s.resume)
		<-done
		t.Fatal("maintenance PUT held ingest lock")
	}
	close(s.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.gate = false
	if e.state.Sequence != 1 || e.sequence != 2 || e.committed.Series["x"].Last.Time != 100 || e.state.Series["x"].Last.Time != 200 {
		t.Fatal("maintenance published newer WAL state")
	}
}

type quotaGCStore struct {
	*gcStore
	quota bool
}

func (s *quotaGCStore) Put(ctx context.Context, key string, bytes []byte, version *string) (string, error) {
	if s.quota && key == "manifest" && len(s.deleted) == 0 {
		return "", fmt.Errorf("storage quota reached")
	}
	return s.memoryStore.Put(ctx, key, bytes, version)
}
func TestReclaimCanFreeSpaceBeforeQuotaBlockedMetadataPUT(t *testing.T) {
	ctx := context.Background()
	s := &quotaGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), false), testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 3)
	s.quota = true
	if err := e.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.deleted) == 0 {
		t.Fatal("quota prevented deletion of already authorized objects")
	}
	checkCatalog(t, e)
}
