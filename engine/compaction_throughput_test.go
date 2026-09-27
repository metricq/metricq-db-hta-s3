package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
)

func TestCatalogInventoryKeepsStablePagesAndRetiresLastReference(t *testing.T) {
	ctx := context.Background()
	e, s := heldMetadataEngine(t)
	initial := ObjectInfo{Key: "data/inventory-test", Size: 500 * 64, LiveBytes: 500 * 64}
	for i := 0; i < 500; i++ {
		initial.Blocks = append(initial.Blocks, BlockInfo{Metric: fmt.Sprintf("canonical.%04d", i), Entry: indexEntry{Blob: blob{Key: initial.Key, Offset: int64(i) * 64, Length: 64, Hash: sha256.Sum256([]byte(fmt.Sprint(i)))}}})
	}
	if _, err := e.writeObjectInventory(ctx, &initial, ObjectInfo{}); err != nil {
		t.Fatal(err)
	}
	if len(initial.Inventory) != 4 {
		t.Fatal("inventory not paged")
	}
	changed := initial
	changed.Blocks = append(append([]BlockInfo{}, initial.Blocks[:200]...), initial.Blocks[201:]...)
	changed.LiveBytes -= 64
	retired, err := e.writeObjectInventory(ctx, &changed, initial)
	if err != nil {
		t.Fatal(err)
	}
	rewritten := 0
	for i, page := range changed.Inventory {
		if page.Ref != initial.Inventory[i].Ref {
			rewritten++
		}
	}
	if rewritten != 1 || len(retired) != 0 {
		t.Fatalf("localized deletion rewrites %d pages, retires %v", rewritten, retired)
	}
	wire := changed
	wire.Blocks = nil
	if err = e.loadObjectInventory(ctx, &wire); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wire.Blocks, changed.Blocks) {
		t.Fatal("inventory recovery differs")
	}
	empty := changed
	empty.Blocks = nil
	empty.LiveBytes = 0
	retired, err = e.writeObjectInventory(ctx, &empty, changed)
	if err != nil {
		t.Fatal(err)
	}
	if len(retired) != 2 || len(empty.Inventory) != 0 {
		t.Fatalf("last-reference deletion: %v", retired)
	}
	// A reader must still verify each page within a coalesced GET.
	s.mu.Lock()
	ref := changed.Inventory[1].Ref
	s.objects[ref.Key][ref.Offset] ^= 1
	s.mu.Unlock()
	wire = changed
	wire.Blocks = nil
	if err = e.loadObjectInventory(ctx, &wire); err == nil {
		t.Fatal("corrupt inventory accepted")
	}
}

func TestSharedCatalogCacheSurvivesPhasesAndRelocation(t *testing.T) {
	ctx := context.Background()
	e, s := heldMetadataEngine(t)
	e.sharedCatalog = newCatalogPageCache()
	root, _, err := e.updateCatalog(ctx, blob{}, map[string]*ObjectInfo{"data/one": {Key: "data/one", Size: 10, LiveBytes: 10}}, "catalog")
	if err != nil {
		t.Fatal(err)
	}
	// The next snapshot uses only the shared cache. Relocation by identical hash
	// also hits it; no old address is followed when looking up an immutable page.
	next := &Engine{store: s, metrics: e.metrics, sharedCatalog: e.sharedCatalog}
	s.mu.Lock()
	delete(s.objects, root.Key)
	s.mu.Unlock()
	root.Key = "catalog/relocated"
	node, err := (&catalogWriter{e: next, ctx: ctx}).read(root)
	if err != nil || len(node.Items) != 1 {
		t.Fatalf("shared cache missed: %v", err)
	}
	// Oversized decoded pages cannot evict or exceed the byte budget.
	cache := newCatalogPageCache()
	cache.add(root, catalogNode{Items: []ObjectInfo{{Key: string(make([]byte, catalogCacheLimit+1))}}}, 0)
	if cache.bytes != 0 {
		t.Fatal("oversized cache page retained")
	}
}

type indexPrefetchStore struct {
	*rangeGCStore
	gets int
}

func (s *indexPrefetchStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.gets++
	return s.rangeGCStore.GetRange(ctx, key, offset, length)
}
func TestIndexPrefetchCoalescesAndVerifiesPages(t *testing.T) {
	ctx := context.Background()
	store := &indexPrefetchStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}}
	e, err := Open(ctx, store, Options{WALDirectory: t.TempDir()}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p, _ := newPack("index")
	var refs []blob
	for i := 0; i < 32; i++ {
		edge, err := writeNode(p, indexNode{Leaf: true, Entries: []indexEntry{{First: int64(i), Last: int64(i), Blob: blob{Key: "data/example", Length: 10}}}})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, edge.Blob)
	}
	if _, err = store.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	store.gets = 0
	if err = e.prefetchNodes(ctx, refs); err != nil {
		t.Fatal(err)
	}
	if store.gets != 1 {
		t.Fatalf("prefetch needs %d GETs", store.gets)
	}
	for _, ref := range refs {
		if _, err = e.readNode(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	if store.gets != 1 {
		t.Fatal("prefetched pages fetched again")
	}
	e.nodeCache = nil
	e.sharedNodes = newIndexPageCache()
	store.mu.Lock()
	store.objects[p.key][refs[5].Offset] ^= 1
	store.mu.Unlock()
	if err = e.prefetchNodes(ctx, refs); err == nil {
		t.Fatal("prefetch accepted corrupt index page")
	}
	if !reflect.DeepEqual(e.nodeCache, map[blob]indexNode(nil)) {
		t.Fatal("failed batch populated cache")
	}

}

func TestLocalityBatchesIndependentLevelsWithinJobBudget(t *testing.T) {
	ctx := context.Background()
	store := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	options := maintenanceOptions(t.TempDir(), true)
	options.OutputObjectBytes = 4 << 20
	options.JobMaxBlocks = 12
	configs := map[string]hta.Config{}
	for _, name := range []string{"a", "b", "c"} {
		configs[name] = hta.Config{IntervalMin: 1000000000000, IntervalMax: 1000000000000, IntervalFactor: 10}
	}
	e, err := Open(ctx, store, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for batch := 0; batch < 4; batch++ {
		for name := range configs {
			points := make([]hta.Point, maxDataBlockRecords)
			for i := range points {
				points[i] = hta.Point{Time: int64(batch*maxDataBlockRecords + i + 1), Value: float64(i)}
			}
			if err = e.Ingest(ctx, name, chunk(points...)); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	e.compactionCompletions = 3
	job, err := e.reserveCompaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	metrics := map[string]bool{}
	for _, input := range job.Inputs {
		metrics[input.Metric] = true
	}
	if !job.Locality || len(metrics) != 3 || len(job.Inputs) != 12 {
		t.Fatalf("locality job: metrics=%d inputs=%d locality=%v", len(metrics), len(job.Inputs), job.Locality)
	}
	repl, packs, err := e.copyJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.applyCompaction(ctx, job, repl, packs); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.unpin(job.Generation)
	e.mu.Unlock()
	drain(t, e)
	for name := range configs {
		r, err := e.Query(ctx, name, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 10000})
		if err != nil || len(r.Value) != 4096 {
			t.Fatalf("%s history: %v", name, err)
		}
		refs := streamBlocks(t, e, name, 0)
		for i := 1; i < len(refs); i++ {
			if refs[i].Key != refs[i-1].Key || refs[i].Offset != refs[i-1].Offset+refs[i-1].Length {
				t.Fatal("level section is not contiguous")
			}
		}
	}
}

func TestGrowingTailMergeWaitsForUsefulGrowthOrAge(t *testing.T) {
	now := time.Now()
	group := []BlockInfo{{Entry: indexEntry{Records: 800}}, {Entry: indexEntry{Records: 10}}}
	if mergeWorthwhile(group, 0, now.Add(-2*time.Minute).UnixNano(), now, 60) {
		t.Fatal("tiny delta repeatedly rewrites a large tail")
	}
	if !mergeWorthwhile(group, 0, now.Add(-2*time.Hour).UnixNano(), now, 60) {
		t.Fatal("sparse mature tail never finishes")
	}
	if !mergeWorthwhile(group, 250, now.Add(-2*time.Minute).UnixNano(), now, 60) {
		t.Fatal("finished prefix is not merged")
	}
	group[1].Entry.Records = 200
	if !mergeWorthwhile(group, 0, now.Add(-2*time.Minute).UnixNano(), now, 60) {
		t.Fatal("25 percent growth did not qualify")
	}
}

func TestContinuousMaintenanceWakesOnCheckpointAndStops(t *testing.T) {
	store := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, store, t.TempDir(), true)
	e.options.CompactionOptions.Continuous = true
	e.options.CompactionOptions.CycleIntervalSeconds = 3600
	fillCompaction(t, e, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); e.RunMaintenance(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		completed := e.compactionCompletions
		e.mu.Unlock()
		if completed > 0 {
			cancel()
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				t.Fatal("maintenance did not stop")
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("continuous worker waited for its one-hour ticker instead of the checkpoint wakeup")
}

func TestPartialBootstrapCanExtendPagedInventory(t *testing.T) {
	e, _ := heldMetadataEngine(t)
	ctx := context.Background()
	old := ObjectInfo{Key: "data/bootstrap", Size: 256 * 128, LiveBytes: 256 * 64}
	for i := 0; i < 256; i++ {
		old.Blocks = append(old.Blocks, BlockInfo{Entry: indexEntry{Blob: blob{Key: old.Key, Offset: int64(i) * 128, Length: 64}}})
	}
	if _, err := e.writeObjectInventory(ctx, &old, ObjectInfo{}); err != nil {
		t.Fatal(err)
	}
	next := old
	next.Blocks = append([]BlockInfo{}, old.Blocks...)
	for i := 0; i < 100; i++ {
		next.Blocks = append(next.Blocks, BlockInfo{Entry: indexEntry{Blob: blob{Key: old.Key, Offset: int64(i)*128 + 64, Length: 64}}})
	}
	next.LiveBytes += 100 * 64
	if _, err := e.writeObjectInventory(ctx, &next, old); err != nil {
		t.Fatal(err)
	}
	loaded := next
	loaded.Blocks = nil
	if err := e.loadObjectInventory(ctx, &loaded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.Blocks, next.Blocks) {
		t.Fatal("partial bootstrap dropped discovered descriptors")
	}
}
