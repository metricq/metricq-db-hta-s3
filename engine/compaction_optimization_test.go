package engine

import (
	"context"
	"fmt"
	"github.com/metricq/metricq-db-hta-s3/storage"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

func streamBlocks(t *testing.T, e *Engine, name string, level int64) []blob {
	t.Helper()
	var refs []blob
	if err := e.indexRange(context.Background(), e.state.Roots[name][level], 0, math.MaxInt64, &refs); err != nil {
		t.Fatal(err)
	}
	return refs
}

// Match production S3 range reads without copying the full mixed object for
// every tiny source block; this keeps the 1500-metric race fixture practical.
type rangeGCStore struct{ *gcStore }

func (s *rangeGCStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if offset < 0 || length < 0 || offset > int64(len(b)) || length > int64(len(b))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return append([]byte(nil), b[offset:offset+length]...), nil
}

func TestCompactionConvergesAcross1500Metrics(t *testing.T) {
	ctx := context.Background()
	configs := map[string]hta.Config{}
	for i := 0; i < 1500; i++ {
		configs[fmt.Sprintf("m%04d", i)] = hta.Config{IntervalMin: 1000000, IntervalMax: 10000000, IntervalFactor: 10}
	}
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), true), configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for batch := 0; batch < 4; batch++ {
		for name := range configs {
			if err = e.Ingest(ctx, name, chunk(hta.Point{Time: int64(batch + 1), Value: float64(batch)})); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for pass := 0; pass < 240; pass++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if pass%20 == 19 {
			done := true
			for name := range configs {
				if len(streamBlocks(t, e, name, 0)) != 1 {
					done = false
					break
				}
			}
			if done {
				t.Logf("converged after %d bounded passes", pass+1)
				break
			}
		}
	}
	for name := range configs {
		if n := len(streamBlocks(t, e, name, 0)); n != 1 {
			t.Fatalf("%s retains %d blocks", name, n)
		}
		response, err := e.Query(ctx, name, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 5})
		if err != nil || len(response.Value) != 4 {
			t.Fatalf("%s samples: %v %v", name, response, err)
		}
	}
	checkCatalog(t, e)
}

func TestAppendOnlyAggregatesConvergeAndRecover(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	options := maintenanceOptions(t.TempDir(), true)
	options.AppendOnlyAggregates = true
	e, err := Open(ctx, s, options, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 12)
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 48000, IntervalMax: 100}
	want := query(t, e, req)
	if n := len(streamBlocks(t, e, "x", 100)); n < 10 {
		t.Fatalf("fixture lacks fragments: %d", n)
	}
	for i := 0; i < 40; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for level, root := range e.state.Roots["x"] {
		if root.Key == "" {
			continue
		}
		if n := len(streamBlocks(t, e, "x", level)); n != 1 {
			t.Fatalf("level %d remains fragmented: %d", level, n)
		}
	}
	if !proto.Equal(want, query(t, e, req)) {
		t.Fatal("compaction changed aggregate response")
	}
	checkCatalog(t, e)
	drain(t, e)
	e.Close()
	options.WALDirectory = t.TempDir()
	recovered, err := Open(ctx, s, options, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if got := query(t, recovered, req); !proto.Equal(want, got) {
		t.Fatal("S3-only recovery changed aggregate response")
	}
}

type coordinationGateStore struct {
	*gcStore
	muGate           sync.Mutex
	prefix           string
	entered, release chan struct{}
}

func (s *coordinationGateStore) Put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	s.muGate.Lock()
	block := s.prefix != "" && strings.HasPrefix(key, s.prefix)
	if block {
		s.prefix = ""
	}
	s.muGate.Unlock()
	if block {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.gcStore.Put(ctx, key, b, v)
}
func TestMaintenanceCoordinationAllowsIngestAndQueries(t *testing.T) {
	for _, phase := range []string{"reserve-job", "abort-job", "abort-trash"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := &coordinationGateStore{gcStore: &gcStore{memoryStore: newStore()}, entered: make(chan struct{}), release: make(chan struct{})}
			e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), true), testConfig, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			fillCompaction(t, e, 3)
			var job CompactionJob
			if phase != "reserve-job" {
				job, err = e.reserveCompaction(ctx)
				if err != nil || job.ID == "" {
					t.Fatalf("reserve: %v", err)
				}
				defer func() { e.mu.Lock(); e.unpin(job.Generation); e.mu.Unlock() }()
			}
			if phase == "abort-trash" {
				s.prefix = "trash/"
			} else {
				s.prefix = "jobs/"
			}
			done := make(chan error, 1)
			go func() {
				if phase == "reserve-job" {
					j, err := e.reserveCompaction(ctx)
					if j.ID != "" {
						e.mu.Lock()
						e.unpin(j.Generation)
						e.mu.Unlock()
					}
					done <- err
				} else {
					done <- e.abortCompaction(ctx)
				}
			}()
			select {
			case <-s.entered:
			case <-ctx.Done():
				t.Fatal("gate not reached")
			}
			releaseOnce := sync.Once{}
			release := func() { releaseOnce.Do(func() { close(s.release) }) }
			defer release()
			admitted := make(chan error, 1)
			go func() {
				if err := e.Ingest(ctx, "x", chunk(hta.Point{Time: 12100, Value: 7})); err != nil {
					admitted <- err
					return
				}
				_, err := e.Query(ctx, "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
				admitted <- err
			}()
			select {
			case err := <-admitted:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				release()
				<-done
				t.Fatal("maintenance storage I/O blocked admission")
			}
			release()
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionWorkerDrainsMultipleJobsPerTick(t *testing.T) {
	s := &gcStore{memoryStore: newStore()}
	options := maintenanceOptions(t.TempDir(), true)
	options.Compaction.IntervalSeconds = 1
	options.Compaction.MaxBlocks = 8
	options.Compaction.MaxCycleSeconds = 2
	options.AppendOnlyAggregates = true
	e, err := Open(context.Background(), s, options, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 30)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.RunMaintenance(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(1900 * time.Millisecond)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		n := e.compactionCompletions
		e.mu.Unlock()
		if n > 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker did not publish multiple jobs within one scheduling interval")
}

func TestCompactionContractsIndexAndKeepsWarmBlocks(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), true)
	e.options.Compaction.MaxBlocks = 512
	fillCompaction(t, e, 130)
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 520000}
	want := query(t, e, req)
	old, err := e.readNode(ctx, e.state.Roots["x"][0])
	if err != nil || old.Leaf {
		t.Fatalf("fixture lacks index depth: %v", err)
	}
	for i := 0; i < 60; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	node, err := e.readNode(ctx, e.state.Roots["x"][0])
	if err != nil || !node.Leaf {
		t.Fatalf("index did not contract: %v", err)
	}
	if !proto.Equal(want, query(t, e, req)) {
		t.Fatal("index contraction changed samples")
	}
	checkCatalog(t, e)
	// Verify copy-only relocation reuses cached decoded blocks by content hash.
	s2 := &gcStore{memoryStore: newStore()}
	copyEngine := maintenanceEngine(t, s2, t.TempDir(), false)
	fillCompaction(t, copyEngine, 6)
	query(t, copyEngine, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 24000})
	before := streamBlocks(t, copyEngine, "x", 0)
	oldRefs := map[blob]bool{}
	for _, ref := range before {
		oldRefs[ref] = true
	}
	if err = copyEngine.CompactOnce(ctx); err != nil {
		t.Fatal(err)
	}
	moved := 0
	for _, ref := range streamBlocks(t, copyEngine, "x", 0) {
		if !oldRefs[ref] {
			moved++
			if _, ok := copyEngine.sharedBlocks.get(ref); !ok {
				t.Fatal("copy-only relocation lost warm records")
			}
		}
	}
	if moved == 0 {
		t.Fatal("fixture did not relocate data")
	}
}

func TestMaintenanceByteBudgetIncludesMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	underlying := newStore()
	limited := maintenanceStore{Store: underlying, budget: &rateBudget{start: time.Now(), rate: 1}}
	if _, err := limited.Put(ctx, "catalog/test", []byte{1, 2}, nil); err == nil {
		t.Fatal("metadata write bypassed byte budget")
	}
	underlying.mu.Lock()
	defer underlying.mu.Unlock()
	if _, ok := underlying.objects["catalog/test"]; ok {
		t.Fatal("cancelled budget uploaded metadata")
	}
}

func TestCompactionDoesNotRenewIngestCooldown(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	configs := map[string]hta.Config{}
	for i := 0; i < 30; i++ {
		configs[fmt.Sprint(i)] = hta.Config{IntervalMin: 1000000, IntervalMax: 10000000, IntervalFactor: 10}
	}
	options := maintenanceOptions(t.TempDir(), true)
	options.Compaction.MaxBlocks = 8
	options.Compaction.CooldownSeconds = 60
	e, err := Open(ctx, s, options, configs, nil)
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
	// Age the fixture logically; no wall-clock sleep or production timestamp override.
	if err = e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
		changes := map[string]*ObjectInfo{}
		err := snapshot.catalogWalk(ctx, snapshot.state.Catalog, 1000, func(o ObjectInfo) bool {
			o.Modified = time.Now().Add(-2 * time.Minute).UnixNano()
			changes[o.Key] = &o
			return true
		})
		if err != nil {
			return manifest{}, err
		}
		next := cloneManifest(snapshot.committed)
		next.Generation++
		if _, err = snapshot.catalogChanges(ctx, &next, changes); err != nil {
			return manifest{}, err
		}
		return next, nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 80; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for name := range configs {
		if n := len(streamBlocks(t, e, name, 0)); n != 1 {
			t.Fatalf("%s cooldown stalled with %d blocks", name, n)
		}
	}
	checkCatalog(t, e)
}

func TestAppendOnlyMixedStreamsConvergePastSingletons(t *testing.T) {
	// The S3 integration test runs this mixed-stream shape with 1500 metrics;
	// keep the ordinary race suite small enough for routine development.
	const metricCount = 150
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	configs := map[string]hta.Config{}
	for i := 0; i < metricCount; i++ {
		configs[fmt.Sprintf("canonical.%05d", i)] = hta.Config{IntervalMin: int64(time.Second), IntervalMax: int64(1000 * time.Second), IntervalFactor: 10}
	}
	options := maintenanceOptions(t.TempDir(), true)
	options.Compaction.MaxBlocks = 512
	options.AppendOnlyAggregates = true
	e, err := Open(ctx, s, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	base := int64(1700000000) * int64(time.Second)
	for batch := 0; batch < 8; batch++ {
		for i := 0; i < metricCount; i++ {
			var points []hta.Point
			for j := 0; j < 16; j++ {
				points = append(points, hta.Point{Time: base + int64(batch*16+j)*int64(time.Second), Value: float64(i + j%7)})
			}
			if err = e.Ingest(ctx, fmt.Sprintf("canonical.%05d", i), chunk(points...)); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 512 && e.MaintenanceStatus().SmallBlocks > metricCount*4; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if i%64 == 63 {
			t.Logf("pass=%d small=%d dead=%d", i+1, e.state.SmallBlocks, e.state.StoredObjectBytes-e.state.LiveObjectBytes)
		}
	}
	for name, levels := range e.state.Roots {
		for level, root := range levels {
			if root.Key == "" {
				continue
			}
			if n := len(streamBlocks(t, e, name, level)); n != 1 {
				entries, _ := e.indexEntriesAfter(ctx, root, 0, 20)
				for _, entry := range entries {
					o, _, _ := e.catalogGet(ctx, e.state.Catalog, entry.Blob.Key)
					t.Logf("entry first=%d last=%d records=%d sourceBlocks=%d modified=%d", entry.First, entry.Last, entry.Records, len(o.Blocks), o.Modified)
				}
				t.Fatalf("%s level %d retains %d blocks behind singleton candidates; small=%d", name, level, n, e.state.SmallBlocks)
			}
		}
	}
	checkCatalog(t, e)
}

func mixedEvacuationFixture(t *testing.T, merge bool) (*Engine, *rangeGCStore, string) {
	t.Helper()
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	configs := map[string]hta.Config{}
	for i := 0; i < 30; i++ {
		configs[fmt.Sprint(i)] = testConfig["x"]
	}
	options := maintenanceOptions(t.TempDir(), merge)
	options.Compaction.MaxBlocks = 8
	e, err := Open(ctx, s, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	var source string
	for batch := 0; batch < 2; batch++ {
		for name := range configs {
			var points []hta.Point
			for i := 1; i <= 40; i++ {
				points = append(points, hta.Point{Time: int64(batch*40+i) * 100, Value: float64(i)})
			}
			if err = e.Ingest(ctx, name, chunk(points...)); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if batch == 0 {
			source = streamBlocks(t, e, "0", 0)[0].Key
		}
	}
	o, found, err := e.catalogGet(ctx, e.state.Catalog, source)
	if err != nil || !found || len(o.Blocks) <= options.Compaction.MaxBlocks || o.LiveBytes >= o.Size {
		t.Fatalf("fixture lacks oversized dirty source: %+v %v", o, err)
	}
	return e, s, source
}

func TestCompactionPrioritizesMergingBeforeEvacuation(t *testing.T) {
	e, _, _ := mixedEvacuationFixture(t, true)
	if err := e.CompactOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocks := 0
	for metric := range e.state.Roots {
		blocks += len(streamBlocks(t, e, metric, 0))
	}
	if blocks != 56 {
		t.Fatalf("eight-block job should merge four raw pairs, remaining=%d want=56", blocks)
	}
	checkCatalog(t, e)
}

func TestCompactionEvacuatesMixedObjectAcrossBoundedJobs(t *testing.T) {
	ctx := context.Background()
	e, s, source := mixedEvacuationFixture(t, false)
	found := true
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 8001}
	want, err := e.Query(ctx, "0", req)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40 && found; i++ {
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
		_, found, err = e.catalogGet(ctx, e.state.Catalog, source)
		if err != nil {
			t.Fatal(err)
		}
	}
	if found {
		t.Fatal("partly dead source never finished evacuation")
	}
	checkCatalog(t, e)
	drain(t, e)
	s.mu.Lock()
	_, exists := s.objects[source]
	s.mu.Unlock()
	if exists {
		t.Fatal("last block moved but old object survived GC")
	}
	got, err := e.Query(ctx, "0", req)
	if err != nil || !proto.Equal(want, got) {
		t.Fatalf("evacuation changed history: %v", err)
	}
}

func TestKnownSingletonsDoNotConsumeMergeSearchBudget(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%t", warm), func(t *testing.T) { singletonSearch(t, warm) })
	}
}

func singletonSearch(t *testing.T, warm bool) {
	t.Helper()
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	configs := map[string]hta.Config{}
	for i := 0; i < 130; i++ {
		configs[fmt.Sprintf("a%03d", i)] = hta.Config{IntervalMin: 1000000, IntervalMax: 10000000, IntervalFactor: 10}
	}
	configs["z"] = configs["a000"]
	options := maintenanceOptions(t.TempDir(), true)
	if !warm {
		options.Compaction.MaxBlocks = 512
	}
	e, err := Open(ctx, s, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for name := range configs {
		if err = e.Ingest(ctx, name, chunk(hta.Point{Time: 1, Value: 1})); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	source := streamBlocks(t, e, "a000", 0)[0].Key
	if warm {
		for name := range configs {
			streamBlocks(t, e, name, 0)
		}
	}
	if err = e.Ingest(ctx, "z", chunk(hta.Point{Time: 2, Value: 2})); err != nil {
		t.Fatal(err)
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	o, _, err := e.catalogGet(ctx, e.state.Catalog, source)
	if err != nil {
		t.Fatal(err)
	}
	key := candidateKey(o)
	e.candidateCursor = key[:len(key)-1] // Start just before the mixed source pack.
	if err = e.CompactOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(streamBlocks(t, e, "z", 0)) != 1 {
		t.Fatal("singleton roots exhausted the search budget")
	}
	checkCatalog(t, e)
}

func TestCompactionPublicationWaitsForFlushAndAllowsIngest(t *testing.T) {
	for _, phase := range []string{"flush-first", "compaction-first"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s := &coordinationGateStore{gcStore: &gcStore{memoryStore: newStore()}, entered: make(chan struct{}), release: make(chan struct{})}
			options := maintenanceOptions(t.TempDir(), true)
			options.AppendOnlyAggregates = true
			e, err := Open(ctx, s, options, testConfig, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			fillCompaction(t, e, 3)
			job, err := e.reserveCompaction(ctx)
			if err != nil || job.ID == "" {
				t.Fatalf("reserve: %v", err)
			}
			defer func() { e.mu.Lock(); e.unpin(job.Generation); e.mu.Unlock() }()
			replacements, packs, err := e.copyJob(ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.Ingest(ctx, "x", chunk(hta.Point{Time: 12100, Value: 7})); err != nil {
				t.Fatal(err)
			}
			flushDone, compactDone := make(chan error, 1), make(chan error, 1)
			releaseOnce := sync.Once{}
			release := func() { releaseOnce.Do(func() { close(s.release) }) }
			defer release()
			if phase == "flush-first" {
				s.prefix = "data/"
				go func() { flushDone <- e.Flush(ctx) }()
			} else {
				s.prefix = "index/compact-"
				go func() { compactDone <- e.applyCompaction(ctx, job, replacements, packs) }()
			}
			select {
			case <-s.entered:
			case <-ctx.Done():
				t.Fatal("publication gate not reached")
			}
			if phase == "flush-first" {
				go func() { compactDone <- e.applyCompaction(ctx, job, replacements, packs) }()
				// The compactor must wait for publication, not discard its outputs.
				select {
				case err := <-compactDone:
					t.Fatalf("compaction failed instead of waiting: %v", err)
				case <-time.After(30 * time.Millisecond):
				}
			} else {
				go func() { flushDone <- e.Flush(ctx) }()
				admitted := make(chan error, 1)
				go func() { admitted <- e.Ingest(ctx, "x", chunk(hta.Point{Time: 12200, Value: 8})) }()
				select {
				case err := <-admitted:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("metadata publication blocked WAL ACK")
				}
			}
			release()
			if err = <-compactDone; err != nil {
				t.Fatal(err)
			}
			if err = <-flushDone; err != nil {
				t.Fatal(err)
			}
			if e.compactionCompletions != 1 || e.state.CompactionJob.Key != "" {
				t.Fatal("prepared job was discarded")
			}
			checkCatalog(t, e)
			got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 12300})
			want := 121
			if phase == "compaction-first" {
				want = 122
			}
			if len(got.Value) != want {
				t.Fatalf("samples=%d want=%d", len(got.Value), want)
			}
		})
	}
}
