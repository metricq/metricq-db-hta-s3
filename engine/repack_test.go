package engine

import (
	"context"
	"math"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// smallObjects counts live data objects and index packs below the
// consolidation limit.
func (f *holdFixture) smallObjects() (n int) {
	f.t.Helper()
	limit := f.e.options.CompactionOptions.defaults().smallObjectLimit()
	if err := f.e.catalogWalk(f.ctx, f.e.state.Catalog, math.MaxInt, func(o ObjectInfo) bool {
		if o.Size < limit {
			n++
		}
		return true
	}); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// Small objects from many checkpoints and jobs are packed together; data and
// catalog accounting stay intact.
func TestRepackPacksSmallObjects(t *testing.T) {
	f := newHoldFixture(t)
	for i := 0; i < 12; i++ {
		f.ingest("x", 40)
		f.ingest("y", 40)
		f.now = f.now.Add(2 * time.Hour)
		f.flush()
	}
	before := f.smallObjects()
	if before < 8 {
		t.Fatalf("fixture: %d small objects", before)
	}
	f.compactAll()
	after := f.smallObjects()
	if after >= before/2 || metricValue(t, f.e.metrics.RepackJobs) == 0 {
		t.Fatalf("small objects %d -> %d, repack jobs %v", before, after, metricValue(t, f.e.metrics.RepackJobs))
	}
	t.Logf("small objects %d -> %d", before, after)
	// Index pages are rewritten with their ancestors, never copied into a
	// numbered pack of copyJob.
	if err := f.e.catalogWalk(f.ctx, f.e.state.Catalog, math.MaxInt, func(o ObjectInfo) bool {
		if strings.HasPrefix(o.Key, "index/compact-") {
			if _, err := strconv.Atoi(path.Base(o.Key)); err == nil {
				t.Errorf("copied index pack %s", o.Key)
			}
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	// Within each object, a stream's blocks form one run: jobs order their
	// inputs by stream instead of copying object by object.
	for name, levels := range f.e.state.Roots {
		for level, root := range levels {
			entries, err := f.e.indexEntriesAfter(f.ctx, root, math.MinInt64, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			objects := map[string]bool{}
			for _, entry := range entries {
				objects[entry.Blob.Key] = true
			}
			runs := 0
			for i, entry := range entries {
				if prev := entries[max(i-1, 0)].Blob; i == 0 || entry.Blob.Key != prev.Key || entry.Blob.Offset != prev.Offset+prev.Length {
					runs++
				}
			}
			if runs != len(objects) {
				t.Errorf("%s level %d: %d runs in %d objects", name, level, runs, len(objects))
			}
		}
	}
	f.checkOrdered("consolidated")
	f.check("consolidated")
	checkCatalog(t, f.e)
	f.restart()
	f.check("after restart")
	checkCatalog(t, f.e)
}

// The cascade merges in one step what separate jobs would merge one after
// the other: the runs are disjoint, and after copying cuts them into full
// sections and a remainder, no merge is due anymore.
func TestMergeCascadeLeavesNothingDue(t *testing.T) {
	const target, fanIn = 4 << 20, 4
	for _, sizes := range [][]int64{
		{4 << 10, 4 << 10, 4 << 10, 4 << 10},
		{1 << 20, 256 << 10, 256 << 10, 256 << 10, 256 << 10, 4 << 10, 4 << 10, 4 << 10, 4 << 10},
		{3 << 20, 3 << 20, 64 << 10, 16 << 10, 16 << 10, 16 << 10, 16 << 10, 16 << 10},
		{1950 << 10, 1500 << 10, 1400 << 10},
		{2100 << 10, 2700 << 10, 2900 << 10, 2500 << 10, 4 << 10, 4 << 10, 4 << 10, 4 << 10},
		{4 << 20, 3500 << 10, 300 << 10, 100 << 10, 100 << 10, 100 << 10, 100 << 10, 600 << 10},
		{8 << 10, 4 << 20, 4 << 20, 3900 << 10, 4 << 20, 20 << 10},
	} {
		sections := make([]localitySection, len(sizes))
		for i, n := range sizes {
			sections[i] = localitySection{begin: i, end: i + 1, bytes: n}
		}
		runs := mergeCascade(sections, target, fanIn)
		if len(runs) == 0 {
			t.Fatalf("%v: nothing merged", sizes)
		}
		var after []localitySection
		next := 0
		for _, r := range runs {
			if r[0] < next || r[1]-r[0] < 2 {
				t.Fatalf("%v: runs %v overlap or are trivial", sizes, runs)
			}
			for i := next; i < r[0]; i++ {
				after = append(after, sections[i])
			}
			var total int64
			for i := r[0]; i < r[1]; i++ {
				total += sections[i].bytes
			}
			for _, n := range cutSections(total, target) {
				after = append(after, localitySection{begin: r[0], end: r[1], bytes: n})
			}
			next = r[1]
		}
		after = append(after, sections[next:]...)
		if first, end := localityMerge(after, target, fanIn); first != end {
			t.Fatalf("%v: merge of %d..%d still due after runs %v", sizes, first, end, runs)
		}
	}
}

// Due merges below a nearly full job wait, so jobs do not leave a partly
// filled output that becomes a start object again, but no longer than
// repackMergeWait. Waiting costs no index reads.
func TestRepackMergesWaitForAFullJob(t *testing.T) {
	ctx := context.Background()
	store := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	options := maintenanceOptions(t.TempDir(), true)
	options.OutputObjectBytes = 4 << 20
	configs := map[string]hta.Config{"a": {IntervalMin: 1000000000000, IntervalMax: 1000000000000, IntervalFactor: 10}}
	e, err := Open(ctx, store, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for batch := 0; batch < 4; batch++ {
		points := make([]hta.Point, maxDataBlockRecords)
		for i := range points {
			points[i] = hta.Point{Time: int64(batch*maxDataBlockRecords + i + 1), Value: float64(i)}
		}
		if err = e.Ingest(ctx, "a", chunk(points...)); err != nil {
			t.Fatal(err)
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	e.compactionCompletions = 1
	job, err := e.reserveCompaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.Repack {
		t.Fatal("repacked four small blocks without waiting")
	}
	if e.repackDueSince.IsZero() {
		t.Fatal("due merges not noted")
	}
	// While the merges wait, selections reuse the plans of unchanged streams
	// instead of reading their index again.
	planReads := func() (n float64) {
		for _, reader := range []string{"repack_start", "repack_walk"} {
			for _, source := range []string{"local", "cache", "store"} {
				n += metricValue(t, e.metrics.IndexPageReads.WithLabelValues(reader, source))
			}
		}
		return n
	}
	before := planReads()
	if before == 0 {
		t.Fatal("planning read no index pages")
	}
	e.compactionCompletions = 1
	if job, err = e.reserveCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	if job.Repack {
		t.Fatal("repacked while waiting")
	}
	if n := planReads() - before; n != 0 {
		t.Fatalf("planning unchanged streams again read %v index pages", n)
	}
	e.repackDueSince = time.Now().Add(-repackMergeWait)
	e.compactionCompletions = 1
	if job, err = e.reserveCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	if !job.Repack || len(job.Inputs) != 4 {
		t.Fatalf("overdue merges: repack=%v inputs=%d", job.Repack, len(job.Inputs))
	}
}

// Repack merges a stream's sections and cuts them into full objects holding
// only that stream; only the newest sections are smaller.
func TestRepackCutsStreamsIntoFullObjects(t *testing.T) {
	previousWait := repackMergeWait
	repackMergeWait = 0
	defer func() { repackMergeWait = previousWait }()
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.CompactionOptions.OutputObjectBytes = 64 << 10
	opts.CompactionOptions.JobMaxBytes = 1 << 20
	opts.CompactionOptions.JobMaxBlocks = 512
	e, err := Open(ctx, s, opts, map[string]hta.Config{"a": {IntervalMin: int64(time.Second), IntervalMax: int64(100000 * time.Second), IntervalFactor: 10}, "b": {IntervalMin: int64(time.Second), IntervalMax: int64(100000 * time.Second), IntervalFactor: 10}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for block := 0; block < 160; block++ {
		for _, name := range []string{"a", "b"} {
			points := make([]hta.Point, maxDataBlockRecords)
			for j := range points {
				n := block*maxDataBlockRecords + j
				points[j] = hta.Point{Time: int64(n+1) * int64(time.Second), Value: math.Sin(float64(n)/31) + float64(n%13)/7}
			}
			if err := e.Ingest(ctx, name, chunk(points...)); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if block%8 == 7 {
			for i := 0; i < 16; i++ {
				if err := e.CompactOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for i := 0; i < 64; i++ {
		if err := e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The layout is final: no stream level is rewritten again.
	jobs := metricValue(t, e.metrics.RepackJobs)
	for i := 0; i < 8; i++ {
		if err := e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := metricValue(t, e.metrics.RepackJobs) - jobs; n > 0 {
		t.Fatalf("%v repack jobs after compaction settled", n)
	}
	sizes := map[string]int64{}
	if err := e.catalogWalk(ctx, e.state.Catalog, math.MaxInt, func(o ObjectInfo) bool {
		sizes[o.Key] = o.Size
		return true
	}); err != nil {
		t.Fatal(err)
	}
	target := opts.CompactionOptions.OutputObjectBytes
	for _, name := range []string{"a", "b"} {
		entries, err := e.indexEntriesAfter(ctx, e.state.Roots[name][0], math.MinInt64, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		open, _ := openSuffix(entries)
		sections := localitySections(entries[:len(entries)-open])
		var layout []int64
		fulls, smaller := 0, 0
		for _, sec := range sections {
			layout = append(layout, sec.bytes>>10)
			if !sec.full(target) {
				smaller++
				continue
			}
			if smaller > 0 {
				t.Fatalf("%s: full section behind smaller ones, sections in KiB %v", name, layout)
			}
			fulls++
			// A full section fills its object alone.
			if key := entries[sec.begin].Blob.Key; sizes[key] > sec.bytes+sec.bytes/16 {
				t.Fatalf("%s: full section of %d bytes shares %s of %d bytes", name, sec.bytes, key, sizes[key])
			}
		}
		if fulls < 3 || smaller > 8 {
			t.Fatalf("%s: %d full and %d smaller sections, in KiB %v", name, fulls, smaller, layout)
		}
		t.Logf("%s: sections in KiB %v", name, layout)
	}
	checkCatalog(t, e)
	// Index page reads are counted by purpose.
	for _, reader := range []string{"repack_start", "publish"} {
		var n float64
		for _, source := range []string{"local", "cache", "store"} {
			n += metricValue(t, e.metrics.IndexPageReads.WithLabelValues(reader, source))
		}
		if n == 0 {
			t.Errorf("no index page reads counted for %s", reader)
		}
	}
}

// Incremental plans equal plans read from scratch, across checkpoints that
// append full and partial blocks and jobs that move blocks; after a
// checkpoint, planning reads only the entries behind the last planned one.
func TestRepackIncrementalPlansMatchFullPlans(t *testing.T) {
	previousWait := repackMergeWait
	repackMergeWait = 0
	defer func() { repackMergeWait = previousWait }()
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.CompactionOptions.OutputObjectBytes = 64 << 10
	opts.CompactionOptions.JobMaxBytes = 1 << 20
	opts.CompactionOptions.JobMaxBlocks = 512
	e, err := Open(ctx, s, opts, map[string]hta.Config{"a": {IntervalMin: int64(time.Second), IntervalMax: int64(100000 * time.Second), IntervalFactor: 10}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	options := e.options.CompactionOptions.defaults()
	plan := func() ([]BlockInfo, map[blob]bool) {
		e.repackCuts = nil
		due, err := e.repackStreamPlan(ctx, e, options, "a", 0)
		if err != nil {
			t.Fatal(err)
		}
		return due, e.repackCuts
	}
	keys := func(due []BlockInfo) []blob {
		var out []blob
		for _, b := range due {
			out = append(out, b.Entry.Blob)
		}
		return out
	}
	n := 0
	var incrementalPages, fullPages float64
	incremental := func() float64 {
		return metricValue(t, e.metrics.CompactionRepackPlans.WithLabelValues("incremental"))
	}
	for step := 0; step < 480; step++ {
		// Full blocks, partial blocks and partial tails in varying order.
		count := []int{maxDataBlockRecords, 300, 2 * maxDataBlockRecords, 700, maxDataBlockRecords + 200}[step%5]
		points := make([]hta.Point, count)
		for j := range points {
			n++
			points[j] = hta.Point{Time: int64(n) * int64(time.Second), Value: math.Sin(float64(n)/31) + float64(n%13)/7}
		}
		if err := e.Ingest(ctx, "a", chunk(points...)); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if step%9 == 8 {
			for i := 0; i < 4; i++ {
				if err := e.CompactOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Background jobs must not change the index between the two plans.
		e.maintenanceMu.Lock()
		pages := func() (r float64) {
			for _, source := range []string{"local", "cache", "store"} {
				r += metricValue(t, e.metrics.IndexPageReads.WithLabelValues("other", source))
			}
			return r
		}
		before := pages()
		got, gotCuts := plan()
		incrementalPages += pages() - before
		kept, keptAfter := e.repackPlans, e.repackAfter
		e.repackPlans, e.repackAfter = nil, nil
		before = pages()
		want, wantCuts := plan()
		fullPages += pages() - before
		e.repackPlans, e.repackAfter = kept, keptAfter
		e.maintenanceMu.Unlock()
		if !reflect.DeepEqual(keys(got), keys(want)) || !reflect.DeepEqual(gotCuts, wantCuts) {
			t.Fatalf("step %d: incremental plan %v cuts %v, full plan %v cuts %v", step, keys(got), gotCuts, keys(want), wantCuts)
		}
	}
	if incremental() < 50 {
		t.Fatalf("only %v incremental plans", incremental())
	}
	// Plans after checkpoints read the entries behind the last planned one,
	// not the stream behind its full prefix. The gain grows with the stream
	// (here up to about 10 leaves): incremental plans read about 57 % of the
	// pages after 480 steps, 39 % after 800.
	if incrementalPages*3 > fullPages*2 {
		t.Fatalf("incremental plans read %v index pages, full plans %v", incrementalPages, fullPages)
	}
	t.Logf("incremental plans read %v index pages, full plans %v", incrementalPages, fullPages)
}
