package engine

import (
	"context"
	"math"
	"path"
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

// The cascade merges in one step what tiered jobs would merge one after the
// other: the runs are disjoint, and afterwards no merge is due anymore.
func TestMergeCascadeLeavesNothingDue(t *testing.T) {
	const target, fanIn = 4 << 20, 4
	for _, sizes := range [][]int64{
		{4 << 10, 4 << 10, 4 << 10, 4 << 10},
		{1 << 20, 256 << 10, 256 << 10, 256 << 10, 256 << 10, 4 << 10, 4 << 10, 4 << 10, 4 << 10},
		{3 << 20, 3 << 20, 64 << 10, 16 << 10, 16 << 10, 16 << 10, 16 << 10, 16 << 10},
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
			merged := localitySection{begin: r[0], end: r[1]}
			for i := r[0]; i < r[1]; i++ {
				merged.bytes += sections[i].bytes
			}
			after = append(after, merged)
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
// repackMergeWait.
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
	e.repackDueSince = time.Now().Add(-repackMergeWait)
	e.compactionCompletions = 1
	if job, err = e.reserveCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	if !job.Repack || len(job.Inputs) != 4 {
		t.Fatalf("overdue merges: repack=%v inputs=%d", job.Repack, len(job.Inputs))
	}
}
