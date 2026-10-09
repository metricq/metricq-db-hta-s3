package engine

import (
	"math"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"
)

// smallObjects counts live data objects and index packs below the
// consolidation limit.
func (f *holdFixture) smallObjects() (n int) {
	f.t.Helper()
	limit := f.e.options.CompactionOptions.defaults().consolidationLimit()
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
func TestConsolidationPacksSmallObjects(t *testing.T) {
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
	if after >= before/2 || metricValue(t, f.e.metrics.ConsolidationJobs) == 0 {
		t.Fatalf("small objects %d -> %d, consolidation jobs %v", before, after, metricValue(t, f.e.metrics.ConsolidationJobs))
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
