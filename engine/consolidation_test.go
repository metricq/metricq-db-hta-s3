package engine

import (
	"math"
	"testing"
	"time"
)

// smallObjects counts live data objects below the consolidation limit.
func (f *holdFixture) smallObjects() (n int) {
	f.t.Helper()
	limit := min(int64(smallObjectBytes), f.e.options.CompactionOptions.defaults().OutputObjectBytes/8)
	if err := f.e.catalogWalk(f.ctx, f.e.state.Catalog, math.MaxInt, func(o ObjectInfo) bool {
		if o.Size < limit && len(o.Blocks) > 0 && !o.Blocks[0].Index {
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
	f.checkOrdered("consolidated")
	f.check("consolidated")
	checkCatalog(t, f.e)
	f.restart()
	f.check("after restart")
	checkCatalog(t, f.e)
}
