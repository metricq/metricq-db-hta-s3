package engine

import (
	"math"
	"testing"
	"time"
)

func TestObjectSections(t *testing.T) {
	block := func(metric string, first, offset, length int64) BlockInfo {
		return BlockInfo{Metric: metric, Entry: indexEntry{First: first, Last: first + 9, Blob: blob{Key: "o", Offset: offset, Length: length}, Records: 10}}
	}
	blocks := []BlockInfo{
		block("x", 20, 100, 100), // out of offset order on purpose
		block("x", 0, 0, 100),
		block("y", 0, 200, 100),
		block("x", 40, 300, 100), // x again after y: a second section
		{Metric: "x", Index: true, Entry: indexEntry{Blob: blob{Key: "o", Offset: 400, Length: 50}}},
		block("x", 30, 450, 100), // adjacent but older than its predecessor
	}
	sections, bytes := objectSections(blocks)
	if sections != 4 || bytes != 500 {
		t.Fatalf("sections %d bytes %d", sections, bytes)
	}
}

// The running section count matches a full catalog count after flushes,
// compaction and restart, and a database without the statistic recounts it.
func TestDataSectionsTrackCatalogAndRecount(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 300)
	f.ingest("y", 300)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	checkCatalog(t, f.e)
	f.ingest("x", 3000)
	f.ingest("y", 3000)
	f.flush()
	checkCatalog(t, f.e)
	f.compactAll()
	checkCatalog(t, f.e)
	f.e.mu.Lock()
	want := f.e.state.DataSections
	if !f.e.state.SectionStatsReady || want == 0 {
		f.e.mu.Unlock()
		t.Fatalf("statistic not tracked: %+v", f.e.state.SectionStatsReady)
	}
	f.e.updateMaintenanceMetrics(f.e.state)
	// Publish a manifest as written before the statistic existed.
	next := cloneMaintenanceManifest(f.e.committed)
	next.rootDirtyKnown = true
	next.Generation = f.e.state.Generation + 1
	next.SectionStatsReady, next.DataSections, next.DataBytes = false, 0, 0
	f.e.mu.Unlock()
	if ratio := metricValue(t, f.e.metrics.FragmentationRatio); math.IsNaN(ratio) || ratio <= 0 {
		t.Fatalf("fragmentation ratio %v", ratio)
	}
	if got := metricValue(t, f.e.metrics.DataSections); got != float64(want) {
		t.Fatalf("section gauge %v, want %d", got, want)
	}
	f.e.mu.Lock()
	err := f.e.publishMaintenance(f.ctx, next)
	f.e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	f.restart()
	if f.e.state.SectionStatsReady || !math.IsNaN(metricValue(t, f.e.metrics.FragmentationRatio)) {
		t.Fatal("legacy manifest reports sections")
	}
	if err := f.e.countDataSections(f.ctx); err != nil {
		t.Fatal(err)
	}
	if !f.e.state.SectionStatsReady || f.e.state.DataSections != want {
		t.Fatalf("recount %d, want %d", f.e.state.DataSections, want)
	}
	checkCatalog(t, f.e)
	f.restart()
	checkCatalog(t, f.e)
	f.ingest("x", 1000)
	f.flush()
	checkCatalog(t, f.e)
	f.check("after recount")
}
