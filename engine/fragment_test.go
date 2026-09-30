package engine

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// metricValue reads a gauge or counter.
func metricValue(t *testing.T, c prometheus.Metric) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	if m.Gauge != nil {
		return m.Gauge.GetValue()
	}
	return m.Counter.GetValue()
}

// blockRecords returns the record counts of a stream's data blocks in order.
func (f *holdFixture) blockRecords(metric string, level int64) []int {
	f.t.Helper()
	entries, err := f.e.indexEntriesAfter(f.ctx, f.e.state.Roots[metric][level], math.MinInt64, math.MaxInt32)
	if err != nil {
		f.t.Fatal(err)
	}
	counts := make([]int, len(entries))
	for i, en := range entries {
		counts[i] = en.Records
	}
	return counts
}

// compactAll runs compaction until a pass publishes nothing.
func (f *holdFixture) compactAll() {
	f.t.Helper()
	for i := 0; i < 64; i++ {
		before := metricValue(f.t, f.e.metrics.Compactions)
		if err := f.e.CompactOnce(f.ctx); err != nil {
			f.t.Fatal(err)
		}
		if metricValue(f.t, f.e.metrics.Compactions) == before && !f.e.compactionScanMore {
			return
		}
	}
	f.t.Fatal("compaction did not settle")
}

// interiorFragments counts partial blocks followed by another block.
func interiorFragments(counts []int) int {
	n := 0
	for i := 0; i+1 < len(counts); i++ {
		if counts[i] < maxDataBlockRecords {
			n++
		}
	}
	return n
}

func TestCheckpointCompletesPartialTail(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 300)
	f.flush()
	// The hold limit expires: all 300 records form a partial tail block.
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	if got := f.blockRecords("x", 0); len(got) != 1 || got[0] != 300 {
		t.Fatalf("x raw blocks %v", got)
	}
	// The next write first completes the tail (724 records), then full blocks.
	f.ingest("x", 2000)
	f.flush()
	if got := f.blockRecords("x", 0); len(got) != 3 || got[1] != 724 || got[2] != 1024 {
		t.Fatalf("aligned write: %v", got)
	}
	f.compactAll()
	if got := f.blockRecords("x", 0); len(got) != 2 || interiorFragments(got) != 0 {
		t.Fatalf("after compaction: %v", got)
	}
	f.checkOrdered("aligned")
	f.check("aligned")
	f.restart()
	f.check("aligned after restart")
}

func TestCompactionMovesInteriorFragmentToTail(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 300)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	// Forget the tail (as for data written before tails were tracked): the
	// next write appends full blocks behind the fragment.
	f.e.mu.Lock()
	f.e.tails = nil
	f.e.mu.Unlock()
	f.ingest("x", 3000)
	f.flush()
	if got := f.blockRecords("x", 0); len(got) != 3 || got[0] != 300 || interiorFragments(got) != 1 {
		t.Fatalf("fixture: %v", got)
	}
	f.compactAll()
	got := f.blockRecords("x", 0)
	if interiorFragments(got) != 0 || got[len(got)-1] != 300 || metricValue(t, f.e.metrics.CompactionRechunkedBlocks) == 0 {
		t.Fatalf("fragment not moved to the tail: %v", got)
	}
	f.checkOrdered("rechunked")
	f.check("rechunked")
	// The tail is completed by the next checkpoint and merged into a full block.
	f.ingest("x", 1000)
	f.flush()
	f.compactAll()
	if got := f.blockRecords("x", 0); interiorFragments(got) != 0 {
		t.Fatalf("after completing the tail: %v", got)
	}
	f.checkOrdered("completed")
	f.check("completed")
	f.restart()
	f.check("completed after restart")
	checkCatalog(t, f.e)
}

func TestFragmentMetricsSeparateOpenTails(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 300)
	f.ingest("y", 50)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	if err := f.e.bootstrapTails(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.e.mu.Lock()
	f.e.updateMaintenanceMetrics(f.e.state)
	small := f.e.state.SmallBlocks
	f.e.mu.Unlock()
	tails := metricValue(t, f.e.metrics.TailBlocks)
	fragments := metricValue(t, f.e.metrics.FragmentBlocks)
	// Every stream (raw and aggregate levels) ends in its only, partial block.
	if small == 0 || tails != float64(small) || fragments != 0 {
		t.Fatalf("small %d tails %v fragments %v", small, tails, fragments)
	}
}

func TestRechunkAcrossIndexLeaves(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 300)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	f.e.mu.Lock()
	f.e.tails = nil
	f.e.mu.Unlock()
	// 200 full blocks behind the fragment span several index leaves (fanout
	// 64) and more than one job (128 blocks).
	f.ingest("x", 200*maxDataBlockRecords)
	f.flush()
	if got := f.blockRecords("x", 0); len(got) != 201 || interiorFragments(got) != 1 {
		t.Fatalf("fixture: %d blocks, %d fragments", len(got), interiorFragments(got))
	}
	f.compactAll()
	got := f.blockRecords("x", 0)
	if interiorFragments(got) != 0 || got[len(got)-1] != 300 {
		t.Fatalf("fragments left: %d of %d blocks, tail %d", interiorFragments(got), len(got), got[len(got)-1])
	}
	f.checkOrdered("rechunked across leaves")
	f.check("rechunked across leaves")
	f.restart()
	f.check("rechunked across leaves after restart")
	checkCatalog(t, f.e)
}

func TestRechunkManyFragmentsShiftsAcrossLeaves(t *testing.T) {
	f := newHoldFixture(t)
	// Alternate full blocks and 300-record fragments without tail alignment:
	// a rechunked run shifts later blocks by several block lengths.
	for i := 0; i < 70; i++ {
		f.e.mu.Lock()
		f.e.tails = nil
		f.e.mu.Unlock()
		f.ingest("x", maxDataBlockRecords+300)
		f.now = f.now.Add(2 * time.Hour)
		f.flush()
	}
	if got := f.blockRecords("x", 0); interiorFragments(got) != 69 {
		t.Fatalf("fixture: %d blocks, %d fragments", len(got), interiorFragments(got))
	}
	f.compactAll()
	if got := f.blockRecords("x", 0); interiorFragments(got) != 0 {
		t.Fatalf("fragments left: %d of %d blocks", interiorFragments(got), len(got))
	}
	f.checkOrdered("many fragments")
	f.check("many fragments")
	f.restart()
	f.check("many fragments after restart")
	checkCatalog(t, f.e)
}
