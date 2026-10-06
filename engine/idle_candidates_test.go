package engine

import (
	"testing"
	"time"
)

// Candidates holding only open tails are scanned once and then skipped
// without catalog reads, until a stream root changes; new work behind such a
// tail is still found and merged.
func TestIdleCandidatesSkippedUntilStreamChanges(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 300)
	f.ingest("y", 300)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	f.compactAll()
	if len(f.e.idleCandidates) == 0 {
		t.Fatal("tail-only candidates not remembered as idle")
	}
	lookups := func() float64 {
		return metricValue(t, f.e.metrics.MetadataCache.WithLabelValues("catalog", "hit")) + metricValue(t, f.e.metrics.MetadataCache.WithLabelValues("catalog", "miss"))
	}
	// One pass with and one without the memory; both read the candidate
	// pages, only the latter the candidates' catalog entries.
	pass := func() float64 {
		before := lookups()
		if err := f.e.CompactOnce(f.ctx); err != nil {
			t.Fatal(err)
		}
		return lookups() - before
	}
	skips := metricValue(t, f.e.metrics.CompactionIdleSkips)
	idlePass := pass()
	if metricValue(t, f.e.metrics.CompactionIdleSkips) == skips {
		t.Fatal("idle candidates not skipped")
	}
	f.e.idleCandidates = nil
	if fullPass := pass(); idlePass >= fullPass {
		t.Fatalf("idle pass read %v catalog pages, full pass %v", idlePass, fullPass)
	}
	// Forget the tails so the next write appends behind the old tail: the
	// tail's object is unchanged, only its stream root moves.
	f.e.mu.Lock()
	f.e.tails = nil
	f.e.mu.Unlock()
	f.ingest("x", 300)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	if got := f.blockRecords("x", 0); len(got) != 2 {
		t.Fatalf("fixture: %v", got)
	}
	stale := 0
	for _, c := range f.e.idleCandidates {
		if !c.unchanged(f.e.state.Roots) {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("moved stream root does not invalidate its idle candidate")
	}
	f.compactAll()
	if got := f.blockRecords("x", 0); len(got) != 1 || got[0] != 600 {
		t.Fatalf("tail behind an idle candidate not merged: %v", got)
	}
	f.checkOrdered("merged")
	f.check("merged")
	checkCatalog(t, f.e)
}
