package engine

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

type holdFixture struct {
	t         *testing.T
	ctx       context.Context
	store     *gcStore
	dir       string
	options   Options
	now       time.Time
	e         *Engine
	reference *Engine
	next      map[string]int64
}

func newHoldFixture(t *testing.T) *holdFixture {
	f := &holdFixture{t: t, ctx: context.Background(), store: &gcStore{memoryStore: newStore()}, dir: t.TempDir(), now: time.Unix(1000, 0), next: map[string]int64{"x": 100, "y": 100}}
	f.options = maintenanceOptions(f.dir, true)
	f.options.HoldMaxAgeSeconds = 3600
	f.options.IngestMemoryLimitBytes = 64 << 20
	f.open()
	var err error
	f.reference, err = Open(f.ctx, newStore(), Options{WALDirectory: t.TempDir(), IngestMemoryLimitBytes: 64 << 20}, batchConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.reference.Close() })
	return f
}

func (f *holdFixture) open() {
	f.t.Helper()
	e, err := Open(f.ctx, f.store, f.options, batchConfig, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	e.now = func() time.Time { return f.now }
	// Open ran before the clock was injected; held streams restart their age now.
	for _, h := range e.held {
		h.since = f.now
	}
	f.e = e
	f.t.Cleanup(func() { e.Close() })
}

// restart simulates a crash: no final flush, WAL and S3 remain.
func (f *holdFixture) restart() {
	f.t.Helper()
	f.e.Close()
	f.open()
}

func (f *holdFixture) ingest(metric string, n int) {
	f.t.Helper()
	points := make([]hta.Point, n)
	for i := range points {
		points[i] = hta.Point{Time: f.next[metric], Value: math.Sin(float64(f.next[metric]) / 700)}
		f.next[metric] += 10
	}
	for _, e := range []*Engine{f.e, f.reference} {
		if err := e.Ingest(f.ctx, metric, chunk(points...)); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *holdFixture) flush() {
	f.t.Helper()
	if err := f.e.Flush(f.ctx); err != nil {
		f.t.Fatal(err)
	}
}

func (f *holdFixture) check(label string) {
	f.t.Helper()
	end := max(f.next["x"], f.next["y"]) + 1000
	requests := []*metricq.HistoryRequest{
		{Type: metricq.HistoryRequest_LAST_VALUE},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: end, IntervalMax: 50},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: end, IntervalMax: 100},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: end, IntervalMax: 1000},
		{Type: metricq.HistoryRequest_AGGREGATE, StartTime: 150, EndTime: end},
	}
	for _, name := range []string{"x", "y"} {
		for _, req := range requests {
			want, err := f.reference.Query(f.ctx, name, req)
			if err != nil {
				f.t.Fatal(err)
			}
			got, err := f.e.Query(f.ctx, name, req)
			if err != nil || !proto.Equal(want, got) {
				f.t.Fatalf("%s: %s %v differs: %v (%v)", label, name, req.Type, err, len(got.GetTimeDelta()))
			}
		}
	}
}

func (f *holdFixture) blocks(metric string, level int64) []blob {
	return streamBlocks(f.t, f.e, metric, level)
}

func (f *holdFixture) heldObjects() int {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	n := 0
	for key := range f.store.objects {
		if strings.HasPrefix(key, "held/") {
			n++
		}
	}
	return n
}

func TestHoldWritesFullBlocksAndRecoversHeldRecords(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 2500)
	f.ingest("y", 10)
	f.flush()
	// Two full raw blocks; the remainder of x and all of y are held.
	if n := len(f.blocks("x", 0)); n != 2 {
		t.Fatalf("x raw blocks %d", n)
	}
	if n := len(f.blocks("y", 0)); n != 0 {
		t.Fatalf("y raw blocks %d", n)
	}
	if len(f.e.state.Held) != 1 || f.e.unsavedBytes() != 0 || f.e.wal.total() != 0 {
		t.Fatalf("held deltas %d unsaved %d WAL %d", len(f.e.state.Held), f.e.unsavedBytes(), f.e.wal.total())
	}
	f.check("held")
	// Newer records exist only in the WAL; the older ones in the delta.
	f.ingest("x", 300)
	f.ingest("y", 5)
	f.restart()
	f.check("restart from delta and WAL")
	f.flush()
	f.check("second flush")
	f.restart()
	f.check("restart after second flush")
	// The hold limit expires: everything is written, deltas become obsolete.
	f.now = f.now.Add(2 * time.Hour)
	f.e.mu.Lock()
	due := f.e.holdDue()
	f.e.mu.Unlock()
	if !due || !f.e.NeedsFlush() {
		t.Fatal("expired hold not due")
	}
	f.flush()
	if f.e.pending.len() != 0 || len(f.e.state.Held) != 0 || len(f.e.held) != 0 && f.e.coveredRecords != 0 {
		t.Fatalf("pending %d deltas %d covered %d", f.e.pending.len(), len(f.e.state.Held), f.e.coveredRecords)
	}
	drain(t, f.e)
	if n := f.heldObjects(); n != 0 {
		t.Fatalf("%d obsolete held objects remain", n)
	}
	f.check("all written")
	f.restart()
	f.check("restart after all written")
	checkCatalog(t, f.e)
}

func TestHoldDeltaStillNeededSkipsWrittenRecords(t *testing.T) {
	f := newHoldFixture(t)
	// One delta covers both streams.
	f.ingest("x", 900)
	f.ingest("y", 20)
	f.flush()
	first := f.e.state.Held[0].Key
	// x fills a block from records in the first delta; y stays held there.
	f.ingest("x", 300)
	f.flush()
	if len(f.blocks("x", 0)) != 1 {
		t.Fatal("x did not write its full block")
	}
	found := false
	for _, ref := range f.e.state.Held {
		found = found || ref.Key == first
	}
	if !found {
		t.Fatal("delta still covering y was dropped")
	}
	if _, ok := f.e.state.HeldWatermarks[streamKey("x", 0)]; !ok {
		t.Fatal("no watermark for x")
	}
	// Recovery must not restore x records from the first delta again.
	f.restart()
	f.check("restart with partly written delta")
	f.flush()
	f.restart()
	f.check("restart after next flush")
}

// checkOrdered fails when consecutive index entries of any stream overlap,
// i.e. records were written twice.
func (f *holdFixture) checkOrdered(label string) {
	f.t.Helper()
	for metric, levels := range f.e.state.Roots {
		for level, root := range levels {
			entries, err := f.e.indexEntriesAfter(f.ctx, root, math.MinInt64, math.MaxInt32)
			if err != nil {
				f.t.Fatal(err)
			}
			for i := 1; i < len(entries); i++ {
				if entries[i].First <= entries[i-1].Last {
					f.t.Fatalf("%s: %s level %d blocks overlap: %d after %d", label, metric, level, entries[i].First, entries[i-1].Last)
				}
			}
		}
	}
}

func TestHoldFullyWrittenStreamKeepsWatermarkForSharedDelta(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 20)
	f.flush()
	// The second delta holds records of both streams.
	f.now = f.now.Add(40 * time.Minute)
	f.ingest("x", 5)
	f.ingest("y", 20)
	f.flush()
	shared := f.e.state.Held[len(f.e.state.Held)-1].Key
	// x expires and is written completely; y keeps the shared delta alive.
	f.now = f.now.Add(40 * time.Minute)
	f.flush()
	if len(f.blocks("x", 0)) == 0 || len(f.blocks("y", 0)) != 0 {
		t.Fatalf("x blocks %d, y blocks %d", len(f.blocks("x", 0)), len(f.blocks("y", 0)))
	}
	found := false
	for _, ref := range f.e.state.Held {
		found = found || ref.Key == shared
	}
	if !found {
		t.Fatal("delta still covering y was dropped")
	}
	if _, ok := f.e.state.HeldWatermarks[streamKey("x", 0)]; !ok {
		t.Fatal("no watermark for fully written x")
	}
	// A later checkpoint without new x records must keep the watermark too.
	f.ingest("y", 5)
	f.flush()
	if _, ok := f.e.state.HeldWatermarks[streamKey("x", 0)]; !ok {
		t.Fatal("watermark for x lost while the shared delta is live")
	}
	f.restart()
	f.check("restart with fully written stream in shared delta")
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	f.checkOrdered("after writing all")
	f.check("all written")
	if len(f.e.state.Held) != 0 || len(f.e.state.HeldWatermarks) != 0 || len(f.e.held) != 0 {
		t.Fatalf("deltas %d watermarks %d held %d remain", len(f.e.state.Held), len(f.e.state.HeldWatermarks), len(f.e.held))
	}
}

func TestHoldWritesLargestStreamsUnderMemoryPressure(t *testing.T) {
	f := newHoldFixture(t)
	f.e.options.HoldMemoryBytes = 200 * pendingRecordBytes
	f.ingest("x", 300)
	f.ingest("y", 40)
	if !f.e.NeedsFlush() {
		t.Fatal("memory pressure does not request a flush")
	}
	f.flush()
	if len(f.blocks("x", 0)) != 1 || f.e.pendingBytes > f.e.options.HoldMemoryBytes {
		t.Fatalf("largest stream not written: blocks %d pending %d", len(f.blocks("x", 0)), f.e.pendingBytes)
	}
	f.check("after pressure flush")
	f.restart()
	f.check("restart after pressure flush")
}

func TestHoldFailedManifestKeepsDeltaStateConsistent(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 1500)
	f.ingest("y", 7)
	f.flush()
	f.ingest("x", 700)
	f.store.fail = "manifest"
	if err := f.e.Flush(f.ctx); err == nil {
		t.Fatal("flush succeeded during outage")
	}
	f.check("after failed flush")
	f.store.fail = ""
	f.flush()
	f.check("after retry")
	f.restart()
	f.check("restart after retry")
}

// Held records are not builder backlog: compaction must still run while a
// large held set exceeds the object target.
func TestHoldDoesNotBlockCompaction(t *testing.T) {
	f := newHoldFixture(t)
	f.e.options.CheckpointUnsavedBytes = 10 * pendingRecordBytes
	for round := 0; round < 2; round++ {
		f.ingest("y", 20)
		f.now = f.now.Add(2 * time.Hour)
		f.flush()
	}
	if n := len(f.blocks("y", 0)); n != 2 {
		t.Fatalf("fixture has %d y fragments", n)
	}
	f.ingest("x", 200)
	f.flush()
	if f.e.pendingBytes < f.e.options.CheckpointUnsavedBytes || f.e.unsavedBytes() != 0 {
		t.Fatalf("fixture pending %d unsaved %d", f.e.pendingBytes, f.e.unsavedBytes())
	}
	if err := f.e.CompactOnce(f.ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(f.blocks("y", 0)); n != 1 {
		t.Fatalf("held records blocked compaction: %d y blocks", n)
	}
	f.check("after compaction")
	f.restart()
	f.check("restart after compaction")
}
