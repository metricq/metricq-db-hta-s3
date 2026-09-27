package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
	"reflect"
	"testing"
	"time"
)

func heldTestManifest(n int) manifest {
	m := manifest{Version: 2, Generation: 1, HeldWatermarks: map[string]int64{}}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("held/test-%08d", i)
		m.Held = append(m.Held, blob{Key: key, Length: 1024, Hash: sha256.Sum256([]byte(key))})
		m.HeldWatermarks[fmt.Sprintf("canonical.metric.%08d/0", i)] = int64(i)
	}
	return m
}
func heldMetadataEngine(t *testing.T) (*Engine, *memoryStore) {
	t.Helper()
	s := newStore()
	e, err := Open(context.Background(), s, Options{WALDirectory: t.TempDir()}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, s
}
func assertHeldRecovery(t *testing.T, e *Engine, m manifest) {
	t.Helper()
	wire := manifest{HeldState: m.HeldState}
	if err := e.loadHeldMetadata(context.Background(), &wire); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wire.Held, m.Held) || !reflect.DeepEqual(wire.HeldWatermarks, m.HeldWatermarks) {
		t.Fatal("held metadata differs after recovery")
	}
}
func TestHeldPagesIndependentRootsAndSharedPackRetirement(t *testing.T) {
	e, _ := heldMetadataEngine(t)
	ctx := context.Background()
	base := heldTestManifest(200)
	if _, err := e.writeHeldMetadata(ctx, &base, manifest{}); err != nil {
		t.Fatal(err)
	}
	assertHeldRecovery(t, e, base)
	shared := base.HeldState.Key
	next := base
	next.Generation++
	next.Held = append(append([]blob{}, base.Held...), blob{Key: "held/new", Length: 100})
	retired, err := e.writeHeldMetadata(ctx, &next, base)
	if err != nil {
		t.Fatal(err)
	}
	if next.heldPages.watermarks != base.heldPages.watermarks {
		t.Fatal("inventory-only edit rewrote watermarks")
	}
	for _, key := range retired {
		if key == shared {
			t.Fatal("retired shared pack with live watermarks")
		}
	}
	assertHeldRecovery(t, e, next)
	assertHeldRecovery(t, e, base)
	// Removing all inventory must retain the pack referenced by the other tree.
	removed := next
	removed.Generation++
	removed.Held = nil
	retired, err = e.writeHeldMetadata(ctx, &removed, next)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range retired {
		if key == shared {
			t.Fatal("inventory removal deleted watermark pack")
		}
	}
	assertHeldRecovery(t, e, removed)
	empty := removed
	empty.Generation++
	empty.HeldWatermarks = nil
	retired, err = e.writeHeldMetadata(ctx, &empty, removed)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, key := range retired {
		found = found || key == shared
	}
	if !found || empty.HeldState.Key != "" {
		t.Fatal("last-reference removal did not retire shared pack")
	}
}
func TestHeldPagesBoundedPathEdits(t *testing.T) {
	e, _ := heldMetadataEngine(t)
	ctx := context.Background()
	base := heldTestManifest(10000)
	if _, err := e.writeHeldMetadata(ctx, &base, manifest{}); err != nil {
		t.Fatal(err)
	}
	next := base
	next.Generation++
	next.Held = append(append([]blob{}, base.Held...), blob{Key: "held/appended", Length: 100})
	if _, err := e.writeHeldMetadata(ctx, &next, base); err != nil {
		t.Fatal(err)
	}
	oldRefs := map[blob]bool{}
	var walk func(*heldTree, func(*heldTree))
	walk = func(n *heldTree, f func(*heldTree)) {
		if n == nil {
			return
		}
		f(n)
		for _, c := range n.children {
			walk(c, f)
		}
	}
	walk(base.heldPages.inventory, func(n *heldTree) { oldRefs[n.ref] = true })
	changed := 0
	walk(next.heldPages.inventory, func(n *heldTree) {
		if !oldRefs[n.ref] {
			changed++
		}
		if len(n.items) > heldPageFanout || len(n.children) > heldPageFanout {
			t.Fatal("unbounded page")
		}
	})
	if changed > 4 {
		t.Fatalf("append rewrote %d pages", changed)
	}
	if next.heldPages.watermarks != base.heldPages.watermarks {
		t.Fatal("rewrote watermarks")
	}
	assertHeldRecovery(t, e, next)
	// Delete most descriptors, including empty branches and root collapse.
	final := next
	final.Generation++
	final.Held = append([]blob{}, next.Held[len(next.Held)-3:]...)
	if _, err := e.writeHeldMetadata(ctx, &final, next); err != nil {
		t.Fatal(err)
	}
	assertHeldRecovery(t, e, final)
	t.Logf("10000 descriptors: append changes %d inventory pages, 0 watermark pages", changed)
}
func TestHeldPagesLegacyMigrationAndUnknownVersion(t *testing.T) {
	e, s := heldMetadataEngine(t)
	ctx := context.Background()
	base := heldTestManifest(100)
	p, _ := newPack("held-state")
	b, _ := encode(heldMetadata{base.Held, base.HeldWatermarks})
	base.HeldState = p.add(b)
	if _, err := s.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	loaded := manifest{HeldState: base.HeldState}
	if err := e.loadHeldMetadata(ctx, &loaded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(base.Held, loaded.Held) {
		t.Fatal("legacy descriptors lost")
	}
	next := loaded
	next.Generation = 2
	next.Held = append(append([]blob{}, loaded.Held...), blob{Key: "held/new", Length: 100})
	if _, err := e.writeHeldMetadata(ctx, &next, loaded); err != nil {
		t.Fatal(err)
	}
	assertHeldRecovery(t, e, next)
	b, _ = encode(heldRoot{Version: 99})
	p, _ = newPack("held-state")
	ref := p.add(b)
	s.Put(ctx, p.key, p.buf.Bytes(), nil)
	if err := e.loadHeldMetadata(ctx, &manifest{HeldState: ref}); err == nil {
		t.Fatal("unknown version accepted")
	}
}
func TestHeldPagesMissingOrCorruptPageFails(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			e, s := heldMetadataEngine(t)
			m := heldTestManifest(200)
			if _, err := e.writeHeldMetadata(context.Background(), &m, manifest{}); err != nil {
				t.Fatal(err)
			}
			ref := m.heldPages.inventory.children[0].ref
			s.mu.Lock()
			if corrupt {
				s.objects[ref.Key][ref.Offset] ^= 1
			} else {
				delete(s.objects, ref.Key)
			}
			s.mu.Unlock()
			if err := e.loadHeldMetadata(context.Background(), &manifest{HeldState: m.HeldState}); err == nil {
				t.Fatal("damaged committed metadata accepted")
			}
		})
	}
}

func TestHeldPagesGCAndAgeOnlyPublicationRecoverHistory(t *testing.T) {
	ctx := context.Background()
	store := &gcStore{memoryStore: newStore()}
	options := maintenanceOptions(t.TempDir(), true)
	options.HoldMaxAgeSeconds = 3600
	options.CheckpointAppendOnlyAggregates = true
	options.IngestMemoryLimitBytes = 128 << 20
	options.HoldMemoryBytes = 64 << 20
	configs := map[string]hta.Config{}
	deliveries := []Delivery{}
	for i := 0; i < 80; i++ {
		name := fmt.Sprintf("canonical.%04d", i)
		configs[name] = hta.Config{IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10}
		points := make([]hta.Point, 1070)
		for j := range points {
			points[j] = hta.Point{Time: int64(j+1) * 100, Value: float64(j)}
		}
		deliveries = append(deliveries, Delivery{Metric: name, Chunk: chunk(points...)})
	}
	e, err := Open(ctx, store, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	now := time.Unix(10000, 0)
	e.now = func() time.Time { return now }
	if n, err := e.IngestBatch(ctx, deliveries); err != nil || n != len(deliveries) {
		t.Fatalf("ingest %d: %v", n, err)
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	old := e.committed.HeldState.Key
	if e.committed.heldPages.watermarks == nil {
		t.Fatal("fixture lacks watermarks")
	}
	if err = e.Ingest(ctx, "canonical.0000", chunk(hta.Point{Time: 107100, Value: 1070})); err != nil {
		t.Fatal(err)
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	drain(t, e)
	store.mu.Lock()
	_, live := store.objects[old]
	store.mu.Unlock()
	if !live {
		t.Fatal("GC deleted pack still used by watermark tree")
	}
	sequence, generation := e.committed.Sequence, e.committed.Generation
	now = now.Add(2 * time.Hour)
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if e.committed.Sequence != sequence || e.committed.Generation <= generation {
		t.Fatal("age-only publication did not retain WAL sequence")
	}
	drain(t, e)
	store.mu.Lock()
	_, live = store.objects[old]
	store.mu.Unlock()
	if live {
		t.Fatal("GC retained pack after last reference disappeared")
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 200000}
	expected, err := e.Query(ctx, "canonical.0000", req)
	if err != nil || len(expected.Value) != 1071 {
		t.Fatalf("query: %v", err)
	}
	e.Close()
	options.WALDirectory = t.TempDir()
	recovered, err := Open(ctx, store, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	actual, err := recovered.Query(ctx, "canonical.0000", req)
	if err != nil || !proto.Equal(expected, actual) {
		t.Fatalf("S3-only recovery lost data: %v", err)
	}
}

func TestHeldPagesLostManifestReplyAndCancellation(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 20)
	f.store.loseReply = true
	f.flush()
	if f.e.wal.total() != 0 || f.e.committed.HeldState.Key == "" {
		t.Fatal("exact manifest reconciliation failed")
	}
	f.store.loseReply = false
	f.restart()
	f.check("lost CAS reply with held pages")
	f.ingest("x", 20)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sequence := f.e.committed.Sequence
	if err := f.e.Flush(ctx); err == nil {
		t.Fatal("cancelled publication accepted")
	}
	if f.e.committed.Sequence != sequence || f.e.wal.total() == 0 {
		t.Fatal("cancelled publication reclaimed acknowledged WAL")
	}
	f.restart()
	f.check("cancelled checkpoint replay")
}
