package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
)

func TestManifestPagesKeepMaintenanceSmallAndRecover(t *testing.T) {
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.HoldMaxAgeSeconds = 3600
	configs := make(map[string]hta.Config)
	for i := 0; i < 1500; i++ {
		configs[fmt.Sprintf("canonical.%04d", i)] = hta.Config{IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10}
	}
	e, err := Open(ctx, s, opts, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for name := range configs {
		if err = e.Ingest(ctx, name, chunk(hta.Point{Time: 100, Value: 1}, hta.Point{Time: 250, Value: 2})); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before := e.committed
	if before.CheckpointState.Key == "" || before.StreamIndex.Key == "" || before.HeldState.Key == "" {
		t.Fatal("metadata was not persisted")
	}
	b, _, err := s.Get(ctx, "manifest")
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 4096 {
		t.Fatalf("manifest is %d bytes", len(b))
	}
	var wire manifest
	if err = decode(b, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Series) != 0 || len(wire.Roots) != 0 || len(wire.Held) != 0 || len(wire.HeldWatermarks) != 0 {
		t.Fatal("hydrated state leaked into manifest")
	}
	s.mu.Lock()
	stateObjects := 0
	for key := range s.objects {
		if strings.HasPrefix(key, "state/") {
			stateObjects++
		}
	}
	s.mu.Unlock()
	if err = e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
		next := cloneMaintenanceManifest(snapshot.committed)
		next.Generation++
		return next, nil
	}); err != nil {
		t.Fatal(err)
	}
	if e.committed.CheckpointState != before.CheckpointState || e.committed.StreamIndex != before.StreamIndex || e.committed.HeldState != before.HeldState {
		t.Fatal("maintenance rewrote unchanged metadata")
	}
	s.mu.Lock()
	afterObjects := 0
	for key := range s.objects {
		if strings.HasPrefix(key, "state/") {
			afterObjects++
		}
	}
	s.mu.Unlock()
	if afterObjects != stateObjects {
		t.Fatal("maintenance uploaded checkpoint pages")
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	opts.WALDirectory = t.TempDir()
	recovered, err := Open(ctx, s, opts, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.pending.len() != 4500 || len(recovered.state.Series) != 1500 {
		t.Fatalf("recovery lost held state: %d records, %d series", recovered.pending.len(), len(recovered.state.Series))
	}
	t.Logf("1500 metrics: manifest=%d bytes, maintenance reuses all metadata roots", len(b))
}

func TestMetadataFailureRetainsAcknowledgedWAL(t *testing.T) {
	for _, prefix := range []string{"state/", "roots/", "held-state/"} {
		t.Run(prefix, func(t *testing.T) {
			ctx := context.Background()
			s := &prefixFailureStore{gcStore: &gcStore{memoryStore: newStore()}}
			opts := maintenanceOptions(t.TempDir(), true)
			opts.HoldMaxAgeSeconds = 3600
			e, err := Open(ctx, s, opts, testConfig, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			ingest(t, e, hta.Point{Time: 100, Value: 1}, hta.Point{Time: 250, Value: 2})
			s.prefix = prefix
			if err = e.Flush(ctx); err == nil {
				t.Fatal("metadata failure accepted")
			}
			if e.wal.total() == 0 {
				t.Fatal("acknowledged WAL was reclaimed")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			s.prefix = ""
			recovered, err := Open(ctx, s, opts, testConfig, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			if recovered.state.Series["x"].Last.Time != 250 {
				t.Fatal("acknowledged data lost")
			}
			if err = recovered.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if recovered.wal.total() != 0 {
				t.Fatal("successful metadata commit did not reclaim WAL")
			}
		})
	}
}

type prefixFailureStore struct {
	*gcStore
	prefix string
}

func (s *prefixFailureStore) Put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	if s.prefix != "" && strings.HasPrefix(key, s.prefix) {
		return "", fmt.Errorf("metadata unavailable")
	}
	return s.gcStore.Put(ctx, key, b, v)
}

func TestMissingOrCorruptManifestPageFailsStartup(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			ctx := context.Background()
			s := &gcStore{memoryStore: newStore()}
			opts := maintenanceOptions(t.TempDir(), true)
			e, err := Open(ctx, s, opts, testConfig, nil)
			if err != nil {
				t.Fatal(err)
			}
			ingest(t, e, hta.Point{Time: 100, Value: 1})
			if err = e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			key := e.committed.CheckpointState.Key
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			if corrupt {
				s.objects[key][0] ^= 1
			} else {
				delete(s.objects, key)
			}
			s.mu.Unlock()
			opts.WALDirectory = t.TempDir()
			recovered, err := Open(ctx, s, opts, testConfig, nil)
			if err == nil {
				recovered.Close()
				t.Fatal("accepted missing/corrupt checkpoint metadata")
			}
		})
	}
}

func TestBackgroundHoldDeadlinesAreBatchedWithoutDelayingPressure(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.HoldMaxAgeSeconds = 60
	opts.HoldExpiryIntervalSeconds = 30
	e, err := Open(ctx, s, opts, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	now := time.Unix(1, 0)
	e.now = func() time.Time { return now }
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	now = time.Unix(62, 0)
	if !e.NeedsFlush() || e.needsScheduledFlush() {
		t.Fatal("age-only deadline not batched")
	}
	e.options.CheckpointUnsavedBytes = 1
	ingest(t, e, hta.Point{Time: 200, Value: 2})
	if !e.needsScheduledFlush() {
		t.Fatal("pressure was delayed")
	}
	e.options.CheckpointUnsavedBytes = 1 << 20
	now = time.Unix(91, 0)
	if !e.needsScheduledFlush() {
		t.Fatal("batch deadline not honoured")
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// Assert that obsolete metadata can be deleted without deleting packs which
// still contain unchanged shard pages.
func TestMetadataGCProtectsSharedPages(t *testing.T) {
	ctx := context.Background()
	s := &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}
	opts := maintenanceOptions(t.TempDir(), true)
	opts.CompactionOptions.LocalityDisabled = true
	configs := map[string]hta.Config{"a": {IntervalMin: 100, IntervalMax: 10000}, "b": {IntervalMin: 100, IntervalMax: 10000}}
	if metadataShard("a") == metadataShard("b") {
		t.Fatal("fixture shares a shard")
	}
	e, err := Open(ctx, s, opts, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if err = e.Ingest(ctx, name, chunk(hta.Point{Time: 100, Value: 1})); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	old := e.committed.CheckpointState.Key
	for i := 2; i < 6; i++ {
		if err = e.Ingest(ctx, "a", chunk(hta.Point{Time: int64(i) * 100, Value: 2})); err != nil {
			t.Fatal(err)
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		if err = e.Reclaim(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = s.Get(ctx, old); err != nil {
		t.Fatal("unchanged shard was reclaimed", err)
	}
	if err = e.Ingest(ctx, "b", chunk(hta.Point{Time: 200, Value: 2})); err != nil {
		t.Fatal(err)
	}
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err = e.Reclaim(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = s.Get(ctx, old); err != storage.ErrNotFound {
		t.Fatal("unreferenced metadata pack was not reclaimed", err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	opts.WALDirectory = t.TempDir()
	recovered, err := Open(ctx, s, opts, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.state.Series["a"].Last.Time != 500 || recovered.state.Series["b"].Last.Time != 200 {
		t.Fatal("recovery after GC lost state")
	}
}
