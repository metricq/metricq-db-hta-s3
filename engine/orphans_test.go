package engine

import (
	"context"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

// Objects that no manifest reaches in two sweeps go to the trash journal and
// are deleted; one found only once stays; reachable objects are never touched.
func TestOrphanSweepRetiresObjectsUnreachableTwice(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), true)
	fillCompaction(t, e, 30)
	drain(t, e)
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1 << 40}
	want := query(t, e, req)
	put := func(key string) {
		absent := ""
		if _, err := s.Put(ctx, key, []byte("left by an interrupted checkpoint"), &absent); err != nil {
			t.Fatal(err)
		}
	}
	orphans := []string{"data/orphan", "held/orphan", "state/orphan", "catalog/orphan"}
	for _, key := range orphans {
		put(key)
	}
	if err := e.SweepOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if got := metricValue(t, e.metrics.OrphanCandidates); got != float64(len(orphans)) {
		t.Fatalf("first sweep found %v candidates, want %d", got, len(orphans))
	}
	// Publications between the sweeps create and retire objects.
	ingest(t, e, hta.Point{Time: 1 << 30, Value: 1})
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := e.CompactOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put("data/fresh")
	if err := e.SweepOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if got := metricValue(t, e.metrics.OrphansRetired); got != float64(len(orphans)) {
		t.Fatalf("retired %v orphans, want %d", got, len(orphans))
	}
	drain(t, e)
	s.mu.Lock()
	for _, key := range orphans {
		if _, ok := s.objects[key]; ok {
			t.Errorf("orphan %s survived", key)
		}
	}
	if _, ok := s.objects["data/fresh"]; !ok {
		t.Error("object found unreachable once was deleted")
	}
	s.mu.Unlock()
	// Everything the manifest reaches still exists.
	e.mu.Lock()
	snapshot := &Engine{store: s, options: e.options, metrics: e.metrics, state: cloneMaintenanceManifest(e.committed)}
	e.mu.Unlock()
	reach, _, err := snapshot.reachableKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	for key := range reach {
		if _, ok := s.objects[key]; !ok {
			t.Errorf("reachable %s missing", key)
		}
	}
	s.mu.Unlock()
	checkCatalog(t, e)
	if got := query(t, e, req); !proto.Equal(want, got) && len(got.TimeDelta) < len(want.TimeDelta) {
		t.Fatal("history lost")
	}
}

// Staging objects of a pending compaction job are not orphans: recovery
// removes them once the job is fenced.
func TestOrphanSweepKeepsPendingJobStaging(t *testing.T) {
	ctx := context.Background()
	s := &gcStore{memoryStore: newStore()}
	e := maintenanceEngine(t, s, t.TempDir(), true)
	fillCompaction(t, e, 30)
	job, err := e.reserveCompaction(ctx)
	if err != nil || job.ID == "" {
		t.Fatalf("no job: %v", err)
	}
	e.mu.Lock()
	pending := e.state.CompactionJob.Key != ""
	e.mu.Unlock()
	if !pending {
		t.Fatal("reservation did not publish the job")
	}
	staging := job.OutputPrefixes[0] + "0"
	absent := ""
	if _, err := s.Put(ctx, staging, []byte("staged"), &absent); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := e.SweepOrphans(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := metricValue(t, e.metrics.OrphansRetired); got != 0 {
		t.Fatalf("retired %v objects of a pending job", got)
	}
}
