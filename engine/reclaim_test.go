package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type reclaimStore struct {
	*gcStore
	mu        sync.Mutex
	manifests int
	failOnce  map[string]bool
}

func (s *reclaimStore) Put(ctx context.Context, key string, b []byte, version *string) (string, error) {
	if key == "manifest" {
		s.mu.Lock()
		s.manifests++
		s.mu.Unlock()
	}
	return s.gcStore.Put(ctx, key, b, version)
}
func (s *reclaimStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	fail := s.failOnce[key]
	delete(s.failOnce, key)
	s.mu.Unlock()
	if fail {
		return fmt.Errorf("injected delete failure")
	}
	return s.gcStore.Delete(ctx, key)
}
func (s *reclaimStore) objects(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for key := range s.memoryStore.objects {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}

func TestReclaimBatchesJournalPagesPerPublication(t *testing.T) {
	ctx := context.Background()
	s := &reclaimStore{gcStore: &gcStore{memoryStore: newStore()}, failOnce: map[string]bool{}}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), true), testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 2)
	for e.state.TrashHead != e.state.TrashComplete || len(e.state.TrashCleanups) != 0 {
		if err = e.Reclaim(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// 1000 junk objects in large pages, plus 40 tiny pages as produced by flushes.
	var junk []string
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("data/junk-%04d", i)
		if _, err = s.Put(ctx, key, []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
		junk = append(junk, key)
	}
	journal := func(keys []string) {
		t.Helper()
		if err := e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
			next := cloneManifest(snapshot.committed)
			next.Generation++
			return next, snapshot.appendTrash(ctx, &next, keys, 0)
		}); err != nil {
			t.Fatal(err)
		}
	}
	journal(junk[:880])
	for i := 880; i < 1000; i += 3 {
		journal(junk[i:min(i+3, 1000)])
	}
	s.failOnce[junk[900]] = true // A failed DELETE must not skip later keys.
	s.mu.Lock()
	s.manifests = 0
	s.mu.Unlock()
	passes := 0
	for e.state.TrashHead != e.state.TrashComplete || len(e.state.TrashCleanups) != 0 {
		if passes++; passes > 20 {
			t.Fatal("reclamation did not finish")
		}
		if err = e.Reclaim(ctx); err != nil && !strings.Contains(err.Error(), "injected") {
			t.Fatal(err)
		}
	}
	if n := s.objects("data/junk-"); n != 0 {
		t.Fatalf("%d journaled objects survived", n)
	}
	if n := s.objects("trash/"); n != 0 {
		t.Fatalf("%d journal pages survived", n)
	}
	if e.state.TrashObjects != 0 {
		t.Fatalf("trash counter %d", e.state.TrashObjects)
	}
	// 1000 keys in 55 pages previously needed about 60 publications for data
	// keys plus one for every page; now a publication covers up to 256 keys.
	if s.manifests > 8 {
		t.Fatalf("%d manifest publications for 1000 keys", s.manifests)
	}
	t.Logf("%d passes, %d manifest publications", passes, s.manifests)
	checkCatalog(t, e)
}

func TestReclaimFoldsPageCleanupIntoNextPublication(t *testing.T) {
	ctx := context.Background()
	s := &reclaimStore{gcStore: &gcStore{memoryStore: newStore()}}
	e, err := Open(ctx, s, maintenanceOptions(t.TempDir(), true), testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	fillCompaction(t, e, 1)
	for e.state.TrashHead != e.state.TrashComplete || len(e.state.TrashCleanups) != 0 {
		if err = e.Reclaim(ctx); err != nil {
			t.Fatal(err)
		}
	}
	journal := func(prefix string) {
		t.Helper()
		var keys []string
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("data/%s-%d", prefix, i)
			if _, err := s.Put(ctx, key, []byte{1}, nil); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key)
		}
		if err := e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
			next := cloneManifest(snapshot.committed)
			next.Generation++
			return next, snapshot.appendTrash(ctx, &next, keys, 0)
		}); err != nil {
			t.Fatal(err)
		}
	}
	journal("first")
	if err = e.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if e.reclaimDue() {
		t.Fatal("a small journal is due directly after a publication")
	}
	finished := append([]string(nil), e.state.TrashCleanups...)
	if len(finished) != 1 || s.objects(finished[0]) != 1 {
		t.Fatalf("finished page not deferred: %v", finished)
	}
	journal("second")
	s.mu.Lock()
	s.manifests = 0
	s.mu.Unlock()
	if err = e.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if s.manifests != 1 || s.objects(finished[0]) != 0 || s.objects("data/second-") != 0 {
		t.Fatalf("publications=%d, first page=%d, second keys=%d", s.manifests, s.objects(finished[0]), s.objects("data/second-"))
	}
	for _, key := range e.state.TrashCleanups {
		if key == finished[0] {
			t.Fatal("deleted page remains listed")
		}
	}
	// An idle journal clears its remaining list once, then does nothing.
	if err = e.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.state.TrashCleanups) != 0 || s.objects("trash/") != 0 {
		t.Fatal("idle journal cleanup incomplete")
	}
	s.mu.Lock()
	s.manifests = 0
	s.mu.Unlock()
	if err = e.Reclaim(ctx); err != nil || s.manifests != 0 {
		t.Fatalf("idle reclaim published %d manifests: %v", s.manifests, err)
	}
}
