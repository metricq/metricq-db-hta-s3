package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
)

// Tree pages of one update share a pack; retired packs are exactly those no
// page of the new tree references, so deleting them at once (as GC does
// later) never breaks the published tree. The tree stays within its pack
// bound by rebuilding.
func TestCatalogTreeSharesPacksAndRetiresExactly(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	e := openTest(t, s, t.TempDir())
	model := map[string]ObjectInfo{}
	updates := map[string]*ObjectInfo{}
	for i := 0; i < 3000; i++ {
		o := ObjectInfo{Key: fmt.Sprintf("data/%04d", i), Size: int64(i), LiveBytes: int64(i)}
		model[o.Key] = o
		updates[o.Key] = &o
	}
	root, _, err := e.updateCatalog(ctx, blob{}, updates, "catalog")
	if err != nil {
		t.Fatal(err)
	}
	w := catalogWriter{e: e, ctx: ctx, prefix: "catalog"}
	packs, pages, err := w.treePacks(root)
	if err != nil || len(packs) != 1 || len(pages) < 2 {
		t.Fatalf("one update wrote %d packs for %d pages: %v", len(packs), len(pages), err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	rebuilt := false
	for round := 0; round < 60; round++ {
		updates := map[string]*ObjectInfo{}
		for k := 0; k < 1+round%3; k++ {
			key := fmt.Sprintf("data/%04d", r.IntN(3200))
			if _, ok := model[key]; ok && r.IntN(3) == 0 {
				delete(model, key)
				updates[key] = nil
				continue
			}
			o := ObjectInfo{Key: key, Size: int64(round), LiveBytes: int64(k), Modified: int64(round)}
			model[key] = o
			updates[key] = &o
		}
		before, _, err := w.treePacks(root)
		if err != nil {
			t.Fatal(err)
		}
		next, retired, err := e.updateCatalog(ctx, root, updates, "catalog")
		if err != nil {
			t.Fatal(err)
		}
		after, _, err := w.treePacks(next)
		if err != nil {
			t.Fatal(err)
		}
		if len(before) >= catalogTreePacks && len(after) == 1 {
			rebuilt = true
		}
		if len(after) > catalogTreePacks {
			t.Fatalf("round %d: tree references %d packs", round, len(after))
		}
		for _, key := range retired {
			if after[key] || !before[key] {
				t.Fatalf("round %d: retired %s wrongly", round, key)
			}
		}
		for key := range before {
			if !after[key] && !contains(retired, key) {
				t.Fatalf("round %d: unreferenced pack %s not retired", round, key)
			}
		}
		s.mu.Lock()
		for _, key := range retired {
			delete(s.objects, key)
		}
		s.mu.Unlock()
		root = next
		// A reader without caches sees exactly the model.
		fresh := &Engine{store: s, metrics: e.metrics}
		var got []ObjectInfo
		if err := fresh.catalogWalk(ctx, root, math.MaxInt, func(o ObjectInfo) bool { got = append(got, o); return true }); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Key < got[j].Key })
		var want []ObjectInfo
		for _, o := range model {
			want = append(want, o)
		}
		sort.Slice(want, func(i, j int) bool { return want[i].Key < want[j].Key })
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: catalog differs from model (%d vs %d entries)", round, len(got), len(want))
		}
	}
	if !rebuilt {
		t.Fatal("tree never rebuilt")
	}
}

func contains(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// Paged entries count their inventory references, not their descriptors:
// many large objects share a leaf instead of one leaf each. A rebuild
// rewrites such a tree without loading the inventories and keeps them.
func TestCatalogLeavesHoldManyPagedObjects(t *testing.T) {
	ctx := context.Background()
	e := openTest(t, newStore(), t.TempDir())
	next := cloneManifest(e.committed)
	changes := map[string]*ObjectInfo{}
	for i := 0; i < 100; i++ {
		o := &ObjectInfo{Key: fmt.Sprintf("data/%04d", i), Size: 1 << 20}
		for j := 0; j < 800; j++ {
			o.Blocks = append(o.Blocks, BlockInfo{Metric: fmt.Sprintf("m%d", j%50), Entry: indexEntry{First: int64(j), Last: int64(j), Records: 1, Blob: blob{Key: o.Key, Offset: int64(j) * 100, Length: 100}}})
		}
		o.LiveBytes = 80000
		changes[o.Key] = o
	}
	if _, err := e.catalogChanges(ctx, &next, changes); err != nil {
		t.Fatal(err)
	}
	w := catalogWriter{e: e, ctx: ctx, prefix: "catalog"}
	_, pages, err := w.treePacks(next.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) > 10 {
		t.Fatalf("100 paged objects need %d tree pages", len(pages))
	}
	edges, err := w.rebuild(next.Catalog, nil)
	if err != nil {
		t.Fatal(err)
	}
	for len(edges) > 1 {
		if edges, err = w.branches(edges); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	fresh := &Engine{store: e.store, metrics: e.metrics}
	o, found, err := fresh.catalogGet(ctx, edges[0].Ref, "data/0042")
	if err != nil || !found || len(o.Blocks) != 800 || len(o.Inventory) == 0 {
		t.Fatalf("rebuilt entry: found %v, %d blocks, %d inventory pages: %v", found, len(o.Blocks), len(o.Inventory), err)
	}
}
