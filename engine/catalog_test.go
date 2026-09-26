package engine

import (
	"context"
	"fmt"
	"testing"
)

func TestCatalogIsolatesLargeInventoriesFromSmallUpdates(t *testing.T) {
	ctx := context.Background()
	e := openTest(t, newStore(), t.TempDir())
	changes := map[string]*ObjectInfo{}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("data/%03d", i)
		changes[key] = &ObjectInfo{Key: key, Size: 100, LiveBytes: 100}
	}
	large := changes["data/000"]
	large.Blocks = make([]BlockInfo, 5000)
	for i := range large.Blocks {
		large.Blocks[i] = BlockInfo{Metric: fmt.Sprintf("m%04d", i), Entry: indexEntry{Blob: blob{Key: large.Key, Offset: int64(i) * 100, Length: 100}}}
	}
	root, _, err := e.updateCatalog(ctx, blob{}, changes, "catalog")
	if err != nil {
		t.Fatal(err)
	}
	w := catalogWriter{e: e, ctx: ctx}
	branch, err := w.read(root)
	if err != nil || len(branch.Children) < 2 {
		t.Fatalf("large inventory shares a leaf: %v", err)
	}
	largeLeaf := branch.Children[0].Ref
	leaf, err := w.read(largeLeaf)
	if err != nil || len(leaf.Items) != 1 || leaf.Items[0].Key != large.Key {
		t.Fatalf("large inventory not isolated: %v", err)
	}
	updated := *changes["data/001"]
	updated.Modified = 123
	newRoot, retired, err := e.updateCatalog(ctx, root, map[string]*ObjectInfo{updated.Key: &updated}, "catalog")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range retired {
		if key == largeLeaf.Key {
			t.Fatal("small update rewrote unrelated large inventory")
		}
	}
	got, found, err := e.catalogGet(ctx, newRoot, updated.Key)
	if err != nil || !found || got.Modified != 123 {
		t.Fatalf("updated inventory: %+v %v", got, err)
	}
	old, found, err := e.catalogGet(ctx, root, updated.Key)
	if err != nil || !found || old.Modified != 0 {
		t.Fatalf("old snapshot changed: %+v %v", old, err)
	}
	got, found, err = e.catalogGet(ctx, newRoot, large.Key)
	if err != nil || !found || len(got.Blocks) != 5000 {
		t.Fatalf("large inventory lost descriptors: %v", err)
	}
}
