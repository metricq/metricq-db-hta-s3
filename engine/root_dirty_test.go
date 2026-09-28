package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestTrackedRootPagesRecoverChangesAdditionsAndDeletions(t *testing.T) {
	ctx := context.Background()
	e, _ := heldMetadataEngine(t)
	base := cloneMaintenanceManifest(e.committed)
	for i := 0; i < 200; i++ {
		base.Roots[fmt.Sprintf("metric.%04d", i)] = map[int64]blob{0: {Key: "data/test", Length: 1, Offset: int64(i)}}
	}
	base.rootDirtyKnown = true // An empty base root must still write every shard.
	if _, err := e.encodeManifest(ctx, &base, manifest{}); err != nil {
		t.Fatal(err)
	}
	for _, tracked := range []bool{true, false} {
		t.Run(fmt.Sprint("tracked=", tracked), func(t *testing.T) {
			next := cloneManifest(base)
			next.rootDirtyKnown = tracked
			next.rootDirty = map[string]bool{"metric.0000": true, "metric.0001": true, "metric.new": true}
			ref := next.Roots["metric.0000"][0]
			ref.Offset++
			next.Roots["metric.0000"][0] = ref
			delete(next.Roots, "metric.0001")
			next.Roots["metric.new"] = map[int64]blob{100: ref}
			wire, err := e.encodeManifest(ctx, &next, base)
			if err != nil {
				t.Fatal(err)
			}
			for shard, before := range base.rootPages.Pages {
				dirty := shard == metadataShard("metric.0000") || shard == metadataShard("metric.0001") || shard == metadataShard("metric.new")
				if (next.rootPages.Pages[shard] != before) != dirty {
					t.Fatalf("unexpected shard rewrite %d", shard)
				}
			}
			var restored manifest
			if err = decode(wire, &restored); err != nil {
				t.Fatal(err)
			}
			if err = e.loadManifestState(ctx, &restored); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(restored.Roots, next.Roots) {
				t.Fatal("recovered roots differ")
			}
			if next.rootDirtyKnown || len(next.rootDirty) != 0 {
				t.Fatal("transient dirtiness survived serialization")
			}
		})
	}
	unchanged := cloneMaintenanceManifest(base)
	unchanged.rootDirtyKnown = true
	if _, err := e.encodeManifest(ctx, &unchanged, base); err != nil {
		t.Fatal(err)
	}
	if unchanged.StreamIndex != base.StreamIndex || unchanged.rootPages != base.rootPages {
		t.Fatal("unchanged tracked maintenance rewrote roots")
	}
}
