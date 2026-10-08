//go:build review

package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// bucketSnapshot is everything one published manifest reaches.
type bucketSnapshot struct {
	m             manifest
	reach         map[string]string // key -> why
	pendingDelete map[string]bool   // keys in live trash pages
	trashPages    int
	objects       map[string]bool
	catalogBlocks map[[32]byte]int32 // block hash -> stream id
	liveBytes     int64
	candidates    int
	staleCands    int
}

func auditSnapshot(ctx context.Context, s storage.Store) (*bucketSnapshot, *Engine, error) {
	e := &Engine{store: s, metrics: NewMetrics(prometheus.NewRegistry()), nodeCache: make(map[blob]indexNode)}
	b, _, err := e.get(ctx, "manifest")
	if err != nil {
		return nil, nil, err
	}
	if err = decode(b, &e.state); err != nil {
		return nil, nil, err
	}
	if err = e.loadManifestState(ctx, &e.state); err != nil {
		return nil, nil, err
	}
	m := e.state
	r := &bucketSnapshot{m: m, reach: map[string]string{"manifest": "manifest"}, pendingDelete: map[string]bool{}, objects: map[string]bool{}, catalogBlocks: map[[32]byte]int32{}}
	mark := func(key, why string) {
		if key != "" && r.reach[key] == "" {
			r.reach[key] = why
		}
	}
	for k := range metadataObjects(m.CheckpointState, m.seriesPages) {
		mark(k, "state pages")
	}
	for k := range metadataObjects(m.StreamIndex, m.rootPages) {
		mark(k, "root pages")
	}
	for k := range heldObjects(m.HeldState, m.heldPages) {
		mark(k, "held metadata")
	}
	for _, ref := range m.Held {
		mark(ref.Key, "held delta")
	}
	mark(m.CompactionJob.Key, "compaction job")
	for k := range m.Garbage {
		mark(k, "garbage queue")
	}
	for _, k := range m.TrashCleanups {
		mark(k, "finished trash page")
	}
	// Pages newer than TrashComplete are live; their keys await deletion.
	for ref := m.TrashHead; ref.Key != "" && ref != m.TrashComplete; r.trashPages++ {
		mark(ref.Key, "trash page")
		raw, err := e.readBlob(ctx, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("trash page %s: %w", ref.Key, err)
		}
		var p trashPage
		if err = decode(raw, &p); err != nil {
			return nil, nil, err
		}
		for _, k := range p.Keys {
			r.pendingDelete[k] = true
		}
		ref = p.Prev
	}
	// TrashComplete only marks the end of the finished journal; its page is
	// deleted with the cleanups and never read again.
	mark(m.TrashBatch.Key, "trash page (batch)")
	mark(m.TrashPending.Key, "trash page (pending)")
	w := catalogWriter{e: e, ctx: ctx}
	var walk func(ref blob, why string, visit func(ObjectInfo)) error
	walk = func(ref blob, why string, visit func(ObjectInfo)) error {
		if ref.Key == "" {
			return nil
		}
		mark(ref.Key, why)
		n, err := w.read(ref)
		if err != nil {
			return err
		}
		for _, item := range n.Items {
			visit(item)
		}
		for _, child := range n.Children {
			if err := walk(child.Ref, why, visit); err != nil {
				return err
			}
		}
		return nil
	}
	// read() loads paged inventories into Blocks and keeps their references.
	if err := walk(m.Catalog, "catalog pages", func(o ObjectInfo) {
		for _, page := range o.Inventory {
			mark(page.Ref.Key, "object inventory")
		}
		mark(o.Key, "catalog object")
		r.objects[o.Key] = true
		r.liveBytes += o.LiveBytes
		for _, bi := range o.Blocks {
			r.catalogBlocks[bi.Entry.Blob.Hash] = streamID(bi.Metric, bi.Level)
		}
	}); err != nil {
		return nil, nil, err
	}
	if err := walk(m.Candidates, "candidate pages", func(o ObjectInfo) {
		r.candidates++
		target := o.Target
		if target == "" {
			target = o.Key
		}
		if _, ok := r.objects[target]; !ok {
			r.staleCands++
		}
	}); err != nil {
		return nil, nil, err
	}
	return r, e, nil
}

// Stream ids keep the per-block maps small (about 500,000 blocks).
var streamIDs = map[string]int32{}
var streamNames []string

func streamID(metric string, level int64) int32 {
	name := fmt.Sprintf("%s/%d", metric, level)
	id, ok := streamIDs[name]
	if !ok {
		id = int32(len(streamNames))
		streamIDs[name] = id
		streamNames = append(streamNames, name)
	}
	return id
}

// auditSnapshotRetry retries a snapshot that raced with a deletion.
func auditSnapshotRetry(ctx context.Context, s storage.Store) (*bucketSnapshot, *Engine, error) {
	for attempt := 0; ; attempt++ {
		r, e, err := auditSnapshot(ctx, s)
		if err == nil || attempt == 4 || !(errors.Is(err, storage.ErrNotFound) || strings.Contains(err.Error(), "NoSuchKey")) {
			return r, e, err
		}
	}
}

// Read-only audit of a live bucket: lists every key below the prefix and
// compares it with everything the published manifest reaches (metadata
// pages, catalog and candidate trees, object inventories, held deltas, the
// trash journal and its pending keys, every index page and data block). It
// reports orphans, dangling references and catalog/index mismatches. The
// database keeps running: a second manifest snapshot after the listing
// separates races (objects written or deleted meanwhile) from real findings.
// It issues only LIST, HEAD and GET requests.
//
//	METRICQ_LIVE_S3_ENDPOINT=http://127.0.0.1:19001 METRICQ_LIVE_S3_BUCKET=metricq \
//	METRICQ_LIVE_S3_PREFIX=db-hta-s3-dummy AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//	go test -tags review ./engine -run TestReviewLiveBucketAudit -v
func TestReviewLiveBucketAudit(t *testing.T) {
	endpoint := os.Getenv("METRICQ_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set METRICQ_LIVE_S3_ENDPOINT")
	}
	ctx := context.Background()
	s, err := storage.NewS3(ctx, storage.S3Config{Bucket: os.Getenv("METRICQ_LIVE_S3_BUCKET"), Prefix: os.Getenv("METRICQ_LIVE_S3_PREFIX"), Endpoint: endpoint, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	a, e, err := auditSnapshotRetry(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	// Every index tree of snapshot A: pages and data blocks must be catalog
	// blocks. Missing objects are rechecked against snapshot B.
	indexBlocks := map[[32]byte]int32{}
	missingStreams := map[string]error{}
	var walkIndex func(ref blob, stream int32) error
	walkIndex = func(ref blob, stream int32) error {
		indexBlocks[ref.Hash] = stream
		n, err := e.readNode(ctx, ref)
		if err != nil {
			return err
		}
		for _, entry := range n.Entries {
			if n.Leaf {
				indexBlocks[entry.Blob.Hash] = stream
			} else if err := walkIndex(entry.Blob, stream); err != nil {
				return err
			}
		}
		return nil
	}
	streams := 0
	for metric, levels := range a.m.Roots {
		for level, root := range levels {
			if root.Key == "" {
				continue
			}
			streams++
			if err := walkIndex(root, streamID(metric, level)); err != nil {
				if !errors.Is(err, storage.ErrNotFound) && !strings.Contains(err.Error(), "NoSuchKey") {
					t.Fatalf("index of %s level %d: %v", metric, level, err)
				}
				missingStreams[fmt.Sprintf("%s\x00%d", metric, level)] = err
			}
			e.nodeCache = make(map[blob]indexNode)
		}
	}
	var keys []string
	token := ""
	for {
		page, next, err := storage.Lister(s).List(ctx, "", token, 1000)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, page...)
		if next == "" {
			break
		}
		token = next
	}
	b, e2, err := auditSnapshotRetry(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	// Streams whose walk hit a deleted object: a race if their root moved,
	// otherwise walk them again in snapshot B to confirm.
	realMissing := map[string]error{}
	for k, werr := range missingStreams {
		parts := strings.SplitN(k, "\x00", 2)
		var level int64
		fmt.Sscan(parts[1], &level)
		if a.m.Roots[parts[0]][level] != b.m.Roots[parts[0]][level] {
			continue
		}
		e = e2
		if err := walkIndex(b.m.Roots[parts[0]][level], streamID(parts[0], level)); err != nil {
			realMissing[parts[0]+"/"+parts[1]] = fmt.Errorf("%v (first walk: %v)", err, werr)
		}
	}
	notInCatalog, notInIndex := 0, 0
	var notInCatalogEx, notInIndexEx []string
	for hash, stream := range indexBlocks {
		if _, ok := a.catalogBlocks[hash]; !ok {
			if _, ok := b.catalogBlocks[hash]; !ok {
				notInCatalog++
				if len(notInCatalogEx) < 5 {
					notInCatalogEx = append(notInCatalogEx, fmt.Sprintf("%x (%s)", hash[:6], streamNames[stream]))
				}
			}
		}
	}
	for hash, stream := range a.catalogBlocks {
		if _, ok := indexBlocks[hash]; !ok {
			// Retired by a publication after snapshot A?
			if _, still := b.catalogBlocks[hash]; !still {
				continue
			}
			name := streamNames[stream]
			i := strings.LastIndexByte(name, '/')
			if _, moved := missingStreams[name[:i]+"\x00"+name[i+1:]]; moved {
				continue
			}
			notInIndex++
			if len(notInIndexEx) < 5 {
				notInIndexEx = append(notInIndexEx, fmt.Sprintf("%x (%s)", hash[:6], name))
			}
		}
	}
	type group struct {
		count int
		bytes int64
		ex    []string
	}
	prefixOf := func(k string) string {
		i := strings.IndexByte(k, '/')
		if i <= 0 {
			return k
		}
		if strings.HasPrefix(k, "data/compact-") {
			if strings.Contains(k, "/locality/") {
				return "data/compact-*/locality"
			}
			return "data/compact-*"
		}
		return k[:i]
	}
	add := func(m map[string]*group, k string, size int64) {
		p := prefixOf(k)
		if m[p] == nil {
			m[p] = &group{}
		}
		g := m[p]
		g.count++
		g.bytes += size
		if len(g.ex) < 3 {
			g.ex = append(g.ex, k)
		}
	}
	all, orphans := map[string]*group{}, map[string]*group{}
	listed := map[string]bool{}
	pendingPresent := 0
	var listedBytes int64
	for _, k := range keys {
		size, err := e2.objectSize(ctx, k)
		if err != nil {
			continue // deleted since the listing
		}
		listed[k] = true
		listedBytes += size
		add(all, k, size)
		switch {
		case a.reach[k] != "" || b.reach[k] != "":
		case a.pendingDelete[k] || b.pendingDelete[k]:
			pendingPresent++
		default:
			add(orphans, k, size)
		}
	}
	var dangling []string
	for k, why := range b.reach {
		if !listed[k] && !b.pendingDelete[k] {
			if _, err := e2.objectSize(ctx, k); err != nil {
				dangling = append(dangling, k+" ("+why+")")
			}
		}
	}
	// Publications after snapshot B may have retired those keys already:
	// keep only keys a third snapshot still reaches and that are missing.
	if len(dangling) > 0 {
		c, e3, err := auditSnapshotRetry(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("dangling candidates in snapshot B (generation %d): %v; rechecked in generation %d\n", b.m.Generation, dangling, c.m.Generation)
		dangling = dangling[:0]
		for k, why := range c.reach {
			if _, err := e3.objectSize(ctx, k); err != nil && !c.pendingDelete[k] {
				dangling = append(dangling, k+" ("+why+")")
			}
		}
	}
	sort.Strings(dangling)
	print := func(title string, m map[string]*group) {
		fmt.Printf("%s:\n", title)
		var ps []string
		for p := range m {
			ps = append(ps, p)
		}
		sort.Strings(ps)
		for _, p := range ps {
			g := m[p]
			fmt.Printf("  %-26s %6d objects %10.1f MB  e.g. %s\n", p, g.count, float64(g.bytes)/1e6, strings.Join(g.ex, ", "))
		}
	}
	fmt.Printf("manifest generations %d / %d, %d streams, %d catalog objects (%.1f MB live), %d candidates (%d without catalog object)\n", a.m.Generation, b.m.Generation, streams, len(b.objects), float64(b.liveBytes)/1e6, b.candidates, b.staleCands)
	fmt.Printf("trash journal: %d live pages, %d keys pending deletion (%d still present), TrashObjects %d, cleanups %d, garbage %d\n", b.trashPages, len(b.pendingDelete), pendingPresent, b.m.TrashObjects, len(b.m.TrashCleanups), len(b.m.Garbage))
	fmt.Printf("listed %d objects, %.1f MB; reachable %d keys\n", len(listed), float64(listedBytes)/1e6, len(b.reach))
	print("listed by prefix", all)
	print("orphans (listed, unreachable in both snapshots, not pending deletion)", orphans)
	fmt.Printf("dangling (reachable in snapshot B, missing): %d %v\n", len(dangling), dangling[:min(len(dangling), 10)])
	fmt.Printf("streams with missing index objects: %d raced, %d real %v\n", len(missingStreams)-len(realMissing), len(realMissing), realMissing)
	fmt.Printf("index blocks/pages %d, catalog blocks %d; in index but in neither catalog %d %v; in catalog but not index %d %v\n", len(indexBlocks), len(a.catalogBlocks), notInCatalog, notInCatalogEx, notInIndex, notInIndexEx)
}
