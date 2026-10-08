package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const catalogFanout = 64

// errCatalogBudget marks an operation that needs more catalog metadata than
// its bound allows; compaction shrinks later jobs instead of failing forever.
var errCatalogBudget = errors.New("catalog metadata byte budget exceeded")

// Leaves aim at about 64 KiB of entries (before compression): a paged
// entry costs its inventory references, an inline entry its descriptors.
const catalogLeafTargetBytes = 64 << 10

// Tree pages written by one update share one pack. A pack stays until no
// page of the published tree references it; once the tree references more
// than catalogTreePacks packs, the next update rewrites the whole tree into
// a fresh pack, which also refills sparse leaves.
const catalogTreePacks = 16

// BlockInfo locates one data block or index page of a stream in an object.
type BlockInfo struct {
	Metric string
	Level  int64
	Entry  indexEntry
	Index  bool
}

// withoutAggregate compares with catalog descriptors, which carry none.
func (b BlockInfo) withoutAggregate() BlockInfo {
	b.Entry = b.Entry.withoutAggregate()
	return b
}

// ObjectInfo is the catalog entry of one object: its live blocks and sizes.
type ObjectInfo struct {
	Inventory         []objectInventoryPage
	Key               string
	Size, LiveBytes   int64
	Blocks            []BlockInfo
	Created, Modified int64
	Target            string // Candidate entry points to the object catalog key.
}
type catalogEdge struct {
	First, Last string
	Ref         blob
}
type catalogNode struct {
	Items    []ObjectInfo
	Children []catalogEdge
}

type catalogWriter struct {
	e      *Engine
	ctx    context.Context
	prefix string
	// Packs of this update, the last one open; pages records each page for
	// the shared cache and the tree page directory.
	packs []*pack
	pages []writtenCatalogPage
}

type writtenCatalogPage struct {
	ref     blob
	node    catalogNode
	encoded int
}

// catalogTreePages remembers the child references of the pages of each
// tree (by prefix), so the packs a tree references can be listed after
// every update without reading its leaves again.
type catalogTreePages struct {
	mu       sync.Mutex
	children map[string]map[[32]byte][]blob
}

func newCatalogTreePages() *catalogTreePages {
	return &catalogTreePages{children: map[string]map[[32]byte][]blob{}}
}

func (t *catalogTreePages) get(prefix string, ref blob) ([]blob, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	children, ok := t.children[prefix][ref.Hash]
	return children, ok
}

func (t *catalogTreePages) remember(prefix string, ref blob, n catalogNode) {
	if t == nil {
		return
	}
	children := make([]blob, len(n.Children))
	for i, edge := range n.Children {
		children[i] = edge.Ref
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.children[prefix] == nil {
		t.children[prefix] = map[[32]byte][]blob{}
	}
	t.children[prefix][ref.Hash] = children
}

// keep drops the pages of a tree that its current version no longer has.
func (t *catalogTreePages) keep(prefix string, pages map[[32]byte]bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for hash := range t.children[prefix] {
		if !pages[hash] {
			delete(t.children[prefix], hash)
		}
	}
}

func (w *catalogWriter) read(ref blob) (catalogNode, error) {
	var n catalogNode
	if err := w.ctx.Err(); err != nil {
		return n, err
	}
	if w.e.sharedCatalog != nil {
		if cached, ok := w.e.sharedCatalog.get(ref); ok {
			w.e.metrics.MetadataCache.WithLabelValues("catalog", "hit").Inc()
			return cached, nil
		}
	}
	if cached, ok := w.e.catalogCache[ref]; ok {
		return cached, nil
	}
	w.e.metrics.MetadataCache.WithLabelValues("catalog", "miss").Inc()
	if ref.Length > 32<<20 {
		return n, fmt.Errorf("catalog page too large")
	}
	if w.e.catalogReadBudget > 0 && w.e.catalogReadBytes+ref.Length > w.e.catalogReadBudget {
		return n, errCatalogBudget
	}
	w.e.catalogReadBytes += ref.Length
	b, err := w.e.readBlob(w.ctx, ref)
	if err != nil {
		return n, err
	}
	err = decode(b, &n)
	if err == nil && ((len(n.Items) == 0) == (len(n.Children) == 0) || len(n.Items) > catalogFanout || len(n.Children) > catalogFanout) {
		err = fmt.Errorf("invalid catalog node")
	}
	if err == nil && w.e.sharedCatalog != nil {
		w.e.sharedCatalog.add(ref, n, len(b))
		return n, nil
	}
	if err == nil {
		cost := int64(len(b))
		for _, o := range n.Items {
			cost += int64(len(o.Blocks))*192 + int64(len(o.Key))
		}
		if cost <= 32<<20 {
			if w.e.catalogCache == nil {
				w.e.catalogCache = make(map[blob]catalogNode)
			}
			if w.e.catalogCacheBytes+cost > 32<<20 {
				w.e.catalogCache = make(map[blob]catalogNode)
				w.e.catalogCacheBytes = 0
			}
			w.e.catalogCache[ref] = n
			w.e.catalogCacheBytes += cost
		}
	}
	return n, err
}
func (w *catalogWriter) write(n catalogNode) (catalogEdge, error) {
	b, err := encode(catalogWire(n))
	if err != nil {
		return catalogEdge{}, err
	}
	if len(b) > 32<<20 {
		return catalogEdge{}, fmt.Errorf("catalog page exceeds memory budget")
	}
	if len(w.packs) == 0 || w.packs[len(w.packs)-1].buf.Len()+len(b) > 4<<20 {
		p, err := w.e.newMetadataPack(w.prefix)
		if err != nil {
			return catalogEdge{}, err
		}
		w.packs = append(w.packs, p)
	}
	ref := w.packs[len(w.packs)-1].add(b)
	w.pages = append(w.pages, writtenCatalogPage{ref: ref, node: n, encoded: len(b)})
	edge := catalogEdge{Ref: ref}
	if len(n.Items) > 0 {
		edge.First = n.Items[0].Key
		edge.Last = n.Items[len(n.Items)-1].Key
	} else {
		edge.First = n.Children[0].First
		edge.Last = n.Children[len(n.Children)-1].Last
	}
	return edge, nil
}

// flush uploads the packs of this update concurrently. Publication still
// waits for every PUT and never retries failed calls.
func (w *catalogWriter) flush() error {
	errs := make([]error, len(w.packs))
	var wg sync.WaitGroup
	for i, p := range w.packs {
		wg.Add(1)
		go func(i int, p *pack) {
			defer wg.Done()
			absent := ""
			_, errs[i] = w.e.put(w.ctx, p.key, p.buf.Bytes(), &absent)
		}(i, p)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	for _, page := range w.pages {
		if w.e.sharedCatalog != nil {
			w.e.sharedCatalog.tree.remember(w.prefix, page.ref, page.node)
			w.e.sharedCatalog.add(page.ref, catalogWire(page.node), page.encoded)
		}
	}
	w.packs, w.pages = nil, nil
	return nil
}

// readCatalogWire reads a tree page as stored: paged entries keep their
// inventory references without loading the descriptors.
func (w *catalogWriter) readCatalogWire(ref blob) (catalogNode, error) {
	var n catalogNode
	if ref.Length > 32<<20 {
		return n, fmt.Errorf("catalog page too large")
	}
	b, err := w.e.readBlob(w.ctx, ref)
	if err != nil {
		return n, err
	}
	if err = decode(b, &n); err != nil {
		return n, err
	}
	if (len(n.Items) == 0) == (len(n.Children) == 0) || len(n.Items) > catalogFanout || len(n.Children) > catalogFanout {
		return n, fmt.Errorf("invalid catalog node")
	}
	return n, nil
}

// treePacks returns the pack keys and page hashes of all pages of a tree.
func (w *catalogWriter) treePacks(root blob) (map[string]bool, map[[32]byte]bool, error) {
	packs, pages := map[string]bool{}, map[[32]byte]bool{}
	var tree *catalogTreePages
	if w.e.sharedCatalog != nil {
		tree = w.e.sharedCatalog.tree
	}
	var walk func(ref blob) error
	walk = func(ref blob) error {
		packs[ref.Key] = true
		pages[ref.Hash] = true
		children, known := tree.get(w.prefix, ref)
		if !known {
			n, ok := catalogNode{}, false
			if w.e.sharedCatalog != nil {
				n, ok = w.e.sharedCatalog.get(ref)
			}
			if !ok {
				var err error
				if n, err = w.readCatalogWire(ref); err != nil {
					return err
				}
			}
			tree.remember(w.prefix, ref, n)
			for _, edge := range n.Children {
				children = append(children, edge.Ref)
			}
		}
		for _, child := range children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if root.Key == "" {
		return packs, pages, nil
	}
	return packs, pages, walk(root)
}

// rebuild writes the whole tree with the updates applied into fresh packs.
func (w *catalogWriter) rebuild(root blob, updates map[string]*ObjectInfo) ([]catalogEdge, error) {
	byKey := map[string]ObjectInfo{}
	var walk func(ref blob) error
	walk = func(ref blob) error {
		n, ok := catalogNode{}, false
		if w.e.sharedCatalog != nil {
			n, ok = w.e.sharedCatalog.get(ref)
		}
		if !ok {
			var err error
			if n, err = w.readCatalogWire(ref); err != nil {
				return err
			}
		}
		for _, o := range n.Items {
			byKey[o.Key] = o
		}
		for _, edge := range n.Children {
			if err := walk(edge.Ref); err != nil {
				return err
			}
		}
		return nil
	}
	if root.Key != "" {
		if err := walk(root); err != nil {
			return nil, err
		}
	}
	for key, v := range updates {
		if v == nil {
			delete(byKey, key)
		} else {
			byKey[key] = *v
		}
	}
	items := make([]ObjectInfo, 0, len(byKey))
	for _, v := range byKey {
		items = append(items, v)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return w.leaves(items)
}

func (w *catalogWriter) leaves(items []ObjectInfo) ([]catalogEdge, error) {
	var edges []catalogEdge
	for i := 0; i < len(items); {
		end, cost := i, 0
		for end < len(items) && end-i < catalogFanout {
			item := items[end]
			// Descriptor counts vary widely between mixed packs. Isolate a
			// large object's inventory instead of recompressing it whenever
			// one of up to 63 unrelated neighboring objects changes.
			next := 128 + len(item.Key) + len(item.Target)
			if len(item.Inventory) > 0 {
				next += 128 * len(item.Inventory)
			} else {
				next += 192 * len(item.Blocks)
			}
			if end > i && next > catalogLeafTargetBytes-cost {
				break
			}
			cost += next
			end++
		}
		edge, err := w.write(catalogNode{Items: items[i:end]})
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
		i = end
	}
	return edges, nil
}
func (w *catalogWriter) branches(edges []catalogEdge) ([]catalogEdge, error) {
	var out []catalogEdge
	for i := 0; i < len(edges); i += catalogFanout {
		edge, err := w.write(catalogNode{Children: edges[i:min(i+catalogFanout, len(edges))]})
		if err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, nil
}
func (w *catalogWriter) apply(ref blob, updates map[string]*ObjectInfo) ([]catalogEdge, error) {
	if ref.Key == "" {
		items := make([]ObjectInfo, 0, len(updates))
		for _, v := range updates {
			if v != nil {
				items = append(items, *v)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
		return w.leaves(items)
	}
	n, err := w.read(ref)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		edge := catalogEdge{Ref: ref}
		if len(n.Items) > 0 {
			edge.First = n.Items[0].Key
			edge.Last = n.Items[len(n.Items)-1].Key
		} else {
			edge.First = n.Children[0].First
			edge.Last = n.Children[len(n.Children)-1].Last
		}
		return []catalogEdge{edge}, nil
	}
	if len(n.Items) > 0 {
		byKey := make(map[string]ObjectInfo, len(n.Items)+len(updates))
		for _, v := range n.Items {
			byKey[v.Key] = v
		}
		for key, v := range updates {
			if v == nil {
				delete(byKey, key)
			} else {
				byKey[key] = *v
			}
		}
		items := make([]ObjectInfo, 0, len(byKey))
		for _, v := range byKey {
			items = append(items, v)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
		return w.leaves(items)
	}
	groups := make([]map[string]*ObjectInfo, len(n.Children))
	for key, v := range updates {
		i := sort.Search(len(n.Children), func(i int) bool { return n.Children[i].Last >= key })
		if i == len(n.Children) {
			i--
		}
		if groups[i] == nil {
			groups[i] = make(map[string]*ObjectInfo)
		}
		groups[i][key] = v
	}
	var edges []catalogEdge
	for i, child := range n.Children {
		if len(groups[i]) == 0 {
			edges = append(edges, child)
			continue
		}
		more, err := w.apply(child.Ref, groups[i])
		if err != nil {
			return nil, err
		}
		edges = append(edges, more...)
	}
	return w.branches(edges)
}

// updateCatalog applies updates to a catalog or candidate tree and returns
// the new root and the packs no page of the new tree references.
func (e *Engine) updateCatalog(ctx context.Context, root blob, updates map[string]*ObjectInfo, prefix string) (blob, []string, error) {
	if len(updates) == 0 {
		return root, nil, nil
	}
	w := catalogWriter{e: e, ctx: ctx, prefix: prefix}
	before, _, err := w.treePacks(root)
	if err != nil {
		return blob{}, nil, err
	}
	var edges []catalogEdge
	if len(before) >= catalogTreePacks {
		e.metrics.CatalogRebuilds.WithLabelValues(prefix).Inc()
		edges, err = w.rebuild(root, updates)
	} else {
		edges, err = w.apply(root, updates)
	}
	if err != nil {
		return blob{}, nil, err
	}
	for len(edges) > 1 {
		edges, err = w.branches(edges)
		if err != nil {
			return blob{}, nil, err
		}
	}
	if err = w.flush(); err != nil {
		return blob{}, nil, err
	}
	next := blob{}
	if len(edges) > 0 {
		next = edges[0].Ref
	}
	after, pages, err := w.treePacks(next)
	if err != nil {
		return blob{}, nil, err
	}
	if e.sharedCatalog != nil {
		e.sharedCatalog.tree.keep(prefix, pages)
	}
	e.metrics.CatalogTreePacks.WithLabelValues(prefix).Set(float64(len(after)))
	e.metrics.CatalogTreePages.WithLabelValues(prefix).Set(float64(len(pages)))
	var retired []string
	for key := range before {
		if !after[key] {
			retired = append(retired, key)
		}
	}
	sort.Strings(retired)
	return next, retired, nil
}
func (e *Engine) catalogGet(ctx context.Context, root blob, key string) (ObjectInfo, bool, error) {
	w := catalogWriter{e: e, ctx: ctx}
	for root.Key != "" {
		n, err := w.read(root)
		if err != nil {
			return ObjectInfo{}, false, err
		}
		if len(n.Items) > 0 {
			i := sort.Search(len(n.Items), func(i int) bool { return n.Items[i].Key >= key })
			if i < len(n.Items) && n.Items[i].Key == key {
				o := n.Items[i]
				if err := e.loadObjectInventory(ctx, &o); err != nil {
					return ObjectInfo{}, false, err
				}
				return o, true, nil
			}
			return ObjectInfo{}, false, nil
		}
		i := sort.Search(len(n.Children), func(i int) bool { return n.Children[i].Last >= key })
		if i == len(n.Children) || key < n.Children[i].First {
			return ObjectInfo{}, false, nil
		}
		root = n.Children[i].Ref
	}
	return ObjectInfo{}, false, nil
}
func (e *Engine) catalogWalk(ctx context.Context, root blob, limit int, visit func(ObjectInfo) bool) error {
	w := catalogWriter{e: e, ctx: ctx}
	pages := 0
	stop := false
	var walk func(blob) error
	walk = func(ref blob) error {
		if ref.Key == "" || stop {
			return nil
		}
		pages++
		if pages > limit {
			return fmt.Errorf("catalog page budget exceeded")
		}
		n, err := w.read(ref)
		if err != nil {
			return err
		}
		for _, item := range n.Items {
			if err := e.loadObjectInventory(ctx, &item); err != nil {
				return err
			}
			if !visit(item) {
				stop = true
				return nil
			}
		}
		for _, child := range n.Children {
			if err := walk(child.Ref); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}

// smallObjectBytes marks objects that consolidation packs together regardless
// of dead bytes; compaction never consolidates above compaction_output_object_bytes/8.
const smallObjectBytes = 512 << 10

func candidateKey(o ObjectInfo) string {
	if o.Size == 0 || len(o.Blocks) == 0 {
		return ""
	}
	fragmented := false
	for _, b := range o.Blocks {
		if !b.Index && b.Entry.Records > 0 && b.Entry.Records <= maxDataBlockRecords/2 {
			fragmented = true
			break
		}
	}
	if o.LiveBytes >= o.Size && !fragmented && o.Size >= smallObjectBytes {
		return ""
	}
	rank := 999 - int(999*(o.Size-o.LiveBytes)/o.Size)
	return fmt.Sprintf("%03d/%s", rank, o.Key)
}

// objectSections counts the contiguous data sections of one object: runs of
// blocks of one stream, adjacent in the object (gaps up to maxCoalescedGap)
// and ascending in time. A query reads each with one range request, so the
// sum over all objects is the request count of reading every stream in full.
func objectSections(blocks []BlockInfo) (sections, bytes int64) {
	data := make([]BlockInfo, 0, len(blocks))
	for _, b := range blocks {
		if !b.Index {
			data = append(data, b)
		}
	}
	sort.Slice(data, func(i, j int) bool { return data[i].Entry.Blob.Offset < data[j].Entry.Blob.Offset })
	for i, b := range data {
		bytes += b.Entry.Blob.Length
		if i > 0 {
			prev := data[i-1]
			if b.Metric == prev.Metric && b.Level == prev.Level && b.Entry.First > prev.Entry.Last && b.Entry.Blob.Offset-prev.Entry.Blob.Offset-prev.Entry.Blob.Length <= maxCoalescedGap {
				continue
			}
		}
		sections++
	}
	return sections, bytes
}

func (e *Engine) catalogChanges(ctx context.Context, next *manifest, changes map[string]*ObjectInfo) ([]string, error) {
	uploads := newInventoryUploads(ctx, func(ctx context.Context, p *pack) error {
		absent := ""
		_, err := e.put(ctx, p.key, p.buf.Bytes(), &absent)
		return err
	})
	defer uploads.close()
	candidates := make(map[string]*ObjectInfo)
	var trash []string
	for key, value := range changes {
		if err := uploads.ctx.Err(); err != nil {
			return nil, uploads.failure()
		}
		old, ok, err := e.catalogGet(ctx, e.state.Catalog, key)
		if err != nil {
			return nil, err
		}
		if ok {
			for _, b := range old.Blocks {
				if next.MaintenanceStatsReady && !b.Index && b.Entry.Records > 0 && b.Entry.Records < maxDataBlockRecords {
					next.SmallBlocks--
					next.SmallBlockBytes -= b.Entry.Blob.Length
				}
			}
			if next.SectionStatsReady {
				sections, bytes := objectSections(old.Blocks)
				next.DataSections -= sections
				next.DataBytes -= bytes
			}
			next.LiveObjectBytes -= old.LiveBytes
			next.StoredObjectBytes -= old.Size
			next.LiveObjects--
			if candidate := candidateKey(old); candidate != "" {
				candidates[candidate] = nil
				if next.MaintenanceStatsReady {
					next.CandidateObjects--
				}
			}
		}
		if value != nil {
			retired, err := e.prepareObjectInventory(value, old, uploads.submit)
			if err != nil {
				return nil, err
			}
			trash = append(trash, retired...)
		} else {
			for key := range inventoryObjects(old.Inventory) {
				trash = append(trash, key)
			}
		}
		if value != nil {
			for _, b := range value.Blocks {
				if next.MaintenanceStatsReady && !b.Index && b.Entry.Records > 0 && b.Entry.Records < maxDataBlockRecords {
					next.SmallBlocks++
					next.SmallBlockBytes += b.Entry.Blob.Length
				}
			}
			if next.SectionStatsReady {
				sections, bytes := objectSections(value.Blocks)
				next.DataSections += sections
				next.DataBytes += bytes
			}
			next.LiveObjectBytes += value.LiveBytes
			next.StoredObjectBytes += value.Size
			next.LiveObjects++
			if candidate := candidateKey(*value); candidate != "" {
				if next.MaintenanceStatsReady {
					next.CandidateObjects++
				}
				// Size lets consolidation find small objects without catalog reads.
				candidates[candidate] = &ObjectInfo{Key: candidate, Target: key, Modified: time.Now().UnixNano(), Size: value.Size}
			}
		}
	}
	// No catalog root can reference an inventory upload that has not succeeded.
	if err := uploads.wait(); err != nil {
		return nil, err
	}
	root, retired, err := e.updateCatalog(ctx, e.state.Catalog, changes, "catalog")
	if err != nil {
		return nil, err
	}
	next.Catalog = root
	trash = append(trash, retired...)
	root, retired, err = e.updateCatalog(ctx, e.state.Candidates, candidates, "candidates")
	if err != nil {
		return nil, err
	}
	next.Candidates = root
	trash = append(trash, retired...)
	return trash, nil
}

// Incremental candidate scan. Exhausting a page budget records a cursor,
// rather than repeatedly scanning only the hottest prefix of a large catalog.
func (e *Engine) catalogScan(ctx context.Context, root blob, after string, limit int, visit func(ObjectInfo) bool) (string, error) {
	w := catalogWriter{e: e, ctx: ctx}
	pages := 0
	stopped := false
	last := after
	var walk func(blob) error
	walk = func(ref blob) error {
		if ref.Key == "" || stopped {
			return nil
		}
		if pages == limit {
			stopped = true
			return nil
		}
		pages++
		n, err := w.read(ref)
		if err != nil {
			return err
		}
		for _, item := range n.Items {
			if item.Key <= after {
				continue
			}
			last = item.Key
			if err := e.loadObjectInventory(ctx, &item); err != nil {
				return err
			}
			if !visit(item) {
				stopped = true
				return nil
			}
		}
		for _, child := range n.Children {
			if child.Last <= after {
				continue
			}
			if err := walk(child.Ref); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return after, err
	}
	if !stopped {
		return "", nil
	}
	return last, nil
}

// Filter each source inventory once rather than scanning/splicing it for every
// retired block. A missing descriptor aborts publication before deleting data.
func (e *Engine) retireCatalogBlocks(ctx context.Context, changes map[string]*ObjectInfo, retired map[blob]bool) error {
	groups := make(map[string]map[blob]bool)
	for ref := range retired {
		if groups[ref.Key] == nil {
			groups[ref.Key] = make(map[blob]bool)
		}
		groups[ref.Key][ref] = true
	}
	for key, refs := range groups {
		o := changes[key]
		if o == nil {
			old, ok, err := e.catalogGet(ctx, e.state.Catalog, key)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("retired object absent from catalog: %s", key)
			}
			o = &old
			changes[key] = o
		}
		kept := make([]BlockInfo, 0, len(o.Blocks))
		for _, b := range o.Blocks {
			if refs[b.Entry.Blob] {
				o.LiveBytes -= b.Entry.Blob.Length
				delete(refs, b.Entry.Blob)
			} else {
				kept = append(kept, b)
			}
		}
		if len(refs) > 0 {
			return fmt.Errorf("retired block absent from catalog: %s", key)
		}
		o.Blocks = kept
	}
	return nil
}
