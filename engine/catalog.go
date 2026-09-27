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

const catalogLeafTargetBytes = 256 << 10

type BlockInfo struct {
	Metric string
	Level  int64
	Entry  indexEntry
	Index  bool
}
type ObjectInfo struct {
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
	e            *Engine
	ctx          context.Context
	prefix       string
	retired      []string
	created      []string
	pending      []*pack
	pendingBytes int
}

func (w *catalogWriter) read(ref blob) (catalogNode, error) {
	var n catalogNode
	if err := w.ctx.Err(); err != nil {
		return n, err
	}
	if cached, ok := w.e.catalogCache[ref]; ok {
		return cached, nil
	}
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
	p, err := w.e.newMetadataPack(w.prefix)
	if err != nil {
		return catalogEdge{}, err
	}
	b, err := encode(n)
	if err != nil {
		return catalogEdge{}, err
	}
	if len(b) > 32<<20 {
		return catalogEdge{}, fmt.Errorf("catalog page exceeds memory budget")
	}
	ref := p.add(b)
	if w.pendingBytes+len(b) > 4<<20 {
		if err = w.flush(); err != nil {
			return catalogEdge{}, err
		}
	}
	w.pending = append(w.pending, p)
	w.pendingBytes += len(b)
	w.created = append(w.created, p.key)
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

// Independent immutable pages upload concurrently with a bounded batch buffer.
// Publication still waits for every PUT and never retries failed calls.
func (w *catalogWriter) flush() error {
	for begin := 0; begin < len(w.pending); begin += 8 {
		batch := w.pending[begin:min(begin+8, len(w.pending))]
		errs := make([]error, len(batch))
		var wg sync.WaitGroup
		for i, p := range batch {
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
	}
	w.pending = nil
	w.pendingBytes = 0
	return nil
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
			next := 128 + len(item.Key) + len(item.Target) + 192*len(item.Blocks)
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
	w.retired = append(w.retired, ref.Key)
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
func (e *Engine) updateCatalog(ctx context.Context, root blob, updates map[string]*ObjectInfo, prefix string) (blob, []string, error) {
	if len(updates) == 0 {
		return root, nil, nil
	}
	w := catalogWriter{e: e, ctx: ctx, prefix: prefix}
	edges, err := w.apply(root, updates)
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
	if len(edges) == 0 {
		return blob{}, w.retired, nil
	}
	return edges[0].Ref, w.retired, nil
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
				return n.Items[i], true, nil
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
	if o.LiveBytes >= o.Size && !fragmented {
		return ""
	}
	rank := 999 - int(999*(o.Size-o.LiveBytes)/o.Size)
	return fmt.Sprintf("%03d/%s", rank, o.Key)
}
func (e *Engine) catalogChanges(ctx context.Context, next *manifest, changes map[string]*ObjectInfo) ([]string, error) {
	candidates := make(map[string]*ObjectInfo)
	var trash []string
	for key, value := range changes {
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
			for _, b := range value.Blocks {
				if next.MaintenanceStatsReady && !b.Index && b.Entry.Records > 0 && b.Entry.Records < maxDataBlockRecords {
					next.SmallBlocks++
					next.SmallBlockBytes += b.Entry.Blob.Length
				}
			}
			next.LiveObjectBytes += value.LiveBytes
			next.StoredObjectBytes += value.Size
			next.LiveObjects++
			if candidate := candidateKey(*value); candidate != "" {
				if next.MaintenanceStatsReady {
					next.CandidateObjects++
				}
				candidates[candidate] = &ObjectInfo{Key: candidate, Target: key, Modified: time.Now().UnixNano()}
			}
		}
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
