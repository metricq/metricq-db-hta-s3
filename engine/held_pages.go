package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

// Both collections share immutable packs, but have independent copy-on-write
// roots. IDs order descriptors by publication, including age-only checkpoints.
const heldPageFanout = 64
const heldPackBytes = 4 << 20

type heldRoot struct {
	Version               int
	Inventory, Watermarks blob
}
type heldEntry struct {
	Key       string
	Delta     blob
	Watermark int64
}
type heldEdge struct {
	First, Last string
	Ref         blob
}
type heldPage struct {
	Items    []heldEntry
	Children []heldEdge
}

// Unexported hydrated trees are immutable and never included in the manifest.
type heldTree struct {
	ref      blob
	items    []heldEntry
	children []*heldTree
}
type heldTrees struct{ inventory, watermarks *heldTree }

func (n *heldTree) first() string {
	if len(n.items) > 0 {
		return n.items[0].Key
	}
	return n.children[0].first()
}
func (n *heldTree) last() string {
	if len(n.items) > 0 {
		return n.items[len(n.items)-1].Key
	}
	return n.children[len(n.children)-1].last()
}
func heldEntries(n *heldTree, out *[]heldEntry) {
	if n == nil {
		return
	}
	*out = append(*out, n.items...)
	for _, child := range n.children {
		heldEntries(child, out)
	}
}
func heldTreeObjects(n *heldTree, keys map[string]bool) {
	if n == nil {
		return
	}
	keys[n.ref.Key] = true
	for _, child := range n.children {
		heldTreeObjects(child, keys)
	}
}
func heldObjects(root blob, trees heldTrees) map[string]bool {
	keys := map[string]bool{}
	if root.Key != "" {
		keys[root.Key] = true
	}
	heldTreeObjects(trees.inventory, keys)
	heldTreeObjects(trees.watermarks, keys)
	return keys
}

type heldPageWriter struct {
	e      *Engine
	ctx    context.Context
	prefix string
	kind   string
	p      *pack
}

func (w *heldPageWriter) flush() error {
	if w.p == nil {
		return nil
	}
	absent := ""
	_, err := w.e.put(w.ctx, w.p.key, w.p.buf.Bytes(), &absent)
	w.p = nil
	return err
}
func (w *heldPageWriter) add(value any) (blob, error) {
	if err := w.ctx.Err(); err != nil {
		return blob{}, err
	}
	b, err := encode(value)
	if err != nil {
		return blob{}, err
	}
	if len(b) > heldPackBytes {
		return blob{}, fmt.Errorf("held metadata page too large")
	}
	if w.p != nil && w.p.buf.Len()+len(b) > heldPackBytes {
		if err = w.flush(); err != nil {
			return blob{}, err
		}
	}
	if w.p == nil {
		w.p, err = newPack(w.prefix)
		if err != nil {
			return blob{}, err
		}
	}
	return w.p.add(b), nil
}
func (w *heldPageWriter) node(n *heldTree) (*heldTree, error) {
	wire := heldPage{Items: n.items}
	for _, child := range n.children {
		wire.Children = append(wire.Children, heldEdge{child.first(), child.last(), child.ref})
	}
	ref, err := w.add(wire)
	w.e.metrics.MetadataPages.WithLabelValues(w.kind).Inc()
	n.ref = ref
	return n, err
}

// Sorted updates rebuild only affected leaves and their paths. nil means delete.
func (w *heldPageWriter) update(n *heldTree, updates map[string]*heldEntry) ([]*heldTree, error) {
	if len(updates) == 0 {
		if n == nil {
			return nil, nil
		}
		return []*heldTree{n}, nil
	}
	var result []*heldTree
	if n == nil || len(n.items) > 0 {
		values := map[string]heldEntry{}
		if n != nil {
			for _, v := range n.items {
				values[v.Key] = v
			}
		}
		for key, v := range updates {
			if v == nil {
				delete(values, key)
			} else {
				values[key] = *v
			}
		}
		items := make([]heldEntry, 0, len(values))
		for _, v := range values {
			items = append(items, v)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
		for start := 0; start < len(items); start += heldPageFanout {
			child, err := w.node(&heldTree{items: items[start:min(start+heldPageFanout, len(items))]})
			if err != nil {
				return nil, err
			}
			result = append(result, child)
		}
		return result, nil
	}
	groups := make([]map[string]*heldEntry, len(n.children))
	for key, v := range updates {
		i := sort.Search(len(n.children), func(i int) bool { return n.children[i].last() >= key })
		if i == len(n.children) {
			i--
		}
		if groups[i] == nil {
			groups[i] = map[string]*heldEntry{}
		}
		groups[i][key] = v
	}
	var children []*heldTree
	for i, child := range n.children {
		out, err := w.update(child, groups[i])
		if err != nil {
			return nil, err
		}
		children = append(children, out...)
	}
	for start := 0; start < len(children); start += heldPageFanout {
		chunk := children[start:min(start+heldPageFanout, len(children))]
		if len(chunk) == 1 {
			result = append(result, chunk[0])
			continue
		}
		child, err := w.node(&heldTree{children: chunk})
		if err != nil {
			return nil, err
		}
		result = append(result, child)
	}
	return result, nil
}
func (w *heldPageWriter) apply(n *heldTree, desired []heldEntry) (*heldTree, error) {
	old := []heldEntry{}
	heldEntries(n, &old)
	values := map[string]heldEntry{}
	for _, v := range old {
		values[v.Key] = v
	}
	changes := map[string]*heldEntry{}
	for _, v := range desired {
		if before, ok := values[v.Key]; !ok || before != v {
			copy := v
			changes[v.Key] = &copy
		}
		delete(values, v.Key)
	}
	for key := range values {
		changes[key] = nil
	}
	nodes, err := w.update(n, changes)
	if err != nil {
		return nil, err
	}
	for len(nodes) > 1 {
		var parents []*heldTree
		for start := 0; start < len(nodes); start += heldPageFanout {
			parent, err := w.node(&heldTree{children: nodes[start:min(start+heldPageFanout, len(nodes))]})
			if err != nil {
				return nil, err
			}
			parents = append(parents, parent)
		}
		nodes = parents
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	return nodes[0], nil
}
func (e *Engine) writeHeldMetadata(ctx context.Context, next *manifest, base manifest) ([]string, error) {
	next.HeldState, next.heldPages = base.HeldState, base.heldPages
	if reflect.DeepEqual(next.Held, base.Held) && reflect.DeepEqual(next.HeldWatermarks, base.HeldWatermarks) && (base.HeldState.Key != "" || len(next.Held)+len(next.HeldWatermarks) == 0) {
		return nil, nil
	}
	prefix := "held-state"
	if next.stagingNamespace != "" {
		prefix += "/compact-" + next.stagingNamespace
	}
	w := heldPageWriter{e: e, ctx: ctx, prefix: prefix}
	entries := []heldEntry{}
	heldEntries(base.heldPages.inventory, &entries)
	ids := map[string]string{}
	for _, v := range entries {
		ids[v.Delta.Key] = v.Key
	}
	inventory := make([]heldEntry, 0, len(next.Held))
	for i, ref := range next.Held {
		id := ids[ref.Key]
		if id == "" {
			id = fmt.Sprintf("%020d/%020d", next.Generation, i)
		}
		inventory = append(inventory, heldEntry{Key: id, Delta: ref})
	}
	watermarks := make([]heldEntry, 0, len(next.HeldWatermarks))
	for key, v := range next.HeldWatermarks {
		watermarks = append(watermarks, heldEntry{Key: key, Watermark: v})
	}
	var err error
	w.kind = "held-inventory"
	next.heldPages.inventory, err = w.apply(base.heldPages.inventory, inventory)
	if err != nil {
		return nil, err
	}
	w.kind = "held-watermarks"
	next.heldPages.watermarks, err = w.apply(base.heldPages.watermarks, watermarks)
	if err != nil {
		return nil, err
	}
	next.HeldState = blob{}
	if next.heldPages.inventory != nil || next.heldPages.watermarks != nil {
		root := heldRoot{Version: 1}
		if next.heldPages.inventory != nil {
			root.Inventory = next.heldPages.inventory.ref
		}
		if next.heldPages.watermarks != nil {
			root.Watermarks = next.heldPages.watermarks.ref
		}
		next.HeldState, err = w.add(root)
		if err != nil {
			return nil, err
		}
	}
	if err = w.flush(); err != nil {
		return nil, err
	}
	live := heldObjects(next.HeldState, next.heldPages)
	var obsolete []string
	for key := range heldObjects(base.HeldState, base.heldPages) {
		if !live[key] {
			obsolete = append(obsolete, key)
		}
	}
	return obsolete, nil
}
func (e *Engine) readHeldTree(ctx context.Context, ref blob, depth int, seen map[blob]bool, inventory bool) (*heldTree, error) {
	if ref.Key == "" {
		return nil, nil
	}
	if ref.Length > heldPackBytes {
		return nil, fmt.Errorf("held metadata page too large")
	}
	b, err := e.readBlob(ctx, ref)
	if err != nil {
		return nil, err
	}
	return e.readHeldPage(ctx, ref, b, depth, seen, inventory)
}
func (e *Engine) readHeldPage(ctx context.Context, ref blob, b []byte, depth int, seen map[blob]bool, inventory bool) (*heldTree, error) {
	if depth > 32 || seen[ref] {
		return nil, fmt.Errorf("invalid held metadata tree")
	}
	seen[ref] = true
	var page heldPage
	if err := decode(b, &page); err != nil {
		return nil, err
	}
	if (len(page.Items) == 0) == (len(page.Children) == 0) || len(page.Items) > heldPageFanout || len(page.Children) > heldPageFanout {
		return nil, fmt.Errorf("invalid held metadata page")
	}
	n := &heldTree{ref: ref, items: page.Items}
	for i, v := range page.Items {
		if v.Key == "" || (i > 0 && page.Items[i-1].Key >= v.Key) || (inventory && v.Delta.Key == "") || (!inventory && v.Delta != (blob{})) {
			return nil, fmt.Errorf("invalid held metadata entry")
		}
	}
	refs := make([]blob, len(page.Children))
	for i, edge := range page.Children {
		if edge.Ref.Key == "" || edge.Ref.Length <= 0 || edge.Ref.Length > heldPackBytes {
			return nil, fmt.Errorf("invalid held metadata child")
		}
		refs[i] = edge.Ref
	}
	blocks, err := e.fetchEncoded(ctx, refs)
	if err != nil {
		return nil, err
	}
	for i, edge := range page.Children {
		child, err := e.readHeldPage(ctx, edge.Ref, blocks[i], depth+1, seen, inventory)
		if err != nil {
			return nil, err
		}
		if child == nil || edge.First != child.first() || edge.Last != child.last() || (i > 0 && page.Children[i-1].Last >= edge.First) {
			return nil, fmt.Errorf("invalid held metadata edge")
		}
		n.children = append(n.children, child)
	}
	return n, nil
}
func (e *Engine) loadHeldMetadata(ctx context.Context, m *manifest) error {
	if m.HeldState.Key == "" {
		return nil
	}
	b, err := e.readBlob(ctx, m.HeldState)
	if err != nil {
		return err
	}
	var root heldRoot
	if err = decode(b, &root); err != nil {
		return err
	}
	if root.Version != 1 {
		return fmt.Errorf("unsupported held metadata version %d", root.Version)
	}
	if root.Inventory.Key == "" && root.Watermarks.Key == "" {
		return fmt.Errorf("empty held metadata root")
	}
	m.heldPages.inventory, err = e.readHeldTree(ctx, root.Inventory, 0, map[blob]bool{}, true)
	if err != nil {
		return err
	}
	m.heldPages.watermarks, err = e.readHeldTree(ctx, root.Watermarks, 0, map[blob]bool{}, false)
	if err != nil {
		return err
	}
	entries := []heldEntry{}
	heldEntries(m.heldPages.inventory, &entries)
	m.Held = nil
	keys := map[string]bool{}
	for _, v := range entries {
		if keys[v.Delta.Key] {
			return fmt.Errorf("duplicate held descriptor")
		}
		keys[v.Delta.Key] = true
		m.Held = append(m.Held, v.Delta)
	}
	entries = nil
	heldEntries(m.heldPages.watermarks, &entries)
	m.HeldWatermarks = map[string]int64{}
	for _, v := range entries {
		m.HeldWatermarks[v.Key] = v.Watermark
	}
	return nil
}
