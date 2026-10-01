package engine

import (
	"context"
	"fmt"
	"math"
	"sync"
)

// maxTailEntries bounds the pinned rightmost index paths (about 90 bytes per
// decoded entry). Streams beyond it fall back to the shared cache and store.
const maxTailEntries = 1 << 19

// Every flush appends to the rightmost path of each changed stream. The
// shared LRU cache does not retain these pages across flushes at high stream
// counts, so each checkpoint would re-read them serially under the ingestion
// mutex. The flush that wrote a path keeps its decoded pages instead. Pages are
// immutable and matched by exact reference, so a root replaced by compaction
// simply misses and is read once from the store.
type tailPath struct {
	pages   []blob
	entries int
}

func streamKey(metric string, level int64) string {
	return fmt.Sprintf("%s\x00%d", metric, level)
}

// rightmostPath collects the new root's rightmost pages from this flush's pack.
func rightmostPath(root blob, nodes map[blob]indexNode) tailPath {
	var path tailPath
	for ref := root; ; {
		n, ok := nodes[ref]
		if !ok {
			break
		}
		path.pages = append(path.pages, ref)
		path.entries += len(n.Entries)
		if n.Leaf {
			break
		}
		ref = n.Entries[len(n.Entries)-1].Blob
	}
	return path
}

// Caller holds mu after a successful commit.
func (e *Engine) pinTailPaths(staged map[string]tailPath, nodes map[blob]indexNode) {
	if e.tailPages == nil {
		e.tailPages = make(map[blob]indexNode)
		e.tailPaths = make(map[string]tailPath)
	}
	for stream, path := range staged {
		if len(path.pages) > 0 {
			if leaf, ok := nodes[path.pages[len(path.pages)-1]]; ok && leaf.Leaf && len(leaf.Entries) > 0 {
				e.setTail(stream, path.pages[0], leaf.Entries)
			}
		}
		if old, ok := e.tailPaths[stream]; ok {
			for _, ref := range old.pages {
				delete(e.tailPages, ref)
			}
			e.tailEntries -= old.entries
			delete(e.tailPaths, stream)
		}
		if len(path.pages) == 0 || e.tailEntries+path.entries > maxTailEntries {
			continue
		}
		for _, ref := range path.pages {
			e.tailPages[ref] = nodes[ref]
		}
		e.tailPaths[stream] = path
		e.tailEntries += path.entries
	}
}

// Caller holds mu. Pinned roots of unchanged streams tell compaction selection
// which streams consist of a single block, independent of shared-cache warmth.
func (e *Engine) singletonTailRoots() map[blob]bool {
	roots := make(map[blob]bool)
	for _, path := range e.tailPaths {
		if path.entries == 1 {
			roots[path.pages[0]] = true
		}
	}
	return roots
}

// streamTail describes the open suffix of a stream: its newest partial
// blocks that together still fit into one block (a partial tail, possibly
// followed by a block whose merge is deferred). Such blocks fill or merge in
// time and are reported apart from fragments inside a stream, which
// compaction repairs. Checkpoints complete the suffix to a full block instead
// of appending behind it.
type streamTail struct {
	root    blob
	blocks  int // partial blocks in the open suffix
	records int // records in the open suffix
}

func (t streamTail) partial() bool { return t.blocks > 0 }

// openSuffix returns the trailing partial entries that fit into one block.
func openSuffix(entries []indexEntry) (blocks, records int) {
	for i := len(entries) - 1; i >= 0; i-- {
		n := entries[i].Records
		if n <= 0 || n >= maxDataBlockRecords || records+n > maxDataBlockRecords {
			break
		}
		blocks++
		records += n
	}
	return blocks, records
}

// setTail records the open suffix of a stream under root from its rightmost
// leaf entries. A suffix reaching into the previous leaf is undercounted;
// the fragment scan sees the full suffix. Caller holds mu.
func (e *Engine) setTail(stream string, root blob, leaf []indexEntry) {
	if e.tails == nil {
		e.tails = make(map[string]streamTail)
	}
	blocks, records := openSuffix(leaf)
	e.tailBlocks += blocks - e.tails[stream].blocks
	e.tails[stream] = streamTail{root: root, blocks: blocks, records: records}
}

// rightmostLeaf returns the entries of a stream's newest index leaf.
func (e *Engine) rightmostLeaf(ctx context.Context, ptr blob) ([]indexEntry, error) {
	for ptr.Key != "" {
		n, err := e.readNode(ctx, ptr)
		if err != nil {
			return nil, err
		}
		if n.Leaf {
			return n.Entries, nil
		}
		ptr = n.Entries[len(n.Entries)-1].Blob
	}
	return nil, nil
}

// refreshTails reads the newest entry of the given streams from their current
// roots. Roots replaced meanwhile are skipped: their writer records them.
func (e *Engine) refreshTails(ctx context.Context, roots map[string]blob) error {
	type result struct {
		stream string
		root   blob
		leaf   []indexEntry
	}
	work := make(chan result)
	results := make(chan result)
	var errOnce sync.Once
	var firstErr error
	var workers sync.WaitGroup
	for i := 0; i < maxParallelBlockFetches; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r := &Engine{store: e.store, metrics: e.metrics, options: e.options, sharedNodes: e.sharedNodes, nodeCache: make(map[blob]indexNode)}
			for w := range work {
				leaf, err := r.rightmostLeaf(ctx, w.root)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				w.leaf = leaf
				results <- w
			}
		}()
	}
	go func() {
		defer close(work)
		for stream, root := range roots {
			if ctx.Err() != nil {
				return
			}
			work <- result{stream: stream, root: root}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	for r := range results {
		metric, level := splitStreamKey(r.stream)
		e.mu.Lock()
		if e.state.Roots[metric][level] == r.root {
			e.setTail(r.stream, r.root, r.leaf)
		}
		e.mu.Unlock()
	}
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// bootstrapTails classifies the tail of every stream once after startup.
func (e *Engine) bootstrapTails(ctx context.Context) error {
	roots := make(map[string]blob)
	e.mu.Lock()
	for metric, levels := range e.state.Roots {
		for level, root := range levels {
			stream := streamKey(metric, level)
			if tail, ok := e.tails[stream]; root.Key != "" && (!ok || tail.root != root) {
				roots[stream] = root
			}
		}
	}
	e.mu.Unlock()
	if err := e.refreshTails(ctx, roots); err != nil {
		return err
	}
	e.mu.Lock()
	e.tailsKnown = true
	e.updateTailMetrics(e.state)
	e.mu.Unlock()
	return nil
}

// updateTailMetrics splits small blocks into open tails and fragments. Every
// root change records its stream's tail, so a running total suffices; it runs
// after each ingest batch. The catalog count may briefly lag. Caller holds mu.
func (e *Engine) updateTailMetrics(m manifest) {
	if !e.tailsKnown || !m.MaintenanceStatsReady {
		e.metrics.TailBlocks.Set(math.NaN())
		e.metrics.FragmentBlocks.Set(math.NaN())
		return
	}
	tails := e.tailBlocks
	e.metrics.TailBlocks.Set(float64(tails))
	e.metrics.FragmentBlocks.Set(float64(max(0, m.SmallBlocks-int64(tails))))
}

// tailGap returns how many records complete the open suffix of a stream under
// root to a full block, or zero when there is none or it is unknown. Caller
// holds mu.
func (e *Engine) tailGap(metric string, level int64, root blob) int {
	tail, ok := e.tails[streamKey(metric, level)]
	if !ok || tail.root != root || !tail.partial() {
		return 0
	}
	return maxDataBlockRecords - tail.records
}
