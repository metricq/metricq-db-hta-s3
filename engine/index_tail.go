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
				e.setTail(stream, path.pages[0], leaf.Entries[len(leaf.Entries)-1])
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

// streamTail records the size of a stream's newest data block. Every stream
// has such an open tail until it fills, so partial tails are reported apart
// from fragments inside a stream, which compaction repairs. Checkpoints use
// the size to complete a partial tail instead of appending behind it.
type streamTail struct {
	root    blob
	records int
}

func (t streamTail) partial() bool { return t.records > 0 && t.records < maxDataBlockRecords }

// setTail records the newest entry of a stream under root. Caller holds mu.
func (e *Engine) setTail(stream string, root blob, last indexEntry) {
	if e.tails == nil {
		e.tails = make(map[string]streamTail)
	}
	e.tails[stream] = streamTail{root: root, records: last.Records}
}

// refreshTails reads the newest entry of the given streams from their current
// roots. Roots replaced meanwhile are skipped: their writer records them.
func (e *Engine) refreshTails(ctx context.Context, roots map[string]blob) error {
	type result struct {
		stream string
		root   blob
		last   indexEntry
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
				last, err := r.lastIndexEntry(ctx, w.root)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				w.last = last
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
			e.setTail(r.stream, r.root, r.last)
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

// updateTailMetrics splits small blocks into open tails and fragments. Tails
// are exact once bootstrapped; the catalog count may briefly lag. Caller holds mu.
func (e *Engine) updateTailMetrics(m manifest) {
	if !e.tailsKnown || !m.MaintenanceStatsReady {
		e.metrics.TailBlocks.Set(math.NaN())
		e.metrics.FragmentBlocks.Set(math.NaN())
		return
	}
	tails := 0
	for stream, tail := range e.tails {
		metric, level := splitStreamKey(stream)
		if tail.partial() && e.state.Roots[metric][level] == tail.root {
			tails++
		}
	}
	e.metrics.TailBlocks.Set(float64(tails))
	e.metrics.FragmentBlocks.Set(float64(max(0, m.SmallBlocks-int64(tails))))
}

// tailGap returns how many records complete the partial tail of a stream under
// root, or zero when the tail is full or unknown. Caller holds mu.
func (e *Engine) tailGap(metric string, level int64, root blob) int {
	tail, ok := e.tails[streamKey(metric, level)]
	if !ok || tail.root != root || !tail.partial() {
		return 0
	}
	return maxDataBlockRecords - tail.records
}
