package engine

import "fmt"

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
