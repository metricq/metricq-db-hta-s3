package engine

import (
	"context"
	"fmt"
)

// Prefetch only known needed pages; bounded batches use the same checksum and
// physical range coalescing as data queries. It does not traverse history.
func (e *Engine) prefetchNodes(ctx context.Context, refs []blob) error {
	seen := map[blob]bool{}
	var missing []blob
	for _, ref := range refs {
		if ref.Key == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		if _, ok := e.tailPages[ref]; ok {
			continue
		}
		if _, ok := e.nodeCache[ref]; ok {
			continue
		}
		if e.sharedNodes != nil {
			if _, ok := e.sharedNodes.get(ref); ok {
				continue
			}
		}
		if e.nodeReadLimit > 0 && e.nodeReads+len(missing) >= e.nodeReadLimit {
			break
		}
		missing = append(missing, ref)
	}
	for start := 0; start < len(missing); start += maxQueryBlockBatch {
		batch := missing[start:min(start+maxQueryBlockBatch, len(missing))]
		encoded, err := e.fetchEncoded(ctx, batch)
		if err != nil {
			return err
		}
		for i, b := range encoded {
			n, err := decodeNode(b)
			if err != nil {
				return fmt.Errorf("prefetch index page: %w", err)
			}
			if e.nodeCache == nil {
				e.nodeCache = map[blob]indexNode{}
			}
			e.nodeCache[batch[i]] = n
			if e.sharedNodes != nil {
				e.sharedNodes.add(batch[i], n)
			}
		}
	}
	return nil
}
