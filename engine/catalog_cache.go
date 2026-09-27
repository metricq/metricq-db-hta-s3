package engine

import (
	"container/list"
	"sync"
)

const catalogCacheLimit = 32 << 20

type cachedCatalogPage struct {
	hash [32]byte
	node catalogNode
	cost int64
}

// Immutable decoded pages are shared across selection, publication and flush.
// The byte bound includes decoded inventories, not only compressed bytes.
type catalogPageCache struct {
	mu    sync.Mutex
	pages map[[32]byte]*list.Element
	lru   list.List
	bytes int64
}

func newCatalogPageCache() *catalogPageCache {
	return &catalogPageCache{pages: map[[32]byte]*list.Element{}}
}
func catalogPageCost(n catalogNode, encoded int) int64 {
	cost := int64(encoded) + int64(len(n.Children))*128
	for _, edge := range n.Children {
		cost += int64(len(edge.First) + len(edge.Last) + len(edge.Ref.Key))
	}
	for _, o := range n.Items {
		cost += int64(len(o.Inventory)) * 128
		for _, page := range o.Inventory {
			cost += int64(len(page.Ref.Key))
		}
		cost += int64(len(o.Blocks))*192 + int64(len(o.Key)+len(o.Target)) + 128
		for _, b := range o.Blocks {
			cost += int64(len(b.Metric) + len(b.Entry.Blob.Key))
		}
	}
	return cost
}
func (c *catalogPageCache) get(ref blob) (catalogNode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.pages[ref.Hash]; p != nil {
		c.lru.MoveToFront(p)
		return p.Value.(cachedCatalogPage).node, true
	}
	return catalogNode{}, false
}
func (c *catalogPageCache) add(ref blob, n catalogNode, encoded int) {
	cost := catalogPageCost(n, encoded)
	if cost > catalogCacheLimit {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.pages[ref.Hash]; p != nil {
		c.lru.MoveToFront(p)
		return
	}
	p := c.lru.PushFront(cachedCatalogPage{ref.Hash, n, cost})
	c.pages[ref.Hash] = p
	c.bytes += cost
	for c.bytes > catalogCacheLimit {
		p := c.lru.Back()
		v := p.Value.(cachedCatalogPage)
		delete(c.pages, v.hash)
		c.bytes -= v.cost
		c.lru.Remove(p)
	}
}
