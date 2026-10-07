package engine

import (
	"container/list"
	"sync"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

const dataBlockCacheBytes = 128 << 20

type cachedBlock struct {
	key     blob
	records []hta.Record
	cost    int64
}

// Data blocks are immutable once packed and often re-requested by overlapping
// or auto-refreshing dashboard queries. A bounded, byte-budgeted shared cache
// turns a repeat request for the same block into a memory hit instead of a
// fresh store GET, gunzip and gob decode. Content hashes retain hits across
// copy-only compaction without charging duplicate cache entries.
type dataBlockCache struct {
	mu    sync.Mutex
	bytes int64
	items map[[32]byte]*list.Element
	lru   list.List
}

func newDataBlockCache() *dataBlockCache {
	return &dataBlockCache{items: make(map[[32]byte]*list.Element)}
}

func (c *dataBlockCache) get(key blob) ([]hta.Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key.Hash]; element != nil {
		c.lru.MoveToFront(element)
		return element.Value.(cachedBlock).records, true
	}
	return nil, false
}

// alias makes a cached block also hit under the address of its recompressed
// copy (same records, other bytes).
func (c *dataBlockCache) alias(old, recompressed blob) {
	c.mu.Lock()
	element := c.items[old.Hash]
	c.mu.Unlock()
	if element != nil {
		block := element.Value.(cachedBlock)
		c.add(recompressed, block.records, block.cost)
	}
}

func (c *dataBlockCache) add(key blob, records []hta.Record, cost int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key.Hash]; element != nil {
		c.lru.MoveToFront(element)
		return
	}
	c.items[key.Hash] = c.lru.PushFront(cachedBlock{key, records, cost})
	c.bytes += cost
	for c.bytes > dataBlockCacheBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		c.bytes -= oldest.Value.(cachedBlock).cost
		delete(c.items, oldest.Value.(cachedBlock).key.Hash)
		c.lru.Remove(oldest)
	}
}
