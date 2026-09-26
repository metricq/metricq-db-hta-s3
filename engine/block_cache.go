package engine

import (
	"container/list"
	"sync"

	"github.com/metricq/metricq-db-hta-go/hta"
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
// fresh store GET, gunzip and gob decode.
type dataBlockCache struct {
	mu    sync.Mutex
	bytes int64
	items map[blob]*list.Element
	lru   list.List
}

func newDataBlockCache() *dataBlockCache {
	return &dataBlockCache{items: make(map[blob]*list.Element)}
}

func (c *dataBlockCache) get(key blob) ([]hta.Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		c.lru.MoveToFront(element)
		return element.Value.(cachedBlock).records, true
	}
	return nil, false
}

func (c *dataBlockCache) add(key blob, records []hta.Record, cost int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		c.lru.MoveToFront(element)
		return
	}
	c.items[key] = c.lru.PushFront(cachedBlock{key, records, cost})
	c.bytes += cost
	for c.bytes > dataBlockCacheBytes && c.lru.Len() > 0 {
		oldest := c.lru.Back()
		c.bytes -= oldest.Value.(cachedBlock).cost
		delete(c.items, oldest.Value.(cachedBlock).key)
		c.lru.Remove(oldest)
	}
}
