package engine

import (
	"container/list"
	"sync"
)

const indexCachePages = 8192

type cachedPage struct {
	key  blob
	node indexNode
}

// Index pages are immutable and content-addressed by their checked range.
// A bounded shared cache avoids fetching the same roots for every request.
type indexPageCache struct {
	mu    sync.Mutex
	pages map[[32]byte]*list.Element
	lru   list.List
}

func newIndexPageCache() *indexPageCache {
	return &indexPageCache{pages: make(map[[32]byte]*list.Element)}
}

func (c *indexPageCache) get(key blob) (indexNode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.pages[key.Hash]; element != nil {
		c.lru.MoveToFront(element)
		return element.Value.(cachedPage).node, true
	}
	return indexNode{}, false
}

func (c *indexPageCache) add(key blob, node indexNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.pages[key.Hash]; element != nil {
		c.lru.MoveToFront(element)
		return
	}
	c.pages[key.Hash] = c.lru.PushFront(cachedPage{key, node})
	if c.lru.Len() > indexCachePages {
		oldest := c.lru.Back()
		delete(c.pages, oldest.Value.(cachedPage).key.Hash)
		c.lru.Remove(oldest)
	}
}
