package cache

import (
	"container/list"
	"sync"
)

// BlockKey uniquely identifies a cached block within an SSTable.
type BlockKey struct {
	SSTID       string
	BlockOffset int64
}

// LRUBlockCache is an in-memory LRU cache for SSTable data blocks.
type LRUBlockCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	ll       *list.List
	items    map[BlockKey]*list.Element
}

type lruEntry struct {
	key  BlockKey
	data []byte
}

// NewLRUBlockCache creates a new LRU block cache with the given max size in bytes.
func NewLRUBlockCache(maxBytes int64) *LRUBlockCache {
	return &LRUBlockCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[BlockKey]*list.Element),
	}
}

// Get retrieves a block from the cache, returning nil if not found.
func (c *LRUBlockCache) Get(key BlockKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		data := ele.Value.(*lruEntry).data
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp
	}
	return nil
}

// Put inserts a block into the cache, evicting old entries as needed.
func (c *LRUBlockCache) Put(key BlockKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		old := ele.Value.(*lruEntry)
		c.curBytes += int64(len(data)) - int64(len(old.data))
		cp := make([]byte, len(data))
		copy(cp, data)
		old.data = cp
	} else {
		cp := make([]byte, len(data))
		copy(cp, data)
		entry := &lruEntry{key: key, data: cp}
		ele := c.ll.PushFront(entry)
		c.items[key] = ele
		c.curBytes += int64(len(data))
	}

	for c.curBytes > c.maxBytes && c.ll.Len() > 0 {
		c.evictOldest()
	}
}

func (c *LRUBlockCache) evictOldest() {
	ele := c.ll.Back()
	if ele == nil {
		return
	}
	entry := ele.Value.(*lruEntry)
	c.ll.Remove(ele)
	delete(c.items, entry.key)
	c.curBytes -= int64(len(entry.data))
}

// Len returns the number of entries in the cache.
func (c *LRUBlockCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Size returns the total bytes currently cached.
func (c *LRUBlockCache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}
