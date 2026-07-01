package cache

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DiskCache stores SSTables on local disk with LRU eviction.
type DiskCache struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64
	curBytes int64
	ll       *list.List
	items    map[string]*list.Element
}

type diskEntry struct {
	key  string
	size int64
}

// NewDiskCache creates a new disk-based SSTable cache.
func NewDiskCache(dir string, maxBytes int64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating cache dir: %w", err)
	}
	return &DiskCache{
		dir:      dir,
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}, nil
}

func (c *DiskCache) path(key string) string {
	return filepath.Join(c.dir, key)
}

// Get retrieves the full SSTable data from disk cache.
func (c *DiskCache) Get(key string) ([]byte, error) {
	c.mu.Lock()
	ele, ok := c.items[key]
	if ok {
		c.ll.MoveToFront(ele)
	}
	c.mu.Unlock()

	if !ok {
		return nil, nil
	}

	data, err := os.ReadFile(c.path(key))
	if err != nil {
		// File may have been externally removed; clean up.
		c.remove(key)
		return nil, nil
	}
	return data, nil
}

// Put writes SSTable data to disk cache.
func (c *DiskCache) Put(key string, data []byte) error {
	p := c.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return err
	}

	size := int64(len(data))

	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.items[key]; ok {
		old := ele.Value.(*diskEntry)
		c.curBytes += size - old.size
		old.size = size
		c.ll.MoveToFront(ele)
	} else {
		entry := &diskEntry{key: key, size: size}
		ele := c.ll.PushFront(entry)
		c.items[key] = ele
		c.curBytes += size
	}

	for c.curBytes > c.maxBytes && c.ll.Len() > 0 {
		c.evictOldest()
	}

	return nil
}

func (c *DiskCache) evictOldest() {
	ele := c.ll.Back()
	if ele == nil {
		return
	}
	entry := ele.Value.(*diskEntry)
	c.ll.Remove(ele)
	delete(c.items, entry.key)
	c.curBytes -= entry.size
	_ = os.Remove(c.path(entry.key))
}

func (c *DiskCache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		entry := ele.Value.(*diskEntry)
		c.ll.Remove(ele)
		delete(c.items, entry.key)
		c.curBytes -= entry.size
		_ = os.Remove(c.path(entry.key))
	}
}

// Has returns true if the key exists in the cache.
func (c *DiskCache) Has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[key]
	return ok
}
