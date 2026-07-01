package moraine

import (
	"math/rand"
	"sync"
)

const (
	maxHeight   = 12
	probability = 0.25
	// Overhead per entry: pointers for skip-list levels, key/value slice headers, SeqNum.
	entryOverhead = 128
)

// Entry represents a key-value pair stored in the MemTable. A nil Value
// indicates a tombstone (deletion marker).
type Entry struct {
	Key    string
	Value  []byte
	SeqNum uint64
}

// node is an internal skip-list node.
type node struct {
	entry Entry
	next  []*node
}

func newNode(key string, value []byte, seq uint64, height int) *node {
	return &node{
		entry: Entry{
			Key:    key,
			Value:  value,
			SeqNum: seq,
		},
		next: make([]*node, height),
	}
}

// MemTable is a skip-list backed in-memory sorted table for an LSM-tree.
// Concurrent reads are safe while a single writer holds the write lock.
type MemTable struct {
	mu       sync.RWMutex
	head     *node
	height   int
	length   int
	size     int64
	IsFrozen bool
	rng      *rand.Rand
}

// NewMemTable creates an empty MemTable ready for writes.
func NewMemTable() *MemTable {
	return &MemTable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rng:    rand.New(rand.NewSource(rand.Int63())),
	}
}

// randomHeight returns a random level height for a new node using geometric
// distribution with p = 0.25.
func (m *MemTable) randomHeight() int {
	h := 1
	for h < maxHeight && m.rng.Float64() < probability {
		h++
	}
	return h
}

// findPrevious populates prev with the predecessor node at each level for the
// given key. It is used by both Get and mutation methods.
func (m *MemTable) findPrevious(key string, prev []*node) {
	curr := m.head
	for level := m.height - 1; level >= 0; level-- {
		for curr.next[level] != nil && curr.next[level].entry.Key < key {
			curr = curr.next[level]
		}
		prev[level] = curr
	}
}

// Put inserts or updates a key-value pair in the MemTable. It panics if the
// table has been frozen.
func (m *MemTable) Put(key string, value []byte, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IsFrozen {
		panic("memtable: put on frozen table")
	}

	prev := make([]*node, maxHeight)
	m.findPrevious(key, prev)

	// If the key already exists, update it in place.
	if next := prev[0].next[0]; next != nil && next.entry.Key == key {
		// Adjust tracked size for the value change.
		oldSize := int64(len(next.entry.Value))
		newSize := int64(len(value))
		m.size += newSize - oldSize

		next.entry.Value = value
		next.entry.SeqNum = seq
		return
	}

	// Insert a new node.
	h := m.randomHeight()
	nd := newNode(key, value, seq, h)

	// If the new node is taller than the current list, link from the head
	// at the new levels.
	if h > m.height {
		for level := m.height; level < h; level++ {
			prev[level] = m.head
		}
		m.height = h
	}

	for level := 0; level < h; level++ {
		nd.next[level] = prev[level].next[level]
		prev[level].next[level] = nd
	}

	m.length++
	m.size += int64(len(key)) + int64(len(value)) + entryOverhead
}

// Delete inserts a tombstone for the given key. It panics if the table has
// been frozen.
func (m *MemTable) Delete(key string, seq uint64) {
	m.Put(key, nil, seq)
}

// Get looks up a key and returns the most recent Entry. The second return
// value is false if the key was never written.
func (m *MemTable) Get(key string) (*Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	curr := m.head
	for level := m.height - 1; level >= 0; level-- {
		for curr.next[level] != nil && curr.next[level].entry.Key < key {
			curr = curr.next[level]
		}
	}

	nd := curr.next[0]
	if nd != nil && nd.entry.Key == key {
		e := nd.entry // copy
		return &e, true
	}
	return nil, false
}

// Len returns the number of entries (including tombstones) in the table.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.length
}

// Size returns the approximate heap size in bytes consumed by all entries.
func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Freeze marks the MemTable as immutable and returns it. Any subsequent
// Put or Delete will panic.
func (m *MemTable) Freeze() *MemTable {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.IsFrozen = true
	return m
}

// MemTableIterator iterates over MemTable entries in sorted key order.
// It holds a snapshot of the entries taken at creation time.
type MemTableIterator struct {
	entries []Entry
	pos     int
}

// Iterator returns an iterator positioned before the first entry. Call Next
// to advance to the first entry. All entries are snapshotted under the read
// lock so the iterator is safe for concurrent use.
func (m *MemTable) Iterator() *MemTableIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []Entry
	curr := m.head.next[0]
	for curr != nil {
		entries = append(entries, curr.entry)
		curr = curr.next[0]
	}
	return &MemTableIterator{entries: entries, pos: -1}
}

// IteratorFrom returns an iterator positioned before the first entry with
// key > afterKey. Uses skip-list O(log n) seek. If afterKey is empty, this
// behaves identically to Iterator().
func (m *MemTable) IteratorFrom(afterKey string) *MemTableIterator {
	if afterKey == "" {
		return m.Iterator()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Use skip-list levels to seek to the first node with key > afterKey.
	curr := m.head
	for level := m.height - 1; level >= 0; level-- {
		for curr.next[level] != nil && curr.next[level].entry.Key <= afterKey {
			curr = curr.next[level]
		}
	}

	// curr.next[0] is now the first node with key > afterKey (or nil).
	var entries []Entry
	nd := curr.next[0]
	for nd != nil {
		entries = append(entries, nd.entry)
		nd = nd.next[0]
	}
	return &MemTableIterator{entries: entries, pos: -1}
}

// Next advances the iterator to the next entry. It returns false when the
// iterator is exhausted.
func (it *MemTableIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

// Entry returns the entry at the current iterator position. The caller must
// have called Next at least once and it must have returned true.
func (it *MemTableIterator) Entry() Entry {
	return it.entries[it.pos]
}

// Err always returns nil for MemTableIterator (iteration is over an in-memory snapshot).
func (it *MemTableIterator) Err() error {
	return nil
}
