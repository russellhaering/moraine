package moraine

import "container/heap"

// Iterator is the interface for iterating over LSM entries in sorted key order.
type Iterator interface {
	// Next advances the iterator to the next entry. Returns false when exhausted or on error.
	Next() bool
	// Entry returns the current entry. Only valid after a successful call to Next.
	Entry() Entry
	// Err returns any error encountered during iteration.
	Err() error
}

// prioritizedIterator pairs an Iterator with a priority. Lower priority means
// the source is newer (active memtable = 0, frozen memtables = 1, 2, ..., etc).
type prioritizedIterator struct {
	iter     Iterator
	priority int
}

// mergeHeapItem holds the current entry from a prioritized iterator, used for
// heap-based merge.
type mergeHeapItem struct {
	entry    Entry
	priority int
	index    int // index into the sources slice
}

type mergeHeap []mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].entry.Key != h[j].entry.Key {
		return h[i].entry.Key < h[j].entry.Key
	}
	// Same key: lower priority wins (newer source).
	return h[i].priority < h[j].priority
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeHeapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// MergeIterator merges multiple sorted iterators, deduplicating by key (keeping
// the entry from the lowest-priority source, i.e. the newest) and filtering
// tombstones.
type MergeIterator struct {
	sources []prioritizedIterator
	h       mergeHeap
	current Entry
	err     error
	started bool
}

// NewMergeIterator creates a MergeIterator from the given prioritized iterators.
// Each iterator must yield entries in sorted key order. Priority values must be
// unique per source; lower priority = newer data.
func NewMergeIterator(sources []prioritizedIterator) *MergeIterator {
	return &MergeIterator{
		sources: sources,
	}
}

// Next advances the MergeIterator to the next non-tombstone, deduplicated entry.
func (m *MergeIterator) Next() bool {
	if m.err != nil {
		return false
	}

	// Initialize the heap on first call.
	if !m.started {
		m.started = true
		m.h = make(mergeHeap, 0, len(m.sources))
		for i, src := range m.sources {
			if src.iter.Next() {
				heap.Push(&m.h, mergeHeapItem{
					entry:    src.iter.Entry(),
					priority: src.priority,
					index:    i,
				})
			}
			if err := src.iter.Err(); err != nil {
				m.err = err
				return false
			}
		}
	}

	for m.h.Len() > 0 {
		// Pop the smallest item.
		winner := heap.Pop(&m.h).(mergeHeapItem)

		// Drain all other entries with the same key (dedup).
		for m.h.Len() > 0 && m.h[0].entry.Key == winner.entry.Key {
			dup := heap.Pop(&m.h).(mergeHeapItem)
			// Advance the losing iterator.
			if m.sources[dup.index].iter.Next() {
				heap.Push(&m.h, mergeHeapItem{
					entry:    m.sources[dup.index].iter.Entry(),
					priority: m.sources[dup.index].priority,
					index:    dup.index,
				})
			}
			if err := m.sources[dup.index].iter.Err(); err != nil {
				m.err = err
				return false
			}
		}

		// Advance the winning iterator.
		if m.sources[winner.index].iter.Next() {
			heap.Push(&m.h, mergeHeapItem{
				entry:    m.sources[winner.index].iter.Entry(),
				priority: m.sources[winner.index].priority,
				index:    winner.index,
			})
		}
		if err := m.sources[winner.index].iter.Err(); err != nil {
			m.err = err
			return false
		}

		// Skip tombstones.
		if winner.entry.Value == nil {
			continue
		}

		m.current = winner.entry
		return true
	}

	return false
}

// Entry returns the current entry.
func (m *MergeIterator) Entry() Entry {
	return m.current
}

// Err returns any error encountered during iteration.
func (m *MergeIterator) Err() error {
	return m.err
}
