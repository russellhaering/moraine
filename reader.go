package moraine

import (
	"cmp"
	"container/list"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/russellhaering/moraine/cache"
	"github.com/russellhaering/moraine/objstore"
)

// Reader provides the read path for the LSM tree. It searches active MemTable,
// frozen MemTables, L0 SSTables, and sorted runs in order.
type Reader struct {
	store      objstore.ObjectStore
	blockCache *cache.LRUBlockCache
	diskCache  *cache.DiskCache
	tables     *tableCache
}

// NewReader creates a new Reader.
func NewReader(store objstore.ObjectStore, blockCache *cache.LRUBlockCache, diskCache *cache.DiskCache) *Reader {
	return &Reader{
		store:      store,
		blockCache: blockCache,
		diskCache:  diskCache,
		tables:     newTableCache(defaultTableCacheSize),
	}
}

// defaultTableCacheSize bounds how many opened SSTable readers (index and
// bloom filter, not data blocks) a Reader keeps.
const defaultTableCacheSize = 1024

// tableCache is an LRU of opened SSTable readers keyed by object path.
// SSTables are immutable, so a cached reader never goes stale; readers for
// files removed by compaction simply age out.
type tableCache struct {
	mu    sync.Mutex
	max   int
	ll    *list.List
	items map[string]*list.Element
}

type tableCacheEntry struct {
	path   string
	reader *SSTableReader
}

func newTableCache(max int) *tableCache {
	return &tableCache{max: max, ll: list.New(), items: map[string]*list.Element{}}
}

func (c *tableCache) get(path string) *SSTableReader {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[path]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*tableCacheEntry).reader
	}
	return nil
}

func (c *tableCache) put(path string, r *SSTableReader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[path]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*tableCacheEntry).reader = r
		return
	}
	c.items[path] = c.ll.PushFront(&tableCacheEntry{path: path, reader: r})
	for c.ll.Len() > c.max {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*tableCacheEntry).path)
	}
}

// Get searches for a key through all levels of the LSM tree.
// Search order: active MemTable -> frozen MemTables -> L0 SSTables -> sorted runs.
// Returns (entry, true) if found, (nil, false) if not found.
func (r *Reader) Get(ctx context.Context, key string, active *MemTable, frozen []*MemTable, manifest *Manifest) (*Entry, bool, error) {
	// 1. Check active MemTable.
	if e, ok := active.Get(key); ok {
		return e, true, nil
	}

	// 2. Check frozen MemTables (newest first).
	for _, mt := range frozen {
		if e, ok := mt.Get(key); ok {
			return e, true, nil
		}
	}

	// 3. Check L0 SSTables (newest first).
	for _, sst := range manifest.L0 {
		reader, err := r.loadSSTable(ctx, sst)
		if err != nil {
			return nil, false, fmt.Errorf("reader: load L0 %s: %w", sst.ID, err)
		}
		// Bloom filter check.
		if !reader.BloomMayContain(key) {
			continue
		}
		e, err := reader.Get(ctx, key)
		if err != nil {
			return nil, false, fmt.Errorf("reader: get from L0 %s: %w", sst.ID, err)
		}
		if e != nil {
			return e, true, nil
		}
	}
	// 4. Check sorted runs (newest first).
	for _, run := range manifest.SortedRuns {
		for _, sst := range run.SSTables {
			// Key range check.
			if key < sst.MinKey || key > sst.MaxKey {
				continue
			}
			reader, err := r.loadSSTable(ctx, sst)
			if err != nil {
				return nil, false, fmt.Errorf("reader: load L%d %s: %w", run.Level, sst.ID, err)
			}
			if !reader.BloomMayContain(key) {
				continue
			}
			e, err := reader.Get(ctx, key)
			if err != nil {
				return nil, false, fmt.Errorf("reader: get from L%d %s: %w", run.Level, sst.ID, err)
			}
			if e != nil {
				return e, true, nil
			}
		}
	}
	return nil, false, nil
}

// loadSSTable returns a reader for sst, opening it with ranged reads on first
// use and reusing it afterwards.
func (r *Reader) loadSSTable(ctx context.Context, sst SSTInfo) (*SSTableReader, error) {
	if reader := r.tables.get(sst.Path); reader != nil {
		return reader, nil
	}
	reader, err := NewSSTableReader(ctx, r.store, sst, r.blockCache, r.diskCache)
	if err != nil {
		return nil, err
	}
	r.tables.put(sst.Path, reader)
	return reader, nil
}

// Scan returns all non-tombstone entries in key order across all levels.
// This is primarily used for index building and compaction.
func (r *Reader) Scan(ctx context.Context, active *MemTable, frozen []*MemTable, manifest *Manifest) ([]Entry, error) {
	// Collect all entries with their source priority (lower = newer).
	type prioritizedEntry struct {
		entry    Entry
		priority int
	}

	var all []prioritizedEntry
	priority := 0

	// Active MemTable.
	iter := active.Iterator()
	for iter.Next() {
		all = append(all, prioritizedEntry{entry: iter.Entry(), priority: priority})
	}
	priority++

	// Frozen MemTables.
	for _, mt := range frozen {
		iter := mt.Iterator()
		for iter.Next() {
			all = append(all, prioritizedEntry{entry: iter.Entry(), priority: priority})
		}
		priority++
	}

	// L0 SSTables.
	for _, sst := range manifest.L0 {
		reader, err := r.loadSSTable(ctx, sst)
		if err != nil {
			return nil, err
		}
		si := reader.Iterator()
		for si.Next() {
			all = append(all, prioritizedEntry{entry: si.Entry(), priority: priority})
		}
		priority++
	}

	// Sorted runs.
	for _, run := range manifest.SortedRuns {
		for _, sst := range run.SSTables {
			reader, err := r.loadSSTable(ctx, sst)
			if err != nil {
				return nil, err
			}
			si := reader.Iterator()
			for si.Next() {
				all = append(all, prioritizedEntry{entry: si.Entry(), priority: priority})
			}
		}
		priority++
	}

	// Deduplicate: for each key, keep the entry with the lowest priority (newest).
	seen := make(map[string]int) // key -> index in result
	var result []Entry

	// Sort by key, then priority.
	// We'll use a map to track the best entry per key.
	best := make(map[string]prioritizedEntry)
	for _, pe := range all {
		existing, ok := best[pe.entry.Key]
		if !ok || pe.priority < existing.priority {
			best[pe.entry.Key] = pe
		}
	}

	for _, pe := range best {
		// Skip tombstones.
		if pe.entry.Value == nil {
			continue
		}
		result = append(result, pe.entry)
		seen[pe.entry.Key] = len(result) - 1
	}

	_ = seen // silence unused warning

	// Sort result by key.
	sortEntries(result)
	return result, nil
}

func sortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		return cmp.Compare(a.Key, b.Key)
	})
}

// ScanRangeResult holds the results of a cursor-based range scan.
type ScanRangeResult struct {
	Entries []Entry
	HasMore bool
}

// ScanRange returns up to limit non-tombstone entries with key > afterKey,
// in sorted key order, using seek-based iteration for efficiency.
// If afterKey is empty, iteration starts from the beginning.
func (r *Reader) ScanRange(ctx context.Context, afterKey string, limit int, active *MemTable, frozen []*MemTable, manifest *Manifest) (*ScanRangeResult, error) {
	var sources []prioritizedIterator
	priority := 0

	// Active MemTable.
	sources = append(sources, prioritizedIterator{
		iter:     active.IteratorFrom(afterKey),
		priority: priority,
	})
	priority++

	// Frozen MemTables (newest first).
	for _, mt := range frozen {
		sources = append(sources, prioritizedIterator{
			iter:     mt.IteratorFrom(afterKey),
			priority: priority,
		})
		priority++
	}

	// L0 SSTables (newest first).
	for _, sst := range manifest.L0 {
		// Skip SSTables entirely if MaxKey <= afterKey.
		if afterKey != "" && sst.MaxKey <= afterKey {
			priority++
			continue
		}

		reader, err := r.loadSSTable(ctx, sst)
		if err != nil {
			return nil, fmt.Errorf("reader: load L0 %s: %w", sst.ID, err)
		}

		sources = append(sources, prioritizedIterator{
			iter:     reader.IteratorFrom(ctx, afterKey),
			priority: priority,
		})
		priority++
	}

	// Sorted runs (newest first).
	for _, run := range manifest.SortedRuns {
		for _, sst := range run.SSTables {
			// Skip SSTables entirely if MaxKey <= afterKey.
			if afterKey != "" && sst.MaxKey <= afterKey {
				continue
			}

			reader, err := r.loadSSTable(ctx, sst)
			if err != nil {
				return nil, fmt.Errorf("reader: load L%d %s: %w", run.Level, sst.ID, err)
			}

			sources = append(sources, prioritizedIterator{
				iter:     reader.IteratorFrom(ctx, afterKey),
				priority: priority,
			})
		}
		priority++
	}

	// Merge and collect up to limit+1 entries.
	merge := NewMergeIterator(sources)
	var entries []Entry
	for merge.Next() {
		entries = append(entries, merge.Entry())
		if len(entries) > limit {
			break
		}
	}
	if err := merge.Err(); err != nil {
		return nil, fmt.Errorf("reader: merge: %w", err)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	return &ScanRangeResult{
		Entries: entries,
		HasMore: hasMore,
	}, nil
}

// ScanSince returns all entries (including tombstones) with SeqNum > sinceSeq,
// after deduplication. SSTables whose MaxSeq <= sinceSeq are skipped entirely.
// Unlike Scan, tombstones are included so the caller can handle deletes.
func (r *Reader) ScanSince(ctx context.Context, sinceSeq uint64, active *MemTable, frozen []*MemTable, manifest *Manifest) ([]Entry, error) {
	type prioritizedEntry struct {
		entry    Entry
		priority int
	}

	var all []prioritizedEntry
	priority := 0

	// Active MemTable — iterate all, filter by SeqNum.
	iter := active.Iterator()
	for iter.Next() {
		e := iter.Entry()
		if e.SeqNum > sinceSeq {
			all = append(all, prioritizedEntry{entry: e, priority: priority})
		}
	}
	priority++

	// Frozen MemTables.
	for _, mt := range frozen {
		iter := mt.Iterator()
		for iter.Next() {
			e := iter.Entry()
			if e.SeqNum > sinceSeq {
				all = append(all, prioritizedEntry{entry: e, priority: priority})
			}
		}
		priority++
	}

	// L0 SSTables — skip entirely if MaxSeq <= sinceSeq.
	for _, sst := range manifest.L0 {
		if sst.MaxSeq <= sinceSeq {
			continue
		}
		reader, err := r.loadSSTable(ctx, sst)
		if err != nil {
			return nil, err
		}
		si := reader.Iterator()
		for si.Next() {
			e := si.Entry()
			if e.SeqNum > sinceSeq {
				all = append(all, prioritizedEntry{entry: e, priority: priority})
			}
		}
		priority++
	}

	// Sorted runs — same skip/filter logic.
	for _, run := range manifest.SortedRuns {
		for _, sst := range run.SSTables {
			if sst.MaxSeq <= sinceSeq {
				continue
			}
			reader, err := r.loadSSTable(ctx, sst)
			if err != nil {
				return nil, err
			}
			si := reader.Iterator()
			for si.Next() {
				e := si.Entry()
				if e.SeqNum > sinceSeq {
					all = append(all, prioritizedEntry{entry: e, priority: priority})
				}
			}
		}
		priority++
	}

	// Deduplicate: for each key, keep the entry with the lowest priority (newest source).
	best := make(map[string]prioritizedEntry)
	for _, pe := range all {
		existing, ok := best[pe.entry.Key]
		if !ok || pe.priority < existing.priority {
			best[pe.entry.Key] = pe
		}
	}

	// Only emit entries whose winning version has SeqNum > sinceSeq.
	// (This is already guaranteed since all candidates have SeqNum > sinceSeq,
	// but we keep the check explicit for clarity.)
	result := make([]Entry, 0, len(best))
	for _, pe := range best {
		if pe.entry.SeqNum > sinceSeq {
			result = append(result, pe.entry)
		}
	}

	sortEntries(result)
	return result, nil
}

// ScanPrefix returns up to limit non-tombstone entries whose key has the
// given prefix, including a key equal to the prefix. Keys are returned in
// sorted order. A limit <= 0 returns every match.
func (r *Reader) ScanPrefix(ctx context.Context, prefix string, limit int, active *MemTable, frozen []*MemTable, manifest *Manifest) ([]Entry, error) {
	var sources []prioritizedIterator
	priority := 0

	// Active MemTable.
	sources = append(sources, prioritizedIterator{
		iter:     active.SeekFrom(prefix),
		priority: priority,
	})
	priority++

	// Frozen MemTables (newest first).
	for _, mt := range frozen {
		sources = append(sources, prioritizedIterator{
			iter:     mt.SeekFrom(prefix),
			priority: priority,
		})
		priority++
	}

	// L0 SSTables (newest first).
	for _, sst := range manifest.L0 {
		if !sstMayHavePrefix(sst, prefix) {
			priority++
			continue
		}
		reader, err := r.loadSSTable(ctx, sst)
		if err != nil {
			return nil, fmt.Errorf("reader: load L0 %s: %w", sst.ID, err)
		}
		sources = append(sources, prioritizedIterator{
			iter:     reader.SeekFrom(ctx, prefix),
			priority: priority,
		})
		priority++
	}

	// Sorted runs (newest first).
	for _, run := range manifest.SortedRuns {
		for _, sst := range run.SSTables {
			if !sstMayHavePrefix(sst, prefix) {
				continue
			}
			reader, err := r.loadSSTable(ctx, sst)
			if err != nil {
				return nil, fmt.Errorf("reader: load L%d %s: %w", run.Level, sst.ID, err)
			}
			sources = append(sources, prioritizedIterator{
				iter:     reader.SeekFrom(ctx, prefix),
				priority: priority,
			})
		}
		priority++
	}

	// Merge and collect entries until the keys leave the prefix range.
	merge := NewMergeIterator(sources)
	var entries []Entry
	for merge.Next() {
		e := merge.Entry()
		if !strings.HasPrefix(e.Key, prefix) {
			break
		}
		if e.Value != nil {
			entries = append(entries, e)
			if limit > 0 && len(entries) == limit {
				break
			}
		}
	}
	if err := merge.Err(); err != nil {
		return nil, fmt.Errorf("reader: merge: %w", err)
	}
	return entries, nil
}

// sstMayHavePrefix reports whether sst's key range can contain a key with
// the given prefix.
func sstMayHavePrefix(sst SSTInfo, prefix string) bool {
	if sst.MaxKey < prefix {
		return false
	}
	return sst.MinKey <= prefix || strings.HasPrefix(sst.MinKey, prefix)
}
