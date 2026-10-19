package moraine

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

// Compactor merges L0 SSTables into sorted runs in the background.
type Compactor struct {
	store     objstore.ObjectStore
	prefix    string
	manifest  *ManifestStore
	threshold int // L0 count that triggers compaction
	// targetSSTableSize is the approximate size at which compaction cuts a
	// new sorted-run SSTable. Zero means DefaultTargetSSTableSize.
	targetSSTableSize int64
	// epoch, when nonzero, is the writer epoch this compactor works for. It
	// stops compacting once a newer writer owns the database, so a process
	// that has been taken over cannot commit a manifest over the new owner's.
	epoch uint64

	// onCommit, if set, is called with each manifest the compactor saves,
	// before replaced SSTables are deleted.
	onCommit func(*Manifest)

	// gcGrace is how long replaced SSTables are kept after the manifest that
	// stopped referencing them is saved, so in-flight reads holding an older
	// manifest snapshot can finish.
	gcGrace time.Duration
	now     func() time.Time

	// runMu serializes compaction runs. Two overlapping compactions of the
	// same database would each rebuild the first sorted run from their own
	// snapshot, and the later commit would drop the earlier one's output.
	runMu sync.Mutex

	gcMu    sync.Mutex
	pending []pendingDelete
	// orphansQueued records that this compactor has scanned object storage
	// for SSTables no manifest references (left by crashes, failed flushes or
	// a previous owner's lost delete queue).
	orphansQueued bool
}

type pendingDelete struct {
	path     string
	deleteAt time.Time
}

// NewCompactor creates a new compactor.
func NewCompactor(store objstore.ObjectStore, prefix string, threshold int) *Compactor {
	if threshold <= 0 {
		threshold = 4
	}
	return &Compactor{
		store:     store,
		prefix:    prefix,
		manifest:  NewManifestStore(store, prefix),
		threshold: threshold,
		gcGrace:   DefaultGCGracePeriod,
		now:       time.Now,
	}
}

// DefaultTargetSSTableSize is the size at which compaction cuts sorted-run
// SSTables when none is configured.
const DefaultTargetSSTableSize = 64 << 20

// sstEntryOverhead approximates the serialized per-entry cost beyond key and
// value bytes (length prefixes and sequence number).
const sstEntryOverhead = 16

// DefaultGCGracePeriod is how long replaced SSTables are retained before
// deletion when no grace period is configured.
const DefaultGCGracePeriod = 10 * time.Minute

// gcRetryDelay is how long a failed delete waits before it is retried.
const gcRetryDelay = time.Minute

// scheduleDelete queues paths for deletion once the grace period elapses.
func (c *Compactor) scheduleDelete(paths []string) {
	if len(paths) == 0 {
		return
	}
	c.gcMu.Lock()
	defer c.gcMu.Unlock()
	at := c.now().Add(c.gcGrace)
	for _, p := range paths {
		c.pending = append(c.pending, pendingDelete{path: p, deleteAt: at})
	}
}

// sweep deletes queued SSTables whose grace period has elapsed, skipping any
// the latest manifest references. Failed deletes stay queued and are retried
// on the next sweep.
func (c *Compactor) sweep(ctx context.Context) {
	c.gcMu.Lock()
	now := c.now()
	var due, keep []pendingDelete
	for _, p := range c.pending {
		if now.Before(p.deleteAt) {
			keep = append(keep, p)
		} else {
			due = append(due, p)
		}
	}
	c.pending = keep
	c.gcMu.Unlock()
	if len(due) == 0 {
		return
	}

	m, err := c.manifest.LoadLatest(ctx)
	if err != nil {
		slog.Warn("compactor: load manifest for gc", "err", err)
		c.requeue(due, now.Add(gcRetryDelay))
		return
	}
	live := referencedPaths(m)

	var failed []pendingDelete
	for _, p := range due {
		if _, ok := live[p.path]; ok {
			continue
		}
		if err := c.store.Delete(ctx, p.path); err != nil && !errors.Is(err, objstore.ErrNotFound) {
			slog.Warn("compactor: delete replaced sstable", "path", p.path, "err", err)
			failed = append(failed, p)
		}
	}
	c.requeue(failed, now.Add(gcRetryDelay))
}

// requeue puts deletes back on the queue to be retried at the given time.
func (c *Compactor) requeue(ps []pendingDelete, at time.Time) {
	if len(ps) == 0 {
		return
	}
	c.gcMu.Lock()
	defer c.gcMu.Unlock()
	for _, p := range ps {
		p.deleteAt = at
		c.pending = append(c.pending, p)
	}
}

// nextDeleteAt returns when the earliest queued delete becomes due.
func (c *Compactor) nextDeleteAt() (time.Time, bool) {
	c.gcMu.Lock()
	defer c.gcMu.Unlock()
	var next time.Time
	for _, p := range c.pending {
		if next.IsZero() || p.deleteAt.Before(next) {
			next = p.deleteAt
		}
	}
	return next, !next.IsZero()
}

// referencedPaths returns the object paths of every SSTable m references.
func referencedPaths(m *Manifest) map[string]struct{} {
	live := map[string]struct{}{}
	if m == nil {
		return live
	}
	for _, sst := range m.L0 {
		live[sst.Path] = struct{}{}
	}
	for _, run := range m.SortedRuns {
		for _, sst := range run.SSTables {
			live[sst.Path] = struct{}{}
		}
	}
	return live
}

// queueOrphans lists the WAL and sorted-run SSTables under the prefix and
// queues for deletion those the latest manifest does not reference. Objects
// that may belong to a write still in flight (WAL IDs at or past WALIDNext in
// the current epoch, sorted runs newer than the manifest) and objects whose
// names it does not recognize are left alone. Deletion still waits out the
// grace period, so a reader holding an older manifest can finish.
func (c *Compactor) queueOrphans(ctx context.Context, m *Manifest) error {
	live := referencedPaths(m)
	var orphans []string
	for _, dir := range []string{"/wal/", "/sorted/"} {
		keys, err := c.store.List(ctx, c.prefix+dir)
		if err != nil {
			return fmt.Errorf("compactor: list %s: %w", dir, err)
		}
		for _, k := range keys {
			if _, ok := live[k]; ok {
				continue
			}
			if c.mayBeInFlight(k, m) {
				continue
			}
			orphans = append(orphans, k)
		}
	}
	if len(orphans) > 0 {
		slog.Info("compactor: queued unreferenced sstables for deletion", "count", len(orphans))
	}
	c.scheduleDelete(orphans)
	return nil
}

// mayBeInFlight reports whether key could be an SSTable a writer or
// compactor has written but not yet recorded in a manifest, or is not an
// SSTable name this package produces.
func (c *Compactor) mayBeInFlight(key string, m *Manifest) bool {
	if rest, ok := strings.CutPrefix(key, c.prefix+"/wal/"); ok {
		rest, ok = strings.CutSuffix(rest, ".sst")
		if !ok {
			return true
		}
		epoch, idStr := uint64(0), rest
		if e, id, found := strings.Cut(rest, "/"); found {
			n, err := strconv.ParseUint(e, 10, 64)
			if err != nil {
				return true
			}
			epoch, idStr = n, id
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return true
		}
		return epoch > m.WriterEpoch || (epoch == m.WriterEpoch && id >= m.WALIDNext)
	}
	if rest, ok := strings.CutPrefix(key, c.prefix+"/sorted/sorted-L1-"); ok {
		rest, ok = strings.CutSuffix(rest, ".sst")
		if !ok {
			return true
		}
		// Names are sorted-L1-{version}-{n}; older databases have
		// sorted-L1-{version} with no output number.
		verStr, nStr, hasN := strings.Cut(rest, "-")
		version, err := strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			return true
		}
		if hasN {
			if _, err := strconv.ParseUint(nStr, 10, 64); err != nil {
				return true
			}
		}
		return version > m.Version
	}
	return true
}

// MaybeCompact checks if compaction is needed and performs it.
func (c *Compactor) MaybeCompact(ctx context.Context) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	c.sweep(ctx)

	m, err := c.loadOwned(ctx)
	if err != nil || m == nil {
		return err
	}

	c.gcMu.Lock()
	scanOrphans := !c.orphansQueued
	c.orphansQueued = true
	c.gcMu.Unlock()
	if scanOrphans {
		if err := c.queueOrphans(ctx, m); err != nil {
			slog.Warn("compactor: scan for unreferenced sstables", "err", err)
			c.gcMu.Lock()
			c.orphansQueued = false
			c.gcMu.Unlock()
		}
	}

	if len(m.L0) < c.threshold {
		return nil
	}

	return c.compact(ctx, m)
}

// CompactAll merges every L0 SSTable into the sorted runs regardless of the
// threshold, so a database reopened later reads as few SSTables as possible.
func (c *Compactor) CompactAll(ctx context.Context) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	m, err := c.loadOwned(ctx)
	if err != nil || m == nil || len(m.L0) == 0 {
		return err
	}
	return c.compact(ctx, m)
}

// loadOwned loads the latest manifest and checks that this compactor's writer
// still owns the database.
func (c *Compactor) loadOwned(ctx context.Context) (*Manifest, error) {
	m, err := c.manifest.LoadLatest(ctx)
	if err != nil || m == nil {
		return nil, err
	}
	if c.epoch != 0 && m.WriterEpoch != c.epoch {
		return nil, fmt.Errorf("compactor: epoch %d superseded by epoch %d: %w", c.epoch, m.WriterEpoch, ErrStaleWriter)
	}
	return m, nil
}

func (c *Compactor) targetSize() int64 {
	if c.targetSSTableSize > 0 {
		return c.targetSSTableSize
	}
	return DefaultTargetSSTableSize
}

// overlapsRange reports whether sst's key range intersects [lo, hi].
func overlapsRange(sst SSTInfo, lo, hi string) bool {
	return sst.MaxKey >= lo && sst.MinKey <= hi
}

// readSSTable fetches a whole SSTable object and parses it.
func (c *Compactor) readSSTable(ctx context.Context, sst SSTInfo) (*SSTableReader, error) {
	data, err := c.store.Get(ctx, sst.Path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", sst.Path, err)
	}
	reader, err := NewSSTableReaderFromBytes(sst.ID, data)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", sst.ID, err)
	}
	return reader, nil
}

// runIterator iterates over the SSTables of a sorted run in key order,
// fetching each object only when the previous one is exhausted, so only one
// run SSTable is held in memory at a time. The SSTables must not overlap.
type runIterator struct {
	ctx  context.Context
	c    *Compactor
	ssts []SSTInfo
	cur  *SSTableIterator
	err  error
}

func (it *runIterator) Next() bool {
	for it.err == nil {
		if it.cur != nil {
			if it.cur.Next() {
				return true
			}
			if err := it.cur.Err(); err != nil {
				it.err = err
				return false
			}
			it.cur = nil
		}
		if len(it.ssts) == 0 {
			return false
		}
		reader, err := it.c.readSSTable(it.ctx, it.ssts[0])
		if err != nil {
			it.err = err
			return false
		}
		it.ssts = it.ssts[1:]
		it.cur = reader.Iterator()
	}
	return false
}

func (it *runIterator) Entry() Entry { return it.cur.Entry() }
func (it *runIterator) Err() error   { return it.err }

// compactionMerge merges sorted sources into one entry per key. Unlike
// MergeIterator it keeps tombstones, and it picks the entry with the highest
// sequence number for a key, preferring the newer source on a tie (the
// semantics of deduplicateEntries).
type compactionMerge struct {
	sources []prioritizedIterator
	h       mergeHeap
	current Entry
	err     error
	started bool
}

func (m *compactionMerge) push(i int) bool {
	src := m.sources[i]
	if src.iter.Next() {
		heap.Push(&m.h, mergeHeapItem{entry: src.iter.Entry(), priority: src.priority, index: i})
		return true
	}
	if err := src.iter.Err(); err != nil {
		m.err = err
	}
	return false
}

func (m *compactionMerge) Entry() Entry { return m.current }

func (m *compactionMerge) Next() bool {
	if m.err != nil {
		return false
	}
	if !m.started {
		m.started = true
		for i := range m.sources {
			m.push(i)
			if m.err != nil {
				return false
			}
		}
	}
	if m.h.Len() == 0 {
		return false
	}
	best := heap.Pop(&m.h).(mergeHeapItem)
	m.push(best.index)
	for m.err == nil && m.h.Len() > 0 && m.h[0].entry.Key == best.entry.Key {
		dup := heap.Pop(&m.h).(mergeHeapItem)
		if dup.entry.SeqNum > best.entry.SeqNum {
			best = dup
		}
		m.push(dup.index)
	}
	if m.err != nil {
		return false
	}
	m.current = best.entry
	return true
}

// compact merges every L0 SSTable with the first sorted run's SSTables whose
// key ranges overlap the L0 tables' combined key range. The merged output is
// cut into SSTables of about the target size. Run SSTables outside that range
// are kept as they are, so a compaction rewrites only the part of the
// database the L0 tables touch.
func (c *Compactor) compact(ctx context.Context, m *Manifest) error {
	slog.Info("compaction starting", "l0_count", len(m.L0))

	// Key range covered by L0. The run SSTables overlapping it form a
	// contiguous slice of the key-ordered run.
	lo, hi := m.L0[0].MinKey, m.L0[0].MaxKey
	for _, sst := range m.L0[1:] {
		lo = min(lo, sst.MinKey)
		hi = max(hi, sst.MaxKey)
	}

	var keptRun, replacedRun []SSTInfo
	var existingRuns []SortedRun
	if len(m.SortedRuns) > 0 {
		for _, sst := range m.SortedRuns[0].SSTables {
			if overlapsRange(sst, lo, hi) {
				replacedRun = append(replacedRun, sst)
			} else {
				keptRun = append(keptRun, sst)
			}
		}
		existingRuns = m.SortedRuns[1:] // Keep remaining runs.
	}
	slices.SortFunc(replacedRun, func(a, b SSTInfo) int { return cmp.Compare(a.MinKey, b.MinKey) })

	// Sources, newest first: L0 in manifest order, then the run. Whole L0
	// objects are read up front; run SSTables do not overlap each other, so
	// they are read one at a time as the merge reaches them.
	var sources []prioritizedIterator
	for i, sst := range m.L0 {
		reader, err := c.readSSTable(ctx, sst)
		if err != nil {
			return fmt.Errorf("compactor: %w", err)
		}
		sources = append(sources, prioritizedIterator{iter: reader.Iterator(), priority: i})
	}
	if len(replacedRun) > 0 {
		sources = append(sources, prioritizedIterator{
			iter:     &runIterator{ctx: ctx, c: c, ssts: replacedRun},
			priority: len(m.L0),
		})
	}

	// Stream the merge into SSTables cut at the target size. Only the output
	// SSTable being built is held in memory.
	target := c.targetSize()
	var outputs []SSTInfo
	var mergedEntries int
	var writer *SSTableWriter
	var written int64
	finish := func() error {
		if writer == nil {
			return nil
		}
		// Unique per compaction: manifest version plus output number.
		sstID := fmt.Sprintf("sorted-L1-%020d-%06d", m.Version+1, len(outputs))
		sstPath := fmt.Sprintf("%s/sorted/%s.sst", c.prefix, sstID)
		data, meta, err := writer.Finish()
		if err != nil {
			return fmt.Errorf("compactor: write merged sst: %w", err)
		}
		if err := c.store.Put(ctx, sstPath, data, false); err != nil {
			return fmt.Errorf("compactor: put merged sst: %w", err)
		}
		outputs = append(outputs, SSTInfo{
			ID:     sstID,
			Path:   sstPath,
			MinKey: meta.MinKey,
			MaxKey: meta.MaxKey,
			MinSeq: meta.MinSeq,
			MaxSeq: meta.MaxSeq,
			Size:   meta.Size,
		})
		mergedEntries += meta.EntryCount
		writer, written = nil, 0
		return nil
	}

	merge := &compactionMerge{sources: sources}
	for merge.Next() {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("compactor: %w", err)
		}
		e := merge.Entry()
		if writer == nil {
			// The writer's ID is only a label; the stored ID is set in finish.
			writer = NewSSTableWriter(fmt.Sprintf("sorted-L1-%020d-%06d", m.Version+1, len(outputs)), DefaultBlockSize)
		}
		writer.Add(e)
		written += int64(len(e.Key)+len(e.Value)) + sstEntryOverhead
		if written >= target {
			if err := finish(); err != nil {
				return err
			}
		}
	}
	if err := merge.err; err != nil {
		return fmt.Errorf("compactor: merge: %w", err)
	}
	if err := finish(); err != nil {
		return err
	}

	// New sorted run: the untouched SSTables plus the merge output, in key
	// order.
	runSSTs := append(slices.Clone(keptRun), outputs...)
	slices.SortFunc(runSSTs, func(a, b SSTInfo) int { return cmp.Compare(a.MinKey, b.MinKey) })
	newSortedRun := SortedRun{Level: 1, SSTables: runSSTs}

	// Track which L0 SSTables we compacted so we only remove those.
	compactedL0 := make(map[string]struct{}, len(m.L0))
	for _, sst := range m.L0 {
		compactedL0[sst.ID] = struct{}{}
	}

	// Retry loop: a concurrent writer flush may bump the manifest version
	// between our read and CAS write. On conflict, reload and re-derive.
	const maxRetries = 5
	var err error
	for attempt := range maxRetries {
		base := m
		if attempt > 0 {
			base, err = c.manifest.LoadLatest(ctx)
			if err != nil || base == nil {
				return fmt.Errorf("compactor: reload manifest: %w", err)
			}
		}
		if c.epoch != 0 && base.WriterEpoch != c.epoch {
			return fmt.Errorf("compactor: epoch %d superseded by epoch %d: %w", c.epoch, base.WriterEpoch, ErrStaleWriter)
		}

		// Remove only the L0 SSTables we actually compacted; keep any
		// new L0 entries that a concurrent writer may have added.
		var remainingL0 []SSTInfo
		for _, sst := range base.L0 {
			if _, ok := compactedL0[sst.ID]; !ok {
				remainingL0 = append(remainingL0, sst)
			}
		}

		newManifest := &Manifest{
			Version:        base.Version + 1,
			WriterEpoch:    base.WriterEpoch,
			CompactorEpoch: base.CompactorEpoch + 1,
			WALIDNext:      base.WALIDNext,
			LastSeqNum:     base.LastSeqNum,
			L0:             remainingL0,
			SortedRuns:     append([]SortedRun{newSortedRun}, existingRuns...),
		}

		if err := c.manifest.Save(ctx, newManifest); err != nil {
			if errors.Is(err, objstore.ErrPreconditionFailed) && attempt < maxRetries-1 {
				continue
			}
			return fmt.Errorf("compactor: update manifest: %w", err)
		}
		slog.Info("compaction complete",
			"merged_entries", mergedEntries,
			"output_sstables", len(outputs),
			"replaced_run_sstables", len(replacedRun),
			"kept_run_sstables", len(keptRun),
			"manifest_version", newManifest.Version)
		if c.onCommit != nil {
			c.onCommit(newManifest)
		}
		// Schedule the compacted L0 SSTables and the replaced run SSTables
		// for deletion. They are not deleted now: reads that already took a
		// snapshot of an older manifest (in this process or a process being
		// taken over) may still load them. sweep deletes them after the grace
		// period. Run SSTables outside the compacted range stay referenced.
		var garbage []string
		for _, sst := range m.L0 {
			garbage = append(garbage, sst.Path)
		}
		for _, sst := range replacedRun {
			garbage = append(garbage, sst.Path)
		}
		c.scheduleDelete(garbage)
		break
	}

	return nil
}
