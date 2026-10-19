package moraine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
		version, err := strconv.ParseUint(rest, 10, 64)
		if err != nil {
			return true
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

func (c *Compactor) compact(ctx context.Context, m *Manifest) error {
	slog.Info("compaction starting", "l0_count", len(m.L0))

	// Merge all L0 SSTables into a single sorted run.
	var allEntries []Entry
	for _, sst := range m.L0 {
		data, err := c.store.Get(ctx, sst.Path)
		if err != nil {
			return fmt.Errorf("compactor: read %s: %w", sst.Path, err)
		}
		reader, err := NewSSTableReaderFromBytes(sst.ID, data)
		if err != nil {
			return fmt.Errorf("compactor: parse %s: %w", sst.ID, err)
		}
		iter := reader.Iterator()
		for iter.Next() {
			allEntries = append(allEntries, iter.Entry())
		}
	}

	// Also merge with the first sorted run if it exists, to avoid unbounded
	// growth of sorted runs.
	var existingRuns []SortedRun
	if len(m.SortedRuns) > 0 {
		firstRun := m.SortedRuns[0]
		for _, sst := range firstRun.SSTables {
			data, err := c.store.Get(ctx, sst.Path)
			if err != nil {
				return fmt.Errorf("compactor: read sorted run %s: %w", sst.Path, err)
			}
			reader, err := NewSSTableReaderFromBytes(sst.ID, data)
			if err != nil {
				return fmt.Errorf("compactor: parse sorted run %s: %w", sst.ID, err)
			}
			iter := reader.Iterator()
			for iter.Next() {
				allEntries = append(allEntries, iter.Entry())
			}
		}
		existingRuns = m.SortedRuns[1:] // Keep remaining runs.
	}

	// Deduplicate by key, keeping highest seqnum.
	merged := deduplicateEntries(allEntries)

	// Write merged SSTable.
	sstID := fmt.Sprintf("sorted-L1-%020d", m.Version+1)
	sstPath := fmt.Sprintf("%s/sorted/%s.sst", c.prefix, sstID)

	writer := NewSSTableWriter(sstID, DefaultBlockSize)
	for _, e := range merged {
		writer.Add(e)
	}

	data, meta, err := writer.Finish()
	if err != nil {
		return fmt.Errorf("compactor: write merged sst: %w", err)
	}

	if err := c.store.Put(ctx, sstPath, data, false); err != nil {
		return fmt.Errorf("compactor: put merged sst: %w", err)
	}

	// Build new sorted run from compaction result.
	newSortedRun := SortedRun{
		Level: 1,
		SSTables: []SSTInfo{{
			ID:     meta.ID,
			Path:   sstPath,
			MinKey: meta.MinKey,
			MaxKey: meta.MaxKey,
			MinSeq: meta.MinSeq,
			MaxSeq: meta.MaxSeq,
			Size:   meta.Size,
		}},
	}

	// Track which L0 SSTables we compacted so we only remove those.
	compactedL0 := make(map[string]struct{}, len(m.L0))
	for _, sst := range m.L0 {
		compactedL0[sst.ID] = struct{}{}
	}

	// Retry loop: a concurrent writer flush may bump the manifest version
	// between our read and CAS write. On conflict, reload and re-derive.
	const maxRetries = 5
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
			"merged_entries", meta.EntryCount,
			"manifest_version", newManifest.Version)
		if c.onCommit != nil {
			c.onCommit(newManifest)
		}
		// Schedule the compacted L0 SSTables and the replaced first sorted run
		// for deletion. They are not deleted now: reads that already took a
		// snapshot of an older manifest (in this process or a process being
		// taken over) may still load them. sweep deletes them after the grace
		// period. Use the original manifest m to track what we compacted.
		var garbage []string
		for _, sst := range m.L0 {
			if _, ok := compactedL0[sst.ID]; ok {
				garbage = append(garbage, sst.Path)
			}
		}
		if len(m.SortedRuns) > 0 {
			for _, sst := range m.SortedRuns[0].SSTables {
				garbage = append(garbage, sst.Path)
			}
		}
		c.scheduleDelete(garbage)
		break
	}

	return nil
}

// deduplicateEntries keeps the entry with the highest seqnum for each key.
// If sequence numbers tie, the first entry wins; callers pass entries in
// newest-source-first order.
func deduplicateEntries(entries []Entry) []Entry {
	best := make(map[string]Entry)
	for _, e := range entries {
		if existing, ok := best[e.Key]; !ok || e.SeqNum > existing.SeqNum {
			best[e.Key] = e
		}
	}

	result := make([]Entry, 0, len(best))
	for _, e := range best {
		result = append(result, e)
	}
	sortEntries(result)
	return result
}
