package moraine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	epoch     uint64

	// onCommit, if set, is called with each manifest the compactor saves,
	// before replaced SSTables are deleted.
	onCommit func(*Manifest)

	// gcGrace is how long replaced SSTables are kept after the manifest that
	// stopped referencing them is saved, so in-flight reads holding an older
	// manifest snapshot can finish.
	gcGrace time.Duration
	now     func() time.Time

	gcMu    sync.Mutex
	pending []pendingDelete
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

// sweep deletes queued SSTables whose grace period has elapsed. Failed
// deletes stay queued and are retried on the next sweep.
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

	var failed []pendingDelete
	for _, p := range due {
		if err := c.store.Delete(ctx, p.path); err != nil && !errors.Is(err, objstore.ErrNotFound) {
			slog.Warn("compactor: delete replaced sstable", "path", p.path, "err", err)
			failed = append(failed, p)
		}
	}
	if len(failed) > 0 {
		c.gcMu.Lock()
		c.pending = append(c.pending, failed...)
		c.gcMu.Unlock()
	}
}

// MaybeCompact checks if compaction is needed and performs it.
func (c *Compactor) MaybeCompact(ctx context.Context) error {
	c.sweep(ctx)

	m, err := c.manifest.LoadLatest(ctx)
	if err != nil || m == nil {
		return err
	}

	if len(m.L0) < c.threshold {
		return nil
	}

	return c.compact(ctx, m)
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
		reader, err := NewSSTableReaderLegacy(sst.ID, data)
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
			reader, err := NewSSTableReaderLegacy(sst.ID, data)
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
