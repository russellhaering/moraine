package moraine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/russellhaering/moraine/objstore"
)

// ErrStaleWriter is returned when a writer tries to commit after a newer writer
// has claimed the same database prefix.
var ErrStaleWriter = errors.New("stale writer")

// Writer is the single-writer for an LSM database. It manages the active
// MemTable, frozen MemTables pending flush, and the WAL. It uses epoch-based
// fencing to ensure only one writer is active at a time.
type Writer struct {
	mu sync.Mutex

	store    objstore.ObjectStore
	prefix   string
	manifest *ManifestStore

	active  *MemTable
	frozen  []*MemTable // newest first
	wal     *WAL
	epoch   uint64
	seqNum  uint64
	maxSize int64

	currentManifest *Manifest
}

// WriterConfig configures the writer.
type WriterConfig struct {
	Store           objstore.ObjectStore
	Prefix          string
	MemTableMaxSize int64 // bytes before auto-flush
}

// OpenWriter opens a writer by loading (or creating) the manifest, incrementing
// the writer epoch, and performing CAS to claim ownership.
func OpenWriter(ctx context.Context, cfg WriterConfig) (*Writer, error) {
	ms := NewManifestStore(cfg.Store, cfg.Prefix)

	existing, err := ms.LoadLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("writer: load manifest: %w", err)
	}

	var m *Manifest
	if existing == nil {
		m = &Manifest{
			Version:     1,
			WriterEpoch: 1,
			WALIDNext:   1,
		}
	} else {
		m = &Manifest{
			Version:        existing.Version + 1,
			WriterEpoch:    existing.WriterEpoch + 1,
			CompactorEpoch: existing.CompactorEpoch,
			WALIDNext:      existing.WALIDNext,
			LastSeqNum:     existing.LastSeqNum,
			L0:             existing.L0,
			SortedRuns:     existing.SortedRuns,
		}
	}

	if err := ms.Save(ctx, m); err != nil {
		return nil, fmt.Errorf("writer: claim epoch %d: %w", m.WriterEpoch, err)
	}

	slog.Info("writer opened", "epoch", m.WriterEpoch, "manifest_version", m.Version)

	maxSize := cfg.MemTableMaxSize
	if maxSize <= 0 {
		maxSize = 64 << 20
	}

	return &Writer{
		store:           cfg.Store,
		prefix:          cfg.Prefix,
		manifest:        ms,
		active:          NewMemTable(),
		wal:             newEpochWAL(cfg.Store, cfg.Prefix, m.WALIDNext, m.WriterEpoch),
		epoch:           m.WriterEpoch,
		seqNum:          m.LastSeqNum,
		maxSize:         maxSize,
		currentManifest: m,
	}, nil
}

// Put inserts or updates a key-value pair. The caller should call Flush
// after the write for strong consistency guarantees.
func (w *Writer) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seqNum++
	seq := w.seqNum
	w.active.Put(key, value, seq)

	// Auto-flush if MemTable is too large.
	if w.active.Size() >= w.maxSize {
		if err := w.flushLocked(ctx); err != nil {
			return 0, err
		}
	}

	return seq, nil
}

// Delete inserts a tombstone for the given key.
func (w *Writer) Delete(ctx context.Context, key string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seqNum++
	seq := w.seqNum
	w.active.Delete(key, seq)
	return seq, nil
}

// Get reads a key from the writer's local state (active + frozen MemTables).
// This provides strong read-after-write consistency.
func (w *Writer) Get(key string) (*Entry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check active MemTable first.
	if e, ok := w.active.Get(key); ok {
		return e, true
	}

	// Check frozen MemTables (newest first).
	for _, mt := range w.frozen {
		if e, ok := mt.Get(key); ok {
			return e, true
		}
	}

	return nil, false
}

// Flush synchronously flushes the current MemTable to WAL, providing
// durability for all writes.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked(ctx)
}

func (w *Writer) flushLocked(ctx context.Context) error {
	if w.active.Len() == 0 {
		return nil
	}

	latest, err := w.manifest.LoadLatest(ctx)
	if err != nil {
		return fmt.Errorf("writer: reload manifest: %w", err)
	}
	if latest != nil {
		if latest.WriterEpoch != w.epoch {
			return fmt.Errorf("writer: epoch %d superseded by epoch %d: %w", w.epoch, latest.WriterEpoch, ErrStaleWriter)
		}
		w.currentManifest = latest
	}

	frozen := w.active.Freeze()
	w.frozen = append([]*MemTable{frozen}, w.frozen...)
	w.active = NewMemTable()

	walID, meta, err := w.wal.Write(ctx, frozen)
	if err != nil {
		return fmt.Errorf("writer: flush wal: %w", err)
	}

	// Prepend new L0 SSTable (newest first).
	sstInfo := SSTInfo{
		ID:     meta.ID,
		Path:   w.wal.walPath(walID),
		MinKey: meta.MinKey,
		MaxKey: meta.MaxKey,
		MinSeq: meta.MinSeq,
		MaxSeq: meta.MaxSeq,
		Size:   meta.Size,
	}

	// Retry loop: reload latest manifest and attempt CAS save.
	// A concurrent compactor may bump the version between our read and write.
	const maxRetries = 5
	for attempt := range maxRetries {
		latest, err := w.manifest.LoadLatest(ctx)
		if err != nil {
			return fmt.Errorf("writer: reload manifest: %w", err)
		}
		if latest != nil {
			if latest.WriterEpoch != w.epoch {
				return fmt.Errorf("writer: epoch %d superseded by epoch %d: %w", w.epoch, latest.WriterEpoch, ErrStaleWriter)
			}
			w.currentManifest = latest
		}

		lastSeqNum := w.currentManifest.LastSeqNum
		if meta.MaxSeq > lastSeqNum {
			lastSeqNum = meta.MaxSeq
		}

		newManifest := &Manifest{
			Version:        w.currentManifest.Version + 1,
			WriterEpoch:    w.epoch,
			CompactorEpoch: w.currentManifest.CompactorEpoch,
			WALIDNext:      walID + 1,
			LastSeqNum:     lastSeqNum,
			SortedRuns:     w.currentManifest.SortedRuns,
			L0:             append([]SSTInfo{sstInfo}, w.currentManifest.L0...),
		}

		if err := w.manifest.Save(ctx, newManifest); err != nil {
			if errors.Is(err, objstore.ErrPreconditionFailed) && attempt < maxRetries-1 {
				continue
			}
			return fmt.Errorf("writer: update manifest: %w", err)
		}

		w.currentManifest = newManifest
		slog.Info("writer flushed", "wal_id", walID, "entries", meta.EntryCount,
			"manifest_version", newManifest.Version)
		break
	}

	return nil
}

// FrozenMemTables returns the list of frozen MemTables for the reader to search.
func (w *Writer) FrozenMemTables() []*MemTable {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]*MemTable, len(w.frozen))
	copy(result, w.frozen)
	return result
}

// ActiveMemTable returns the active MemTable for the reader to search.
func (w *Writer) ActiveMemTable() *MemTable {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active
}

// CurrentManifest returns the current manifest state.
func (w *Writer) CurrentManifest() *Manifest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentManifest
}

// DropFrozen removes all frozen MemTables (called after successful read path
// initialization ensures L0 SSTables are available).
func (w *Writer) DropFrozen() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.frozen = nil
}
