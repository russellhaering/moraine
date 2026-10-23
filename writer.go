package moraine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

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

	// onFlush, if set, is called with the L0 SSTable count after a flush
	// commits. It runs with w.mu held and must not block or call back into
	// the writer.
	onFlush func(l0 int)
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
	if w.active.Len() > 0 {
		w.frozen = append([]*MemTable{w.active.Freeze()}, w.frozen...)
		w.active = NewMemTable()
	}

	// Flush oldest first so L0 stays newest-first. A table left behind by an
	// earlier failed flush is retried here before anything newer, and stays
	// readable from w.frozen until it is durable.
	flushed := false
	for len(w.frozen) > 0 {
		if err := w.flushTable(ctx, w.frozen[len(w.frozen)-1]); err != nil {
			return err
		}
		w.frozen = w.frozen[:len(w.frozen)-1]
		flushed = true
	}
	if flushed && w.onFlush != nil {
		w.onFlush(len(w.currentManifest.L0))
	}
	return nil
}

// checkEpochLocked reloads the latest manifest, adopting it if this writer
// still owns the database.
func (w *Writer) checkEpochLocked(ctx context.Context) error {
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
	return nil
}

// flushTable writes one frozen MemTable to the WAL and records it in the
// manifest as the newest L0 SSTable.
func (w *Writer) flushTable(ctx context.Context, frozen *MemTable) error {
	start := time.Now()
	if err := w.checkEpochLocked(ctx); err != nil {
		return err
	}

	walID, meta, err := w.wal.Write(ctx, frozen)
	if errors.Is(err, objstore.ErrPreconditionFailed) {
		// This writer still owns the epoch (checked above), so the ID can
		// only be taken by an earlier attempt of ours whose result was lost.
		// Nothing references that object; skip past it.
		w.wal.nextID++
		walID, meta, err = w.wal.Write(ctx, frozen)
	}
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
		if err := w.checkEpochLocked(ctx); err != nil {
			return err
		}
		// A save that reported failure may still have landed.
		if slices.ContainsFunc(w.currentManifest.L0, func(s SSTInfo) bool { return s.Path == sstInfo.Path }) {
			break
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
			"manifest_version", newManifest.Version, "bytes", meta.Size, "duration", time.Since(start))
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

// unflushedBytes is the size of the active and frozen MemTables.
func (w *Writer) unflushedBytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := w.active.Size()
	for _, mt := range w.frozen {
		n += mt.Size()
	}
	return n
}

// CurrentManifest returns the current manifest state.
func (w *Writer) CurrentManifest() *Manifest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentManifest
}

// adoptManifest records a manifest written by the compactor so that reads in
// this process observe compaction results (and stop referencing SSTables the
// compactor is about to delete). It is a no-op if m is older than what we
// already hold.
func (w *Writer) adoptManifest(m *Manifest) {
	if m == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentManifest == nil || m.Version > w.currentManifest.Version {
		w.currentManifest = m
	}
}

// DropFrozen removes all frozen MemTables (called after successful read path
// initialization ensures L0 SSTables are available).
func (w *Writer) DropFrozen() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.frozen = nil
}

// BatchOpType identifies the kind of a BatchOp.
type BatchOpType int

const (
	BatchPut BatchOpType = iota
	BatchDelete
)

// BatchOp is a single operation in a batch write.
type BatchOp struct {
	Type  BatchOpType
	Key   string
	Value []byte // ignored for BatchDelete
}

// WriteBatch applies multiple operations under one lock, so readers never see
// a partial batch and the batch lands in a single flush. Unknown op types are
// rejected before anything is applied. Returns a sequence number per op.
func (w *Writer) WriteBatch(ctx context.Context, ops []BatchOp) ([]uint64, error) {
	for _, op := range ops {
		if op.Type != BatchPut && op.Type != BatchDelete {
			return nil, fmt.Errorf("writer: unknown batch op type: %d", op.Type)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(ops) == 0 {
		return nil, nil
	}

	seqs := make([]uint64, len(ops))
	for i, op := range ops {
		w.seqNum++
		seqs[i] = w.seqNum
		if op.Type == BatchPut {
			w.active.Put(op.Key, op.Value, w.seqNum)
		} else {
			w.active.Delete(op.Key, w.seqNum)
		}
	}

	if w.active.Size() >= w.maxSize {
		if err := w.flushLocked(ctx); err != nil {
			return nil, err
		}
	}
	return seqs, nil
}

// CompareAndSwap sets key to newValue only if its current value equals expected.
// A nil expected means the key must be absent or deleted (put-if-absent); a nil
// newValue deletes the key. Only the writer's local state is consulted, which is
// authoritative in this single-writer design only if the key is also checked
// against on-disk data, so SSTables are consulted through lookup.
//
// lookup is called with w.mu held and receives the current manifest; it must
// not call back into any Writer method that takes w.mu.
func (w *Writer) CompareAndSwap(ctx context.Context, key string, expected, newValue []byte, lookup func(ctx context.Context, key string, m *Manifest) (*Entry, bool, error)) (bool, uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	e, ok := w.active.Get(key)
	if !ok {
		for _, mt := range w.frozen {
			if e, ok = mt.Get(key); ok {
				break
			}
		}
	}
	if !ok {
		var err error
		e, ok, err = lookup(ctx, key, w.currentManifest)
		if err != nil {
			return false, 0, err
		}
	}

	// A tombstone counts as absent.
	present := ok && e.Value != nil
	var match bool
	if expected == nil {
		match = !present
	} else {
		match = present && bytes.Equal(e.Value, expected)
	}
	if !match {
		return false, 0, nil
	}

	w.seqNum++
	seq := w.seqNum
	if newValue == nil {
		w.active.Delete(key, seq)
	} else {
		w.active.Put(key, newValue, seq)
	}

	if w.active.Size() >= w.maxSize {
		if err := w.flushLocked(ctx); err != nil {
			return false, 0, err
		}
	}
	return true, seq, nil
}
