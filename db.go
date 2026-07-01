package moraine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/russellhaering/moraine/cache"
	"github.com/russellhaering/moraine/objstore"
)

// DB is the top-level LSM database, tying together the writer, reader,
// and compactor.
type DB struct {
	writer    *Writer
	reader    *Reader
	compactor *Compactor
	store     objstore.ObjectStore
	prefix    string

	compactInterval time.Duration
	compactThresh   int

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// DBConfig holds configuration for opening an LSM database.
type DBConfig struct {
	Store           objstore.ObjectStore
	Prefix          string
	MemTableMaxSize int64
	BlockCacheSize  int64 // bytes for in-memory block cache
	DiskCacheDir    string
	DiskCacheSize   int64
	L0CompactThresh int
	CompactInterval time.Duration
}

// Open opens an LSM database.
func Open(ctx context.Context, cfg DBConfig) (*DB, error) {
	writer, err := OpenWriter(ctx, WriterConfig{
		Store:           cfg.Store,
		Prefix:          cfg.Prefix,
		MemTableMaxSize: cfg.MemTableMaxSize,
	})
	if err != nil {
		return nil, fmt.Errorf("lsm: open writer: %w", err)
	}

	blockCacheSize := cfg.BlockCacheSize
	if blockCacheSize <= 0 {
		blockCacheSize = 256 << 20 // 256MB
	}
	blockCache := cache.NewLRUBlockCache(blockCacheSize)

	var diskCache *cache.DiskCache
	if cfg.DiskCacheDir != "" {
		diskCacheSize := cfg.DiskCacheSize
		if diskCacheSize <= 0 {
			diskCacheSize = 1 << 30 // 1GB
		}
		dc, err := cache.NewDiskCache(cfg.DiskCacheDir, diskCacheSize)
		if err != nil {
			slog.Warn("failed to create disk cache", "err", err)
		} else {
			diskCache = dc
		}
	}

	reader := NewReader(cfg.Store, blockCache, diskCache)

	compactThresh := cfg.L0CompactThresh
	if compactThresh <= 0 {
		compactThresh = 4
	}
	compactor := NewCompactor(cfg.Store, cfg.Prefix, compactThresh)

	compactInterval := cfg.CompactInterval
	if compactInterval <= 0 {
		compactInterval = 30 * time.Second
	}

	db := &DB{
		writer:          writer,
		reader:          reader,
		compactor:       compactor,
		store:           cfg.Store,
		prefix:          cfg.Prefix,
		compactInterval: compactInterval,
		compactThresh:   compactThresh,
		closeCh:         make(chan struct{}),
	}

	// Start background compaction.
	db.wg.Add(1)
	go db.compactLoop()

	return db, nil
}

// Put inserts or updates a key-value pair.
func (db *DB) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return db.writer.Put(ctx, key, value)
}

// Delete marks a key as deleted.
func (db *DB) Delete(ctx context.Context, key string) (uint64, error) {
	return db.writer.Delete(ctx, key)
}

// Get reads a key, searching MemTables first for read-after-write consistency,
// then L0 and sorted runs.
func (db *DB) Get(ctx context.Context, key string) (*Entry, bool, error) {
	// First check writer's local state (active + frozen MemTables).
	if e, ok := db.writer.Get(key); ok {
		return e, true, nil
	}

	// Then search on-disk SSTables.
	manifest := db.writer.CurrentManifest()
	return db.reader.Get(ctx, key, NewMemTable(), nil, manifest)
}

// Flush synchronously flushes the active MemTable.
func (db *DB) Flush(ctx context.Context) error {
	return db.writer.Flush(ctx)
}

// Scan returns all non-tombstone entries in key order.
func (db *DB) Scan(ctx context.Context) ([]Entry, error) {
	active := db.writer.ActiveMemTable()
	frozen := db.writer.FrozenMemTables()
	manifest := db.writer.CurrentManifest()
	return db.reader.Scan(ctx, active, frozen, manifest)
}

// ScanRange returns up to limit non-tombstone entries with key > afterKey,
// using seek-based cursor pagination.
func (db *DB) ScanRange(ctx context.Context, afterKey string, limit int) (*ScanRangeResult, error) {
	active := db.writer.ActiveMemTable()
	frozen := db.writer.FrozenMemTables()
	manifest := db.writer.CurrentManifest()
	return db.reader.ScanRange(ctx, afterKey, limit, active, frozen, manifest)
}

// ScanSince returns all entries (including tombstones) with SeqNum > sinceSeq,
// skipping SSTables that contain no new data.
func (db *DB) ScanSince(ctx context.Context, sinceSeq uint64) ([]Entry, error) {
	active := db.writer.ActiveMemTable()
	frozen := db.writer.FrozenMemTables()
	manifest := db.writer.CurrentManifest()
	return db.reader.ScanSince(ctx, sinceSeq, active, frozen, manifest)
}

// Close shuts down the database, stopping background compaction.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		close(db.closeCh)
	})
	db.wg.Wait()
	return nil
}

func (db *DB) compactLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.compactInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			if err := db.compactor.MaybeCompact(context.Background()); err != nil {
				slog.Error("compaction error", "err", err)
			}
		}
	}
}

// Compact triggers an immediate compaction check (useful for testing).
func (db *DB) Compact(ctx context.Context) error {
	return db.compactor.MaybeCompact(ctx)
}
