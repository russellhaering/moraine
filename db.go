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
	pool            *CompactionPool
	compactOnClose  bool

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
	closeErr  error
}

// DBConfig holds configuration for opening an LSM database.
type DBConfig struct {
	Store           objstore.ObjectStore
	Prefix          string
	MemTableMaxSize int64
	BlockCacheSize  int64 // bytes for in-memory block cache
	DiskCacheDir    string
	DiskCacheSize   int64
	// BlockCache and DiskCache, when set, are used instead of creating
	// per-database caches from the size and directory settings above, so
	// many databases in one process can share a memory and disk budget.
	// Cache keys are object paths, so databases sharing a cache must use
	// distinct prefixes.
	BlockCache      *cache.LRUBlockCache
	DiskCache       *cache.DiskCache
	L0CompactThresh int
	// CompactInterval is how often a database without a CompactionPool
	// checks whether to compact. It is unused with a pool.
	CompactInterval time.Duration
	// CompactionPool, when set, runs this database's compaction and garbage
	// collection on the pool's shared workers instead of a polling goroutine
	// per database.
	CompactionPool *CompactionPool
	// CompactOnClose makes Close merge every L0 SSTable into the sorted runs
	// after its final flush, so the next Open reads fewer SSTables at the
	// cost of a slower Close.
	CompactOnClose bool
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

	blockCache := cfg.BlockCache
	if blockCache == nil {
		blockCacheSize := cfg.BlockCacheSize
		if blockCacheSize <= 0 {
			blockCacheSize = 256 << 20 // 256MB
		}
		blockCache = cache.NewLRUBlockCache(blockCacheSize)
	}

	diskCache := cfg.DiskCache
	if diskCache == nil && cfg.DiskCacheDir != "" {
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
	compactor.onCommit = writer.adoptManifest
	compactor.epoch = writer.epoch

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
		pool:            cfg.CompactionPool,
		compactOnClose:  cfg.CompactOnClose,
		closeCh:         make(chan struct{}),
	}

	// Start background compaction.
	if db.pool != nil {
		writer.onFlush = func(l0 int) {
			if l0 >= compactThresh {
				db.pool.trigger(compactor, l0)
			}
		}
		db.pool.register(compactor, len(writer.CurrentManifest().L0))
	} else {
		db.wg.Add(1)
		go db.compactLoop()
	}

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

// WriteBatch applies multiple operations atomically under a single lock.
// All operations land in the same flush. Unknown op types are rejected.
func (db *DB) WriteBatch(ctx context.Context, ops []BatchOp) ([]uint64, error) {
	return db.writer.WriteBatch(ctx, ops)
}

// CompareAndSwap atomically sets key to newValue only if its current value
// equals expected. A nil expected means put-if-absent; a nil newValue deletes.
// The lookup function is used to check on-disk SSTables when the key is not
// found in the writer's local state.
func (db *DB) CompareAndSwap(ctx context.Context, key string, expected, newValue []byte) (bool, uint64, error) {
	return db.writer.CompareAndSwap(ctx, key, expected, newValue,
		func(ctx context.Context, key string, m *Manifest) (*Entry, bool, error) {
			return db.reader.Get(ctx, key, NewMemTable(), nil, m)
		})
}

// ScanPrefix returns up to limit non-tombstone entries whose key has the
// given prefix. Keys are returned in sorted order.
func (db *DB) ScanPrefix(ctx context.Context, prefix string, limit int) ([]Entry, error) {
	active := db.writer.ActiveMemTable()
	frozen := db.writer.FrozenMemTables()
	manifest := db.writer.CurrentManifest()
	return db.reader.ScanPrefix(ctx, prefix, limit, active, frozen, manifest)
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

// Close flushes any buffered writes to object storage and shuts down the
// database, stopping background compaction. With CompactOnClose it then
// compacts every L0 SSTable. It is safe to call more than once; subsequent
// calls return the result of the first.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		if db.pool != nil {
			db.pool.unregister(db.compactor)
		}
		close(db.closeCh)
		db.wg.Wait()
		ctx := context.Background()
		db.closeErr = db.writer.Flush(ctx)
		if db.closeErr == nil && db.compactOnClose {
			db.closeErr = db.compactor.CompactAll(ctx)
		}
	})
	return db.closeErr
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
