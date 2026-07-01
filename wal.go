package moraine

import (
	"context"
	"fmt"

	"github.com/russellhaering/moraine/objstore"
)

// WAL writes frozen MemTables as sequential SSTable files to object storage.
// WAL files are used for recovery only; normal reads go through MemTable or L0.
type WAL struct {
	store  objstore.ObjectStore
	prefix string
	nextID uint64
}

// NewWAL creates a new WAL writer.
func NewWAL(store objstore.ObjectStore, prefix string, startID uint64) *WAL {
	return &WAL{
		store:  store,
		prefix: prefix,
		nextID: startID,
	}
}

// walPath returns the object storage key for a WAL file.
func (w *WAL) walPath(id uint64) string {
	return fmt.Sprintf("%s/wal/%020d.sst", w.prefix, id)
}

// Write converts a frozen MemTable to an SSTable and writes it to object storage
// using a conditional put for fencing. Returns the WAL ID, SSTable metadata,
// and any error.
func (w *WAL) Write(ctx context.Context, mt *MemTable) (uint64, SSTableMeta, error) {
	id := w.nextID

	writer := NewSSTableWriter(fmt.Sprintf("wal-%020d", id), DefaultBlockSize)

	iter := mt.Iterator()
	for iter.Next() {
		writer.Add(iter.Entry())
	}

	data, meta, err := writer.Finish()
	if err != nil {
		return 0, SSTableMeta{}, fmt.Errorf("wal: finish sstable: %w", err)
	}

	key := w.walPath(id)
	if err := w.store.Put(ctx, key, data, true); err != nil {
		return 0, SSTableMeta{}, fmt.Errorf("wal: write %s: %w", key, err)
	}

	w.nextID++
	return id, meta, nil
}

// NextID returns the next WAL ID that will be assigned.
func (w *WAL) NextID() uint64 {
	return w.nextID
}

// ReadWAL reads a WAL file from object storage and returns an SSTableReader.
func ReadWAL(ctx context.Context, store objstore.ObjectStore, prefix string, id uint64) (*SSTableReader, error) {
	key := fmt.Sprintf("%s/wal/%020d.sst", prefix, id)
	data, err := store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("wal: read %s: %w", key, err)
	}
	return NewSSTableReader(fmt.Sprintf("wal-%020d", id), data)
}
