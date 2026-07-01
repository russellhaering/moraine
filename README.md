# Moraine

Moraine is an embedded Go key/value store built as an object-storage-backed LSM tree. It stores immutable SSTables in an `objstore.ObjectStore` implementation, tracks state with versioned manifests, and exposes a small byte-slice API for applications that want durable sorted storage without running a database server.

The code was extracted from WasmDB and is intentionally boring inside: MemTables, SSTables, WAL, manifests, readers, writers, and compaction keep their standard storage-engine names.

## Features

- Embedded Go library; no server process.
- Pluggable object storage through `objstore.ObjectStore`.
- In-memory object store for tests and local experiments.
- S3-compatible object store implementation.
- Single-writer LSM engine with epoch fencing.
- Versioned manifest files with conditional writes.
- WAL persisted as SSTables in object storage.
- Skip-list MemTable.
- SSTables with data blocks, index, footer, and bloom filter.
- Read path over active MemTable, frozen MemTables, L0 SSTables, and sorted runs.
- Cursor-style range scans with `ScanRange`.
- Sequence-number tailing with `ScanSince`.
- In-memory block cache and optional disk SSTable cache.
- Apache-2.0 licensed.

## Status

Moraine is a small extracted storage engine. The API is usable, but young. Treat it as pre-1.0 until the module has tagged releases and compatibility promises.

Important current constraints:

- Values are raw `[]byte`; serialization is the caller's responsibility.
- Keys are strings and sort lexicographically.
- A database prefix is intended to have one active writer.
- There are no multi-key transactions.
- Object-store consistency and conditional-write semantics matter for correctness.

## Install

```bash
go get github.com/russellhaering/moraine
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/objstore"
)

func main() {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := moraine.Open(ctx, moraine.DBConfig{
		Store:           store,
		Prefix:          "example",
		MemTableMaxSize: 64 << 20,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Put(ctx, "hello", []byte("world")); err != nil {
		log.Fatal(err)
	}
	if err := db.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	entry, ok, err := db.Get(ctx, "hello")
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("%s=%s\n", entry.Key, entry.Value)
	}
}
```

## S3-backed storage

```go
store, err := objstore.NewS3Store(ctx, objstore.S3Config{
	Bucket: "my-bucket",
	Region: "us-west-2",
	Prefix: "moraine/",
})
if err != nil {
	return err
}

db, err := moraine.Open(ctx, moraine.DBConfig{
	Store:           store,
	Prefix:          "tables/users",
	MemTableMaxSize: 64 << 20,
	BlockCacheSize:  256 << 20,
	DiskCacheDir:    "/tmp/moraine-cache",
	DiskCacheSize:   1 << 30,
	L0CompactThresh: 4,
})
```

`Endpoint` can be set in `objstore.S3Config` for S3-compatible services such as MinIO.

## API overview

```go
func Open(ctx context.Context, cfg DBConfig) (*DB, error)

func (db *DB) Put(ctx context.Context, key string, value []byte) (uint64, error)
func (db *DB) Delete(ctx context.Context, key string) (uint64, error)
func (db *DB) Get(ctx context.Context, key string) (*Entry, bool, error)
func (db *DB) Flush(ctx context.Context) error
func (db *DB) Scan(ctx context.Context) ([]Entry, error)
func (db *DB) ScanRange(ctx context.Context, afterKey string, limit int) (*ScanRangeResult, error)
func (db *DB) ScanSince(ctx context.Context, sinceSeq uint64) ([]Entry, error)
func (db *DB) Compact(ctx context.Context) error
func (db *DB) Close() error
```

`Put` and `Delete` return a monotonically increasing sequence number. `Delete` writes a tombstone. `ScanSince` includes tombstones so downstream indexes can observe deletes.

## Object stores

Moraine depends on this interface:

```go
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte, ifNoneMatch bool) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	Head(ctx context.Context, key string) (*ObjectMeta, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Exists(ctx context.Context, key string) (bool, error)
}
```

The package includes:

- `objstore.NewMemoryStore()` for tests and local use.
- `objstore.NewS3Store(ctx, cfg)` for S3-compatible storage.

## Development

```bash
go test ./...
go build ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
