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
- Optional document/index layer with attribute, full-text, and vector search.
- In-memory block cache and optional disk SSTable cache.
- Apache-2.0 licensed.

## Status

Moraine is a small extracted storage engine. The API is usable, but young. Treat it as pre-1.0 until the module has tagged releases and compatibility promises.

Important current constraints:

- The root `moraine.DB` stores raw `[]byte`; serialization is the caller's responsibility.
- The `indexed` package provides a higher-level document API when you want built-in serialization and indexes.
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

## Layers

Moraine is organized as opt-in layers:

- `moraine`: the embedded object-storage-backed sorted key/value store.
- `indexed`: an embedded document table backed by `moraine.DB` with derived indexes.
- The future HTTP/server layer should build on the indexed layer rather than changing the raw KV API.

The indexed layer treats Moraine as the durable source of truth. Attribute, full-text, and vector indexes are derived state. Writes enqueue indexing work asynchronously; call `WaitForIndexes(ctx)` when a search must observe all writes accepted before that call.

```go
schema := &document.Schema{
	Fields: []document.FieldDefinition{
		{Name: "kind", Type: document.FieldTypeString, Indexed: true},
		{Name: "title", Type: document.FieldTypeString, FullText: true},
	},
}

table, err := indexed.OpenTable(ctx, indexed.TableConfig{
	Name:     "docs",
	DB:       db,
	Schema:   schema,
	CacheDir: "/tmp/moraine-indexes",
})
if err != nil {
	return err
}
defer table.Close()

err = table.PutDocument(ctx, &document.Document{
	ID:      "doc-1",
	Content: "the quick brown fox",
	Attributes: map[string]any{
		"kind":  "note",
		"title": "Forest notes",
	},
})
if err != nil {
	return err
}
if err := table.WaitForIndexes(ctx); err != nil {
	return err
}

docs, err := table.SearchAttributes(ctx, []index.Filter{
	{Field: "kind", Op: index.OpEq, Value: "note"},
}, 10, 0)
if err != nil {
	return err
}
_ = docs
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

## Cloudflare R2-backed storage

Cloudflare R2 works through the S3-compatible implementation. R2 uses an account-scoped endpoint and the S3 region `auto`.

```go
store, err := objstore.NewR2Store(ctx, objstore.R2Config{
	AccountID:       os.Getenv("MORAINE_R2_ACCOUNT_ID"),
	Bucket:          os.Getenv("MORAINE_R2_BUCKET"),
	AccessKeyID:     os.Getenv("MORAINE_R2_ACCESS_KEY_ID"),
	SecretAccessKey: os.Getenv("MORAINE_R2_SECRET_ACCESS_KEY"),
	Prefix:          "moraine",
})
if err != nil {
	return err
}
```

To run the opt-in R2 integration test, create an R2 bucket and an Object Read & Write token for that bucket, then export:

```bash
export MORAINE_R2_ACCOUNT_ID="<cloudflare-account-id>"
export MORAINE_R2_BUCKET="<bucket-name>"
export MORAINE_R2_ACCESS_KEY_ID="<r2-access-key-id>"
export MORAINE_R2_SECRET_ACCESS_KEY="<r2-secret-access-key>"

go test -run TestR2Integration ./...
```

Optional settings:

- `MORAINE_R2_PREFIX` scopes test objects under a custom prefix. The test adds a unique child prefix and cleans it up.
- `MORAINE_R2_JURISDICTION` can be set to `eu` or `fedramp` for jurisdiction-specific buckets.
- `MORAINE_R2_ENDPOINT` overrides endpoint construction entirely.

Cloudflare documents the S3 endpoint as `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`; jurisdiction-specific endpoints are required for jurisdictional buckets. See Cloudflare's [R2 S3 API compatibility](https://developers.cloudflare.com/r2/api/s3/api/) and [R2 authentication](https://developers.cloudflare.com/r2/api/tokens/) docs for current setup details.

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

The indexed table API lives in `github.com/russellhaering/moraine/indexed`:

```go
func OpenTable(ctx context.Context, cfg TableConfig) (*Table, error)

func (t *Table) Name() string
func (t *Table) System() bool
func (t *Table) Schema() *document.Schema
func (t *Table) IndexStatus() indexed.IndexStatus
func (t *Table) PutDocument(ctx context.Context, doc *document.Document) error
func (t *Table) PutDocumentsBulk(ctx context.Context, docs []*document.Document) error
func (t *Table) GetDocument(ctx context.Context, id string) (*document.Document, error)
func (t *Table) DeleteDocument(ctx context.Context, id string) error
func (t *Table) ListDocuments(ctx context.Context, limit int, afterKey string) ([]*document.Document, bool, error)
func (t *Table) SearchAttributes(ctx context.Context, filters []index.Filter, limit, offset int) ([]*document.Document, error)
func (t *Table) SearchText(ctx context.Context, query string, limit, offset int) ([]*document.Document, int, error)
func (t *Table) SearchVector(ctx context.Context, query []float32, k int) ([]*document.Document, error)
func (t *Table) SearchVectorByText(ctx context.Context, queryText string, k int) ([]*document.Document, error)
func (t *Table) WaitForIndexes(ctx context.Context) error
func (t *Table) RebuildIndexes(ctx context.Context) error
func (t *Table) SetSchema(ctx context.Context, schema *document.Schema) error
func (t *Table) Close() error
```

Search methods return `indexed.ErrIndexNotReady` until the startup rebuild finishes. Use `WaitForIndexes(ctx)` when a request must observe all writes accepted before the call, or inspect `IndexStatus()` for readiness/progress.

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
