# vfs

Package `vfs` is a hierarchical filesystem stored in a Moraine database. File metadata and small files live in the database; larger files are split into content-addressed chunks in object storage, stored once and shared by every file and namespace that references them.

A `Store` holds any number of **namespaces**, each a complete file tree. Forking a namespace copies only metadata, so namespaces are cheap enough to use as checkpoints, isolated working copies and forks, and `Diff` and `Apply` merge changes between them.

## Opening a store

```go
store, err := vfs.Open(db, vfs.Options{
	Blobs:      objectStore,       // required: where large contents go
	BlobPrefix: "agents/a1/blobs", // required: not shared with another Store
})
if err != nil {
	return err
}
main, err := store.FS("main")
```

| Option            | Default                   | Meaning |
| ----------------- | ------------------------- | ------- |
| `Blobs`           | required                  | Object store for file contents at or above the inline threshold. |
| `BlobPrefix`      | required                  | Object key prefix for blobs. Must belong to this Store alone. |
| `KeyPrefix`       | `"vfs/"`                  | Database key prefix for metadata. |
| `InlineThreshold` | 64 KiB                    | Files smaller than this are stored in the database itself. |
| `ChunkSize`       | 4 MiB                     | Size of each chunk of a large file; only a file's last chunk may be shorter. It can change between opens, and existing files keep their chunks. |
| `GCGrace`         | 10 minutes                | How long a blob must stay unreferenced before `CollectGarbage` deletes it. |
| `Now`             | `time.Now`                | Clock override, for tests. |

A Store assumes it is the only writer of its key prefix and blob prefix, just as a Moraine database has one writer.

## Using a filesystem

`FS` is one namespace. Paths follow `io/fs` conventions: unrooted, slash-separated, with `"."` as the root. Namespace names start with a letter or digit and contain only letters, digits, `.`, `_` and `-`. A namespace that has never been written to is an empty filesystem.

```go
_ = main.MkdirAll(ctx, "notes", 0o755)
_ = main.WriteFile(ctx, "notes/plan.md", []byte("# Plan"), 0o644)
data, _ := main.ReadFile(ctx, "notes/plan.md")
entries, _ := main.ReadDir(ctx, "notes")
```

- **Reading:** `Stat`, `ReadFile`, `ReadDir` and `Open`. A `File` supports `Read`, `ReadAt`, `Seek` and `ReadDir`. Reads from large files use ranged requests and fetch only the chunks they touch.
- **Writing:** `WriteFile`, `Mkdir`, `MkdirAll`, `Remove`, `RemoveAll` and `Rename`. Parent directories must already exist, except under `MkdirAll`. `Rename` moves a whole subtree. `RemoveAll(".")` empties the namespace.
- **Streaming:** `Create` returns an `io.WriteCloser` that buffers at most one chunk, so a file of any size is written in constant memory. Nothing is visible until `Close`, which replaces the file atomically. An abandoned writer leaves only unreferenced blobs, which garbage collection reclaims.
- **Appending:** `Append` extends a file without rewriting it. A chunked file re-uploads only its last chunk and any new ones. An inline file that grows past the threshold becomes chunked. Concurrent appends retry on conflict, so none are lost.
- **`io/fs` view:** `IOFS(ctx)` returns an `fs.FS` that implements `fs.ReadFileFS`, `fs.ReadDirFS` and `fs.StatFS`, so `fs.WalkDir`, `fs.Glob` and similar helpers work on it.
- **Errors:** errors are `*fs.PathError` values wrapping `fs.ErrNotExist`, `fs.ErrExist` or `fs.ErrInvalid`, or the package's `ErrNotEmpty`, `ErrIsDir` and `ErrNotDir`. Check them with `errors.Is`.

Each `FS` method is atomic. Changes are durable once the database is flushed; blobs are always uploaded before the metadata that references them is written.

## Namespaces: checkpoint, fork, merge

```go
_ = store.Fork(ctx, "main", "ckpt-1") // a checkpoint
_ = store.Fork(ctx, "main", "base")   // the snapshot the worker starts from
_ = store.Fork(ctx, "main", "worker") // an isolated working copy
// ... the worker edits its namespace ...

changes, _ := store.Diff(ctx, "base", "worker")
paths := make([]string, len(changes))
for i, c := range changes {
	paths[i] = c.Path
}

// Merge only if main has not touched those paths since the fork.
conflicts, err := store.ApplyIfUnchanged(ctx, "worker", "main", "base", paths)
```

- **`Fork(src, dst)`** makes `dst` an exact copy of `src`, replacing whatever `dst` held. Only metadata is copied.
- **`Diff(base, other)`** lists `Added`, `Modified` and `Removed` paths, sorted by path. Files are compared by content hash, so rewriting identical bytes, or chunking them differently, is not a change. Modification times are ignored.
- **`Apply(from, to, paths)`** makes each listed path in `to` match `from`, copying it or removing it, in one batch. It changes nothing if the result would leave an entry without a parent directory.
- **`ApplyIfUnchanged(from, to, base, paths)`** first checks for conflicts, under the same lock as the apply. If `to` changed any path since `base` that equals, contains or lies inside a listed path, it applies nothing and returns those conflicts. Use this rather than a separate `Diff` followed by `Apply`, which can race with writers.
- **`Namespaces`** lists the namespaces that have been written to. **`DeleteNamespace`** removes one.
- **`CopyTo(src, dstStore, dstName)`** copies a namespace into another Store, possibly in another database. It uploads any blobs the destination lacks.

## Garbage collection

Removing files and namespaces never deletes blobs, because other namespaces may still share them. Call `CollectGarbage` periodically to reclaim them:

```go
stats, err := store.CollectGarbage(ctx)
// stats.Blobs, Referenced, Deleted, Waiting, DeleteErrors
```

A blob is deleted only after it has been unreferenced, and unused by any write, for the whole `GCGrace` period. This makes collection safe to run alongside writers. Every write that references a blob restarts its grace period before committing, and each deletion re-checks eligibility under the same lock. The argument is written out in full above `CollectGarbage` in `gc.go`. Grace periods are tracked in memory, so a Store that restarts begins them again.

## Storage layout

Metadata keys, under `KeyPrefix`:

```
ns/{namespace}                    namespace registry
e/{namespace}/{parent}\x00{name}  one JSON entry per file or directory
```

Keying entries by parent makes listing a directory a single prefix scan, already sorted by name. The root directory has no entry.

An entry records mode, size, modification time and the SHA-256 of the file's full contents. A small file's bytes are stored inline. A large file instead has a list of chunk references (hash and size) plus the SHA-256 state after every chunk but the last. That saved state lets `Append` extend the whole-file hash by reading only the last chunk. Chunk blobs are stored at `{BlobPrefix}/{sha256}`. A whole chunk read by `ReadFile` or `Append` is checked against its hash; a ranged read by `File` is not. Entries from older versions that point to a single whole-file blob are still readable, and are rewritten as chunks the first time they are appended to.
