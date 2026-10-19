package vfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

// countingStore counts blob uploads and downloads.
type countingStore struct {
	objstore.ObjectStore
	puts, gets, getBytes atomic.Int64
}

func (c *countingStore) Put(ctx context.Context, key string, data []byte, ifNoneMatch bool) error {
	c.puts.Add(1)
	return c.ObjectStore.Put(ctx, key, data, ifNoneMatch)
}

func (c *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.gets.Add(1)
	return c.ObjectStore.Get(ctx, key)
}

// counted makes e's Store count its blob traffic.
func (e *env) counted() *countingStore {
	c := &countingStore{ObjectStore: e.store}
	e.s.blobs = c
	return c
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func rawInode(t *testing.T, e *env, ns, p string) *inode {
	t.Helper()
	n, err := e.s.get(context.Background(), ns, p)
	must(t, err)
	if n == nil {
		t.Fatalf("%s missing", p)
	}
	return n
}

func TestChunkedRoundTrip(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	data := pattern(testChunk*3 + 100)
	must(t, f.WriteFile(ctx, "big", data, 0o644))

	n := rawInode(t, e, "main", "big")
	if n.Blob || len(n.Chunks) != 4 || n.Hash != sum(data) || n.Size != int64(len(data)) {
		t.Fatalf("inode: blob=%v chunks=%d hash ok=%v", n.Blob, len(n.Chunks), n.Hash == sum(data))
	}
	for i, c := range n.Chunks[:3] {
		if c.Size != testChunk {
			t.Fatalf("chunk %d has %d bytes", i, c.Size)
		}
	}
	if got, err := f.ReadFile(ctx, "big"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadFile: %d bytes, %v", len(got), err)
	}
	if k := e.blobCount(t); k != 4 {
		t.Fatalf("%d blobs, want 4", k)
	}

	// A file between the inline threshold and one chunk is a single chunk.
	mid := pattern(testChunk / 2)
	must(t, f.WriteFile(ctx, "mid", mid, 0o644))
	if n := rawInode(t, e, "main", "mid"); len(n.Chunks) != 1 || n.Hash != sum(mid) {
		t.Fatalf("mid inode: %+v", n.Chunks)
	}
	// Diff compares the whole-file hash, however the file was chunked.
	must(t, e.s.Fork(ctx, "main", "copy"))
	if ch, err := e.s.Diff(ctx, "main", "copy"); err != nil || len(ch) != 0 {
		t.Fatalf("diff after fork: %v %v", ch, err)
	}
}

func TestChunkedRangedReads(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	data := pattern(testChunk*4 + 37)
	must(t, f.WriteFile(ctx, "big", data, 0o644))
	file, err := f.Open(ctx, "big")
	must(t, err)

	for _, tc := range []struct{ off, n int }{
		{0, 10}, {testChunk - 5, 10}, {testChunk - 1, 2}, {testChunk, testChunk},
		{testChunk - 3, testChunk*2 + 6}, {10, len(data) - 10}, {len(data) - 5, 5},
	} {
		buf := make([]byte, tc.n)
		n, err := file.ReadAt(buf, int64(tc.off))
		if err != nil || n != tc.n || !bytes.Equal(buf, data[tc.off:tc.off+tc.n]) {
			t.Fatalf("ReadAt(%d, %d) = %d, %v", tc.n, tc.off, n, err)
		}
	}
	// Reading past the end is short with io.EOF.
	buf := make([]byte, 20)
	if n, err := file.ReadAt(buf, int64(len(data)-5)); n != 5 || err != io.EOF || !bytes.Equal(buf[:5], data[len(data)-5:]) {
		t.Fatalf("ReadAt past end = %d, %v", n, err)
	}
	// Ranged reads fetch no whole chunks.
	c := e.counted()
	file.ReadAt(make([]byte, 10), testChunk-5)
	if c.gets.Load() != 0 {
		t.Fatalf("ranged read did %d whole-blob GETs", c.gets.Load())
	}
	if _, err := file.Seek(testChunk-4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(got, data[testChunk-4:]) {
		t.Fatalf("read after seek: %d bytes, %v", len(got), err)
	}
}

func TestAppendUploadsOnlyTail(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	data := pattern(testChunk*3 + 100)
	must(t, f.WriteFile(ctx, "log", data, 0o640))
	before := rawInode(t, e, "main", "log")
	c := e.counted()

	// Grow the partial last chunk.
	add := pattern(50)
	must(t, f.Append(ctx, "log", add))
	data = append(data, add...)
	if c.puts.Load() != 1 || c.gets.Load() != 1 {
		t.Fatalf("append within last chunk: %d puts, %d gets, want 1, 1", c.puts.Load(), c.gets.Load())
	}
	n := rawInode(t, e, "main", "log")
	if n.Hash != sum(data) || n.Size != int64(len(data)) || n.Mode != 0o640 || len(n.Chunks) != 4 {
		t.Fatalf("after append: size %d mode %v chunks %d", n.Size, n.Mode, len(n.Chunks))
	}
	for i := range 3 {
		if n.Chunks[i] != before.Chunks[i] {
			t.Fatalf("chunk %d changed", i)
		}
	}

	// Spill into two new chunks: rewritten tail + 2 new.
	c.puts.Store(0)
	add = pattern(testChunk*2 - 20)
	must(t, f.Append(ctx, "log", add))
	data = append(data, add...)
	if c.puts.Load() != 3 {
		t.Fatalf("append spilling over: %d puts, want 3", c.puts.Load())
	}
	n = rawInode(t, e, "main", "log")
	if n.Hash != sum(data) || len(n.Chunks) != 6 {
		t.Fatalf("after spill: chunks %d", len(n.Chunks))
	}
	if got, err := f.ReadFile(ctx, "log"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadFile after appends: %v", err)
	}
	// The result is identical to writing the whole file at once.
	must(t, f.WriteFile(ctx, "whole", data, 0o640))
	w := rawInode(t, e, "main", "whole")
	if w.Hash != n.Hash || fmt.Sprint(w.Chunks) != fmt.Sprint(n.Chunks) {
		t.Fatal("appended file differs from the same content written whole")
	}

	// A no-op append changes nothing; appending creates missing files.
	c.puts.Store(0)
	must(t, f.Append(ctx, "log", nil))
	must(t, f.Append(ctx, "new", []byte("hi")))
	must(t, f.Append(ctx, "new", []byte(" there")))
	if got, _ := f.ReadFile(ctx, "new"); string(got) != "hi there" || c.puts.Load() != 0 {
		t.Fatalf("new = %q, %d puts", got, c.puts.Load())
	}
}

func TestAppendInlineGrowsToChunked(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.WriteFile(ctx, "f", []byte("0123456789"), 0o644))
	if n := rawInode(t, e, "main", "f"); n.Chunks != nil || n.Data == nil {
		t.Fatal("small file not inline")
	}
	more := pattern(testChunk + 30)
	must(t, f.Append(ctx, "f", more))
	want := append([]byte("0123456789"), more...)
	n := rawInode(t, e, "main", "f")
	if len(n.Chunks) != 2 || n.Hash != sum(want) || n.Data != nil {
		t.Fatalf("grown inode: %d chunks", len(n.Chunks))
	}
	if got, err := f.ReadFile(ctx, "f"); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := f.Append(ctx, "nodir/x", []byte("x")); err == nil {
		t.Fatal("append with missing parent succeeded")
	}
	if err := f.Append(ctx, ".", []byte("x")); err == nil {
		t.Fatal("append to root succeeded")
	}
}

func TestConcurrentAppends(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.WriteFile(ctx, "log", pattern(testChunk+10), 0o644))
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.Append(ctx, "log", []byte("0123456789")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got, err := f.ReadFile(ctx, "log")
	must(t, err)
	n := rawInode(t, e, "main", "log")
	if len(got) != testChunk+10+100 || n.Hash != sum(got) {
		t.Fatalf("%d bytes after concurrent appends, hash ok=%v", len(got), n.Hash == sum(got))
	}
}

func TestCreateStreams(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	c := e.counted()
	data := pattern(testChunk*5 + 11)

	w, err := f.Create(ctx, "stream", 0o600)
	must(t, err)
	cw := w.(*fileWriter)
	for off := 0; off < len(data); off += 100 {
		end := min(off+100, len(data))
		if _, err := w.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		if len(cw.c.buf) > testChunk {
			t.Fatalf("buffered %d bytes, more than a chunk", len(cw.c.buf))
		}
	}
	// Full chunks went out as the stream progressed; the last is held back.
	if got := c.puts.Load(); got != 5 {
		t.Fatalf("%d uploads before Close, want 5", got)
	}
	if _, err := f.Stat(ctx, "stream"); err == nil {
		t.Fatal("file visible before Close")
	}
	must(t, w.Close())
	if c.puts.Load() != 6 {
		t.Fatalf("%d uploads after Close, want 6", c.puts.Load())
	}
	n := rawInode(t, e, "main", "stream")
	if n.Hash != sum(data) || len(n.Chunks) != 6 || n.Mode != 0o600 {
		t.Fatalf("inode: %d chunks mode %v", len(n.Chunks), n.Mode)
	}
	if got, err := f.ReadFile(ctx, "stream"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("second Close succeeded")
	}

	// Small totals end up inline, and empty files work; replacing is atomic.
	w, err = f.Create(ctx, "stream", 0o644)
	must(t, err)
	w.Write([]byte("tiny"))
	must(t, w.Close())
	if n := rawInode(t, e, "main", "stream"); n.Chunks != nil || string(n.Data) != "tiny" {
		t.Fatalf("small stream not inline: %+v", n)
	}
	w, err = f.Create(ctx, "empty", 0o644)
	must(t, err)
	must(t, w.Close())
	if got, err := f.ReadFile(ctx, "empty"); err != nil || len(got) != 0 {
		t.Fatalf("empty: %q %v", got, err)
	}

	// Error conventions match WriteFile.
	must(t, f.Mkdir(ctx, "d", 0o755))
	if _, err := f.Create(ctx, "d", 0o644); err == nil {
		t.Fatal("Create over a directory succeeded")
	}
	if _, err := f.Create(ctx, "missing/x", 0o644); err == nil {
		t.Fatal("Create with missing parent succeeded")
	}
	if _, err := f.Create(ctx, "../x", 0o644); err == nil {
		t.Fatal("Create with invalid path succeeded")
	}
}

// legacyBlob writes data as a pre-chunking single-blob inode.
func legacyBlob(t *testing.T, e *env, ns, p string, data []byte) {
	t.Helper()
	ctx := context.Background()
	hash := hashContent(data)
	must(t, e.s.putBlob(ctx, hash, data))
	n := &inode{Mode: 0o644, Size: int64(len(data)), ModTime: 1, Hash: hash, Blob: true}
	_, err := e.db.Put(ctx, e.s.entryKey(ns, p), encodeInode(n))
	must(t, err)
	_, err = e.db.Put(ctx, e.s.registryKey(ns), []byte{1})
	must(t, err)
}

func TestLegacyBlobInode(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	data := pattern(testChunk*2 + 5) // larger than a chunk, but one legacy blob
	legacyBlob(t, e, "main", "old", data)

	if got, err := f.ReadFile(ctx, "old"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadFile: %v", err)
	}
	file, err := f.Open(ctx, "old")
	must(t, err)
	buf := make([]byte, 30)
	if n, err := file.ReadAt(buf, testChunk-10); err != nil || n != 30 || !bytes.Equal(buf, data[testChunk-10:testChunk+20]) {
		t.Fatalf("ReadAt: %d %v", n, err)
	}

	// Copying to another database carries the blob.
	dst := newEnv(t)
	must(t, e.s.CopyTo(ctx, "main", dst.s, "copy"))
	if got, err := dst.fs(t, "copy").ReadFile(ctx, "old"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("copied: %v", err)
	}

	// GC keeps it while referenced, then deletes it once orphaned.
	e.clock.Advance(DefaultGCGrace + time.Second)
	if st, err := e.s.CollectGarbage(ctx); err != nil || st.Referenced != 1 || st.Deleted != 0 {
		t.Fatalf("gc: %+v %v", st, err)
	}
	// Appending migrates it to chunks.
	must(t, f.Append(ctx, "old", []byte("tail")))
	want := append(bytes.Clone(data), "tail"...)
	if n := rawInode(t, e, "main", "old"); n.Blob || n.Hash != sum(want) || len(n.Chunks) != 3 {
		t.Fatalf("after append: blob=%v chunks=%d", n.Blob, len(n.Chunks))
	}
	if got, err := f.ReadFile(ctx, "old"); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadFile after append: %v", err)
	}
	must(t, f.Remove(ctx, "old"))
	e.s.CollectGarbage(ctx)
	e.clock.Advance(DefaultGCGrace + time.Second)
	if _, err := e.s.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if k := e.blobCount(t); k != 0 {
		t.Fatalf("%d blobs left after removing the file", k)
	}
}

func TestChunkGarbageCollection(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	data := pattern(testChunk*3 + 1) // 4 chunks
	must(t, f.WriteFile(ctx, "a", data, 0o644))
	must(t, e.s.Fork(ctx, "main", "ckpt"))
	// Rewrite so that only the first chunks are shared with the checkpoint.
	changed := append(bytes.Clone(data[:testChunk*2]), pattern(testChunk)...)
	must(t, f.WriteFile(ctx, "a", changed, 0o644))

	run := func() GCStats {
		t.Helper()
		st, err := e.s.CollectGarbage(ctx)
		must(t, err)
		return st
	}
	e.clock.Advance(DefaultGCGrace + time.Second)
	if st := run(); st.Deleted != 0 {
		t.Fatalf("deleted chunks a namespace still references: %+v", st)
	}
	e.clock.Advance(DefaultGCGrace + time.Second)
	if st := run(); st.Deleted != 0 {
		t.Fatalf("second run: %+v", st)
	}
	if got, err := e.fs(t, "ckpt").ReadFile(ctx, "a"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("checkpoint damaged: %v", err)
	}

	must(t, e.s.DeleteNamespace(ctx, "ckpt"))
	run() // first sighting
	e.clock.Advance(DefaultGCGrace + time.Second)
	// The checkpoint's last two chunks are now orphaned; the first two are
	// still referenced by main.
	if st := run(); st.Deleted != 2 {
		t.Fatalf("after grace: %+v", st)
	}
	if got, err := f.ReadFile(ctx, "a"); err != nil || !bytes.Equal(got, changed) {
		t.Fatalf("live file damaged: %v", err)
	}
}

// slowDeleteStore delays every Delete and reports when one starts.
type slowDeleteStore struct {
	objstore.ObjectStore
	delay   time.Duration
	started chan struct{}
	deletes atomic.Int64
}

func (s *slowDeleteStore) Delete(ctx context.Context, key string) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	time.Sleep(s.delay)
	err := s.ObjectStore.Delete(ctx, key)
	s.deletes.Add(1)
	return err
}

// TestGarbageCollectionDoesNotBlockWrites checks that writes proceed while
// GC is in its deletion phase, instead of waiting for all deletes.
func TestGarbageCollectionDoesNotBlockWrites(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	const orphans = 10
	for i := range orphans {
		must(t, e.s.putBlob(ctx, hashContent(pattern(100+i)), pattern(100+i)))
	}
	if _, err := e.s.CollectGarbage(ctx); err != nil { // first sighting
		t.Fatal(err)
	}
	e.clock.Advance(DefaultGCGrace + time.Second)

	slow := &slowDeleteStore{ObjectStore: e.store, delay: 50 * time.Millisecond, started: make(chan struct{}, 1)}
	e.s.blobs = slow
	done := make(chan GCStats)
	go func() {
		st, err := e.s.CollectGarbage(ctx)
		if err != nil {
			t.Error(err)
		}
		done <- st
	}()
	<-slow.started // deletion phase is under way

	deletedAtWrite := make(chan int64, 2)
	for i, data := range [][]byte{[]byte("inline"), pattern(testChunk + 5)} {
		must(t, f.WriteFile(ctx, fmt.Sprintf("w%d", i), data, 0o644))
		deletedAtWrite <- slow.deletes.Load()
	}
	// Both writes, including one that uploads blobs, finished before the
	// deletion phase did.
	if got := <-deletedAtWrite; got >= orphans {
		t.Fatalf("inline write waited for %d deletes", got)
	}
	if got := <-deletedAtWrite; got >= orphans {
		t.Fatalf("blob write waited for %d deletes", got)
	}
	if st := <-done; st.Deleted != orphans {
		t.Fatalf("gc: %+v", st)
	}
	if got, err := f.ReadFile(ctx, "w1"); err != nil || !bytes.Equal(got, pattern(testChunk+5)) {
		t.Fatalf("write during gc damaged: %v", err)
	}
}

// TestGarbageCollectionSkipsBlobReusedAfterMark covers a write that reuses a
// candidate between the mark and its deletion.
func TestGarbageCollectionSkipsBlobReusedAfterMark(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	data := pattern(100)
	must(t, e.s.putBlob(ctx, hashContent(data), data))
	if _, err := e.s.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(DefaultGCGrace + time.Second)

	var stats GCStats
	keys, err := e.store.List(ctx, "blobs/")
	must(t, err)
	candidates, err := e.s.markGarbage(ctx, keys, &stats)
	must(t, err)
	if len(candidates) != 1 {
		t.Fatalf("%d candidates, want 1", len(candidates))
	}
	// A write reuses the blob and commits after the mark.
	must(t, f.WriteFile(ctx, "reuse", data, 0o644))
	e.s.sweepBlob(ctx, candidates[0], &stats)
	if stats.Deleted != 0 {
		t.Fatal("swept a blob a write reused after the mark")
	}
	if got, err := f.ReadFile(ctx, "reuse"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadFile: %v", err)
	}
}
