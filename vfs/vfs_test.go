package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	moraine "github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/objstore"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type env struct {
	store *objstore.MemoryStore
	db    *moraine.DB
	s     *Store
	clock *clock
}

const testInline = 16

func newEnv(t *testing.T) *env {
	t.Helper()
	store := objstore.NewMemoryStore()
	e := &env{store: store, clock: &clock{now: time.Unix(1_700_000_000, 0)}}
	e.open(t)
	return e
}

// open (re)opens the database and Store over the env's object store.
func (e *env) open(t *testing.T) {
	t.Helper()
	db, err := moraine.Open(context.Background(), moraine.DBConfig{Store: e.store, Prefix: "db", CompactInterval: time.Hour})
	if err != nil {
		t.Fatalf("moraine.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	s, err := Open(db, Options{Blobs: e.store, BlobPrefix: "blobs", InlineThreshold: testInline, Now: e.clock.Now})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	e.db, e.s = db, s
}

func (e *env) fs(t *testing.T, ns string) *FS {
	t.Helper()
	f, err := e.s.FS(ns)
	if err != nil {
		t.Fatalf("FS(%q): %v", ns, err)
	}
	return f
}

func (e *env) blobCount(t *testing.T) int {
	t.Helper()
	keys, err := e.store.List(context.Background(), "blobs/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return len(keys)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var big = bytes.Repeat([]byte("0123456789abcdef"), 8) // 128 bytes, stored as a blob

func TestFileOperations(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")

	must(t, f.MkdirAll(ctx, "src/pkg", 0o755))
	must(t, f.WriteFile(ctx, "src/pkg/small.go", []byte("package pkg"), 0o644))
	must(t, f.WriteFile(ctx, "src/pkg/big.bin", big, 0o600))
	must(t, f.WriteFile(ctx, "README", nil, 0o644))

	if got, err := f.ReadFile(ctx, "src/pkg/small.go"); err != nil || string(got) != "package pkg" {
		t.Fatalf("ReadFile small = %q, %v", got, err)
	}
	if got, err := f.ReadFile(ctx, "src/pkg/big.bin"); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("ReadFile big = %d bytes, %v", len(got), err)
	}
	if got, err := f.ReadFile(ctx, "README"); err != nil || len(got) != 0 {
		t.Fatalf("ReadFile empty = %q, %v", got, err)
	}
	if n := e.blobCount(t); n != 1 {
		t.Fatalf("%d blobs, want 1 (only big.bin)", n)
	}

	fi, err := f.Stat(ctx, "src/pkg/big.bin")
	must(t, err)
	if fi.Name() != "big.bin" || fi.Size() != int64(len(big)) || fi.Mode() != 0o600 || fi.IsDir() || !fi.ModTime().Equal(e.clock.Now()) {
		t.Fatalf("Stat = %s %d %v %v %v", fi.Name(), fi.Size(), fi.Mode(), fi.IsDir(), fi.ModTime())
	}

	entries, err := f.ReadDir(ctx, "src/pkg")
	must(t, err)
	var names []string
	for _, de := range entries {
		names = append(names, de.Name())
	}
	if fmt.Sprint(names) != "[big.bin small.go]" {
		t.Fatalf("ReadDir = %v", names)
	}
	root, err := f.ReadDir(ctx, ".")
	must(t, err)
	if len(root) != 2 || root[0].Name() != "README" || !root[1].IsDir() {
		t.Fatalf("ReadDir(.) = %v", root)
	}
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.MkdirAll(ctx, "d/sub", 0o755))
	must(t, f.WriteFile(ctx, "d/file", []byte("x"), 0o644))

	cases := []struct {
		name string
		err  error
		want error
	}{
		{"write missing parent", f.WriteFile(ctx, "nope/x", nil, 0o644), fs.ErrNotExist},
		{"write under file", f.WriteFile(ctx, "d/file/x", nil, 0o644), ErrNotDir},
		{"write over dir", f.WriteFile(ctx, "d", nil, 0o644), ErrIsDir},
		{"mkdir existing", f.Mkdir(ctx, "d", 0o755), fs.ErrExist},
		{"mkdirall through file", f.MkdirAll(ctx, "d/file/x", 0o755), ErrNotDir},
		{"remove non-empty", f.Remove(ctx, "d"), ErrNotEmpty},
		{"remove missing", f.Remove(ctx, "missing"), fs.ErrNotExist},
		{"rename into self", f.Rename(ctx, "d", "d/sub/d"), fs.ErrInvalid},
		{"rename file over dir", f.Rename(ctx, "d/file", "d/sub"), ErrIsDir},
		{"rename dir over file", f.Rename(ctx, "d/sub", "d/file"), ErrNotDir},
		{"invalid path", f.WriteFile(ctx, "/abs", nil, 0o644), fs.ErrInvalid},
		{"dotdot path", f.WriteFile(ctx, "d/../x", nil, 0o644), fs.ErrInvalid},
	}
	for _, c := range cases {
		var pe *fs.PathError
		if !errors.Is(c.err, c.want) || !errors.As(c.err, &pe) {
			t.Errorf("%s: err = %v, want *fs.PathError wrapping %v", c.name, c.err, c.want)
		}
	}
	if _, err := f.ReadFile(ctx, "d"); !errors.Is(err, ErrIsDir) {
		t.Errorf("ReadFile dir: %v", err)
	}
	if _, err := f.ReadDir(ctx, "d/file"); !errors.Is(err, ErrNotDir) {
		t.Errorf("ReadDir file: %v", err)
	}
	if err := f.MkdirAll(ctx, "d/sub", 0o755); err != nil {
		t.Errorf("MkdirAll existing: %v", err)
	}
	if err := f.RemoveAll(ctx, "missing"); err != nil {
		t.Errorf("RemoveAll missing: %v", err)
	}
	if _, err := e.s.FS("../x"); err == nil {
		t.Error("invalid namespace accepted")
	}
}

func TestRenameAndRemoveAll(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.MkdirAll(ctx, "a/b/c", 0o755))
	must(t, f.WriteFile(ctx, "a/top", []byte("1"), 0o644))
	must(t, f.WriteFile(ctx, "a/b/mid", []byte("2"), 0o644))
	must(t, f.WriteFile(ctx, "a/b/c/deep", big, 0o644))
	must(t, f.WriteFile(ctx, "ab", []byte("sibling"), 0o644)) // shares a name prefix with a
	must(t, f.Mkdir(ctx, "empty", 0o755))

	e.clock.Advance(time.Hour)
	must(t, f.Rename(ctx, "a", "empty")) // replaces an empty directory
	for p, want := range map[string]string{"empty/top": "1", "empty/b/mid": "2", "empty/b/c/deep": string(big), "ab": "sibling"} {
		got, err := f.ReadFile(ctx, p)
		if err != nil || string(got) != want {
			t.Fatalf("after rename ReadFile(%s) = %q, %v", p, got, err)
		}
	}
	if _, err := f.Stat(ctx, "a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old dir still present: %v", err)
	}
	fi, _ := f.Stat(ctx, "empty/top")
	if fi.ModTime().Equal(e.clock.Now()) {
		t.Fatal("rename changed modification time")
	}

	must(t, f.Rename(ctx, "ab", "empty/b/mid")) // file replaces file
	if got, _ := f.ReadFile(ctx, "empty/b/mid"); string(got) != "sibling" {
		t.Fatalf("file replace: %q", got)
	}

	must(t, f.RemoveAll(ctx, "empty/b"))
	tree, err := e.s.tree(ctx, "main")
	must(t, err)
	var paths []string
	for p := range tree {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	if fmt.Sprint(paths) != "[empty empty/top]" {
		t.Fatalf("after RemoveAll: %v", paths)
	}
	must(t, f.RemoveAll(ctx, "."))
	if root, _ := f.ReadDir(ctx, "."); len(root) != 0 {
		t.Fatalf("RemoveAll(.) left %v", root)
	}
}

func TestIOFS(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.MkdirAll(ctx, "dir/sub", 0o755))
	must(t, f.WriteFile(ctx, "dir/a.txt", []byte("hello"), 0o644))
	must(t, f.WriteFile(ctx, "dir/sub/b.bin", big, 0o644))
	must(t, f.WriteFile(ctx, "top.txt", nil, 0o644))

	if err := fstest.TestFS(f.IOFS(ctx), "dir/a.txt", "dir/sub/b.bin", "top.txt"); err != nil {
		t.Fatal(err)
	}
	matches, err := fs.Glob(f.IOFS(ctx), "dir/*.txt")
	if err != nil || fmt.Sprint(matches) != "[dir/a.txt]" {
		t.Fatalf("Glob = %v, %v", matches, err)
	}
}

func TestBlobRangedReads(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.WriteFile(ctx, "big", big, 0o644))

	file, err := f.Open(ctx, "big")
	must(t, err)
	defer file.Close()
	buf := make([]byte, 10)
	n, err := file.ReadAt(buf, 20)
	if err != nil || n != 10 || string(buf) != string(big[20:30]) {
		t.Fatalf("ReadAt = %d %q %v", n, buf, err)
	}
	if _, err := file.Seek(-5, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(file)
	if err != nil || string(rest) != string(big[len(big)-5:]) {
		t.Fatalf("read after seek = %q, %v", rest, err)
	}
}

func TestDedupAndPersistence(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.WriteFile(ctx, "one", big, 0o644))
	must(t, f.WriteFile(ctx, "two", big, 0o644))
	if n := e.blobCount(t); n != 1 {
		t.Fatalf("identical contents stored %d times", n)
	}
	must(t, e.db.Close())

	e.open(t)
	f = e.fs(t, "main")
	for _, p := range []string{"one", "two"} {
		if got, err := f.ReadFile(ctx, p); err != nil || !bytes.Equal(got, big) {
			t.Fatalf("after reopen ReadFile(%s): %d bytes, %v", p, len(got), err)
		}
	}
}

func TestCheckpointRestore(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	main := e.fs(t, "main")
	must(t, main.WriteFile(ctx, "keep", []byte("v1"), 0o644))
	must(t, e.s.Fork(ctx, "main", "ckpt-1"))

	must(t, main.WriteFile(ctx, "keep", []byte("v2"), 0o644))
	must(t, main.WriteFile(ctx, "new", big, 0o644))

	ckpt := e.fs(t, "ckpt-1")
	if got, _ := ckpt.ReadFile(ctx, "keep"); string(got) != "v1" {
		t.Fatalf("checkpoint changed: %q", got)
	}
	must(t, e.s.Fork(ctx, "ckpt-1", "main"))
	if got, _ := main.ReadFile(ctx, "keep"); string(got) != "v1" {
		t.Fatalf("restore: keep = %q", got)
	}
	if _, err := main.Stat(ctx, "new"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("restore kept a later file: %v", err)
	}
	names, err := e.s.Namespaces(ctx)
	must(t, err)
	if fmt.Sprint(names) != "[ckpt-1 main]" {
		t.Fatalf("Namespaces = %v", names)
	}
	must(t, e.s.DeleteNamespace(ctx, "ckpt-1"))
	if names, _ := e.s.Namespaces(ctx); fmt.Sprint(names) != "[main]" {
		t.Fatalf("after delete Namespaces = %v", names)
	}
}

func TestDiffAndApply(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	main := e.fs(t, "main")
	must(t, main.MkdirAll(ctx, "src", 0o755))
	must(t, main.WriteFile(ctx, "src/a", []byte("a"), 0o644))
	must(t, main.WriteFile(ctx, "src/b", []byte("b"), 0o644))
	must(t, main.WriteFile(ctx, "same", []byte("s"), 0o644))
	must(t, e.s.Fork(ctx, "main", "worker"))

	w := e.fs(t, "worker")
	e.clock.Advance(time.Minute)
	must(t, w.WriteFile(ctx, "src/a", []byte("A"), 0o644)) // modified
	must(t, w.Remove(ctx, "src/b"))                        // removed
	must(t, w.MkdirAll(ctx, "docs", 0o755))                // added dir
	must(t, w.WriteFile(ctx, "docs/x", big, 0o644))        // added file
	must(t, w.WriteFile(ctx, "same", []byte("s"), 0o644))  // rewritten, same content

	changes, err := e.s.Diff(ctx, "main", "worker")
	must(t, err)
	var got []string
	for _, c := range changes {
		got = append(got, fmt.Sprintf("%s:%s", c.Op, c.Path))
	}
	want := "[added:docs added:docs/x modified:src/a removed:src/b]"
	if fmt.Sprint(got) != want {
		t.Fatalf("Diff = %v, want %s", got, want)
	}

	// A subset that would orphan a file is rejected without changes.
	if err := e.s.Apply(ctx, "worker", "main", []string{"docs/x"}); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Apply orphaning subset: %v", err)
	}
	if _, err := main.Stat(ctx, "docs/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("rejected Apply changed the target")
	}

	var paths []string
	for _, c := range changes {
		paths = append(paths, c.Path)
	}
	must(t, e.s.Apply(ctx, "worker", "main", paths))
	if after, _ := e.s.Diff(ctx, "main", "worker"); len(after) != 0 {
		t.Fatalf("namespaces differ after Apply: %v", after)
	}
}

func TestCopyToAnotherDatabase(t *testing.T) {
	ctx := context.Background()
	src := newEnv(t)
	dst := newEnv(t)
	f := src.fs(t, "main")
	must(t, f.MkdirAll(ctx, "d", 0o755))
	must(t, f.WriteFile(ctx, "d/big", big, 0o644))
	must(t, f.WriteFile(ctx, "small", []byte("s"), 0o644))

	must(t, src.s.CopyTo(ctx, "main", dst.s, "forked"))
	g := dst.fs(t, "forked")
	if got, err := g.ReadFile(ctx, "d/big"); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("copied blob: %d bytes, %v", len(got), err)
	}
	if got, err := g.ReadFile(ctx, "small"); err != nil || string(got) != "s" {
		t.Fatalf("copied inline: %q, %v", got, err)
	}
	if n := dst.blobCount(t); n != 1 {
		t.Fatalf("destination has %d blobs, want 1", n)
	}
}

func TestGarbageCollection(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	other := bytes.Repeat([]byte("z"), 64)

	must(t, f.WriteFile(ctx, "keep", big, 0o644))
	must(t, f.WriteFile(ctx, "drop", other, 0o644))
	must(t, e.s.Fork(ctx, "main", "ckpt"))
	must(t, f.Remove(ctx, "drop")) // still referenced by ckpt

	run := func() GCStats {
		t.Helper()
		st, err := e.s.CollectGarbage(ctx)
		must(t, err)
		return st
	}
	e.clock.Advance(DefaultGCGrace + time.Second)
	if st := run(); st.Deleted != 0 || st.Referenced != 2 {
		t.Fatalf("with checkpoint reference: %+v", st)
	}

	must(t, e.s.DeleteNamespace(ctx, "ckpt"))
	if st := run(); st.Deleted != 0 || st.Waiting != 1 {
		t.Fatalf("first unreferenced sighting: %+v", st)
	}
	e.clock.Advance(DefaultGCGrace)
	if st := run(); st.Deleted != 1 {
		t.Fatalf("after grace: %+v", st)
	}
	if got, err := f.ReadFile(ctx, "keep"); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("referenced blob damaged: %v", err)
	}

	// Rewriting deleted content uploads it again.
	must(t, f.WriteFile(ctx, "again", other, 0o644))
	if got, err := f.ReadFile(ctx, "again"); err != nil || !bytes.Equal(got, other) {
		t.Fatalf("rewrite after gc: %v", err)
	}
}

// TestGarbageCollectionSparesUncommittedBlobs covers a blob uploaded by a
// write whose metadata is not committed yet.
func TestGarbageCollectionSparesUncommittedBlobs(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	hash := hashContent(big)
	must(t, e.s.putBlob(ctx, hash, big)) // uploaded, not yet referenced

	e.clock.Advance(DefaultGCGrace / 2)
	run := func() {
		t.Helper()
		if _, err := e.s.CollectGarbage(ctx); err != nil {
			t.Fatal(err)
		}
	}
	run()
	e.clock.Advance(DefaultGCGrace / 2)
	run() // grace since the write has passed, but not since first sighting
	if n := e.blobCount(t); n != 1 {
		t.Fatal("blob deleted within grace of its first unreferenced sighting")
	}
}

// TestGarbageCollectionAfterCrash checks that GC never deletes a blob whose
// last reference was removed only in memory: after a crash the reference
// would come back.
func TestGarbageCollectionAfterCrash(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.WriteFile(ctx, "file", big, 0o644))
	must(t, e.db.Flush(ctx))
	must(t, f.Remove(ctx, "file"))

	e.s.CollectGarbage(ctx)
	e.clock.Advance(DefaultGCGrace + time.Second)
	if st, err := e.s.CollectGarbage(ctx); err != nil || st.Deleted != 1 {
		t.Fatalf("gc: %+v %v", st, err)
	}

	// Simulate a crash: reopen without closing. The removal must have been
	// flushed by GC, so the file stays gone rather than dangling.
	e.open(t)
	f = e.fs(t, "main")
	if _, err := f.Stat(ctx, "file"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("after crash, file referencing a deleted blob is back: %v", err)
	}
}

func TestConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	f := e.fs(t, "main")
	must(t, f.Mkdir(ctx, "d", 0o755))
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := []byte(strings.Repeat(fmt.Sprint(i), testInline*(i%2)+1))
			if err := f.WriteFile(ctx, fmt.Sprintf("d/%02d", i), data, 0o644); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 5 {
			if _, err := e.s.CollectGarbage(ctx); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
	entries, err := f.ReadDir(ctx, "d")
	must(t, err)
	if len(entries) != 20 {
		t.Fatalf("%d entries, want 20", len(entries))
	}
}

// TestGarbageCollectionSparesReusedBlob covers a write that reuses an
// unreferenced blob's content just as its grace period ends: the write finds
// the blob present and does not upload it, so GC must not delete it before
// the write's metadata lands.
func TestGarbageCollectionSparesReusedBlob(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	hash := hashContent(big)
	must(t, e.s.putBlob(ctx, hash, big))
	if _, err := e.s.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(DefaultGCGrace + time.Second)

	must(t, e.s.putBlob(ctx, hash, big)) // a new write deduplicates against it
	if st, err := e.s.CollectGarbage(ctx); err != nil || st.Deleted != 0 {
		t.Fatalf("gc deleted a blob a write just reused: %+v %v", st, err)
	}
}
