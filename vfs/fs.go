package vfs

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"strings"

	moraine "github.com/russellhaering/moraine"
)

// FS is one namespace of a Store. Paths follow io/fs conventions: unrooted,
// slash-separated, with "." naming the root directory.
type FS struct {
	s  *Store
	ns string
}

// Namespace returns the namespace name.
func (f *FS) Namespace() string { return f.ns }

// Stat returns information about the named file or directory.
func (f *FS) Stat(ctx context.Context, name string) (fs.FileInfo, error) {
	name, err := cleanPath("stat", name)
	if err != nil {
		return nil, err
	}
	n, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	if n == nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return newFileInfo(name, n), nil
}

// ReadFile returns the contents of the named file.
func (f *FS) ReadFile(ctx context.Context, name string) ([]byte, error) {
	name, err := cleanPath("read", name)
	if err != nil {
		return nil, err
	}
	n, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: err}
	}
	if n == nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	if n.isDir() {
		return nil, &fs.PathError{Op: "read", Path: name, Err: ErrIsDir}
	}
	data, err := f.s.contents(ctx, n)
	if err != nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: err}
	}
	return data, nil
}

// ReadDir returns the entries of the named directory, sorted by name.
func (f *FS) ReadDir(ctx context.Context, name string) ([]fs.DirEntry, error) {
	name, err := cleanPath("readdir", name)
	if err != nil {
		return nil, err
	}
	n, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	if n == nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	if !n.isDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: ErrNotDir}
	}
	prefix := f.s.childPrefix(f.ns, name)
	children, err := f.s.db.ScanPrefix(ctx, prefix, 0)
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	out := make([]fs.DirEntry, 0, len(children))
	for _, e := range children {
		cn, err := decodeInode(e.Value)
		if err != nil {
			return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
		}
		out = append(out, fs.FileInfoToDirEntry(newFileInfo(e.Key[len(prefix):], cn)))
	}
	return out, nil
}

// Open opens the named file or directory for reading.
func (f *FS) Open(ctx context.Context, name string) (*File, error) {
	name, err := cleanPath("open", name)
	if err != nil {
		return nil, err
	}
	n, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	if n == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &File{fsys: f, ctx: ctx, path: name, n: n}, nil
}

// requireDir checks that p exists and is a directory. Callers hold s.mu.
func (f *FS) requireDir(ctx context.Context, op, p string) error {
	n, err := f.s.get(ctx, f.ns, p)
	if err != nil {
		return &fs.PathError{Op: op, Path: p, Err: err}
	}
	if n == nil {
		return &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
	if !n.isDir() {
		return &fs.PathError{Op: op, Path: p, Err: ErrNotDir}
	}
	return nil
}

// WriteFile writes data to the named file, creating it with perm if
// necessary. Its parent directory must exist.
func (f *FS) WriteFile(ctx context.Context, name string, data []byte, perm fs.FileMode) error {
	name, err := cleanPath("write", name)
	if err != nil {
		return err
	}
	if name == "." {
		return &fs.PathError{Op: "write", Path: name, Err: ErrIsDir}
	}
	parent, _ := split(name)

	// Fail fast before uploading, then check again under the lock.
	if err := f.checkWritable(ctx, "write", name, parent); err != nil {
		return err
	}
	n, err := f.s.newFileInode(ctx, data, perm)
	if err != nil {
		return &fs.PathError{Op: "write", Path: name, Err: err}
	}

	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if err := f.checkWritable(ctx, "write", name, parent); err != nil {
		return err
	}
	return f.commit(ctx, "write", name, []moraine.BatchOp{
		{Type: moraine.BatchPut, Key: f.s.entryKey(f.ns, name), Value: encodeInode(n)},
	})
}

// Create returns a writer that replaces the named file, creating it with
// perm if necessary. Its parent directory must exist. The writer buffers at
// most one chunk, uploading each full chunk as it fills, so files of any size
// can be written in constant memory. Nothing is visible until Close commits
// the file, atomically replacing any previous contents; a writer that is
// abandoned without Close leaves only unreferenced blobs for CollectGarbage.
// A Writer is not safe for concurrent use.
func (f *FS) Create(ctx context.Context, name string, perm fs.FileMode) (io.WriteCloser, error) {
	name, err := cleanPath("write", name)
	if err != nil {
		return nil, err
	}
	if name == "." {
		return nil, &fs.PathError{Op: "write", Path: name, Err: ErrIsDir}
	}
	parent, _ := split(name)
	if err := f.checkWritable(ctx, "write", name, parent); err != nil {
		return nil, err
	}
	return &fileWriter{f: f, ctx: ctx, name: name, parent: parent, perm: perm, c: f.s.newChunker(ctx)}, nil
}

type fileWriter struct {
	f      *FS
	ctx    context.Context
	name   string
	parent string
	perm   fs.FileMode
	c      *chunker
	err    error // first write error; the file cannot be committed
	closed bool
}

func (w *fileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, &fs.PathError{Op: "write", Path: w.name, Err: fs.ErrClosed}
	}
	if w.err != nil {
		return 0, w.err
	}
	if err := w.c.write(p); err != nil {
		w.err = &fs.PathError{Op: "write", Path: w.name, Err: err}
		return 0, w.err
	}
	return len(p), nil
}

// Close uploads the final chunk and commits the file.
func (w *fileWriter) Close() error {
	if w.closed {
		return &fs.PathError{Op: "write", Path: w.name, Err: fs.ErrClosed}
	}
	w.closed = true
	if w.err != nil {
		return w.err
	}
	n, err := w.c.finish(w.perm)
	if err != nil {
		return &fs.PathError{Op: "write", Path: w.name, Err: err}
	}
	w.f.s.mu.Lock()
	defer w.f.s.mu.Unlock()
	if err := w.f.checkWritable(w.ctx, "write", w.name, w.parent); err != nil {
		return err
	}
	return w.f.commit(w.ctx, "write", w.name, []moraine.BatchOp{
		{Type: moraine.BatchPut, Key: w.f.s.entryKey(w.f.ns, w.name), Value: encodeInode(n)},
	})
}

// Append adds data to the end of the named file, creating it with mode 0644
// if it does not exist. Its parent directory must exist. Appending to a
// chunked file re-uploads only its last chunk, extended by data, and any new
// chunks; the file's other chunks are untouched and never read. An inline
// file that grows past the inline threshold becomes chunked. (A file stored
// as a single legacy blob is read once and rewritten as chunks.) Concurrent
// appends are serialized by retrying on conflict, so none is lost.
func (f *FS) Append(ctx context.Context, name string, data []byte) error {
	name, err := cleanPath("append", name)
	if err != nil {
		return err
	}
	if name == "." {
		return &fs.PathError{Op: "append", Path: name, Err: ErrIsDir}
	}
	parent, _ := split(name)
	key := f.s.entryKey(f.ns, name)
	for {
		if err := f.checkWritable(ctx, "append", name, parent); err != nil {
			return err
		}
		// Upload outside the lock against the entry as it is now, then
		// commit only if the entry is still byte-for-byte the same.
		old, err := f.rawEntry(ctx, key)
		if err != nil {
			return &fs.PathError{Op: "append", Path: name, Err: err}
		}
		var cur *inode
		if old != nil {
			if cur, err = decodeInode(old); err != nil {
				return &fs.PathError{Op: "append", Path: name, Err: err}
			}
		}
		if cur != nil && len(data) == 0 {
			return nil
		}
		n, err := f.s.appendInode(ctx, cur, data)
		if err != nil {
			return &fs.PathError{Op: "append", Path: name, Err: err}
		}

		f.s.mu.Lock()
		now, err := f.rawEntry(ctx, key)
		if err == nil && bytes.Equal(now, old) {
			if err = f.checkWritable(ctx, "append", name, parent); err == nil {
				err = f.commit(ctx, "append", name, []moraine.BatchOp{
					{Type: moraine.BatchPut, Key: key, Value: encodeInode(n)},
				})
			}
			f.s.mu.Unlock()
			return err
		}
		f.s.mu.Unlock()
		if err != nil {
			return &fs.PathError{Op: "append", Path: name, Err: err}
		}
	}
}

// rawEntry returns the stored bytes of an entry, or nil if absent.
func (f *FS) rawEntry(ctx context.Context, key string) ([]byte, error) {
	e, ok, err := f.s.db.Get(ctx, key)
	if err != nil || !ok {
		return nil, err
	}
	return e.Value, nil
}

func (f *FS) checkWritable(ctx context.Context, op, name, parent string) error {
	if err := f.requireDir(ctx, op, parent); err != nil {
		return err
	}
	existing, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return &fs.PathError{Op: op, Path: name, Err: err}
	}
	if existing != nil && existing.isDir() {
		return &fs.PathError{Op: op, Path: name, Err: ErrIsDir}
	}
	return nil
}

// commit registers the namespace and applies ops in one batch. Callers hold
// s.mu.
func (f *FS) commit(ctx context.Context, op, name string, ops []moraine.BatchOp) error {
	ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: f.s.registryKey(f.ns), Value: []byte{1}})
	if _, err := f.s.db.WriteBatch(ctx, ops); err != nil {
		return &fs.PathError{Op: op, Path: name, Err: err}
	}
	return nil
}

func (f *FS) dirInode(perm fs.FileMode) *inode {
	return &inode{Mode: fs.ModeDir | perm&fs.ModePerm, ModTime: f.s.now().UnixNano()}
}

// Mkdir creates a directory. Its parent must exist and the name must not.
func (f *FS) Mkdir(ctx context.Context, name string, perm fs.FileMode) error {
	name, err := cleanPath("mkdir", name)
	if err != nil {
		return err
	}
	if name == "." {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrExist}
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	parent, _ := split(name)
	if err := f.requireDir(ctx, "mkdir", parent); err != nil {
		return err
	}
	existing, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return &fs.PathError{Op: "mkdir", Path: name, Err: err}
	}
	if existing != nil {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrExist}
	}
	return f.commit(ctx, "mkdir", name, []moraine.BatchOp{
		{Type: moraine.BatchPut, Key: f.s.entryKey(f.ns, name), Value: encodeInode(f.dirInode(perm))},
	})
}

// MkdirAll creates a directory and any missing parents. It succeeds if the
// directory already exists.
func (f *FS) MkdirAll(ctx context.Context, name string, perm fs.FileMode) error {
	name, err := cleanPath("mkdir", name)
	if err != nil {
		return err
	}
	if name == "." {
		return nil
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var ops []moraine.BatchOp
	parts := strings.Split(name, "/")
	for i := range parts {
		p := strings.Join(parts[:i+1], "/")
		n, err := f.s.get(ctx, f.ns, p)
		if err != nil {
			return &fs.PathError{Op: "mkdir", Path: p, Err: err}
		}
		if n == nil {
			ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: f.s.entryKey(f.ns, p), Value: encodeInode(f.dirInode(perm))})
			continue
		}
		if len(ops) == 0 && !n.isDir() {
			return &fs.PathError{Op: "mkdir", Path: p, Err: ErrNotDir}
		}
	}
	if len(ops) == 0 {
		return nil
	}
	return f.commit(ctx, "mkdir", name, ops)
}

// Remove removes a file or an empty directory.
func (f *FS) Remove(ctx context.Context, name string) error {
	name, err := cleanPath("remove", name)
	if err != nil {
		return err
	}
	if name == "." {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	n, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}
	if n == nil {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	if n.isDir() {
		children, err := f.s.db.ScanPrefix(ctx, f.s.childPrefix(f.ns, name), 1)
		if err != nil {
			return &fs.PathError{Op: "remove", Path: name, Err: err}
		}
		if len(children) > 0 {
			return &fs.PathError{Op: "remove", Path: name, Err: ErrNotEmpty}
		}
	}
	return f.commit(ctx, "remove", name, []moraine.BatchOp{
		{Type: moraine.BatchDelete, Key: f.s.entryKey(f.ns, name)},
	})
}

// RemoveAll removes name and everything below it. It succeeds if name does
// not exist. RemoveAll(".") empties the filesystem.
func (f *FS) RemoveAll(ctx context.Context, name string) error {
	name, err := cleanPath("removeall", name)
	if err != nil {
		return err
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	n, err := f.s.get(ctx, f.ns, name)
	if err != nil {
		return &fs.PathError{Op: "removeall", Path: name, Err: err}
	}
	if n == nil {
		return nil
	}
	var ops []moraine.BatchOp
	if n.isDir() {
		below, err := f.s.subtreeKeys(ctx, f.ns, name)
		if err != nil {
			return &fs.PathError{Op: "removeall", Path: name, Err: err}
		}
		for _, e := range below {
			ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: e.Key})
		}
	}
	if name != "." {
		ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: f.s.entryKey(f.ns, name)})
	}
	if len(ops) == 0 {
		return nil
	}
	return f.commit(ctx, "removeall", name, ops)
}

// Rename moves oldname to newname, along with everything below it if it is
// a directory. A file may replace a file; a directory may replace an empty
// directory. Modification times are preserved.
func (f *FS) Rename(ctx context.Context, oldname, newname string) error {
	oldname, err := cleanPath("rename", oldname)
	if err != nil {
		return err
	}
	newname, err = cleanPath("rename", newname)
	if err != nil {
		return err
	}
	if oldname == "." || newname == "." {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrInvalid}
	}
	if oldname == newname {
		return nil
	}

	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	src, err := f.s.get(ctx, f.ns, oldname)
	if err != nil {
		return &fs.PathError{Op: "rename", Path: oldname, Err: err}
	}
	if src == nil {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrNotExist}
	}
	if src.isDir() && strings.HasPrefix(newname, oldname+"/") {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrInvalid}
	}
	newParent, _ := split(newname)
	if err := f.requireDir(ctx, "rename", newParent); err != nil {
		return err
	}
	dst, err := f.s.get(ctx, f.ns, newname)
	if err != nil {
		return &fs.PathError{Op: "rename", Path: newname, Err: err}
	}
	if dst != nil {
		switch {
		case dst.isDir() && !src.isDir():
			return &fs.PathError{Op: "rename", Path: newname, Err: ErrIsDir}
		case !dst.isDir() && src.isDir():
			return &fs.PathError{Op: "rename", Path: newname, Err: ErrNotDir}
		case dst.isDir():
			children, err := f.s.db.ScanPrefix(ctx, f.s.childPrefix(f.ns, newname), 1)
			if err != nil {
				return &fs.PathError{Op: "rename", Path: newname, Err: err}
			}
			if len(children) > 0 {
				return &fs.PathError{Op: "rename", Path: newname, Err: ErrNotEmpty}
			}
		}
	}

	ops := []moraine.BatchOp{
		{Type: moraine.BatchDelete, Key: f.s.entryKey(f.ns, oldname)},
		{Type: moraine.BatchPut, Key: f.s.entryKey(f.ns, newname), Value: encodeInode(src)},
	}
	if src.isDir() {
		prefix := f.s.nsPrefix(f.ns)
		below, err := f.s.subtreeKeys(ctx, f.ns, oldname)
		if err != nil {
			return &fs.PathError{Op: "rename", Path: oldname, Err: err}
		}
		for _, e := range below {
			p := keyPath(prefix, e.Key)
			moved := newname + p[len(oldname):]
			ops = append(ops,
				moraine.BatchOp{Type: moraine.BatchDelete, Key: e.Key},
				moraine.BatchOp{Type: moraine.BatchPut, Key: f.s.entryKey(f.ns, moved), Value: e.Value},
			)
		}
	}
	return f.commit(ctx, "rename", oldname, ops)
}

// IOFS returns an fs.FS view of f whose operations use ctx. It implements
// fs.ReadFileFS, fs.ReadDirFS and fs.StatFS, so fs.WalkDir, fs.Glob and
// similar helpers work on it.
func (f *FS) IOFS(ctx context.Context) fs.FS {
	return ioFS{f: f, ctx: ctx}
}

type ioFS struct {
	f   *FS
	ctx context.Context
}

func (i ioFS) Open(name string) (fs.File, error)          { return i.f.Open(i.ctx, name) }
func (i ioFS) ReadFile(name string) ([]byte, error)       { return i.f.ReadFile(i.ctx, name) }
func (i ioFS) ReadDir(name string) ([]fs.DirEntry, error) { return i.f.ReadDir(i.ctx, name) }
func (i ioFS) Stat(name string) (fs.FileInfo, error)      { return i.f.Stat(i.ctx, name) }
