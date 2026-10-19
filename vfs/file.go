package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"time"
)

// fileInfo implements fs.FileInfo for an entry.
type fileInfo struct {
	name string
	n    *inode
}

func newFileInfo(p string, n *inode) fileInfo {
	return fileInfo{name: path.Base(p), n: n}
}

func (fi fileInfo) Name() string       { return fi.name }
func (fi fileInfo) Size() int64        { return fi.n.Size }
func (fi fileInfo) Mode() fs.FileMode  { return fi.n.Mode }
func (fi fileInfo) ModTime() time.Time { return time.Unix(0, fi.n.ModTime) }
func (fi fileInfo) IsDir() bool        { return fi.n.isDir() }
func (fi fileInfo) Sys() any           { return nil }

// File is an open file or directory. Blob-backed files are read with ranged
// requests, so ReadAt and Seek do not fetch the whole blob.
type File struct {
	fsys *FS
	ctx  context.Context
	path string
	n    *inode
	off  int64

	dirEntries []fs.DirEntry // loaded on the first ReadDir
	dirLoaded  bool
}

var (
	_ fs.ReadDirFile = (*File)(nil)
	_ io.ReaderAt    = (*File)(nil)
	_ io.Seeker      = (*File)(nil)
)

// Stat returns information about the file.
func (f *File) Stat() (fs.FileInfo, error) {
	return newFileInfo(f.path, f.n), nil
}

// Close releases the file. It never fails.
func (f *File) Close() error { return nil }

// Read reads from the current offset.
func (f *File) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.off)
	f.off += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes starting at off.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if f.n.isDir() {
		return 0, &fs.PathError{Op: "read", Path: f.path, Err: ErrIsDir}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: f.path, Err: fs.ErrInvalid}
	}
	if off >= f.n.Size {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	want := min(int64(len(p)), f.n.Size-off)
	var n int
	if f.n.Blob {
		data, err := f.fsys.s.blobs.GetRange(f.ctx, f.fsys.s.blobKey(f.n.Hash), off, want)
		if err != nil {
			return 0, &fs.PathError{Op: "read", Path: f.path, Err: fmt.Errorf("vfs: read blob %s: %w", f.n.Hash, err)}
		}
		n = copy(p, data)
		if int64(n) < want {
			return n, &fs.PathError{Op: "read", Path: f.path, Err: io.ErrUnexpectedEOF}
		}
	} else {
		n = copy(p, f.n.Data[off:off+want])
	}
	if int64(n) < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}

// Seek sets the offset for the next Read.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.off + offset
	case io.SeekEnd:
		abs = f.n.Size + offset
	default:
		return 0, &fs.PathError{Op: "seek", Path: f.path, Err: fs.ErrInvalid}
	}
	if abs < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.path, Err: fs.ErrInvalid}
	}
	f.off = abs
	return abs, nil
}

// ReadDir reads directory entries as fs.ReadDirFile specifies: with n > 0 at
// most n entries per call and io.EOF at the end; with n <= 0 all remaining
// entries.
func (f *File) ReadDir(n int) ([]fs.DirEntry, error) {
	if !f.n.isDir() {
		return nil, &fs.PathError{Op: "readdir", Path: f.path, Err: ErrNotDir}
	}
	if !f.dirLoaded {
		entries, err := f.fsys.ReadDir(f.ctx, f.path)
		if err != nil {
			var pe *fs.PathError
			if errors.As(err, &pe) {
				return nil, pe
			}
			return nil, err
		}
		f.dirEntries, f.dirLoaded = entries, true
	}
	if n <= 0 {
		out := f.dirEntries
		f.dirEntries = nil
		return out, nil
	}
	if len(f.dirEntries) == 0 {
		return nil, io.EOF
	}
	n = min(n, len(f.dirEntries))
	out := f.dirEntries[:n]
	f.dirEntries = f.dirEntries[n:]
	return out, nil
}
