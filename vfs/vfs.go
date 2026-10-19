// Package vfs provides a hierarchical filesystem stored in a Moraine database,
// with large file contents kept as content-addressed blobs in object storage.
//
// A Store holds any number of namespaces, each a complete file tree. Forking
// a namespace copies only metadata, so namespaces serve as checkpoints,
// isolated scratch copies, and forks; Diff and Apply merge changes between
// them. Files smaller than Options.InlineThreshold are stored in the database
// itself; larger ones are stored once per distinct content under
// Options.BlobPrefix and shared by every namespace that references them.
//
// Each FS method is atomic. Changes become durable when the database is
// flushed; blobs are uploaded before the metadata that references them is
// written. A Store assumes it is the only writer of its key prefix and blob
// prefix, as a Moraine database has a single writer.
package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	moraine "github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/objstore"
)

const (
	// DefaultInlineThreshold is the file size at and above which contents
	// are stored as blobs rather than in the database.
	DefaultInlineThreshold = 64 << 10

	// DefaultGCGrace is how long a blob must stay unreferenced, and unused by
	// any write, before CollectGarbage deletes it.
	DefaultGCGrace = 10 * time.Minute
)

var (
	// ErrNotEmpty is returned when removing or replacing a non-empty directory.
	ErrNotEmpty = errors.New("directory not empty")
	// ErrIsDir is returned when a file operation is applied to a directory.
	ErrIsDir = errors.New("is a directory")
	// ErrNotDir is returned when a path component that must be a directory
	// is a file.
	ErrNotDir = errors.New("not a directory")
)

// Options configures a Store.
type Options struct {
	// Blobs stores file contents at or above InlineThreshold. Required.
	Blobs objstore.ObjectStore
	// BlobPrefix is the object key prefix for blobs. Required; it must not
	// be shared with another Store.
	BlobPrefix string
	// KeyPrefix is the database key prefix for filesystem metadata.
	// Defaults to "vfs/".
	KeyPrefix string
	// InlineThreshold defaults to DefaultInlineThreshold.
	InlineThreshold int
	// GCGrace defaults to DefaultGCGrace.
	GCGrace time.Duration
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// Store is a set of filesystem namespaces in one database.
type Store struct {
	db         *moraine.DB
	blobs      objstore.ObjectStore
	blobPrefix string
	prefix     string
	inline     int
	grace      time.Duration
	now        func() time.Time

	// mu serializes metadata changes, which read before they write.
	mu sync.Mutex

	// blobMu guards the blob bookkeeping that keeps garbage collection from
	// deleting a blob a write is about to reference.
	blobMu     sync.Mutex
	firstUnref map[string]time.Time // blob hash -> when GC first found it unreferenced
}

// Open returns a Store over db.
func Open(db *moraine.DB, opts Options) (*Store, error) {
	if db == nil || opts.Blobs == nil {
		return nil, errors.New("vfs: database and blob store are required")
	}
	if opts.BlobPrefix == "" {
		return nil, errors.New("vfs: blob prefix is required")
	}
	s := &Store{
		db:         db,
		blobs:      opts.Blobs,
		blobPrefix: strings.TrimSuffix(opts.BlobPrefix, "/") + "/",
		prefix:     opts.KeyPrefix,
		inline:     opts.InlineThreshold,
		grace:      opts.GCGrace,
		now:        opts.Now,
		firstUnref: map[string]time.Time{},
	}
	if s.prefix == "" {
		s.prefix = "vfs/"
	}
	if s.inline <= 0 {
		s.inline = DefaultInlineThreshold
	}
	if s.grace <= 0 {
		s.grace = DefaultGCGrace
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// ---------- Keys and entries ----------

// Metadata keys:
//
//	{prefix}ns/{namespace}                  namespace registry
//	{prefix}e/{namespace}/{parent}\x00{name} one entry per file or directory
//
// Keying entries by parent directory makes a directory's children a single
// prefix scan, sorted by name. The root directory "." has no entry.

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func validNamespace(name string) error {
	if !namespacePattern.MatchString(name) {
		return fmt.Errorf("vfs: invalid namespace %q", name)
	}
	return nil
}

func (s *Store) registryKey(ns string) string { return s.prefix + "ns/" + ns }
func (s *Store) nsPrefix(ns string) string    { return s.prefix + "e/" + ns + "/" }

func (s *Store) entryKey(ns, p string) string {
	parent, base := split(p)
	return s.nsPrefix(ns) + parent + "\x00" + base
}

// childPrefix is the key prefix of dir's direct children.
func (s *Store) childPrefix(ns, dir string) string {
	return s.nsPrefix(ns) + dir + "\x00"
}

// keyPath recovers a path from an entry key under nsPrefix.
func keyPath(nsPrefix, key string) string {
	parent, base, _ := strings.Cut(key[len(nsPrefix):], "\x00")
	if parent == "." {
		return base
	}
	return parent + "/" + base
}

// split returns the parent directory and final element of a non-root path.
func split(p string) (parent, base string) {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return ".", p
	}
	return p[:i], p[i+1:]
}

// cleanPath validates an io/fs-style path.
func cleanPath(op, name string) (string, error) {
	if !fs.ValidPath(name) || strings.ContainsRune(name, 0) {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return name, nil
}

// inode is the stored metadata for a file or directory.
type inode struct {
	Mode    fs.FileMode `json:"mode"`
	Size    int64       `json:"size,omitempty"`
	ModTime int64       `json:"mtime,omitempty"` // Unix nanoseconds
	Hash    string      `json:"hash,omitempty"`  // SHA-256 of file contents
	Data    []byte      `json:"data,omitempty"`  // inline contents
	Blob    bool        `json:"blob,omitempty"`  // contents are the blob named Hash
}

func (n *inode) isDir() bool { return n.Mode.IsDir() }

// sameContent reports whether two entries describe the same file or
// directory, ignoring modification times.
func (n *inode) sameContent(o *inode) bool {
	return n.Mode == o.Mode && n.Hash == o.Hash
}

var rootInode = &inode{Mode: fs.ModeDir | 0o755}

func decodeInode(v []byte) (*inode, error) {
	var n inode
	if err := json.Unmarshal(v, &n); err != nil {
		return nil, fmt.Errorf("vfs: decode entry: %w", err)
	}
	return &n, nil
}

func encodeInode(n *inode) []byte {
	b, err := json.Marshal(n)
	if err != nil {
		panic(fmt.Sprintf("vfs: encode entry: %v", err)) // inode always marshals
	}
	return b
}

// get returns the entry for p, or nil if it does not exist.
func (s *Store) get(ctx context.Context, ns, p string) (*inode, error) {
	if p == "." {
		return rootInode, nil
	}
	e, ok, err := s.db.Get(ctx, s.entryKey(ns, p))
	if err != nil || !ok || e.Value == nil {
		return nil, err
	}
	return decodeInode(e.Value)
}

// tree returns every entry in a namespace, keyed by path.
func (s *Store) tree(ctx context.Context, ns string) (map[string]*inode, error) {
	prefix := s.nsPrefix(ns)
	entries, err := s.db.ScanPrefix(ctx, prefix, 0)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*inode, len(entries))
	for _, e := range entries {
		n, err := decodeInode(e.Value)
		if err != nil {
			return nil, err
		}
		out[keyPath(prefix, e.Key)] = n
	}
	return out, nil
}

// subtreeKeys returns the keys of every entry strictly below dir.
func (s *Store) subtreeKeys(ctx context.Context, ns, dir string) ([]moraine.Entry, error) {
	if dir == "." {
		return s.db.ScanPrefix(ctx, s.nsPrefix(ns), 0)
	}
	// Direct children are keyed "{dir}\x00name"; deeper entries "{dir}/...".
	children, err := s.db.ScanPrefix(ctx, s.childPrefix(ns, dir), 0)
	if err != nil {
		return nil, err
	}
	deeper, err := s.db.ScanPrefix(ctx, s.nsPrefix(ns)+dir+"/", 0)
	if err != nil {
		return nil, err
	}
	return append(children, deeper...), nil
}

// register records ns in the namespace registry if it is not already there.
// Callers hold s.mu.
func (s *Store) register(ctx context.Context, ns string) error {
	e, ok, err := s.db.Get(ctx, s.registryKey(ns))
	if err != nil {
		return err
	}
	if ok && e.Value != nil {
		return nil
	}
	_, err = s.db.Put(ctx, s.registryKey(ns), []byte{1})
	return err
}

// ---------- Blobs ----------

func hashContent(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Store) blobKey(hash string) string { return s.blobPrefix + hash }

// putBlob uploads data under its hash unless it is already present. It first
// restarts the blob's garbage-collection grace period, so the blob cannot be
// deleted between the existence check and the metadata write that
// references it.
func (s *Store) putBlob(ctx context.Context, hash string, data []byte) error {
	s.blobMu.Lock()
	delete(s.firstUnref, hash)
	s.blobMu.Unlock()

	err := s.blobs.Put(ctx, s.blobKey(hash), data, true)
	if errors.Is(err, objstore.ErrPreconditionFailed) {
		return nil // already stored; content addressing makes it identical
	}
	if err != nil {
		return fmt.Errorf("vfs: store blob %s: %w", hash, err)
	}
	return nil
}

// contents returns a file's full contents.
func (s *Store) contents(ctx context.Context, n *inode) ([]byte, error) {
	if !n.Blob {
		return n.Data, nil
	}
	data, err := s.blobs.Get(ctx, s.blobKey(n.Hash))
	if err != nil {
		return nil, fmt.Errorf("vfs: read blob %s: %w", n.Hash, err)
	}
	if hashContent(data) != n.Hash {
		return nil, fmt.Errorf("vfs: blob %s does not match its hash", n.Hash)
	}
	return data, nil
}

// newFileInode builds the entry for data, uploading it as a blob if it is
// large enough.
func (s *Store) newFileInode(ctx context.Context, data []byte, perm fs.FileMode) (*inode, error) {
	n := &inode{
		Mode:    perm & fs.ModePerm,
		Size:    int64(len(data)),
		ModTime: s.now().UnixNano(),
		Hash:    hashContent(data),
	}
	if len(data) >= s.inline {
		if err := s.putBlob(ctx, n.Hash, data); err != nil {
			return nil, err
		}
		n.Blob = true
	} else if len(data) > 0 {
		n.Data = slices.Clone(data)
	}
	return n, nil
}

// ---------- Namespaces ----------

// FS returns the filesystem for a namespace. Namespace names start with a
// letter or digit and contain only letters, digits, '.', '_' and '-'. A
// namespace with no entries is an empty filesystem.
func (s *Store) FS(name string) (*FS, error) {
	if err := validNamespace(name); err != nil {
		return nil, err
	}
	return &FS{s: s, ns: name}, nil
}

// Namespaces lists the namespaces that have been written to, in order.
func (s *Store) Namespaces(ctx context.Context) ([]string, error) {
	prefix := s.prefix + "ns/"
	entries, err := s.db.ScanPrefix(ctx, prefix, 0)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Key[len(prefix):]
	}
	return names, nil
}

// Fork makes dst an exact copy of src, replacing whatever dst held. Only
// metadata is copied; blobs are shared.
func (s *Store) Fork(ctx context.Context, src, dst string) error {
	if err := validNamespace(src); err != nil {
		return err
	}
	if err := validNamespace(dst); err != nil {
		return err
	}
	if src == dst {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replaceLocked(ctx, src, dst)
}

// replaceLocked rewrites dst to match src in one batch.
func (s *Store) replaceLocked(ctx context.Context, src, dst string) error {
	srcEntries, err := s.db.ScanPrefix(ctx, s.nsPrefix(src), 0)
	if err != nil {
		return err
	}
	dstEntries, err := s.db.ScanPrefix(ctx, s.nsPrefix(dst), 0)
	if err != nil {
		return err
	}
	srcPrefix, dstPrefix := s.nsPrefix(src), s.nsPrefix(dst)
	keep := make(map[string]struct{}, len(srcEntries))
	ops := make([]moraine.BatchOp, 0, len(srcEntries)+len(dstEntries)+1)
	for _, e := range srcEntries {
		key := dstPrefix + e.Key[len(srcPrefix):]
		keep[key] = struct{}{}
		ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: key, Value: e.Value})
	}
	for _, e := range dstEntries {
		if _, ok := keep[e.Key]; !ok {
			ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: e.Key})
		}
	}
	ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: s.registryKey(dst), Value: []byte{1}})
	_, err = s.db.WriteBatch(ctx, ops)
	return err
}

// DeleteNamespace removes a namespace and all its entries. Blobs it alone
// referenced are reclaimed by CollectGarbage.
func (s *Store) DeleteNamespace(ctx context.Context, name string) error {
	if err := validNamespace(name); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.db.ScanPrefix(ctx, s.nsPrefix(name), 0)
	if err != nil {
		return err
	}
	ops := make([]moraine.BatchOp, 0, len(entries)+1)
	for _, e := range entries {
		ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: e.Key})
	}
	ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: s.registryKey(name)})
	_, err = s.db.WriteBatch(ctx, ops)
	return err
}

// ChangeOp is the kind of a Change.
type ChangeOp int

const (
	Added ChangeOp = iota + 1
	Modified
	Removed
)

func (op ChangeOp) String() string {
	switch op {
	case Added:
		return "added"
	case Modified:
		return "modified"
	case Removed:
		return "removed"
	}
	return fmt.Sprintf("ChangeOp(%d)", int(op))
}

// Change describes one path that differs between two namespaces.
type Change struct {
	Path string
	Op   ChangeOp
	// IsDir reports whether the path is a directory in the namespace it
	// exists in (other for Added and Modified, base for Removed).
	IsDir bool
}

// Diff lists the paths whose entries differ between base and other, sorted
// by path. Contents are compared by hash and modification times are ignored,
// so rewriting a file with identical contents is not a change.
func (s *Store) Diff(ctx context.Context, base, other string) ([]Change, error) {
	if err := validNamespace(base); err != nil {
		return nil, err
	}
	if err := validNamespace(other); err != nil {
		return nil, err
	}
	a, err := s.tree(ctx, base)
	if err != nil {
		return nil, err
	}
	b, err := s.tree(ctx, other)
	if err != nil {
		return nil, err
	}
	var changes []Change
	for p, bn := range b {
		an, ok := a[p]
		switch {
		case !ok:
			changes = append(changes, Change{Path: p, Op: Added, IsDir: bn.isDir()})
		case !an.sameContent(bn):
			changes = append(changes, Change{Path: p, Op: Modified, IsDir: bn.isDir()})
		}
	}
	for p, an := range a {
		if _, ok := b[p]; !ok {
			changes = append(changes, Change{Path: p, Op: Removed, IsDir: an.isDir()})
		}
	}
	slices.SortFunc(changes, func(x, y Change) int { return strings.Compare(x.Path, y.Path) })
	return changes, nil
}

// Apply makes each listed path in to match from: copied if it exists in
// from, removed if it does not. All paths change in one batch, and Apply
// fails without changing anything if the result would leave an entry whose
// parent is missing or is not a directory. Pass paths from Diff(to, from),
// or a subset closed under parents and children, to merge work back.
func (s *Store) Apply(ctx context.Context, from, to string, paths []string) error {
	if err := validNamespace(from); err != nil {
		return err
	}
	if err := validNamespace(to); err != nil {
		return err
	}
	for _, p := range paths {
		if _, err := cleanPath("apply", p); err != nil {
			return err
		}
		if p == "." {
			return &fs.PathError{Op: "apply", Path: p, Err: fs.ErrInvalid}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.tree(ctx, to)
	if err != nil {
		return err
	}
	var ops []moraine.BatchOp
	for _, p := range paths {
		e, ok, err := s.db.Get(ctx, s.entryKey(from, p))
		if err != nil {
			return err
		}
		key := s.entryKey(to, p)
		if ok && e.Value != nil {
			n, err := decodeInode(e.Value)
			if err != nil {
				return err
			}
			result[p] = n
			ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: key, Value: e.Value})
		} else {
			delete(result, p)
			ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: key})
		}
	}
	for p := range result {
		parent, _ := split(p)
		if parent == "." {
			continue
		}
		if pn, ok := result[parent]; !ok || !pn.isDir() {
			return &fs.PathError{Op: "apply", Path: p, Err: fmt.Errorf("parent %q would not be a directory: %w", parent, fs.ErrInvalid)}
		}
	}
	if len(ops) == 0 {
		return nil
	}
	ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: s.registryKey(to), Value: []byte{1}})
	_, err = s.db.WriteBatch(ctx, ops)
	return err
}

// CopyTo makes namespace dstName in dst an exact copy of namespace src,
// copying any blobs dst does not already have. Use it to fork a filesystem
// into another database.
func (s *Store) CopyTo(ctx context.Context, src string, dst *Store, dstName string) error {
	if err := validNamespace(src); err != nil {
		return err
	}
	if err := validNamespace(dstName); err != nil {
		return err
	}
	if dst == s {
		return s.Fork(ctx, src, dstName)
	}
	srcPrefix := s.nsPrefix(src)
	entries, err := s.db.ScanPrefix(ctx, srcPrefix, 0)
	if err != nil {
		return err
	}
	copied := map[string]bool{}
	for _, e := range entries {
		n, err := decodeInode(e.Value)
		if err != nil {
			return err
		}
		if !n.Blob || copied[n.Hash] {
			continue
		}
		data, err := s.contents(ctx, n)
		if err != nil {
			return err
		}
		if err := dst.putBlob(ctx, n.Hash, data); err != nil {
			return err
		}
		copied[n.Hash] = true
	}

	dst.mu.Lock()
	defer dst.mu.Unlock()
	dstPrefix := dst.nsPrefix(dstName)
	existing, err := dst.db.ScanPrefix(ctx, dstPrefix, 0)
	if err != nil {
		return err
	}
	keep := make(map[string]struct{}, len(entries))
	ops := make([]moraine.BatchOp, 0, len(entries)+len(existing)+1)
	for _, e := range entries {
		key := dstPrefix + e.Key[len(srcPrefix):]
		keep[key] = struct{}{}
		ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: key, Value: e.Value})
	}
	for _, e := range existing {
		if _, ok := keep[e.Key]; !ok {
			ops = append(ops, moraine.BatchOp{Type: moraine.BatchDelete, Key: e.Key})
		}
	}
	ops = append(ops, moraine.BatchOp{Type: moraine.BatchPut, Key: dst.registryKey(dstName), Value: []byte{1}})
	_, err = dst.db.WriteBatch(ctx, ops)
	return err
}
