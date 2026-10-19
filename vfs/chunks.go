package vfs

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"fmt"
	"hash"
	"io/fs"
	"slices"
)

// chunkRef names one chunk of a file: a content-addressed blob holding
// Size bytes whose SHA-256 is Hash.
type chunkRef struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// A file at or above the inline threshold is stored as a list of chunks, each
// a blob of exactly Options.ChunkSize bytes except the last, which holds the
// remainder (and, for a file smaller than a chunk, is the whole file).
//
// The inode's Hash is always the SHA-256 of the whole file, so Diff and
// equality are independent of how the file is chunked. To let Append extend
// that hash without reading the whole file, a chunked inode also stores
// HashState: the marshaled SHA-256 state after every chunk except the last.
// Append reads only the last chunk, restores the state, and feeds the tail
// and the new data through it. Because the state precedes the last chunk,
// the last chunk is never hashed into it, so the state stays valid however
// the last chunk grows.

// blobHashes returns the hash of every blob the entry references.
func (n *inode) blobHashes() []string {
	switch {
	case len(n.Chunks) > 0:
		out := make([]string, len(n.Chunks))
		for i, c := range n.Chunks {
			out[i] = c.Hash
		}
		return out
	case n.Blob:
		return []string{n.Hash}
	}
	return nil
}

// blobRefs returns the entry's contents as chunks: its chunk list, or the
// single blob of a legacy entry. It returns nil for inline files.
func (n *inode) blobRefs() []chunkRef {
	switch {
	case len(n.Chunks) > 0:
		return n.Chunks
	case n.Blob:
		return []chunkRef{{Hash: n.Hash, Size: n.Size}}
	}
	return nil
}

// chunker splits a stream into chunks, uploading each full chunk as it
// goes. It buffers at most one chunk, and always keeps the final chunk
// buffered until finish, because only then is it known to be the last.
type chunker struct {
	s      *Store
	ctx    context.Context
	h      hash.Hash // over every flushed chunk
	chunks []chunkRef
	buf    []byte
	total  int64 // bytes accepted, including flushed ones
}

func (s *Store) newChunker(ctx context.Context) *chunker {
	return &chunker{s: s, ctx: ctx, h: sha256.New()}
}

// resumeChunker returns a chunker that continues a chunked file whose last
// chunk has been removed from n: chunks are n.Chunks without the last one,
// and the hash state is restored from n.HashState (or, for an inode without
// one, recomputed by reading the earlier chunks).
func (s *Store) resumeChunker(ctx context.Context, n *inode) (*chunker, error) {
	c := s.newChunker(ctx)
	c.chunks = slices.Clone(n.Chunks[:len(n.Chunks)-1])
	for _, ch := range c.chunks {
		c.total += ch.Size
	}
	if u, ok := c.h.(encoding.BinaryUnmarshaler); ok && len(n.HashState) > 0 {
		if err := u.UnmarshalBinary(n.HashState); err == nil {
			return c, nil
		}
	}
	c.h = sha256.New()
	for _, ch := range c.chunks {
		data, err := s.getChunk(ctx, ch)
		if err != nil {
			return nil, err
		}
		c.h.Write(data)
	}
	return c, nil
}

func (c *chunker) write(p []byte) error {
	size := c.s.chunkSize
	for len(p) > 0 {
		if len(c.buf) == size {
			if err := c.flush(); err != nil {
				return err
			}
		}
		n := min(size-len(c.buf), len(p))
		c.buf = append(c.buf, p[:n]...)
		c.total += int64(n)
		p = p[n:]
	}
	return nil
}

// flush uploads the buffered chunk, which is not the last.
func (c *chunker) flush() error {
	ref := chunkRef{Hash: hashContent(c.buf), Size: int64(len(c.buf))}
	if err := c.s.putBlob(c.ctx, ref.Hash, c.buf); err != nil {
		return err
	}
	c.h.Write(c.buf)
	c.chunks = append(c.chunks, ref)
	c.buf = nil
	return nil
}

// finish uploads the last chunk and returns the entry. Content below the
// inline threshold stays in the entry itself.
func (c *chunker) finish(perm fs.FileMode) (*inode, error) {
	n := &inode{
		Mode:    perm & fs.ModePerm,
		Size:    c.total,
		ModTime: c.s.now().UnixNano(),
	}
	if len(c.chunks) == 0 && c.total < int64(c.s.inline) {
		n.Hash = hashContent(c.buf)
		if len(c.buf) > 0 {
			n.Data = c.buf
		}
		return n, nil
	}
	state, err := c.h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("vfs: hash state: %w", err)
	}
	ref := chunkRef{Hash: hashContent(c.buf), Size: int64(len(c.buf))}
	if err := c.s.putBlob(c.ctx, ref.Hash, c.buf); err != nil {
		return nil, err
	}
	c.h.Write(c.buf)
	n.Chunks = append(c.chunks, ref)
	n.HashState = state
	n.Hash = hex.EncodeToString(c.h.Sum(nil))
	return n, nil
}

// getBlob reads a whole blob and verifies it against its hash.
func (s *Store) getBlob(ctx context.Context, hash string) ([]byte, error) {
	data, err := s.blobs.Get(ctx, s.blobKey(hash))
	if err != nil {
		return nil, fmt.Errorf("vfs: read blob %s: %w", hash, err)
	}
	if hashContent(data) != hash {
		return nil, fmt.Errorf("vfs: blob %s does not match its hash", hash)
	}
	return data, nil
}

// getChunk reads a whole chunk and verifies it.
func (s *Store) getChunk(ctx context.Context, c chunkRef) ([]byte, error) {
	data, err := s.getBlob(ctx, c.Hash)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != c.Size {
		return nil, fmt.Errorf("vfs: blob %s has %d bytes, want %d", c.Hash, len(data), c.Size)
	}
	return data, nil
}
