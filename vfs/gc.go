package vfs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/russellhaering/moraine/objstore"
)

// GCStats summarizes a CollectGarbage run.
type GCStats struct {
	Blobs        int // blobs found under the blob prefix
	Referenced   int // blobs referenced by some namespace
	Deleted      int // blobs deleted
	Waiting      int // unreferenced blobs still inside the grace period
	DeleteErrors int // deletes that failed and will be retried next run
}

// CollectGarbage deletes blobs that no namespace references. A blob is
// deleted only once it has been seen unreferenced, and has not been used by
// a write, for the full grace period, so content written but not yet
// committed survives. Garbage collection tracks that history in memory, so a
// blob orphaned before a restart is deleted one grace period after the first
// run in the new process.
//
// CollectGarbage flushes the database first, so a deletion is never based on
// a removed reference that a crash could bring back.
//
// Writers are blocked only briefly. The metadata lock is held just to flush
// and mark; deletion then runs without it, taking the blob lock one blob at
// a time, so a write waits for at most one object-store delete.
//
// Why a blob a write references after the mark cannot be deleted: a blob is
// a deletion candidate only if firstUnref holds a sighting at least one grace
// period old, and the delete phase re-checks that under blobMu immediately
// before each delete. Every path that makes a namespace reference blob content
// passes it through putBlob or holdBlob, which clear the sighting under blobMu
// before the metadata commit: WriteFile, Create and Append (via the chunker),
// and CopyTo (which holds each blob in the destination store). So a write
// whose commit follows the mark has cleared the sighting first; the
// re-check then sees none and skips the blob, and a sighting made after the
// clear is new and inside the grace period. The remaining paths add no
// reference to content that was not already referenced: Fork, Apply,
// ApplyIfUnchanged and Rename copy entries that exist in a namespace when they
// run, and an entry's blobs are either referenced at the mark (not candidates)
// or were referenced by a write after the mark, which cleared the sighting as
// above. DeleteNamespace and Remove only drop references. A delete itself
// runs under blobMu, so a writer that wants the same blob waits for it and
// then finds the blob absent and uploads it again.
func (s *Store) CollectGarbage(ctx context.Context) (GCStats, error) {
	var stats GCStats
	keys, err := s.blobs.List(ctx, s.blobPrefix)
	if err != nil {
		return stats, fmt.Errorf("vfs: list blobs: %w", err)
	}
	stats.Blobs = len(keys)

	candidates, err := s.markGarbage(ctx, keys, &stats)
	if err != nil {
		return stats, err
	}
	for _, key := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		s.sweepBlob(ctx, key, &stats)
	}
	return stats, nil
}

// markGarbage flushes the database, finds the referenced blobs and returns
// the keys of the blobs that have been unreferenced for the full grace
// period. It holds s.mu, so no write commits while it runs.
func (s *Store) markGarbage(ctx context.Context, keys []string, stats *GCStats) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Flush(ctx); err != nil {
		return nil, fmt.Errorf("vfs: flush before gc: %w", err)
	}
	referenced, err := s.referencedBlobs(ctx)
	if err != nil {
		return nil, err
	}

	s.blobMu.Lock()
	defer s.blobMu.Unlock()
	now := s.now()
	var candidates []string
	listed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		hash := strings.TrimPrefix(key, s.blobPrefix)
		listed[hash] = struct{}{}
		if _, ok := referenced[hash]; ok {
			stats.Referenced++
			delete(s.firstUnref, hash)
			continue
		}
		// putBlob clears a blob's sighting, so the grace period runs from
		// the later of the first unreferenced sighting and the last write
		// that used the blob.
		since, seen := s.firstUnref[hash]
		if !seen {
			s.firstUnref[hash] = now
			since = now
		}
		if now.Sub(since) < s.grace {
			stats.Waiting++
			continue
		}
		candidates = append(candidates, key)
	}
	for hash := range s.firstUnref {
		if _, ok := listed[hash]; !ok {
			delete(s.firstUnref, hash)
		}
	}
	return candidates, nil
}

// sweepBlob deletes one candidate if it is still eligible. The check and the
// delete happen under blobMu, so a concurrent putBlob either clears the
// sighting first (the blob is skipped) or waits for the delete.
func (s *Store) sweepBlob(ctx context.Context, key string, stats *GCStats) {
	hash := strings.TrimPrefix(key, s.blobPrefix)
	s.blobMu.Lock()
	defer s.blobMu.Unlock()
	since, seen := s.firstUnref[hash]
	if !seen || s.now().Sub(since) < s.grace {
		stats.Waiting++ // a write used the blob since the mark
		return
	}
	if err := s.blobs.Delete(ctx, key); err != nil && !errors.Is(err, objstore.ErrNotFound) {
		stats.DeleteErrors++
		return
	}
	delete(s.firstUnref, hash)
	stats.Deleted++
}

// referencedBlobs returns the hashes of every blob any namespace references.
// Callers hold s.mu.
func (s *Store) referencedBlobs(ctx context.Context) (map[string]struct{}, error) {
	entries, err := s.db.ScanPrefix(ctx, s.prefix+"e/", 0)
	if err != nil {
		return nil, err
	}
	refs := map[string]struct{}{}
	for _, e := range entries {
		n, err := decodeInode(e.Value)
		if err != nil {
			return nil, err
		}
		for _, h := range n.blobHashes() {
			refs[h] = struct{}{}
		}
	}
	return refs, nil
}
