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
func (s *Store) CollectGarbage(ctx context.Context) (GCStats, error) {
	var stats GCStats
	keys, err := s.blobs.List(ctx, s.blobPrefix)
	if err != nil {
		return stats, fmt.Errorf("vfs: list blobs: %w", err)
	}
	stats.Blobs = len(keys)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Flush(ctx); err != nil {
		return stats, fmt.Errorf("vfs: flush before gc: %w", err)
	}
	referenced, err := s.referencedBlobs(ctx)
	if err != nil {
		return stats, err
	}

	s.blobMu.Lock()
	defer s.blobMu.Unlock()
	now := s.now()
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
		if err := s.blobs.Delete(ctx, key); err != nil && !errors.Is(err, objstore.ErrNotFound) {
			stats.DeleteErrors++
			continue
		}
		delete(s.firstUnref, hash)
		stats.Deleted++
	}
	for hash := range s.firstUnref {
		if _, ok := listed[hash]; !ok {
			delete(s.firstUnref, hash)
		}
	}
	return stats, nil
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
