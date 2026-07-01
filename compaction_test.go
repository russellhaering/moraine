package moraine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

// TestCompactionDuringActiveWrites verifies that compaction and concurrent writes
// don't cause data loss. Writes happen on one goroutine while compaction runs on another.
func TestCompactionDuringActiveWrites(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour, // manual compaction only
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Phase 1: Create enough L0 SSTables for compaction.
	for i := 0; i < 50; i++ {
		db.Put(ctx, fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("val-%05d", i)))
	}
	db.Flush(ctx)

	for i := 50; i < 100; i++ {
		db.Put(ctx, fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("val-%05d", i)))
	}
	db.Flush(ctx)

	// Phase 2: Start concurrent writes while compacting.
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Writer goroutine: continues writing keys 100-199.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			key := fmt.Sprintf("key-%05d", i)
			val := fmt.Sprintf("val-%05d", i)
			if _, err := db.Put(ctx, key, []byte(val)); err != nil {
				errCh <- fmt.Errorf("Put(%s): %w", key, err)
				return
			}
		}
		if err := db.Flush(ctx); err != nil {
			errCh <- fmt.Errorf("Flush: %w", err)
		}
	}()

	// Compaction goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := db.Compact(ctx); err != nil {
			errCh <- fmt.Errorf("Compact: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}

	// Verify all 200 keys are readable.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%05d", i)
		expected := fmt.Sprintf("val-%05d", i)
		e, ok, err := db.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%s): not found", key)
		}
		if string(e.Value) != expected {
			t.Fatalf("Get(%s): expected %s, got %s", key, expected, string(e.Value))
		}
	}
}

// TestCompactionDeduplicatesOverwrites verifies that compaction keeps only the
// latest value when a key has been overwritten across multiple L0 SSTables.
func TestCompactionDeduplicatesOverwrites(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 3,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write "shared" key in three separate flushes with different values.
	db.Put(ctx, "shared", []byte("v1"))
	db.Put(ctx, "unique-a", []byte("a"))
	db.Flush(ctx)

	db.Put(ctx, "shared", []byte("v2"))
	db.Put(ctx, "unique-b", []byte("b"))
	db.Flush(ctx)

	db.Put(ctx, "shared", []byte("v3"))
	db.Put(ctx, "unique-c", []byte("c"))
	db.Flush(ctx)

	// Compact.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The latest value should win.
	e, ok, err := db.Get(ctx, "shared")
	if err != nil || !ok {
		t.Fatalf("Get(shared): err=%v ok=%v", err, ok)
	}
	if string(e.Value) != "v3" {
		t.Fatalf("expected v3, got %s", string(e.Value))
	}

	// Unique keys should still exist.
	for _, k := range []string{"unique-a", "unique-b", "unique-c"} {
		_, ok, err := db.Get(ctx, k)
		if err != nil || !ok {
			t.Fatalf("Get(%s): err=%v ok=%v", k, err, ok)
		}
	}
}

// TestCompactionPreservesTombstones verifies that a delete followed by compaction
// still results in the key being absent.
func TestCompactionPreservesTombstones(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write and flush.
	db.Put(ctx, "keep", []byte("yes"))
	db.Put(ctx, "remove", []byte("yes"))
	db.Flush(ctx)

	// Delete and flush.
	db.Delete(ctx, "remove")
	db.Flush(ctx)

	// Compact.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// "keep" should be present.
	e, ok, err := db.Get(ctx, "keep")
	if err != nil || !ok {
		t.Fatalf("Get(keep): err=%v ok=%v", err, ok)
	}
	if string(e.Value) != "yes" {
		t.Fatalf("expected 'yes', got %s", string(e.Value))
	}

	// "remove" should be a tombstone (present but nil value).
	e, ok, err = db.Get(ctx, "remove")
	if err != nil {
		t.Fatalf("Get(remove): %v", err)
	}
	if ok && e.Value != nil {
		t.Fatalf("expected remove to be deleted, got %q", string(e.Value))
	}
}

// TestMultipleCompactionCycles verifies correctness across repeated compaction rounds.
func TestMultipleCompactionCycles(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	totalKeys := 0

	for cycle := 0; cycle < 3; cycle++ {
		// Write a batch.
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("cycle%d-key-%03d", cycle, i)
			val := fmt.Sprintf("cycle%d-val-%03d", cycle, i)
			db.Put(ctx, key, []byte(val))
			totalKeys++
		}
		db.Flush(ctx)

		// Write another batch to trigger compaction threshold.
		for i := 20; i < 40; i++ {
			key := fmt.Sprintf("cycle%d-key-%03d", cycle, i)
			val := fmt.Sprintf("cycle%d-val-%03d", cycle, i)
			db.Put(ctx, key, []byte(val))
			totalKeys++
		}
		db.Flush(ctx)

		// Compact.
		if err := db.Compact(ctx); err != nil {
			t.Fatalf("Compact cycle %d: %v", cycle, err)
		}
	}

	// Verify all keys from all cycles.
	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("cycle%d-key-%03d", cycle, i)
			expected := fmt.Sprintf("cycle%d-val-%03d", cycle, i)
			e, ok, err := db.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get(%s): %v", key, err)
			}
			if !ok {
				t.Fatalf("Get(%s): not found after %d compaction cycles", key, cycle+1)
			}
			if string(e.Value) != expected {
				t.Fatalf("Get(%s): expected %s, got %s", key, expected, string(e.Value))
			}
		}
	}

	// Also verify Scan returns all keys.
	entries, err := db.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(entries) != totalKeys {
		t.Fatalf("Scan: expected %d entries, got %d", totalKeys, len(entries))
	}
}

// TestCompactionThenReopen verifies data is correct after compaction + restart.
func TestCompactionThenReopen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write enough for compaction.
	for i := 0; i < 30; i++ {
		db.Put(ctx, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("val-%03d", i)))
	}
	db.Flush(ctx)

	for i := 30; i < 60; i++ {
		db.Put(ctx, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("val-%03d", i)))
	}
	db.Flush(ctx)

	db.Compact(ctx)

	// Write more after compaction.
	for i := 60; i < 80; i++ {
		db.Put(ctx, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("val-%03d", i)))
	}
	db.Flush(ctx)
	db.Close()

	// Reopen.
	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 80; i++ {
		key := fmt.Sprintf("key-%03d", i)
		expected := fmt.Sprintf("val-%03d", i)
		e, ok, err := db2.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%s): not found after compaction+reopen", key)
		}
		if string(e.Value) != expected {
			t.Fatalf("Get(%s): expected %s, got %s", key, expected, string(e.Value))
		}
	}
}
