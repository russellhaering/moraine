package moraine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

// TestWALRecoveryAfterRestart writes data, flushes, closes the DB, then reopens
// it from the same object store and verifies all data is still readable.
func TestWALRecoveryAfterRestart(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		MemTableMaxSize: 1 << 20,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write and flush data.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := fmt.Sprintf("val-%03d", i)
		if _, err := db.Put(ctx, key, []byte(val)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Close the DB.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen from the same store — this exercises manifest + L0 recovery.
	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		MemTableMaxSize: 1 << 20,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Verify all keys are readable.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%03d", i)
		expected := fmt.Sprintf("val-%03d", i)
		e, ok, err := db2.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s) after reopen: %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%s) after reopen: not found", key)
		}
		if string(e.Value) != expected {
			t.Fatalf("Get(%s) after reopen: expected %s, got %s", key, expected, string(e.Value))
		}
	}
}

// TestWALRecoveryMultipleFlushes writes data across multiple flushes, reopens,
// and verifies all data from all flushes is recovered.
func TestWALRecoveryMultipleFlushes(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		MemTableMaxSize: 1 << 20,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Three separate flush cycles.
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("batch%d-key-%03d", batch, i)
			val := fmt.Sprintf("batch%d-val-%03d", batch, i)
			db.Put(ctx, key, []byte(val))
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("Flush batch %d: %v", batch, err)
		}
	}
	db.Close()

	// Reopen.
	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		MemTableMaxSize: 1 << 20,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Verify all 30 keys.
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("batch%d-key-%03d", batch, i)
			expected := fmt.Sprintf("batch%d-val-%03d", batch, i)
			e, ok, err := db2.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get(%s): %v", key, err)
			}
			if !ok {
				t.Fatalf("Get(%s): not found after reopen", key)
			}
			if string(e.Value) != expected {
				t.Fatalf("Get(%s): expected %s, got %s", key, expected, string(e.Value))
			}
		}
	}
}

// TestWALRecoveryWithDeletesThenReopen verifies tombstones survive a restart.
func TestWALRecoveryWithDeletesThenReopen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write, flush, delete, flush.
	db.Put(ctx, "alive", []byte("yes"))
	db.Put(ctx, "doomed", []byte("yes"))
	db.Flush(ctx)

	db.Delete(ctx, "doomed")
	db.Flush(ctx)
	db.Close()

	// Reopen.
	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// "alive" should be readable.
	e, ok, err := db2.Get(ctx, "alive")
	if err != nil || !ok {
		t.Fatalf("Get(alive): err=%v ok=%v", err, ok)
	}
	if string(e.Value) != "yes" {
		t.Fatalf("expected 'yes', got %q", string(e.Value))
	}

	// "doomed" should be a tombstone: either not found, or found with nil/empty value.
	e, ok, err = db2.Get(ctx, "doomed")
	if err != nil {
		t.Fatalf("Get(doomed): %v", err)
	}
	if ok && len(e.Value) > 0 {
		t.Fatalf("expected doomed to be deleted, got %q", string(e.Value))
	}
}

// TestWALRecoveryOverwriteThenReopen verifies the latest value survives restart.
func TestWALRecoveryOverwriteThenReopen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	db.Put(ctx, "key1", []byte("v1"))
	db.Flush(ctx)

	db.Put(ctx, "key1", []byte("v2"))
	db.Flush(ctx)
	db.Close()

	// Reopen.
	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	e, ok, err := db2.Get(ctx, "key1")
	if err != nil || !ok {
		t.Fatalf("Get(key1): err=%v ok=%v", err, ok)
	}
	if string(e.Value) != "v2" {
		t.Fatalf("expected v2 after reopen, got %s", string(e.Value))
	}
}

func TestReopenAdvancesSequenceNumbersBeforeCompaction(t *testing.T) {
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

	seq, err := db.Put(ctx, "key1", []byte("v1"))
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if seq != 1 {
		t.Fatalf("Put v1 seq = %d, want 1", seq)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush v1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}

	seq, err = db2.Put(ctx, "key1", []byte("v2"))
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if seq != 2 {
		t.Fatalf("Put v2 seq = %d, want 2", seq)
	}
	if err := db2.Flush(ctx); err != nil {
		t.Fatalf("Flush v2: %v", err)
	}
	if err := db2.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	entry, ok, err := db2.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get key1 after compaction: %v", err)
	}
	if !ok || string(entry.Value) != "v2" {
		t.Fatalf("Get key1 after compaction = (%v, %v), want v2", entry, ok)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close db2: %v", err)
	}

	db3, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen after compaction: %v", err)
	}
	defer db3.Close()

	entry, ok, err = db3.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get key1 after reopen: %v", err)
	}
	if !ok || string(entry.Value) != "v2" {
		t.Fatalf("Get key1 after reopen = (%v, %v), want v2", entry, ok)
	}
}

func TestReopenDeleteTombstoneSurvivesCompaction(t *testing.T) {
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

	if _, err := db.Put(ctx, "doomed", []byte("v1")); err != nil {
		t.Fatalf("Put doomed: %v", err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush v1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}

	seq, err := db2.Delete(ctx, "doomed")
	if err != nil {
		t.Fatalf("Delete doomed: %v", err)
	}
	if seq != 2 {
		t.Fatalf("Delete seq = %d, want 2", seq)
	}
	if err := db2.Flush(ctx); err != nil {
		t.Fatalf("Flush delete: %v", err)
	}
	if err := db2.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close db2: %v", err)
	}

	db3, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		L0CompactThresh: 2,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen after compaction: %v", err)
	}
	defer db3.Close()

	entry, ok, err := db3.Get(ctx, "doomed")
	if err != nil {
		t.Fatalf("Get doomed: %v", err)
	}
	if !ok || entry.Value != nil {
		t.Fatalf("Get doomed = (%v, %v), want tombstone", entry, ok)
	}
}

// TestRecoveryAfterCompactionThenReopen verifies data survives compaction + restart.
func TestRecoveryAfterCompactionThenReopen(t *testing.T) {
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

	// Create 2 L0 SSTables, then compact.
	for i := 0; i < 50; i++ {
		db.Put(ctx, fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("val-%05d", i)))
	}
	db.Flush(ctx)

	for i := 50; i < 100; i++ {
		db.Put(ctx, fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("val-%05d", i)))
	}
	db.Flush(ctx)

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
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

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%05d", i)
		expected := fmt.Sprintf("val-%05d", i)
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

// TestScanAfterReopen verifies Scan returns correct results after reopen.
func TestScanAfterReopen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	db, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 15; i++ {
		db.Put(ctx, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("val-%03d", i)))
	}
	db.Flush(ctx)
	db.Close()

	db2, err := Open(ctx, DBConfig{
		Store:           store,
		Prefix:          "test",
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	entries, err := db2.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(entries) != 15 {
		t.Fatalf("expected 15 entries after reopen scan, got %d", len(entries))
	}
}
