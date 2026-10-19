package moraine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

// TestCompareAndSwapFlushedKeyDoesNotDeadlock exercises the CAS path that has
// to consult on-disk SSTables (key not in any MemTable).
func TestCompareAndSwapFlushedKeyDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, DBConfig{Store: objstore.NewMemoryStore(), Prefix: "test", CompactInterval: time.Hour})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Put(ctx, "k", []byte("v1"))
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ok, _, err := db.CompareAndSwap(ctx, "k", []byte("v1"), []byte("v2"))
		if err != nil || !ok {
			t.Errorf("CAS flushed key: ok=%v err=%v", ok, err)
		}
		ok, _, err = db.CompareAndSwap(ctx, "new", nil, []byte("x"))
		if err != nil || !ok {
			t.Errorf("put-if-absent: ok=%v err=%v", ok, err)
		}
		ok, _, err = db.CompareAndSwap(ctx, "new", nil, []byte("y"))
		if err != nil || ok {
			t.Errorf("second put-if-absent should fail: ok=%v err=%v", ok, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CompareAndSwap deadlocked")
	}

	e, ok, err := db.Get(ctx, "k")
	if err != nil || !ok || string(e.Value) != "v2" {
		t.Fatalf("Get(k) = %v %v %v, want v2", e, ok, err)
	}
}

// TestCompactionGCGracePeriod verifies replaced SSTables survive until the
// grace period elapses, so reads holding an older manifest keep working.
func TestCompactionGCGracePeriod(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db, err := Open(ctx, DBConfig{Store: store, Prefix: "test", L0CompactThresh: 2, CompactInterval: time.Hour})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	now := time.Unix(1000, 0)
	db.compactor.now = func() time.Time { return now }

	db.Put(ctx, "a", []byte("1"))
	db.Flush(ctx)
	db.Put(ctx, "b", []byte("2"))
	db.Flush(ctx)

	old := db.writer.CurrentManifest()
	if len(old.L0) != 2 {
		t.Fatalf("expected 2 L0 sstables, got %d", len(old.L0))
	}

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// A read using the pre-compaction manifest must still succeed.
	if _, ok, err := db.reader.Get(ctx, "a", NewMemTable(), nil, old); err != nil || !ok {
		t.Fatalf("read with old manifest after compaction: ok=%v err=%v", ok, err)
	}
	for _, sst := range old.L0 {
		if exists, _ := store.Exists(ctx, sst.Path); !exists {
			t.Fatalf("%s deleted before grace period", sst.Path)
		}
	}

	// A sweep after the grace period removes them.
	now = now.Add(DefaultGCGracePeriod + time.Second)
	db.compactor.sweep(ctx)
	for _, sst := range old.L0 {
		if exists, _ := store.Exists(ctx, sst.Path); exists {
			t.Fatalf("%s not deleted after grace period", sst.Path)
		}
	}

	// Current reads still work.
	for _, k := range []string{"a", "b"} {
		if _, ok, err := db.Get(ctx, k); err != nil || !ok {
			t.Fatalf("Get(%s): ok=%v err=%v", k, ok, err)
		}
	}
	_, _, err = db.reader.Get(ctx, "a", NewMemTable(), nil, old)
	if err == nil || !errors.Is(err, objstore.ErrNotFound) {
		t.Logf("read with stale manifest after sweep: %v", err)
	}
}
