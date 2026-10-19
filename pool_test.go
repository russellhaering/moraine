package moraine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPoolCompactsWhenFlushReachesThreshold(t *testing.T) {
	ctx := context.Background()
	pool := NewCompactionPool(2)
	defer pool.Close()
	db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{CompactionPool: pool, L0CompactThresh: 3})
	defer db.Close()

	for i := range 3 {
		db.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"))
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	waitFor(t, "compaction", func() bool {
		m := db.writer.CurrentManifest()
		return len(m.L0) == 0 && len(m.SortedRuns) == 1
	})
	for i := range 3 {
		if _, ok, err := db.Get(ctx, fmt.Sprintf("k%d", i)); err != nil || !ok {
			t.Fatalf("Get(k%d): ok=%v err=%v", i, ok, err)
		}
	}
}

// TestPoolDoesNotPollIdleDatabases checks that a database with nothing to do
// costs no object-store requests once its first run is done.
func TestPoolDoesNotPollIdleDatabases(t *testing.T) {
	ctx := context.Background()
	pool := NewCompactionPool(1)
	defer pool.Close()
	store := newCountingStore()
	db := openTestDB(t, store, DBConfig{CompactionPool: pool, CompactInterval: time.Millisecond})
	defer db.Close()

	db.Put(ctx, "k", []byte("v"))
	db.Flush(ctx)
	waitFor(t, "first run", pool.idle)

	store.resetReads()
	time.Sleep(100 * time.Millisecond)
	store.mu.Lock()
	gets, ranges, lists := store.gets, store.ranges, store.lists
	store.mu.Unlock()
	if gets+ranges+lists != 0 {
		t.Fatalf("idle database made %d gets, %d ranged gets, %d lists", gets, ranges, lists)
	}
}

func TestPoolSweepsWhenDeletesComeDue(t *testing.T) {
	ctx := context.Background()
	pool := NewCompactionPool(1)
	defer pool.Close()
	store := objstore.NewMemoryStore()
	db := openTestDB(t, store, DBConfig{CompactionPool: pool, L0CompactThresh: 2})
	defer db.Close()
	waitFor(t, "first run", pool.idle)
	db.compactor.gcMu.Lock()
	db.compactor.gcGrace = 20 * time.Millisecond
	db.compactor.gcMu.Unlock()

	db.Put(ctx, "a", []byte("1"))
	db.Flush(ctx)
	replaced := db.writer.CurrentManifest().L0[0].Path
	db.Put(ctx, "b", []byte("2"))
	db.Flush(ctx)

	waitFor(t, "replaced sstable deletion", func() bool {
		exists, _ := store.Exists(ctx, replaced)
		return !exists
	})
}

func TestPoolDropsDatabaseAfterTakeover(t *testing.T) {
	pool := NewCompactionPool(1)
	defer pool.Close()
	store := objstore.NewMemoryStore()
	old := openTestDB(t, store, DBConfig{CompactionPool: pool, L0CompactThresh: 1})
	waitFor(t, "first run", pool.idle)

	takeover := openTestDB(t, store, DBConfig{})
	defer takeover.Close()

	pool.wake(old.compactor)
	waitFor(t, "superseded database to leave the pool", func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		_, ok := pool.entries[old.compactor]
		return !ok
	})
	if err := old.Close(); err != nil && !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("Close: %v", err)
	}
}

func TestPoolCloseUnregisters(t *testing.T) {
	pool := NewCompactionPool(1)
	defer pool.Close()
	db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{CompactionPool: pool})
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	pool.mu.Lock()
	n := len(pool.entries)
	pool.mu.Unlock()
	if n != 0 {
		t.Fatalf("pool still holds %d entries after Close", n)
	}
}

func TestPoolManyDatabases(t *testing.T) {
	ctx := context.Background()
	pool := NewCompactionPool(2)
	defer pool.Close()
	store := objstore.NewMemoryStore()

	const n = 20
	dbs := make([]*DB, n)
	for i := range dbs {
		dbs[i] = openTestDB(t, store, DBConfig{Prefix: fmt.Sprintf("agent-%d", i), CompactionPool: pool, L0CompactThresh: 2})
	}
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 6 {
				db.Put(ctx, fmt.Sprintf("k%d", j), []byte(fmt.Sprint(i)))
				if err := db.Flush(ctx); err != nil {
					t.Errorf("db %d Flush: %v", i, err)
				}
			}
		}()
	}
	wg.Wait()
	waitFor(t, "all databases compacted", func() bool {
		for _, db := range dbs {
			if len(db.writer.CurrentManifest().L0) >= 2 {
				return false
			}
		}
		return pool.idle()
	})
	for i, db := range dbs {
		for j := range 6 {
			e, ok, err := db.Get(ctx, fmt.Sprintf("k%d", j))
			if err != nil || !ok || string(e.Value) != fmt.Sprint(i) {
				t.Fatalf("db %d Get(k%d) = %v %v %v", i, j, e, ok, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("db %d Close: %v", i, err)
		}
	}
}

func TestCompactOnClose(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openTestDB(t, store, DBConfig{CompactOnClose: true})
	db.Put(ctx, "a", []byte("1"))
	db.Flush(ctx)
	db.Put(ctx, "b", []byte("2"))
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := NewManifestStore(store, "test").LoadLatest(ctx)
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	if len(m.L0) != 0 || len(m.SortedRuns) != 1 {
		t.Fatalf("after Close: %d L0, %d sorted runs; want 0 and 1", len(m.L0), len(m.SortedRuns))
	}
	db2 := openTestDB(t, store, DBConfig{})
	defer db2.Close()
	for _, k := range []string{"a", "b"} {
		if _, ok, err := db2.Get(ctx, k); err != nil || !ok {
			t.Fatalf("Get(%s): ok=%v err=%v", k, ok, err)
		}
	}
}

// TestOverlappingCompactionsDoNotLoseData starts a compaction, lets the
// writer flush more data, and starts a second compaction while the first is
// stalled writing its output. If the second could finish first, the first
// would then commit a sorted run built from its older snapshot and drop the
// newer data.
func TestOverlappingCompactionsDoNotLoseData(t *testing.T) {
	ctx := context.Background()
	store := newCountingStore()
	db := openTestDB(t, store, DBConfig{L0CompactThresh: 1})
	defer db.Close()

	db.Put(ctx, "x", []byte("1"))
	db.Flush(ctx)

	stalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.mu.Lock()
	store.failPut = func(key string) (error, bool) {
		if strings.Contains(key, "/sorted/") {
			once.Do(func() {
				close(stalled)
				<-release
			})
		}
		return nil, false
	}
	store.mu.Unlock()

	first := make(chan error, 1)
	go func() { first <- db.Compact(ctx) }()
	<-stalled

	db.Put(ctx, "y", []byte("2"))
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	second := make(chan error, 1)
	go func() { second <- db.Compact(ctx) }()
	select {
	case err := <-second:
		t.Logf("second compaction finished while the first was stalled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first compaction: %v", err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second compaction: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second compaction did not finish")
	}

	for _, k := range []string{"x", "y"} {
		if _, ok, err := db.Get(ctx, k); err != nil || !ok {
			t.Fatalf("Get(%s) after overlapping compactions: ok=%v err=%v", k, ok, err)
		}
	}
}
