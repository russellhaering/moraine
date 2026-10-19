package moraine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/russellhaering/moraine/cache"
	"github.com/russellhaering/moraine/objstore"
)

// countingStore counts reads and can fail selected writes.
type countingStore struct {
	*objstore.MemoryStore

	mu     sync.Mutex
	gets   int
	ranges int
	// failPut, if set, is consulted before each Put. Returning
	// (err, true) fails the put; applyFirst also stores the object first,
	// simulating a write that landed but whose response was lost.
	failPut func(key string) (err error, applyFirst bool)
}

func newCountingStore() *countingStore {
	return &countingStore{MemoryStore: objstore.NewMemoryStore()}
}

func (s *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets++
	s.mu.Unlock()
	return s.MemoryStore.Get(ctx, key)
}

func (s *countingStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.mu.Lock()
	s.ranges++
	s.mu.Unlock()
	return s.MemoryStore.GetRange(ctx, key, offset, length)
}

func (s *countingStore) Put(ctx context.Context, key string, data []byte, ifNoneMatch bool) error {
	s.mu.Lock()
	fail := s.failPut
	s.mu.Unlock()
	if fail != nil {
		if err, applyFirst := fail(key); err != nil {
			if applyFirst {
				_ = s.MemoryStore.Put(ctx, key, data, ifNoneMatch)
			}
			return err
		}
	}
	return s.MemoryStore.Put(ctx, key, data, ifNoneMatch)
}

func (s *countingStore) reads() (gets, ranges int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets, s.ranges
}

func (s *countingStore) resetReads() {
	s.mu.Lock()
	s.gets, s.ranges = 0, 0
	s.mu.Unlock()
}

func openTestDB(t *testing.T, store objstore.ObjectStore, cfg DBConfig) *DB {
	t.Helper()
	cfg.Store = store
	if cfg.Prefix == "" {
		cfg.Prefix = "test"
	}
	if cfg.CompactInterval == 0 {
		cfg.CompactInterval = time.Hour
	}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func keys(es []Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Key)
	}
	return out
}

func TestScanPrefixIncludesKeyEqualToPrefix(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{})
	defer db.Close()

	db.Put(ctx, "s/1/meta", []byte("m"))
	db.Put(ctx, "s/1/meta/x", []byte("x"))
	db.Put(ctx, "s/1/metb", []byte("other"))
	db.Put(ctx, "s/1/mets", []byte("other"))

	want := "[s/1/meta s/1/meta/x]"
	check := func(stage string) {
		t.Helper()
		es, err := db.ScanPrefix(ctx, "s/1/meta", 10)
		if err != nil {
			t.Fatalf("%s: ScanPrefix: %v", stage, err)
		}
		if got := fmt.Sprint(keys(es)); got != want {
			t.Fatalf("%s: ScanPrefix = %s, want %s", stage, got, want)
		}
	}
	check("memtable")
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	check("l0")
	db.Put(ctx, "z", []byte("z"))
	db.Flush(ctx)
	if err := db.compactor.compact(ctx, db.writer.CurrentManifest()); err != nil {
		t.Fatalf("compact: %v", err)
	}
	check("sorted run")
}

func TestScanPrefixLimit(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{})
	defer db.Close()

	for i := range 5 {
		db.Put(ctx, fmt.Sprintf("p/%d", i), []byte("v"))
	}
	db.Delete(ctx, "p/1")

	es, _ := db.ScanPrefix(ctx, "p/", 2)
	if got := fmt.Sprint(keys(es)); got != "[p/0 p/2]" {
		t.Fatalf("limit 2 = %s", got)
	}
	es, _ = db.ScanPrefix(ctx, "p/", 0)
	if got := fmt.Sprint(keys(es)); got != "[p/0 p/2 p/3 p/4]" {
		t.Fatalf("limit 0 = %s, want every live key", got)
	}
}

// TestSSTableReadersAreCached verifies that opening an SSTable costs at most
// two ranged reads, does not touch every data block, and is not repeated on
// later lookups.
func TestSSTableReadersAreCached(t *testing.T) {
	ctx := context.Background()
	store := newCountingStore()
	db := openTestDB(t, store, DBConfig{})
	defer db.Close()

	for i := range 5000 {
		db.Put(ctx, fmt.Sprintf("k/%06d", i), make([]byte, 100))
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	store.resetReads()
	if _, ok, err := db.Get(ctx, "k/000123"); err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	gets, ranges := store.reads()
	// Footer, index and bloom (one or two reads) plus one data block.
	if gets != 0 || ranges > 3 {
		t.Fatalf("first Get: %d full reads, %d ranged reads; want 0 and <= 3", gets, ranges)
	}

	store.resetReads()
	if _, ok, err := db.Get(ctx, "k/000124"); err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if gets, ranges := store.reads(); gets != 0 || ranges != 0 {
		t.Fatalf("second Get in same block: %d full reads, %d ranged reads; want none", gets, ranges)
	}
}

func TestSSTableReaderMetadataFromSSTInfo(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openTestDB(t, store, DBConfig{})
	defer db.Close()

	db.Put(ctx, "b", []byte("1"))
	db.Put(ctx, "d", []byte("2"))
	db.Flush(ctx)
	info := db.writer.CurrentManifest().L0[0]

	for _, size := range []int64{info.Size, 0} {
		in := info
		in.Size = size
		r, err := NewSSTableReader(ctx, store, in, nil, nil)
		if err != nil {
			t.Fatalf("size %d: NewSSTableReader: %v", size, err)
		}
		m := r.Meta()
		if m.MinKey != "b" || m.MaxKey != "d" || m.EntryCount != 2 || m.MaxSeq != info.MaxSeq {
			t.Fatalf("size %d: meta = %+v", size, m)
		}
		e, err := r.Get(ctx, "d")
		if err != nil || e == nil || string(e.Value) != "2" {
			t.Fatalf("size %d: Get(d) = %v, %v", size, e, err)
		}
	}
}

func TestDiskCacheStoresBlocks(t *testing.T) {
	ctx := context.Background()
	store := newCountingStore()
	dir := t.TempDir()
	db := openTestDB(t, store, DBConfig{DiskCacheDir: dir})
	db.Put(ctx, "k", []byte("v"))
	db.Flush(ctx)
	if _, ok, err := db.Get(ctx, "k"); err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	db.Close()

	var files int
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(path, ".blocks") {
			files++
		}
		return nil
	})
	if files == 0 {
		t.Fatal("disk cache holds no blocks after a read")
	}
}

func TestSharedBlockCache(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	shared := cache.NewLRUBlockCache(1 << 20)
	a := openTestDB(t, store, DBConfig{Prefix: "a", BlockCache: shared})
	defer a.Close()
	b := openTestDB(t, store, DBConfig{Prefix: "b", BlockCache: shared})
	defer b.Close()

	a.Put(ctx, "k", []byte("from-a"))
	a.Flush(ctx)
	b.Put(ctx, "k", []byte("from-b"))
	b.Flush(ctx)
	for db, want := range map[*DB]string{a: "from-a", b: "from-b"} {
		e, ok, err := db.Get(ctx, "k")
		if err != nil || !ok || string(e.Value) != want {
			t.Fatalf("Get = %v %v %v, want %s", e, ok, err, want)
		}
	}
	if shared.Len() < 2 {
		t.Fatalf("shared cache holds %d blocks, want blocks from both databases", shared.Len())
	}
}

// TestFlushRetriesTableLeftByFailedFlush covers a flush whose manifest save
// fails after the WAL object was written: the table must stay readable, be
// retried by the next flush, and be durable after it.
func TestFlushRetriesTableLeftByFailedFlush(t *testing.T) {
	ctx := context.Background()
	store := newCountingStore()
	db := openTestDB(t, store, DBConfig{})

	db.Put(ctx, "first", []byte("1"))
	store.failPut = func(key string) (error, bool) {
		if strings.Contains(key, "/manifest/") {
			return errors.New("injected manifest failure"), false
		}
		return nil, false
	}
	if err := db.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded despite manifest failure")
	}
	store.failPut = nil

	if _, ok, _ := db.Get(ctx, "first"); !ok {
		t.Fatal("unflushed table not readable after failed flush")
	}
	db.Put(ctx, "second", []byte("2"))
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2 := openTestDB(t, store, DBConfig{})
	defer db2.Close()
	for _, k := range []string{"first", "second"} {
		if _, ok, err := db2.Get(ctx, k); err != nil || !ok {
			t.Fatalf("after reopen Get(%s): ok=%v err=%v", k, ok, err)
		}
	}
}

// TestFlushSkipsWALIDTakenByLostWrite covers a WAL put that landed but
// reported failure: the retry must not wedge on the now-taken ID.
func TestFlushSkipsWALIDTakenByLostWrite(t *testing.T) {
	ctx := context.Background()
	store := newCountingStore()
	db := openTestDB(t, store, DBConfig{})

	db.Put(ctx, "k", []byte("v"))
	failed := false
	store.failPut = func(key string) (error, bool) {
		if strings.Contains(key, "/wal/") && !failed {
			failed = true
			return errors.New("injected timeout"), true
		}
		return nil, false
	}
	if err := db.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded despite WAL failure")
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	db.Close()

	db2 := openTestDB(t, store, DBConfig{})
	defer db2.Close()
	if _, ok, err := db2.Get(ctx, "k"); err != nil || !ok {
		t.Fatalf("after reopen: ok=%v err=%v", ok, err)
	}
}

func TestOrphanedSSTablesAreCollected(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openTestDB(t, store, DBConfig{})
	defer db.Close()

	db.Put(ctx, "k", []byte("v"))
	db.Flush(ctx)
	m := db.writer.CurrentManifest()
	live := m.L0[0].Path

	orphanWAL := fmt.Sprintf("test/wal/%020d/%020d.sst", m.WriterEpoch, uint64(0))
	orphanOldEpoch := fmt.Sprintf("test/wal/%020d/%020d.sst", m.WriterEpoch-1, uint64(7))
	orphanSorted := "test/sorted/sorted-L1-00000000000000000001.sst"
	inFlightWAL := fmt.Sprintf("test/wal/%020d/%020d.sst", m.WriterEpoch, m.WALIDNext)
	inFlightSorted := fmt.Sprintf("test/sorted/sorted-L1-%020d.sst", m.Version+1)
	unknown := "test/wal/notes.txt"
	for _, k := range []string{orphanWAL, orphanOldEpoch, orphanSorted, inFlightWAL, inFlightSorted, unknown} {
		store.Put(ctx, k, []byte("x"), false)
	}

	now := time.Unix(1000, 0)
	db.compactor.now = func() time.Time { return now }
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if exists, _ := store.Exists(ctx, orphanWAL); !exists {
		t.Fatal("orphan deleted before grace period")
	}

	now = now.Add(DefaultGCGracePeriod + time.Second)
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for k, wantExists := range map[string]bool{
		orphanWAL: false, orphanOldEpoch: false, orphanSorted: false,
		inFlightWAL: true, inFlightSorted: true, unknown: true, live: true,
	} {
		if exists, _ := store.Exists(ctx, k); exists != wantExists {
			t.Errorf("%s exists=%v, want %v", k, exists, wantExists)
		}
	}
	if _, ok, err := db.Get(ctx, "k"); err != nil || !ok {
		t.Fatalf("Get after gc: ok=%v err=%v", ok, err)
	}
}

func TestCompactorStopsAfterTakeover(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	old := openTestDB(t, store, DBConfig{L0CompactThresh: 1})
	old.Put(ctx, "k", []byte("v"))
	old.Flush(ctx)

	takeover := openTestDB(t, store, DBConfig{L0CompactThresh: 1})
	defer takeover.Close()

	if err := old.Compact(ctx); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("Compact on superseded database: %v, want ErrStaleWriter", err)
	}
	if err := takeover.Compact(ctx); err != nil {
		t.Fatalf("Compact by new owner: %v", err)
	}
	if _, ok, err := takeover.Get(ctx, "k"); err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
}
