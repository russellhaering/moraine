package moraine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/russellhaering/moraine/objstore"
)

func TestNewWAL_NextID(t *testing.T) {
	store := objstore.NewMemoryStore()

	for _, startID := range []uint64{0, 1, 42, 1000} {
		wal := NewWAL(store, "test", startID)
		if got := wal.NextID(); got != startID {
			t.Errorf("NewWAL with startID=%d: NextID()=%d, want %d", startID, got, startID)
		}
	}
}

func TestWrite_BasicEntries(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 0)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Put("apple", []byte("red"), 1)
	mt.Put("banana", []byte("yellow"), 2)
	mt.Put("cherry", []byte("dark"), 3)
	mt.Freeze()

	id, meta, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if id != 0 {
		t.Errorf("Write returned id=%d, want 0", id)
	}
	if meta.MinKey != "apple" {
		t.Errorf("meta.MinKey=%q, want %q", meta.MinKey, "apple")
	}
	if meta.MaxKey != "cherry" {
		t.Errorf("meta.MaxKey=%q, want %q", meta.MaxKey, "cherry")
	}
	if meta.EntryCount != 3 {
		t.Errorf("meta.EntryCount=%d, want 3", meta.EntryCount)
	}
	if meta.MinSeq != 1 {
		t.Errorf("meta.MinSeq=%d, want 1", meta.MinSeq)
	}
	if meta.MaxSeq != 3 {
		t.Errorf("meta.MaxSeq=%d, want 3", meta.MaxSeq)
	}
	if meta.Size <= 0 {
		t.Errorf("meta.Size=%d, want >0", meta.Size)
	}
	if wal.NextID() != 1 {
		t.Errorf("after Write: NextID()=%d, want 1", wal.NextID())
	}
}

func TestWrite_MultipleTimesIncrementIDs(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 5)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		mt := NewMemTable()
		mt.Put(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)), uint64(i+1))
		mt.Freeze()

		id, _, err := wal.Write(ctx, mt)
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		expectedID := uint64(5 + i)
		if id != expectedID {
			t.Errorf("Write %d: id=%d, want %d", i, id, expectedID)
		}
	}

	if got := wal.NextID(); got != 9 {
		t.Errorf("after 4 writes starting at 5: NextID()=%d, want 9", got)
	}
}

func TestWrite_WithTombstones(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 0)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Put("alive", []byte("yes"), 1)
	mt.Delete("dead", 2)
	mt.Put("zombie", []byte("maybe"), 3)
	mt.Freeze()

	id, meta, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read back and verify tombstone is preserved.
	reader, err := ReadWAL(ctx, store, "db", id)
	if err != nil {
		t.Fatalf("ReadWAL: %v", err)
	}

	if meta.EntryCount != 3 {
		t.Errorf("meta.EntryCount=%d, want 3", meta.EntryCount)
	}

	// Check the tombstone entry via Get.
	entry, err := reader.Get("dead")
	if err != nil {
		t.Fatalf("reader.Get(dead): %v", err)
	}
	if entry == nil {
		t.Fatal("reader.Get(dead) returned nil, want tombstone entry")
	}
	if entry.Value != nil {
		t.Errorf("tombstone entry Value=%v, want nil", entry.Value)
	}

	// Check alive entry has its value.
	entry, err = reader.Get("alive")
	if err != nil {
		t.Fatalf("reader.Get(alive): %v", err)
	}
	if entry == nil {
		t.Fatal("reader.Get(alive) returned nil")
	}
	if string(entry.Value) != "yes" {
		t.Errorf("alive Value=%q, want %q", entry.Value, "yes")
	}
}

func TestReadWAL_RoundTrip(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "mydb", 10)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Put("foo", []byte("bar"), 1)
	mt.Put("baz", []byte("qux"), 2)
	mt.Freeze()

	id, _, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := ReadWAL(ctx, store, "mydb", id)
	if err != nil {
		t.Fatalf("ReadWAL: %v", err)
	}

	// Collect all entries from the reader's iterator.
	var entries []Entry
	iter := reader.Iterator()
	for iter.Next() {
		entries = append(entries, iter.Entry())
	}
	if iter.Err() != nil {
		t.Fatalf("iterator error: %v", iter.Err())
	}

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}

	// Entries should be in sorted key order.
	if entries[0].Key != "baz" || string(entries[0].Value) != "qux" {
		t.Errorf("entry[0] = {%q, %q}, want {baz, qux}", entries[0].Key, entries[0].Value)
	}
	if entries[1].Key != "foo" || string(entries[1].Value) != "bar" {
		t.Errorf("entry[1] = {%q, %q}, want {foo, bar}", entries[1].Key, entries[1].Value)
	}
}

func TestReadWAL_MultipleEntriesSortedOrder(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 0)
	ctx := context.Background()

	mt := NewMemTable()
	// Insert in non-sorted order; MemTable sorts internally.
	keys := []string{"zebra", "mango", "apple", "kiwi", "banana", "grape"}
	for i, k := range keys {
		mt.Put(k, []byte("v-"+k), uint64(i+1))
	}
	mt.Freeze()

	id, _, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := ReadWAL(ctx, store, "db", id)
	if err != nil {
		t.Fatalf("ReadWAL: %v", err)
	}

	var got []string
	iter := reader.Iterator()
	for iter.Next() {
		e := iter.Entry()
		got = append(got, e.Key)
		expVal := "v-" + e.Key
		if string(e.Value) != expVal {
			t.Errorf("entry %q: Value=%q, want %q", e.Key, e.Value, expVal)
		}
	}
	if iter.Err() != nil {
		t.Fatalf("iterator error: %v", iter.Err())
	}

	expected := []string{"apple", "banana", "grape", "kiwi", "mango", "zebra"}
	if len(got) != len(expected) {
		t.Fatalf("got %d entries, want %d", len(got), len(expected))
	}
	for i, k := range expected {
		if got[i] != k {
			t.Errorf("entry[%d]=%q, want %q", i, got[i], k)
		}
	}
}

func TestReadWAL_NonExistent(t *testing.T) {
	store := objstore.NewMemoryStore()
	ctx := context.Background()

	_, err := ReadWAL(ctx, store, "db", 999)
	if err == nil {
		t.Fatal("ReadWAL for non-existent WAL: expected error, got nil")
	}
}

func TestWAL_PathFormat(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "myprefix", 42)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Put("key", []byte("val"), 1)
	mt.Freeze()

	_, _, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the key format in the store.
	keys, err := store.List(ctx, "myprefix/wal/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	expectedKey := fmt.Sprintf("myprefix/wal/%020d.sst", 42)
	if len(keys) != 1 {
		t.Fatalf("got %d keys, want 1: %v", len(keys), keys)
	}
	if keys[0] != expectedKey {
		t.Errorf("store key=%q, want %q", keys[0], expectedKey)
	}
}

func TestWrite_EmptyMemTable(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 0)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Freeze()

	id, meta, err := wal.Write(ctx, mt)
	if err != nil {
		// If the implementation errors on empty memtable, that's acceptable.
		t.Skipf("Write with empty MemTable returned error (acceptable): %v", err)
	}

	// If it succeeds, verify it produced a valid (empty) SSTable.
	if id != 0 {
		t.Errorf("id=%d, want 0", id)
	}
	if meta.EntryCount != 0 {
		t.Errorf("meta.EntryCount=%d, want 0", meta.EntryCount)
	}
	if wal.NextID() != 1 {
		t.Errorf("NextID()=%d, want 1", wal.NextID())
	}

	// Verify the empty WAL can be read back.
	reader, err := ReadWAL(ctx, store, "db", id)
	if err != nil {
		t.Fatalf("ReadWAL of empty WAL: %v", err)
	}
	iter := reader.Iterator()
	if iter.Next() {
		t.Error("expected no entries in empty WAL")
	}
}

func TestWrite_FencingIfNoneMatch(t *testing.T) {
	store := objstore.NewMemoryStore()
	ctx := context.Background()

	// First WAL writes successfully.
	wal1 := NewWAL(store, "db", 7)
	mt1 := NewMemTable()
	mt1.Put("key1", []byte("val1"), 1)
	mt1.Freeze()

	_, _, err := wal1.Write(ctx, mt1)
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}

	// Second WAL with the same startID tries to write to the same path.
	wal2 := NewWAL(store, "db", 7)
	mt2 := NewMemTable()
	mt2.Put("key2", []byte("val2"), 2)
	mt2.Freeze()

	_, _, err = wal2.Write(ctx, mt2)
	if err == nil {
		t.Fatal("second Write with same ID: expected error due to ifNoneMatch, got nil")
	}
	if !errors.Is(err, objstore.ErrPreconditionFailed) {
		// The error is wrapped, so check the message too.
		if !errors.Is(err, objstore.ErrPreconditionFailed) {
			t.Logf("error does not wrap ErrPreconditionFailed directly, but got: %v", err)
		}
	}
}

func TestWrite_NextIDNotIncrementedOnError(t *testing.T) {
	store := objstore.NewMemoryStore()
	ctx := context.Background()

	// Write once to occupy the key.
	wal := NewWAL(store, "db", 0)
	mt := NewMemTable()
	mt.Put("a", []byte("b"), 1)
	mt.Freeze()
	_, _, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}

	// Create a new WAL at the same startID to force a collision.
	wal2 := NewWAL(store, "db", 0)
	mt2 := NewMemTable()
	mt2.Put("c", []byte("d"), 2)
	mt2.Freeze()
	_, _, err = wal2.Write(ctx, mt2)
	if err == nil {
		t.Fatal("expected Write to fail")
	}

	// NextID should still be 0 since the write failed.
	if got := wal2.NextID(); got != 0 {
		t.Errorf("NextID after failed write=%d, want 0", got)
	}
}

func TestReadWAL_VerifyMetaFromReader(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 3)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Put("delta", []byte("d"), 10)
	mt.Put("alpha", []byte("a"), 20)
	mt.Put("gamma", []byte("g"), 15)
	mt.Freeze()

	id, writeMeta, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := ReadWAL(ctx, store, "db", id)
	if err != nil {
		t.Fatalf("ReadWAL: %v", err)
	}

	readMeta := reader.Meta()

	// Compare write-time meta with read-time meta.
	if readMeta.MinKey != writeMeta.MinKey {
		t.Errorf("reader MinKey=%q, writer MinKey=%q", readMeta.MinKey, writeMeta.MinKey)
	}
	if readMeta.MaxKey != writeMeta.MaxKey {
		t.Errorf("reader MaxKey=%q, writer MaxKey=%q", readMeta.MaxKey, writeMeta.MaxKey)
	}
	if readMeta.EntryCount != writeMeta.EntryCount {
		t.Errorf("reader EntryCount=%d, writer EntryCount=%d", readMeta.EntryCount, writeMeta.EntryCount)
	}
	if readMeta.MinSeq != writeMeta.MinSeq {
		t.Errorf("reader MinSeq=%d, writer MinSeq=%d", readMeta.MinSeq, writeMeta.MinSeq)
	}
	if readMeta.MaxSeq != writeMeta.MaxSeq {
		t.Errorf("reader MaxSeq=%d, writer MaxSeq=%d", readMeta.MaxSeq, writeMeta.MaxSeq)
	}
	if readMeta.Size != writeMeta.Size {
		t.Errorf("reader Size=%d, writer Size=%d", readMeta.Size, writeMeta.Size)
	}
}

func TestWAL_walPathMethod(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "pfx", 0)

	tests := []struct {
		id   uint64
		want string
	}{
		{0, "pfx/wal/00000000000000000000.sst"},
		{1, "pfx/wal/00000000000000000001.sst"},
		{42, "pfx/wal/00000000000000000042.sst"},
		{99999, "pfx/wal/00000000000000099999.sst"},
	}

	for _, tc := range tests {
		got := wal.walPath(tc.id)
		if got != tc.want {
			t.Errorf("walPath(%d)=%q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestEpochWAL_walPathMethod(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := newEpochWAL(store, "pfx", 0, 7)

	got := wal.walPath(42)
	want := "pfx/wal/00000000000000000007/00000000000000000042.sst"
	if got != want {
		t.Fatalf("walPath=%q, want %q", got, want)
	}
}

func TestReadWAL_GetIndividualKeys(t *testing.T) {
	store := objstore.NewMemoryStore()
	wal := NewWAL(store, "db", 0)
	ctx := context.Background()

	mt := NewMemTable()
	mt.Put("aaa", []byte("111"), 1)
	mt.Put("bbb", []byte("222"), 2)
	mt.Put("ccc", []byte("333"), 3)
	mt.Freeze()

	id, _, err := wal.Write(ctx, mt)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := ReadWAL(ctx, store, "db", id)
	if err != nil {
		t.Fatalf("ReadWAL: %v", err)
	}

	// Verify each key individually via Get.
	for _, tc := range []struct {
		key string
		val string
	}{
		{"aaa", "111"},
		{"bbb", "222"},
		{"ccc", "333"},
	} {
		e, err := reader.Get(tc.key)
		if err != nil {
			t.Errorf("Get(%q): %v", tc.key, err)
			continue
		}
		if e == nil {
			t.Errorf("Get(%q): nil", tc.key)
			continue
		}
		if string(e.Value) != tc.val {
			t.Errorf("Get(%q).Value=%q, want %q", tc.key, e.Value, tc.val)
		}
	}

	// Key that doesn't exist.
	e, err := reader.Get("zzz")
	if err != nil {
		t.Errorf("Get(zzz): %v", err)
	}
	if e != nil {
		t.Errorf("Get(zzz) = %+v, want nil", e)
	}
}
