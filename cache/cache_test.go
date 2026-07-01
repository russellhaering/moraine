package cache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// LRUBlockCache tests
// ---------------------------------------------------------------------------

func TestLRU_PutGet_BasicRoundTrip(t *testing.T) {
	c := NewLRUBlockCache(1024)
	key := BlockKey{SSTID: "sst-1", BlockOffset: 0}
	data := []byte("hello world")

	c.Put(key, data)

	got := c.Get(key)
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}
}

func TestLRU_GetMissReturnsNil(t *testing.T) {
	c := NewLRUBlockCache(1024)
	key := BlockKey{SSTID: "nonexistent", BlockOffset: 42}

	got := c.Get(key)
	if got != nil {
		t.Fatalf("Get on missing key returned %v, want nil", got)
	}
}

func TestLRU_GetReturnsCopy(t *testing.T) {
	c := NewLRUBlockCache(1024)
	key := BlockKey{SSTID: "sst-1", BlockOffset: 0}
	original := []byte("immutable")

	c.Put(key, original)

	got := c.Get(key)
	// Mutate the returned slice.
	for i := range got {
		got[i] = 'X'
	}

	// A second Get must still return the original data.
	got2 := c.Get(key)
	if !bytes.Equal(got2, original) {
		t.Fatalf("cache was mutated through returned slice: got %q, want %q", got2, original)
	}
}

func TestLRU_PutUpdatesExistingKey(t *testing.T) {
	c := NewLRUBlockCache(1024)
	key := BlockKey{SSTID: "sst-1", BlockOffset: 0}

	c.Put(key, []byte("short"))
	if c.Size() != 5 {
		t.Fatalf("size after first put: got %d, want 5", c.Size())
	}

	newData := []byte("much longer payload")
	c.Put(key, newData)

	got := c.Get(key)
	if !bytes.Equal(got, newData) {
		t.Fatalf("Get after update: got %q, want %q", got, newData)
	}
	if c.Len() != 1 {
		t.Fatalf("Len after update: got %d, want 1", c.Len())
	}
	if c.Size() != int64(len(newData)) {
		t.Fatalf("Size after update: got %d, want %d", c.Size(), len(newData))
	}
}

func TestLRU_Eviction(t *testing.T) {
	// Each value is 10 bytes; allow at most 25 bytes → room for 2 items.
	c := NewLRUBlockCache(25)

	k1 := BlockKey{SSTID: "sst", BlockOffset: 1}
	k2 := BlockKey{SSTID: "sst", BlockOffset: 2}
	k3 := BlockKey{SSTID: "sst", BlockOffset: 3}

	c.Put(k1, make([]byte, 10)) // 10 bytes
	c.Put(k2, make([]byte, 10)) // 20 bytes
	c.Put(k3, make([]byte, 10)) // 30 > 25 → evict k1

	if c.Get(k1) != nil {
		t.Fatal("k1 should have been evicted")
	}
	if c.Get(k2) == nil {
		t.Fatal("k2 should still be present")
	}
	if c.Get(k3) == nil {
		t.Fatal("k3 should still be present")
	}
}

func TestLRU_LRUOrdering(t *testing.T) {
	// 25 bytes max, 10-byte values → holds 2.
	c := NewLRUBlockCache(25)

	k1 := BlockKey{SSTID: "sst", BlockOffset: 1}
	k2 := BlockKey{SSTID: "sst", BlockOffset: 2}
	k3 := BlockKey{SSTID: "sst", BlockOffset: 3}

	c.Put(k1, make([]byte, 10))
	c.Put(k2, make([]byte, 10))

	// Access k1 so it becomes most-recently-used.
	c.Get(k1)

	// Insert k3 → should evict k2 (the least recently used), not k1.
	c.Put(k3, make([]byte, 10))

	if c.Get(k1) == nil {
		t.Fatal("k1 was accessed recently and should NOT have been evicted")
	}
	if c.Get(k2) != nil {
		t.Fatal("k2 should have been evicted as LRU")
	}
	if c.Get(k3) == nil {
		t.Fatal("k3 should be present")
	}
}

func TestLRU_LenAndSize(t *testing.T) {
	c := NewLRUBlockCache(100)

	if c.Len() != 0 || c.Size() != 0 {
		t.Fatalf("empty cache: Len=%d Size=%d", c.Len(), c.Size())
	}

	k1 := BlockKey{SSTID: "a", BlockOffset: 0}
	k2 := BlockKey{SSTID: "b", BlockOffset: 0}

	c.Put(k1, []byte("12345"))      // +5
	c.Put(k2, []byte("1234567890")) // +10

	if c.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", c.Len())
	}
	if c.Size() != 15 {
		t.Fatalf("Size: got %d, want 15", c.Size())
	}

	// Update k1 with larger data.
	c.Put(k1, []byte("12345678")) // was 5, now 8 → total 18
	if c.Len() != 2 {
		t.Fatalf("Len after update: got %d, want 2", c.Len())
	}
	if c.Size() != 18 {
		t.Fatalf("Size after update: got %d, want 18", c.Size())
	}
}

func TestLRU_LenAndSizeAfterEviction(t *testing.T) {
	c := NewLRUBlockCache(15)

	k1 := BlockKey{SSTID: "a", BlockOffset: 0}
	k2 := BlockKey{SSTID: "b", BlockOffset: 0}
	k3 := BlockKey{SSTID: "c", BlockOffset: 0}

	c.Put(k1, make([]byte, 8))
	c.Put(k2, make([]byte, 6)) // 14 total
	c.Put(k3, make([]byte, 7)) // 21 > 15 → evict k1 (8) → 13, still > 15? no, 13 ≤ 15

	if c.Len() != 2 {
		t.Fatalf("Len after eviction: got %d, want 2", c.Len())
	}
	if c.Size() != 13 {
		t.Fatalf("Size after eviction: got %d, want 13", c.Size())
	}
}

func TestLRU_ConcurrentAccess(t *testing.T) {
	c := NewLRUBlockCache(4096)
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := BlockKey{SSTID: fmt.Sprintf("sst-%d", g), BlockOffset: int64(i)}
				c.Put(key, []byte(fmt.Sprintf("v-%d-%d", g, i)))
				c.Get(key)
				c.Len()
				c.Size()
			}
		}()
	}
	wg.Wait()

	// Sanity: nothing panicked, and cache is internally consistent.
	if c.Len() < 0 || c.Size() < 0 {
		t.Fatal("negative Len or Size after concurrent access")
	}
}

func TestLRU_PutDoesNotRetainCallerSlice(t *testing.T) {
	c := NewLRUBlockCache(1024)
	key := BlockKey{SSTID: "sst", BlockOffset: 0}

	buf := []byte("original")
	c.Put(key, buf)

	// Mutate the caller's slice after Put.
	for i := range buf {
		buf[i] = 'Z'
	}

	got := c.Get(key)
	if !bytes.Equal(got, []byte("original")) {
		t.Fatalf("Put did not copy data: got %q, want %q", got, "original")
	}
}

func TestLRU_EvictMultiple(t *testing.T) {
	// maxBytes=10, insert a single item of 15 bytes — must evict everything
	// and then the new item itself might exceed, but it still gets stored
	// (the loop evicts oldest while curBytes > max).
	c := NewLRUBlockCache(10)

	k1 := BlockKey{SSTID: "a", BlockOffset: 0}
	k2 := BlockKey{SSTID: "b", BlockOffset: 0}

	c.Put(k1, make([]byte, 5))
	c.Put(k2, make([]byte, 5)) // 10 total, at limit

	// Insert a big item — should evict both k1 and k2.
	k3 := BlockKey{SSTID: "c", BlockOffset: 0}
	c.Put(k3, make([]byte, 10))

	if c.Get(k1) != nil {
		t.Fatal("k1 should have been evicted")
	}
	if c.Get(k2) != nil {
		t.Fatal("k2 should have been evicted")
	}
	if c.Get(k3) == nil {
		t.Fatal("k3 should be present")
	}
	if c.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", c.Len())
	}
}

// ---------------------------------------------------------------------------
// DiskCache tests
// ---------------------------------------------------------------------------

func TestDisk_PutGet_BasicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello disk cache")
	if err := c.Put("block-0", data); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get("block-0")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}

	// Verify the file actually exists on disk.
	if _, err := os.Stat(filepath.Join(dir, "block-0")); err != nil {
		t.Fatalf("file not written to disk: %v", err)
	}
}

func TestDisk_GetMissReturnsNil(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.Get("nope")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("Get on missing key returned %v, want nil", got)
	}
}

func TestDisk_PutNestedKeyPath(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	key := "a/b/c.sst"
	data := []byte("nested data")

	if err := c.Put(key, data); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}

	// The file must exist at the nested path.
	full := filepath.Join(dir, "a", "b", "c.sst")
	if _, err := os.Stat(full); err != nil {
		t.Fatalf("nested file not on disk: %v", err)
	}
}

func TestDisk_Eviction(t *testing.T) {
	dir := t.TempDir()
	// Allow 25 bytes → room for 2 × 10-byte values.
	c, err := NewDiskCache(dir, 25)
	if err != nil {
		t.Fatal(err)
	}

	c.Put("k1", make([]byte, 10))
	c.Put("k2", make([]byte, 10))
	c.Put("k3", make([]byte, 10)) // evict k1

	// k1 should be evicted from the map and removed from disk.
	if c.Has("k1") {
		t.Fatal("k1 should have been evicted from map")
	}
	if _, err := os.Stat(filepath.Join(dir, "k1")); !os.IsNotExist(err) {
		t.Fatal("k1 file should have been removed from disk")
	}

	// k2 and k3 should still exist.
	for _, k := range []string{"k2", "k3"} {
		if !c.Has(k) {
			t.Fatalf("%s should still be in cache", k)
		}
		if _, err := os.Stat(filepath.Join(dir, k)); err != nil {
			t.Fatalf("%s file should exist on disk: %v", k, err)
		}
	}
}

func TestDisk_Has(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	if c.Has("x") {
		t.Fatal("Has should return false for missing key")
	}

	c.Put("x", []byte("value"))
	if !c.Has("x") {
		t.Fatal("Has should return true after Put")
	}
}

func TestDisk_GetAfterExternalDeletion(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	c.Put("ephemeral", []byte("data"))

	// Externally remove the file behind the cache's back.
	os.Remove(filepath.Join(dir, "ephemeral"))

	got, err := c.Get("ephemeral")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("Get after external deletion should return nil, got %q", got)
	}

	// The cache should have cleaned up internal state.
	if c.Has("ephemeral") {
		t.Fatal("Has should return false after failed Get cleaned up")
	}
}

func TestDisk_PutUpdatesExistingKey(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	c.Put("key", []byte("short"))

	newData := []byte("a much longer payload")
	c.Put("key", newData)

	got, err := c.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatalf("Get after update: got %q, want %q", got, newData)
	}

	// File on disk should have the new content.
	raw, _ := os.ReadFile(filepath.Join(dir, "key"))
	if !bytes.Equal(raw, newData) {
		t.Fatalf("file on disk: got %q, want %q", raw, newData)
	}
}

func TestDisk_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	c, err := NewDiskCache(dir, 8192)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("g%d-i%d", g, i)
				_ = c.Put(key, []byte(fmt.Sprintf("val-%d-%d", g, i)))
				_, _ = c.Get(key)
				c.Has(key)
			}
		}()
	}
	wg.Wait()
}

func TestDisk_EvictionLRUOrder(t *testing.T) {
	dir := t.TempDir()
	// 25 bytes max, 10-byte values → holds 2.
	c, err := NewDiskCache(dir, 25)
	if err != nil {
		t.Fatal(err)
	}

	c.Put("k1", make([]byte, 10))
	c.Put("k2", make([]byte, 10))

	// Touch k1 to make it recently used.
	c.Get("k1")

	// Insert k3 → should evict k2 (LRU), not k1.
	c.Put("k3", make([]byte, 10))

	if c.Has("k2") {
		t.Fatal("k2 should have been evicted as LRU")
	}
	if !c.Has("k1") {
		t.Fatal("k1 was recently accessed and should still be present")
	}
	if !c.Has("k3") {
		t.Fatal("k3 should be present")
	}
}

func TestDisk_NewDiskCacheCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "deep")
	_, err := NewDiskCache(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("cache dir was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("path is not a directory")
	}
}

func TestDisk_EvictMultiple(t *testing.T) {
	dir := t.TempDir()
	// 10 bytes max; two 5-byte entries fill it, then one 10-byte entry evicts both.
	c, err := NewDiskCache(dir, 10)
	if err != nil {
		t.Fatal(err)
	}

	c.Put("a", make([]byte, 5))
	c.Put("b", make([]byte, 5))
	c.Put("big", make([]byte, 10))

	if c.Has("a") {
		t.Fatal("a should have been evicted")
	}
	if c.Has("b") {
		t.Fatal("b should have been evicted")
	}
	if !c.Has("big") {
		t.Fatal("big should be present")
	}
}
