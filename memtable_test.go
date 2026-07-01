package moraine

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	m := NewMemTable()

	m.Put("alpha", []byte("v1"), 1)
	m.Put("beta", []byte("v2"), 2)
	m.Put("gamma", []byte("v3"), 3)

	tests := []struct {
		key    string
		want   string
		wantOK bool
	}{
		{"alpha", "v1", true},
		{"beta", "v2", true},
		{"gamma", "v3", true},
		{"delta", "", false},
	}

	for _, tt := range tests {
		e, ok := m.Get(tt.key)
		if ok != tt.wantOK {
			t.Fatalf("Get(%q): got ok=%v, want %v", tt.key, ok, tt.wantOK)
		}
		if ok && string(e.Value) != tt.want {
			t.Fatalf("Get(%q): got value=%q, want %q", tt.key, e.Value, tt.want)
		}
	}

	// Overwrite an existing key.
	m.Put("beta", []byte("v2-updated"), 4)
	e, ok := m.Get("beta")
	if !ok || string(e.Value) != "v2-updated" {
		t.Fatalf("Get after overwrite: got (%v, %v), want (v2-updated, true)", e, ok)
	}
	if e.SeqNum != 4 {
		t.Fatalf("SeqNum after overwrite: got %d, want 4", e.SeqNum)
	}
}

func TestDelete(t *testing.T) {
	m := NewMemTable()

	m.Put("key1", []byte("value1"), 1)
	m.Delete("key1", 2)

	e, ok := m.Get("key1")
	if !ok {
		t.Fatal("expected tombstone entry to be found")
	}
	if e.Value != nil {
		t.Fatalf("expected nil Value for tombstone, got %q", e.Value)
	}
	if e.SeqNum != 2 {
		t.Fatalf("expected SeqNum 2, got %d", e.SeqNum)
	}

	// Delete a key that was never inserted -- should create a tombstone.
	m.Delete("key2", 3)
	e, ok = m.Get("key2")
	if !ok {
		t.Fatal("expected tombstone for previously-absent key")
	}
	if e.Value != nil {
		t.Fatalf("expected nil Value for tombstone, got %q", e.Value)
	}
}

func TestIteratorOrdering(t *testing.T) {
	m := NewMemTable()

	keys := []string{"mango", "apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for i, k := range keys {
		m.Put(k, []byte(k), uint64(i+1))
	}

	sort.Strings(keys)

	it := m.Iterator()
	var got []string
	for it.Next() {
		got = append(got, it.Entry().Key)
	}

	if len(got) != len(keys) {
		t.Fatalf("iterator returned %d entries, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("iterator[%d] = %q, want %q", i, got[i], keys[i])
		}
	}
}

func TestConcurrentReads(t *testing.T) {
	m := NewMemTable()

	const numKeys = 200
	for i := 0; i < numKeys; i++ {
		m.Put(fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("val-%04d", i)), uint64(i))
	}

	const readers = 8
	var wg sync.WaitGroup
	wg.Add(readers)

	errs := make(chan error, readers)

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < numKeys; i++ {
				key := fmt.Sprintf("key-%04d", i)
				want := fmt.Sprintf("val-%04d", i)
				e, ok := m.Get(key)
				if !ok {
					errs <- fmt.Errorf("missing key %q", key)
					return
				}
				if string(e.Value) != want {
					errs <- fmt.Errorf("Get(%q) = %q, want %q", key, e.Value, want)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

func TestSizeTracking(t *testing.T) {
	m := NewMemTable()

	if m.Size() != 0 {
		t.Fatalf("empty table size: got %d, want 0", m.Size())
	}

	m.Put("k", []byte("v"), 1)
	s1 := m.Size()
	if s1 <= 0 {
		t.Fatalf("size after one Put should be positive, got %d", s1)
	}

	// Adding a second, larger entry should increase size.
	m.Put("longer-key", []byte("a-longer-value"), 2)
	s2 := m.Size()
	if s2 <= s1 {
		t.Fatalf("size should grow after second Put: got %d, previous %d", s2, s1)
	}

	// Overwriting with a shorter value should decrease size.
	m.Put("longer-key", []byte("x"), 3)
	s3 := m.Size()
	if s3 >= s2 {
		t.Fatalf("size should shrink after shorter overwrite: got %d, previous %d", s3, s2)
	}

	// Len should reflect the number of distinct keys.
	if m.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", m.Len())
	}
}

func TestFreeze(t *testing.T) {
	m := NewMemTable()
	m.Put("a", []byte("1"), 1)

	frozen := m.Freeze()
	if frozen != m {
		t.Fatal("Freeze should return the same MemTable pointer")
	}
	if !m.IsFrozen {
		t.Fatal("expected IsFrozen to be true after Freeze")
	}

	// Reads should still work on a frozen table.
	e, ok := m.Get("a")
	if !ok || string(e.Value) != "1" {
		t.Fatalf("Get on frozen table: got (%v, %v)", e, ok)
	}

	// Iterator should still work on a frozen table.
	it := m.Iterator()
	if !it.Next() {
		t.Fatal("expected iterator to have at least one entry on frozen table")
	}
	if it.Entry().Key != "a" {
		t.Fatalf("iterator entry key: got %q, want %q", it.Entry().Key, "a")
	}

	// Put should panic on a frozen table.
	assertPanics(t, "Put on frozen table", func() {
		m.Put("b", []byte("2"), 2)
	})

	// Delete should panic on a frozen table.
	assertPanics(t, "Delete on frozen table", func() {
		m.Delete("a", 3)
	})
}

func TestIteratorFromEmpty(t *testing.T) {
	m := NewMemTable()
	it := m.IteratorFrom("anything")
	if it.Next() {
		t.Fatal("expected no entries from empty memtable")
	}
}

func TestIteratorFromBeginning(t *testing.T) {
	m := NewMemTable()
	m.Put("b", []byte("2"), 1)
	m.Put("c", []byte("3"), 2)
	m.Put("d", []byte("4"), 3)

	// afterKey="" should return all entries.
	it := m.IteratorFrom("")
	var got []string
	for it.Next() {
		got = append(got, it.Entry().Key)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
}

func TestIteratorFromMiddle(t *testing.T) {
	m := NewMemTable()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		m.Put(k, []byte(k), 1)
	}

	// afterKey="b" should return c, d, e.
	it := m.IteratorFrom("b")
	var got []string
	for it.Next() {
		got = append(got, it.Entry().Key)
	}
	expected := []string{"c", "d", "e"}
	if len(got) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(got), got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("entry[%d]: expected %q, got %q", i, expected[i], got[i])
		}
	}
}

func TestIteratorFromExactKey(t *testing.T) {
	m := NewMemTable()
	for _, k := range []string{"a", "b", "c"} {
		m.Put(k, []byte(k), 1)
	}

	// afterKey="b" should NOT include "b" — only entries with key > afterKey.
	it := m.IteratorFrom("b")
	var got []string
	for it.Next() {
		got = append(got, it.Entry().Key)
	}
	if len(got) != 1 || got[0] != "c" {
		t.Fatalf("expected [c], got %v", got)
	}
}

func TestIteratorFromPastEnd(t *testing.T) {
	m := NewMemTable()
	m.Put("a", []byte("1"), 1)
	m.Put("b", []byte("2"), 2)

	it := m.IteratorFrom("z")
	if it.Next() {
		t.Fatal("expected no entries when afterKey is past all keys")
	}
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%s: expected panic but did not get one", name)
		}
	}()
	fn()
}
