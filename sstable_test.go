package moraine

import (
	"fmt"
	"testing"
)

func TestWriteReadRoundTrip(t *testing.T) {
	w := NewSSTableWriter("test-1", DefaultBlockSize)
	w.Add(Entry{Key: "apple", Value: []byte("red"), SeqNum: 1})
	w.Add(Entry{Key: "banana", Value: []byte("yellow"), SeqNum: 2})
	w.Add(Entry{Key: "cherry", Value: []byte("dark-red"), SeqNum: 3})

	data, meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.EntryCount != 3 {
		t.Fatalf("expected 3 entries, got %d", meta.EntryCount)
	}
	if meta.MinKey != "apple" {
		t.Fatalf("expected MinKey=apple, got %s", meta.MinKey)
	}
	if meta.MaxKey != "cherry" {
		t.Fatalf("expected MaxKey=cherry, got %s", meta.MaxKey)
	}
	if meta.MinSeq != 1 || meta.MaxSeq != 3 {
		t.Fatalf("expected seq range [1,3], got [%d,%d]", meta.MinSeq, meta.MaxSeq)
	}
	if meta.Size != int64(len(data)) {
		t.Fatalf("expected Size=%d, got %d", len(data), meta.Size)
	}

	r, err := NewSSTableReader("test-1", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	rmeta := r.Meta()
	if rmeta.EntryCount != meta.EntryCount {
		t.Fatalf("reader entry count %d != writer %d", rmeta.EntryCount, meta.EntryCount)
	}
	if rmeta.MinKey != meta.MinKey || rmeta.MaxKey != meta.MaxKey {
		t.Fatalf("reader key range [%s,%s] != writer [%s,%s]",
			rmeta.MinKey, rmeta.MaxKey, meta.MinKey, meta.MaxKey)
	}
	if rmeta.MinSeq != meta.MinSeq || rmeta.MaxSeq != meta.MaxSeq {
		t.Fatalf("reader seq range [%d,%d] != writer [%d,%d]",
			rmeta.MinSeq, rmeta.MaxSeq, meta.MinSeq, meta.MaxSeq)
	}

	// Verify all values via Get.
	for _, tc := range []struct {
		key string
		val string
	}{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark-red"},
	} {
		e, err := r.Get(tc.key)
		if err != nil {
			t.Fatalf("Get(%s): %v", tc.key, err)
		}
		if e == nil {
			t.Fatalf("Get(%s): not found", tc.key)
		}
		if string(e.Value) != tc.val {
			t.Fatalf("Get(%s): expected %s, got %s", tc.key, tc.val, string(e.Value))
		}
	}
}

func TestMultipleDataBlocks(t *testing.T) {
	// Use a tiny block size to force multiple blocks.
	w := NewSSTableWriter("test-multi", 64)

	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("value-%05d", i)
		w.Add(Entry{Key: key, Value: []byte(val), SeqNum: uint64(i + 1)})
	}

	data, meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.EntryCount != numEntries {
		t.Fatalf("expected %d entries, got %d", numEntries, meta.EntryCount)
	}

	r, err := NewSSTableReader("test-multi", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	// Must have more than one index entry (meaning more than one data block).
	if len(r.index) <= 1 {
		t.Fatalf("expected multiple data blocks, got %d", len(r.index))
	}

	// Verify every entry is retrievable via Get.
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%05d", i)
		expected := fmt.Sprintf("value-%05d", i)
		e, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if e == nil {
			t.Fatalf("Get(%s): not found", key)
		}
		if string(e.Value) != expected {
			t.Fatalf("Get(%s): expected %s, got %s", key, expected, string(e.Value))
		}
	}
}

func TestGetBinarySearch(t *testing.T) {
	// Insert entries out of order to verify sorting and binary search.
	w := NewSSTableWriter("test-bsearch", 64)
	w.Add(Entry{Key: "delta", Value: []byte("4"), SeqNum: 4})
	w.Add(Entry{Key: "alpha", Value: []byte("1"), SeqNum: 1})
	w.Add(Entry{Key: "charlie", Value: []byte("3"), SeqNum: 3})
	w.Add(Entry{Key: "bravo", Value: []byte("2"), SeqNum: 2})

	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := NewSSTableReader("test-bsearch", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	// Existing keys.
	for _, tc := range []struct {
		key string
		val string
	}{
		{"alpha", "1"},
		{"bravo", "2"},
		{"charlie", "3"},
		{"delta", "4"},
	} {
		e, err := r.Get(tc.key)
		if err != nil {
			t.Fatalf("Get(%s): %v", tc.key, err)
		}
		if e == nil {
			t.Fatalf("Get(%s): not found", tc.key)
		}
		if string(e.Value) != tc.val {
			t.Fatalf("Get(%s): expected %s, got %s", tc.key, tc.val, string(e.Value))
		}
	}

	// Non-existing keys.
	for _, key := range []string{"aaa", "az", "echo", "zzz"} {
		e, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if e != nil {
			t.Fatalf("Get(%s): expected nil, got %+v", key, e)
		}
	}
}

func TestBloomFilter(t *testing.T) {
	w := NewSSTableWriter("test-bloom", DefaultBlockSize)

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("bloom-key-%05d", i)
		w.Add(Entry{Key: keys[i], Value: []byte("v"), SeqNum: uint64(i + 1)})
	}

	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := NewSSTableReader("test-bloom", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	// All inserted keys must pass the bloom filter (no false negatives).
	for _, key := range keys {
		if !r.BloomMayContain(key) {
			t.Fatalf("BloomMayContain(%s) = false, expected true", key)
		}
	}

	// Check false positive rate with keys known to be absent.
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		key := fmt.Sprintf("absent-key-%06d", i)
		if r.BloomMayContain(key) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(testCount)
	// Allow up to 5% false positive rate (should be around 1%).
	if fpRate > 0.05 {
		t.Fatalf("bloom false positive rate too high: %.4f (%d/%d)", fpRate, falsePositives, testCount)
	}
	t.Logf("bloom false positive rate: %.4f (%d/%d)", fpRate, falsePositives, testCount)
}

func TestSSTableIteratorOrdering(t *testing.T) {
	w := NewSSTableWriter("test-iter", 64)

	// Insert in reverse order to test sorting.
	for i := 49; i >= 0; i-- {
		key := fmt.Sprintf("iter-%05d", i)
		w.Add(Entry{Key: key, Value: []byte(key), SeqNum: uint64(i + 1)})
	}

	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := NewSSTableReader("test-iter", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	iter := r.Iterator()
	var prev string
	count := 0
	for iter.Next() {
		e := iter.Entry()
		if prev != "" && e.Key <= prev {
			t.Fatalf("iterator out of order: %s after %s", e.Key, prev)
		}
		prev = e.Key
		count++
	}

	if count != 50 {
		t.Fatalf("expected 50 entries from iterator, got %d", count)
	}
}

func TestEmptySSTable(t *testing.T) {
	w := NewSSTableWriter("test-empty", DefaultBlockSize)

	data, meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.EntryCount != 0 {
		t.Fatalf("expected 0 entries, got %d", meta.EntryCount)
	}
	if meta.MinKey != "" || meta.MaxKey != "" {
		t.Fatalf("expected empty key range, got [%s, %s]", meta.MinKey, meta.MaxKey)
	}

	r, err := NewSSTableReader("test-empty", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	rmeta := r.Meta()
	if rmeta.EntryCount != 0 {
		t.Fatalf("reader expected 0 entries, got %d", rmeta.EntryCount)
	}

	// Get on empty table should return nil.
	e, err := r.Get("anything")
	if err != nil {
		t.Fatalf("Get on empty: %v", err)
	}
	if e != nil {
		t.Fatalf("Get on empty: expected nil, got %+v", e)
	}

	// Iterator should produce no entries.
	iter := r.Iterator()
	if iter.Next() {
		t.Fatal("iterator on empty SSTable should return false immediately")
	}

	// Bloom filter should not contain anything.
	if r.BloomMayContain("anything") {
		t.Fatal("BloomMayContain on empty SSTable should return false")
	}
}

func TestDuplicateKeysHighestSeqNum(t *testing.T) {
	w := NewSSTableWriter("test-dup", DefaultBlockSize)
	w.Add(Entry{Key: "key", Value: []byte("old"), SeqNum: 1})
	w.Add(Entry{Key: "key", Value: []byte("new"), SeqNum: 5})
	w.Add(Entry{Key: "key", Value: []byte("mid"), SeqNum: 3})

	data, meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if meta.EntryCount != 3 {
		t.Fatalf("expected 3 entries, got %d", meta.EntryCount)
	}

	r, err := NewSSTableReader("test-dup", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	e, err := r.Get("key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e == nil {
		t.Fatal("Get: expected entry, got nil")
	}
	// Should return the entry with the highest sequence number.
	if string(e.Value) != "new" || e.SeqNum != 5 {
		t.Fatalf("Get: expected (new, seqnum=5), got (%s, seqnum=%d)", string(e.Value), e.SeqNum)
	}
}

func TestFooterMagicValidation(t *testing.T) {
	w := NewSSTableWriter("test-magic", DefaultBlockSize)
	w.Add(Entry{Key: "a", Value: []byte("b"), SeqNum: 1})

	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Corrupt the magic bytes (located at len-8 through len-5).
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[len(corrupted)-8] = 0xFF
	corrupted[len(corrupted)-7] = 0xFF

	_, err = NewSSTableReader("test-magic", corrupted)
	if err == nil {
		t.Fatal("expected error for corrupted magic, got nil")
	}
}

func TestSSTableIteratorFromEmpty(t *testing.T) {
	w := NewSSTableWriter("test-from-empty", DefaultBlockSize)
	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := NewSSTableReader("test-from-empty", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	it := r.IteratorFrom("anything")
	if it.Next() {
		t.Fatal("expected no entries from empty SSTable")
	}
}

func TestSSTableIteratorFromBeginning(t *testing.T) {
	w := NewSSTableWriter("test-from-begin", 64)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%05d", i)
		w.Add(Entry{Key: key, Value: []byte(key), SeqNum: uint64(i + 1)})
	}
	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := NewSSTableReader("test-from-begin", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	// Empty afterKey should return all entries.
	it := r.IteratorFrom("")
	count := 0
	for it.Next() {
		count++
	}
	if it.Err() != nil {
		t.Fatalf("unexpected error: %v", it.Err())
	}
	if count != 20 {
		t.Fatalf("expected 20 entries, got %d", count)
	}
}

func TestSSTableIteratorFromMiddle(t *testing.T) {
	// Use tiny block size for multi-block SSTable.
	w := NewSSTableWriter("test-from-mid", 64)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%05d", i)
		w.Add(Entry{Key: key, Value: []byte(key), SeqNum: uint64(i + 1)})
	}
	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := NewSSTableReader("test-from-mid", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	// After "key-00024" should give keys 25..49 (25 entries).
	it := r.IteratorFrom("key-00024")
	var got []string
	for it.Next() {
		got = append(got, it.Entry().Key)
	}
	if it.Err() != nil {
		t.Fatalf("unexpected error: %v", it.Err())
	}
	if len(got) != 25 {
		t.Fatalf("expected 25 entries, got %d", len(got))
	}
	if got[0] != "key-00025" {
		t.Fatalf("expected first key key-00025, got %s", got[0])
	}
	if got[len(got)-1] != "key-00049" {
		t.Fatalf("expected last key key-00049, got %s", got[len(got)-1])
	}
}

func TestSSTableIteratorFromPastEnd(t *testing.T) {
	w := NewSSTableWriter("test-from-past", 64)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%05d", i)
		w.Add(Entry{Key: key, Value: []byte(key), SeqNum: uint64(i + 1)})
	}
	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := NewSSTableReader("test-from-past", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	it := r.IteratorFrom("zzz")
	if it.Next() {
		t.Fatal("expected no entries when afterKey is past all keys")
	}
}

func TestSSTableIteratorFromExactKey(t *testing.T) {
	w := NewSSTableWriter("test-from-exact", 64)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%05d", i)
		w.Add(Entry{Key: key, Value: []byte(key), SeqNum: uint64(i + 1)})
	}
	data, _, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := NewSSTableReader("test-from-exact", data)
	if err != nil {
		t.Fatalf("NewSSTableReader: %v", err)
	}

	// Exact key match: should NOT include the afterKey itself.
	it := r.IteratorFrom("key-00005")
	var got []string
	for it.Next() {
		got = append(got, it.Entry().Key)
	}
	if it.Err() != nil {
		t.Fatalf("unexpected error: %v", it.Err())
	}
	// Should get keys 6..9 (4 entries).
	if len(got) != 4 {
		t.Fatalf("expected 4 entries, got %d: %v", len(got), got)
	}
	if got[0] != "key-00006" {
		t.Fatalf("expected first key key-00006, got %s", got[0])
	}
}

func TestDataTooShortForFooter(t *testing.T) {
	_, err := NewSSTableReader("bad", make([]byte, 10))
	if err == nil {
		t.Fatal("expected error for data shorter than footer")
	}
}

// TestDecodeEntryLegacyZeroLengthTombstone verifies that entries written by
// the pre-extraction engine — which encoded deletions as zero-length values
// rather than the tombstone sentinel — decode as tombstones (nil Value).
// Regression test for production stores written before the extraction.
func TestDecodeEntryLegacyZeroLengthTombstone(t *testing.T) {
	// Hand-encode the legacy form: keyLen, key, valLen=0, seqNum.
	var block []byte
	block = appendUint32(block, 3)
	block = append(block, "doc"...)
	block = appendUint32(block, 0) // legacy tombstone: zero-length value, no sentinel
	block = appendUint64(block, 42)

	entry, n, err := decodeEntry(block, 0)
	if err != nil {
		t.Fatalf("decode legacy tombstone: %v", err)
	}
	if n != len(block) {
		t.Fatalf("consumed %d bytes, want %d", n, len(block))
	}
	if entry.Key != "doc" || entry.SeqNum != 42 {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if entry.Value != nil {
		t.Fatalf("legacy zero-length value must decode as tombstone (nil), got %#v", entry.Value)
	}

	// Sentinel-encoded tombstones still decode identically.
	sentinel := appendEntry(nil, Entry{Key: "doc", Value: nil, SeqNum: 42})
	entry2, _, err := decodeEntry(sentinel, 0)
	if err != nil {
		t.Fatalf("decode sentinel tombstone: %v", err)
	}
	if entry2.Value != nil {
		t.Fatalf("sentinel tombstone must decode as nil, got %#v", entry2.Value)
	}
}
