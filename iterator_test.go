package moraine

import (
	"errors"
	"testing"
)

// sliceIterator is a test helper that iterates over a fixed slice of entries.
type sliceIterator struct {
	entries []Entry
	pos     int
	err     error
}

func newSliceIterator(entries []Entry) *sliceIterator {
	return &sliceIterator{entries: entries, pos: -1}
}

func (it *sliceIterator) Next() bool {
	if it.err != nil {
		return false
	}
	it.pos++
	return it.pos < len(it.entries)
}

func (it *sliceIterator) Entry() Entry {
	return it.entries[it.pos]
}

func (it *sliceIterator) Err() error {
	return it.err
}

func TestMergeIteratorBasicMerge(t *testing.T) {
	src1 := newSliceIterator([]Entry{
		{Key: "a", Value: []byte("1"), SeqNum: 1},
		{Key: "c", Value: []byte("3"), SeqNum: 3},
		{Key: "e", Value: []byte("5"), SeqNum: 5},
	})
	src2 := newSliceIterator([]Entry{
		{Key: "b", Value: []byte("2"), SeqNum: 2},
		{Key: "d", Value: []byte("4"), SeqNum: 4},
	})

	m := NewMergeIterator([]prioritizedIterator{
		{iter: src1, priority: 0},
		{iter: src2, priority: 1},
	})

	expected := []string{"a", "b", "c", "d", "e"}
	var got []string
	for m.Next() {
		got = append(got, m.Entry().Key)
	}
	if m.Err() != nil {
		t.Fatalf("unexpected error: %v", m.Err())
	}
	if len(got) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(got))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("entry[%d]: expected %q, got %q", i, expected[i], got[i])
		}
	}
}

func TestMergeIteratorDedup(t *testing.T) {
	// Both sources have key "b". Source with priority 0 (newer) should win.
	src1 := newSliceIterator([]Entry{
		{Key: "a", Value: []byte("a-new"), SeqNum: 10},
		{Key: "b", Value: []byte("b-new"), SeqNum: 10},
	})
	src2 := newSliceIterator([]Entry{
		{Key: "b", Value: []byte("b-old"), SeqNum: 5},
		{Key: "c", Value: []byte("c-old"), SeqNum: 5},
	})

	m := NewMergeIterator([]prioritizedIterator{
		{iter: src1, priority: 0},
		{iter: src2, priority: 1},
	})

	var got []Entry
	for m.Next() {
		got = append(got, m.Entry())
	}
	if m.Err() != nil {
		t.Fatalf("unexpected error: %v", m.Err())
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries after dedup, got %d", len(got))
	}
	// Check that "b" has the newer value.
	if string(got[1].Value) != "b-new" {
		t.Fatalf("expected b-new for key b, got %s", string(got[1].Value))
	}
}

func TestMergeIteratorTombstoneFiltering(t *testing.T) {
	// Source 0 (newer) has a tombstone for "b".
	src1 := newSliceIterator([]Entry{
		{Key: "b", Value: nil, SeqNum: 10}, // tombstone
	})
	src2 := newSliceIterator([]Entry{
		{Key: "a", Value: []byte("a"), SeqNum: 1},
		{Key: "b", Value: []byte("b-old"), SeqNum: 5},
		{Key: "c", Value: []byte("c"), SeqNum: 2},
	})

	m := NewMergeIterator([]prioritizedIterator{
		{iter: src1, priority: 0},
		{iter: src2, priority: 1},
	})

	var got []string
	for m.Next() {
		got = append(got, m.Entry().Key)
	}
	if m.Err() != nil {
		t.Fatalf("unexpected error: %v", m.Err())
	}
	// "b" should be filtered out because the winning entry is a tombstone.
	expected := []string{"a", "c"}
	if len(got) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(got), got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("entry[%d]: expected %q, got %q", i, expected[i], got[i])
		}
	}
}

func TestMergeIteratorEmptySources(t *testing.T) {
	// All empty sources.
	m := NewMergeIterator([]prioritizedIterator{
		{iter: newSliceIterator(nil), priority: 0},
		{iter: newSliceIterator(nil), priority: 1},
	})
	if m.Next() {
		t.Fatal("expected no entries from empty sources")
	}
	if m.Err() != nil {
		t.Fatalf("unexpected error: %v", m.Err())
	}
}

func TestMergeIteratorNoSources(t *testing.T) {
	m := NewMergeIterator(nil)
	if m.Next() {
		t.Fatal("expected no entries from nil sources")
	}
	if m.Err() != nil {
		t.Fatalf("unexpected error: %v", m.Err())
	}
}

func TestMergeIteratorSingleSource(t *testing.T) {
	src := newSliceIterator([]Entry{
		{Key: "x", Value: []byte("1"), SeqNum: 1},
		{Key: "y", Value: []byte("2"), SeqNum: 2},
	})
	m := NewMergeIterator([]prioritizedIterator{
		{iter: src, priority: 0},
	})

	var got []string
	for m.Next() {
		got = append(got, m.Entry().Key)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
}

func TestMergeIteratorErrorPropagation(t *testing.T) {
	testErr := errors.New("test error")
	src := &sliceIterator{
		entries: []Entry{{Key: "a", Value: []byte("1"), SeqNum: 1}},
		pos:     -1,
		err:     testErr,
	}
	m := NewMergeIterator([]prioritizedIterator{
		{iter: src, priority: 0},
	})
	if m.Next() {
		t.Fatal("expected no entries when source has error")
	}
	if m.Err() != testErr {
		t.Fatalf("expected test error, got %v", m.Err())
	}
}
