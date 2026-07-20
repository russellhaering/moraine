package index

import (
	"testing"

	"github.com/russellhaering/moraine/document"
)

func TestBleveIndexSearchAndReopen(t *testing.T) {
	cacheDir := t.TempDir()
	schema := &document.Schema{
		Fields: []document.FieldDefinition{
			{Name: "title", Type: document.FieldTypeString, FullText: true},
		},
	}

	idx, err := NewBleveIndex(cacheDir, "docs", schema)
	if err != nil {
		t.Fatalf("NewBleveIndex: %v", err)
	}

	if err := idx.IndexDocument(&document.Document{
		ID:         "doc1",
		Content:    "the quick brown fox",
		Attributes: map[string]any{"title": "Forest Notes"},
	}); err != nil {
		t.Fatalf("IndexDocument doc1: %v", err)
	}
	if err := idx.IndexDocument(&document.Document{
		ID:         "doc2",
		Content:    "database internals",
		Attributes: map[string]any{"title": "Storage Notes"},
	}); err != nil {
		t.Fatalf("IndexDocument doc2: %v", err)
	}

	results, total, err := idx.Search("fox", 10, 0)
	if err != nil {
		t.Fatalf("Search fox: %v", err)
	}
	if total != 1 || len(results) != 1 || results[0].DocID != "doc1" {
		t.Fatalf("fox results = total %d %#v", total, results)
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewBleveIndex(cacheDir, "docs", schema)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	results, total, err = reopened.Search("attr_title:storage", 10, 0)
	if err != nil {
		t.Fatalf("Search title: %v", err)
	}
	if total != 1 || len(results) != 1 || results[0].DocID != "doc2" {
		t.Fatalf("title results = total %d %#v", total, results)
	}
}

func TestBleveIndexDoesNotDynamicallyIndexUnmappedStringAttributes(t *testing.T) {
	cacheDir := t.TempDir()
	schema := &document.Schema{
		Fields: []document.FieldDefinition{
			{Name: "title", Type: document.FieldTypeString, FullText: true},
			{Name: "status", Type: document.FieldTypeString, Indexed: true},
		},
	}

	idx, err := NewBleveIndex(cacheDir, "dynamic-fields", schema)
	if err != nil {
		t.Fatalf("NewBleveIndex: %v", err)
	}
	defer idx.Close()

	if err := idx.IndexDocument(&document.Document{
		ID:         "doc1",
		Attributes: map[string]any{"title": "Visible title", "status": "secret-status"},
	}); err != nil {
		t.Fatalf("IndexDocument: %v", err)
	}

	_, total, err := idx.Search("attr_status:secret-status", 10, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if total != 0 {
		t.Fatalf("expected unmapped status field to be unsearchable, got total %d", total)
	}
}
