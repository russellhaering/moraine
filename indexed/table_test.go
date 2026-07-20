package indexed

import (
	"context"
	"errors"
	"testing"
	"time"

	moraine "github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/document"
	"github.com/russellhaering/moraine/index"
	"github.com/russellhaering/moraine/objstore"
)

type staticEmbedder struct{}

func (staticEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if text == "near-one" {
		return []float32{1, 0}, nil
	}
	return []float32{0, 1}, nil
}

func TestTableWaitForIndexesAndAttributeSearch(t *testing.T) {
	ctx := context.Background()
	tbl, cleanup := openIndexedTestTable(t, "attrs", nil, nil, "")
	defer cleanup()

	doc := &document.Document{
		ID:         "doc1",
		Attributes: map[string]any{"color": "red"},
	}
	if err := tbl.PutDocument(ctx, doc); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}
	if doc.Version != 1 || doc.CreatedAt.IsZero() || doc.UpdatedAt.IsZero() {
		t.Fatalf("document metadata was not populated: %#v", doc)
	}

	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}

	results, err := tbl.SearchAttributes(ctx, []index.Filter{{Field: "color", Op: index.OpEq, Value: "red"}}, 10, 0)
	if err != nil {
		t.Fatalf("SearchAttributes: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc1" {
		t.Fatalf("results = %#v", results)
	}
}

func TestTableDeleteRemovesIndexes(t *testing.T) {
	ctx := context.Background()
	tbl, cleanup := openIndexedTestTable(t, "delete", nil, nil, "")
	defer cleanup()

	if err := tbl.PutDocument(ctx, &document.Document{
		ID:         "doc1",
		Attributes: map[string]any{"color": "red"},
	}); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}
	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes put: %v", err)
	}

	if err := tbl.DeleteDocument(ctx, "doc1"); err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}
	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes delete: %v", err)
	}

	results, err := tbl.SearchAttributes(ctx, []index.Filter{{Field: "color", Op: index.OpEq, Value: "red"}}, 10, 0)
	if err != nil {
		t.Fatalf("SearchAttributes: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results after delete, got %#v", results)
	}
}

func TestTableTextAndVectorSearch(t *testing.T) {
	ctx := context.Background()
	schema := &document.Schema{
		Fields: []document.FieldDefinition{
			{Name: "title", Type: document.FieldTypeString, FullText: true},
			{Name: "kind", Type: document.FieldTypeString, Indexed: true},
		},
		EmbeddingModel:      "test",
		EmbeddingDimensions: 2,
	}
	tbl, cleanup := openIndexedTestTable(t, "all-indexes", schema, staticEmbedder{}, t.TempDir())
	defer cleanup()

	if err := tbl.PutDocument(ctx, &document.Document{
		ID:         "doc1",
		Content:    "near-one",
		Attributes: map[string]any{"title": "Quick fox", "kind": "note"},
	}); err != nil {
		t.Fatalf("PutDocument doc1: %v", err)
	}
	if err := tbl.PutDocument(ctx, &document.Document{
		ID:         "doc2",
		Content:    "other",
		Attributes: map[string]any{"title": "Database storage", "kind": "note"},
	}); err != nil {
		t.Fatalf("PutDocument doc2: %v", err)
	}
	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}

	textResults, total, err := tbl.SearchText(ctx, "fox", 10, 0)
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if total != 1 || len(textResults) != 1 || textResults[0].ID != "doc1" {
		t.Fatalf("text results total=%d docs=%#v", total, textResults)
	}

	vectorResults, err := tbl.SearchVector(ctx, []float32{1, 0}, 1)
	if err != nil {
		t.Fatalf("SearchVector: %v", err)
	}
	if len(vectorResults) != 1 || vectorResults[0].ID != "doc1" {
		t.Fatalf("vector results = %#v", vectorResults)
	}
}

func TestTableRestartRebuildsInMemoryAttributeIndex(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	cacheDir := t.TempDir()
	schema := &document.Schema{
		Fields: []document.FieldDefinition{
			{Name: "color", Type: document.FieldTypeString, Indexed: true},
		},
	}

	db1 := openMoraineForIndexedTest(t, store, "restart")
	tbl1, err := OpenTable(ctx, TableConfig{
		Name:     "docs",
		Schema:   schema,
		DB:       db1,
		CacheDir: cacheDir,
	})
	if err != nil {
		t.Fatalf("OpenTable first: %v", err)
	}
	if err := tbl1.PutDocument(ctx, &document.Document{
		ID:         "doc1",
		Attributes: map[string]any{"color": "red"},
	}); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}
	if err := tbl1.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes first: %v", err)
	}
	if err := tbl1.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	db2 := openMoraineForIndexedTest(t, store, "restart")
	tbl2, err := OpenTable(ctx, TableConfig{
		Name:     "docs",
		Schema:   schema,
		DB:       db2,
		CacheDir: cacheDir,
	})
	if err != nil {
		t.Fatalf("OpenTable second: %v", err)
	}
	defer tbl2.Close()

	if err := tbl2.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes second: %v", err)
	}
	results, err := tbl2.SearchAttributes(ctx, []index.Filter{{Field: "color", Op: index.OpEq, Value: "red"}}, 10, 0)
	if err != nil {
		t.Fatalf("SearchAttributes: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc1" {
		t.Fatalf("results after restart = %#v", results)
	}
}

func TestSearchTextUnavailableWithoutCacheDir(t *testing.T) {
	ctx := context.Background()
	tbl, cleanup := openIndexedTestTable(t, "no-text", nil, nil, "")
	defer cleanup()

	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}

	_, _, err := tbl.SearchText(ctx, "anything", 10, 0)
	if !errors.Is(err, ErrFullTextUnavailable) {
		t.Fatalf("expected ErrFullTextUnavailable, got %v", err)
	}
}

func TestOpenTableRejectsUnsafeName(t *testing.T) {
	ctx := context.Background()
	db := openMoraineForIndexedTest(t, objstore.NewMemoryStore(), "unsafe-name")
	defer db.Close()

	_, err := OpenTable(ctx, TableConfig{
		Name: "../docs",
		DB:   db,
	})
	if err == nil {
		t.Fatal("expected unsafe name error")
	}
}

func TestSearchAttributesValidatesFilters(t *testing.T) {
	ctx := context.Background()
	tbl, cleanup := openIndexedTestTable(t, "filter-validation", nil, nil, "")
	defer cleanup()

	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}
	_, err := tbl.SearchAttributes(ctx, []index.Filter{{Field: "score", Op: index.OpGt, Value: "high"}}, 10, 0)
	if err == nil {
		t.Fatal("expected filter validation error")
	}
}

func TestSearchAttributesOnlyIndexesSchemaIndexedFields(t *testing.T) {
	ctx := context.Background()
	schema := &document.Schema{
		Fields: []document.FieldDefinition{
			{Name: "indexed", Type: document.FieldTypeString, Indexed: true},
			{Name: "unindexed", Type: document.FieldTypeString},
		},
	}
	tbl, cleanup := openIndexedTestTable(t, "schema-attrs", schema, nil, "")
	defer cleanup()

	if err := tbl.PutDocument(ctx, &document.Document{
		ID: "doc1",
		Attributes: map[string]any{
			"indexed":   "yes",
			"unindexed": "hidden",
		},
	}); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}
	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}

	results, err := tbl.SearchAttributes(ctx, []index.Filter{{Field: "indexed", Op: index.OpEq, Value: "yes"}}, 10, 0)
	if err != nil {
		t.Fatalf("Search indexed: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc1" {
		t.Fatalf("indexed results = %#v", results)
	}

	results, err = tbl.SearchAttributes(ctx, []index.Filter{{Field: "unindexed", Op: index.OpEq, Value: "hidden"}}, 10, 0)
	if err != nil {
		t.Fatalf("Search unindexed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results for unindexed field, got %#v", results)
	}
}

func TestTableAccessorsReturnOwnedSchemaSnapshot(t *testing.T) {
	ctx := context.Background()
	schema := &document.Schema{
		Fields: []document.FieldDefinition{
			{Name: "color", Type: document.FieldTypeString, Indexed: true},
		},
	}
	tbl, cleanup := openIndexedTestTable(t, "accessors", schema, nil, "")
	defer cleanup()

	if tbl.Name() != "accessors" {
		t.Fatalf("Name() = %q", tbl.Name())
	}
	if tbl.System() {
		t.Fatal("System() = true")
	}

	schema.Fields[0].Name = "mutated"
	got := tbl.Schema()
	if got.Fields[0].Name != "color" {
		t.Fatalf("table schema reflected caller mutation: %#v", got.Fields)
	}

	got.Fields[0].Name = "mutated-again"
	got = tbl.Schema()
	if got.Fields[0].Name != "color" {
		t.Fatalf("Schema() did not return a snapshot: %#v", got.Fields)
	}

	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}
	status := tbl.IndexStatus()
	if !status.Ready || status.Err != nil {
		t.Fatalf("unexpected index status: %#v", status)
	}
}

func TestTableOperationsAfterCloseReturnErrClosed(t *testing.T) {
	ctx := context.Background()
	tbl, cleanup := openIndexedTestTable(t, "closed", nil, nil, "")
	_ = cleanup

	if err := tbl.WaitForIndexes(ctx); err != nil {
		t.Fatalf("WaitForIndexes: %v", err)
	}
	if err := tbl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := tbl.PutDocument(ctx, &document.Document{ID: "doc1"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("PutDocument after close = %v", err)
	}
	if _, err := tbl.SearchAttributes(ctx, nil, 10, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("SearchAttributes after close = %v", err)
	}
	if err := tbl.WaitForIndexes(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("WaitForIndexes after close = %v", err)
	}
}

func openIndexedTestTable(t *testing.T, name string, schema *document.Schema, embedder Embedder, cacheDir string) (*Table, func()) {
	t.Helper()
	store := objstore.NewMemoryStore()
	db := openMoraineForIndexedTest(t, store, name)
	tbl, err := OpenTable(context.Background(), TableConfig{
		Name:     name,
		Schema:   schema,
		DB:       db,
		CacheDir: cacheDir,
		Embedder: embedder,
	})
	if err != nil {
		t.Fatalf("OpenTable: %v", err)
	}
	return tbl, func() {
		if err := tbl.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func openMoraineForIndexedTest(t *testing.T, store objstore.ObjectStore, prefix string) *moraine.DB {
	t.Helper()
	db, err := moraine.Open(context.Background(), moraine.DBConfig{
		Store:           store,
		Prefix:          prefix,
		MemTableMaxSize: 1 << 20,
		CompactInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}
