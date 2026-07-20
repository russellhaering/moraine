package index

import (
	"context"
	"testing"
	"time"

	moraine "github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/document"
	"github.com/russellhaering/moraine/objstore"
)

type testIndexTarget struct {
	attrs  *AttributeIndex
	resets int
}

func (t *testIndexTarget) ResetIndexes(context.Context) error {
	t.attrs = NewAttributeIndex()
	t.resets++
	return nil
}

func (t *testIndexTarget) IndexEntry(_ context.Context, entry moraine.Entry) error {
	if entry.Value == nil || document.IsTombstone(entry.Value) {
		t.attrs.DeleteDocument(entry.Key)
		return nil
	}
	doc, err := document.Deserialize(entry.Value)
	if err != nil {
		return err
	}
	t.attrs.IndexDocument(entry.Key, doc.Attributes)
	return nil
}

func TestBuilderRebuildAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	db := openIndexTestDB(t, objstore.NewMemoryStore(), "builder-rebuild")
	defer db.Close()

	putIndexTestDoc(t, db, "doc1", map[string]any{"color": "red"})
	putIndexTestDoc(t, db, "doc2", map[string]any{"color": "blue"})
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	target := &testIndexTarget{}
	builder := NewBuilder(BuilderConfig{
		DB:       db,
		Target:   target,
		CacheDir: t.TempDir(),
		Name:     "docs",
	})

	last, err := builder.Rebuild(ctx)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if last == 0 {
		t.Fatal("expected non-zero checkpoint")
	}
	if target.resets != 1 {
		t.Fatalf("expected 1 reset, got %d", target.resets)
	}
	if got := target.attrs.Search([]Filter{{Field: "color", Op: OpEq, Value: "red"}}); len(got) != 1 || got[0] != "doc1" {
		t.Fatalf("red search = %v", got)
	}
}

func TestBuilderCatchUpProcessesTombstone(t *testing.T) {
	ctx := context.Background()
	db := openIndexTestDB(t, objstore.NewMemoryStore(), "builder-catchup")
	defer db.Close()

	putIndexTestDoc(t, db, "doc1", map[string]any{"color": "red"})
	putIndexTestDoc(t, db, "doc2", map[string]any{"color": "blue"})
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	cacheDir := t.TempDir()
	target := &testIndexTarget{}
	builder := NewBuilder(BuilderConfig{DB: db, Target: target, CacheDir: cacheDir, Name: "docs"})
	if _, err := builder.Rebuild(ctx); err != nil {
		t.Fatalf("initial Rebuild: %v", err)
	}

	if _, err := db.Delete(ctx, "doc1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush delete: %v", err)
	}

	if _, err := builder.CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if got := target.attrs.Search([]Filter{}); len(got) != 1 || got[0] != "doc2" {
		t.Fatalf("all docs after catch-up = %v", got)
	}
}

func TestBuilderReportsConfigurationErrors(t *testing.T) {
	ctx := context.Background()

	if _, err := (*Builder)(nil).Rebuild(ctx); err == nil {
		t.Fatal("expected nil builder error")
	}
	if _, err := NewBuilder(BuilderConfig{}).CatchUp(ctx); err == nil {
		t.Fatal("expected missing DB error")
	}

	db := openIndexTestDB(t, objstore.NewMemoryStore(), "builder-validation")
	defer db.Close()
	if _, err := NewBuilder(BuilderConfig{DB: db}).Rebuild(ctx); err == nil {
		t.Fatal("expected missing target error")
	}
}

func TestValidateName(t *testing.T) {
	for _, name := range []string{"docs", "tenant_01", "abc.def"} {
		if err := ValidateName(name); err != nil {
			t.Fatalf("ValidateName(%q): %v", name, err)
		}
	}

	for _, name := range []string{"", ".", "..", "../docs", "tenant/docs", `tenant\docs`} {
		if err := ValidateName(name); err == nil {
			t.Fatalf("ValidateName(%q) succeeded", name)
		}
	}
}

func openIndexTestDB(t *testing.T, store objstore.ObjectStore, prefix string) *moraine.DB {
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

func putIndexTestDoc(t *testing.T, db *moraine.DB, id string, attrs map[string]any) {
	t.Helper()
	now := time.Now().UTC()
	doc := &document.Document{
		ID:         id,
		Attributes: attrs,
		CreatedAt:  now,
		UpdatedAt:  now,
		Version:    1,
	}
	data, err := document.Serialize(doc)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if _, err := db.Put(context.Background(), id, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
}
