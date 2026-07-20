package index

import "testing"

func TestVectorIndexSaveLoadRejectsWrongDimensions(t *testing.T) {
	cacheDir := t.TempDir()

	idx := NewVectorIndex(2)
	if err := idx.Add("doc1", []float32{1, 0}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := idx.Add("doc2", []float32{0, 1}); err != nil {
		t.Fatalf("Add second vector: %v", err)
	}
	if err := idx.Save(cacheDir, "docs"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if loaded := LoadVectorIndex(cacheDir, "docs", 3); loaded != nil {
		t.Fatal("expected nil for mismatched dimensions")
	}
	if loaded := LoadVectorIndex(cacheDir, "docs", 2); loaded == nil {
		t.Fatal("expected vector index for matching dimensions")
	}
}
