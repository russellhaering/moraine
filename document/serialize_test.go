package document

import (
	"testing"
	"time"
)

func TestSerializeRoundTrip(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 123, time.UTC)
	doc := &Document{
		Content: "hello world",
		Attributes: map[string]any{
			"color": "red",
			"score": float64(42),
			"tags":  []any{"go", "search"},
		},
		Embedding:      []float32{0.1, 0.2, 0.3},
		EmbeddingModel: "test-model",
		CreatedAt:      now,
		UpdatedAt:      now.Add(time.Second),
		Version:        7,
	}

	data, err := Serialize(doc)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	got, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if got.Content != doc.Content {
		t.Fatalf("content mismatch: got %q want %q", got.Content, doc.Content)
	}
	if got.Version != doc.Version {
		t.Fatalf("version mismatch: got %d want %d", got.Version, doc.Version)
	}
	if got.EmbeddingModel != doc.EmbeddingModel {
		t.Fatalf("embedding model mismatch: got %q want %q", got.EmbeddingModel, doc.EmbeddingModel)
	}
	if len(got.Embedding) != len(doc.Embedding) {
		t.Fatalf("embedding length mismatch: got %d want %d", len(got.Embedding), len(doc.Embedding))
	}
	if got.Attributes["color"] != "red" {
		t.Fatalf("attribute mismatch: %#v", got.Attributes)
	}
}

func TestSchemaValidate(t *testing.T) {
	schema := &Schema{
		Fields: []FieldDefinition{
			{Name: "title", Type: FieldTypeString, Required: true},
			{Name: "score", Type: FieldTypeFloat},
			{Name: "tags", Type: FieldTypeStringSlice},
		},
	}

	if err := schema.Validate(map[string]any{
		"title": "A",
		"score": 1,
		"tags":  []string{"x", "y"},
	}); err != nil {
		t.Fatalf("Validate valid attrs: %v", err)
	}

	if err := schema.Validate(map[string]any{"score": 1}); err == nil {
		t.Fatal("expected missing required field error")
	}
	if err := schema.Validate(map[string]any{"title": "A", "extra": true}); err == nil {
		t.Fatal("expected unknown field error")
	}
}
