package index

import (
	"reflect"
	"testing"
)

func TestAttributeIndexFilters(t *testing.T) {
	idx := NewAttributeIndex()
	idx.IndexDocument("doc1", map[string]any{"color": "red", "score": 10, "active": true, "tags": []string{"go", "db"}})
	idx.IndexDocument("doc2", map[string]any{"color": "blue", "score": float64(20), "active": false, "tags": []any{"python"}})
	idx.IndexDocument("doc3", map[string]any{"color": "red", "score": int64(30), "active": true, "tags": []any{"go"}})

	tests := []struct {
		name    string
		filters []Filter
		want    []string
	}{
		{
			name:    "string eq",
			filters: []Filter{{Field: "color", Op: OpEq, Value: "red"}},
			want:    []string{"doc1", "doc3"},
		},
		{
			name:    "numeric gt",
			filters: []Filter{{Field: "score", Op: OpGt, Value: 10}},
			want:    []string{"doc2", "doc3"},
		},
		{
			name:    "numeric lte",
			filters: []Filter{{Field: "score", Op: OpLte, Value: float64(20)}},
			want:    []string{"doc1", "doc2"},
		},
		{
			name:    "bool eq",
			filters: []Filter{{Field: "active", Op: OpEq, Value: true}},
			want:    []string{"doc1", "doc3"},
		},
		{
			name:    "in",
			filters: []Filter{{Field: "color", Op: OpIn, Value: []any{"blue", "green"}}},
			want:    []string{"doc2"},
		},
		{
			name:    "contains",
			filters: []Filter{{Field: "tags", Op: OpContains, Value: "go"}},
			want:    []string{"doc1", "doc3"},
		},
		{
			name: "multiple filters",
			filters: []Filter{
				{Field: "color", Op: OpEq, Value: "red"},
				{Field: "score", Op: OpGt, Value: 20},
			},
			want: []string{"doc3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idx.Search(tt.filters)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Search() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAttributeIndexReindexAndDelete(t *testing.T) {
	idx := NewAttributeIndex()
	idx.IndexDocument("doc1", map[string]any{"color": "red"})
	idx.IndexDocument("doc2", map[string]any{"color": "red"})

	idx.IndexDocument("doc1", map[string]any{"color": "blue"})
	if got := idx.Search([]Filter{{Field: "color", Op: OpEq, Value: "red"}}); !reflect.DeepEqual(got, []string{"doc2"}) {
		t.Fatalf("red search after reindex = %v", got)
	}

	idx.DeleteDocument("doc2")
	if got := idx.Search([]Filter{}); !reflect.DeepEqual(got, []string{"doc1"}) {
		t.Fatalf("all search after delete = %v", got)
	}
}

func TestAttributeIndexNeqExcludesMultiValueContainingExcludedValue(t *testing.T) {
	idx := NewAttributeIndex()
	idx.IndexDocument("doc1", map[string]any{"tags": []string{"go", "db"}})
	idx.IndexDocument("doc2", map[string]any{"tags": []string{"python"}})

	got := idx.Search([]Filter{{Field: "tags", Op: OpNeq, Value: "go"}})
	if !reflect.DeepEqual(got, []string{"doc2"}) {
		t.Fatalf("neq search = %v", got)
	}
}

func TestValidateFilters(t *testing.T) {
	if err := ValidateFilters([]Filter{{Field: "score", Op: OpGt, Value: "high"}}); err == nil {
		t.Fatal("expected numeric validation error")
	}
	if err := ValidateFilters([]Filter{{Field: "color", Op: FilterOp("bad"), Value: "red"}}); err == nil {
		t.Fatal("expected unknown op validation error")
	}
	if err := ValidateFilters([]Filter{{Field: "color", Op: OpIn, Value: []string{"red"}}}); err != nil {
		t.Fatalf("expected valid in filter: %v", err)
	}
}
