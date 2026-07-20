// Package document defines the document model used by Moraine's indexed layer.
package document

import "time"

// Document represents a single document in an indexed table.
type Document struct {
	ID             string         `json:"id"`
	Content        string         `json:"content,omitempty"`
	Attributes     map[string]any `json:"attributes,omitempty"`
	Embedding      []float32      `json:"embedding,omitempty"`
	EmbeddingModel string         `json:"embedding_model,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Version        uint64         `json:"version"`
}

// Clone returns a deep copy of the document.
func (d *Document) Clone() *Document {
	if d == nil {
		return nil
	}

	cp := *d
	if d.Attributes != nil {
		cp.Attributes = make(map[string]any, len(d.Attributes))
		for k, v := range d.Attributes {
			cp.Attributes[k] = cloneAttributeValue(v)
		}
	}
	if d.Embedding != nil {
		cp.Embedding = make([]float32, len(d.Embedding))
		copy(cp.Embedding, d.Embedding)
	}
	return &cp
}

func cloneAttributeValue(v any) any {
	switch vv := v.(type) {
	case []any:
		cp := make([]any, len(vv))
		for i, elem := range vv {
			cp[i] = cloneAttributeValue(elem)
		}
		return cp
	case []string:
		cp := make([]string, len(vv))
		copy(cp, vv)
		return cp
	case []int:
		cp := make([]int, len(vv))
		copy(cp, vv)
		return cp
	case []int64:
		cp := make([]int64, len(vv))
		copy(cp, vv)
		return cp
	case []float64:
		cp := make([]float64, len(vv))
		copy(cp, vv)
		return cp
	case []float32:
		cp := make([]float32, len(vv))
		copy(cp, vv)
		return cp
	default:
		return v
	}
}
