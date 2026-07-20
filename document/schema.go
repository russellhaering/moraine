package document

import (
	"fmt"
	"time"
)

// FieldType represents the type of a schema field.
type FieldType int

const (
	FieldTypeString FieldType = iota + 1
	FieldTypeInt
	FieldTypeFloat
	FieldTypeBool
	FieldTypeStringSlice
	FieldTypeIntSlice
	FieldTypeFloatSlice
	FieldTypeDatetime
	FieldTypeReference
)

func (ft FieldType) String() string {
	switch ft {
	case FieldTypeString:
		return "string"
	case FieldTypeInt:
		return "int"
	case FieldTypeFloat:
		return "float"
	case FieldTypeBool:
		return "bool"
	case FieldTypeStringSlice:
		return "[]string"
	case FieldTypeIntSlice:
		return "[]int"
	case FieldTypeFloatSlice:
		return "[]float"
	case FieldTypeDatetime:
		return "datetime"
	case FieldTypeReference:
		return "reference"
	default:
		return "unknown"
	}
}

// ParseFieldType parses a field type string.
func ParseFieldType(s string) (FieldType, error) {
	switch s {
	case "string":
		return FieldTypeString, nil
	case "int":
		return FieldTypeInt, nil
	case "float":
		return FieldTypeFloat, nil
	case "bool":
		return FieldTypeBool, nil
	case "[]string":
		return FieldTypeStringSlice, nil
	case "[]int":
		return FieldTypeIntSlice, nil
	case "[]float":
		return FieldTypeFloatSlice, nil
	case "datetime":
		return FieldTypeDatetime, nil
	case "reference":
		return FieldTypeReference, nil
	default:
		return 0, fmt.Errorf("unknown field type: %q", s)
	}
}

// MarshalText implements encoding.TextMarshaler.
func (ft FieldType) MarshalText() ([]byte, error) {
	return []byte(ft.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (ft *FieldType) UnmarshalText(data []byte) error {
	parsed, err := ParseFieldType(string(data))
	if err != nil {
		return err
	}
	*ft = parsed
	return nil
}

// FieldDefinition describes a single field in a document schema.
type FieldDefinition struct {
	Name        string    `json:"name"`
	Type        FieldType `json:"type"`
	Required    bool      `json:"required,omitempty"`
	Indexed     bool      `json:"indexed,omitempty"`
	FullText    bool      `json:"full_text,omitempty"`
	ReferenceDB string    `json:"reference_db,omitempty"`
}

// Schema defines the attributes and embedding configuration for a table.
type Schema struct {
	Fields              []FieldDefinition `json:"fields"`
	EmbeddingModel      string            `json:"embedding_model,omitempty"`
	EmbeddingDimensions int               `json:"embedding_dimensions,omitempty"`
}

// Clone returns a deep copy of the schema.
func (s *Schema) Clone() *Schema {
	if s == nil {
		return nil
	}
	cp := *s
	if s.Fields != nil {
		cp.Fields = make([]FieldDefinition, len(s.Fields))
		copy(cp.Fields, s.Fields)
	}
	return &cp
}

// SchemaChange describes index-relevant changes between two schemas.
type SchemaChange struct {
	EmbeddingChanged      bool
	IndexedFieldsChanged  bool
	FullTextFieldsChanged bool
}

// Changed returns true if any index-relevant schema setting changed.
func (sc SchemaChange) Changed() bool {
	return sc.EmbeddingChanged || sc.IndexedFieldsChanged || sc.FullTextFieldsChanged
}

// DiffSchemas compares two schemas and reports index-relevant changes.
func DiffSchemas(old, new *Schema) SchemaChange {
	var sc SchemaChange

	oldModel, oldDims := "", 0
	newModel, newDims := "", 0
	if old != nil {
		oldModel = old.EmbeddingModel
		oldDims = old.EmbeddingDimensions
	}
	if new != nil {
		newModel = new.EmbeddingModel
		newDims = new.EmbeddingDimensions
	}
	sc.EmbeddingChanged = oldModel != newModel || oldDims != newDims
	sc.IndexedFieldsChanged = !equalStringSet(indexedFieldSet(old), indexedFieldSet(new))
	sc.FullTextFieldsChanged = !equalStringSet(fullTextFieldSet(old), fullTextFieldSet(new))
	return sc
}

// Validate checks that attrs conform to the schema.
func (s *Schema) Validate(attrs map[string]any) error {
	if s == nil {
		return nil
	}

	fieldDefs := make(map[string]FieldDefinition, len(s.Fields))
	for _, f := range s.Fields {
		if f.Name == "" {
			return fmt.Errorf("schema contains unnamed field")
		}
		fieldDefs[f.Name] = f
	}

	for name := range attrs {
		if _, ok := fieldDefs[name]; !ok {
			return fmt.Errorf("unknown field %q", name)
		}
	}

	for _, f := range s.Fields {
		val, ok := attrs[f.Name]
		if !ok {
			if f.Required {
				return fmt.Errorf("required field %q is missing", f.Name)
			}
			continue
		}
		if err := validateFieldValue(f, val); err != nil {
			return fmt.Errorf("field %q: %w", f.Name, err)
		}
	}

	return nil
}

func validateFieldValue(f FieldDefinition, val any) error {
	switch f.Type {
	case FieldTypeString, FieldTypeReference:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string, got %T", val)
		}
	case FieldTypeInt:
		switch val.(type) {
		case int, int64, float64:
		default:
			return fmt.Errorf("expected int, got %T", val)
		}
	case FieldTypeFloat:
		switch val.(type) {
		case float64, float32, int, int64:
		default:
			return fmt.Errorf("expected float, got %T", val)
		}
	case FieldTypeBool:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", val)
		}
	case FieldTypeStringSlice:
		slice, ok := toAnySlice(val)
		if !ok {
			return fmt.Errorf("expected []string, got %T", val)
		}
		for i, v := range slice {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("element [%d]: expected string, got %T", i, v)
			}
		}
	case FieldTypeIntSlice:
		slice, ok := toAnySlice(val)
		if !ok {
			return fmt.Errorf("expected []int, got %T", val)
		}
		for i, v := range slice {
			switch v.(type) {
			case int, int64, float64:
			default:
				return fmt.Errorf("element [%d]: expected int, got %T", i, v)
			}
		}
	case FieldTypeFloatSlice:
		slice, ok := toAnySlice(val)
		if !ok {
			return fmt.Errorf("expected []float, got %T", val)
		}
		for i, v := range slice {
			switch v.(type) {
			case float64, float32, int, int64:
			default:
				return fmt.Errorf("element [%d]: expected float, got %T", i, v)
			}
		}
	case FieldTypeDatetime:
		switch v := val.(type) {
		case string:
			if _, err := time.Parse(time.RFC3339, v); err != nil {
				return fmt.Errorf("expected RFC3339 datetime string, got %q", v)
			}
		case time.Time:
		default:
			return fmt.Errorf("expected datetime string or time.Time, got %T", val)
		}
	default:
		return fmt.Errorf("unknown field type %v", f.Type)
	}
	return nil
}

func toAnySlice(v any) ([]any, bool) {
	switch vv := v.(type) {
	case []any:
		return vv, true
	case []string:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out, true
	case []int:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out, true
	case []int64:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out, true
	case []float32:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out, true
	case []float64:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out, true
	default:
		return nil, false
	}
}

func indexedFieldSet(s *Schema) map[string]struct{} {
	m := make(map[string]struct{})
	if s == nil {
		return m
	}
	for _, f := range s.Fields {
		if f.Indexed {
			m[f.Name] = struct{}{}
		}
	}
	return m
}

func fullTextFieldSet(s *Schema) map[string]struct{} {
	m := make(map[string]struct{})
	if s == nil {
		return m
	}
	for _, f := range s.Fields {
		if f.FullText {
			m[f.Name] = struct{}{}
		}
	}
	return m
}

func equalStringSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
