// Package index provides reusable indexing primitives for Moraine documents.
package index

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/google/btree"
)

// FilterOp represents an attribute filter operation.
type FilterOp string

const (
	OpEq       FilterOp = "eq"
	OpNeq      FilterOp = "neq"
	OpGt       FilterOp = "gt"
	OpGte      FilterOp = "gte"
	OpLt       FilterOp = "lt"
	OpLte      FilterOp = "lte"
	OpIn       FilterOp = "in"
	OpContains FilterOp = "contains"
)

// Filter represents a single attribute filter.
type Filter struct {
	Field string   `json:"field"`
	Op    FilterOp `json:"op"`
	Value any      `json:"value"`
}

// ValidateFilters checks that filters are well-formed.
func ValidateFilters(filters []Filter) error {
	for i, f := range filters {
		if f.Field == "" {
			return fmt.Errorf("filter %d: field is required", i)
		}
		switch f.Op {
		case OpEq, OpNeq, OpContains:
		case OpGt, OpGte, OpLt, OpLte:
			if _, ok := toFloat64(f.Value); !ok {
				return fmt.Errorf("filter %d: %s requires a numeric value", i, f.Op)
			}
		case OpIn:
			if !isSlice(f.Value) {
				return fmt.Errorf("filter %d: in requires a slice value", i)
			}
		default:
			return fmt.Errorf("filter %d: unknown op %q", i, f.Op)
		}
	}
	return nil
}

// AttributeIndex provides in-memory inverted indexes for document attributes.
type AttributeIndex struct {
	mu sync.RWMutex

	stringIndexes  map[string]map[string]map[string]struct{}
	numericIndexes map[string]*btree.BTreeG[numericEntry]
	boolIndexes    map[string][2]map[string]struct{}
	docFields      map[string]map[string][]any
}

type numericEntry struct {
	value float64
	docID string
}

func numericLess(a, b numericEntry) bool {
	if a.value != b.value {
		return a.value < b.value
	}
	return a.docID < b.docID
}

// NewAttributeIndex creates an empty attribute index.
func NewAttributeIndex() *AttributeIndex {
	return &AttributeIndex{
		stringIndexes:  make(map[string]map[string]map[string]struct{}),
		numericIndexes: make(map[string]*btree.BTreeG[numericEntry]),
		boolIndexes:    make(map[string][2]map[string]struct{}),
		docFields:      make(map[string]map[string][]any),
	}
}

// IndexDocument adds or updates a document's attributes in the index.
func (idx *AttributeIndex) IndexDocument(docID string, attrs map[string]any) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.removeDocLocked(docID)

	fields := make(map[string][]any)
	for field, value := range attrs {
		for _, elem := range attributeValues(value) {
			switch v := elem.(type) {
			case string:
				idx.indexString(field, v, docID)
				fields[field] = append(fields[field], v)
			case float64:
				idx.indexNumeric(field, v, docID)
				fields[field] = append(fields[field], v)
			case bool:
				idx.indexBool(field, v, docID)
				fields[field] = append(fields[field], v)
			}
		}
	}

	idx.docFields[docID] = fields
}

// DeleteDocument removes a document from the index.
func (idx *AttributeIndex) DeleteDocument(docID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeDocLocked(docID)
}

// Search applies filters and returns matching document IDs in stable order.
func (idx *AttributeIndex) Search(filters []Filter) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result map[string]struct{}

	if len(filters) == 0 {
		result = make(map[string]struct{}, len(idx.docFields))
		for id := range idx.docFields {
			result[id] = struct{}{}
		}
	} else {
		for _, f := range filters {
			matches := idx.applyFilter(f)
			if result == nil {
				result = matches
				continue
			}
			for id := range result {
				if _, ok := matches[id]; !ok {
					delete(result, id)
				}
			}
		}
	}

	ids := make([]string, 0, len(result))
	for id := range result {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Count returns the number of indexed documents.
func (idx *AttributeIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.docFields)
}

// String returns a debug summary.
func (idx *AttributeIndex) String() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return fmt.Sprintf("AttributeIndex{docs=%d, string_fields=%d, numeric_fields=%d, bool_fields=%d}",
		len(idx.docFields), len(idx.stringIndexes), len(idx.numericIndexes), len(idx.boolIndexes))
}

func (idx *AttributeIndex) removeDocLocked(docID string) {
	fields, ok := idx.docFields[docID]
	if !ok {
		return
	}

	for field, values := range fields {
		for _, value := range values {
			switch v := value.(type) {
			case string:
				if fm, ok := idx.stringIndexes[field]; ok {
					if docs, ok := fm[v]; ok {
						delete(docs, docID)
						if len(docs) == 0 {
							delete(fm, v)
						}
					}
				}
			case float64:
				if tree, ok := idx.numericIndexes[field]; ok {
					tree.Delete(numericEntry{value: v, docID: docID})
				}
			case bool:
				bi := 0
				if v {
					bi = 1
				}
				if bf, ok := idx.boolIndexes[field]; ok {
					delete(bf[bi], docID)
				}
			}
		}
	}

	delete(idx.docFields, docID)
}

func (idx *AttributeIndex) indexString(field, value, docID string) {
	if _, ok := idx.stringIndexes[field]; !ok {
		idx.stringIndexes[field] = make(map[string]map[string]struct{})
	}
	if _, ok := idx.stringIndexes[field][value]; !ok {
		idx.stringIndexes[field][value] = make(map[string]struct{})
	}
	idx.stringIndexes[field][value][docID] = struct{}{}
}

func (idx *AttributeIndex) indexNumeric(field string, value float64, docID string) {
	if _, ok := idx.numericIndexes[field]; !ok {
		idx.numericIndexes[field] = btree.NewG[numericEntry](16, numericLess)
	}
	idx.numericIndexes[field].ReplaceOrInsert(numericEntry{value: value, docID: docID})
}

func (idx *AttributeIndex) indexBool(field string, value bool, docID string) {
	if _, ok := idx.boolIndexes[field]; !ok {
		idx.boolIndexes[field] = [2]map[string]struct{}{
			make(map[string]struct{}),
			make(map[string]struct{}),
		}
	}
	bi := 0
	if value {
		bi = 1
	}
	idx.boolIndexes[field][bi][docID] = struct{}{}
}

func (idx *AttributeIndex) applyFilter(f Filter) map[string]struct{} {
	result := make(map[string]struct{})

	switch f.Op {
	case OpEq:
		idx.filterEq(f.Field, f.Value, result)
	case OpNeq:
		idx.filterNeq(f.Field, f.Value, result)
	case OpGt:
		idx.filterNumericRange(f.Field, f.Value, result, false, false)
	case OpGte:
		idx.filterNumericRange(f.Field, f.Value, result, true, false)
	case OpLt:
		idx.filterNumericRange(f.Field, f.Value, result, false, true)
	case OpLte:
		idx.filterNumericRange(f.Field, f.Value, result, true, true)
	case OpIn:
		for _, v := range attributeValues(f.Value) {
			idx.filterEq(f.Field, v, result)
		}
	case OpContains:
		idx.filterEq(f.Field, f.Value, result)
	}

	return result
}

func (idx *AttributeIndex) filterEq(field string, value any, result map[string]struct{}) {
	switch v := normalizeScalar(value).(type) {
	case string:
		if fm, ok := idx.stringIndexes[field]; ok {
			if docs, ok := fm[v]; ok {
				for id := range docs {
					result[id] = struct{}{}
				}
			}
		}
	case float64:
		if tree, ok := idx.numericIndexes[field]; ok {
			tree.AscendGreaterOrEqual(numericEntry{value: v}, func(e numericEntry) bool {
				if e.value != v {
					return false
				}
				result[e.docID] = struct{}{}
				return true
			})
		}
	case bool:
		bi := 0
		if v {
			bi = 1
		}
		if bf, ok := idx.boolIndexes[field]; ok {
			for id := range bf[bi] {
				result[id] = struct{}{}
			}
		}
	}
}

func (idx *AttributeIndex) filterNeq(field string, value any, result map[string]struct{}) {
	want := normalizeScalar(value)
	for docID, fields := range idx.docFields {
		values, ok := fields[field]
		if !ok {
			continue
		}
		hasExcludedValue := false
		for _, value := range values {
			if attributeValuesEqual(value, want) {
				hasExcludedValue = true
				break
			}
		}
		if !hasExcludedValue {
			result[docID] = struct{}{}
		}
	}
}

func (idx *AttributeIndex) filterNumericRange(field string, value any, result map[string]struct{}, inclusive, lessThan bool) {
	v, ok := toFloat64(value)
	if !ok {
		return
	}

	tree, ok := idx.numericIndexes[field]
	if !ok {
		return
	}

	if lessThan {
		tree.Ascend(func(e numericEntry) bool {
			if inclusive {
				if e.value > v {
					return false
				}
				result[e.docID] = struct{}{}
				return true
			}
			if e.value >= v {
				return false
			}
			result[e.docID] = struct{}{}
			return true
		})
		return
	}

	tree.AscendGreaterOrEqual(numericEntry{value: v}, func(e numericEntry) bool {
		if inclusive || e.value > v {
			result[e.docID] = struct{}{}
		}
		return true
	})
}

func attributeValues(v any) []any {
	switch vv := v.(type) {
	case []any:
		out := make([]any, 0, len(vv))
		for _, elem := range vv {
			out = append(out, normalizeScalar(elem))
		}
		return out
	case []string:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out
	case []int:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = float64(elem)
		}
		return out
	case []int64:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = float64(elem)
		}
		return out
	case []float32:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = float64(elem)
		}
		return out
	case []float64:
		out := make([]any, len(vv))
		for i, elem := range vv {
			out[i] = elem
		}
		return out
	default:
		return []any{normalizeScalar(v)}
	}
}

func normalizeScalar(v any) any {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	default:
		return v
	}
}

func attributeValuesEqual(a, b any) bool {
	return reflect.DeepEqual(normalizeScalar(a), normalizeScalar(b))
}

func isSlice(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case []any, []string, []int, []int64, []float32, []float64:
		return true
	default:
		rv := reflect.ValueOf(v)
		return rv.IsValid() && rv.Kind() == reflect.Slice
	}
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
