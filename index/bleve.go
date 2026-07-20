package index

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/russellhaering/moraine/document"
)

// BleveIndex wraps a Bleve full-text search index.
type BleveIndex struct {
	index bleve.Index
	path  string
}

// NewBleveIndex creates or opens a Bleve index at cacheDir/bleve/name.
func NewBleveIndex(cacheDir, name string, schema *document.Schema) (*BleveIndex, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}

	path := filepath.Join(cacheDir, "bleve", name)

	idx, err := bleve.Open(path)
	if err == nil {
		return &BleveIndex{index: idx, path: path}, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("bleve: mkdir: %w", err)
	}

	idx, err = bleve.New(path, buildMapping(schema))
	if err != nil {
		return nil, fmt.Errorf("bleve: create index: %w", err)
	}

	return &BleveIndex{index: idx, path: path}, nil
}

func buildMapping(schema *document.Schema) *mapping.IndexMappingImpl {
	indexMapping := bleve.NewIndexMapping()
	docMapping := bleve.NewDocumentMapping()
	docMapping.Dynamic = false

	contentField := bleve.NewTextFieldMapping()
	docMapping.AddFieldMappingsAt("content", contentField)

	if schema != nil {
		for _, f := range schema.Fields {
			switch {
			case f.FullText:
				fm := bleve.NewTextFieldMapping()
				docMapping.AddFieldMappingsAt("attr_"+f.Name, fm)
			case f.Type == document.FieldTypeInt || f.Type == document.FieldTypeFloat:
				fm := bleve.NewNumericFieldMapping()
				docMapping.AddFieldMappingsAt("attr_"+f.Name, fm)
			case f.Type == document.FieldTypeBool:
				fm := bleve.NewBooleanFieldMapping()
				docMapping.AddFieldMappingsAt("attr_"+f.Name, fm)
			case f.Type == document.FieldTypeDatetime:
				fm := bleve.NewDateTimeFieldMapping()
				docMapping.AddFieldMappingsAt("attr_"+f.Name, fm)
			}
		}
	}

	indexMapping.DefaultMapping = docMapping
	return indexMapping
}

func bleveDoc(doc *document.Document) map[string]any {
	m := make(map[string]any)
	if doc.Content != "" {
		m["content"] = doc.Content
	}
	for k, v := range doc.Attributes {
		m["attr_"+k] = v
	}
	return m
}

// IndexDocument adds or updates a document in the index.
func (b *BleveIndex) IndexDocument(doc *document.Document) error {
	return b.index.Index(doc.ID, bleveDoc(doc))
}

// DeleteDocument removes a document from the index.
func (b *BleveIndex) DeleteDocument(docID string) error {
	return b.index.Delete(docID)
}

// FTSResult holds a full-text search result.
type FTSResult struct {
	DocID string
	Score float64
}

// Search performs a full-text search query.
func (b *BleveIndex) Search(queryStr string, limit, offset int) ([]FTSResult, int, error) {
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	query := bleve.NewQueryStringQuery(queryStr)
	req := bleve.NewSearchRequestOptions(query, limit, offset, false)

	result, err := b.index.Search(req)
	if err != nil {
		return nil, 0, fmt.Errorf("bleve: search: %w", err)
	}

	results := make([]FTSResult, len(result.Hits))
	for i, hit := range result.Hits {
		results[i] = FTSResult{
			DocID: hit.ID,
			Score: hit.Score,
		}
	}
	return results, int(result.Total), nil
}

// Path returns the filesystem path of the Bleve index directory.
func (b *BleveIndex) Path() string {
	return b.path
}

// Close closes the Bleve index.
func (b *BleveIndex) Close() error {
	return b.index.Close()
}

// DocCount returns the number of documents in the index.
func (b *BleveIndex) DocCount() (uint64, error) {
	return b.index.DocCount()
}
