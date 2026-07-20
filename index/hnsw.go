package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/coder/hnsw"
)

// VectorIndex wraps an HNSW vector index for similarity search.
type VectorIndex struct {
	mu      sync.RWMutex
	graph   *hnsw.Graph[string]
	vectors map[string][]float32
	dims    int
}

// NewVectorIndex creates a new in-memory HNSW vector index.
func NewVectorIndex(dimensions int) *VectorIndex {
	return &VectorIndex{
		graph:   hnsw.NewGraph[string](),
		vectors: make(map[string][]float32),
		dims:    dimensions,
	}
}

// LoadVectorIndex loads a vector index from disk, or returns nil if it is missing or invalid.
func LoadVectorIndex(cacheDir, name string, dimensions int) *VectorIndex {
	if cacheDir == "" || dimensions <= 0 {
		return nil
	}
	if err := ValidateName(name); err != nil {
		return nil
	}

	path := filepath.Join(cacheDir, "hnsw", name+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var persisted persistedVectorIndex
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil
	}
	if persisted.Dimensions != dimensions {
		return nil
	}

	idx := NewVectorIndex(dimensions)
	for docID, vector := range persisted.Vectors {
		if err := idx.Add(docID, vector); err != nil {
			return nil
		}
	}
	return idx
}

// Add inserts or updates a vector for docID.
func (v *VectorIndex) Add(docID string, embedding []float32) (err error) {
	if len(embedding) != v.dims {
		return fmt.Errorf("vector: expected %d dimensions, got %d", v.dims, len(embedding))
	}

	vec := make([]float32, len(embedding))
	copy(vec, embedding)

	v.mu.Lock()
	defer v.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("vector: add %q: %v", docID, r)
		}
	}()
	v.graph.Delete(docID)
	v.graph.Add(hnsw.MakeNode(docID, vec))
	v.vectors[docID] = vec
	return nil
}

// Delete removes a document from the vector index.
func (v *VectorIndex) Delete(docID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.graph.Delete(docID)
	delete(v.vectors, docID)
}

// VectorSearchResult holds a vector search result.
type VectorSearchResult struct {
	DocID    string
	Distance float32
}

// Search finds the k nearest neighbors to query.
func (v *VectorIndex) Search(query []float32, k int) (results []VectorSearchResult, err error) {
	if len(query) != v.dims {
		return nil, fmt.Errorf("vector: expected %d dimensions, got %d", v.dims, len(query))
	}
	if k <= 0 {
		return []VectorSearchResult{}, nil
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	results = make([]VectorSearchResult, 0, len(v.vectors))
	for docID, vector := range v.vectors {
		results = append(results, VectorSearchResult{
			DocID:    docID,
			Distance: hnsw.CosineDistance(vector, query),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Distance != results[j].Distance {
			return results[i].Distance < results[j].Distance
		}
		return results[i].DocID < results[j].DocID
	})
	if len(results) > k {
		results = results[:k]
	}
	return results, nil
}

// Save serializes the vector index to disk.
func (v *VectorIndex) Save(cacheDir, name string) error {
	if cacheDir == "" {
		return nil
	}
	if err := ValidateName(name); err != nil {
		return err
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	dir := filepath.Join(cacheDir, "hnsw")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	vectors := make(map[string][]float32, len(v.vectors))
	for docID, vector := range v.vectors {
		cp := make([]float32, len(vector))
		copy(cp, vector)
		vectors[docID] = cp
	}

	data, err := json.Marshal(persistedVectorIndex{
		Dimensions: v.dims,
		Vectors:    vectors,
	})
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dir, name+".json"), data, 0o644)
}

// Count returns the approximate number of vectors in the index.
func (v *VectorIndex) Count() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.vectors)
}

type persistedVectorIndex struct {
	Dimensions int                  `json:"dimensions"`
	Vectors    map[string][]float32 `json:"vectors"`
}
