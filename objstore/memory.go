package objstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory implementation of ObjectStore for testing.
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemoryStore creates a new in-memory object store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[string][]byte),
	}
}

func (m *MemoryStore) Put(_ context.Context, key string, data []byte, ifNoneMatch bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ifNoneMatch {
		if _, exists := m.objects[key]; exists {
			return ErrPreconditionFailed
		}
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[key] = cp
	return nil
}

func (m *MemoryStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *MemoryStore) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	if offset >= int64(len(data)) {
		return nil, fmt.Errorf("offset %d beyond object size %d", offset, len(data))
	}

	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	cp := make([]byte, end-offset)
	copy(cp, data[offset:end])
	return cp, nil
}

func (m *MemoryStore) Head(_ context.Context, key string) (*ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	return &ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *MemoryStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *MemoryStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.objects[key]
	return ok, nil
}
