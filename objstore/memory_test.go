package objstore

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStorePutGet(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if err := store.Put(ctx, "key1", []byte("value1"), false); err != nil {
		t.Fatalf("Put: %v", err)
	}

	data, err := store.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("got %q, want %q", string(data), "value1")
	}
}

func TestMemoryStoreNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_, err := store.Get(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryStoreConditionalPut(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// First put with ifNoneMatch should succeed.
	if err := store.Put(ctx, "key1", []byte("v1"), true); err != nil {
		t.Fatalf("first Put: %v", err)
	}

	// Second put with ifNoneMatch should fail.
	err := store.Put(ctx, "key1", []byte("v2"), true)
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed, got %v", err)
	}

	// Verify original value unchanged.
	data, _ := store.Get(ctx, "key1")
	if string(data) != "v1" {
		t.Errorf("got %q, want %q", string(data), "v1")
	}

	// Unconditional put should overwrite.
	if err := store.Put(ctx, "key1", []byte("v3"), false); err != nil {
		t.Fatalf("unconditional Put: %v", err)
	}
	data, _ = store.Get(ctx, "key1")
	if string(data) != "v3" {
		t.Errorf("got %q, want %q", string(data), "v3")
	}
}

func TestMemoryStoreGetRange(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_ = store.Put(ctx, "key1", []byte("hello world"), false)

	data, err := store.GetRange(ctx, "key1", 6, 5)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(data) != "world" {
		t.Errorf("got %q, want %q", string(data), "world")
	}
}

func TestMemoryStoreList(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_ = store.Put(ctx, "prefix/a", []byte("1"), false)
	_ = store.Put(ctx, "prefix/b", []byte("2"), false)
	_ = store.Put(ctx, "other/c", []byte("3"), false)

	keys, err := store.List(ctx, "prefix/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_ = store.Put(ctx, "key1", []byte("value"), false)
	_ = store.Delete(ctx, "key1")

	exists, _ := store.Exists(ctx, "key1")
	if exists {
		t.Error("expected key to be deleted")
	}
}

func TestMemoryStoreExists(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	exists, _ := store.Exists(ctx, "key1")
	if exists {
		t.Error("expected false for nonexistent key")
	}

	_ = store.Put(ctx, "key1", []byte("value"), false)
	exists, _ = store.Exists(ctx, "key1")
	if !exists {
		t.Error("expected true for existing key")
	}
}

func TestMemoryStoreHead(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	_ = store.Put(ctx, "key1", []byte("hello"), false)
	meta, err := store.Head(ctx, "key1")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if meta.Key != "key1" {
		t.Errorf("Key: got %q, want %q", meta.Key, "key1")
	}
	if meta.Size != 5 {
		t.Errorf("Size: got %d, want 5", meta.Size)
	}
}
