package objstore

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned when the requested object does not exist.
var ErrNotFound = errors.New("object not found")

// ErrPreconditionFailed is returned when a conditional write fails
// (e.g., ifNoneMatch and the object already exists).
var ErrPreconditionFailed = errors.New("precondition failed")

// ObjectMeta holds metadata about an object.
type ObjectMeta struct {
	Key  string
	Size int64
	ETag string
}

// ObjectStore abstracts object storage operations.
type ObjectStore interface {
	// Put writes data to the given key. If ifNoneMatch is true, the write
	// only succeeds if the key does not already exist.
	Put(ctx context.Context, key string, data []byte, ifNoneMatch bool) error

	// Get retrieves the full object at the given key.
	Get(ctx context.Context, key string) ([]byte, error)

	// GetRange retrieves a byte range from the object.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)

	// Head returns metadata without fetching the body.
	Head(ctx context.Context, key string) (*ObjectMeta, error)

	// Delete removes the object at the given key.
	Delete(ctx context.Context, key string) error

	// List returns keys matching the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists returns true if the key exists.
	Exists(ctx context.Context, key string) (bool, error)
}

// ReadCloserToBytes reads all bytes from an io.ReadCloser and closes it.
func ReadCloserToBytes(rc io.ReadCloser) ([]byte, error) {
	defer rc.Close()
	return io.ReadAll(rc)
}
