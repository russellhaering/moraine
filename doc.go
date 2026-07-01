// Package moraine provides an embedded object-storage-backed LSM key/value store.
//
// A DB stores string keys and byte-slice values. Writes go to an in-memory
// MemTable and are flushed to immutable SSTables in an objstore.ObjectStore.
// The manifest tracks WAL files, L0 SSTables, sorted runs, sequence numbers,
// and writer epochs.
//
// The package is intentionally small: callers bring their own serialization,
// object-store backend, and higher-level data model.
package moraine
