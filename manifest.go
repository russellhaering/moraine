package moraine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/russellhaering/moraine/objstore"
)

// SSTInfo describes a single SSTable's location and metadata for the manifest.
type SSTInfo struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	MinKey string `json:"min_key"`
	MaxKey string `json:"max_key"`
	MinSeq uint64 `json:"min_seq"`
	MaxSeq uint64 `json:"max_seq"`
	Size   int64  `json:"size"`
}

// SortedRun represents a set of non-overlapping SSTables at a compaction level.
type SortedRun struct {
	Level    int       `json:"level"`
	SSTables []SSTInfo `json:"sstables"`
}

// Manifest tracks the LSM-tree state: which SSTables exist, WAL recovery point,
// and writer/compactor epochs.
type Manifest struct {
	Version        uint64      `json:"version"`
	WriterEpoch    uint64      `json:"writer_epoch"`
	CompactorEpoch uint64      `json:"compactor_epoch"`
	WALIDNext      uint64      `json:"wal_id_next"`
	L0             []SSTInfo   `json:"l0"`
	SortedRuns     []SortedRun `json:"sorted_runs"`
}

// ManifestStore handles reading and writing manifest files with CAS semantics.
type ManifestStore struct {
	store  objstore.ObjectStore
	prefix string
}

// NewManifestStore creates a new ManifestStore.
func NewManifestStore(store objstore.ObjectStore, prefix string) *ManifestStore {
	return &ManifestStore{store: store, prefix: prefix}
}

func (ms *ManifestStore) manifestPath(version uint64) string {
	return fmt.Sprintf("%s/manifest/%020d.manifest", ms.prefix, version)
}

// Save writes a manifest file using conditional put (CAS). Returns
// ErrPreconditionFailed if the version already exists.
func (ms *ManifestStore) Save(ctx context.Context, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: marshal: %w", err)
	}

	key := ms.manifestPath(m.Version)
	if err := ms.store.Put(ctx, key, data, true); err != nil {
		return fmt.Errorf("manifest: save version %d: %w", m.Version, err)
	}
	return nil
}

// Load reads a specific manifest version.
func (ms *ManifestStore) Load(ctx context.Context, version uint64) (*Manifest, error) {
	key := ms.manifestPath(version)
	data, err := ms.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("manifest: load version %d: %w", version, err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: unmarshal version %d: %w", version, err)
	}
	return &m, nil
}

// LoadLatest finds and loads the highest-versioned manifest. Returns nil if
// no manifests exist.
func (ms *ManifestStore) LoadLatest(ctx context.Context) (*Manifest, error) {
	prefix := fmt.Sprintf("%s/manifest/", ms.prefix)
	keys, err := ms.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("manifest: list: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	// Keys are lexicographically sortable due to zero-padded version numbers.
	sort.Strings(keys)
	latestKey := keys[len(keys)-1]

	// Extract version from the key.
	data, err := ms.store.Get(ctx, latestKey)
	if err != nil {
		return nil, fmt.Errorf("manifest: load latest (%s): %w", latestKey, err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: unmarshal latest: %w", err)
	}
	return &m, nil
}

// ListVersions returns all manifest version numbers in ascending order.
func (ms *ManifestStore) ListVersions(ctx context.Context) ([]uint64, error) {
	prefix := fmt.Sprintf("%s/manifest/", ms.prefix)
	keys, err := ms.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var versions []uint64
	for _, k := range keys {
		// Extract the version number from the key path.
		base := k[strings.LastIndex(k, "/")+1:]
		var v uint64
		if _, err := fmt.Sscanf(base, "%d.manifest", &v); err == nil {
			versions = append(versions, v)
		}
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions, nil
}
