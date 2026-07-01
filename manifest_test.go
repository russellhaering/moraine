package moraine

import (
	"context"
	"testing"

	"github.com/russellhaering/moraine/objstore"
)

// TestManifestSaveAndLoad tests basic manifest persistence.
func TestManifestSaveAndLoad(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	m := &Manifest{
		Version:     1,
		WriterEpoch: 1,
		WALIDNext:   5,
		L0: []SSTInfo{
			{ID: "sst-1", Path: "test/wal/1.sst", MinKey: "a", MaxKey: "z", MinSeq: 1, MaxSeq: 10},
		},
	}

	if err := ms.Save(ctx, m); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := ms.Load(ctx, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.Version != 1 || loaded.WriterEpoch != 1 || loaded.WALIDNext != 5 {
		t.Fatalf("unexpected manifest: %+v", loaded)
	}
	if len(loaded.L0) != 1 || loaded.L0[0].ID != "sst-1" {
		t.Fatalf("unexpected L0: %+v", loaded.L0)
	}
}

// TestManifestLoadLatest verifies that LoadLatest returns the highest version.
func TestManifestLoadLatest(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	for v := uint64(1); v <= 5; v++ {
		m := &Manifest{
			Version:     v,
			WriterEpoch: v,
			WALIDNext:   v,
		}
		if err := ms.Save(ctx, m); err != nil {
			t.Fatalf("Save v%d: %v", v, err)
		}
	}

	latest, err := ms.LoadLatest(ctx)
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	if latest.Version != 5 {
		t.Fatalf("expected version 5, got %d", latest.Version)
	}
}

// TestManifestCASPreventsOverwrite verifies that saving the same version twice fails.
func TestManifestCASPreventsOverwrite(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	m := &Manifest{Version: 1, WriterEpoch: 1}
	if err := ms.Save(ctx, m); err != nil {
		t.Fatalf("Save first: %v", err)
	}

	// Saving the same version again should fail (conditional put).
	err := ms.Save(ctx, m)
	if err == nil {
		t.Fatal("expected error on duplicate version save")
	}
}

// TestManifestListVersions verifies version listing.
func TestManifestListVersions(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	for v := uint64(1); v <= 3; v++ {
		ms.Save(ctx, &Manifest{Version: v})
	}

	versions, err := ms.ListVersions(ctx)
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(versions))
	}
	for i, v := range versions {
		if v != uint64(i+1) {
			t.Fatalf("expected version %d at index %d, got %d", i+1, i, v)
		}
	}
}

// TestManifestLoadLatestEmpty verifies LoadLatest returns nil with no manifests.
func TestManifestLoadLatestEmpty(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	m, err := ms.LoadLatest(ctx)
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	if m != nil {
		t.Fatalf("expected nil, got %+v", m)
	}
}

// TestManifestPreservesSortedRuns verifies sorted runs survive save/load.
func TestManifestPreservesSortedRuns(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	m := &Manifest{
		Version: 1,
		SortedRuns: []SortedRun{
			{
				Level: 1,
				SSTables: []SSTInfo{
					{ID: "sorted-1", Path: "test/sorted/1.sst", MinKey: "a", MaxKey: "m"},
					{ID: "sorted-2", Path: "test/sorted/2.sst", MinKey: "n", MaxKey: "z"},
				},
			},
		},
	}

	if err := ms.Save(ctx, m); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := ms.Load(ctx, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(loaded.SortedRuns) != 1 {
		t.Fatalf("expected 1 sorted run, got %d", len(loaded.SortedRuns))
	}
	if len(loaded.SortedRuns[0].SSTables) != 2 {
		t.Fatalf("expected 2 sstables in sorted run, got %d", len(loaded.SortedRuns[0].SSTables))
	}
	if loaded.SortedRuns[0].SSTables[0].MinKey != "a" || loaded.SortedRuns[0].SSTables[1].MaxKey != "z" {
		t.Fatalf("sorted run key ranges incorrect: %+v", loaded.SortedRuns[0])
	}
}

// TestWriterEpochIncrementsOnReopen verifies that each OpenWriter call increments
// the writer epoch (fencing mechanism).
func TestWriterEpochIncrementsOnReopen(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()

	w1, err := OpenWriter(ctx, WriterConfig{Store: store, Prefix: "test"})
	if err != nil {
		t.Fatalf("OpenWriter 1: %v", err)
	}
	epoch1 := w1.epoch

	w2, err := OpenWriter(ctx, WriterConfig{Store: store, Prefix: "test"})
	if err != nil {
		t.Fatalf("OpenWriter 2: %v", err)
	}
	epoch2 := w2.epoch

	if epoch2 <= epoch1 {
		t.Fatalf("expected epoch2 (%d) > epoch1 (%d)", epoch2, epoch1)
	}
}

// TestManifestVersionGrowsAcrossFlushes verifies the manifest version keeps
// incrementing as flushes happen.
func TestManifestVersionGrowsAcrossFlushes(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	ms := NewManifestStore(store, "test")

	w, err := OpenWriter(ctx, WriterConfig{Store: store, Prefix: "test"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Initial version from OpenWriter.
	v0 := w.CurrentManifest().Version

	// Put + flush should bump the version.
	w.Put(ctx, "k1", []byte("v1"))
	w.Flush(ctx)
	v1 := w.CurrentManifest().Version

	w.Put(ctx, "k2", []byte("v2"))
	w.Flush(ctx)
	v2 := w.CurrentManifest().Version

	if v1 <= v0 {
		t.Fatalf("expected v1 (%d) > v0 (%d)", v1, v0)
	}
	if v2 <= v1 {
		t.Fatalf("expected v2 (%d) > v1 (%d)", v2, v1)
	}

	// LoadLatest should match.
	latest, err := ms.LoadLatest(ctx)
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	if latest.Version != v2 {
		t.Fatalf("expected latest version %d, got %d", v2, latest.Version)
	}
}
