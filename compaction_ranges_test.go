package moraine

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/russellhaering/moraine/objstore"
)

func openRangeTestDB(t *testing.T, store objstore.ObjectStore, target int64) *DB {
	t.Helper()
	return openTestDB(t, store, DBConfig{L0CompactThresh: 2, TargetSSTableSize: target})
}

func putRange(t *testing.T, db *DB, from, to int, tag string) {
	t.Helper()
	ctx := context.Background()
	for i := from; i < to; i++ {
		if _, err := db.Put(ctx, fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("%s-%05d-padding-padding", tag, i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func firstRun(t *testing.T, db *DB) []SSTInfo {
	t.Helper()
	m := db.writer.CurrentManifest()
	if len(m.SortedRuns) != 1 {
		t.Fatalf("expected one sorted run, got %d", len(m.SortedRuns))
	}
	return m.SortedRuns[0].SSTables
}

func assertNonOverlapping(t *testing.T, ssts []SSTInfo) {
	t.Helper()
	for i := 1; i < len(ssts); i++ {
		if ssts[i-1].MaxKey >= ssts[i].MinKey {
			t.Fatalf("sstables overlap: %+v then %+v", ssts[i-1], ssts[i])
		}
	}
}

func TestCompactionSplitsRunByTargetSize(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openRangeTestDB(t, store, 2000)
	defer db.Close()

	putRange(t, db, 0, 200, "a")
	putRange(t, db, 200, 400, "b")
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	run := firstRun(t, db)
	if len(run) < 4 {
		t.Fatalf("expected the run to be split into several sstables, got %d", len(run))
	}
	assertNonOverlapping(t, run)
	names := map[string]bool{}
	for _, sst := range run {
		if names[sst.Path] {
			t.Fatalf("duplicate path %s", sst.Path)
		}
		names[sst.Path] = true
		if !strings.Contains(sst.Path, "sorted-L1-") {
			t.Fatalf("unexpected path %s", sst.Path)
		}
		if sst.Size > 4000 {
			t.Fatalf("sstable %s far over the target: %d", sst.Path, sst.Size)
		}
	}
	if n := len(db.writer.CurrentManifest().L0); n != 0 {
		t.Fatalf("L0 not empty: %d", n)
	}

	res, err := db.ScanRange(ctx, "", 1000)
	if err != nil || len(res.Entries) != 400 {
		t.Fatalf("ScanRange: err=%v n=%d", err, len(res.Entries))
	}
}

func TestCompactionRewritesOnlyOverlappingSSTables(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openRangeTestDB(t, store, 2000)
	defer db.Close()

	putRange(t, db, 0, 200, "a")
	putRange(t, db, 200, 400, "b")
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	before := firstRun(t, db)

	// Overwrite and delete inside a narrow range.
	putRange(t, db, 100, 110, "c")
	if _, err := db.Delete(ctx, "key-00105"); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := firstRun(t, db)
	assertNonOverlapping(t, after)

	beforePaths := map[string]SSTInfo{}
	for _, sst := range before {
		beforePaths[sst.Path] = sst
	}
	kept, replaced := 0, 0
	afterPaths := map[string]bool{}
	for _, sst := range after {
		afterPaths[sst.Path] = true
		if _, ok := beforePaths[sst.Path]; ok {
			kept++
		}
	}
	for _, sst := range before {
		if !afterPaths[sst.Path] {
			replaced++
			if !overlapsRange(sst, "key-00100", "key-00109") {
				t.Fatalf("rewrote non-overlapping sstable %+v", sst)
			}
		} else if overlapsRange(sst, "key-00100", "key-00109") {
			t.Fatalf("kept an overlapping sstable %+v", sst)
		}
	}
	if replaced == 0 || kept == 0 || kept < len(before)-3 {
		t.Fatalf("expected only a few sstables replaced: kept=%d replaced=%d of %d", kept, replaced, len(before))
	}

}

func TestCompactionGCOnlyReplacedSSTables(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openRangeTestDB(t, store, 2000)
	defer db.Close()
	now := time.Unix(1000, 0)
	db.compactor.now = func() time.Time { return now }

	putRange(t, db, 0, 200, "a")
	putRange(t, db, 200, 400, "b")
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	before := firstRun(t, db)
	putRange(t, db, 100, 110, "c")
	putRange(t, db, 105, 106, "d")
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after := map[string]bool{}
	for _, sst := range firstRun(t, db) {
		after[sst.Path] = true
	}

	now = now.Add(DefaultGCGracePeriod + time.Second)
	db.compactor.sweep(ctx)
	for _, sst := range before {
		exists, _ := store.Exists(ctx, sst.Path)
		if exists != after[sst.Path] {
			t.Fatalf("%s: exists=%v but referenced=%v", sst.Path, exists, after[sst.Path])
		}
	}
	res, err := db.ScanPrefix(ctx, "key-", 0)
	if err != nil || len(res) != 400 {
		t.Fatalf("ScanPrefix: err=%v n=%d", err, len(res))
	}
}

// TestRangeRunReadsAfterManyRounds checks every read path against a model
// across compaction rounds with overwrites and deletes.
func TestRangeRunReadsAfterManyRounds(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openRangeTestDB(t, store, 1500)
	defer db.Close()

	rng := rand.New(rand.NewSource(1))
	model := map[string]string{}
	const keySpace = 300
	for round := 0; round < 8; round++ {
		lo := rng.Intn(keySpace)
		for n := 0; n < 3; n++ {
			for j := 0; j < 40; j++ {
				k := fmt.Sprintf("key-%05d", (lo+rng.Intn(60))%keySpace)
				if rng.Intn(4) == 0 {
					if _, err := db.Delete(ctx, k); err != nil {
						t.Fatal(err)
					}
					delete(model, k)
				} else {
					v := fmt.Sprintf("r%d-%d-padding-padding", round, j)
					if _, err := db.Put(ctx, k, []byte(v)); err != nil {
						t.Fatal(err)
					}
					model[k] = v
				}
			}
			if err := db.Flush(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Compact(ctx); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		assertNonOverlapping(t, firstRun(t, db))
		verifyAgainstModel(t, db, model, keySpace)
	}
	if len(firstRun(t, db)) < 2 {
		t.Fatal("expected a multi-sstable run")
	}

	// Reopen: the manifest is the only state.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2 := openRangeTestDB(t, store, 1500)
	defer db2.Close()
	verifyAgainstModel(t, db2, model, keySpace)
}

func verifyAgainstModel(t *testing.T, db *DB, model map[string]string, keySpace int) {
	t.Helper()
	ctx := context.Background()
	var want []string
	for k := range model {
		want = append(want, k)
	}
	sort.Strings(want)

	for i := 0; i < keySpace; i++ {
		k := fmt.Sprintf("key-%05d", i)
		e, ok, err := db.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		v, inModel := model[k]
		if inModel {
			if !ok || e.Value == nil || string(e.Value) != v {
				t.Fatalf("Get(%s) = %v %v, want %q", k, e, ok, v)
			}
		} else if ok && e.Value != nil {
			t.Fatalf("Get(%s) = %q, want deleted", k, e.Value)
		}
	}

	scan, err := db.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := keys(scan); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Scan keys mismatch: got %d want %d", len(got), len(want))
	}

	// ScanRange pages.
	var paged []string
	after := ""
	for {
		res, err := db.ScanRange(ctx, after, 17)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range res.Entries {
			paged = append(paged, e.Key)
		}
		if !res.HasMore {
			break
		}
		after = res.Entries[len(res.Entries)-1].Key
	}
	if strings.Join(paged, ",") != strings.Join(want, ",") {
		t.Fatalf("ScanRange mismatch: got %d want %d", len(paged), len(want))
	}

	// ScanPrefix.
	for _, prefix := range []string{"key-000", "key-001", "key-0021", "key-00299", "nokey"} {
		var wantP []string
		for _, k := range want {
			if strings.HasPrefix(k, prefix) {
				wantP = append(wantP, k)
			}
		}
		res, err := db.ScanPrefix(ctx, prefix, 0)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(keys(res), ",") != strings.Join(wantP, ",") {
			t.Fatalf("ScanPrefix(%s) mismatch: got %d want %d", prefix, len(res), len(wantP))
		}
	}

	// ScanSince(0) returns the winning version of every key, live or deleted.
	since, err := db.ScanSince(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	live := 0
	seen := map[string]bool{}
	for _, e := range since {
		if seen[e.Key] {
			t.Fatalf("ScanSince returned %s twice", e.Key)
		}
		seen[e.Key] = true
		if e.Value != nil {
			live++
			if model[e.Key] != string(e.Value) {
				t.Fatalf("ScanSince %s = %q, want %q", e.Key, e.Value, model[e.Key])
			}
		} else if _, ok := model[e.Key]; ok {
			t.Fatalf("ScanSince %s is a tombstone but key is live", e.Key)
		}
	}
	if live != len(model) {
		t.Fatalf("ScanSince live = %d, want %d", live, len(model))
	}
}

// TestRangeRunKeepsTombstones: tombstones survive compaction into the run,
// as they always have, so they still shadow older runs and reach ScanSince.
func TestRangeRunKeepsTombstones(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openRangeTestDB(t, store, 1000)
	defer db.Close()

	putRange(t, db, 0, 100, "a")
	for i := 0; i < 100; i += 2 {
		db.Delete(ctx, fmt.Sprintf("key-%05d", i))
	}
	db.Flush(ctx)
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	m := db.writer.CurrentManifest()
	var tombstones, live int
	for _, sst := range m.SortedRuns[0].SSTables {
		r, err := NewSSTableReader(ctx, store, sst, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		it := r.Iterator()
		for it.Next() {
			if it.Entry().Value == nil {
				tombstones++
			} else {
				live++
			}
		}
	}
	if tombstones != 50 || live != 50 {
		t.Fatalf("tombstones=%d live=%d, want 50/50", tombstones, live)
	}
}

// TestRangeRunSplitsLegacySingleSSTable: a run holding one big SSTable (the
// old layout) keeps working and is split when it is next rewritten.
func TestRangeRunSplitsLegacySingleSSTable(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openRangeTestDB(t, store, 1<<30)
	putRange(t, db, 0, 300, "a")
	putRange(t, db, 300, 600, "b")
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(firstRun(t, db)); n != 1 {
		t.Fatalf("expected one big sstable, got %d", n)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openRangeTestDB(t, store, 2000)
	defer db.Close()
	putRange(t, db, 10, 20, "c")
	putRange(t, db, 15, 25, "d")
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	run := firstRun(t, db)
	if len(run) < 4 {
		t.Fatalf("expected the legacy sstable to be split, got %d", len(run))
	}
	assertNonOverlapping(t, run)
	res, err := db.ScanPrefix(ctx, "key-", 0)
	if err != nil || len(res) != 600 {
		t.Fatalf("ScanPrefix: err=%v n=%d", err, len(res))
	}
}

func TestOrphanScanRecognizesRangeRunNames(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	db := openTestDB(t, store, DBConfig{})
	defer db.Close()
	db.Put(ctx, "k", []byte("v"))
	db.Flush(ctx)
	m := db.writer.CurrentManifest()

	orphan := "test/sorted/sorted-L1-00000000000000000001-000003.sst"
	inFlight := fmt.Sprintf("test/sorted/sorted-L1-%020d-000002.sst", m.Version+1)
	badSuffix := "test/sorted/sorted-L1-00000000000000000001-xyz.sst"
	for _, k := range []string{orphan, inFlight, badSuffix} {
		store.Put(ctx, k, []byte("x"), false)
	}
	now := time.Unix(1000, 0)
	db.compactor.now = func() time.Time { return now }
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(DefaultGCGracePeriod + time.Second)
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]bool{orphan: false, inFlight: true, badSuffix: true} {
		if exists, _ := store.Exists(ctx, k); exists != want {
			t.Fatalf("%s: exists=%v want %v", k, exists, want)
		}
	}
}
