package moraine_test

import (
	"context"
	"testing"
	"time"

	"github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/objstore"
)

func TestExternalPackageOpenWithObjstorePersistsPublicBehavior(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemoryStore()
	cfg := moraine.DBConfig{
		Store:           store,
		Prefix:          "external-api",
		MemTableMaxSize: 1 << 20,
		L0CompactThresh: 10,
		CompactInterval: time.Hour,
	}

	db, err := moraine.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if _, err := db.Put(ctx, "alpha", []byte("one")); err != nil {
		t.Fatalf("Put alpha: %v", err)
	}
	if _, err := db.Put(ctx, "beta", []byte("two")); err != nil {
		t.Fatalf("Put beta: %v", err)
	}
	if _, err := db.Put(ctx, "gamma", []byte("three")); err != nil {
		t.Fatalf("Put gamma: %v", err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := moraine.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopened.Close()

	entry, ok, err := reopened.Get(ctx, "alpha")
	if err != nil {
		t.Fatalf("Get alpha after reopen: %v", err)
	}
	if !ok || string(entry.Value) != "one" {
		t.Fatalf("Get alpha after reopen = (%v, %v), want (one, true)", entry, ok)
	}

	entry, ok, err = reopened.Get(ctx, "beta")
	if err != nil {
		t.Fatalf("Get beta after reopen: %v", err)
	}
	if !ok || string(entry.Value) != "two" {
		t.Fatalf("Get beta after reopen = (%v, %v), want (two, true)", entry, ok)
	}

	firstPage, err := reopened.ScanRange(ctx, "", 2)
	if err != nil {
		t.Fatalf("ScanRange first page: %v", err)
	}
	if len(firstPage.Entries) != 2 ||
		firstPage.Entries[0].Key != "alpha" || string(firstPage.Entries[0].Value) != "one" ||
		firstPage.Entries[1].Key != "beta" || string(firstPage.Entries[1].Value) != "two" ||
		!firstPage.HasMore {
		t.Fatalf("ScanRange first page = %+v, want alpha/one and beta/two with HasMore", firstPage)
	}

	secondPage, err := reopened.ScanRange(ctx, "beta", 2)
	if err != nil {
		t.Fatalf("ScanRange second page: %v", err)
	}
	if len(secondPage.Entries) != 1 || secondPage.Entries[0].Key != "gamma" || string(secondPage.Entries[0].Value) != "three" || secondPage.HasMore {
		t.Fatalf("ScanRange second page = %+v, want gamma/three without HasMore", secondPage)
	}
}
