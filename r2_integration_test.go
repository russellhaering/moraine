package moraine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/objstore"
)

func TestR2Integration(t *testing.T) {
	store := newR2IntegrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("object store contract", func(t *testing.T) {
		const key = "object-store/key"
		const value = "hello from r2"

		if err := store.Put(ctx, key, []byte(value), true); err != nil {
			t.Fatalf("Put: %v", err)
		}
		err := store.Put(ctx, key, []byte("overwrite"), true)
		if !errors.Is(err, objstore.ErrPreconditionFailed) {
			t.Fatalf("conditional Put error = %v, want ErrPreconditionFailed", err)
		}

		got, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(got) != value {
			t.Fatalf("Get = %q, want %q", string(got), value)
		}

		got, err = store.GetRange(ctx, key, 6, 4)
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		if string(got) != "from" {
			t.Fatalf("GetRange = %q, want %q", string(got), "from")
		}

		meta, err := store.Head(ctx, key)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		if meta.Size != int64(len(value)) {
			t.Fatalf("Head size = %d, want %d", meta.Size, len(value))
		}

		keys, err := store.List(ctx, "object-store/")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !contains(keys, key) {
			t.Fatalf("List = %v, want %q", keys, key)
		}

		if err := store.Delete(ctx, key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		exists, err := store.Exists(ctx, key)
		if err != nil {
			t.Fatalf("Exists after delete: %v", err)
		}
		if exists {
			t.Fatalf("Exists after delete = true, want false")
		}
	})

	t.Run("database persists and compacts", func(t *testing.T) {
		cfg := moraine.DBConfig{
			Store:           store,
			Prefix:          "db",
			MemTableMaxSize: 1 << 20,
			BlockCacheSize:  1 << 20,
			L0CompactThresh: 2,
			CompactInterval: time.Hour,
		}

		db, err := moraine.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := []byte(fmt.Sprintf("value-%03d", i))
			if _, err := db.Put(ctx, key, value); err != nil {
				t.Fatalf("Put %s: %v", key, err)
			}
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("Flush first batch: %v", err)
		}

		for i := 10; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := []byte(fmt.Sprintf("value-%03d", i))
			if _, err := db.Put(ctx, key, value); err != nil {
				t.Fatalf("Put %s: %v", key, err)
			}
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("Flush second batch: %v", err)
		}
		if err := db.Compact(ctx); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		reopened, err := moraine.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("Reopen: %v", err)
		}
		defer reopened.Close()

		entry, ok, err := reopened.Get(ctx, "key-005")
		if err != nil {
			t.Fatalf("Get key-005: %v", err)
		}
		if !ok || string(entry.Value) != "value-005" {
			t.Fatalf("Get key-005 = (%v, %v), want value-005", entry, ok)
		}

		page, err := reopened.ScanRange(ctx, "key-017", 5)
		if err != nil {
			t.Fatalf("ScanRange: %v", err)
		}
		if len(page.Entries) != 2 || page.Entries[0].Key != "key-018" || page.Entries[1].Key != "key-019" || page.HasMore {
			t.Fatalf("ScanRange = %+v, want key-018/key-019 without more", page)
		}
	})
}

func newR2IntegrationStore(t *testing.T) *objstore.S3Store {
	t.Helper()

	requiredEnv := []string{
		"MORAINE_R2_ACCOUNT_ID",
		"MORAINE_R2_BUCKET",
		"MORAINE_R2_ACCESS_KEY_ID",
		"MORAINE_R2_SECRET_ACCESS_KEY",
	}
	var missing []string
	for _, key := range requiredEnv {
		if os.Getenv(key) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Skipf("set %s to run Cloudflare R2 integration tests", strings.Join(missing, ", "))
	}

	basePrefix := strings.Trim(os.Getenv("MORAINE_R2_PREFIX"), "/")
	if basePrefix == "" {
		basePrefix = "moraine-integration"
	}
	prefix := fmt.Sprintf("%s/%d", basePrefix, time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := objstore.NewR2Store(ctx, objstore.R2Config{
		AccountID:       os.Getenv("MORAINE_R2_ACCOUNT_ID"),
		Bucket:          os.Getenv("MORAINE_R2_BUCKET"),
		AccessKeyID:     os.Getenv("MORAINE_R2_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("MORAINE_R2_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("MORAINE_R2_SESSION_TOKEN"),
		Prefix:          prefix,
		Jurisdiction:    os.Getenv("MORAINE_R2_JURISDICTION"),
		Endpoint:        os.Getenv("MORAINE_R2_ENDPOINT"),
	})
	if err != nil {
		t.Fatalf("NewR2Store: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		keys, err := store.List(ctx, "")
		if err != nil {
			t.Logf("cleanup List: %v", err)
			return
		}
		for _, key := range keys {
			if err := store.Delete(ctx, key); err != nil {
				t.Logf("cleanup Delete %q: %v", key, err)
			}
		}
	})

	return store
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
