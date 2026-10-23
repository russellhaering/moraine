package moraine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/russellhaering/moraine/objstore"
)

// lockedBuffer is a bytes.Buffer safe for concurrent log writes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(l.b.String()), "\n") {
		var r map[string]any
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

// captureLogs sends the default slog logger to a JSON buffer for the test.
func captureLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func TestFlushAndCompactionLogDuration(t *testing.T) {
	logs := captureLogs(t)
	ctx := context.Background()
	db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{L0CompactThresh: 2})
	for _, k := range []string{"a", "b"} {
		if _, err := db.Put(ctx, k, []byte("value-"+k)); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	for msg, fields := range map[string][]string{
		"writer flushed":      {"wal_id", "entries", "manifest_version", "bytes", "duration"},
		"compaction complete": {"merged_entries", "output_sstables", "manifest_version", "bytes_written", "duration"},
	} {
		recs := logs.records(t, msg)
		if len(recs) == 0 {
			t.Fatalf("no %q log line", msg)
		}
		for _, f := range fields {
			if _, ok := recs[0][f]; !ok {
				t.Errorf("%q has no %s field: %v", msg, f, recs[0])
			}
		}
		if d, _ := recs[0]["duration"].(float64); d <= 0 {
			t.Errorf("%q duration = %v, want > 0 (nanoseconds)", msg, recs[0]["duration"])
		}
	}
}

func TestL0CompactThreshHonored(t *testing.T) {
	ctx := context.Background()
	for _, thresh := range []int{0, 3, 6} {
		want := thresh
		if want == 0 {
			want = 4 // the default
		}
		db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{L0CompactThresh: thresh})
		for i := 1; i <= want; i++ {
			if _, err := db.Put(ctx, string(rune('a'+i)), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := db.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := db.Compact(ctx); err != nil {
				t.Fatal(err)
			}
			l0 := len(db.writer.CurrentManifest().L0)
			if i < want && l0 != i {
				t.Fatalf("thresh %d: after %d flushes L0 = %d, want %d (no compaction yet)", thresh, i, l0, i)
			}
			if i == want && l0 != 0 {
				t.Fatalf("thresh %d: after %d flushes L0 = %d, want 0 (compacted)", thresh, i, l0)
			}
		}
	}
}

func TestUnflushedBytes(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, objstore.NewMemoryStore(), DBConfig{})
	if n := db.UnflushedBytes(); n != 0 {
		t.Fatalf("empty db: UnflushedBytes = %d", n)
	}
	if _, err := db.Put(ctx, "k", make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if n := db.UnflushedBytes(); n < 1000 {
		t.Fatalf("after a 1000-byte put: UnflushedBytes = %d", n)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := db.UnflushedBytes(); n != 0 {
		t.Fatalf("after flush: UnflushedBytes = %d", n)
	}
}
