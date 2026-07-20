package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	moraine "github.com/russellhaering/moraine"
)

// EntryIndexer applies Moraine entries to a derived index.
type EntryIndexer interface {
	ResetIndexes(ctx context.Context) error
	IndexEntry(ctx context.Context, entry moraine.Entry) error
}

// Builder rebuilds or catches up derived indexes from a Moraine DB.
type Builder struct {
	db       *moraine.DB
	target   EntryIndexer
	cacheDir string
	name     string
}

// BuilderConfig configures a Builder.
type BuilderConfig struct {
	DB       *moraine.DB
	Target   EntryIndexer
	CacheDir string
	Name     string
}

// NewBuilder creates a checkpointed index builder.
func NewBuilder(cfg BuilderConfig) *Builder {
	return &Builder{
		db:       cfg.DB,
		target:   cfg.Target,
		cacheDir: cfg.CacheDir,
		name:     cfg.Name,
	}
}

// CatchUp validates the checkpoint and incrementally indexes entries since it.
// It performs a full rebuild if there is no checkpoint or the checkpoint is
// ahead of the store.
func (b *Builder) CatchUp(ctx context.Context) (uint64, error) {
	if err := b.validate(); err != nil {
		return 0, err
	}

	last, err := LoadCheckpoint(b.cacheDir, b.name)
	if err != nil {
		last = 0
	}

	maxSeq, err := b.maxSeqInStore(ctx)
	if err != nil {
		return 0, err
	}
	if last == 0 || last > maxSeq {
		return b.Rebuild(ctx)
	}
	return b.indexSince(ctx, last)
}

// Rebuild recreates indexes from live records and writes a fresh checkpoint.
func (b *Builder) Rebuild(ctx context.Context) (uint64, error) {
	if err := b.validate(); err != nil {
		return 0, err
	}

	if err := b.target.ResetIndexes(ctx); err != nil {
		return 0, fmt.Errorf("index: reset: %w", err)
	}

	entries, err := b.db.Scan(ctx)
	if err != nil {
		return 0, fmt.Errorf("index: scan: %w", err)
	}

	var maxSeq uint64
	for _, e := range entries {
		if err := b.target.IndexEntry(ctx, e); err != nil {
			return 0, err
		}
		if e.SeqNum > maxSeq {
			maxSeq = e.SeqNum
		}
	}

	if err := SaveCheckpoint(b.cacheDir, b.name, maxSeq); err != nil {
		return 0, err
	}
	return maxSeq, nil
}

func (b *Builder) validate() error {
	if b == nil {
		return errors.New("index: nil builder")
	}
	if err := ValidateName(b.name); err != nil {
		return err
	}
	if b.db == nil {
		return errors.New("index: DB is required")
	}
	if b.target == nil {
		return errors.New("index: target is required")
	}
	return nil
}

func (b *Builder) indexSince(ctx context.Context, since uint64) (uint64, error) {
	entries, err := b.db.ScanSince(ctx, since)
	if err != nil {
		return since, fmt.Errorf("index: scan since %d: %w", since, err)
	}

	last := since
	for _, e := range entries {
		if err := b.target.IndexEntry(ctx, e); err != nil {
			return last, err
		}
		if e.SeqNum > last {
			last = e.SeqNum
		}
	}

	if err := SaveCheckpoint(b.cacheDir, b.name, last); err != nil {
		return last, err
	}
	return last, nil
}

func (b *Builder) maxSeqInStore(ctx context.Context) (uint64, error) {
	entries, err := b.db.ScanSince(ctx, 0)
	if err != nil {
		return 0, fmt.Errorf("index: scan max seq: %w", err)
	}

	var maxSeq uint64
	for _, e := range entries {
		if e.SeqNum > maxSeq {
			maxSeq = e.SeqNum
		}
	}
	return maxSeq, nil
}

type checkpoint struct {
	LastSeqNum uint64 `json:"last_seq_num"`
}

// SaveCheckpoint stores the last fully indexed sequence number.
func SaveCheckpoint(cacheDir, name string, lastSeq uint64) error {
	if cacheDir == "" || name == "" {
		return nil
	}
	if err := ValidateName(name); err != nil {
		return err
	}

	path := CheckpointPath(cacheDir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("index: checkpoint mkdir: %w", err)
	}

	data, err := json.Marshal(checkpoint{LastSeqNum: lastSeq})
	if err != nil {
		return fmt.Errorf("index: checkpoint marshal: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("index: checkpoint write: %w", err)
	}
	return nil
}

// LoadCheckpoint loads the last fully indexed sequence number.
func LoadCheckpoint(cacheDir, name string) (uint64, error) {
	if cacheDir == "" || name == "" {
		return 0, nil
	}
	if err := ValidateName(name); err != nil {
		return 0, err
	}

	data, err := os.ReadFile(CheckpointPath(cacheDir, name))
	if err != nil {
		return 0, err
	}

	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return 0, err
	}
	return cp.LastSeqNum, nil
}

// DeleteCheckpoint removes a checkpoint, forcing a full rebuild on next catch-up.
func DeleteCheckpoint(cacheDir, name string) error {
	if cacheDir == "" || name == "" {
		return nil
	}
	if err := ValidateName(name); err != nil {
		return err
	}
	if err := os.Remove(CheckpointPath(cacheDir, name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// CheckpointPath returns the checkpoint path for an index name.
func CheckpointPath(cacheDir, name string) string {
	return filepath.Join(cacheDir, "checkpoints", name+".json")
}
