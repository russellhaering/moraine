// Package indexed provides a document database layer on top of Moraine.
package indexed

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	moraine "github.com/russellhaering/moraine"
	"github.com/russellhaering/moraine/document"
	"github.com/russellhaering/moraine/index"
)

var (
	// ErrClosed is returned when a table operation is attempted after Close.
	ErrClosed = errors.New("indexed: table closed")

	// ErrFullTextUnavailable is returned when full-text search is not configured.
	ErrFullTextUnavailable = errors.New("indexed: full-text search unavailable")

	// ErrVectorUnavailable is returned when vector search is not configured.
	ErrVectorUnavailable = errors.New("indexed: vector search unavailable")

	// ErrIndexNotReady is returned when a search is attempted before the
	// startup rebuild has completed.
	ErrIndexNotReady = errors.New("indexed: indexes not ready")

	// ErrIndexDegraded wraps the first indexing error observed by the table.
	ErrIndexDegraded = errors.New("indexed: index degraded")
)

// Embedder turns text into embedding vectors for vector search.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// TableConfig configures an indexed table.
type TableConfig struct {
	Name           string
	System         bool
	Schema         *document.Schema
	DB             *moraine.DB
	CacheDir       string
	Embedder       Embedder
	IndexQueueSize int
}

// IndexStatus describes current derived-index progress.
type IndexStatus struct {
	Ready          bool
	LastIndexedSeq uint64
	AcceptedSeq    uint64
	Err            error
}

// Table ties together Moraine storage and derived indexes for one document table.
type Table struct {
	name   string
	system bool
	schema *document.Schema

	db       *moraine.DB
	cacheDir string
	embedder Embedder

	schemaMu sync.RWMutex

	idxMu  sync.RWMutex
	bleve  *index.BleveIndex
	vector *index.VectorIndex
	attrs  *index.AttributeIndex

	writeMu sync.Mutex

	indexCh chan indexOp
	indexWg sync.WaitGroup

	closeMu sync.RWMutex
	closed  bool

	acceptedSeq atomic.Uint64
	statusMu    sync.RWMutex
	startupDone bool
	indexErr    error
	lastIndexed uint64

	closeOnce sync.Once
}

type indexOpKind int

const (
	indexOpCatchUp indexOpKind = iota + 1
	indexOpRebuild
	indexOpPut
	indexOpDelete
)

type indexOp struct {
	kind indexOpKind
	doc  *document.Document
	id   string
	seq  uint64
	ctx  context.Context
	done chan error
}

// OpenTable opens an indexed document table backed by an existing Moraine DB.
func OpenTable(ctx context.Context, cfg TableConfig) (*Table, error) {
	return NewTable(ctx, cfg)
}

// NewTable opens an indexed document table backed by an existing Moraine DB.
func NewTable(_ context.Context, cfg TableConfig) (*Table, error) {
	if cfg.Name == "" {
		return nil, errors.New("indexed: table name is required")
	}
	if err := index.ValidateName(cfg.Name); err != nil {
		return nil, err
	}
	if cfg.DB == nil {
		return nil, errors.New("indexed: DB is required")
	}

	attrs := index.NewAttributeIndex()

	schema := cfg.Schema.Clone()

	var bleveIdx *index.BleveIndex
	if cfg.CacheDir != "" {
		var err error
		bleveIdx, err = index.NewBleveIndex(cfg.CacheDir, cfg.Name, schema)
		if err != nil {
			return nil, err
		}
	}

	var vectorIdx *index.VectorIndex
	if schema != nil && schema.EmbeddingDimensions > 0 {
		vectorIdx = index.LoadVectorIndex(cfg.CacheDir, cfg.Name, schema.EmbeddingDimensions)
		if vectorIdx == nil {
			vectorIdx = index.NewVectorIndex(schema.EmbeddingDimensions)
		}
	}

	queueSize := cfg.IndexQueueSize
	if queueSize <= 0 {
		queueSize = 1024
	}

	t := &Table{
		name:     cfg.Name,
		system:   cfg.System,
		schema:   schema,
		db:       cfg.DB,
		cacheDir: cfg.CacheDir,
		embedder: cfg.Embedder,
		bleve:    bleveIdx,
		vector:   vectorIdx,
		attrs:    attrs,
		indexCh:  make(chan indexOp, queueSize),
	}

	// Attribute indexes are in-memory, so table startup must rebuild from the
	// Moraine source of truth even when persisted text/vector indexes exist.
	t.indexCh <- indexOp{kind: indexOpRebuild, ctx: context.Background()}
	t.indexWg.Add(1)
	go t.indexWorker()

	return t, nil
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.name
}

// System reports whether the table is marked as a system table.
func (t *Table) System() bool {
	return t.system
}

// Schema returns a snapshot of the table schema.
func (t *Table) Schema() *document.Schema {
	return t.schemaSnapshot()
}

// IndexStatus returns current index readiness and progress.
func (t *Table) IndexStatus() IndexStatus {
	ready, lastIndexed, err := t.indexStatus()
	return IndexStatus{
		Ready:          ready && err == nil,
		LastIndexedSeq: lastIndexed,
		AcceptedSeq:    t.acceptedSeq.Load(),
		Err:            err,
	}
}

// PutDocument validates, serializes, stores, and asynchronously indexes a document.
func (t *Table) PutDocument(ctx context.Context, doc *document.Document) error {
	if doc == nil {
		return errors.New("indexed: nil document")
	}
	if err := t.beginOp(); err != nil {
		return err
	}
	defer t.endOp()

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	prepared, err := t.prepareDocument(ctx, doc)
	if err != nil {
		return err
	}

	data, err := document.Serialize(prepared)
	if err != nil {
		return fmt.Errorf("serialize: %w", err)
	}

	seq, err := t.db.Put(ctx, prepared.ID, data)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	if err := t.db.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if err := t.enqueueIndexOpLocked(context.Background(), indexOp{
		kind: indexOpPut,
		doc:  prepared.Clone(),
		seq:  seq,
		ctx:  context.Background(),
	}); err != nil {
		return err
	}
	recordAccepted(&t.acceptedSeq, seq)

	*doc = *prepared.Clone()
	return nil
}

// PutDocumentsBulk stores multiple documents with one Moraine flush.
func (t *Table) PutDocumentsBulk(ctx context.Context, docs []*document.Document) error {
	if err := t.beginOp(); err != nil {
		return err
	}
	defer t.endOp()

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	prepared := make([]*document.Document, 0, len(docs))
	type pendingPut struct {
		doc  *document.Document
		data []byte
	}
	puts := make([]pendingPut, 0, len(docs))

	for i, doc := range docs {
		if doc == nil {
			return fmt.Errorf("doc %d: nil document", i)
		}
		cp, err := t.prepareDocument(ctx, doc)
		if err != nil {
			return fmt.Errorf("doc %d: %w", i, err)
		}
		data, err := document.Serialize(cp)
		if err != nil {
			return fmt.Errorf("serialize doc %q: %w", cp.ID, err)
		}
		prepared = append(prepared, cp)
		puts = append(puts, pendingPut{doc: cp, data: data})
	}

	seqs := make([]uint64, len(puts))
	for i, put := range puts {
		seq, err := t.db.Put(ctx, put.doc.ID, put.data)
		if err != nil {
			return fmt.Errorf("put doc %q: %w", put.doc.ID, err)
		}
		seqs[i] = seq
	}
	if err := t.db.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	var maxSeq uint64
	for i, put := range puts {
		if err := t.enqueueIndexOpLocked(context.Background(), indexOp{
			kind: indexOpPut,
			doc:  put.doc.Clone(),
			seq:  seqs[i],
			ctx:  context.Background(),
		}); err != nil {
			return err
		}
		if seqs[i] > maxSeq {
			maxSeq = seqs[i]
		}
	}
	recordAccepted(&t.acceptedSeq, maxSeq)

	for i, doc := range docs {
		*doc = *prepared[i].Clone()
	}
	return nil
}

// GetDocument retrieves a document by ID.
func (t *Table) GetDocument(ctx context.Context, id string) (*document.Document, error) {
	if err := t.beginOp(); err != nil {
		return nil, err
	}
	defer t.endOp()

	return t.getDocumentLocked(ctx, id)
}

func (t *Table) getDocumentLocked(ctx context.Context, id string) (*document.Document, error) {
	entry, ok, err := t.db.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	if !ok || entry.Value == nil || document.IsTombstone(entry.Value) {
		return nil, nil
	}

	doc, err := document.Deserialize(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("deserialize: %w", err)
	}
	doc.ID = id
	return doc, nil
}

// DeleteDocument deletes a document by ID and asynchronously removes it from indexes.
func (t *Table) DeleteDocument(ctx context.Context, id string) error {
	if err := t.beginOp(); err != nil {
		return err
	}
	defer t.endOp()

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	seq, err := t.db.Delete(ctx, id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if err := t.db.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if err := t.enqueueIndexOpLocked(context.Background(), indexOp{
		kind: indexOpDelete,
		id:   id,
		seq:  seq,
		ctx:  context.Background(),
	}); err != nil {
		return err
	}
	recordAccepted(&t.acceptedSeq, seq)
	return nil
}

// ListDocuments returns up to limit documents with key > afterKey.
func (t *Table) ListDocuments(ctx context.Context, limit int, afterKey string) ([]*document.Document, bool, error) {
	if err := t.beginOp(); err != nil {
		return nil, false, err
	}
	defer t.endOp()

	result, err := t.db.ScanRange(ctx, afterKey, limit)
	if err != nil {
		return nil, false, fmt.Errorf("scan range: %w", err)
	}

	docs := make([]*document.Document, 0, len(result.Entries))
	for _, entry := range result.Entries {
		if entry.Value == nil || document.IsTombstone(entry.Value) {
			continue
		}
		doc, err := document.Deserialize(entry.Value)
		if err != nil {
			return nil, false, fmt.Errorf("deserialize %q: %w", entry.Key, err)
		}
		doc.ID = entry.Key
		docs = append(docs, doc)
	}
	return docs, result.HasMore, nil
}

// SearchAttributes performs an attribute filter search.
func (t *Table) SearchAttributes(ctx context.Context, filters []index.Filter, limit, offset int) ([]*document.Document, error) {
	if err := t.beginOp(); err != nil {
		return nil, err
	}
	defer t.endOp()

	if err := index.ValidateFilters(filters); err != nil {
		return nil, err
	}

	if err := t.currentIndexError(); err != nil {
		return nil, err
	}

	t.idxMu.RLock()
	ids := t.attrs.Search(filters)
	t.idxMu.RUnlock()

	ids = paginateIDs(ids, limit, offset)
	return t.fetchDocs(ctx, ids)
}

// SearchText performs a full-text search.
func (t *Table) SearchText(ctx context.Context, query string, limit, offset int) ([]*document.Document, int, error) {
	if err := t.beginOp(); err != nil {
		return nil, 0, err
	}
	defer t.endOp()

	if err := t.currentIndexError(); err != nil {
		return nil, 0, err
	}

	t.idxMu.RLock()
	defer t.idxMu.RUnlock()

	if t.bleve == nil {
		return nil, 0, ErrFullTextUnavailable
	}

	results, total, err := t.bleve.Search(query, limit, offset)
	if err != nil {
		return nil, 0, err
	}

	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.DocID
	}

	docs, err := t.fetchDocs(ctx, ids)
	return docs, total, err
}

// SearchVector performs a vector similarity search.
func (t *Table) SearchVector(ctx context.Context, query []float32, k int) ([]*document.Document, error) {
	if err := t.beginOp(); err != nil {
		return nil, err
	}
	defer t.endOp()

	return t.searchVectorLocked(ctx, query, k)
}

func (t *Table) searchVectorLocked(ctx context.Context, query []float32, k int) ([]*document.Document, error) {
	if err := t.currentIndexError(); err != nil {
		return nil, err
	}

	t.idxMu.RLock()
	defer t.idxMu.RUnlock()

	if t.vector == nil {
		return nil, ErrVectorUnavailable
	}

	results, err := t.vector.Search(query, k)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.DocID
	}
	return t.fetchDocs(ctx, ids)
}

// SearchVectorByText embeds queryText and performs vector search.
func (t *Table) SearchVectorByText(ctx context.Context, queryText string, k int) ([]*document.Document, error) {
	if err := t.beginOp(); err != nil {
		return nil, err
	}
	defer t.endOp()

	if t.embedder == nil {
		return nil, errors.New("indexed: embedder not configured")
	}

	queryVec, err := t.embedder.Embed(ctx, queryText)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	return t.searchVectorLocked(ctx, queryVec, k)
}

// WaitForIndexes blocks until indexes include all writes accepted before the call.
func (t *Table) WaitForIndexes(ctx context.Context) error {
	if t.isClosed() {
		return ErrClosed
	}

	target := t.acceptedSeq.Load()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if t.isClosed() {
			return ErrClosed
		}
		startupDone, lastIndexed, indexErr := t.indexStatus()
		if indexErr != nil {
			return indexErr
		}
		if startupDone && lastIndexed >= target {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RebuildIndexes forces a full rebuild from Moraine and waits for it to finish.
func (t *Table) RebuildIndexes(ctx context.Context) error {
	if err := t.beginOp(); err != nil {
		return err
	}
	defer t.endOp()
	return t.rebuildIndexesLocked(ctx)
}

func (t *Table) rebuildIndexesLocked(ctx context.Context) error {
	done := make(chan error, 1)
	if err := t.enqueueIndexOpLocked(context.Background(), indexOp{
		kind: indexOpRebuild,
		ctx:  ctx,
		done: done,
	}); err != nil {
		return err
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetSchema updates the table schema and rebuilds derived indexes.
func (t *Table) SetSchema(ctx context.Context, schema *document.Schema) error {
	if err := t.beginOp(); err != nil {
		return err
	}
	defer t.endOp()

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.schemaMu.Lock()
	t.schema = schema.Clone()
	t.schemaMu.Unlock()

	return t.rebuildIndexesLocked(ctx)
}

// Close shuts down indexing, saves derived index state, and closes the Moraine DB.
func (t *Table) Close() error {
	var err error
	t.closeOnce.Do(func() {
		t.closeMu.Lock()
		t.closed = true
		close(t.indexCh)
		t.closeMu.Unlock()

		t.indexWg.Wait()

		t.idxMu.Lock()
		if t.vector != nil {
			if saveErr := t.vector.Save(t.cacheDir, t.name); saveErr != nil && err == nil {
				err = saveErr
			}
		}
		if t.bleve != nil {
			if closeErr := t.bleve.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
		t.idxMu.Unlock()

		if closeErr := t.db.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	})
	return err
}

// ResetIndexes recreates all derived indexes. It implements index.EntryIndexer.
func (t *Table) ResetIndexes(ctx context.Context) error {
	schema := t.schemaSnapshot()

	t.idxMu.Lock()
	defer t.idxMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if t.bleve != nil {
		path := t.bleve.Path()
		if err := t.bleve.Close(); err != nil {
			return err
		}
		if t.cacheDir != "" {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
		}
		t.bleve = nil
	}

	if t.cacheDir != "" {
		bleveIdx, err := index.NewBleveIndex(t.cacheDir, t.name, schema)
		if err != nil {
			return err
		}
		t.bleve = bleveIdx
	}

	t.attrs = index.NewAttributeIndex()

	if t.cacheDir != "" {
		if err := os.Remove(filepath.Join(t.cacheDir, "hnsw", t.name+".hnsw")); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Remove(filepath.Join(t.cacheDir, "hnsw", t.name+".json")); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	t.vector = nil
	if schema != nil && schema.EmbeddingDimensions > 0 {
		t.vector = index.NewVectorIndex(schema.EmbeddingDimensions)
	}

	return nil
}

// IndexEntry applies a Moraine entry to derived indexes. It implements index.EntryIndexer.
func (t *Table) IndexEntry(ctx context.Context, entry moraine.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.Value == nil || document.IsTombstone(entry.Value) {
		return t.applyDelete(entry.Key)
	}

	doc, err := document.Deserialize(entry.Value)
	if err != nil {
		return fmt.Errorf("deserialize %q: %w", entry.Key, err)
	}
	doc.ID = entry.Key
	return t.applyDoc(doc)
}

func (t *Table) prepareDocument(ctx context.Context, doc *document.Document) (*document.Document, error) {
	schema := t.schemaSnapshot()
	if schema != nil {
		if err := schema.Validate(doc.Attributes); err != nil {
			return nil, fmt.Errorf("validation: %w", err)
		}
	}

	prepared := doc.Clone()
	now := time.Now().UTC()

	isNew := prepared.ID == ""
	if isNew {
		prepared.ID = ulid.Make().String()
	}

	if !isNew {
		existing, err := t.getDocumentLocked(ctx, prepared.ID)
		if err != nil {
			return nil, fmt.Errorf("check existing: %w", err)
		}
		if existing != nil {
			prepared.CreatedAt = existing.CreatedAt
			prepared.Version = existing.Version + 1
		}
	}

	if prepared.CreatedAt.IsZero() {
		prepared.CreatedAt = now
	}
	prepared.UpdatedAt = now
	if prepared.Version == 0 {
		prepared.Version = 1
	}

	if t.embedder != nil && schema != nil && schema.EmbeddingModel != "" {
		text := buildEmbeddingText(prepared)
		if text != "" {
			emb, err := t.embedder.Embed(ctx, text)
			if err != nil {
				return nil, fmt.Errorf("embedding: %w", err)
			}
			prepared.Embedding = emb
			prepared.EmbeddingModel = schema.EmbeddingModel
		}
	}

	if err := validateEmbeddingDimensions(schema, prepared); err != nil {
		return nil, err
	}

	return prepared, nil
}

func (t *Table) beginOp() error {
	t.closeMu.RLock()
	if t.closed {
		t.closeMu.RUnlock()
		return ErrClosed
	}
	return nil
}

func (t *Table) endOp() {
	t.closeMu.RUnlock()
}

func (t *Table) isClosed() bool {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	return t.closed
}

func (t *Table) schemaSnapshot() *document.Schema {
	t.schemaMu.RLock()
	defer t.schemaMu.RUnlock()
	return t.schema.Clone()
}

func (t *Table) enqueueIndexOpLocked(ctx context.Context, op indexOp) error {
	select {
	case t.indexCh <- op:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Table) indexWorker() {
	defer t.indexWg.Done()

	for op := range t.indexCh {
		err := t.processIndexOp(op)
		if op.done != nil {
			op.done <- err
		}
	}
}

func (t *Table) processIndexOp(op indexOp) error {
	ctx := op.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	var last uint64
	switch op.kind {
	case indexOpCatchUp:
		last, err = index.NewBuilder(index.BuilderConfig{
			DB:       t.db,
			Target:   t,
			CacheDir: t.cacheDir,
			Name:     t.name,
		}).CatchUp(ctx)
		if err == nil {
			t.markStartupComplete(last)
			t.clearIndexError()
		}
	case indexOpRebuild:
		last, err = index.NewBuilder(index.BuilderConfig{
			DB:       t.db,
			Target:   t,
			CacheDir: t.cacheDir,
			Name:     t.name,
		}).Rebuild(ctx)
		if err == nil {
			t.markStartupComplete(last)
			t.clearIndexError()
		}
	case indexOpPut:
		err = t.applyDoc(op.doc)
		if err == nil {
			err = index.SaveCheckpoint(t.cacheDir, t.name, op.seq)
		}
		if err == nil {
			t.markIndexed(op.seq)
		}
	case indexOpDelete:
		err = t.applyDelete(op.id)
		if err == nil {
			err = index.SaveCheckpoint(t.cacheDir, t.name, op.seq)
		}
		if err == nil {
			t.markIndexed(op.seq)
		}
	default:
		err = fmt.Errorf("indexed: unknown index op %d", op.kind)
	}

	if err != nil {
		t.setIndexError(err)
	}
	return err
}

func (t *Table) applyDoc(doc *document.Document) error {
	if doc == nil {
		return errors.New("indexed: nil index document")
	}

	t.idxMu.RLock()
	defer t.idxMu.RUnlock()

	if t.bleve != nil {
		if err := t.bleve.IndexDocument(doc); err != nil {
			return fmt.Errorf("bleve index %q: %w", doc.ID, err)
		}
	}
	if t.vector != nil {
		if len(doc.Embedding) > 0 {
			if err := t.vector.Add(doc.ID, doc.Embedding); err != nil {
				return fmt.Errorf("vector index %q: %w", doc.ID, err)
			}
		} else {
			t.vector.Delete(doc.ID)
		}
	}
	t.attrs.IndexDocument(doc.ID, indexedAttributes(t.schemaSnapshot(), doc.Attributes))
	return nil
}

func (t *Table) applyDelete(id string) error {
	t.idxMu.RLock()
	defer t.idxMu.RUnlock()

	if t.bleve != nil {
		if err := t.bleve.DeleteDocument(id); err != nil {
			return fmt.Errorf("bleve delete %q: %w", id, err)
		}
	}
	if t.vector != nil {
		t.vector.Delete(id)
	}
	t.attrs.DeleteDocument(id)
	return nil
}

func (t *Table) fetchDocs(ctx context.Context, ids []string) ([]*document.Document, error) {
	docs := make([]*document.Document, 0, len(ids))
	for _, id := range ids {
		doc, err := t.getDocumentLocked(ctx, id)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs, nil
}

func (t *Table) indexStatus() (bool, uint64, error) {
	t.statusMu.RLock()
	defer t.statusMu.RUnlock()
	return t.startupDone, t.lastIndexed, t.indexErr
}

func (t *Table) currentIndexError() error {
	t.statusMu.RLock()
	defer t.statusMu.RUnlock()
	if !t.startupDone {
		return ErrIndexNotReady
	}
	return t.indexErr
}

func (t *Table) setIndexError(err error) {
	if err == nil {
		return
	}
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	if t.indexErr == nil {
		t.indexErr = fmt.Errorf("%w: %v", ErrIndexDegraded, err)
	}
}

func (t *Table) clearIndexError() {
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	t.indexErr = nil
}

func (t *Table) markStartupComplete(seq uint64) {
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	t.startupDone = true
	if seq > t.lastIndexed {
		t.lastIndexed = seq
	}
}

func (t *Table) markIndexed(seq uint64) {
	t.statusMu.Lock()
	defer t.statusMu.Unlock()
	if seq > t.lastIndexed {
		t.lastIndexed = seq
	}
}

func recordAccepted(dst *atomic.Uint64, seq uint64) {
	for {
		old := dst.Load()
		if seq <= old {
			return
		}
		if dst.CompareAndSwap(old, seq) {
			return
		}
	}
}

func paginateIDs(ids []string, limit, offset int) []string {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ids) {
		return []string{}
	}
	if limit <= 0 {
		return ids[offset:]
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end]
}

func validateEmbeddingDimensions(schema *document.Schema, doc *document.Document) error {
	if schema == nil || schema.EmbeddingDimensions <= 0 || len(doc.Embedding) == 0 {
		return nil
	}
	if len(doc.Embedding) != schema.EmbeddingDimensions {
		return fmt.Errorf("embedding: expected %d dimensions, got %d", schema.EmbeddingDimensions, len(doc.Embedding))
	}
	return nil
}

func buildEmbeddingText(doc *document.Document) string {
	var parts []string
	if doc.Content != "" {
		parts = append(parts, doc.Content)
	}
	keys := make([]string, 0, len(doc.Attributes))
	for key := range doc.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := doc.Attributes[key]
		switch vv := v.(type) {
		case string:
			parts = append(parts, vv)
		case []string:
			parts = append(parts, vv...)
		case []any:
			for _, elem := range vv {
				if s, ok := elem.(string); ok {
					parts = append(parts, s)
				}
			}
		}
	}
	return strings.Join(parts, "\n")
}

func indexedAttributes(schema *document.Schema, attrs map[string]any) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	if schema == nil {
		return attrs
	}

	filtered := make(map[string]any)
	for _, field := range schema.Fields {
		if !field.Indexed {
			continue
		}
		if value, ok := attrs[field.Name]; ok {
			filtered[field.Name] = value
		}
	}
	return filtered
}
