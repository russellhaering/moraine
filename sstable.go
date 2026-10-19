package moraine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/russellhaering/moraine/cache"
	"github.com/russellhaering/moraine/objstore"
	"hash/fnv"
	"math"
	"sort"
)

const (
	// DefaultBlockSize is the target size for data blocks.
	DefaultBlockSize = 4096

	// footerSize is the fixed size of the SSTable footer in bytes.
	footerSize = 48

	// magic is the magic number identifying a valid SSTable.
	magic = 0x57444231

	// bloomBitsPerKey is the number of bits per key in the bloom filter (~1% FPR).
	bloomBitsPerKey = 10

	// bloomHashCount is the number of hash functions for the bloom filter.
	bloomHashCount = 7

	// tombstoneValueLen marks a nil value in serialized entries. Real values
	// cannot reach this length in memory.
	tombstoneValueLen = ^uint32(0)
)

// SSTableMeta holds metadata about an SSTable.
type SSTableMeta struct {
	ID         string
	Size       int64
	EntryCount int
	MinKey     string
	MaxKey     string
	MinSeq     uint64
	MaxSeq     uint64
}

// indexEntry represents a single entry in the SSTable index block.
type indexEntry struct {
	FirstKey string
	Offset   uint64
	Size     uint64
}

// ---------- Bloom filter helpers ----------

// bloomCreate creates a bloom filter bit array for the given keys.
func bloomCreate(keys []string) []byte {
	if len(keys) == 0 {
		return nil
	}
	nBits := len(keys) * bloomBitsPerKey
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8 // round up to whole bytes
	buf := make([]byte, nBytes)

	for _, key := range keys {
		h1, h2 := bloomHashes(key)
		for i := 0; i < bloomHashCount; i++ {
			bit := (h1 + uint64(i)*h2) % uint64(nBits)
			buf[bit/8] |= 1 << (bit % 8)
		}
	}
	return buf
}

// bloomContains checks whether a key may be present in the bloom filter.
func bloomContains(filter []byte, key string) bool {
	if len(filter) == 0 {
		return false
	}
	nBits := uint64(len(filter)) * 8
	h1, h2 := bloomHashes(key)
	for i := 0; i < bloomHashCount; i++ {
		bit := (h1 + uint64(i)*h2) % nBits
		if filter[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// bloomHashes returns two independent FNV hashes used for double hashing.
func bloomHashes(key string) (uint64, uint64) {
	h1 := fnv.New64()
	h1.Write([]byte(key))
	v1 := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write([]byte(key))
	v2 := h2.Sum64()

	return v1, v2
}

// ---------- SSTableWriter ----------

// SSTableWriter builds an SSTable in memory.
type SSTableWriter struct {
	id        string
	blockSize int
	entries   []Entry
}

// NewSSTableWriter creates a new SSTableWriter. If blockSize is 0, DefaultBlockSize is used.
func NewSSTableWriter(id string, blockSize int) *SSTableWriter {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	return &SSTableWriter{
		id:        id,
		blockSize: blockSize,
	}
}

// Add appends an entry to the SSTable being built.
func (w *SSTableWriter) Add(entry Entry) {
	w.entries = append(w.entries, entry)
}

// Finish serializes the SSTable and returns the raw bytes and metadata.
func (w *SSTableWriter) Finish() ([]byte, SSTableMeta, error) {
	// Sort entries by key, then by descending sequence number for duplicate keys.
	sort.Slice(w.entries, func(i, j int) bool {
		if w.entries[i].Key != w.entries[j].Key {
			return w.entries[i].Key < w.entries[j].Key
		}
		return w.entries[i].SeqNum > w.entries[j].SeqNum
	})

	var buf []byte
	var index []indexEntry
	var keys []string

	meta := SSTableMeta{
		ID:         w.id,
		EntryCount: len(w.entries),
		MinSeq:     math.MaxUint64,
	}

	if len(w.entries) > 0 {
		meta.MinKey = w.entries[0].Key
		meta.MaxKey = w.entries[len(w.entries)-1].Key
	}

	// Write data blocks.
	blockStart := 0
	blockOffset := 0
	for i, e := range w.entries {
		keys = append(keys, e.Key)

		if e.SeqNum < meta.MinSeq {
			meta.MinSeq = e.SeqNum
		}
		if e.SeqNum > meta.MaxSeq {
			meta.MaxSeq = e.SeqNum
		}

		buf = appendEntry(buf, e)

		atEnd := i == len(w.entries)-1
		blockLen := len(buf) - blockOffset

		if blockLen >= w.blockSize || atEnd {
			index = append(index, indexEntry{
				FirstKey: w.entries[blockStart].Key,
				Offset:   uint64(blockOffset),
				Size:     uint64(blockLen),
			})
			blockOffset = len(buf)
			blockStart = i + 1
		}
	}

	// Handle empty SSTable: ensure MinSeq is sensible.
	if len(w.entries) == 0 {
		meta.MinSeq = 0
	}

	// Write index block.
	indexOffset := uint64(len(buf))
	for _, ie := range index {
		buf = appendUint32(buf, uint32(len(ie.FirstKey)))
		buf = append(buf, ie.FirstKey...)
		buf = appendUint64(buf, ie.Offset)
		buf = appendUint64(buf, ie.Size)
	}
	indexSize := uint64(len(buf)) - indexOffset

	// Write bloom filter block.
	bloomOffset := uint64(len(buf))
	bloom := bloomCreate(keys)
	buf = append(buf, bloom...)
	bloomSize := uint64(len(buf)) - bloomOffset

	// Write footer (48 bytes).
	buf = appendUint64(buf, indexOffset)
	buf = appendUint64(buf, indexSize)
	buf = appendUint64(buf, bloomOffset)
	buf = appendUint64(buf, bloomSize)
	buf = appendUint64(buf, uint64(len(w.entries)))
	buf = appendUint32(buf, magic)
	buf = appendUint32(buf, 0) // padding

	meta.Size = int64(len(buf))

	return buf, meta, nil
}

// ---------- SSTableReader ----------

// tailReadSize is how many bytes NewSSTableReader fetches from the end of an
// SSTable of known size. The index and bloom filter sit just before the
// footer, so for typical tables one ranged read covers all three.
const tailReadSize = 64 << 10

// SSTableReader provides read access to a serialized SSTable. A reader opened
// with NewSSTableReader holds only the index and bloom filter and fetches data
// blocks on demand through its caches; one opened with
// NewSSTableReaderFromBytes holds the whole table in memory.
type SSTableReader struct {
	id    string
	index []indexEntry
	bloom []byte
	meta  SSTableMeta

	// Ranged-read state.
	store      objstore.ObjectStore
	path       string
	blockCache *cache.LRUBlockCache
	diskCache  *cache.DiskCache

	// data is the whole table for readers opened from bytes.
	data []byte
}

type sstFooter struct {
	indexOffset, indexSize uint64
	bloomOffset, bloomSize uint64
	entryCount             uint64
}

func parseFooter(b []byte) (sstFooter, error) {
	if len(b) < footerSize {
		return sstFooter{}, errors.New("sstable: data too short for footer")
	}
	b = b[len(b)-footerSize:]
	if m := binary.LittleEndian.Uint32(b[40:44]); m != magic {
		return sstFooter{}, fmt.Errorf("sstable: bad magic %x", m)
	}
	return sstFooter{
		indexOffset: binary.LittleEndian.Uint64(b[0:8]),
		indexSize:   binary.LittleEndian.Uint64(b[8:16]),
		bloomOffset: binary.LittleEndian.Uint64(b[16:24]),
		bloomSize:   binary.LittleEndian.Uint64(b[24:32]),
		entryCount:  binary.LittleEndian.Uint64(b[32:40]),
	}, nil
}

func parseIndex(indexData []byte) ([]indexEntry, error) {
	var index []indexEntry
	pos := 0
	for pos < len(indexData) {
		if pos+4 > len(indexData) {
			return nil, errors.New("sstable: truncated index entry")
		}
		keyLen := int(binary.LittleEndian.Uint32(indexData[pos : pos+4]))
		pos += 4
		if pos+keyLen+16 > len(indexData) {
			return nil, errors.New("sstable: truncated index entry")
		}
		firstKey := string(indexData[pos : pos+keyLen])
		pos += keyLen
		offset := binary.LittleEndian.Uint64(indexData[pos : pos+8])
		pos += 8
		size := binary.LittleEndian.Uint64(indexData[pos : pos+8])
		pos += 8
		index = append(index, indexEntry{FirstKey: firstKey, Offset: offset, Size: size})
	}
	return index, nil
}

// NewSSTableReader opens the SSTable described by info with ranged reads. The
// footer, index and bloom filter are loaded eagerly, in one read when info.Size
// is known and the metadata fits in the table's tail; data blocks are fetched
// lazily, checking blockCache and then diskCache (either may be nil) before
// object storage. Key and sequence ranges come from info rather than from
// scanning the table.
func NewSSTableReader(ctx context.Context, store objstore.ObjectStore, info SSTInfo, blockCache *cache.LRUBlockCache, diskCache *cache.DiskCache) (*SSTableReader, error) {
	var tail []byte
	var tailStart uint64
	var err error
	if info.Size > 0 {
		n := min(int64(tailReadSize), info.Size)
		tailStart = uint64(info.Size - n)
		tail, err = store.GetRange(ctx, info.Path, int64(tailStart), n)
	} else {
		tail, err = store.GetRange(ctx, info.Path, -int64(footerSize), footerSize)
	}
	if err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	f, err := parseFooter(tail)
	if err != nil {
		return nil, err
	}
	if info.Size <= 0 {
		// The footer read was a suffix read; locate it from the footer itself.
		tailStart = f.bloomOffset + f.bloomSize
	}
	if f.indexOffset+f.indexSize != f.bloomOffset {
		return nil, errors.New("sstable: corrupt footer offsets")
	}

	// The index and bloom filter are contiguous and end at the footer.
	metaEnd := f.bloomOffset + f.bloomSize
	var metaData []byte
	if f.indexOffset >= tailStart && metaEnd-tailStart <= uint64(len(tail)) {
		metaData = tail[f.indexOffset-tailStart : metaEnd-tailStart]
	} else {
		metaData, err = store.GetRange(ctx, info.Path, int64(f.indexOffset), int64(metaEnd-f.indexOffset))
		if err != nil {
			return nil, fmt.Errorf("sstable: read index: %w", err)
		}
		if uint64(len(metaData)) != metaEnd-f.indexOffset {
			return nil, errors.New("sstable: short index read")
		}
	}

	index, err := parseIndex(metaData[:f.indexSize])
	if err != nil {
		return nil, err
	}
	var bloom []byte
	if f.bloomSize > 0 {
		bloom = metaData[f.indexSize:]
	}

	return &SSTableReader{
		id:    info.ID,
		index: index,
		bloom: bloom,
		meta: SSTableMeta{
			ID:         info.ID,
			MinKey:     info.MinKey,
			MaxKey:     info.MaxKey,
			MinSeq:     info.MinSeq,
			MaxSeq:     info.MaxSeq,
			Size:       info.Size,
			EntryCount: int(f.entryCount),
		},
		store:      store,
		path:       info.Path,
		blockCache: blockCache,
		diskCache:  diskCache,
	}, nil
}

// NewSSTableReaderFromBytes parses an SSTable held entirely in memory, as
// compaction and WAL replay do after reading a whole object.
func NewSSTableReaderFromBytes(id string, data []byte) (*SSTableReader, error) {
	f, err := parseFooter(data)
	if err != nil {
		return nil, err
	}
	dataLen := uint64(len(data))
	if f.indexOffset+f.indexSize > dataLen || f.bloomOffset+f.bloomSize > dataLen {
		return nil, errors.New("sstable: corrupt footer offsets")
	}
	index, err := parseIndex(data[f.indexOffset : f.indexOffset+f.indexSize])
	if err != nil {
		return nil, err
	}
	for _, ie := range index {
		if ie.Offset+ie.Size > f.indexOffset {
			return nil, errors.New("sstable: corrupt index offsets")
		}
	}
	var bloom []byte
	if f.bloomSize > 0 {
		bloom = data[f.bloomOffset : f.bloomOffset+f.bloomSize]
	}

	// Compute metadata by scanning entries.
	meta := SSTableMeta{
		ID:         id,
		Size:       int64(len(data)),
		EntryCount: int(f.entryCount),
		MinSeq:     math.MaxUint64,
	}
	if f.entryCount == 0 {
		meta.MinSeq = 0
	}
	first := true
	for _, ie := range index {
		block := data[ie.Offset : ie.Offset+ie.Size]
		for off := 0; off < len(block); {
			e, n, err := decodeEntry(block, off)
			if err != nil {
				break
			}
			if first {
				meta.MinKey = e.Key
				first = false
			}
			meta.MaxKey = e.Key
			meta.MinSeq = min(meta.MinSeq, e.SeqNum)
			meta.MaxSeq = max(meta.MaxSeq, e.SeqNum)
			off += n
		}
	}

	return &SSTableReader{id: id, index: index, bloom: bloom, meta: meta, data: data}, nil
}

// Meta returns the SSTable metadata.
func (r *SSTableReader) Meta() SSTableMeta {
	return r.meta
}

// BloomMayContain returns true if the bloom filter indicates the key might be present.
func (r *SSTableReader) BloomMayContain(key string) bool {
	return bloomContains(r.bloom, key)
}

// block returns the data block described by ie, from the in-memory table,
// the block cache, the disk cache or object storage, in that order.
func (r *SSTableReader) block(ctx context.Context, ie indexEntry) ([]byte, error) {
	if r.data != nil {
		return r.data[ie.Offset : ie.Offset+ie.Size], nil
	}
	key := cache.BlockKey{SSTID: r.path, BlockOffset: int64(ie.Offset)}
	if r.blockCache != nil {
		if b := r.blockCache.Get(key); b != nil {
			return b, nil
		}
	}
	diskKey := fmt.Sprintf("%s.blocks/%d", r.path, ie.Offset)
	if r.diskCache != nil {
		if b, err := r.diskCache.Get(diskKey); err == nil && b != nil {
			if r.blockCache != nil {
				r.blockCache.Put(key, b)
			}
			return b, nil
		}
	}
	b, err := r.store.GetRange(ctx, r.path, int64(ie.Offset), int64(ie.Size))
	if err != nil {
		return nil, err
	}
	if uint64(len(b)) != ie.Size {
		return nil, fmt.Errorf("sstable: short block read at %d: got %d of %d bytes", ie.Offset, len(b), ie.Size)
	}
	if r.blockCache != nil {
		r.blockCache.Put(key, b)
	}
	if r.diskCache != nil {
		_ = r.diskCache.Put(diskKey, b)
	}
	return b, nil
}

// Get looks up a key using binary search on the index, then scans the data block.
// Returns nil if the key is not found.
func (r *SSTableReader) Get(ctx context.Context, key string) (*Entry, error) {
	if len(r.index) == 0 {
		return nil, nil
	}

	// Binary search to find the data block that may contain the key.
	// We want the last block whose FirstKey <= key.
	blockIdx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].FirstKey > key
	}) - 1

	if blockIdx < 0 {
		return nil, nil
	}

	block, err := r.block(ctx, r.index[blockIdx])
	if err != nil {
		return nil, fmt.Errorf("sstable: read block: %w", err)
	}

	// Scan the block for the key. Return the entry with the highest sequence
	// number (first occurrence, since entries are sorted by key asc, seqnum desc).
	var found *Entry
	off := 0
	for off < len(block) {
		e, n, err := decodeEntry(block, off)
		if err != nil {
			return nil, fmt.Errorf("sstable: decode error in block: %w", err)
		}
		off += n

		if e.Key == key {
			ec := e
			found = &ec
			break // First match has highest seqnum due to sort order.
		}
		if e.Key > key {
			break // Past the target key; no point continuing.
		}
	}
	return found, nil
}

// ---------- SSTableIterator ----------

// SSTableIterator iterates over SSTable entries in sorted order.
type SSTableIterator struct {
	reader   *SSTableReader
	ctx      context.Context
	blockIdx int
	blockOff int
	block    []byte // currently loaded data block
	current  Entry
	valid    bool
	err      error
}

// Iterator returns an iterator over all entries in the SSTable.
func (r *SSTableReader) Iterator() *SSTableIterator {
	return &SSTableIterator{reader: r, ctx: context.Background()}
}

// IteratorFrom returns an iterator positioned before the first entry with
// key > afterKey. Uses binary search on the index to find the right block,
// then scans within the block past afterKey. If afterKey is empty, this
// behaves identically to Iterator().
func (r *SSTableReader) IteratorFrom(ctx context.Context, afterKey string) *SSTableIterator {
	return r.seek(ctx, afterKey, false)
}

// SeekFrom returns an iterator positioned before the first entry with
// key >= key.
func (r *SSTableReader) SeekFrom(ctx context.Context, key string) *SSTableIterator {
	return r.seek(ctx, key, true)
}

func (r *SSTableReader) seek(ctx context.Context, key string, inclusive bool) *SSTableIterator {
	if key == "" || len(r.index) == 0 {
		return &SSTableIterator{reader: r, ctx: ctx}
	}

	// Binary search: find the last block whose FirstKey <= key. Keys are
	// unique within an SSTable, so no earlier block can hold a match.
	blockIdx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].FirstKey > key
	}) - 1
	if blockIdx < 0 {
		// All blocks have FirstKey > key, so start from the beginning.
		return &SSTableIterator{reader: r, ctx: ctx}
	}

	// Load the block and position the iterator within it.
	block, err := r.block(ctx, r.index[blockIdx])
	if err != nil {
		return &SSTableIterator{reader: r, ctx: ctx, blockIdx: len(r.index), err: err}
	}
	off := 0
	for off < len(block) {
		e, n, err := decodeEntry(block, off)
		if err != nil {
			return &SSTableIterator{reader: r, ctx: ctx, blockIdx: len(r.index), err: err}
		}
		if e.Key > key || (inclusive && e.Key == key) {
			return &SSTableIterator{reader: r, ctx: ctx, blockIdx: blockIdx, blockOff: off, block: block}
		}
		off += n
	}

	// Every entry in this block sorts before the target; start from the next block.
	return &SSTableIterator{reader: r, ctx: ctx, blockIdx: blockIdx + 1}
}

// Next advances the iterator to the next entry. Returns false when exhausted.
func (it *SSTableIterator) Next() bool {
	for it.blockIdx < len(it.reader.index) {
		// Load block if not already loaded.
		if it.block == nil {
			block, err := it.reader.block(it.ctx, it.reader.index[it.blockIdx])
			if err != nil {
				it.err = err
				it.valid = false
				return false
			}
			it.block = block
			it.blockOff = 0
		}
		if it.blockOff < len(it.block) {
			e, n, err := decodeEntry(it.block, it.blockOff)
			if err != nil {
				it.err = err
				it.valid = false
				return false
			}
			it.blockOff += n
			it.current = e
			it.valid = true
			return true
		}
		// Move to the next block.
		it.blockIdx++
		it.block = nil
		it.blockOff = 0
	}

	it.valid = false
	return false
}

// Entry returns the current entry. Only valid after a successful call to Next.
func (it *SSTableIterator) Entry() Entry {
	return it.current
}

// Err returns any error encountered during iteration.
func (it *SSTableIterator) Err() error {
	return it.err
}

// ---------- Encoding helpers ----------

// appendEntry appends a serialized entry to buf and returns the extended buffer.
func appendEntry(buf []byte, e Entry) []byte {
	buf = appendUint32(buf, uint32(len(e.Key)))
	buf = append(buf, e.Key...)
	if e.Value == nil {
		buf = appendUint32(buf, tombstoneValueLen)
		buf = appendUint64(buf, e.SeqNum)
		return buf
	}
	buf = appendUint32(buf, uint32(len(e.Value)))
	buf = append(buf, e.Value...)
	buf = appendUint64(buf, e.SeqNum)
	return buf
}

// decodeEntry decodes a single entry starting at offset in block.
// Returns the entry, the number of bytes consumed, and any error.
func decodeEntry(block []byte, offset int) (Entry, int, error) {
	pos := offset

	if pos+4 > len(block) {
		return Entry{}, 0, errors.New("sstable: truncated key length")
	}
	keyLen := int(binary.LittleEndian.Uint32(block[pos : pos+4]))
	pos += 4

	if pos+keyLen > len(block) {
		return Entry{}, 0, errors.New("sstable: truncated key")
	}
	key := string(block[pos : pos+keyLen])
	pos += keyLen

	if pos+4 > len(block) {
		return Entry{}, 0, errors.New("sstable: truncated value length")
	}
	valLenRaw := binary.LittleEndian.Uint32(block[pos : pos+4])
	pos += 4

	// Zero-length values are also decoded as tombstones: the pre-extraction
	// engine encoded deletions as len-0 values (no sentinel), so stores written
	// by it contain them. Documents and real values always serialize to at
	// least one byte, making the legacy form unambiguous in practice.
	if valLenRaw == tombstoneValueLen || valLenRaw == 0 {
		if pos+8 > len(block) {
			return Entry{}, 0, errors.New("sstable: truncated seq num")
		}
		seqNum := binary.LittleEndian.Uint64(block[pos : pos+8])
		pos += 8
		return Entry{Key: key, Value: nil, SeqNum: seqNum}, pos - offset, nil
	}

	valLen := int(valLenRaw)
	if pos+valLen > len(block) {
		return Entry{}, 0, errors.New("sstable: truncated value")
	}
	value := make([]byte, valLen)
	copy(value, block[pos:pos+valLen])
	pos += valLen

	if pos+8 > len(block) {
		return Entry{}, 0, errors.New("sstable: truncated seq num")
	}
	seqNum := binary.LittleEndian.Uint64(block[pos : pos+8])
	pos += 8

	return Entry{Key: key, Value: value, SeqNum: seqNum}, pos - offset, nil
}

func appendUint32(buf []byte, v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return append(buf, b...)
}

func appendUint64(buf []byte, v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return append(buf, b...)
}
