package moraine

import (
	"encoding/binary"
	"errors"
	"fmt"
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

// SSTableReader provides read access to a serialized SSTable.
type SSTableReader struct {
	id    string
	data  []byte
	index []indexEntry
	bloom []byte
	meta  SSTableMeta
}

// NewSSTableReader parses the footer and index from the raw SSTable bytes.
func NewSSTableReader(id string, data []byte) (*SSTableReader, error) {
	if len(data) < footerSize {
		return nil, errors.New("sstable: data too short for footer")
	}

	footer := data[len(data)-footerSize:]
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexSize := binary.LittleEndian.Uint64(footer[8:16])
	bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
	bloomSize := binary.LittleEndian.Uint64(footer[24:32])
	entryCount := binary.LittleEndian.Uint64(footer[32:40])
	magicVal := binary.LittleEndian.Uint32(footer[40:44])

	if magicVal != magic {
		return nil, fmt.Errorf("sstable: bad magic %x", magicVal)
	}

	dataLen := uint64(len(data))
	if indexOffset+indexSize > dataLen || bloomOffset+bloomSize > dataLen {
		return nil, errors.New("sstable: corrupt footer offsets")
	}

	// Parse index block.
	indexData := data[indexOffset : indexOffset+indexSize]
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
		index = append(index, indexEntry{
			FirstKey: firstKey,
			Offset:   offset,
			Size:     size,
		})
	}

	// Load bloom filter.
	var bloom []byte
	if bloomSize > 0 {
		bloom = data[bloomOffset : bloomOffset+bloomSize]
	}

	// Compute metadata by scanning entries.
	meta := SSTableMeta{
		ID:         id,
		Size:       int64(len(data)),
		EntryCount: int(entryCount),
		MinSeq:     math.MaxUint64,
	}

	if int(entryCount) == 0 {
		meta.MinSeq = 0
	}

	if len(index) > 0 {
		// Scan first block for MinKey.
		firstBlock := data[index[0].Offset : index[0].Offset+index[0].Size]
		firstEntry, _, err := decodeEntry(firstBlock, 0)
		if err == nil {
			meta.MinKey = firstEntry.Key
		}

		// Scan last block for MaxKey.
		lastIdx := index[len(index)-1]
		lastBlock := data[lastIdx.Offset : lastIdx.Offset+lastIdx.Size]
		var lastEntry Entry
		off := 0
		for off < len(lastBlock) {
			e, n, err := decodeEntry(lastBlock, off)
			if err != nil {
				break
			}
			lastEntry = e
			off += n
		}
		meta.MaxKey = lastEntry.Key

		// Scan all entries for sequence number range.
		for _, ie := range index {
			block := data[ie.Offset : ie.Offset+ie.Size]
			off := 0
			for off < len(block) {
				e, n, err := decodeEntry(block, off)
				if err != nil {
					break
				}
				if e.SeqNum < meta.MinSeq {
					meta.MinSeq = e.SeqNum
				}
				if e.SeqNum > meta.MaxSeq {
					meta.MaxSeq = e.SeqNum
				}
				off += n
			}
		}
	}

	return &SSTableReader{
		id:    id,
		data:  data,
		index: index,
		bloom: bloom,
		meta:  meta,
	}, nil
}

// Meta returns the SSTable metadata.
func (r *SSTableReader) Meta() SSTableMeta {
	return r.meta
}

// BloomMayContain returns true if the bloom filter indicates the key might be present.
func (r *SSTableReader) BloomMayContain(key string) bool {
	return bloomContains(r.bloom, key)
}

// Get looks up a key using binary search on the index, then scans the data block.
// Returns nil if the key is not found.
func (r *SSTableReader) Get(key string) (*Entry, error) {
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

	ie := r.index[blockIdx]
	block := r.data[ie.Offset : ie.Offset+ie.Size]

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

// Iterator returns an iterator over all entries in the SSTable.
func (r *SSTableReader) Iterator() *SSTableIterator {
	return &SSTableIterator{
		reader:   r,
		blockIdx: 0,
		blockOff: 0,
	}
}

// IteratorFrom returns an iterator positioned before the first entry with
// key > afterKey. Uses binary search on the index to find the right block,
// then scans within the block past afterKey. If afterKey is empty, this
// behaves identically to Iterator().
func (r *SSTableReader) IteratorFrom(afterKey string) *SSTableIterator {
	if afterKey == "" || len(r.index) == 0 {
		return r.Iterator()
	}

	// Binary search: find the last block whose FirstKey <= afterKey.
	blockIdx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].FirstKey > afterKey
	}) - 1

	if blockIdx < 0 {
		// All blocks have FirstKey > afterKey, so start from the beginning.
		return r.Iterator()
	}

	// Scan within the block to find the first entry with key > afterKey.
	ie := r.index[blockIdx]
	block := r.data[ie.Offset : ie.Offset+ie.Size]
	off := 0
	for off < len(block) {
		e, n, err := decodeEntry(block, off)
		if err != nil {
			return &SSTableIterator{reader: r, blockIdx: len(r.index), err: err}
		}
		if e.Key > afterKey {
			// Position the iterator here.
			return &SSTableIterator{
				reader:   r,
				blockIdx: blockIdx,
				blockOff: off,
			}
		}
		off += n
	}

	// All entries in this block have key <= afterKey, start from next block.
	return &SSTableIterator{
		reader:   r,
		blockIdx: blockIdx + 1,
		blockOff: 0,
	}
}

// ---------- SSTableIterator ----------

// SSTableIterator iterates over SSTable entries in sorted order.
type SSTableIterator struct {
	reader   *SSTableReader
	blockIdx int
	blockOff int
	current  Entry
	valid    bool
	err      error
}

// Next advances the iterator to the next entry. Returns false when exhausted.
func (it *SSTableIterator) Next() bool {
	for it.blockIdx < len(it.reader.index) {
		ie := it.reader.index[it.blockIdx]
		block := it.reader.data[ie.Offset : ie.Offset+ie.Size]

		if it.blockOff < len(block) {
			e, n, err := decodeEntry(block, it.blockOff)
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
