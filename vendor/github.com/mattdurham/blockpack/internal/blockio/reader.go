package blockio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"regexp"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/mattdurham/blockpack/internal/arena"
	aslice "github.com/mattdurham/blockpack/internal/arena/slice"
)

// BOT: Can we separate reader and writer into their own folders?

// Block holds decoded columns and metadata for a single block.
type Block struct {
	spanCount int
	columns   map[string]*Column

	MinStart        uint64
	MaxStart        uint64
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
	ColumnNameBloom [columnNameBloomBits / 8]byte
}

// SpanCount returns number of spans in the block.
func (b *Block) SpanCount() int {
	return b.spanCount
}

// Columns returns all decoded columns keyed by name.
func (b *Block) Columns() map[string]*Column {
	return b.columns
}

// GetColumn returns a specific column by name, or nil if not found
func (b *Block) GetColumn(name string) *Column {
	return b.columns[name]
}

// ReaderProvider supplies random access to blockpack data.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64) (int, error)
}

// CloseableReaderProvider extends ReaderProvider with resource cleanup.
// Implementations that hold open file descriptors or other resources should implement this.
type CloseableReaderProvider interface {
	ReaderProvider
	Close() error
}

// Reader exposes blockpack data for predicate evaluation across blocks.
// Blocks are parsed lazily on first access for better performance on selective queries.
type Reader struct {
	name  string    // File name/path for cache keys
	cache ByteCache // Optional byte cache for I/O operations
	// BOT: remove data
	data                  []byte                           // Raw file data (optional)
	provider              ReaderProvider                   // Data provider
	dataSize              int64                            // Total data size
	blockEntries          []blockIndexEntry                // Block metadata (includes bloom filters)
	parsedBlocks          map[int]*Block                   // Lazily parsed blocks cache
	dedicatedIndex        map[string]*dedicatedColumnIndex // Parsed dedicated column indexes (lazy)
	dedicatedIndexOffsets map[string]dedicatedIndexMeta    // Metadata for lazy parsing
	rangeBucketMeta       map[string]*RangeBucketMetadata  // Range bucket metadata for dedicated columns
	metadataBytes         []byte                           // Raw metadata for lazy column parsing
	totalSpans            int
	traceBlockIndex       map[[16]byte][]int // Trace ID -> block IDs mapping
}

type dedicatedColumnIndex struct {
	typ    ColumnType
	values map[string][]int
}

type dedicatedIndexMeta struct {
	typ    ColumnType
	offset int // Offset in metadataBytes where this column's index starts
	length int // Length of this column's index data
}

type fileFooter struct {
	magic              uint32
	version            uint8
	metadataOffset     uint64
	metadataLen        uint64
	metadataCRC        uint32
	metricStreamOffset uint64 // v9: offset to metric stream blocks
	metricStreamLen    uint64 // v9: length of metric stream blocks
}

// BOT: Get rid of this.
// NewReader parses blockpack data (v5 format) from an in-memory buffer.
func NewReader(data []byte) (*Reader, error) {
	return NewReaderFromProvider(&byteSliceProvider{data: data})
}

// NewReaderFromProvider parses blockpack data (v5 format) from a provider.
// BOT: Get rid of this.
func NewReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	return NewReaderWithCache("", provider, nil)
}

// NewReaderWithCache parses blockpack data from a provider with optional caching.
// The name parameter is used for cache keys and must uniquely identify the file.
// If name is empty, caching will be disabled even if a cache is provided.
// The cache parameter is optional (pass nil for no caching).
// We should NOT have a separate byte cache, instead use a layered reader provider.
func NewReaderWithCache(name string, provider ReaderProvider, cache ByteCache) (*Reader, error) {
	dataSize, err := provider.Size()
	if err != nil {
		return nil, err
	}

	// Need at least footer size (41 bytes)
	if dataSize < int64(footerSize) {
		return nil, fmt.Errorf("data too small for footer")
	}

	// BOT: Why is this uncached?
	footerBytes, err := readProviderRangeUncached(provider, dataSize-int64(footerSize), int(footerSize))
	if err != nil {
		return nil, err
	}
	footer, err := readFooter(footerBytes)
	if err != nil {
		return nil, err
	}
	if footer.magic != magicNumber {
		return nil, fmt.Errorf("invalid magic")
	}
	if footer.version != versionV10 && footer.version != versionV11 {
		return nil, fmt.Errorf("unsupported version %d (only v10/v11 supported)", footer.version)
	}

	if footer.metadataOffset+footer.metadataLen > uint64(dataSize-int64(footerSize)) {
		return nil, fmt.Errorf("metadata out of bounds")
	}

	metadata, err := readProviderRangeUncached(provider, int64(footer.metadataOffset), int(footer.metadataLen))
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(metadata) != footer.metadataCRC {
		return nil, fmt.Errorf("metadata crc mismatch")
	}

	// Can we make this lazier? And only access when the querier needs it?
	entries, dedicatedIndexOffsets, traceBlockIndex, err := parseV5MetadataLazy(metadata, footer.version)
	if err != nil {
		return nil, err
	}

	total := 0
	for _, entry := range entries {
		total += int(entry.SpanCount)
	}

	reader := &Reader{
		name:                  name,
		cache:                 cache,
		provider:              provider,
		dataSize:              dataSize,
		blockEntries:          entries,
		parsedBlocks:          make(map[int]*Block),
		dedicatedIndex:        make(map[string]*dedicatedColumnIndex),
		dedicatedIndexOffsets: dedicatedIndexOffsets,
		rangeBucketMeta:       make(map[string]*RangeBucketMetadata),
		metadataBytes:         metadata,
		totalSpans:            total,
		traceBlockIndex:       traceBlockIndex,
	}
	// BOT: NEVER USE THIS, always assume object storage.
	if sliceProvider, ok := provider.(*byteSliceProvider); ok {
		reader.data = sliceProvider.data
	}
	return reader, nil
}

// BOT: Providers should be in there own package under blockpack.
type byteSliceProvider struct {
	data []byte
}

type rangeReaderProvider struct {
	base   ReaderProvider
	offset int64
	length int64
}

func newRangeReaderProvider(base ReaderProvider, offset int64, length int64) *rangeReaderProvider {
	return &rangeReaderProvider{
		base:   base,
		offset: offset,
		length: length,
	}
}

func (p *rangeReaderProvider) Size() (int64, error) {
	return p.length, nil
}

func (p *rangeReaderProvider) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= p.length {
		return 0, io.EOF
	}
	// Allow partial reads at the end of the range
	if off+int64(len(b)) > p.length {
		b = b[:p.length-off]
	}
	return p.base.ReadAt(b, p.offset+off)
}

func (p *byteSliceProvider) Size() (int64, error) {
	return int64(len(p.data)), nil
}

func (p *byteSliceProvider) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// readProviderRangeUncached reads a byte range from the provider without caching.
// Used during Reader construction before the Reader is fully initialized.
func readProviderRangeUncached(provider ReaderProvider, off int64, length int) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid read length %d", length)
	}
	buf := make([]byte, length)
	n, err := provider.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != length {
		return nil, fmt.Errorf("short read: %d != %d", n, length)
	}
	return buf, nil
}

// readProviderRange reads a byte range from the provider, using cache if available.
func (r *Reader) readProviderRange(off int64, length int) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid read length %d", length)
	}

	// Check cache first (if present)
	if r.cache != nil && r.name != "" {
		key := fmt.Sprintf("%s:%d:%d", r.name, off, length)
		if data, found := r.cache.Get(key); found {
			return data, nil // Cache hit - no I/O
		}
	}

	// Cache miss or no cache - read from provider
	var data []byte

	buf := make([]byte, length)
	n, err := r.provider.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != length {
		return nil, fmt.Errorf("short read: %d != %d", n, length)
	}
	data = buf

	// Store in cache for future reads
	if r.cache != nil && r.name != "" {
		key := fmt.Sprintf("%s:%d:%d", r.name, off, length)
		r.cache.Put(key, data)
	}

	return data, nil
}

func (r *Reader) readRange(off int64, length int) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid read length %d", length)
	}
	end := off + int64(length)
	if off < 0 || end < off || end > r.dataSize {
		return nil, fmt.Errorf("read out of bounds (offset %d length %d size %d)", off, length, r.dataSize)
	}
	if r.data != nil {
		start := int(off)
		stop := int(end)
		return r.data[start:stop], nil
	}
	return r.readProviderRange(off, length)
}

func readFooter(data []byte) (fileFooter, error) {
	if len(data) < footerSize {
		return fileFooter{}, fmt.Errorf("data too small for footer (expected %d bytes)", footerSize)
	}

	start := len(data) - footerSize
	footer := fileFooter{
		magic:              binary.LittleEndian.Uint32(data[start : start+4]),
		version:            data[start+4],
		metadataOffset:     binary.LittleEndian.Uint64(data[start+5 : start+13]),
		metadataLen:        binary.LittleEndian.Uint64(data[start+13 : start+21]),
		metadataCRC:        binary.LittleEndian.Uint32(data[start+21 : start+25]),
		metricStreamOffset: binary.LittleEndian.Uint64(data[start+25 : start+33]),
		metricStreamLen:    binary.LittleEndian.Uint64(data[start+33 : start+41]),
	}
	return footer, nil
}

// parseV5MetadataLazy parses block entries but only records offsets for dedicated indexes
// Actual dedicated indexes are parsed on-demand when first accessed
func parseV5MetadataLazy(data []byte, version uint8) ([]blockIndexEntry, map[string]dedicatedIndexMeta, map[[16]byte][]int, error) {
	if len(data) < 4 {
		return nil, nil, nil, fmt.Errorf("truncated metadata header")
	}
	blockCount := int(binary.LittleEndian.Uint32(data[0:4]))
	offset := 4

	entries := make([]blockIndexEntry, blockCount)
	for i := 0; i < blockCount; i++ {
		var err error
		reader := bytes.NewReader(data[offset:])
		entries[i], err = readBlockIndexEntry(reader, version)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("block index %d: %w", i, err)
		}
		// Calculate how many bytes were read
		bytesRead := len(data[offset:]) - reader.Len()
		offset += bytesRead
	}

	dedicatedOffsets, offset, err := scanDedicatedIndexOffsets(data, offset)
	if err != nil {
		return nil, nil, nil, err
	}

	// Parse per-column offsets for each block
	offset, err = parseColumnIndexData(data, offset, entries)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse column index: %w", err)
	}

	// Parse trace block index
	traceBlockIndex, err := parseTraceBlockIndex(data, offset)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse trace block index: %w", err)
	}

	return entries, dedicatedOffsets, traceBlockIndex, nil
}

// parseTraceBlockIndex parses the trace block index
// Format:
//   - trace_count (4 bytes)
//   - For each trace (sorted by trace_id):
//   - trace_id (16 bytes)
//   - block_count (2 bytes)
//   - block_ids (2 bytes each)
func parseTraceBlockIndex(data []byte, offset int) (map[[16]byte][]int, error) {
	// Read trace count
	if offset+4 > len(data) {
		return nil, fmt.Errorf("truncated trace count")
	}
	traceCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	traceIndex := make(map[[16]byte][]int, traceCount)

	for i := 0; i < traceCount; i++ {
		// Read trace_id (16 bytes)
		if offset+16 > len(data) {
			return nil, fmt.Errorf("truncated trace_id at trace %d", i)
		}
		var traceID [16]byte
		copy(traceID[:], data[offset:offset+16])
		offset += 16

		// Read block_count (2 bytes)
		if offset+2 > len(data) {
			return nil, fmt.Errorf("truncated block_count at trace %d", i)
		}
		blockCount := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2

		// Read block_ids (2 bytes each)
		blockIDs := make([]int, blockCount)
		for j := 0; j < blockCount; j++ {
			if offset+2 > len(data) {
				return nil, fmt.Errorf("truncated block_id at trace %d, block %d", i, j)
			}
			blockIDs[j] = int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2
		}

		traceIndex[traceID] = blockIDs
	}

	return traceIndex, nil
}

// scanDedicatedIndexOffsets scans the metadata and records where each dedicated column's index is located
// Does NOT parse the actual values - just records offsets for lazy loading
// Returns the offsets map and the final offset after scanning all dedicated indexes
func scanDedicatedIndexOffsets(data []byte, offset int) (map[string]dedicatedIndexMeta, int, error) {
	if offset+4 > len(data) {
		return nil, 0, fmt.Errorf("truncated dedicated index count")
	}
	dedicatedCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	offsets := make(map[string]dedicatedIndexMeta, dedicatedCount)
	for i := 0; i < dedicatedCount; i++ {
		startOffset := offset

		// Read column name
		if offset+2 > len(data) {
			return nil, 0, fmt.Errorf("truncated dedicated column name length")
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen+1 > len(data) {
			return nil, 0, fmt.Errorf("truncated dedicated column name")
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typ := ColumnType(data[offset])
		offset++

		// Skip bucket metadata if present
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("truncated bucket metadata flag for %s", name)
		}
		hasBuckets := data[offset]
		offset++

		if hasBuckets == 1 {
			// Skip bucket metadata
			if offset+8+8+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated bucket metadata header for %s", name)
			}
			offset += 8 // min
			offset += 8 // max
			if offset+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated boundary count for %s", name)
			}
			boundaryCount := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if offset+boundaryCount*8 > len(data) {
				return nil, 0, fmt.Errorf("truncated bucket boundaries for %s", name)
			}
			offset += boundaryCount * 8
		}

		// Read value count but don't parse values
		if offset+4 > len(data) {
			return nil, 0, fmt.Errorf("truncated dedicated value count for %s", name)
		}
		valueCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		// Skip over all values and their block IDs
		for j := 0; j < valueCount; j++ {
			// Skip value key
			switch typ {
			case ColumnTypeString:
				if offset+4 > len(data) {
					return nil, 0, fmt.Errorf("truncated string length")
				}
				strLen := int(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4 + strLen
			case ColumnTypeInt64, ColumnTypeUint64, ColumnTypeFloat64:
				offset += 8
			case ColumnTypeBool:
				offset += 1
			case ColumnTypeBytes:
				if offset+4 > len(data) {
					return nil, 0, fmt.Errorf("truncated bytes length")
				}
				bytesLen := int(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4 + bytesLen
			case ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration:
				// Read length prefix, then skip that many bytes
				if offset+1 > len(data) {
					return nil, 0, fmt.Errorf("truncated range key length")
				}
				length := int(data[offset])
				offset += 1 + length // length prefix + key data
			default:
				return nil, 0, fmt.Errorf("unsupported dedicated type %d", typ)
			}

			// Skip block IDs
			if offset+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated block id count")
			}
			idCount := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if offset+idCount*4 > len(data) {
				return nil, 0, fmt.Errorf("truncated block ids")
			}
			offset += idCount * 4
		}

		// Record the offset and length for this column
		length := offset - startOffset
		offsets[name] = dedicatedIndexMeta{
			typ:    typ,
			offset: startOffset,
			length: length,
		}
	}

	return offsets, offset, nil
}

// parseColumnIndexData parses the per-column offset data for all blocks
func parseColumnIndexData(data []byte, offset int, entries []blockIndexEntry) (int, error) {
	for i := range entries {
		// Read column count
		if offset+4 > len(data) {
			return 0, fmt.Errorf("truncated column count for block %d", i)
		}
		columnCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		entries[i].ColumnIndex = make([]ColumnIndexEntry, columnCount)
		for j := 0; j < columnCount; j++ {
			// Read column name length
			if offset+2 > len(data) {
				return 0, fmt.Errorf("truncated column name length for block %d, column %d", i, j)
			}
			nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2

			// Read column name
			if offset+nameLen > len(data) {
				return 0, fmt.Errorf("truncated column name for block %d, column %d", i, j)
			}
			name := string(data[offset : offset+nameLen])
			offset += nameLen

			// Read offset
			if offset+4 > len(data) {
				return 0, fmt.Errorf("truncated column offset for block %d, column %d", i, j)
			}
			colOffset := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			// Read length
			if offset+4 > len(data) {
				return 0, fmt.Errorf("truncated column length for block %d, column %d", i, j)
			}
			colLength := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			// Validate column bounds are within block bounds
			blockSize := entries[i].Length
			if uint64(colOffset)+uint64(colLength) > uint64(blockSize) {
				return 0, fmt.Errorf("block %d column %d out of bounds: offset=%d length=%d blockSize=%d",
					i, j, colOffset, colLength, blockSize)
			}

			entries[i].ColumnIndex[j] = ColumnIndexEntry{
				Name:   name,
				Offset: colOffset,
				Length: colLength,
			}
		}
	}
	return offset, nil
}

// ensureDedicatedColumnParsed parses a specific dedicated column index on-demand if not already parsed
// The arena parameter is optional - if nil, uses heap allocation
func (r *Reader) ensureDedicatedColumnParsed(column string, arena *arena.Arena) error {
	// Already parsed?
	if _, ok := r.dedicatedIndex[column]; ok {
		return nil
	}

	// Get offset metadata
	meta, ok := r.dedicatedIndexOffsets[column]
	if !ok {
		return nil // Column doesn't exist - not an error
	}

	// Parse just this one column
	idx, bucketMeta, err := parseSingleDedicatedColumn(r.metadataBytes, meta.offset, meta.typ, arena)
	if err != nil {
		return fmt.Errorf("parse dedicated column %s: %w", column, err)
	}

	r.dedicatedIndex[column] = idx
	if bucketMeta != nil {
		r.rangeBucketMeta[column] = bucketMeta
	}
	return nil
}

// parseSingleDedicatedColumn parses one dedicated column's index from metadata
// The arena parameter is optional - if nil, uses heap allocation
func parseSingleDedicatedColumn(data []byte, offset int, typ ColumnType, arena *arena.Arena) (*dedicatedColumnIndex, *RangeBucketMetadata, error) {
	// Skip column name (already read in scan phase)
	if offset+2 > len(data) {
		return nil, nil, fmt.Errorf("truncated name length")
	}
	nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2 + nameLen + 1 // name + type byte

	// Parse bucket metadata if present
	if offset >= len(data) {
		return nil, nil, fmt.Errorf("truncated bucket flag")
	}
	hasBuckets := data[offset]
	offset++

	var bucketMeta *RangeBucketMetadata
	if hasBuckets == 1 {
		if offset+8+8+4 > len(data) {
			return nil, nil, fmt.Errorf("truncated bucket header")
		}
		min := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		max := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		boundaryCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+boundaryCount*8 > len(data) {
			return nil, nil, fmt.Errorf("truncated bucket boundaries")
		}
		var boundaries []int64
		if arena == nil {
			boundaries = make([]int64, boundaryCount)
		} else {
			boundaries = aslice.Make[int64](arena, boundaryCount).Raw()
		}
		for i := 0; i < boundaryCount; i++ {
			boundaries[i] = int64(binary.LittleEndian.Uint64(data[offset:]))
			offset += 8
		}
		bucketMeta = &RangeBucketMetadata{
			Min:        min,
			Max:        max,
			Boundaries: boundaries,
			ColumnType: typ,
		}
	}

	// Parse values
	if offset+4 > len(data) {
		return nil, nil, fmt.Errorf("truncated value count")
	}
	valueCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	values := make(map[string][]int, valueCount)
	for j := 0; j < valueCount; j++ {
		key, nextOffset, err := readDedicatedValue(typ, data, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("value %d: %w", j, err)
		}
		offset = nextOffset

		if offset+4 > len(data) {
			return nil, nil, fmt.Errorf("truncated block id count")
		}
		idCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+idCount*4 > len(data) {
			return nil, nil, fmt.Errorf("truncated block ids")
		}
		var ids []int
		if arena == nil {
			ids = make([]int, idCount)
		} else {
			ids = aslice.Make[int](arena, idCount).Raw()
		}
		for k := 0; k < idCount; k++ {
			ids[k] = int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
		}
		values[key.Encode()] = ids
	}

	return &dedicatedColumnIndex{
		typ:    typ,
		values: values,
	}, bucketMeta, nil
}

// Blocks returns block metadata (including bloom filters) for all blocks.
// Blocks are not parsed until GetBlock() is called.
func (r *Reader) Blocks() []*Block {
	// Return lightweight metadata blocks for filtering
	blocks := make([]*Block, len(r.blockEntries))
	for i, entry := range r.blockEntries {
		blocks[i] = &Block{
			spanCount:       int(entry.SpanCount),
			columns:         nil, // Not parsed yet
			MinStart:        entry.MinStart,
			MaxStart:        entry.MaxStart,
			MinTraceID:      entry.MinTraceID,
			MaxTraceID:      entry.MaxTraceID,
			ColumnNameBloom: entry.ColumnNameBloom,
		}
	}
	return blocks
}

// BlockValueStats returns the value statistics for a block (nil if not available)
func (r *Reader) BlockValueStats(blockIdx int) map[string]AttributeStats {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return nil
	}
	return r.blockEntries[blockIdx].ValueStats
}

// BlockSize returns the size in bytes of the specified block.
func (r *Reader) BlockSize(blockIdx int) (int64, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return 0, fmt.Errorf("block index %d out of range [0, %d)", blockIdx, len(r.blockEntries))
	}
	return int64(r.blockEntries[blockIdx].Length), nil
}

// BlockColumnIndex returns per-column metadata (name, offset, compressed size) for a block.
// Returns nil if block index is out of range.
func (r *Reader) BlockColumnIndex(blockIdx int) []ColumnIndexEntry {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return nil
	}
	return r.blockEntries[blockIdx].ColumnIndex
}

// MetadataOverhead returns the size of metadata that is always read (footer + metadata section)
// This is read even when all blocks are pruned, so should be included in bytes_read tracking
func (r *Reader) MetadataOverhead() int64 {
	return footerSize + int64(len(r.metadataBytes))
}

// BlockColumnStats returns per-column stats for the requested columns in a block
// without fully decoding column data.
func (r *Reader) BlockColumnStats(blockIdx int, columns []string) (map[string]ColumnStatsWithType, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return nil, fmt.Errorf("block index %d out of range [0, %d)", blockIdx, len(r.blockEntries))
	}
	entry := r.blockEntries[blockIdx]
	blockBytes, err := r.readRange(int64(entry.Offset), int(entry.Length))
	if err != nil {
		return nil, fmt.Errorf("block %d offset out of bounds: %w", blockIdx, err)
	}
	want := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		want[name] = struct{}{}
	}
	return parseBlockColumnStats(blockBytes, want)
}

// GetBlock parses and returns a block by index.
// Blocks are parsed lazily on first access and cached for subsequent calls.
func (r *Reader) GetBlock(idx int) (*Block, error) {
	return r.GetBlockColumns(idx, nil)
}

// BlockWithBytes holds a parsed block and its raw bytes for incremental column loading.
// This enables two-pass column loading: filter columns first, then result columns only for matches.
type BlockWithBytes struct {
	Block    *Block
	RawBytes []byte
	BlockIdx int
}

// GetBlockColumns retrieves a block with optional column filtering.
// If columns is nil, all columns are loaded.
// If columns is non-nil, only the specified columns are loaded.
// Blocks are NOT cached - each call reparses the block (optimized for sequential scans).
func (r *Reader) GetBlockColumns(idx int, columns map[string]struct{}) (*Block, error) {
	return r.GetBlockColumnsReuse(idx, columns, nil)
}

// GetBlockColumnsReuse is like GetBlockColumns but reuses a Block allocation if provided.
// Pass the same reusableBlock pointer in a loop to reuse allocations across iterations.
// This significantly reduces allocations during sequential scans.
// Decoded column data is heap-allocated (no arena support in this convenience function).
func (r *Reader) GetBlockColumnsReuse(idx int, columns map[string]struct{}, reusableBlock *Block) (*Block, error) {
	bwb, err := r.GetBlockWithBytes(idx, columns, reusableBlock, nil, nil)
	if err != nil {
		return nil, err
	}
	return bwb.Block, nil
}

// GetBlockWithBytes loads a block with specified columns and returns both the block and raw bytes.
// The raw bytes can be reused to load additional columns without re-reading from disk.
// If arena is provided, decoded column data will be allocated on the arena instead of heap.
func (r *Reader) GetBlockWithBytes(idx int, columns map[string]struct{}, reusableBlock *Block, cachedBytes []byte, queryArena *arena.Arena) (*BlockWithBytes, error) {
	if idx < 0 || idx >= len(r.blockEntries) {
		return nil, fmt.Errorf("block index %d out of range [0, %d)", idx, len(r.blockEntries))
	}

	entry := r.blockEntries[idx]
	if entry.Kind == blockEntryKindBlockpack {
		return nil, fmt.Errorf("block %d is a nested blockpack", idx)
	}

	// Reuse cached bytes if provided, otherwise read from disk
	blockBytes := cachedBytes
	if blockBytes == nil {
		var err error

		// Always read entire block for object storage efficiency.
		// Reading full blocks (one I/O operation) is much more efficient than
		// selective column reading (N I/O operations) on object storage where
		// each request has high latency (50-100ms) and fixed cost.
		// Reading 2-3x more data is cheaper than making 10-12x more requests.
		// See AGENTS.md for detailed rationale.
		blockBytes, err = r.readRange(int64(entry.Offset), int(entry.Length))
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", idx, err)
		}
	}

	block, err := parseBlockColumnsReuse(blockBytes, columns, reusableBlock, queryArena)
	if err != nil {
		return nil, fmt.Errorf("block %d: %w", idx, err)
	}

	// Attach metadata from index
	block.MinStart = entry.MinStart
	block.MaxStart = entry.MaxStart
	block.MinTraceID = entry.MinTraceID
	block.MaxTraceID = entry.MaxTraceID
	block.ColumnNameBloom = entry.ColumnNameBloom

	return &BlockWithBytes{
		Block:    block,
		RawBytes: blockBytes,
		BlockIdx: idx,
	}, nil
}

// IsBlockpackEntry returns true if the entry at blockIdx points to a nested blockpack payload.
func (r *Reader) IsBlockpackEntry(blockIdx int) bool {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return false
	}
	return r.blockEntries[blockIdx].Kind == blockEntryKindBlockpack
}

// SubReader creates a reader for the nested blockpack at blockIdx.
func (r *Reader) SubReader(blockIdx int) (*Reader, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return nil, fmt.Errorf("block index %d out of range [0, %d)", blockIdx, len(r.blockEntries))
	}
	entry := r.blockEntries[blockIdx]
	if entry.Kind != blockEntryKindBlockpack {
		return nil, fmt.Errorf("block %d is not a nested blockpack", blockIdx)
	}
	provider := newRangeReaderProvider(r.provider, int64(entry.Offset), int64(entry.Length))
	name := r.name
	if name != "" {
		name = fmt.Sprintf("%s[%d:%d]", name, entry.Offset, entry.Offset+entry.Length)
	}
	return NewReaderWithCache(name, provider, r.cache)
}

// CoalescedRead represents a single IO operation that reads multiple consecutive blocks
type CoalescedRead struct {
	StartBlockIdx int    // First block index (inclusive)
	EndBlockIdx   int    // Last block index (exclusive)
	Offset        uint64 // File offset to start reading
	Length        uint64 // Total bytes to read
}

// CoalesceConfig controls how blocks are coalesced into larger reads
type CoalesceConfig struct {
	MaxReadSize   uint64  // Maximum size of a single coalesced read (default: 4MB)
	MaxGapSize    uint64  // Maximum gap between blocks to coalesce (default: 1MB, 0 = only adjacent)
	MaxWasteRatio float64 // Maximum ratio of wasted bytes (gap / total read) (default: 0.5 = 50%)
}

// DefaultCoalesceConfig returns conservative defaults for bandwidth-constrained scenarios.
// Use this when bandwidth costs more than latency (rare - most should use Aggressive).
func DefaultCoalesceConfig() CoalesceConfig {
	return CoalesceConfig{
		MaxReadSize:   4 * 1024 * 1024, // 4MB max per read
		MaxGapSize:    1 * 1024 * 1024, // 1MB max gap
		MaxWasteRatio: 0.5,             // 50% max waste
	}
}

// AggressiveCoalesceConfig returns config that minimizes IO operations for S3/object storage.
// This is the recommended default since S3 latency (~20ms/IO) dominates over bandwidth costs.
// Trades bandwidth efficiency for fewer round-trips.
func AggressiveCoalesceConfig() CoalesceConfig {
	return CoalesceConfig{
		MaxReadSize:   4 * 1024 * 1024, // 4MB max per read
		MaxGapSize:    4 * 1024 * 1024, // Allow large gaps (up to MaxReadSize)
		MaxWasteRatio: 1.0,             // Allow 100% waste (no limit)
	}
}

// CoalesceBlocks groups adjacent blocks into larger reads to reduce IO operations.
// Uses aggressive coalescing by default to minimize IOs for S3/object storage.
// For backward compatibility, maxReadSize parameter is still accepted.
//
// For S3 workloads: fewer IOs is more important than bandwidth savings (use aggressive).
// For bandwidth-constrained scenarios: use CoalesceBlocksWithConfig with DefaultCoalesceConfig.
func (r *Reader) CoalesceBlocks(blockOrder []int, maxReadSize uint64) []CoalescedRead {
	config := AggressiveCoalesceConfig()
	if maxReadSize > 0 {
		config.MaxReadSize = maxReadSize
	}
	return r.CoalesceBlocksWithConfig(blockOrder, config)
}

// CoalesceBlocksWithConfig groups blocks into larger reads with configurable strategy.
// Blocks that are not adjacent or would exceed limits are placed in separate reads.
// This is critical for object storage (S3) where each IO operation has ~20ms latency.
//
// Adaptive coalescing skips large gaps to avoid reading excessive unwanted data:
// - Adjacent blocks (gap=0) are always coalesced
// - Small gaps (< MaxGapSize) are coalesced if waste ratio is acceptable
// - Large gaps cause separate reads
//
// Example: blocks [10, 50, 51, 90]
// - Aggressive: reads 10-90 (huge waste reading 11-49, 52-89)
// - Adaptive: reads [10], [50-51], [90] (3 IOs, no waste)
func (r *Reader) CoalesceBlocksWithConfig(blockOrder []int, config CoalesceConfig) []CoalescedRead {
	if len(blockOrder) == 0 {
		return nil
	}

	if config.MaxReadSize == 0 {
		config.MaxReadSize = 4 * 1024 * 1024 // Default 4MB
	}

	reads := make([]CoalescedRead, 0, len(blockOrder)/10) // Estimate ~10 blocks per read

	// Start first coalesced read
	startBlockIdx := blockOrder[0]
	startEntry := r.blockEntries[startBlockIdx]
	currentOffset := startEntry.Offset
	currentLength := startEntry.Length
	lastBlockIdx := startBlockIdx

	for i := 1; i < len(blockOrder); i++ {
		currBlockIdx := blockOrder[i]
		currEntry := r.blockEntries[currBlockIdx]
		lastEntry := r.blockEntries[lastBlockIdx]

		// Calculate gap between last block and current block
		expectedOffset := lastEntry.Offset + lastEntry.Length
		gapSize := uint64(0)
		if currEntry.Offset > expectedOffset {
			gapSize = currEntry.Offset - expectedOffset
		}
		isAdjacent := gapSize == 0

		// Decide whether to coalesce based on gap size and waste ratio
		shouldCoalesce := false
		if isAdjacent {
			// Always coalesce adjacent blocks
			shouldCoalesce = true
		} else if gapSize <= config.MaxGapSize {
			// Gap is reasonable, check waste ratio
			// Waste ratio = gap / (current data + gap + next block)
			totalWithGap := currentLength + gapSize + currEntry.Length
			wasteRatio := float64(gapSize) / float64(totalWithGap)

			shouldCoalesce = wasteRatio <= config.MaxWasteRatio
		}
		// If gap > MaxGapSize, shouldCoalesce stays false

		// Check if adding this block (with gap) would exceed size limit
		wouldExceedLimit := (currentLength + gapSize + currEntry.Length) > config.MaxReadSize

		if !shouldCoalesce || wouldExceedLimit {
			// Flush current coalesced read
			reads = append(reads, CoalescedRead{
				StartBlockIdx: startBlockIdx,
				EndBlockIdx:   lastBlockIdx + 1, // exclusive
				Offset:        currentOffset,
				Length:        currentLength,
			})

			// Start new coalesced read
			startBlockIdx = currBlockIdx
			currentOffset = currEntry.Offset
			currentLength = currEntry.Length
		} else {
			// Coalesce: include gap and next block
			currentLength += gapSize + currEntry.Length
		}

		lastBlockIdx = currBlockIdx
	}

	// Flush final coalesced read
	reads = append(reads, CoalescedRead{
		StartBlockIdx: startBlockIdx,
		EndBlockIdx:   lastBlockIdx + 1, // exclusive
		Offset:        currentOffset,
		Length:        currentLength,
	})

	return reads
}

// ReadCoalescedBlocks reads a single coalesced chunk and returns individual block bytes.
// Returns a map of blockIdx -> rawBytes for each block in the coalesced read.
//
// Caching strategy:
// 1. First checks cache for individual blocks (file:offset:length keys)
// 2. For cache misses, reads the entire coalesced chunk in one I/O
// 3. Caches both the coalesced chunk AND individual blocks within it
//
// This enables future queries needing just one block to hit cache instead of re-reading.
// Expected impact: 30-50% better cache hit rate for queries with overlapping block access.
func (r *Reader) ReadCoalescedBlocks(cr CoalescedRead) (map[int][]byte, error) {
	blockBytes := make(map[int][]byte, cr.EndBlockIdx-cr.StartBlockIdx)

	// First pass: check cache for individual blocks
	missingBlocks := make([]int, 0, cr.EndBlockIdx-cr.StartBlockIdx)
	for blockIdx := cr.StartBlockIdx; blockIdx < cr.EndBlockIdx; blockIdx++ {
		entry := r.blockEntries[blockIdx]

		// Try to get individual block from cache
		if r.cache != nil && r.name != "" {
			key := fmt.Sprintf("%s:%d:%d", r.name, entry.Offset, entry.Length)
			if data, found := r.cache.Get(key); found {
				blockBytes[blockIdx] = data
				continue
			}
		}

		// Cache miss - need to read this block
		missingBlocks = append(missingBlocks, blockIdx)
	}

	// If all blocks were cached, we're done
	if len(missingBlocks) == 0 {
		return blockBytes, nil
	}

	// Read the entire coalesced chunk in one I/O operation
	coalescedBytes, err := r.readRange(int64(cr.Offset), int(cr.Length))
	if err != nil {
		return nil, fmt.Errorf("read coalesced range [%d, %d): %w", cr.Offset, cr.Offset+cr.Length, err)
	}

	// Second pass: extract and cache individual blocks from coalesced chunk
	currentOffset := uint64(0)
	for blockIdx := cr.StartBlockIdx; blockIdx < cr.EndBlockIdx; blockIdx++ {
		entry := r.blockEntries[blockIdx]
		blockLength := entry.Length

		// Extract this block's bytes from the coalesced chunk
		// Note: this creates a slice backed by coalescedBytes, sharing memory
		blockData := coalescedBytes[currentOffset : currentOffset+blockLength]

		// Only add if not already in blockBytes (from cache hit)
		if _, exists := blockBytes[blockIdx]; !exists {
			blockBytes[blockIdx] = blockData

			// Cache individual block for future queries
			// Make a copy since blockData is a slice of coalescedBytes
			if r.cache != nil && r.name != "" {
				blockCopy := make([]byte, len(blockData))
				copy(blockCopy, blockData)
				key := fmt.Sprintf("%s:%d:%d", r.name, entry.Offset, entry.Length)
				r.cache.Put(key, blockCopy)
			}
		}

		currentOffset += blockLength
	}

	return blockBytes, nil
}

// AddColumnsToBlock adds additional columns to an existing block without re-reading from disk.
// This allows incremental column loading: load filter columns first, then add result columns only for matching blocks.
// If arena is provided, decoded column data is allocated on the arena.
func (r *Reader) AddColumnsToBlock(bwb *BlockWithBytes, additionalColumns map[string]struct{}, queryArena *arena.Arena) error {
	if bwb == nil || bwb.Block == nil {
		return fmt.Errorf("nil block")
	}

	// If additionalColumns is empty, nothing to add
	if len(additionalColumns) == 0 {
		return nil
	}

	// Filter out columns that are already loaded
	columnsToRead := make(map[string]struct{})
	for colName := range additionalColumns {
		if _, exists := bwb.Block.columns[colName]; !exists {
			columnsToRead[colName] = struct{}{}
		}
	}

	if len(columnsToRead) == 0 {
		// All requested columns already loaded
		return nil
	}

	// Read entire block to avoid re-reading header/metadata
	// Selective reading would save column data I/O but cost header/metadata I/O twice
	entry := r.blockEntries[bwb.BlockIdx]
	blockBytes, err := r.readRange(int64(entry.Offset), int(entry.Length))
	if err != nil {
		return fmt.Errorf("read block for additional columns: %w", err)
	}

	// Parse only the additional columns
	additionalBlock, err := parseBlockColumnsReuse(blockBytes, columnsToRead, nil, queryArena)
	if err != nil {
		return fmt.Errorf("parse additional columns: %w", err)
	}

	// Merge additional columns into existing block
	if bwb.Block.columns == nil {
		bwb.Block.columns = make(map[string]*Column)
	}
	for name, col := range additionalBlock.columns {
		// Only add if not already present
		if _, exists := bwb.Block.columns[name]; !exists {
			bwb.Block.columns[name] = col
		}
	}

	return nil
}

// SpanCount returns total spans across all blocks.
func (r *Reader) SpanCount() int {
	return r.totalSpans
}

// BlocksForServiceName returns block IDs that contain the service.name value.
func (r *Reader) BlocksForServiceName(val string) []int {
	return r.BlocksForDedicated("resource.service.name", StringValueKey(val), nil)
}

// BlocksForDedicated returns block IDs that contain the dedicated column value.
func (r *Reader) BlocksForDedicated(column string, key DedicatedValueKey, arena *arena.Arena) []int {
	// Lazy parse this column if needed
	if err := r.ensureDedicatedColumnParsed(column, arena); err != nil {
		return nil
	}

	entry, ok := r.dedicatedIndex[column]
	if !ok {
		return nil
	}
	if entry.typ != key.Type() {
		return nil
	}
	encoded := key.Encode()
	ids := entry.values[encoded]
	if arena == nil {
		out := make([]int, len(ids))
		copy(out, ids)
		return out
	}
	// Use arena allocation
	out := aslice.Make[int](arena, len(ids))
	copy(out.Raw(), ids)
	return out.Raw()
}

// HasTraceBlockIndex returns true if the blockpack file has a trace block index.
// This allows distinguishing between "no index available" vs "trace not found in index".
func (r *Reader) HasTraceBlockIndex() bool {
	return r.traceBlockIndex != nil
}

// BlocksForTraceID returns block IDs that contain spans for the given trace ID.
// Returns nil if the trace ID is not found in the index or if no index exists.
// Use HasTraceBlockIndex() to distinguish between these cases.
func (r *Reader) BlocksForTraceID(traceID [16]byte) []int {
	if r.traceBlockIndex == nil {
		return nil
	}
	blockIDs, ok := r.traceBlockIndex[traceID]
	if !ok {
		return nil
	}
	// Return a copy to avoid external modifications
	out := make([]int, len(blockIDs))
	copy(out, blockIDs)
	return out
}

// DedicatedColumnNames returns the list of all dedicated column names in the file.
func (r *Reader) DedicatedColumnNames() []string {
	result := make([]string, 0, len(r.dedicatedIndexOffsets))
	for name := range r.dedicatedIndexOffsets {
		result = append(result, name)
	}
	return result
}

// DedicatedColumnTypeFromFile returns the type of a dedicated column from the file's index.
// Returns (type, true) if the column exists in the dedicated index, (0, false) otherwise.
// This is different from DedicatedColumnType which only checks the explicit hardcoded list.
func (r *Reader) DedicatedColumnTypeFromFile(column string) (ColumnType, bool) {
	meta, ok := r.dedicatedIndexOffsets[column]
	if !ok {
		return 0, false
	}
	return meta.typ, true
}

// DedicatedValues returns all unique values for a dedicated column.
// This is useful for pattern matching (LIKE queries) where we need to check
// which values in the dedicated index match a pattern, then get their blocks.
func (r *Reader) DedicatedValues(column string) []DedicatedValueKey {
	// Lazy parse this column if needed
	if err := r.ensureDedicatedColumnParsed(column, nil); err != nil {
		return nil
	}

	entry, ok := r.dedicatedIndex[column]
	if !ok {
		return nil
	}

	result := make([]DedicatedValueKey, 0, len(entry.values))
	for encodedKey := range entry.values {
		key, err := DecodeDedicatedKey(encodedKey)
		if err != nil {
			continue
		}
		result = append(result, key)
	}
	return result
}

// GetRangeBucketMetadata returns bucket metadata for a range-bucketed dedicated column
func (r *Reader) GetRangeBucketMetadata(column string) (*RangeBucketMetadata, error) {
	// Ensure column is parsed
	if err := r.ensureDedicatedColumnParsed(column, nil); err != nil {
		return nil, err
	}

	// Return bucket metadata if available
	meta, ok := r.rangeBucketMeta[column]
	if !ok {
		return nil, nil // Not a range-bucketed column
	}
	return meta, nil
}

// BlocksForDedicatedRegex returns block IDs that contain dedicated column values
// matching the given regex pattern. This enables efficient LIKE queries on dedicated
// columns by matching the pattern against the global dictionary (typically 10-100 values)
// rather than scanning all blocks.
func (r *Reader) BlocksForDedicatedRegex(column string, pattern *regexp.Regexp, arena *arena.Arena) []int {
	// Lazy parse this column if needed
	if err := r.ensureDedicatedColumnParsed(column, arena); err != nil {
		return nil
	}

	entry, ok := r.dedicatedIndex[column]
	if !ok {
		return nil
	}

	// Only support string columns for regex matching
	if entry.typ != ColumnTypeString {
		return nil
	}

	// Build set of matching blocks (use map to deduplicate)
	blockSet := make(map[int]struct{})

	// Match pattern against all values in the dedicated index
	for encodedKey, blockIDs := range entry.values {
		key, err := DecodeDedicatedKey(encodedKey)
		if err != nil {
			continue
		}

		// Extract string value
		value := string(key.Data())

		// Check if value matches pattern
		if pattern.MatchString(value) {
			// Add all blocks containing this value
			for _, blockID := range blockIDs {
				blockSet[blockID] = struct{}{}
			}
		}
	}

	// Convert to sorted slice
	if arena == nil {
		result := make([]int, 0, len(blockSet))
		for blockID := range blockSet {
			result = append(result, blockID)
		}
		sort.Ints(result)
		return result
	}
	// Use arena allocation
	result := aslice.Make[int](arena, 0)
	for blockID := range blockSet {
		result = result.AppendOne(arena, blockID)
	}
	resultRaw := result.Raw()
	sort.Ints(resultRaw)
	return resultRaw
}

// readDedicatedValue parses a dedicated value while decoding the metadata index.
func readDedicatedValue(typ ColumnType, data []byte, offset int) (DedicatedValueKey, int, error) {
	switch typ {
	case ColumnTypeString:
		if offset+4 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated string length")
		}
		l := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+l > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated string value")
		}
		val := string(data[offset : offset+l])
		offset += l
		return StringValueKey(val), offset, nil
	case ColumnTypeBytes:
		if offset+4 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated bytes length")
		}
		l := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+l > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated bytes value")
		}
		cp := make([]byte, l)
		copy(cp, data[offset:offset+l])
		offset += l
		return BytesValueKey(cp), offset, nil
	case ColumnTypeInt64:
		if offset+8 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated int64 value")
		}
		v := int64(binary.LittleEndian.Uint64(data[offset:]))
		return IntValueKey(v), offset + 8, nil
	case ColumnTypeUint64:
		if offset+8 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated uint64 value")
		}
		v := binary.LittleEndian.Uint64(data[offset:])
		return UintValueKey(v), offset + 8, nil
	case ColumnTypeFloat64:
		if offset+8 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated float64 value")
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		return FloatValueKey(v), offset + 8, nil
	case ColumnTypeBool:
		if offset+1 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated bool value")
		}
		return BoolValueKey(data[offset] != 0), offset + 1, nil
	case ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration:
		// Range columns can be either:
		// - 2 bytes: bucket ID (uint16) for bucketed columns
		// - 8 bytes: raw value (int64) for direct value indexing (low cardinality)
		// Read length prefix to determine which
		if offset+1 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated range key length")
		}
		length := int(data[offset])
		offset++

		switch length {
		case 2:
			// Bucket ID
			if offset+2 > len(data) {
				return DedicatedValueKey{}, 0, fmt.Errorf("truncated range bucket id")
			}
			bucketID := binary.LittleEndian.Uint16(data[offset:])
			return RangeBucketValueKey(bucketID, typ), offset + 2, nil
		case 8:
			// Raw value (direct indexing)
			if offset+8 > len(data) {
				return DedicatedValueKey{}, 0, fmt.Errorf("truncated range value")
			}
			value := int64(binary.LittleEndian.Uint64(data[offset:]))
			return RangeInt64ValueKey(value, typ), offset + 8, nil
		default:
			return DedicatedValueKey{}, 0, fmt.Errorf("invalid range key length %d", length)
		}
	default:
		return DedicatedValueKey{}, 0, fmt.Errorf("unsupported dedicated type %d", typ)
	}
}

// parseBlockColumnsReuse parses a block, optionally reusing a Block allocation.
// If reusable is nil, allocates a new Block.
// If reusable is non-nil, clears and reuses it (for sequential scans).
// If arena is provided, decoded column slices are allocated on the arena instead of heap.
func parseBlockColumnsReuse(data []byte, want map[string]struct{}, reusable *Block, queryArena *arena.Arena) (*Block, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("data too small for block header")
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magicNumber {
		return nil, fmt.Errorf("invalid magic in block")
	}

	version := data[4]
	if version != versionV10 && version != versionV11 {
		return nil, fmt.Errorf("unsupported block version %d (only v10/v11 supported)", version)
	}

	if len(data) < 24 {
		return nil, fmt.Errorf("data too small for block header")
	}

	spanCount := int(binary.LittleEndian.Uint32(data[8:12]))
	columnCount := int(binary.LittleEndian.Uint32(data[12:16]))
	traceCount := int(binary.LittleEndian.Uint32(data[16:20]))
	traceTableLen := int(binary.LittleEndian.Uint32(data[20:24]))

	offset := 24
	metas := make([]columnMeta, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("truncated column name length at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen+1+32 > len(data) {
			return nil, fmt.Errorf("truncated metadata at column %d", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typ := ColumnType(data[offset])
		offset++
		// Read column metadata: data location (offset + length) and stats location
		// This allows columns to be stored non-contiguously in the file for better compression
		dataOffset := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		dataLen := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		statsOffset := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		statsLen := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		// Skip columns not in want set (if want is specified)
		if want != nil {
			if _, ok := want[name]; !ok {
				continue
			}
		}

		metas = append(metas, columnMeta{
			name:        name,
			typ:         typ,
			dataOffset:  int(dataOffset),
			dataLen:     int(dataLen),
			statsOffset: int(statsOffset),
			statsLen:    int(statsLen),
		})
	}

	// Reuse Block and columns map if provided, otherwise allocate new
	var block *Block
	var columns map[string]*Column
	var existingColumns map[string]*Column

	if reusable != nil {
		block = reusable
		columns = block.columns
		// Initialize columns map if nil (handles zero-value Block)
		if columns == nil {
			columns = make(map[string]*Column, len(metas))
			block.columns = columns
		} else {
			// Save existing columns for reuse, then clear map
			existingColumns = make(map[string]*Column, len(columns))
			for k, v := range columns {
				existingColumns[k] = v
			}
			// Clear map entries (but keep Column objects in existingColumns for reuse)
			for k := range columns {
				delete(columns, k)
			}
		}
	} else {
		block = &Block{}
		columns = make(map[string]*Column, len(metas))
		block.columns = columns
	}

	block.spanCount = spanCount

	// Separate span-level and trace-level columns
	spanMetas := make([]columnMeta, 0, len(metas))
	traceMetas := make([]columnMeta, 0)
	for _, meta := range metas {
		// Columns with dataLen=0 are trace-level (stats only in metadata)
		if meta.dataLen == 0 {
			traceMetas = append(traceMetas, meta)
		} else {
			spanMetas = append(spanMetas, meta)
		}
	}

	// Parse span-level columns
	maxDataEnd, err := parseSpanColumns(spanMetas, data, spanCount, existingColumns, queryArena, columns)
	if err != nil {
		return nil, err
	}
	// Parse trace table and expand trace-level columns
	if err := parseTraceColumns(traceMetas, data, maxDataEnd, traceTableLen, traceCount, spanCount, columns); err != nil {
		return nil, err
	}

	return block, nil
}

func parseBlockColumnStats(data []byte, want map[string]struct{}) (map[string]ColumnStatsWithType, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("data too small for block header")
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magicNumber {
		return nil, fmt.Errorf("invalid magic in block")
	}
	if data[4] != versionV10 && data[4] != versionV11 {
		return nil, fmt.Errorf("unsupported block version %d (only v10/v11 supported)", data[4])
	}

	columnCount := int(binary.LittleEndian.Uint32(data[12:16]))
	offset := 24
	out := make(map[string]ColumnStatsWithType, len(want))

	for i := 0; i < columnCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("truncated column name length at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen+1+32 > len(data) {
			return nil, fmt.Errorf("truncated metadata at column %d", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typ := ColumnType(data[offset])
		offset++
		offset += 8 // dataOffset
		offset += 8 // dataLen
		statsOffset := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
		statsLen := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		if _, ok := want[name]; !ok {
			continue
		}
		statsEnd := statsOffset + statsLen
		if statsOffset < 0 || statsEnd > len(data) {
			return nil, fmt.Errorf("column %s stats out of bounds", name)
		}
		statsData := data[statsOffset:statsEnd]
		stats, err := parseColumnStats(typ, statsData)
		if err != nil {
			return nil, fmt.Errorf("column %s stats: %w", name, err)
		}
		out[name] = ColumnStatsWithType{Type: typ, Stats: stats}
	}

	return out, nil
}

type columnMeta struct {
	name        string
	typ         ColumnType
	dataOffset  int
	dataLen     int
	statsOffset int
	statsLen    int
}

// parseTraceTable deserializes the trace table from blocks
func parseTraceTable(data []byte, expectedTraces int) (map[string]*Column, error) {
	rd := &sliceReader{data: data}

	traceCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read trace count: %w", err)
	}
	if int(traceCount) != expectedTraces {
		return nil, fmt.Errorf("trace count mismatch: got %d, expected %d", traceCount, expectedTraces)
	}

	columnCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	traceColumns := make(map[string]*Column, columnCount)

	for i := 0; i < int(columnCount); i++ {
		nameLen, err := rd.readUint16()
		if err != nil {
			return nil, fmt.Errorf("failed to read name length for column %d: %w", i, err)
		}

		nameBytes, err := rd.readBytes(int(nameLen))
		if err != nil {
			return nil, fmt.Errorf("failed to read name for column %d: %w", i, err)
		}
		name := string(nameBytes)

		typ, err := rd.readUint8()
		if err != nil {
			return nil, fmt.Errorf("failed to read type for column %s: %w", name, err)
		}

		dataLen, err := rd.readUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read data length for column %s: %w", name, err)
		}

		colData, err := rd.readBytes(int(dataLen))
		if err != nil {
			return nil, fmt.Errorf("failed to read data for column %s: %w", name, err)
		}

		// Parse column (trace table columns have traceCount rows, no stats)
		// Use nil arena since trace table is metadata that lives beyond query scope
		col, err := parseColumnReuse(name, ColumnType(typ), int(traceCount), colData, nil, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to parse column %s: %w", name, err)
		}

		traceColumns[name] = col
	}

	return traceColumns, nil
}

// expandTraceColumn expands a trace-level column to span-level using the trace.index column
// expandTraceColumnString expands string trace-level column to span-level.
func expandTraceColumnString(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.StringDict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.StringValue(int(traceIdx)); ok {
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx)
					setBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetStringData(dict, values, present)
}

// expandTraceColumnInt64 expands int64 trace-level column to span-level.
func expandTraceColumnInt64(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.Int64Dict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.Int64Value(int(traceIdx)); ok {
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx)
					setBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetInt64Data(dict, values, present)
}

// expandTraceColumnUint64 expands uint64 trace-level column to span-level.
func expandTraceColumnUint64(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.Uint64Dict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.Uint64Value(int(traceIdx)); ok {
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx)
					setBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetUint64Data(dict, values, present)
}

// expandTraceColumnBool expands bool trace-level column to span-level.
func expandTraceColumnBool(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.BoolDict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.BoolValue(int(traceIdx)); ok {
			for dictIdx, dictVal := range dict {
				if (dictVal == 1) == val {
					values[spanIdx] = uint32(dictIdx)
					setBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetBoolData(dict, values, present)
}

// expandTraceColumnFloat64 expands float64 trace-level column to span-level.
func expandTraceColumnFloat64(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.Float64Dict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.Float64Value(int(traceIdx)); ok {
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx)
					setBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetFloat64Data(dict, values, present)
}

// expandTraceColumnBytes expands bytes trace-level column to span-level.
func expandTraceColumnBytes(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	if inline := traceCol.BytesInline(); inline != nil {
		values := make([][]byte, spanCount)
		for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
			traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
			if !ok {
				continue
			}
			if val, ok := traceCol.BytesValueView(int(traceIdx)); ok {
				values[spanIdx] = val
				setBit(present, spanIdx)
			}
		}
		spanCol.SetBytesInlineData(values, present)
	} else {
		dict, _ := traceCol.BytesDict()
		values := make([]uint32, spanCount)
		for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
			traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
			if !ok {
				continue
			}
			if val, ok := traceCol.BytesValueView(int(traceIdx)); ok {
				for dictIdx, dictVal := range dict {
					if bytes.Equal(dictVal, val) {
						values[spanIdx] = uint32(dictIdx)
						setBit(present, spanIdx)
						break
					}
				}
			}
		}
		spanCol.SetBytesDictData(dict, values, present)
	}
}

func expandTraceColumn(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int) error {
	present := make([]byte, bitsLen(spanCount))
	// NOTE: Don't copy stats from traceCol - caller has already set stats from metadata.

	switch spanCol.Type {
	case ColumnTypeString:
		expandTraceColumnString(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeInt64:
		expandTraceColumnInt64(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeUint64:
		expandTraceColumnUint64(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeBool:
		expandTraceColumnBool(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeFloat64:
		expandTraceColumnFloat64(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeBytes:
		expandTraceColumnBytes(spanCol, traceCol, traceIndexCol, spanCount, present)
	default:
		return fmt.Errorf("unsupported column type: %d", spanCol.Type)
	}

	return nil
}

func parseColumnStats(typ ColumnType, data []byte) (ColumnStats, error) {
	if len(data) < 1 {
		return ColumnStats{}, fmt.Errorf("column stats truncated")
	}
	hasValues := data[0] == 1

	stats := ColumnStats{
		HasValues: hasValues,
	}
	if !hasValues {
		return stats, nil
	}

	switch typ {
	case ColumnTypeString:
		min, next, err := readSizedBytes(data, 1)
		if err != nil {
			return ColumnStats{}, err
		}
		max, _, err := readSizedBytes(data, next)
		if err != nil {
			return ColumnStats{}, err
		}
		stats.StringMin = string(min)
		stats.StringMax = string(max)
	case ColumnTypeBytes:
		min, next, err := readSizedBytes(data, 1)
		if err != nil {
			return ColumnStats{}, err
		}
		max, _, err := readSizedBytes(data, next)
		if err != nil {
			return ColumnStats{}, err
		}
		stats.BytesMin = min
		stats.BytesMax = max
	case ColumnTypeInt64:
		offset := 1
		if offset+16 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for int64")
		}
		stats.IntMin = int64(binary.LittleEndian.Uint64(data[offset:]))
		stats.IntMax = int64(binary.LittleEndian.Uint64(data[offset+8:]))
	case ColumnTypeUint64:
		offset := 1
		if offset+16 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for uint64")
		}
		stats.UintMin = binary.LittleEndian.Uint64(data[offset:])
		stats.UintMax = binary.LittleEndian.Uint64(data[offset+8:])
	case ColumnTypeFloat64:
		offset := 1
		if offset+16 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for float64")
		}
		stats.FloatMin = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		stats.FloatMax = math.Float64frombits(binary.LittleEndian.Uint64(data[offset+8:]))
	case ColumnTypeBool:
		offset := 1
		if offset+2 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for bool")
		}
		stats.BoolMin = data[offset] != 0
		stats.BoolMax = data[offset+1] != 0
	}

	return stats, nil
}

func readSizedBytes(data []byte, offset int) ([]byte, int, error) {
	if offset+4 > len(data) {
		return nil, offset, fmt.Errorf("truncated sized bytes length")
	}
	l := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if offset+l > len(data) {
		return nil, offset, fmt.Errorf("truncated sized bytes value")
	}
	val := make([]byte, l)
	copy(val, data[offset:offset+l])
	return val, offset + l, nil
}

// parseTraceColumns parses trace-level columns from the trace table and expands them to span-level.
func parseTraceColumns(traceMetas []columnMeta, data []byte, maxDataEnd, traceTableLen, traceCount, spanCount int, columns map[string]*Column) error {
	if len(traceMetas) == 0 {
		return nil
	}

	// Find trace table data (after all span column data)
	if maxDataEnd+traceTableLen > len(data) {
		return fmt.Errorf("trace table out of bounds")
	}
	traceTableData := data[maxDataEnd : maxDataEnd+traceTableLen]

	// Parse trace table
	traceTable, err := parseTraceTable(traceTableData, traceCount)
	if err != nil {
		return fmt.Errorf("failed to parse trace table: %w", err)
	}

	// Get trace.index column to map spans to traces
	traceIndexCol, ok := columns["trace.index"]
	if !ok {
		return fmt.Errorf("block missing trace.index column")
	}

	// Expand trace-level columns to span-level using trace.index
	for _, meta := range traceMetas {
		traceCol, ok := traceTable[meta.name]
		if !ok {
			continue
		}
		// Load stats for trace-level column
		statsEnd := meta.statsOffset + meta.statsLen
		if meta.statsOffset < 0 || statsEnd > len(data) {
			return fmt.Errorf("trace column %s stats out of bounds", meta.name)
		}
		statsData := data[meta.statsOffset:statsEnd]
		stats, err := parseColumnStats(meta.typ, statsData)
		if err != nil {
			return fmt.Errorf("trace column %s stats: %w", meta.name, err)
		}

		spanCol := &Column{
			Name:  meta.name,
			Type:  meta.typ,
			Stats: stats,
		}
		if err := expandTraceColumn(spanCol, traceCol, traceIndexCol, spanCount); err != nil {
			return fmt.Errorf("failed to expand trace column %s: %w", meta.name, err)
		}
		columns[meta.name] = spanCol
	}
	return nil
}

// parseSpanColumns parses span-level columns from metadata and populates the columns map.
// Returns the maximum data end offset needed for trace table parsing.
func parseSpanColumns(spanMetas []columnMeta, data []byte, spanCount int, existingColumns map[string]*Column, queryArena *arena.Arena, columns map[string]*Column) (int, error) {
	maxDataEnd := 0
	for _, meta := range spanMetas {
		end := meta.dataOffset + meta.dataLen
		if meta.dataOffset < 0 || end > len(data) {
			return 0, fmt.Errorf("column %s data out of bounds", meta.name)
		}
		if end > maxDataEnd {
			maxDataEnd = end
		}
		statsEnd := meta.statsOffset + meta.statsLen
		if meta.statsOffset < 0 || statsEnd > len(data) {
			return 0, fmt.Errorf("column %s stats out of bounds", meta.name)
		}
		colData := data[meta.dataOffset:end]
		statsData := data[meta.statsOffset:statsEnd]

		// Try to reuse existing column allocation
		var reusableCol *Column
		if existingColumns != nil {
			reusableCol = existingColumns[meta.name]
		}

		col, err := parseColumnReuse(meta.name, meta.typ, spanCount, colData, statsData, reusableCol, queryArena)
		if err != nil {
			return 0, err
		}
		columns[meta.name] = col
	}
	return maxDataEnd, nil
}

// BOT: Move this to own file
// BOT: Can slice reader implement more..friendly names wrappers?
type sliceReader struct {
	data   []byte
	offset int
}

func (sr *sliceReader) readUint8() (uint8, error) {
	if sr.offset+1 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint8")
	}
	val := sr.data[sr.offset]
	sr.offset++
	return val, nil
}

func (sr *sliceReader) readBytes(n int) ([]byte, error) {
	if n < 0 || sr.offset+n > len(sr.data) {
		return nil, fmt.Errorf("truncated buffer")
	}
	out := sr.data[sr.offset : sr.offset+n]
	sr.offset += n
	return out, nil
}

func (sr *sliceReader) readUint32() (uint32, error) {
	if sr.offset+4 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint32")
	}
	val := binary.LittleEndian.Uint32(sr.data[sr.offset : sr.offset+4])
	sr.offset += 4
	return val, nil
}

func (sr *sliceReader) readInt32() (int32, error) {
	if sr.offset+4 > len(sr.data) {
		return 0, fmt.Errorf("truncated int32")
	}
	val := int32(binary.LittleEndian.Uint32(sr.data[sr.offset : sr.offset+4]))
	sr.offset += 4
	return val, nil
}

func (sr *sliceReader) readInt64() (int64, error) {
	if sr.offset+8 > len(sr.data) {
		return 0, fmt.Errorf("truncated int64")
	}
	val := int64(binary.LittleEndian.Uint64(sr.data[sr.offset : sr.offset+8]))
	sr.offset += 8
	return val, nil
}

func (sr *sliceReader) readUint64() (uint64, error) {
	if sr.offset+8 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint64")
	}
	val := binary.LittleEndian.Uint64(sr.data[sr.offset : sr.offset+8])
	sr.offset += 8
	return val, nil
}

func (sr *sliceReader) readFloat64() (float64, error) {
	if sr.offset+8 > len(sr.data) {
		return 0, fmt.Errorf("truncated float64")
	}
	bits := binary.LittleEndian.Uint64(sr.data[sr.offset : sr.offset+8])
	sr.offset += 8
	return math.Float64frombits(bits), nil
}

func (sr *sliceReader) readFixedWidth(width uint8) (uint32, error) {
	switch width {
	case 1:
		v, err := sr.readUint8()
		return uint32(v), err
	case 2:
		v, err := sr.readUint16()
		return uint32(v), err
	case 4:
		v, err := sr.readUint32()
		return v, err
	default:
		return 0, fmt.Errorf("unsupported width %d", width)
	}
}

func (sr *sliceReader) readUint16() (uint16, error) {
	if sr.offset+2 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint16")
	}
	val := binary.LittleEndian.Uint16(sr.data[sr.offset : sr.offset+2])
	sr.offset += 2
	return val, nil
}

func (sr *sliceReader) remaining() int {
	return len(sr.data) - sr.offset
}

func readPresence(rd *sliceReader, rows int) ([]byte, error) {
	length, err := rd.readUint32()
	if err != nil {
		return nil, err
	}
	data, err := rd.readBytes(int(length))
	if err != nil {
		return nil, err
	}
	return decodePresenceRLE(data, rows)
}

func readBlockIndexEntry(rd *bytes.Reader, version uint8) (blockIndexEntry, error) {
	var e blockIndexEntry
	if err := binary.Read(rd, binary.LittleEndian, &e.Offset); err != nil {
		return e, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.Length); err != nil {
		return e, err
	}
	if version >= versionV11 {
		kind, err := rd.ReadByte()
		if err != nil {
			return e, err
		}
		e.Kind = blockEntryKind(kind)
	} else {
		e.Kind = blockEntryKindLeaf
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.SpanCount); err != nil {
		return e, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.MinStart); err != nil {
		return e, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.MaxStart); err != nil {
		return e, err
	}
	if _, err := rd.Read(e.MinTraceID[:]); err != nil {
		return e, err
	}
	if _, err := rd.Read(e.MaxTraceID[:]); err != nil {
		return e, err
	}
	if _, err := rd.Read(e.ColumnNameBloom[:]); err != nil {
		return e, err
	}

	// Read value statistics for v10 format
	if version >= versionV10 {
		valueStats, err := readValueStats(rd)
		if err != nil {
			return e, fmt.Errorf("read value stats: %w", err)
		}
		e.ValueStats = valueStats
	}

	return e, nil
}

// ReadMetricStreamBlocks reads and parses metric stream blocks from the blockpack file.
// Returns metricStreamOffset and metricStreamLen.
//
// For full deserialization of metric streams, use ReadMetricStreamData() or ReadAllMetricStreams().
func (r *Reader) ReadMetricStreamBlocks() (offset uint64, length uint64, error error) {
	// NOTE: Full binary deserialization not yet implemented (blockpack-bd1)
	// For now, just return the offset and length so callers can access raw bytes
	if r.provider == nil {
		return 0, 0, fmt.Errorf("no data provider available")
	}

	// Read footer to get metric stream offset/length
	dataSize, err := r.provider.Size()
	if err != nil {
		return 0, 0, err
	}

	footerBytes, err := r.readProviderRange(dataSize-int64(footerSize), int(footerSize))
	if err != nil {
		return 0, 0, err
	}

	footer, err := readFooter(footerBytes)
	if err != nil {
		return 0, 0, err
	}

	if footer.version != versionV10 && footer.version != versionV11 {
		return 0, 0, fmt.Errorf("aggregate blocks only available in v10/v11 format")
	}

	return footer.metricStreamOffset, footer.metricStreamLen, nil
}

// GetColumnSizes returns the compressed size in bytes for each column across all blocks.
// Returns a map of column name to total compressed size.
func (r *Reader) GetColumnSizes() map[string]int64 {
	columnSizes := make(map[string]int64)

	for _, entry := range r.blockEntries {
		for _, colEntry := range entry.ColumnIndex {
			columnSizes[colEntry.Name] += int64(colEntry.Length)
		}
	}

	return columnSizes
}

// GetMetricStreamBlocksRaw returns the raw bytes of metric stream blocks.
// This is a temporary helper until full deserialization is implemented.
func (r *Reader) GetMetricStreamBlocksRaw() ([]byte, error) {
	offset, length, err := r.ReadMetricStreamBlocks()
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	}

	return r.readProviderRange(int64(offset), int(length))
}

// MetricStreamInfo contains metadata about a compressed metric stream
type MetricStreamInfo struct {
	StreamID         string
	Offset           int64
	CompressionFlag  uint8
	UncompressedLen  uint32
	CompressedLen    uint32
	CompressedOffset int64
}

// ReadMetricStreamHeaders parses metric stream block headers to find all streams
// without decompressing the data. Returns a map of streamID -> stream info.
func (r *Reader) ReadMetricStreamHeaders() (map[string]*MetricStreamInfo, error) {
	// Get raw metric stream blocks
	rawData, err := r.GetMetricStreamBlocksRaw()
	if err != nil {
		return nil, err
	}

	if len(rawData) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(rawData)
	streams := make(map[string]*MetricStreamInfo)

	// Read stream count
	var streamCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &streamCount); err != nil {
		return nil, fmt.Errorf("failed to read stream count: %w", err)
	}

	// Parse each stream header
	for i := uint32(0); i < streamCount; i++ {
		offset := int64(len(rawData)) - int64(buf.Len())

		// Read stream ID
		var streamIDLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &streamIDLen); err != nil {
			return nil, fmt.Errorf("failed to read stream ID length: %w", err)
		}

		streamIDBytes := make([]byte, streamIDLen)
		if _, err := io.ReadFull(buf, streamIDBytes); err != nil {
			return nil, fmt.Errorf("failed to read stream ID: %w", err)
		}
		streamID := string(streamIDBytes)

		// Read compression flag
		var compressionFlag uint8
		if err := binary.Read(buf, binary.LittleEndian, &compressionFlag); err != nil {
			return nil, fmt.Errorf("failed to read compression flag: %w", err)
		}

		// Read uncompressed length
		var uncompressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &uncompressedLen); err != nil {
			return nil, fmt.Errorf("failed to read uncompressed length: %w", err)
		}

		// Read compressed length
		var compressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
			return nil, fmt.Errorf("failed to read compressed length: %w", err)
		}

		compressedOffset := int64(len(rawData)) - int64(buf.Len())

		streams[streamID] = &MetricStreamInfo{
			StreamID:         streamID,
			Offset:           offset,
			CompressionFlag:  compressionFlag,
			UncompressedLen:  uncompressedLen,
			CompressedLen:    compressedLen,
			CompressedOffset: compressedOffset,
		}

		// Skip compressed data
		if _, err := buf.Seek(int64(compressedLen), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("failed to skip compressed data: %w", err)
		}
	}

	return streams, nil
}

// MetricStreamData represents a fully deserialized metric stream with all time buckets and aggregates.
type MetricStreamData struct {
	StreamID      string
	Query         string
	StepSizeNanos int64
	StartTime     int64 // timestamp of first bucket
	StepMillis    uint32
	GroupKeys     [][]MetricStreamValue // unique group key combinations
	Buckets       []MetricStreamBucket  // time-ordered buckets
}

// MetricStreamBucket represents aggregates for a single time bucket.
type MetricStreamBucket struct {
	BucketTime int64                          // implicit: start_time + bucket_index * step_millis * 1_000_000
	Groups     [][]MetricStreamAggregateValue // group_index -> aggregate values
}

// MetricStreamValue represents a single value in a group key.
type MetricStreamValue struct {
	Type  uint8       // 0=String, 1=Int64, 2=Float64, 3=Bool
	Value interface{} // actual value
}

// MetricStreamAggregateValue represents a single aggregate value (Count, Sum, Min, Max, P50, P95, P99).
type MetricStreamAggregateValue struct {
	Type  uint8 // 1=Count, 2=Sum, 3=Min, 4=Max, 5=P50, 6=P95, 7=P99
	Value float64
}

// ReadMetricStream reads and decompresses a specific metric stream by ID.
// Returns the uncompressed stream data (query, step_size, buckets, etc.).
func (r *Reader) ReadMetricStream(streamID string) ([]byte, error) {
	// Get stream headers
	streams, err := r.ReadMetricStreamHeaders()
	if err != nil {
		return nil, err
	}

	streamInfo, ok := streams[streamID]
	if !ok {
		return nil, fmt.Errorf("stream %s not found", streamID)
	}

	// Get raw metric stream blocks
	rawData, err := r.GetMetricStreamBlocksRaw()
	if err != nil {
		return nil, err
	}

	// Extract compressed data for this stream
	compressedData := rawData[streamInfo.CompressedOffset : streamInfo.CompressedOffset+int64(streamInfo.CompressedLen)]

	// Decompress based on compression flag
	switch streamInfo.CompressionFlag {
	case 0: // CompressionNone
		return compressedData, nil

	case 1: // CompressionZstd
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		defer decoder.Close()

		uncompressedData, err := decoder.DecodeAll(compressedData, make([]byte, 0, streamInfo.UncompressedLen))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress stream data: %w", err)
		}

		// Validate uncompressed size
		if uint32(len(uncompressedData)) != streamInfo.UncompressedLen {
			return nil, fmt.Errorf("uncompressed size mismatch: expected %d, got %d",
				streamInfo.UncompressedLen, len(uncompressedData))
		}

		return uncompressedData, nil

	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", streamInfo.CompressionFlag)
	}
}

// ReadMetricStreamData reads and fully deserializes a metric stream by ID.
// Returns structured data with all time buckets, group keys, and aggregate values.
func (r *Reader) ReadMetricStreamData(streamID string) (*MetricStreamData, error) {
	// Get uncompressed stream data
	data, err := r.ReadMetricStream(streamID)
	if err != nil {
		return nil, err
	}

	// Parse the binary data
	return parseMetricStreamData(streamID, data)
}

// ReadAllMetricStreams reads and deserializes all metric streams in the blockpack.
// Returns a map of streamID -> MetricStreamData.
func (r *Reader) ReadAllMetricStreams() (map[string]*MetricStreamData, error) {
	// Get stream headers
	streams, err := r.ReadMetricStreamHeaders()
	if err != nil {
		return nil, err
	}

	if len(streams) == 0 {
		return nil, nil
	}

	result := make(map[string]*MetricStreamData, len(streams))
	for streamID := range streams {
		data, err := r.ReadMetricStreamData(streamID)
		if err != nil {
			return nil, fmt.Errorf("failed to read stream %s: %w", streamID, err)
		}
		result[streamID] = data
	}

	return result, nil
}

// parseMetricStreamData parses the binary format of a decompressed metric stream.
func parseMetricStreamData(streamID string, data []byte) (*MetricStreamData, error) {
	buf := bytes.NewReader(data)

	result := &MetricStreamData{
		StreamID: streamID,
	}

	// Read query
	var queryLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &queryLen); err != nil {
		return nil, fmt.Errorf("failed to read query length: %w", err)
	}
	queryBytes := make([]byte, queryLen)
	if _, err := io.ReadFull(buf, queryBytes); err != nil {
		return nil, fmt.Errorf("failed to read query: %w", err)
	}
	result.Query = string(queryBytes)

	// Read step size
	if err := binary.Read(buf, binary.LittleEndian, &result.StepSizeNanos); err != nil {
		return nil, fmt.Errorf("failed to read step size: %w", err)
	}

	// Read bucket count
	var bucketCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &bucketCount); err != nil {
		return nil, fmt.Errorf("failed to read bucket count: %w", err)
	}

	// Read start time
	if err := binary.Read(buf, binary.LittleEndian, &result.StartTime); err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}

	// Read step millis
	if err := binary.Read(buf, binary.LittleEndian, &result.StepMillis); err != nil {
		return nil, fmt.Errorf("failed to read step millis: %w", err)
	}

	// Read group key count
	var groupKeyCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &groupKeyCount); err != nil {
		return nil, fmt.Errorf("failed to read group key count: %w", err)
	}

	// Read group keys
	result.GroupKeys = make([][]MetricStreamValue, groupKeyCount)
	for i := uint32(0); i < groupKeyCount; i++ {
		// Read field count
		var fieldCount uint8
		if err := binary.Read(buf, binary.LittleEndian, &fieldCount); err != nil {
			return nil, fmt.Errorf("failed to read field count for group key %d: %w", i, err)
		}

		// Read fields
		fields := make([]MetricStreamValue, fieldCount)
		for j := uint8(0); j < fieldCount; j++ {
			value, err := readMetricStreamValue(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to read field %d of group key %d: %w", j, i, err)
			}
			fields[j] = value
		}
		result.GroupKeys[i] = fields
	}

	// Read buckets
	result.Buckets = make([]MetricStreamBucket, bucketCount)
	for i := uint32(0); i < bucketCount; i++ {
		bucket := MetricStreamBucket{
			BucketTime: result.StartTime + int64(i)*int64(result.StepMillis)*1_000_000,
			Groups:     make([][]MetricStreamAggregateValue, groupKeyCount),
		}

		// Read aggregate values for each group
		for j := uint32(0); j < groupKeyCount; j++ {
			// Read value count
			var valueCount uint8
			if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
				return nil, fmt.Errorf("failed to read value count for bucket %d group %d: %w", i, j, err)
			}

			// Read values
			values := make([]MetricStreamAggregateValue, valueCount)
			for k := uint8(0); k < valueCount; k++ {
				var aggType uint8
				if err := binary.Read(buf, binary.LittleEndian, &aggType); err != nil {
					return nil, fmt.Errorf("failed to read agg type for bucket %d group %d value %d: %w", i, j, k, err)
				}

				var aggValue float64
				if err := binary.Read(buf, binary.LittleEndian, &aggValue); err != nil {
					return nil, fmt.Errorf("failed to read agg value for bucket %d group %d value %d: %w", i, j, k, err)
				}

				values[k] = MetricStreamAggregateValue{
					Type:  aggType,
					Value: aggValue,
				}
			}
			bucket.Groups[j] = values
		}

		result.Buckets[i] = bucket
	}

	return result, nil
}

// readMetricStreamValue reads a single value from the binary stream.
func readMetricStreamValue(buf *bytes.Reader) (MetricStreamValue, error) {
	var valueType uint8
	if err := binary.Read(buf, binary.LittleEndian, &valueType); err != nil {
		return MetricStreamValue{}, fmt.Errorf("failed to read value type: %w", err)
	}

	var value interface{}
	switch valueType {
	case 0: // String
		var strLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read string length: %w", err)
		}
		strBytes := make([]byte, strLen)
		if _, err := io.ReadFull(buf, strBytes); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read string: %w", err)
		}
		value = string(strBytes)

	case 1: // Int64
		var i64 int64
		if err := binary.Read(buf, binary.LittleEndian, &i64); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read int64: %w", err)
		}
		value = i64

	case 2: // Float64
		var f64 float64
		if err := binary.Read(buf, binary.LittleEndian, &f64); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read float64: %w", err)
		}
		value = f64

	case 3: // Bool
		var b uint8
		if err := binary.Read(buf, binary.LittleEndian, &b); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read bool: %w", err)
		}
		value = b != 0

	default:
		return MetricStreamValue{}, fmt.Errorf("unsupported value type: %d", valueType)
	}

	return MetricStreamValue{
		Type:  valueType,
		Value: value,
	}, nil
}
