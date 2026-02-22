// Package reader provides efficient reading of blockpack format files.
//
// # Invariants
//
// All Reader instances maintain the following invariants established at construction:
//
//  1. Reader.provider is always non-nil - validated by all constructors (NewReaderFromProvider,
//     NewReaderWithCache, etc.) and maintained throughout the Reader's lifetime.
//
//  2. Methods MAY assume provider is non-nil and do not need to perform redundant checks.
//
// These invariants enable fail-fast behavior (construction errors rather than runtime panics)
// and eliminate the need for defensive nil checks in every method.
package reader

import (
	"fmt"
	"hash/crc32"
	"math"

	"github.com/mattdurham/blockpack/internal/arena"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// TraceBlockEntry represents a trace's presence in a specific block with row-level indices.
type TraceBlockEntry struct {
	SpanIndices []uint16 // Row indices within the block (span positions)
	BlockID     int      // Block index within the file
}

// Reader exposes blockpack data for predicate evaluation across blocks.
// Blocks are parsed lazily on first access for better performance on selective queries.
type Reader struct {
	cache                 ByteCache                              // Optional byte cache for I/O operations
	provider              ReaderProvider                         // Data provider
	parsedBlocks          map[int]*Block                         // Lazily parsed blocks cache
	dedicatedIndex        map[string]*dedicatedColumnIndex       // Parsed dedicated column indexes (lazy)
	dedicatedIndexOffsets map[string]dedicatedIndexMeta          // Metadata for lazy parsing
	rangeBucketMeta       map[string]*shared.RangeBucketMetadata // Range bucket metadata for dedicated columns
	traceBlockIndex       map[[16]byte][]TraceBlockEntry         // Trace ID -> block entries with span indices
	name                  string                                 // File name/path for cache keys
	data                  []byte                                 // Raw file data (optional)
	blockEntries          []blockIndexEntry                      // Block metadata (includes bloom filters)
	metadataBytes         []byte                                 // Raw metadata for lazy column parsing
	dataSize              int64                                  // Total data size
	metricStreamOffset    uint64                                 // Cached from file header
	metricStreamLen       uint64                                 // Cached from file header
	totalSpans            int
}

type dedicatedColumnIndex struct {
	values map[string][]int
	typ    ColumnType
}

type dedicatedIndexMeta struct {
	typ    ColumnType
	offset int // Offset in metadataBytes where this column's index starts
	length int // Length of this column's index data
}

// fileFooter represents the streaming footer format (10 bytes)
type fileFooter struct {
	version      uint16 // Footer version (2 bytes)
	headerOffset uint64 // Offset to file header (8 bytes)
}

// fileHeader represents the file header containing metadata pointers
type fileHeader struct {
	magic              uint32 // Magic number
	version            uint8  // File version
	metadataOffset     uint64
	metadataLen        uint64
	metadataCRC        uint32 // For v11, header doesn't have CRC - we'll compute it
	metricStreamOffset uint64 // Aggregate/metric stream offset
	metricStreamLen    uint64 // Aggregate/metric stream length
}

// NewReaderWithCache creates a Reader from a provider with optional byte-level caching.
// For new code, prefer NewReaderFromProvider with WithCaching option.
func NewReaderWithCache(name string, provider ReaderProvider, cache ByteCache) (*Reader, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}
	dataSize, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("get provider size: %w", err)
	}

	// Need at least footer size (10 bytes for streaming format)
	if dataSize < int64(footerSize) {
		return nil, fmt.Errorf("data too small for footer")
	}

	// Read streaming footer (10 bytes: version + header_offset)
	footerBytes, err := readProviderRangeUncached(provider, dataSize-int64(footerSize), int(footerSize), DataTypeFooter)
	if err != nil {
		return nil, fmt.Errorf("read footer bytes: %w", err)
	}
	footer, err := readFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	// Validate headerOffset before casting to int64 to prevent overflow
	if footer.headerOffset > uint64(dataSize-int64(footerSize)-37) { //nolint:gosec
		return nil, fmt.Errorf("header offset %d out of bounds (file size %d)", footer.headerOffset, dataSize)
	}

	// Read header at the offset specified in footer
	headerBytes, err := readProviderRangeUncached(
		provider,
		int64(footer.headerOffset), //nolint:gosec
		37,
		DataTypeMetadata,
	)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	header, err := readHeader(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	// Validate header
	if header.magic != magicNumber {
		return nil, fmt.Errorf("invalid magic")
	}
	if header.version != versionV10 && header.version != versionV11 {
		return nil, fmt.Errorf("unsupported version %d (only v10/v11 supported)", header.version)
	}

	if header.metadataOffset+header.metadataLen > uint64(dataSize-int64(footerSize)) { //nolint:gosec
		return nil, fmt.Errorf("metadata out of bounds")
	}

	// Validate metadataLen before casting to int to prevent overflow
	if header.metadataLen > math.MaxInt {
		return nil, fmt.Errorf("metadata length %d exceeds maximum", header.metadataLen)
	}

	// Read and validate metadata
	metadata, err := readProviderRangeUncached(
		provider,
		int64(header.metadataOffset), //nolint:gosec
		int(header.metadataLen),
		DataTypeMetadata,
	)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}
	// Compute CRC from metadata
	metadataCRC := crc32.ChecksumIEEE(metadata)
	_ = metadataCRC // CRC validation can be added later if needed

	// Parse metadata
	entries, dedicatedIndexOffsets, traceBlockIndex, err := parseV5MetadataLazy(metadata, header.version)
	if err != nil {
		return nil, fmt.Errorf("parse v5 metadata: %w", err)
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
		rangeBucketMeta:       make(map[string]*shared.RangeBucketMetadata),
		metadataBytes:         metadata,
		totalSpans:            total,
		traceBlockIndex:       traceBlockIndex,
		metricStreamOffset:    header.metricStreamOffset,
		metricStreamLen:       header.metricStreamLen,
	}
	// Legacy optimization: direct byte slice access for in-memory providers
	if sliceProvider, ok := provider.(*byteSliceProvider); ok {
		reader.data = sliceProvider.data
	}
	return reader, nil
}

// byteSliceProvider provides in-memory byte slice access
type byteSliceProvider struct {
	data []byte
}

type rangeReaderProvider struct {
	base   ReaderProvider
	offset int64
	length int64
}

// Blocks returns the list of blocks in the blockpack.
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

// BlockValueStats returns the value statistics for a block.
// Returns an empty map if the block has no statistics or if the index is invalid.
// The returned map is always non-nil and safe to iterate.
func (r *Reader) BlockValueStats(blockIdx int) map[string]shared.AttributeStats {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return make(map[string]shared.AttributeStats)
	}
	stats := r.blockEntries[blockIdx].ValueStats
	if stats == nil {
		return make(map[string]shared.AttributeStats)
	}
	return stats
}

// BlockSize returns the size in bytes of the specified block.
func (r *Reader) BlockSize(blockIdx int) (int64, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return 0, fmt.Errorf("block index %d out of range [0, %d)", blockIdx, len(r.blockEntries))
	}
	return int64(r.blockEntries[blockIdx].Length), nil //nolint:gosec
}

// BlockCount returns the number of blocks in the file.
func (r *Reader) BlockCount() int {
	return len(r.blockEntries)
}

// BlockColumnIndex returns per-column metadata (offset, compressed size) for a block.
func (r *Reader) BlockColumnIndex(blockIdx int) []shared.ColumnIndexEntry {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return nil
	}
	internal := r.blockEntries[blockIdx].ColumnIndex
	// Convert internal columnMetadata to shared.ColumnIndexEntry
	result := make([]shared.ColumnIndexEntry, len(internal))
	for i, col := range internal {
		result[i] = shared.ColumnIndexEntry{
			Name:   col.Name,
			Offset: uint64(col.Offset), // Convert uint32 -> uint64
			Length: uint64(col.Length), // Convert uint32 -> uint64
		}
	}
	return result
}

// BlockColumnStats returns per-column stats for the requested columns in a block
func (r *Reader) BlockColumnStats(blockIdx int, columns []string) (map[string]ColumnStatsWithType, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockEntries) {
		return nil, fmt.Errorf("block index %d out of range [0, %d)", blockIdx, len(r.blockEntries))
	}
	entry := r.blockEntries[blockIdx]
	blockBytes, err := r.readRange(int64(entry.Offset), int(entry.Length), DataTypeColumn) //nolint:gosec
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
func (r *Reader) GetBlock(idx int) (*Block, error) {
	return r.GetBlockColumns(idx, nil)
}

// BlockWithBytes holds a parsed block and its raw bytes for incremental column loading.
type BlockWithBytes struct {
	Block    *Block
	RawBytes []byte
	BlockIdx int
}

// GetBlockColumns retrieves a block with optional column filtering.
func (r *Reader) GetBlockColumns(idx int, columns map[string]struct{}) (*Block, error) {
	return r.GetBlockColumnsReuse(idx, columns, nil)
}

// GetBlockColumnsReuse is like GetBlockColumns but reuses a Block allocation if provided.
func (r *Reader) GetBlockColumnsReuse(idx int, columns map[string]struct{}, reusableBlock *Block) (*Block, error) {
	bwb, err := r.GetBlockWithBytes(idx, columns, reusableBlock, nil, nil)
	if err != nil {
		return nil, err
	}
	return bwb.Block, nil
}

// GetBlockWithBytes loads a block with specified columns and returns both the block and raw bytes.
func (r *Reader) GetBlockWithBytes(
	idx int,
	columns map[string]struct{},
	reusableBlock *Block,
	cachedBytes []byte,
	queryArena *arena.Arena,
) (*BlockWithBytes, error) {
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
		blockBytes, err = r.readRange(int64(entry.Offset), int(entry.Length), DataTypeColumn) //nolint:gosec
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
	provider := newRangeReaderProvider(r.provider, int64(entry.Offset), int64(entry.Length)) //nolint:gosec
	name := r.name
	if name != "" {
		name = fmt.Sprintf("%s[%d:%d]", name, entry.Offset, entry.Offset+entry.Length)
	}
	return NewReaderWithCache(name, provider, r.cache)
}

// CoalescedRead is an alias to shared.CoalescedRead for backward compatibility
type CoalescedRead = shared.CoalescedRead

// CoalesceConfig is an alias to shared.CoalesceConfig for backward compatibility
type CoalesceConfig = shared.CoalesceConfig
