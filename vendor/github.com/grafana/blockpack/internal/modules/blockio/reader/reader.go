package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// footerRaw holds the raw footer fields while readFooter is executing.
type footerRaw struct {
	headerOffset  uint64
	compactOffset uint64
	compactLen    uint32
}

// compactTraceIndex holds the parsed compact trace index section.
type compactTraceIndex struct {
	traceIndex map[[16]byte][]traceBlockRef
	blockTable []compactBlockEntry
}

// Reader reads and decodes a blockpack file.
type Reader struct {
	provider   shared.ReaderProvider
	traceIndex map[[16]byte][]traceBlockRef

	// Range index — lazy.
	rangeOffsets  map[string]rangeIndexMeta
	rangeParsed   map[string]parsedRangeIndex
	compactParsed *compactTraceIndex

	// Options.
	opts readerOptions

	// Parsed during NewReaderFromProvider.
	blockMetas    []shared.BlockMeta
	columnIndexes []columnIndexBlock
	metadataBytes []byte

	footerFields footerRaw

	fileSize int64

	// Compact trace index (v3 footer only).
	compactOffset uint64

	// File header fields (populated in readHeader).
	headerOffset   uint64
	metadataOffset uint64
	metadataLen    uint64

	compactLen uint32

	footerVersion uint16
	fileVersion   uint8
}

// NewReaderFromProvider constructs a Reader by reading the footer, header,
// and metadata section from provider.
func NewReaderFromProvider(provider shared.ReaderProvider, opts ...Option) (*Reader, error) {
	size, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: Size: %w", err)
	}

	r := &Reader{
		provider: provider,
		fileSize: size,
	}

	for _, opt := range opts {
		opt(&r.opts)
	}

	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
	}

	if err = r.readHeader(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
	}

	if err = r.parseV5MetadataLazy(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
	}

	return r, nil
}

// NewLeanReaderFromProvider constructs a Reader using only 2 I/Os: the footer
// (22B) and the compact trace index section. This is the optimal path for
// FindTraceByID workloads. Falls back to NewReaderFromProvider for files
// without a compact trace index (compactLen == 0).
func NewLeanReaderFromProvider(provider shared.ReaderProvider, opts ...Option) (*Reader, error) {
	size, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: Size: %w", err)
	}

	r := &Reader{
		provider: provider,
		fileSize: size,
	}

	for _, opt := range opts {
		opt(&r.opts)
	}

	// I/O #1: read footer.
	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: footer: %w", err)
	}

	// Fall back to full reader when there is no compact section.
	if r.compactLen == 0 {
		return NewReaderFromProvider(provider, opts...)
	}

	// I/O #2: read and parse the compact index (eagerly).
	if err = r.ensureCompactIndexParsed(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: compact index: %w", err)
	}

	// Compact sections are only written for V11 files.
	r.fileVersion = shared.VersionV11

	return r, nil
}

// BlockCount returns the number of blocks in the file.
func (r *Reader) BlockCount() int { return len(r.blockMetas) }

// BlockMeta returns the metadata for the block at blockIdx.
func (r *Reader) BlockMeta(blockIdx int) shared.BlockMeta {
	return r.blockMetas[blockIdx]
}

// BlocksForRange returns the sorted block indices that may contain the given query value
// for the named column. queryValue must be encoded in the same wire format as the stored
// boundary keys (SPECS §5.2.1): 8-byte LE for numeric types, raw string for string/bytes.
//
// A range lookup is performed: the entry with the largest lower boundary ≤ queryValue is
// returned. Returns nil (no error) when queryValue is below all stored lower boundaries.
func (r *Reader) BlocksForRange(colName string, queryValue shared.RangeValueKey) ([]int, error) {
	if err := r.ensureRangeColumnParsed(colName); err != nil {
		return nil, err
	}

	idx := r.rangeParsed[colName]
	entries := idx.entries

	if len(entries) == 0 {
		return nil, nil
	}

	// Binary search: find the last entry whose lower bound ≤ queryValue.
	// sort.Search returns the first index where the condition is FALSE,
	// so we search for the first entry where lower > queryValue, then step back.
	hi := sort.Search(len(entries), func(i int) bool {
		return compareRangeKey(idx.colType, entries[i].lower, queryValue) > 0
	})

	found := hi - 1
	if found < 0 {
		return nil, nil // queryValue is below all lower boundaries
	}

	blockIDs := entries[found].blockIDs
	result := make([]int, len(blockIDs))
	for i, id := range blockIDs {
		result[i] = int(id)
	}

	return result, nil
}

// GetBlockWithBytes reads one block (single I/O) and decodes requested columns.
// wantColumns: nil = all columns. prevBlock: optional reuse target.
// INVARIANT: exactly one readRange call per block (NOTES §1).
func (r *Reader) GetBlockWithBytes(
	blockIdx int,
	wantColumns map[string]struct{},
	prevBlock *BlockWithBytes,
) (*BlockWithBytes, error) {
	var meta shared.BlockMeta
	if blockIdx >= 0 && blockIdx < len(r.blockMetas) {
		meta = r.blockMetas[blockIdx]
	} else if r.compactParsed != nil && blockIdx >= 0 && blockIdx < len(r.compactParsed.blockTable) {
		e := r.compactParsed.blockTable[blockIdx]
		meta = shared.BlockMeta{Offset: e.fileOffset, Length: uint64(e.fileLength)}
	} else {
		return nil, fmt.Errorf("GetBlockWithBytes: blockIdx %d out of range", blockIdx)
	}

	rawBytes, err := r.readRange(meta.Offset, meta.Length, shared.DataTypeBlock)
	if err != nil {
		return nil, fmt.Errorf("GetBlockWithBytes block %d: %w", blockIdx, err)
	}

	var prevBlk *Block
	if prevBlock != nil {
		prevBlk = prevBlock.Block
	}

	blk, err := parseBlockColumnsReuse(rawBytes, r.fileVersion, wantColumns, prevBlk, meta)
	if err != nil {
		return nil, fmt.Errorf("GetBlockWithBytes block %d: parse: %w", blockIdx, err)
	}

	return &BlockWithBytes{Block: blk, RawBytes: rawBytes}, nil
}

// RangeColumnType returns the ColumnType for a range-indexed column, if it exists.
func (r *Reader) RangeColumnType(colName string) (shared.ColumnType, bool) {
	meta, ok := r.rangeOffsets[colName]
	if !ok {
		return 0, false
	}
	return meta.typ, true
}

// ReadBlockRaw reads the raw bytes for the block at blockIdx from the provider.
func (r *Reader) ReadBlockRaw(blockIdx int) ([]byte, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockMetas) {
		return nil, fmt.Errorf("ReadBlockRaw: blockIdx %d out of range [0, %d)", blockIdx, len(r.blockMetas))
	}
	meta := r.blockMetas[blockIdx]
	return r.readRange(meta.Offset, meta.Length, shared.DataTypeBlock)
}

// ReadBlocks reads raw bytes for the given block indices using aggressive coalescing.
// Adjacent block ranges are merged into as few I/O operations as possible (NOTES §13).
// Returns a map from block index to raw byte slice. Invalid indices are silently skipped.
func (r *Reader) ReadBlocks(blockIndices []int) (map[int][]byte, error) {
	if len(blockIndices) == 0 {
		return make(map[int][]byte), nil
	}
	coalesced := CoalesceBlocks(r.blockMetas, blockIndices, shared.AggressiveCoalesceConfig)
	return ReadCoalescedBlocks(r.provider, coalesced)
}

// ParseBlockFromBytes parses a Block from raw bytes using the given meta and column filter.
// wantColumns nil = all columns.
func (r *Reader) ParseBlockFromBytes(rawBytes []byte, wantColumns map[string]struct{}, meta shared.BlockMeta) (*BlockWithBytes, error) {
	blk, err := parseBlockColumnsReuse(rawBytes, r.fileVersion, wantColumns, nil, meta)
	if err != nil {
		return nil, fmt.Errorf("ParseBlockFromBytes: %w", err)
	}
	return &BlockWithBytes{Block: blk, RawBytes: rawBytes}, nil
}

// HasTraceIndex reports whether the reader has a populated trace block index.
func (r *Reader) HasTraceIndex() bool {
	return len(r.traceIndex) > 0
}

// TraceCount returns the number of unique trace IDs in the file.
// For full readers this uses the main trace index (populated during init, no extra I/Os).
// For lean readers (compact index only) this uses the compact trace index.
func (r *Reader) TraceCount() int {
	if len(r.traceIndex) > 0 {
		return len(r.traceIndex)
	}
	if r.compactParsed != nil {
		return len(r.compactParsed.traceIndex)
	}
	return 0
}

// TraceEntry is a single trace-block reference with span indices.
type TraceEntry struct {
	SpanIndices []uint16
	BlockID     int
}

// TraceEntries returns per-block span-index entries for a trace ID.
// Falls back to the compact trace index when the main index is empty (lean reader path).
func (r *Reader) TraceEntries(traceID [16]byte) []TraceEntry {
	refs, ok := r.traceIndex[traceID]
	if !ok && r.compactParsed != nil {
		refs, ok = r.compactParsed.traceIndex[traceID]
	}
	if !ok {
		return nil
	}
	result := make([]TraceEntry, len(refs))
	for i, ref := range refs {
		indices := make([]uint16, len(ref.spanIndices))
		copy(indices, ref.spanIndices)
		result[i] = TraceEntry{
			BlockID:     int(ref.blockID),
			SpanIndices: indices,
		}
	}
	return result
}

// AddColumnsToBlock decodes additional columns from an already-loaded BlockWithBytes.
// No additional I/O is performed.
func (r *Reader) AddColumnsToBlock(bwb *BlockWithBytes, addColumns map[string]struct{}) error {
	if bwb == nil || bwb.Block == nil {
		return fmt.Errorf("AddColumnsToBlock: nil block")
	}

	hdr, err := parseBlockHeader(bwb.RawBytes)
	if err != nil {
		return fmt.Errorf("AddColumnsToBlock: %w", err)
	}

	spanCount := int(hdr.spanCount)
	colCount := int(hdr.columnCount)

	metas, _, err := parseColumnMetadataArray(bwb.RawBytes, 24, colCount)
	if err != nil {
		return fmt.Errorf("AddColumnsToBlock: column metadata: %w", err)
	}

	stats, err := parseColumnStatsSection(bwb.RawBytes, metas, 0)
	if err != nil {
		return fmt.Errorf("AddColumnsToBlock: column stats: %w", err)
	}

	for i, m := range metas {
		if _, want := addColumns[m.name]; !want {
			continue
		}

		if _, exists := bwb.Block.columns[m.name]; exists {
			continue
		}

		if m.dataLen == 0 {
			continue
		}

		start := int(m.dataOffset)    //nolint:gosec // safe: dataOffset bounded by block size < MaxBlockSize
		end := start + int(m.dataLen) //nolint:gosec // safe: dataLen bounded by block size < MaxBlockSize
		if start < 0 || end > len(bwb.RawBytes) {
			return fmt.Errorf(
				"AddColumnsToBlock: col %q data offset %d len %d out of range",
				m.name, m.dataOffset, m.dataLen,
			)
		}

		col := &Column{
			Name:  m.name,
			Type:  m.colType,
			Stats: stats[i],
		}

		decoded, err := readColumnEncoding(bwb.RawBytes[start:end], spanCount, m.colType)
		if err != nil {
			return fmt.Errorf("AddColumnsToBlock: col %q: %w", m.name, err)
		}

		col.StringDict = decoded.StringDict
		col.StringIdx = decoded.StringIdx
		col.Int64Dict = decoded.Int64Dict
		col.Int64Idx = decoded.Int64Idx
		col.Uint64Dict = decoded.Uint64Dict
		col.Uint64Idx = decoded.Uint64Idx
		col.Float64Dict = decoded.Float64Dict
		col.Float64Idx = decoded.Float64Idx
		col.BoolDict = decoded.BoolDict
		col.BoolIdx = decoded.BoolIdx
		col.BytesDict = decoded.BytesDict
		col.BytesIdx = decoded.BytesIdx
		col.BytesInline = decoded.BytesInline
		col.Present = decoded.Present
		col.SpanCount = decoded.SpanCount

		bwb.Block.columns[m.name] = col
	}

	return nil
}
