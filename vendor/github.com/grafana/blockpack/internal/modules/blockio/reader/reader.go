package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"sort"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// footerRaw holds the raw footer fields while readFooter is executing.
type footerRaw struct {
	headerOffset  uint64
	compactOffset uint64
	compactLen    uint32
}

// compactTraceIndex holds the parsed compact trace index section.
type compactTraceIndex struct {
	traceIndex   map[[16]byte][]uint16
	blockTable   []compactBlockEntry
	traceIDBloom []byte // nil for version-1 compact indexes (no bloom); vacuous true on lookup
}

// Reader reads and decodes a blockpack file.
type Reader struct {
	provider rw.ReaderProvider
	// cache is the optional disk-backed file cache for footer/header/metadata/block reads.
	// Nil when no cache is configured.
	cache  *filecache.FileCache
	fileID string

	traceIndex map[[16]byte][]uint16

	// Range index — lazy.
	rangeOffsets  map[string]rangeIndexMeta
	rangeParsed   map[string]parsedRangeIndex
	compactParsed *compactTraceIndex

	// internStrings is the per-reader string intern map for dictionary column values.
	// Shared across all block parses on this reader; single-goroutine use only.
	internStrings map[string]string

	// sketchIdx holds parsed column-major sketch data for the file.
	// Nil for files written before the sketch section was introduced (old format).
	sketchIdx *sketchIndex

	// fileSummary is the lazily computed file-level sketch summary.
	fileSummary *FileSketchSummary

	// tsEntries is the parsed per-file timestamp index (sorted by minTS ascending).
	// Nil for files written before the TS index was introduced.
	tsEntries []tsIndexEntry

	// Parsed during NewReaderFromProvider.
	blockMetas    []shared.BlockMeta
	metadataBytes []byte

	footerFields footerRaw

	fileSize int64

	// Compact trace index (v3 footer only).
	compactOffset uint64

	// File header fields (populated in readHeader).
	headerOffset   uint64
	metadataOffset uint64
	metadataLen    uint64

	fileSummaryOnce sync.Once

	compactLen uint32

	footerVersion uint16
	fileVersion   uint8

	// signalType is parsed from the V12 file header signal_type byte.
	// Defaults to shared.SignalTypeTrace (0x01) for older files.
	signalType uint8

	// intrinsicIndexOffset and intrinsicIndexLen are parsed from the v4 footer.
	// Both are 0 for v3 footer files or files with no intrinsic section.
	intrinsicIndexOffset uint64
	intrinsicIndexLen    uint32

	// intrinsicIndex holds the parsed TOC entries, keyed by column name.
	// Populated by parseIntrinsicTOC during NewReaderFromProvider. Nil for
	// v3 footer files or files with intrinsicIndexLen == 0.
	intrinsicIndex map[string]shared.IntrinsicColMeta

	// intrinsicDecoded caches fully decoded intrinsic columns by name.
	// Populated lazily by GetIntrinsicColumn.
	intrinsicDecoded map[string]*shared.IntrinsicColumn
}

// NewReaderFromProvider constructs a Reader by reading the footer, header,
// and metadata section from provider.
func NewReaderFromProvider(provider rw.ReaderProvider) (*Reader, error) {
	return NewReaderFromProviderWithOptions(provider, Options{})
}

// NewReaderFromProviderWithOptions constructs a Reader with the given options.
// Use this to attach a file cache for footer, header, metadata, and block reads.
func NewReaderFromProviderWithOptions(provider rw.ReaderProvider, opts Options) (*Reader, error) {
	if opts.Cache != nil && opts.FileID == "" {
		return nil, fmt.Errorf("NewReaderFromProvider: Options.FileID must be set when Cache is non-nil")
	}

	size, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: Size: %w", err)
	}

	r := &Reader{
		provider:      provider,
		cache:         opts.Cache,
		fileID:        opts.FileID,
		fileSize:      size,
		internStrings: make(map[string]string),
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

	if err = r.parseIntrinsicTOC(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
	}

	return r, nil
}

// NewLeanReaderFromProvider constructs a Reader using only 2 I/Os: the footer
// (22B) and the compact trace index section. This is the optimal path for
// FindTraceByID workloads. Falls back to NewReaderFromProvider for files
// without a compact trace index (compactLen == 0).
func NewLeanReaderFromProvider(provider rw.ReaderProvider) (*Reader, error) {
	return NewLeanReaderFromProviderWithOptions(provider, Options{})
}

// NewLeanReaderFromProviderWithOptions constructs a lean Reader with the given options.
// Use this to attach a file cache for footer, compact index, and block reads.
func NewLeanReaderFromProviderWithOptions(provider rw.ReaderProvider, opts Options) (*Reader, error) {
	if opts.Cache != nil && opts.FileID == "" {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: Options.FileID must be set when Cache is non-nil")
	}

	size, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: Size: %w", err)
	}

	r := &Reader{
		provider:      provider,
		cache:         opts.Cache,
		fileID:        opts.FileID,
		fileSize:      size,
		internStrings: make(map[string]string),
	}

	// I/O #1: read footer.
	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: footer: %w", err)
	}

	// Fall back to full reader when there is no compact section.
	if r.compactLen == 0 {
		return NewReaderFromProviderWithOptions(provider, opts)
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

// TraceCount returns the number of unique traces in the file.
// Uses the trace index (full or compact) to determine the count.
// Returns 0 if no trace index is available.
func (r *Reader) TraceCount() int {
	if len(r.traceIndex) > 0 {
		return len(r.traceIndex)
	}
	if r.compactParsed != nil {
		return len(r.compactParsed.traceIndex)
	}
	return 0
}

// SignalType returns the signal type stored in the file header.
// Returns shared.SignalTypeTrace for files written with version < 12 (trace-only era).
func (r *Reader) SignalType() uint8 {
	if r.signalType == 0 {
		return shared.SignalTypeTrace
	}
	return r.signalType
}

// BlockMeta returns the metadata for the block at blockIdx.
func (r *Reader) BlockMeta(blockIdx int) shared.BlockMeta {
	return r.blockMetas[blockIdx]
}

// ColumnSketch returns the column-major sketch data for the named column, or nil if
// no sketch section was written or the column was not sketched.
// Implements queryplanner.BlockIndexer.
func (r *Reader) ColumnSketch(col string) queryplanner.ColumnSketch {
	if r.sketchIdx == nil {
		return nil
	}
	cd := r.sketchIdx.columns[col]
	if cd == nil {
		return nil
	}
	return cd
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

// BlocksForRangeInterval returns block indices from all buckets whose range overlaps
// [minKey, maxKey]. The implementation finds the bucket containing minKey (largest
// lower boundary ≤ minKey) and the last bucket whose lower boundary ≤ maxKey, then
// unions all block IDs in between. This correctly includes the bucket containing
// minKey even when its lower boundary is < minKey.
// NOTE-011: Used for case-insensitive regex prefix lookups where the query spans a
// lexicographic range (e.g., all case variants from "DEBUG" to "debug").
func (r *Reader) BlocksForRangeInterval(
	colName string, minKey, maxKey shared.RangeValueKey,
) ([]int, error) {
	if err := r.ensureRangeColumnParsed(colName); err != nil {
		return nil, err
	}

	idx := r.rangeParsed[colName]
	entries := idx.entries

	if len(entries) == 0 {
		return nil, nil
	}

	// Find the bucket that contains minKey: last entry whose lower ≤ minKey.
	lo := sort.Search(len(entries), func(i int) bool {
		return compareRangeKey(idx.colType, entries[i].lower, minKey) > 0
	}) - 1
	if lo < 0 {
		lo = 0 // minKey is below all boundaries; start from the first bucket.
	}

	// Find the last bucket whose lower ≤ maxKey.
	hi := sort.Search(len(entries), func(i int) bool {
		return compareRangeKey(idx.colType, entries[i].lower, maxKey) > 0
	}) - 1
	if hi < 0 {
		return nil, nil // maxKey is below all lower boundaries — no overlap.
	}

	// Union block IDs from all buckets in [lo, hi].
	seen := make(map[int]struct{})
	for i := lo; i <= hi; i++ {
		for _, id := range entries[i].blockIDs {
			seen[int(id)] = struct{}{}
		}
	}

	result := make([]int, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Ints(result)

	return result, nil
}


// ColumnNames returns all column names known to this reader — the union of
// range-indexed columns (rangeOffsets) and sketch columns (sketchIdx).
// These are derived from the file header/metadata, so no block I/O is needed.
func (r *Reader) ColumnNames() []string {
	seen := make(map[string]struct{})
	for col := range r.rangeOffsets {
		seen[col] = struct{}{}
	}
	if r.sketchIdx != nil {
		for col := range r.sketchIdx.columns {
			seen[col] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for col := range seen {
		out = append(out, col)
	}
	sort.Strings(out)
	return out
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
	return r.readRange(meta.Offset, meta.Length, rw.DataTypeBlock)
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

// CoalescedGroups partitions blockIndices into coalesced read groups (~8 MB each) without
// performing any I/O. Used by lazy-fetch callers that stop early on a limit.
func (r *Reader) CoalescedGroups(blockIndices []int) []shared.CoalescedRead {
	return CoalesceBlocks(r.blockMetas, blockIndices, shared.AggressiveCoalesceConfig)
}

// ReadGroup performs the I/O for a single CoalescedRead group.
func (r *Reader) ReadGroup(cr shared.CoalescedRead) (map[int][]byte, error) {
	return ReadCoalescedBlocks(r.provider, []shared.CoalescedRead{cr})
}

// ParseBlockFromBytes parses a Block from raw bytes using the given meta and column filter.
// wantColumns nil = all columns.
func (r *Reader) ParseBlockFromBytes(
	rawBytes []byte,
	wantColumns map[string]struct{},
	meta shared.BlockMeta,
) (*BlockWithBytes, error) {
	blk, err := parseBlockColumnsReuse(rawBytes, wantColumns, nil, meta, r.internStrings)
	if err != nil {
		return nil, fmt.Errorf("ParseBlockFromBytes: %w", err)
	}
	return &BlockWithBytes{Block: blk, RawBytes: rawBytes}, nil
}

// HasTraceIndex reports whether the reader has a populated trace block index.
func (r *Reader) HasTraceIndex() bool {
	return len(r.traceIndex) > 0
}

// TraceEntry is a single trace-block reference.
type TraceEntry struct {
	BlockID int
}

// TraceEntries returns the block IDs containing spans for the given trace ID.
// Falls back to the compact trace index when the main index is empty (lean reader path).
func (r *Reader) TraceEntries(traceID [16]byte) []TraceEntry {
	blockIDs, ok := r.traceIndex[traceID]
	if !ok && r.compactParsed != nil {
		blockIDs, ok = r.compactParsed.traceIndex[traceID]
	}
	if !ok {
		return nil
	}
	result := make([]TraceEntry, len(blockIDs))
	for i, bid := range blockIDs {
		result[i] = TraceEntry{BlockID: int(bid)}
	}
	return result
}

// ResetInternStrings clears the per-reader string intern map without reallocating it.
// NOTE-020: call before each block's first-pass ParseBlockFromBytes in scan loops
// to bound the intern map to one block's unique strings. Single-goroutine use only.
func (r *Reader) ResetInternStrings() {
	for k := range r.internStrings {
		delete(r.internStrings, k)
	}
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

	scratch := acquireDecompScratch()
	defer releaseDecompScratch(scratch)
	ctx := &decodeCtx{scratch: scratch, intern: r.internStrings}

	for _, m := range metas {
		// NOTE-022: nil means "add all missing columns"; non-nil filters by name.
		if addColumns != nil {
			if _, want := addColumns[m.name]; !want {
				continue
			}
		}

		if _, exists := bwb.Block.columns[shared.ColumnKey{Name: m.name, Type: m.colType}]; exists {
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
			Name: m.name,
			Type: m.colType,
		}

		decoded, err := readColumnEncoding(bwb.RawBytes[start:end], spanCount, m.colType, ctx)
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

		bwb.Block.columns[shared.ColumnKey{Name: m.name, Type: m.colType}] = col
	}

	bwb.Block.buildNameIndex()

	return nil
}
