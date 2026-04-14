package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/golang/snappy"
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
// NOTE-PERF-COMPACT: traceIndexRaw stores the raw trace-index bytes in-place (a sub-slice of the
// cached compact-index buffer) rather than a pre-built map. scanTraceIndexRaw scans them linearly
// on each lookup, eliminating O(traceCount) map + []uint16 allocations that were the #1 production
// allocator (36.27% alloc_objects). Allocation on hit is one small []uint16 per lookup — far cheaper
// than materializing every trace's block list at parse time.
//
// NOTE-LAZY-TRACE-INDEX: For lean readers, traceIndexRaw is not populated at construction time.
// Instead, traceIndexOffset/traceIndexLen record where the trace index bytes live in the file so
// they can be fetched lazily — only when the bloom filter reports a hit. This keeps the eager read
// at ~15 MB (bloom + block table) instead of ~700 MB (full compact section).
//
// NOTE-V14-TRACE-LAZY: For V14 lean readers, isV14TraceSection signals that traceIndexRaw must
// be fetched by re-reading the full V14 SectionTraceIndex, splitting out the trace index bytes
// via splitV14CompactSection, and caching them. Unlike V3/V4 (which record a direct file offset),
// V14 stores the compact section as a single snappy-compressed blob, so a direct range read is
// not available — the full blob must be fetched, split, and the trace index portion extracted.
// ensureTraceIndexRaw uses isV14TraceSection to select the correct fetch path.
type compactTraceIndex struct {
	// traceIndexFetchErr holds any error from the lazy fetch so callers can surface it.
	traceIndexFetchErr error
	traceIndexRaw      []byte // raw trace-index bytes; scanned in-place by scanTraceIndexRaw
	blockTable         []compactBlockEntry
	traceIDBloom       []byte // nil for version-1 compact indexes (no bloom); vacuous true on lookup

	// traceIndexOffset and traceIndexLen locate the trace-index bytes within the file.
	// Used by ensureTraceIndexRaw to lazily fetch them on first bloom hit.
	// Both are zero when traceIndexRaw is already populated (full compact read path).
	traceIndexOffset uint64
	traceIndexLen    uint64

	// isV14TraceSection signals that this compactTraceIndex was populated from a V14 file's
	// SectionTraceIndex compact blob (via parseCompactIndexBytesV14Header). When true,
	// ensureTraceIndexRaw re-reads the full V14 section and extracts the trace index bytes
	// via splitV14CompactSection, instead of using traceIndexOffset/traceIndexLen.
	isV14TraceSection bool

	// traceIndexOnce guards the lazy fetch of traceIndexRaw.
	traceIndexOnce sync.Once
}

// Reader reads and decodes a blockpack file.
type Reader struct {
	provider rw.ReaderProvider
	// vectorIndexErr holds any error from lazy vector index parsing.
	vectorIndexErr error
	// cache is the cache used for footer/header/metadata/block reads.
	// Never nil: defaults to filecache.NopCache when no cache is configured.
	cache filecache.Cache

	// sectionDir holds the decoded V14 section directory.
	// Populated by readSectionDirectory() during NewReaderFromProvider for V14 files.
	// Both maps are nil for V3/V4/V5/V6 footer files.
	sectionDir shared.SectionDirectory

	// v14 lazy section errors — set inside the corresponding sync.Once.Do and read after.
	v14RangeErr  error
	v14TraceErr  error
	v14TSErr     error
	v14SketchErr error
	v14BloomErr  error

	traceIndex map[[16]byte][]uint16

	// Range index — lazy.
	rangeOffsets  map[string]rangeIndexMeta
	rangeParsed   map[string]parsedRangeIndex
	compactParsed *compactTraceIndex

	// internStrings is kept for API compatibility with ResetInternStrings callers.
	// ParseBlockFromBytes and AddColumnsToBlock now each allocate a fresh local intern
	// map per call, so this field is no longer borrowed by parse paths.
	internStrings map[string]string

	// sketchIdx holds parsed column-major sketch data for the file.
	// Nil for files written before the sketch section was introduced (old format).
	sketchIdx *sketchIndex

	// fileSummary is the lazily computed file-level sketch summary.
	fileSummary *FileSketchSummary

	// intrinsicIndex holds the parsed TOC entries, keyed by column name.
	// Populated by parseIntrinsicTOC during NewReaderFromProvider. Nil for
	// v3 footer files or files with intrinsicIndexLen == 0.
	intrinsicIndex map[string]shared.IntrinsicColMeta

	// intrinsicDecoded caches fully decoded intrinsic columns by name.
	// Populated lazily by GetIntrinsicColumn.
	intrinsicDecoded map[string]*shared.IntrinsicColumn

	// metaPin holds a reference to the *parsedMetadata retrieved from the process-level
	// cache, ensuring the pointer remains valid for the lifetime of this Reader.
	metaPin *parsedMetadata

	// tocPin holds a reference to the *intrinsicTOC retrieved from the process-level
	// cache, ensuring the pointer remains valid for the lifetime of this Reader.
	tocPin *intrinsicTOC

	// fileBloomParsed is the lazily parsed FileBloom section. Access via FileBloom().
	fileBloomParsed *FileBloom

	// vectorIndexParsed is the lazily parsed VectorIndex. Access via VectorIndex().
	vectorIndexParsed *VectorIndex

	fileID string

	// intrinsicNames is the sorted slice of intrinsic column names, computed once
	// (lazily on first IntrinsicColumnNames call) and reused. Callers only iterate.
	intrinsicNames []string

	// traceIndexRaw holds the raw bytes of the trace index section for lazy parsing.
	// Populated during parseV5MetadataLazy; parsed into traceIndex on first access.
	traceIndexRaw []byte

	// tsRaw holds the raw 20-byte-per-entry TS index body (a zero-copy sub-slice of
	// metadataBytes). tsCount is the number of entries. Entries are sorted by minTS
	// ascending (as written). Nil/0 for files written before the TS index was introduced.
	// NOTE-PERF-TS: raw bytes eliminate O(count) tsIndexEntry allocations at parse time.
	tsRaw []byte

	// Parsed during NewReaderFromProvider.
	blockMetas    []shared.BlockMeta
	metadataBytes []byte

	// fileBloomRaw holds the raw bytes of the FileBloom section, for caller caching.
	// Nil for files written before the FileBloom section was introduced.
	fileBloomRaw []byte

	footerFields footerRaw

	tsCount int

	fileSize int64

	// Compact trace index (v3 footer only).
	compactOffset uint64

	// compactTracesOffset and compactTracesLen are parsed from the v6 footer.
	// When non-zero, the compact section is split: compact_offset/compact_len holds the
	// uncompressed bloom+block_table header, and compactTracesOffset/compactTracesLen holds
	// the snappy-compressed trace index (v3 split format).
	compactTracesOffset uint64

	// File header fields (populated in readHeader).
	headerOffset   uint64
	metadataOffset uint64
	metadataLen    uint64

	// intrinsicIndexOffset and intrinsicIndexLen are parsed from the v4 footer.
	// Both are 0 for v3 footer files or files with no intrinsic section.
	intrinsicIndexOffset uint64

	// vectorIndexOffset and vectorIndexLen are parsed from the agentic v5 footer.
	// Both are 0 for v3/v4 footer files or files with no vector index section.
	vectorIndexOffset uint64

	// V7 footer fields (FooterV7Version = 7, V14 section-directory files only).
	// v7DirOffset and v7DirLen point to the snappy-compressed section directory.
	v7DirOffset uint64

	fileBloomOnce sync.Once

	fileSummaryOnce sync.Once

	// vectorIndexOnce guards lazy parsing of the vector index section.
	vectorIndexOnce sync.Once

	// v14 lazy section loaders — each fires at most once per Reader for V14 files.
	// Non-V14 readers leave these zero-valued; the ensure* helpers are no-ops for them.
	v14RangeOnce  sync.Once
	v14TraceOnce  sync.Once
	v14TSOnce     sync.Once
	v14SketchOnce sync.Once
	v14BloomOnce  sync.Once

	compactLen uint32

	// compactTracesLen is parsed from the v6 footer; see compactTracesOffset.
	compactTracesLen uint32

	intrinsicIndexLen uint32

	// vectorIndexLen is parsed from the agentic v5 footer.
	vectorIndexLen uint32

	v7DirLen uint32

	footerVersion uint16
	fileVersion   uint8

	// signalType is parsed from the V12 file header signal_type byte.
	// Defaults to shared.SignalTypeTrace (0x01) for older files.
	signalType uint8
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

	cache := opts.Cache
	if cache == nil {
		cache = filecache.NopCache
	}
	r := &Reader{
		provider:      provider,
		cache:         cache,
		fileID:        opts.FileID,
		fileSize:      size,
		internStrings: make(map[string]string),
	}

	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
	}

	if r.footerVersion == shared.FooterV7Version {
		// V14 file: read section directory + parse sections from it.
		r.sectionDir, err = r.readSectionDirectory()
		if err != nil {
			return nil, fmt.Errorf("NewReaderFromProvider: section directory: %w", err)
		}
		if err = r.parseSectionsLazyV14(); err != nil {
			return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
		}
		return r, nil
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

	leanCache := opts.Cache
	if leanCache == nil {
		leanCache = filecache.NopCache
	}
	r := &Reader{
		provider:      provider,
		cache:         leanCache,
		fileID:        opts.FileID,
		fileSize:      size,
		internStrings: make(map[string]string),
	}

	// I/O #1: read footer.
	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: footer: %w", err)
	}

	// V14 files: read section directory + all sections (no legacy compact section).
	if r.footerVersion == shared.FooterV7Version {
		r.sectionDir, err = r.readSectionDirectory()
		if err != nil {
			return nil, fmt.Errorf("NewLeanReaderFromProvider: section directory: %w", err)
		}
		if err = r.parseSectionsLazyV14(); err != nil {
			return nil, fmt.Errorf("NewLeanReaderFromProvider: %w", err)
		}
		return r, nil
	}

	// V3/V4 files: fall back to full reader when there is no compact section.
	if r.compactLen == 0 {
		return NewReaderFromProviderWithOptions(provider, opts)
	}

	// I/O #2: read header to validate V13 and extract signal type.
	if err = r.readHeader(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: header: %w", err)
	}

	// I/O #3: read bloom filter + block table only (lazy trace index — fetched on bloom hit).
	if err = r.ensureCompactHeaderParsed(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: compact index: %w", err)
	}

	return r, nil
}

// BlockCount returns the number of blocks in the file.
func (r *Reader) BlockCount() int { return len(r.blockMetas) }

// TraceCount returns the number of unique traces in the file.
// Uses the trace index (full or compact) to determine the count.
// Returns 0 if no trace index is available.
func (r *Reader) TraceCount() int {
	_ = r.ensureV14TraceSection()
	r.ensureTraceIndex()
	if len(r.traceIndex) > 0 {
		return len(r.traceIndex)
	}
	if r.compactParsed != nil && len(r.compactParsed.traceIndexRaw) >= 5 {
		fmtVer := r.compactParsed.traceIndexRaw[0]
		if fmtVer == shared.TraceIndexFmtVersion || fmtVer == shared.TraceIndexFmtVersion2 {
			// Trace count is encoded at bytes [1:5] of the raw trace-index section.
			return int(binary.LittleEndian.Uint32(r.compactParsed.traceIndexRaw[1:]))
		}
	}
	return 0
}

// ensureTraceIndex parses the trace block index from raw bytes if not yet parsed.
// No-op if already parsed or no raw bytes are available.
func (r *Reader) ensureTraceIndex() {
	if r.traceIndex != nil || len(r.traceIndexRaw) == 0 {
		return
	}
	idx, _, err := parseTraceBlockIndex(r.traceIndexRaw)
	if err == nil {
		r.traceIndex = idx
	}
	r.traceIndexRaw = nil // free raw bytes after parsing
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
	_ = r.ensureV14SketchSection()
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
	// max(lo, 0): when lo is -1 (minKey below all boundaries), start from bucket 0.
	lo := max(sort.Search(len(entries), func(i int) bool {
		return compareRangeKey(idx.colType, entries[i].lower, minKey) > 0
	})-1, 0)

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
	slices.Sort(result)

	return result, nil
}

// ColumnNames returns all column names known to this reader — the union of
// range-indexed columns (rangeOffsets) and sketch columns (sketchIdx).
func (r *Reader) ColumnNames() []string {
	// Ensure both V14 lazy sections are loaded before scanning.
	_ = r.ensureV14RangeSection()
	_ = r.ensureV14SketchSection()
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
	slices.Sort(out)
	return out
}

// RangeColumnType returns the ColumnType for a range-indexed column, if it exists.
func (r *Reader) RangeColumnType(colName string) (shared.ColumnType, bool) {
	_ = r.ensureV14RangeSection()
	meta, ok := r.rangeOffsets[colName]
	if !ok {
		return 0, false
	}
	return meta.typ, true
}

// RangeBoundaries exposes the file-level value range for a range-indexed column.
// BucketMin and BucketMax are the global min/max across all blocks (stored in
// the wire format bucket metadata). For RangeFloat64, Float64Bounds holds the
// typed boundary values. For RangeString, StringBounds holds them. For
// RangeBytes, BytesBounds holds them. For numeric types (Int64/Uint64/Duration),
// BucketMin/BucketMax are sufficient for file-level fast reject.
type RangeBoundaries struct {
	Float64Bounds []float64
	StringBounds  []string
	BytesBounds   [][]byte
	BucketMin     int64
	BucketMax     int64
	ColType       shared.ColumnType
}

// RangeColumnBoundaries returns the parsed boundaries for a range-indexed column.
// Returns nil if the column is not range-indexed or an error occurs during parsing.
// The result may be used for file-level fast reject: if a query value falls entirely
// outside [BucketMin, BucketMax], no spans in the file can match.
func (r *Reader) RangeColumnBoundaries(colName string) *RangeBoundaries {
	if err := r.ensureRangeColumnParsed(colName); err != nil {
		return nil
	}
	idx, ok := r.rangeParsed[colName]
	if !ok {
		return nil
	}
	rb := &RangeBoundaries{
		ColType:      idx.colType,
		BucketMin:    idx.bucketMin,
		BucketMax:    idx.bucketMax,
		StringBounds: idx.stringBounds,
		BytesBounds:  idx.bytesBounds,
	}
	// NOTE-PERF-RANGE: decode float64 bounds on demand from raw bytes rather than
	// storing a pre-decoded []float64 in parsedRangeIndex.
	if len(idx.float64BoundsRaw) > 0 {
		count := len(idx.float64BoundsRaw) / 8
		rb.Float64Bounds = make([]float64, count)
		for i := range count {
			rb.Float64Bounds[i] = math.Float64frombits(binary.LittleEndian.Uint64(idx.float64BoundsRaw[i*8:]))
		}
	}
	return rb
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
// Each call allocates its own fresh intern map; ResetInternStrings is a no-op and
// no cross-call intern reuse occurs.
func (r *Reader) ParseBlockFromBytes(
	rawBytes []byte,
	wantColumns map[string]struct{},
	meta shared.BlockMeta,
) (*BlockWithBytes, error) {
	localIntern := make(map[string]string)

	blk, err := parseBlockColumnsReuse(rawBytes, wantColumns, nil, meta, localIntern)
	if err != nil {
		return nil, fmt.Errorf("ParseBlockFromBytes: %w", err)
	}
	return &BlockWithBytes{Block: blk, RawBytes: rawBytes}, nil
}

// ParseBlockFromBytesWithIntern parses a Block from raw bytes using a caller-supplied intern map.
// The intern map must remain valid until after all lazy column decodes complete (i.e. after
// the row-emission loop). The caller is responsible for acquiring and releasing the map via
// AcquireInternMap / ReleaseInternMap.
//
// NOTE-006: Used by scanBlocks to eliminate per-call make(map[string]string) allocations.
// The pooled intern map is held alive for the entire block lifetime (first parse + row loop
// + second parse) before being returned to the pool.
func (r *Reader) ParseBlockFromBytesWithIntern(
	rawBytes []byte,
	wantColumns map[string]struct{},
	meta shared.BlockMeta,
	intern map[string]string,
) (*BlockWithBytes, error) {
	blk, err := parseBlockColumnsReuse(rawBytes, wantColumns, nil, meta, intern)
	if err != nil {
		return nil, fmt.Errorf("ParseBlockFromBytesWithIntern: %w", err)
	}
	return &BlockWithBytes{Block: blk, RawBytes: rawBytes}, nil
}

// HasTraceIndex reports whether the reader has a populated trace block index.
func (r *Reader) HasTraceIndex() bool {
	r.ensureTraceIndex()
	return len(r.traceIndex) > 0
}

// TraceEntry is a single trace-block reference.
type TraceEntry struct {
	BlockID int
}

// TraceEntries returns the block IDs containing spans for the given trace ID.
// Falls back to the compact trace index when the main index is empty (lean reader path).
func (r *Reader) TraceEntries(traceID [16]byte) []TraceEntry {
	_ = r.ensureV14TraceSection()
	r.ensureTraceIndex()
	blockIDs, ok := r.traceIndex[traceID]
	if !ok && r.compactParsed != nil {
		// Ensure trace index bytes are loaded (lazy for lean readers; no-op for full readers).
		_ = r.ensureTraceIndexRaw()
		blockIDs = scanTraceIndexRaw(r.compactParsed.traceIndexRaw, traceID)
		ok = blockIDs != nil
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

// ResetInternStrings is a no-op retained for API compatibility with existing scan loops.
// ParseBlockFromBytes and AddColumnsToBlock now allocate their own fresh intern map per
// call, so there is no shared state to reset between blocks.
func (r *Reader) ResetInternStrings() {}

// FileBloom returns the parsed file-level bloom filter for resource.service.name.
// Returns nil for files written before the FileBloom section was introduced.
// The returned *FileBloom is safe for concurrent use after the first call.
func (r *Reader) FileBloom() *FileBloom {
	// For V14 files, load the bloom section lazily on first call.
	_ = r.ensureV14BloomSection()
	r.fileBloomOnce.Do(func() {
		if r.fileBloomRaw != nil && r.fileBloomParsed == nil {
			fb, _, err := parseFileBloomSection(r.fileBloomRaw)
			if err == nil {
				r.fileBloomParsed = fb
			}
		}
	})
	return r.fileBloomParsed
}

// FileBloomRaw returns a clone of the raw bytes of the FileBloom section.
// Callers may cache this slice (keyed by file path + size) and reconstruct
// a FileBloom via ParseFileBloom without reopening the file.
// Returns nil for files without a FileBloom section (old format).
func (r *Reader) FileBloomRaw() []byte {
	_ = r.ensureV14BloomSection()
	if r.fileBloomRaw == nil {
		return nil
	}
	return slices.Clone(r.fileBloomRaw)
}

// FileSketchSummaryRaw returns the serialized FileSketchSummary as bytes.
// Callers may cache this slice (keyed by file path + size) and reconstruct
// a FileSketchSummary via UnmarshalFileSketchSummary without reopening the file.
// Returns nil for files without a sketch section (old format).
// Returns nil if serialization fails (should not happen in practice).
func (r *Reader) FileSketchSummaryRaw() []byte {
	s := r.FileSketchSummary()
	if s == nil {
		return nil
	}
	b, err := MarshalFileSketchSummary(s)
	if err != nil {
		return nil
	}
	return b
}

// TraceBloomRaw returns a clone of the raw bytes of the compact trace ID bloom filter.
// Callers may cache this slice and use shared.TestTraceIDBloom for trace:id
// file-level rejection without reopening the file.
// Returns nil for files without a compact trace index bloom.
func (r *Reader) TraceBloomRaw() []byte {
	_ = r.ensureV14TraceSection()
	if r.compactLen > 0 {
		_ = r.ensureCompactIndexParsed()
	}
	if r.compactParsed == nil {
		return nil
	}
	return slices.Clone(r.compactParsed.traceIDBloom)
}

// MayContainTraceID returns false only when the compact trace bloom guarantees
// the trace ID is absent from this file. Returns true (conservative) when no
// compact trace index or bloom is present.
func (r *Reader) MayContainTraceID(traceID [16]byte) bool {
	_ = r.ensureV14TraceSection()
	if r.compactLen > 0 {
		_ = r.ensureCompactIndexParsed()
	}
	if r.compactParsed == nil {
		return true
	}
	return shared.TestTraceIDBloom(r.compactParsed.traceIDBloom, traceID)
}

// FooterVersion returns the version number parsed from the file footer.
// Returns 3, 4, or 5 depending on the file format version.
func (r *Reader) FooterVersion() uint16 {
	return r.footerVersion
}

// VectorIndexRaw reads the raw vector index section bytes (for caller caching).
// Returns nil for V3/V4 footer files or files with no vector index section.
func (r *Reader) VectorIndexRaw() ([]byte, error) {
	if r.vectorIndexOffset == 0 || r.vectorIndexLen == 0 {
		return nil, nil
	}
	if r.vectorIndexLen > math.MaxInt32 {
		return nil, fmt.Errorf("VectorIndexRaw: vectorIndexLen %d exceeds MaxInt32", r.vectorIndexLen)
	}
	//nolint:gosec // vectorIndexLen comes from a trusted file footer; overflow checked above
	buf := make([]byte, r.vectorIndexLen)
	// vectorIndexOffset fits in int64: file size is bounded by object storage limits (< 2^63).
	vecOff := int64(r.vectorIndexOffset) //nolint:gosec
	n, err := r.provider.ReadAt(buf, vecOff, rw.DataTypeMetadata)
	if err != nil {
		return nil, fmt.Errorf("VectorIndexRaw: ReadAt: %w", err)
	}
	if n != int(r.vectorIndexLen) {
		return nil, fmt.Errorf("VectorIndexRaw: short read: %d of %d bytes", n, r.vectorIndexLen)
	}
	return buf, nil
}

// VectorIndex returns the lazily-parsed VectorIndex for this file.
// Returns nil, nil for files with no vector index section.
// Safe for concurrent use after the first call.
func (r *Reader) VectorIndex() (*VectorIndex, error) {
	r.vectorIndexOnce.Do(func() {
		raw, err := r.VectorIndexRaw()
		if err != nil {
			r.vectorIndexErr = err
			return
		}
		if raw == nil {
			return
		}
		r.vectorIndexParsed, r.vectorIndexErr = parseVectorIndexSection(raw)
	})
	return r.vectorIndexParsed, r.vectorIndexErr
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

	metas, _, err := parseColumnMetadataArray(bwb.RawBytes, 24, colCount, hdr.version)
	if err != nil {
		return fmt.Errorf("AddColumnsToBlock: column metadata: %w", err)
	}

	// Each AddColumnsToBlock call uses its own fresh intern map; ResetInternStrings
	// is a no-op and no cross-call intern reuse occurs.
	ctx := &decodeCtx{intern: make(map[string]string)}

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

		colData := bwb.RawBytes[start:end]
		if hdr.version == shared.VersionBlockV14 {
			// SPEC-ROOT-012: guard against decompression-bomb OOM.
			if m.uncompressedLen > uint32(shared.MaxBlockSize) { //nolint:gosec
				return fmt.Errorf(
					"AddColumnsToBlock: col %q: uncompressed_len %d exceeds MaxBlockSize",
					m.name,
					m.uncompressedLen,
				)
			}
			decompressed, decErr := snappy.Decode(nil, colData)
			if decErr != nil {
				return fmt.Errorf("AddColumnsToBlock: col %q snappy decode: %w", m.name, decErr)
			}
			colData = decompressed
		}

		decoded, err := readColumnEncoding(colData, spanCount, m.colType, ctx)
		if err != nil {
			return fmt.Errorf("AddColumnsToBlock: col %q: %w", m.name, err)
		}

		col.StringDict = decoded.StringDict
		col.StringIdx = decoded.StringIdx
		col.sparseDictIdx = decoded.sparseDictIdx // NOTE-PERF-1: lazy dense expansion
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
	bwb.Block.BuildIterFields()

	return nil
}

// GetBlockWithBytes reads, parses, and returns a full block. Compatibility shim that
// combines ReadBlockRaw + ParseBlockFromBytes into a single call.
//
// BUG-12 (latent trap): when secondPassCols is non-nil, a second ParseBlockFromBytes call
// is made using secondPassCols as the column filter. This second call REPLACES the bwb
// returned by the first pass — it does not merge with wantColumns. Any column present in
// wantColumns but absent from secondPassCols will be silently discarded.
//
// No current caller passes a non-nil secondPassCols (all call sites use nil), so there is
// no active data loss. If you ever need both wantColumns AND a second set of columns, call
// ParseBlockFromBytes twice and merge the results manually rather than relying on this shim.
func (r *Reader) GetBlockWithBytes(
	blockIdx int,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) (*BlockWithBytes, error) {
	// BUG-4 fix: secondPassCols must be nil — see BUG-12 comment. Check before I/O.
	if secondPassCols != nil {
		return nil, fmt.Errorf(
			"GetBlockWithBytes: secondPassCols must be nil (BUG-12: non-nil secondPassCols silently discards first-pass columns); got %d secondPassCols entries",
			len(secondPassCols),
		)
	}
	raw, err := r.ReadBlockRaw(blockIdx)
	if err != nil {
		return nil, err
	}
	bwb, err := r.ParseBlockFromBytes(raw, wantColumns, r.BlockMeta(blockIdx))
	if err != nil {
		return nil, err
	}
	return bwb, nil
}
