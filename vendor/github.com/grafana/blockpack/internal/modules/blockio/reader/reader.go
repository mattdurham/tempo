package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/modules/sectioncache"
)

// WantColumns specifies which columns to eagerly decode when parsing a block.
// Use WantAll() to load every column, or WantOnly(cols) for query-driven selection.
// A zero value (All=false, Columns=nil) is equivalent to WantOnly(empty) — no eager decodes.
type WantColumns struct {
	// Columns is the set of column names to eagerly decode. Ignored when All is true.
	Columns map[string]struct{}
	// All loads every column eagerly. Use only when the full column set is genuinely
	// required (e.g. tag-name enumeration, compaction, export). For query paths, set
	// Columns to the specific columns the query references.
	All bool
}

// WantAll returns a WantColumns that eagerly decodes every column in the block.
// Use only when all columns are genuinely needed — loading unused columns causes
// 300MB+ memory spikes per block in the query path.
func WantAll() WantColumns { return WantColumns{All: true} }

// WantOnly returns a WantColumns that eagerly decodes only the named columns.
// All other columns are registered lazily at zero decode cost.
func WantOnly(cols map[string]struct{}) WantColumns { return WantColumns{Columns: cols} }

// toInternalMap converts WantColumns to the internal nullable map used by parseBlockColumnsReuse.
// Returns nil (load all) when All=true, the column map otherwise.
func (w WantColumns) toInternalMap() map[string]struct{} {
	if w.All {
		return nil
	}
	return w.Columns
}

// Reader reads and decodes a blockpack file.
type Reader struct {
	provider rw.ReaderProvider
	// vectorIndexErr holds any error from lazy vector index parsing.
	vectorIndexErr error
	// cache is the typed cache used for footer/header/metadata/block reads.
	// Never nil: defaults to sectioncache.NopSectionCache when no cache is configured.
	// Assigned directly from Options.Cache; nil is normalized to NopSectionCache.
	cache sectioncache.SectionCache

	v8TSErr error
	// NOTE: v8ColStatsErr removed (2026-06-29, in-file block pruning removal).

	// Range index removed in #439.

	// intrinsicIndex holds the parsed TOC entries, keyed by column name.
	// Populated by parseIntrinsicTOC during NewReaderFromProvider. Nil for
	// v3 footer files or files with intrinsicIndexLen == 0.
	intrinsicIndex map[string]shared.IntrinsicColMeta

	// intrinsicKeyPrefix caches the fileID-derived prefix of every
	// parsedIntrinsicCache key for this Reader (fileID + "/intrinsic/").
	// NOTE-431: invariant across all intrinsic columns of one file, so it is
	// built once at construction (whenever fileID is non-empty) instead of being
	// re-concatenated per column lookup. Read-only after construction; see
	// intrinsicCacheKey. Empty when fileID is empty (process cache disabled).
	intrinsicKeyPrefix string

	// intrinsicDecoded caches fully decoded intrinsic columns by name.
	// Populated lazily by GetIntrinsicColumn. Protected by intrinsicMu.
	intrinsicDecoded map[string]*shared.IntrinsicColumn

	// preDecodedColumns holds LIVE decoded-column snapshots that
	// readBlockColumnarWithCache observed already present in the process-level
	// parsedV8ColumnCache (NOTE-200) at read time, keyed by (block offset, name, type).
	// NOTE-212: when a wanted column's decoded snapshot is already cached, the reader skips
	// copying its compressed blob into the assembled buffer (the warm-path memmove) because
	// the parser satisfies it from the decoded cache and never reads the compressed bytes.
	// Storing the LIVE pointer lets the parser consume it without re-probing the cache,
	// closing the eviction race a bare probe-then-skip would open (the snapshot could be
	// LRU-evicted between the reader's check and the parser's lookup, leaving the parser to
	// decompress stale assembled-buffer bytes). The map holds strong references for the
	// Reader's lifetime (one querier call), so its entries cannot be evicted out from under
	// the parse. Nil until the first hit on the warm columnar read path. Guarded by
	// preDecodedMu.
	preDecodedColumns map[preDecodedKey]*Column

	// preCompressedColumns holds LIVE compressed column blobs that the combined
	// ToC+columns GetMulti (NOTE-185) returned from memcache but whose DECODED snapshot was
	// NOT in the process-level parsedV8ColumnCache (so preDecodedColumns does not cover
	// them). NOTE-234: instead of copying each such blob into the assembled buffer (a warm-
	// path memmove) only for the parser to sub-slice it straight back out and snappy-decode,
	// the reader stashes the blob here and the parser uses it directly as the column's
	// compressed bytes — eliminating both the copy and that column's contribution to the
	// assembled buffer's size. The blob aliases the memcache GetMulti result, which the
	// section cache owns for the Reader's lifetime (one querier call); the parser copies all
	// data out during decode, so no longer-lived alias is created. Keyed by (block offset,
	// name, type) like preDecodedColumns. Nil until the first such hit. Guarded by
	// preDecodedMu (shared with preDecodedColumns — both populated on the warm columnar read
	// path and read by the parser).
	preCompressedColumns map[preDecodedKey][]byte

	// vectorIndexParsed is the lazily parsed VectorIndex. Access via VectorIndex().
	vectorIndexParsed *VectorIndex

	// tocMap is the decoded unified ToC (V8 format).
	tocMap map[shared.ToCKey]shared.ToCEntry

	// NOTE: colStats map removed (2026-06-29, in-file block pruning removal).

	fileID string

	// tsRaw holds the raw 20-byte-per-entry TS index body (a zero-copy sub-slice of
	// metadataBytes). tsCount is the number of entries. Entries are sorted by minTS
	// ascending (as written). Nil/0 for files written before the TS index was introduced.
	// NOTE-PERF-TS: raw bytes eliminate O(count) tsIndexEntry allocations at parse time.
	tsRaw []byte

	// Parsed during NewReaderFromProvider.
	blockMetas []shared.BlockMeta

	tsCount int

	fileSize int64

	vectorIndexOffset uint64

	// Unified-ToC footer fields (FooterV9, the only supported footer).
	// v8ToCOffset and v8ToCLen point to the snappy-compressed unified ToC blob.
	v8ToCOffset uint64

	// V8 lazy section errors and sync.Once guards (mirror of v14 ones).
	v8TSOnce sync.Once
	// NOTE: v8ColStatsOnce removed (2026-06-29, in-file block pruning removal).

	// vectorIndexOnce guards lazy parsing of the vector index section.
	vectorIndexOnce sync.Once

	// preDecodedMu guards preDecodedColumns. ReadGroupColumnar (which populates it) runs
	// concurrently across blockGroupPipeline workers on the same *Reader, while the parse
	// (which reads it) runs on the sequential consumer goroutine — and a read of group N+1
	// can race the parse of group N. NOTE-212.
	preDecodedMu sync.Mutex

	// vectorIndexLen is parsed from the agentic v5 footer.
	vectorIndexLen uint32

	v8ToCLen uint32

	fileVersion uint8

	// footerVersion is the parsed file-level footer version (always FooterV9Version).
	footerVersion uint16

	// signalType is set from the V8 ToC signal_type byte.
	// Defaults to shared.SignalTypeTrace (0x01) when the field is absent.
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

	sc := opts.Cache
	if sc == nil || reflect.ValueOf(sc).IsNil() {
		sc = sectioncache.NopSectionCache
	}
	r := &Reader{
		provider: provider,
		cache:    sc,
		fileID:   opts.FileID,
		fileSize: size,
	}
	// NOTE-431: precompute the invariant parsedIntrinsicCache key prefix once.
	if r.fileID != "" {
		r.intrinsicKeyPrefix = r.fileID + "/intrinsic/"
	}

	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: %w", err)
	}

	if err = r.parseSectionsV8(); err != nil {
		return nil, fmt.Errorf("NewReaderFromProvider: V8 sections: %w", err)
	}
	return r, nil
}

// NewLeanReaderFromProvider constructs a Reader using only 3 I/Os: the footer,
// the compact trace index section, and the section directory (V13+ files).
// This is the optimal path for FindTraceByID workloads. Falls back to
// NewReaderFromProvider for files without a compact trace index (compactLen == 0).
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

	sc := opts.Cache
	if sc == nil || reflect.ValueOf(sc).IsNil() {
		sc = sectioncache.NopSectionCache
	}
	r := &Reader{
		provider: provider,
		cache:    sc,
		fileID:   opts.FileID,
		fileSize: size,
	}
	// NOTE-431: precompute the invariant parsedIntrinsicCache key prefix once.
	if r.fileID != "" {
		r.intrinsicKeyPrefix = r.fileID + "/intrinsic/"
	}

	// I/O #1: read footer.
	if err = r.readFooter(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: footer: %w", err)
	}

	if err = r.parseSectionsV8(); err != nil {
		return nil, fmt.Errorf("NewLeanReaderFromProvider: V8 sections: %w", err)
	}
	return r, nil
}

// BlockCount returns the number of blocks in the file.
func (r *Reader) BlockCount() int { return len(r.blockMetas) }

// TraceCount returns 0. The TraceID/DFS index was removed in #438.
// Trace counts are no longer maintained in data files; use the value-index instead.
func (r *Reader) TraceCount() int { return 0 }

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

// ReadBlockRaw reads the raw bytes for the block at blockIdx from the provider.
func (r *Reader) ReadBlockRaw(blockIdx int) ([]byte, error) {
	if blockIdx < 0 || blockIdx >= len(r.blockMetas) {
		return nil, fmt.Errorf("ReadBlockRaw: blockIdx %d out of range [0, %d)", blockIdx, len(r.blockMetas))
	}
	meta := r.blockMetas[blockIdx]
	return r.readRange(meta.Offset, meta.Length, rw.DataTypeBlock)
}

// ReadBlockByRef fetches a v2 block directly by its page-addressed BlockRef, without
// consulting the block index or TOC. This is the one-round-trip path for value-index
// query results from v2 files (NOTE-VI-027, issue #417 PR6).
// The returned bytes include the full block payload (including any alignment padding);
// callers should pass the unpadded Length from BlockMeta or the entry BlockRef when
// parsing the block.
func (r *Reader) ReadBlockByRef(pageNum uint32, lenPages uint16) ([]byte, error) {
	const pageSize = 4096
	offset := uint64(pageNum) * pageSize
	length := uint64(lenPages) * pageSize
	if length == 0 {
		return nil, fmt.Errorf("ReadBlockByRef: lenPages must be > 0")
	}
	return r.readRange(offset, length, rw.DataTypeBlock)
}

// ReadBlocks reads raw bytes for the given block indices using aggressive coalescing.
// Adjacent block ranges are merged into as few I/O operations as possible (NOTES §13).
// Returns a map from block index to raw byte slice. Invalid indices are silently skipped.
// SPEC-ROOT-015: all block byte fetches route through r.cache via ReadGroup.
func (r *Reader) ReadBlocks(blockIndices []int) (map[int][]byte, error) {
	if len(blockIndices) == 0 {
		return make(map[int][]byte), nil
	}
	groups := r.CoalescedGroups(blockIndices)
	result := make(map[int][]byte, len(blockIndices))
	for _, cr := range groups {
		groupData, err := r.ReadGroup(cr)
		if err != nil {
			return nil, err
		}
		for k, v := range groupData {
			result[k] = v
		}
	}
	return result, nil
}

// CoalescedGroups partitions blockIndices into coalesced read groups (~8 MB each) without
// performing any I/O. Used by lazy-fetch callers that stop early on a limit.
func (r *Reader) CoalescedGroups(blockIndices []int) []shared.CoalescedRead {
	return CoalesceBlocks(r.blockMetas, blockIndices, shared.AggressiveCoalesceConfig)
}

// ReadGroup performs the I/O for a single CoalescedRead group.
// All block byte fetches are routed through r.cache: cache hits avoid S3 I/O; on any
// cache miss the full group is fetched via ReadCoalescedBlocks and every block is stored
// back into r.cache for future reads.
// SPEC-ROOT-015: raw block bytes always route through r.cache; never directly via provider.
func (r *Reader) ReadGroup(cr shared.CoalescedRead) (map[int][]byte, error) {
	if r.fileID == "" {
		// No stable cache key; fall through to direct fetch.
		return ReadCoalescedBlocks(r.provider, []shared.CoalescedRead{cr})
	}

	result := make(map[int][]byte, len(cr.BlockIDs))
	allHit := true
	for _, blockID := range cr.BlockIDs {
		val, ok, err := r.cache.GetBlockColumns(r.fileID, blockID)
		if err != nil {
			return nil, fmt.Errorf("ReadGroup: cache get block %d: %w", blockID, err)
		}
		if ok {
			result[blockID] = val
		} else {
			allHit = false
		}
	}
	if allHit {
		return result, nil
	}

	// At least one cache miss: fetch the full group from object storage and populate cache.
	fetched, err := ReadCoalescedBlocks(r.provider, []shared.CoalescedRead{cr})
	if err != nil {
		return nil, err
	}
	for blockID, data := range fetched {
		_ = r.cache.CacheBlockColumns(r.fileID, blockID, data) // cache write failure is non-fatal
		result[blockID] = data
	}
	return result, nil
}

// ParseBlockFromBytes parses a Block from raw bytes using the given meta and column filter.
// wantColumns nil = all columns.
// Each call allocates its own fresh intern map; ResetInternStrings is a no-op and
// no cross-call intern reuse occurs.
// ParseBlockFromBytes parses a Block from raw bytes.
// want controls which columns are eagerly decoded — use WantAll() or WantOnly(cols).
// Passing WantOnly with a query-derived column set avoids 300MB+ memory spikes from
// loading unused columns; WantAll() is for cases where every column is needed.
func (r *Reader) ParseBlockFromBytes(
	rawBytes []byte,
	want WantColumns,
	meta shared.BlockMeta,
) (*BlockWithBytes, error) {
	localIntern := make(map[string]string)
	blk, err := parseBlockColumnsReuse(
		rawBytes,
		want.toInternalMap(),
		nil,
		meta,
		localIntern,
		r.fileID,
		r.preDecodedLookup(),
		r.preCompressedLookup(),
	)
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
	want WantColumns,
	meta shared.BlockMeta,
	intern map[string]string,
) (*BlockWithBytes, error) {
	blk, err := parseBlockColumnsReuse(
		rawBytes,
		want.toInternalMap(),
		nil,
		meta,
		intern,
		r.fileID,
		r.preDecodedLookup(),
		r.preCompressedLookup(),
	)
	if err != nil {
		return nil, fmt.Errorf("ParseBlockFromBytesWithIntern: %w", err)
	}
	return &BlockWithBytes{Block: blk, RawBytes: rawBytes}, nil
}

// preDecodedLookup returns a function that resolves a reader-pre-resolved decoded-column
// snapshot under preDecodedMu (NOTE-212), or nil when no columns were pre-resolved so the
// parser pays no lock or call on the common path. The lock is required because reads of
// later groups (which populate the map) run concurrently with the parse of earlier groups.
func (r *Reader) preDecodedLookup() func(preDecodedKey) *Column {
	r.preDecodedMu.Lock()
	empty := len(r.preDecodedColumns) == 0
	r.preDecodedMu.Unlock()
	if empty {
		return nil
	}
	return func(k preDecodedKey) *Column {
		r.preDecodedMu.Lock()
		defer r.preDecodedMu.Unlock()
		return r.preDecodedColumns[k]
	}
}

// preCompressedLookup returns a function resolving a reader-stashed compressed column blob
// (NOTE-234) under preDecodedMu, or nil when none were stashed so the parser pays no lock or
// call on the common path. Mirrors preDecodedLookup; the lock guards concurrent population by
// later block-group reads while earlier groups parse.
func (r *Reader) preCompressedLookup() func(preDecodedKey) []byte {
	r.preDecodedMu.Lock()
	empty := len(r.preCompressedColumns) == 0
	r.preDecodedMu.Unlock()
	if empty {
		return nil
	}
	return func(k preDecodedKey) []byte {
		r.preDecodedMu.Lock()
		defer r.preDecodedMu.Unlock()
		return r.preCompressedColumns[k]
	}
}

// HasTraceIndex always returns false. The TraceID/DFS index was removed in #438.
func (r *Reader) HasTraceIndex() bool { return false }

// BlocksForTraceID always returns nil. The TraceID/DFS index was removed in #438.
// Callers should use the value-index pipeline for trace lookups.
func (r *Reader) BlocksForTraceID(_ [16]byte) []int { return nil }

// BlocksForTraceIDCompact always returns nil. The compact trace index was removed in #438.
func (r *Reader) BlocksForTraceIDCompact(_ [16]byte) []int { return nil }

// TraceEntry is a single trace-block reference.

// TraceEntries always returns nil. The TraceID/DFS index was removed in #438.
// Callers (GetTraceByID, structural expansion) fall back to all-blocks scan.
func (r *Reader) TraceEntries(_ [16]byte) []TraceEntry { return nil }

// ResetInternStrings is a no-op retained for API compatibility with existing scan loops.
// ParseBlockFromBytes and AddColumnsToBlock now allocate their own fresh intern map per
// call, so there is no shared state to reset between blocks.
func (r *Reader) ResetInternStrings() {}

// TraceBloomRaw returns nil. The compact trace bloom was removed with the TraceID index (#438).
func (r *Reader) TraceBloomRaw() []byte { return nil }

// MayContainTraceID returns true (conservative). The compact trace bloom was removed
// with the TraceID index (#438). Callers must use the value-index for trace:id filtering.
func (r *Reader) MayContainTraceID(_ [16]byte) bool { return true }

// NOTE: FooterVersion() and IsV2Format() removed (2026-06-29, v2 lean format unconditional).
// All files are FooterV9 (v2 lean format); there is no longer a version to branch on.

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

		if existing, exists := bwb.Block.columns[shared.ColumnKey{Name: m.name, Type: m.colType}]; exists {
			// If the column was lazily registered (decoded==false), decode it now so the
			// caller gets a fully-populated column. Eagerly-decoded columns are left alone.
			if existing.needsDecode() {
				existing.EnsureDecoded()
			}
			continue
		}

		if m.compressedLen == 0 && m.inlineData == nil {
			continue
		}

		col := &Column{
			Name: m.name,
			Type: m.colType,
		}

		var colData []byte
		if m.inlineData != nil {
			// NOTE-220: inline column — raw blob in the TOC entry, no offset/snappy.
			colData = m.inlineData
		} else {
			start := int(m.dataOffset)          //nolint:gosec // safe: dataOffset bounded by block size < MaxBlockSize
			end := start + int(m.compressedLen) //nolint:gosec // safe: compressedLen bounded by block size < MaxBlockSize
			if start < 0 || end > len(bwb.RawBytes) {
				return fmt.Errorf(
					"AddColumnsToBlock: col %q data offset %d len %d out of range",
					m.name, m.dataOffset, m.compressedLen,
				)
			}

			// SPEC-V14-001: column blobs are snappy-compressed; decompress before decode.
			// SPEC-ROOT-012: decompressV14ColumnData guards against decompression-bomb OOM.
			colData, err = decompressV14ColumnData(m.name, bwb.RawBytes[start:end], m.uncompressedLen, m.zstd)
			if err != nil {
				return fmt.Errorf("AddColumnsToBlock: %w", err)
			}
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
		col.uniformSlab = decoded.uniformSlab     // NOTE-351
		col.uniformStride = decoded.uniformStride // NOTE-351
		col.Present = decoded.Present
		col.SpanCount = decoded.SpanCount
		col.denseFlatIdx = decoded.denseFlatIdx // NOTE-358
		col.decoded.Store(true)                 // NOTE-CONC-001: mark eagerly decoded so needsDecode() is false

		bwb.Block.columns[shared.ColumnKey{Name: m.name, Type: m.colType}] = col
	}

	bwb.Block.buildNameIndex()
	// NOTE-243: AddColumnsToBlock mutates the columns map after the initial parse (the
	// second-pass decode). Reset the lazy iterFields so the next IterFields() rebuilds from
	// the now-complete column set. The once is reset (not eagerly rebuilt) so a metrics-style
	// caller that never enumerates fields still pays nothing.
	bwb.Block.resetIterFields()

	return nil
}

// GetBlockWithBytes reads, parses, and returns a full block. Compatibility shim that
// combines ReadBlockRaw + ParseBlockFromBytes into a single call.
func (r *Reader) GetBlockWithBytes(
	blockIdx int,
	wantColumns map[string]struct{},
) (*BlockWithBytes, error) {
	raw, err := r.ReadBlockRaw(blockIdx)
	if err != nil {
		return nil, err
	}
	bwb, err := r.ParseBlockFromBytes(raw, WantOnly(wantColumns), r.BlockMeta(blockIdx))
	if err != nil {
		return nil, err
	}
	return bwb, nil
}

// ToCSection describes one entry in the file's Table of Contents.
type ToCSection struct {
	Name            string
	Offset          int64
	CompressedBytes int64
	Type            uint32
	SubType         uint32
}

// ToCEntries returns all entries in the file's Table of Contents.
// Cheap — the ToC is fully parsed during NewReaderFromProvider.
func (r *Reader) ToCEntries() []ToCSection {
	out := make([]ToCSection, 0, len(r.tocMap))
	for k, e := range r.tocMap {
		out = append(out, ToCSection{
			Type:            k.Type,
			SubType:         k.SubType,
			Name:            k.Name,
			Offset:          int64(e.Offset), //nolint:gosec
			CompressedBytes: int64(e.Length), //nolint:gosec
		})
	}
	return out
}
