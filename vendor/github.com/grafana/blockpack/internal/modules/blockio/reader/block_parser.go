package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/klauspost/compress/snappy"
)

// blockHeader holds the parsed block header fields.

// resolveColumnData returns the (still-compressed unless inline) bytes for column m. It
// prefers, in order: the inline blob carried in the TOC entry (NOTE-220, no outer snappy);
// a reader-stashed compressed blob from the combined ToC+columns GetMulti whose decoded
// snapshot was not cached (NOTE-234) — in which case the assembled buffer's extent for this
// column was never written and must not be read; and finally the column's extent in rawBytes
// (the assembled buffer). Callers snappy-decompress the result unless m.inlineData != nil.
func resolveColumnData(
	rawBytes []byte,
	meta shared.BlockMeta,
	m colMetaEntry,
	preCompressedLookup func(preDecodedKey) []byte,
) ([]byte, error) {
	if m.inlineData != nil {
		return m.inlineData, nil
	}
	if preCompressedLookup != nil {
		if blob := preCompressedLookup(
			preDecodedKey{blockOffset: meta.Offset, name: m.name, colType: m.colType},
		); blob != nil {
			return blob, nil
		}
	}
	start := int(m.dataOffset) //nolint:gosec // safe: dataOffset bounded by block size < MaxBlockSize
	end := start + int(
		m.compressedLen,
	) //nolint:gosec // safe: compressedLen bounded by block size < MaxBlockSize
	if start < 0 || end > len(rawBytes) {
		return nil, fmt.Errorf(
			"parseBlock: col %q data offset %d len %d out of range (block %d bytes)",
			m.name, m.dataOffset, m.compressedLen, len(rawBytes),
		)
	}
	return rawBytes[start:end], nil
}

// colMetaEntry holds one parsed column metadata entry.

// snappy-compressed byte length on disk
// raw byte length after snappy decompress

// Block header field offsets (M-28a).
// Wire format: magic[4]+version[1]+reserved[3]+span_count[4]+column_count[4]+reserved2[8] = 24 bytes.
const (
	blkHdrOffVersion     = 4  // uint8 block version field
	blkHdrOffSpanCount   = 8  // uint32 span_count field
	blkHdrOffColumnCount = 12 // uint32 column_count field
)

// parseBlockHeader parses the 24-byte block header from data.
func parseBlockHeader(data []byte) (blockHeader, error) {
	if len(data) < int(shared.BlockHeaderV14Size) {
		return blockHeader{}, fmt.Errorf("block header: need %d bytes, have %d", shared.BlockHeaderV14Size, len(data))
	}

	hdr := blockHeader{
		magic:       binary.LittleEndian.Uint32(data[0:]),
		version:     data[blkHdrOffVersion],
		spanCount:   binary.LittleEndian.Uint32(data[blkHdrOffSpanCount:]),
		columnCount: binary.LittleEndian.Uint32(data[blkHdrOffColumnCount:]),
		// bytes 16-23: reserved2 (formerly trace_count + trace_table_len, always zero)
	}

	if hdr.magic != shared.MagicNumber {
		return blockHeader{}, fmt.Errorf("block header: bad magic 0x%08X", hdr.magic)
	}

	// SPEC-ROOT-013: only enc_version=3 (V14 column encoding) is supported by this decoder.
	// V12 blocks use enc_version=2 and are not readable; they must be compacted to V14 first.
	// Accepting V12 here would cause a misleading "unsupported version 2" error deep in column decode.
	// NOTE-220: V15 adds the inline-column TOC flag; its block header layout is identical
	// to V14, so both versions parse the same header and differ only in the per-column TOC
	// entry layout (handled in parseColumnMetadataArray, which branches on hdr.version).
	if hdr.version != shared.VersionBlockV14 && hdr.version != shared.VersionBlockV15 {
		return blockHeader{}, fmt.Errorf(
			"block header: version %d not supported (only V14/V15 supported; V12 files must be compacted to V14+ first)",
			hdr.version,
		)
	}

	// SPEC-ROOT-011: reject implausible field counts to prevent pre-allocation OOM.
	if hdr.columnCount > uint32(shared.MaxColumns) { //nolint:gosec
		return blockHeader{}, fmt.Errorf(
			"block header: column_count %d exceeds MaxColumns %d",
			hdr.columnCount, shared.MaxColumns,
		)
	}
	if hdr.spanCount > uint32(shared.MaxSpans) { //nolint:gosec
		return blockHeader{}, fmt.Errorf(
			"block header: span_count %d exceeds MaxSpans %d",
			hdr.spanCount, shared.MaxSpans,
		)
	}

	return hdr, nil
}

// parseColumnMetadataArray parses colCount column metadata entries starting at offset.
//
// V14 wire format per entry (SPEC-V14-001):
//
//	name_len[2] + name[name_len] + col_type[1] + data_offset[8] + compressed_len[4] + uncompressed_len[4]
//
// V15 wire format per entry (SPEC-V15-001, NOTE-220) adds a flags byte after col_type:
//
//	name_len[2] + name[name_len] + col_type[1] + flags[1]
//	+ if flags.inline == 0: data_offset[8] + compressed_len[4] + uncompressed_len[4]
//	+ if flags.inline == 1: inline_len[1] (≤ ColInlineMaxLen) + inline_data[inline_len]
//
// Returns entries and the new offset after the last entry. blockVersion selects the layout.
func parseColumnMetadataArray(data []byte, offset, colCount int, blockVersion uint8) ([]colMetaEntry, int, error) {
	entries := make([]colMetaEntry, 0, colCount)
	pos := offset
	v15 := blockVersion == shared.VersionBlockV15

	for i := range colCount {
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for name_len", i)
		}

		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if nameLen > shared.MaxNameLen {
			return nil, pos, fmt.Errorf("col_meta[%d]: name_len %d exceeds MaxNameLen", i, nameLen)
		}

		if pos+nameLen > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for name", i)
		}

		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// col_type[1] (+ flags[1] for V15) must be present before the variable tail.
		typeHdr := 1
		if v15 {
			typeHdr = 2
		}
		if pos+typeHdr > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for type", i)
		}

		colType := shared.ColumnType(data[pos])
		pos++

		var flags uint8
		if v15 {
			flags = data[pos]
			pos++
		}

		// NOTE-220: V15 inline column — raw blob follows the flags byte directly.
		if v15 && flags&shared.ColFlagInline != 0 {
			if pos+1 > len(data) {
				return nil, pos, fmt.Errorf("col_meta[%d]: short for inline_len", i)
			}
			inlineLen := int(data[pos])
			pos++
			if pos+inlineLen > len(data) {
				return nil, pos, fmt.Errorf("col_meta[%d]: short for inline_data", i)
			}
			entries = append(entries, colMetaEntry{
				name:            name,
				colType:         colType,
				inlineData:      data[pos : pos+inlineLen],
				uncompressedLen: uint32(inlineLen), //nolint:gosec // safe: inlineLen ≤ ColInlineMaxLen (255)
			})
			pos += inlineLen
			continue
		}

		// colMetaFixedSize: data_offset[8] + compressed_len[4] + uncompressed_len[4] = 16 bytes
		const colMetaFixedSize = 16
		if pos+colMetaFixedSize > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for offsets", i)
		}

		dataOffset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		// SPEC-V14-001: compressed_len[4 LE] + uncompressed_len[4 LE]
		compressedLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		uncompressedLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		entries = append(entries, colMetaEntry{
			name:            name,
			colType:         colType,
			dataOffset:      dataOffset,
			compressedLen:   compressedLen,
			uncompressedLen: uncompressedLen,
		})
	}

	return entries, pos, nil
}

// parseBlockColumnsReuse decodes rawBytes into a Block.
// wantColumns: if non-nil, only decode columns in this set.
// prevBlock: if non-nil and same column set, reuse Column allocations.
// intern is the caller's per-reader string intern table; if nil a new map is used.
// decompBufPool holds temporary snappy decompression buffers.
// Each column decode in parseBlockColumnsReuse decompresses into this buffer and immediately
// calls readColumnEncoding, which fully copies all decoded data (Present bitmap, Dict entries,
// Idx arrays) out of the buffer. No Column field sub-slices the decompressed bytes, so the
// buffer is safe to reuse for the next column in the same parse call.
var decompBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 256<<10); return &b }}

// decompBufMaxPooledCap bounds the backing capacity any buffer may carry back into
// decompBufPool. NOTE-346: decompressV14ColumnDataInto grows dst up to the column's
// uncompressed length, which is only bounded by shared.MaxBlockSize (1 GiB). A single
// large-column decode (e.g. a wide trace:id/span:id XORBytes column) therefore grew its
// pooled buffer to tens/hundreds of MiB, and the unconditional Put pinned that giant
// backing array in the pool for the process lifetime. Under concurrent heavy queries the
// pool accumulated several such arrays, keeping querier RSS elevated long after the load
// that produced them had drained — sync.Pool only releases entries across GC cycles, but a
// buffer this large that keeps getting reused never ages out. Buffers whose capacity
// exceeds this cap are dropped (GC'd) instead of pooled, so the rare giant decode does not
// leave a permanent high-water allocation behind; the next decode re-grows from the 256 KiB
// baseline. This mirrors assembledBufMaxPooledCap (NOTE-208). The cap is comfortably above
// the warm-path column-decode working set (a 256 KiB seed already covers typical columns),
// so the common case still recycles its buffer with no churn.
const decompBufMaxPooledCap = 8 << 20 // 8 MiB

// putDecompBuf returns a snappy-decode scratch buffer to decompBufPool, dropping any buffer
// grown beyond decompBufMaxPooledCap (NOTE-346) so an outsized one-off decode cannot pin a
// large backing array in the pool indefinitely. ptr must be non-nil.
func putDecompBuf(ptr *[]byte) {
	if cap(*ptr) > decompBufMaxPooledCap {
		return // drop oversized buffer: let it GC rather than pin RSS in the pool
	}
	decompBufPool.Put(ptr)
}

// NOTE-153: lazyColumnStore (NOTE-002) is a per-block []Column arena sized to the column count,
// allocated fresh on every WantOnly parse. A 2026-06-09 querier alloc_space profile showed this
// single make([]Column, 0, len(metas)) as the largest allocator (~21% / 29 GB) on the metrics
// block-scan path (attribute group-by queries, e.g. M6 rate() by span.http.request.method): the
// arena holds Column structs for *non-wanted* columns that exist only so a caller *could* lazily
// decode them, but the two-pass WantOnly path only ever touches wanted columns (plus a lazy
// span:start on v3 files, decoded inside the row loop). Pool the backing array; the caller returns
// it via Block.ReleaseLazyColumnStore once the block is fully scanned. Entries are zeroed on
// release so retained compressedEncoding sub-slices (which alias rawBytes) cannot keep large block
// buffers alive through the pool, and any lazily-decoded slices are dropped for GC.
var lazyColumnStorePool = sync.Pool{New: func() any { s := make([]Column, 0, 64); return &s }}

// acquireLazyColumnStore returns a pooled *[]Column whose slice has length 0 and capacity ≥ n.
func acquireLazyColumnStore(n int) *[]Column {
	p, _ := lazyColumnStorePool.Get().(*[]Column)
	if cap(*p) < n {
		*p = make([]Column, 0, n)
	} else {
		*p = (*p)[:0]
	}
	return p
}

// ReleaseLazyColumnStore returns the block's lazy-column arena to the shared pool.
//
// NOTE-153 SAFETY: call only after the block is fully consumed — i.e. after every row has been
// scanned and no lazy (non-wanted) column will be accessed again. Eagerly-decoded (wanted)
// columns are unaffected: their decoded data is owned by Column structs that the columns map
// references directly, not by this arena (the arena holds only lazily-registered columns).
// Entries are zeroed before Put so the pool retains no references to rawBytes (via
// compressedEncoding) or to any lazily-decoded dict/idx slices. Idempotent; nil-safe.
func (b *Block) ReleaseLazyColumnStore() {
	if b == nil || b.lazyStorePtr == nil {
		return
	}
	s := *b.lazyStorePtr
	for i := range s {
		s[i] = Column{}
	}
	*b.lazyStorePtr = s[:0]
	lazyColumnStorePool.Put(b.lazyStorePtr)
	b.lazyStorePtr = nil
	b.lazyColumnStore = nil
}

// resolveBlockColMetas returns the block's column-metadata array, reusing the per-block parsed
// ToC cached by NOTE-241/242 (blockColTypesCache) when present and parsing + caching it otherwise.
// On a hit the returned slice is the shared READ-ONLY cached metas; callers must not mutate it (the
// parser only reads entries). On a miss it parses parseColumnMetadataArray from rawBytes at offset 24
// and caches the result keyed by fileID+blockOff so a later warm read (reader or parser) skips the
// re-parse. fileID == "" disables the cache, parsing on every call as before. NOTE-242.
func resolveBlockColMetas(
	rawBytes []byte,
	fileID string,
	blockOff uint64,
	colCount int,
	blockVersion uint8,
) ([]colMetaEntry, error) {
	if fileID != "" {
		if cached := blockColTypesCache.Get(blockColTypesCacheKey(fileID, blockOff)); cached != nil {
			return cached.metas, nil
		}
	}
	metas, tocEnd, err := parseColumnMetadataArray(rawBytes, 24, colCount, blockVersion)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: column metadata: %w", err)
	}
	cacheParsedBlockColTypes(fileID, blockOff, metas, tocEnd)
	return metas, nil
}

func parseBlockColumnsReuse(
	rawBytes []byte,
	wantColumns map[string]struct{},
	prevBlock *Block,
	meta shared.BlockMeta,
	intern map[string]string,
	fileID string,
	preDecodedLookup func(preDecodedKey) *Column,
	preCompressedLookup func(preDecodedKey) []byte,
) (*Block, error) {
	if intern == nil {
		intern = make(map[string]string)
	}
	ctx := &decodeCtx{intern: intern}

	// Acquire a reusable decompression buffer for all columns in this block.
	// Returned to the pool after all columns are decoded.
	decompBufPtr := decompBufPool.Get().(*[]byte)
	decompBuf := (*decompBufPtr)[:0]
	defer func() {
		*decompBufPtr = decompBuf[:0]
		putDecompBuf(decompBufPtr)
	}()
	hdr, err := parseBlockHeader(rawBytes)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: %w", err)
	}

	spanCount := int(hdr.spanCount)
	colCount := int(hdr.columnCount)

	// NOTE-242: reuse the per-block parsed column-metadata array if an earlier read cached
	// it (blockColTypesCache, populated at first ToC parse by NOTE-241). parseColumnMetadataArray
	// re-decodes every metadata entry from the ToC bytes and allocates one string(name) per
	// column plus the entries slice — all deterministic for a given block. NOTE-241 cached this
	// exact slice for the columnar-read sizing/prune pass but the parser still re-parsed it on
	// every warm block read, so for trace blocks with hundreds of columns the same allocation ran
	// twice per query. Probing the cache here elides the parser's copy of that work too; on a hit
	// the shared READ-ONLY metas are reused (the parser never mutates an entry — verified all use
	// sites read m.name/m.colType/m.dataOffset/etc. only). The header is still parsed (cheap,
	// alloc-free) for spanCount/columnCount. On a miss (cold block, no fileID) we parse + cache as
	// before, so the result is byte-for-byte identical.
	metas, err := resolveBlockColMetas(rawBytes, fileID, meta.Offset, colCount, hdr.version)
	if err != nil {
		return nil, err
	}

	var columns map[shared.ColumnKey]*Column
	if prevBlock != nil && prevBlock.columns != nil {
		columns = prevBlock.columns
		// Clear values from existing columns; we will re-populate.
		for _, col := range columns {
			resetColumn(col)
		}
	} else {
		columns = make(map[shared.ColumnKey]*Column, colCount)
	}

	for _, m := range metas {
		if wantColumns != nil {
			if _, ok := wantColumns[m.name]; !ok {
				continue
			}
		}

		// Trace-level columns (compressedLen == 0) are skipped here.
		// NOTE-220: an inline column also has compressedLen == 0 but carries inlineData,
		// so it must NOT be skipped — only a column with neither blob nor inline data is.
		if m.compressedLen == 0 && m.inlineData == nil {
			continue
		}

		key := shared.ColumnKey{Name: m.name, Type: m.colType}
		var col *Column
		if existing, ok := columns[key]; ok {
			col = existing
		} else {
			col = &Column{}
		}

		col.Name = m.name
		col.Type = m.colType

		// NOTE-212: a reader-pre-resolved decoded snapshot takes priority over both the
		// process cache and the compressed bytes. readBlockColumnarWithCache observed this
		// column's decoded snapshot already present in parsedV8ColumnCache and therefore
		// SKIPPED copying its compressed blob into the assembled buffer — so rawBytes for
		// this column's extent are NOT valid here. The live snapshot it stashed must be
		// used. Holding the live pointer (rather than re-probing parsedV8ColumnCache)
		// closes the eviction race: the snapshot cannot be LRU-evicted while the Reader
		// retains it, so we never fall through to decompressing stale assembled bytes. The
		// lookup is nil when nothing was pre-resolved (the common path pays no lock/call).
		if preDecodedLookup != nil {
			if snap := preDecodedLookup(preDecodedKey{blockOffset: meta.Offset, name: m.name, colType: m.colType}); snap != nil {
				copyDecodedColumnInto(col, snap)
				columns[key] = col
				continue
			}
		}

		// NOTE-200: consult the process-level decoded-column cache before snappy
		// decompress + readColumnEncoding. A Reader is created fresh per query (per block
		// per querier call), so the same on-disk block's wanted columns were re-decoded on
		// every warm query. The cache key is keyed on the block's stable byte offset within
		// the file plus the column name+type. On a hit we copy the immutable decoded slices
		// into the per-query col (which keeps its own fresh sync.Once / sparseDictIdx so the
		// lazy dense expansion of NOTE-PERF-1 runs per query and never mutates the shared
		// snapshot). On a miss we decode and store a snapshot.
		v8Key := ""
		if fileID != "" {
			v8Key = v8ColumnCacheKey(fileID, meta.Offset, m.name, m.colType)
			if cached := parsedV8ColumnCache.Get(v8Key); cached != nil {
				copyDecodedColumnInto(col, cached)
				columns[key] = col
				continue
			}
		}

		colData, cdErr := resolveColumnData(rawBytes, meta, m, preCompressedLookup)
		if cdErr != nil {
			return nil, cdErr
		}
		if m.inlineData == nil {

			// SPEC-V14-001: each column blob is snappy-compressed; decompress before decode.
			// SPEC-ROOT-012: decompressV14ColumnData guards against decompression-bomb OOM.
			// Reuse decompBuf across columns: all decoders copy data out (Present bitmap,
			// Dict values, Idx arrays), so colData is safe to overwrite after readColumnEncoding.
			var decErr error
			colData, decompBuf, decErr = decompressV14ColumnDataInto(decompBuf, m.name, colData, m.uncompressedLen)
			if decErr != nil {
				return nil, fmt.Errorf("parseBlock: %w", decErr)
			}
		}

		decoded, err := readColumnEncoding(colData, spanCount, m.colType, ctx)
		if err != nil {
			return nil, fmt.Errorf("parseBlock: col %q: %w", m.name, err)
		}

		// Copy decoded fields into col (preserving pointer if reusing).
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
		col.uniformSlab = decoded.uniformSlab     // NOTE-351
		col.uniformStride = decoded.uniformStride // NOTE-351
		col.Present = decoded.Present
		col.SpanCount = decoded.SpanCount
		col.sparseDictIdx = decoded.sparseDictIdx // NOTE-PERF-1: lazy dense expansion
		col.denseFlatIdx = decoded.denseFlatIdx   // NOTE-358
		col.decoded.Store(true)                   // NOTE-CONC-001: mark eagerly decoded so needsDecode() is false

		// NOTE-200: store an immutable snapshot of the decoded slices for reuse by later
		// queries on the same block. The snapshot shares the freshly-decoded slices (they
		// are never mutated in place — dense expansion builds a new Idx slice on the
		// per-query col, leaving the snapshot's sparseDictIdx intact). Put is a no-op on
		// caches that have not been sized; LRU evicts under the configured byte budget.
		if v8Key != "" {
			snap := snapshotDecodedColumn(decoded, m.name, m.colType)
			_ = parsedV8ColumnCache.Put(v8Key, snap)
		}

		columns[key] = col
	}

	// NOTE-001: Lazy registration — when wantColumns is non-nil, eagerly-skipped columns
	// are registered with compressedEncoding pointing into rawBytes and NO immediate decode.
	// Both snappy decompression and full column decode are deferred to the first accessor call
	// (ensureDecompressed then decodeNow). IsPresent() also triggers full decode on a lazy
	// column (it calls decodeNow via decodeOnce). SPEC-V14-002: non-wanted columns pay zero
	// CPU/memory cost for decompression until first accessed.
	//
	// NOTE-002: Arena-like pre-allocation — one []Column slice sized to len(metas)
	// replaces N individual *Column heap allocations. Pointers into the slice are stable
	// because capacity is fixed upfront and append never reallocates.
	var lazyStore []Column
	var lazyStorePtr *[]Column // NOTE-153: pool handle, returned via ReleaseLazyColumnStore
	if wantColumns != nil {
		lazyStorePtr = acquireLazyColumnStore(len(metas))
		lazyStore = *lazyStorePtr
		for _, m := range metas {
			if _, wanted := wantColumns[m.name]; wanted {
				continue // already eagerly decoded
			}

			if m.compressedLen == 0 && m.inlineData == nil {
				continue // trace-level column, no data
			}

			key := shared.ColumnKey{Name: m.name, Type: m.colType}
			if _, exists := columns[key]; exists {
				continue // already registered (shouldn't happen, but guard)
			}

			var ok bool
			lazyStore, ok = appendLazyColumn(lazyStore, m, rawBytes, spanCount, fileID, meta.Offset)
			if !ok {
				continue
			}
			// Safe: cap was set to len(metas) and we append ≤ len(metas) items, so no realloc.
			columns[key] = &lazyStore[len(lazyStore)-1]
		}
		// NOTE-153: write the (possibly grown) slice header back to the pool handle so the
		// pooled backing array — and the &lazyStore[i] pointers stored in columns — stay valid.
		*lazyStorePtr = lazyStore
	}

	blk := &Block{
		spanCount:       spanCount,
		columns:         columns,
		lazyColumnStore: lazyStore,
		lazyStorePtr:    lazyStorePtr,
		meta:            meta,
	}
	blk.buildNameIndex()
	// NOTE-243: iterFields is now built lazily on first IterFields() call, so the parser
	// no longer eagerly walks all columns here. Metrics queries never enumerate fields.

	return blk, nil
}

// appendLazyColumn appends the lazily-registered Column for one non-wanted TOC entry to store
// and returns the grown slice. ok=false (store unchanged) when the column should be skipped
// (offset out of range or oversized). Both snappy decompression and full column decode are
// deferred to first access (NOTE-001/NOTE-002, SPEC-V14-002). The Column is constructed in
// place via append (it contains a sync.Once and so must never be copied by value).
// blockOffset is the block's stable byte offset used to derive the deferred-decode cache key.
func appendLazyColumn(
	store []Column,
	m colMetaEntry,
	rawBytes []byte,
	spanCount int,
	fileID string,
	blockOffset uint64,
) ([]Column, bool) {
	// NOTE-220: inline column — its raw blob lives in the TOC entry (already in memory) so
	// there is nothing to defer-decompress. Register it with rawEncoding set directly;
	// ensureDecompressed becomes a no-op (compressedEncoding nil) and decodeNow decodes
	// straight from rawEncoding on first access.
	if m.inlineData != nil {
		return append(store, Column{
			Name:        m.name,
			Type:        m.colType,
			SpanCount:   spanCount,
			rawEncoding: m.inlineData, // zero-copy sub-slice of the TOC bytes
			internMap:   nil,
		}), true
	}

	start := int(m.dataOffset)          //nolint:gosec
	end := start + int(m.compressedLen) //nolint:gosec
	if start < 0 || end > len(rawBytes) {
		// NOTE-154: this loop only runs on the WantOnly path, where the buffer is usually a
		// columnar-assembled buffer (SPEC-005) containing only the ToC and the *wanted*
		// columns. A non-wanted column sitting beyond the buffer is the expected, normal
		// result of that optimization — not corruption — so this is logged at Debug, not Warn.
		// (At Warn it flooded the querier ~50k lines/h/pod and the per-skip slog formatting was
		// itself hot-path overhead.) Genuine corruption surfaces on the eager-decode path for
		// wanted columns and during header/metadata parsing.
		slog.Debug("block_parser: lazy column offset out of range — skipping",
			"column", m.name, "start", start, "end", end,
			"block_size", len(rawBytes))
		return store, false
	}

	// SPEC-ROOT-012: TOC bomb guard — reject oversized columns at registration time without
	// paying the cost of decompression. Full snappy decode is deferred to first access via
	// ensureDecompressed (SPEC-V14-002).
	if m.uncompressedLen > uint32(shared.MaxBlockSize) { //nolint:gosec
		slog.Warn("block_parser: lazy column uncompressed_len exceeds MaxBlockSize — skipping",
			"column", m.name,
			"uncompressed_len", m.uncompressedLen,
			"max_block_size", shared.MaxBlockSize)
		return store, false
	}

	// NOTE-201: precompute the process-cache key for the deferred-decode path so decodeNow can
	// consult/populate parsedV8ColumnCache on first access. Empty when no stable fileID is
	// available — decodeNow then decodes without caching.
	lazyKey := ""
	if fileID != "" {
		lazyKey = v8ColumnCacheKey(fileID, blockOffset, m.name, m.colType)
	}
	return append(store, Column{
		Name:               m.name,
		Type:               m.colType,
		SpanCount:          spanCount,
		compressedEncoding: rawBytes[start:end], // zero-copy sub-slice; decompressed on first access
		uncompressedLen:    m.uncompressedLen,
		internMap:          nil, // nil → internString skips map; safe for concurrent lazy decode
		v8CacheKey:         lazyKey,
	}), true
}

// preDecodedKey identifies one decoded column by its block's stable byte offset within
// the file plus the column's name and type. NOTE-212: Reader.preDecodedColumns is keyed
// by this so the parser can look up a reader-pre-resolved decoded snapshot without
// reconstructing the longer string parsedV8ColumnCache key.
type preDecodedKey struct {
	name        string
	blockOffset uint64
	colType     shared.ColumnType
}

// blockColTypesCacheKey builds the blockColTypesCache key for one block. The block's byte
// offset within the file is stable across queries and uniquely identifies the block, so
// (fileID, offset) keys exactly one block's name->type mapping. NOTE-214.
func blockColTypesCacheKey(fileID string, blockOffset uint64) string {
	return fileID + "/v8coltypes/" + strconv.FormatUint(blockOffset, 10)
}

// v8ColumnCacheKey builds the parsedV8ColumnCache key for one block column. The block's
// byte offset within the file is stable across queries and uniquely identifies the block,
// so (fileID, offset, name, type) keys exactly one decoded column. NOTE-200.
func v8ColumnCacheKey(fileID string, blockOffset uint64, name string, colType shared.ColumnType) string {
	return fileID + "/v8col/" +
		strconv.FormatUint(blockOffset, 10) + "/" +
		name + "/" +
		strconv.Itoa(int(colType))
}

// snapshotDecodedColumn builds an immutable cache snapshot holding only the decoded slices
// of src (which readColumnEncoding allocated fresh for this parse). The snapshot shares
// those slices; they are read-only after decode — dense expansion (NOTE-PERF-1) builds a new
// Idx on the per-query Column and never overwrites sparseDictIdx in place. NOTE-200.
func snapshotDecodedColumn(src *Column, name string, colType shared.ColumnType) *Column {
	return &Column{
		Name:          name,
		Type:          colType,
		StringDict:    src.StringDict,
		StringIdx:     src.StringIdx,
		Int64Dict:     src.Int64Dict,
		Int64Idx:      src.Int64Idx,
		Uint64Dict:    src.Uint64Dict,
		Uint64Idx:     src.Uint64Idx,
		Float64Dict:   src.Float64Dict,
		Float64Idx:    src.Float64Idx,
		BoolDict:      src.BoolDict,
		BoolIdx:       src.BoolIdx,
		BytesDict:     src.BytesDict,
		BytesIdx:      src.BytesIdx,
		BytesInline:   src.BytesInline,
		uniformSlab:   src.uniformSlab,   // NOTE-351
		uniformStride: src.uniformStride, // NOTE-351
		Present:       src.Present,
		SpanCount:     src.SpanCount,
		sparseDictIdx: src.sparseDictIdx,
		denseFlatIdx:  src.denseFlatIdx, // NOTE-358
	}
}

// copyDecodedColumnInto copies the immutable decoded slices from a cache snapshot into the
// per-query Column dst, marking it fully decoded. dst keeps its own zero-valued sync.Once /
// atomic state, so the lazy dense expansion (NOTE-PERF-1) runs independently per query and
// the shared snapshot's sparseDictIdx is never mutated. NOTE-200.
func copyDecodedColumnInto(dst, snap *Column) {
	dst.StringDict = snap.StringDict
	dst.StringIdx = snap.StringIdx
	dst.Int64Dict = snap.Int64Dict
	dst.Int64Idx = snap.Int64Idx
	dst.Uint64Dict = snap.Uint64Dict
	dst.Uint64Idx = snap.Uint64Idx
	dst.Float64Dict = snap.Float64Dict
	dst.Float64Idx = snap.Float64Idx
	dst.BoolDict = snap.BoolDict
	dst.BoolIdx = snap.BoolIdx
	dst.BytesDict = snap.BytesDict
	dst.BytesIdx = snap.BytesIdx
	dst.BytesInline = snap.BytesInline
	dst.uniformSlab = snap.uniformSlab     // NOTE-351
	dst.uniformStride = snap.uniformStride // NOTE-351
	dst.Present = snap.Present
	dst.SpanCount = snap.SpanCount
	dst.sparseDictIdx = snap.sparseDictIdx // NOTE-PERF-1: per-query col gets its own dense Idx
	dst.denseFlatIdx = snap.denseFlatIdx   // NOTE-358
	dst.decoded.Store(true)                // NOTE-CONC-001: mark eagerly decoded
}

// decompressV14ColumnData applies SPEC-ROOT-012 guards and snappy-decompresses a V14 column blob.
// Returns the decompressed bytes or an error if the TOC length, frame header, or decode fails.
// SPEC-ROOT-012: guards against decompression-bomb OOM via both TOC and snappy frame-header checks.
func decompressV14ColumnData(name string, data []byte, uncompressedLen uint32) ([]byte, error) {
	if uncompressedLen > uint32(shared.MaxBlockSize) { //nolint:gosec
		return nil, fmt.Errorf("col %q: uncompressed_len %d exceeds MaxBlockSize", name, uncompressedLen)
	}
	frameLen, lenErr := snappy.DecodedLen(data)
	if lenErr != nil {
		return nil, fmt.Errorf("col %q: snappy frame header: %w", name, lenErr)
	}
	if frameLen > shared.MaxBlockSize {
		return nil, fmt.Errorf("col %q: snappy frame claims %d bytes, exceeds MaxBlockSize", name, frameLen)
	}
	// NOTE-259: pre-sized, unzeroed dst — snappy.Decode overwrites every byte of frameLen, so
	// the memclr from snappy's internal make([]byte, frameLen) is waste. []byte is pointer-free.
	decompressed, decErr := snappy.Decode(shared.MakeNoZeroBytes(frameLen), data)
	if decErr != nil {
		return nil, fmt.Errorf("col %q snappy decode: %w", name, decErr)
	}
	if uint32(len(decompressed)) != uncompressedLen { //nolint:gosec
		return nil, fmt.Errorf(
			"col %q: decoded length %d does not match uncompressed_len %d",
			name,
			len(decompressed),
			uncompressedLen,
		)
	}
	return decompressed, nil
}

// decompressV14ColumnDataInto is like decompressV14ColumnData but decompresses into dst,
// growing it as needed. Returns the decoded slice (sub-slice of grown dst) and the grown dst.
// The caller must not use colData after dst is reused for the next column.
func decompressV14ColumnDataInto(
	dst []byte,
	name string,
	data []byte,
	uncompressedLen uint32,
) (colData []byte, grownDst []byte, err error) {
	if uncompressedLen > uint32(shared.MaxBlockSize) { //nolint:gosec
		return nil, dst, fmt.Errorf("col %q: uncompressed_len %d exceeds MaxBlockSize", name, uncompressedLen)
	}
	frameLen, lenErr := snappy.DecodedLen(data)
	if lenErr != nil {
		return nil, dst, fmt.Errorf("col %q: snappy frame header: %w", name, lenErr)
	}
	if frameLen > shared.MaxBlockSize {
		return nil, dst, fmt.Errorf("col %q: snappy frame claims %d bytes, exceeds MaxBlockSize", name, frameLen)
	}
	// Grow dst to hold the decompressed data if needed — avoids a new allocation when buf is large enough.
	if cap(dst) < frameLen {
		dst = make([]byte, 0, frameLen)
	}
	// NOTE-262: hand snappy.Decode a slice grown to its full capacity, NOT dst[:0]. snappy
	// reuses the dst backing array only when frameLen <= len(dst) — it checks len, not cap —
	// so passing dst[:0] (len 0) made it take the make([]byte, frameLen) zeroed-alloc branch
	// on every column even though dst was just grown to cap >= frameLen, defeating the
	// buffer reuse this function's "avoids a new allocation" comment promised. dst[:cap(dst)]
	// has len >= frameLen, so snappy decodes in place (no alloc, no memclr); the returned
	// slice is resliced to frameLen as before.
	decoded, decErr := snappy.Decode(dst[:cap(dst)], data)
	if decErr != nil {
		return nil, dst, fmt.Errorf("col %q snappy decode: %w", name, decErr)
	}
	if uint32(len(decoded)) != uncompressedLen { //nolint:gosec
		return nil, dst, fmt.Errorf(
			"col %q: decoded length %d does not match uncompressed_len %d",
			name, len(decoded), uncompressedLen,
		)
	}
	// Return decoded as colData and decoded[:0] as the new decompBuf.
	// If snappy reused dst's backing array, decoded[:0] retains its full capacity.
	// If snappy allocated a new buffer, decoded[:0] carries that buffer's capacity forward.
	// Either way, the next column's Decode call reuses whatever buffer was just used.
	return decoded, decoded[:0], nil
}

// resetColumn zeroes a Column's value fields while retaining the allocation.
// IMPORTANT: both decodeOnce and decoded must be reset here because Column instances are
// reused across block parses (ParseBlockFromBytes re-uses prevBlock.columns). decoded.Store(false)
// is the primary guard — needsDecode() reads decoded atomically and would skip decodeNow for
// any column that still has decoded==true from the previous block. Resetting decodeOnce alone
// is not sufficient: decoded.Load() is the outer check, decodeOnce is the inner serializer.
func resetColumn(col *Column) {
	col.StringDict = col.StringDict[:0]
	col.StringIdx = col.StringIdx[:0]
	col.Int64Dict = col.Int64Dict[:0]
	col.Int64Idx = col.Int64Idx[:0]
	col.Uint64Dict = col.Uint64Dict[:0]
	col.Uint64Idx = col.Uint64Idx[:0]
	col.Float64Dict = col.Float64Dict[:0]
	col.Float64Idx = col.Float64Idx[:0]
	col.BoolDict = col.BoolDict[:0]
	col.BoolIdx = col.BoolIdx[:0]
	col.BytesDict = col.BytesDict[:0]
	col.BytesIdx = col.BytesIdx[:0]
	col.BytesInline = nil
	col.uniformSlab = nil // NOTE-351
	col.uniformStride = 0 // NOTE-351
	col.Present = nil
	// NOTE-001: clear lazy decode fields so reused columns don't carry stale state.
	col.rawEncoding = nil
	col.compressedEncoding = nil // SPEC-V14-002: clear deferred decompression state
	col.uncompressedLen = 0
	col.internMap = nil
	col.sparseDictIdx = nil // NOTE-PERF-1: clear deferred dense expansion
	col.v8CacheKey = ""     // NOTE-201: clear stale lazy-decode cache key on reuse
	col.decodeOnce = sync.Once{}
	col.denseOnce = sync.Once{}
	col.decompressOnce = sync.Once{}
	col.decoded.Store(false) // NOTE-CONC-001: reset atomic so needsDecode() returns true for reused column
}
