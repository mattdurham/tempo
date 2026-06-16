package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/klauspost/compress/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/objectcache"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// parsedSketchCache caches fully parsed sketchIndex objects by fileID+"/sketch".
// Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedSketchCache objectcache.Cache[sketchIndex]

// parsedSketchSummaryCache caches the fully built FileSketchSummary by fileID+"/sketch-summary".
// FileSketchSummary is expensive to build (TopK aggregation across all blocks) and
// was previously rebuilt on every query because it was only cached per-Reader (short-lived).
// Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedSketchSummaryCache objectcache.Cache[FileSketchSummary]

// parsedIntrinsicCache caches fully decoded IntrinsicColumn objects by
// fileID+"/intrinsic/"+colName. Strong references: entries persist until Clear is called.
// SPEC-OC-003, NOTE-003 (reader NOTES.md)
var parsedIntrinsicCache objectcache.Cache[shared.IntrinsicColumn]

// parsedV8ColumnCache caches fully decoded V8 block Column snapshots by
// fileID+"/v8col/"+blockOffset+"/"+colName+"/"+colType. Strong references: entries
// persist until Clear is called. NOTE-200: a Reader is created fresh per query (per
// block per querier call), so the per-block decode of wanted V8 columns — snappy
// decompress + readColumnEncoding (dict/idx build, radix sort) — was rerun on every
// warm query even when a prior query already decoded the same column from the same
// on-disk block. This process-level cache holds the immutable decoded slices so the
// warm path copies them into the per-query Column instead of re-decoding.
// SPEC-OC-003, NOTE-200 (reader NOTES.md)
var parsedV8ColumnCache objectcache.Cache[Column]

// parsedTraceChunkCache caches the decompressed mini-body of a single chunked-trace-index
// chunk (issue #350: the per-chunk decoded-unit cache that completes the #341 chunk-caching
// follow-up). Keyed by fileID + "/tracechunk/" + sectionOffset + "/" + chunkIdx.
//
// NOTE-404: the chunkedTraceIndex (r.chunkedTrace) is per-Reader and a Reader is constructed
// fresh per query (per block, per querier call), so its in-struct chunkCache (NOTE-292) only
// memoizes within ONE query. A warm file therefore re-fetched + re-snappy-decoded the same
// chunk on every repeat trace-by-id lookup. This process-level cache holds the decoded
// mini-body across queries keyed by (fileID, section offset, chunkIdx) so a warm-file repeat
// lookup that resolves to the same chunk becomes a cache hit instead of a provider round-trip
// + decode. Cache-safe: readRangeDecodeSnappy returns a freshly allocated caller-owned buffer
// (the snappy.Decode dst that escapes), never a pooled/shared one. Strong references: entries
// persist until Clear is called.
// SPEC-OC-003, NOTE-404 (reader NOTES.md)
var parsedTraceChunkCache objectcache.Cache[traceChunk]

// traceChunk wraps a decoded chunked-trace-index mini-body so it can be byte-budgeted by the
// objectcache LRU. The body is immutable once decoded.
type traceChunk struct {
	body []byte
}

// SizeBytes reports the chunk's retained footprint for objectcache LRU budgeting.
func (c *traceChunk) SizeBytes() int64 {
	return int64(len(c.body)) + 16 // body + slice header overhead
}

// blockColTypesCache caches the per-block parsed ToC (column-metadata array + tocEnd) by
// fileID+"/v8coltypes/"+blockOffset. NOTE-214: the combined ToC+columns GetMulti
// (NOTE-185) requests the compressed blob of EVERY wanted column, but on the warm path
// many of those columns already have a decoded snapshot in parsedV8ColumnCache and the
// fetched blob is discarded (NOTE-212/213 skip the copy). The blob can only be probed
// against parsedV8ColumnCache by its (name, type) key, and the type is not known until
// the ToC is decoded — which happens AFTER the GetMulti. Caching the parsed ToC at first
// parse lets the warm path probe parsedV8ColumnCache BEFORE building the GetMulti and drop
// already-decoded columns from the request, cutting the wasted memcache Get traffic (the
// MemCache.Get CPU sink the standing target points at).
//
// NOTE-241: the cache now retains the full parsed []colMetaEntry + tocEnd, not just the
// name->type mapping. parseColumnMetadataArray ran on EVERY warm block read — re-decoding
// hundreds of column-metadata entries from the ToC bytes and allocating one string(name)
// per column plus the entries slice, all of which are deterministic for a given block. On
// the fully-warm wide path the parse result is consumed only by the NOTE-213 sizing loop
// (which finds every column pre-decoded) and then discarded, so the parse is pure
// per-query allocation/CPU. Caching the parsed metas lets a warm read skip
// parseBlockHeader + parseColumnMetadataArray entirely and reuse the shared (read-only)
// slice. Inline columns' inlineData sub-slices the transient ToC buffer, so they are
// deep-copied into the cached entry to avoid aliasing memcache-owned bytes.
// SPEC-OC-003, NOTE-214/241 (reader NOTES.md)
var blockColTypesCache objectcache.Cache[blockColTypes]

// parsedTraceSparseCache caches the sparse trace-index offset index (the NOTE-260
// traceIdxSamples slice) by fileID+"/tracesparse/"+len(traceIndexRaw). NOTE-265: a Reader
// (and thus its compactTraceIndex) is created fresh per query, so the O(traceCount) walk
// that builds traceIdxSamples — calling traceEntryStride on every entry — was rerun on
// every bloom-hit lookup against the same on-disk trace-index section. The 2026-06-09/13
// querier profiles showed traceEntryStride at ~1.77% and the ensureTraceIdxSamples build
// closure at ~0.99% self-time, all of it this repeated rebuild. The samples reference only
// byte OFFSETS into traceIndexRaw plus a copied [16]byte trace ID (no aliasing of the raw
// bytes), and the section's layout is fully determined by its length, so the built index
// is safe to share across Readers for the same fileID+length. Strong references: entries
// persist until Clear is called.
// SPEC-OC-003, NOTE-265 (reader NOTES.md)
var parsedTraceSparseCache objectcache.Cache[traceSparseIndex]

// traceSparseIndex wraps the cached sparse offset index so it has a stable pointer identity
// for objectcache.Cache. ok records whether the build succeeded; a build that hit a
// malformed entry caches ok=false so the lookup falls back to a full linear scan without
// re-attempting (and re-failing) the walk on every query.
type traceSparseIndex struct {
	offsets []int32
	// NOTE-289: sampleIDs holds each sample's 16-byte trace ID as a big-endian uint64
	// pair (hi at 2k, lo at 2k+1), parallel to offsets[k]. Captured for free during the
	// build walk so the warm-path binary search probes this cache-resident slice instead
	// of chasing random offsets into the cold, per-query-fetched traceIndexRaw section.
	sampleIDs []uint64
	ok        bool
}

// SizeBytes estimates the in-memory size of the sparse index for objectcache LRU budgeting.
func (t *traceSparseIndex) SizeBytes() int64 {
	// Each sample is one int32 byte offset (4 bytes) + two uint64 trace-ID halves
	// (16 bytes) = 20 bytes (NOTE-279/289: one sample per traceIdxSampleStride entries,
	// so this is still ~stride× smaller than a dense per-entry index).
	return int64(len(t.offsets))*4 + int64(len(t.sampleIDs))*8 + 16
}

// blockColTypes holds a block's fully-parsed ToC: the column-metadata array (in wire order)
// and the byte offset one past the last metadata entry (tocEnd). The cached metas slice is
// treated as READ-ONLY by all readers — it is shared across queries, so no consumer may
// mutate an entry or the slice. One block can carry the same name with multiple types; the
// per-name type lookup used by NOTE-214 pruning is derived from metas on demand (typesFor).
// Wrapped in a named struct for stable pointer identity in objectcache.Cache.
type blockColTypes struct {
	metas  []colMetaEntry
	tocEnd int
}

// typesFor returns the column type(s) present under name across this block's ToC. One block
// can carry the same name with different types (rare), so the result may hold several types.
// Allocation-free in the common single-type case (returns a one-element view via the small
// fixed-size buffer); callers must not retain the returned slice past the next typesFor call
// on the same buffer.
func (b *blockColTypes) typesFor(name string, buf *[4]shared.ColumnType) []shared.ColumnType {
	n := 0
	for i := range b.metas {
		if b.metas[i].name != name {
			continue
		}
		if n < len(buf) {
			buf[n] = b.metas[i].colType
		}
		n++
	}
	if n == 0 {
		return nil
	}
	if n > len(buf) {
		n = len(buf) // defensive cap; a name with >4 types is not expected
	}
	return buf[:n]
}

// SizeBytes estimates the in-memory size of the parsed ToC for objectcache LRU budgeting.
func (b *blockColTypes) SizeBytes() int64 {
	n := int64(0)
	for i := range b.metas {
		n += int64(
			len(b.metas[i].name),
		) + int64(
			len(b.metas[i].inlineData),
		) + 56 // name backing + inline bytes backing + sizeof(colMetaEntry)
		// NOTE-362: the struct overhead was previously hard-coded at 48, but
		// sizeof(colMetaEntry) is 56 bytes after narrowing dataOffset to uint32 (it was 64
		// before). Account for the true struct size so the LRU budget does not under-count
		// the retained entries and silently hold more than its configured limit.
	}
	return n + 32
}

// SetIntrinsicCacheBytes sets the byte budget for the process-level intrinsic column
// cache. Must be called before the first GetIntrinsicColumn call.
// Pass 0 to revert to the default (20% of GOMEMLIMIT, or 256 MiB fallback).
//
// Callers should set a budget appropriate for the process role:
//   - Queriers: 256-512 MiB (decoded intrinsic columns are short-lived per query)
//   - Backend workers: default (compaction benefits from larger cache)
func SetIntrinsicCacheBytes(n int64) {
	// NOTE-371: the intrinsic and V8-column caches no longer share the same byte
	// budget. NOTE-344 fixed a SizeBytes() bug that let the intrinsic cache balloon
	// to ~9 GiB under a nominal 512 MiB budget; that it ran for so long without
	// query failures showed most of what it held was never re-read before eviction
	// would have happened anyway (the compressed bytes are always available from
	// memcached, so a miss costs only a re-decode, not an extra round trip). The V8
	// column cache (NOTE-200) is a separately-proven win on the warm columnar path,
	// so it keeps the full budget while the intrinsic cache gets a smaller dedicated
	// share. Splitting the lever lets the intrinsic footprint shrink without
	// touching the V8 cache's hit rate. n <= 0 is passed through unchanged so the
	// objectcache default (20% of GOMEMLIMIT) still applies to every cache.
	intrinsicBytes := n
	if n > 0 {
		intrinsicBytes = n / 2
	}
	parsedIntrinsicCache.SetMaxBytes(intrinsicBytes)
	// parsedIntrinsicTOCCache removed 2026-06-12 (legacy V4/V5/V6 format)
	parsedV8ColumnCache.SetMaxBytes(n)         // NOTE-200: proven warm-path win, full budget
	blockColTypesCache.SetMaxBytes(n / 16)     // NOTE-214: name->type maps are tiny vs decoded columns
	parsedTraceSparseCache.SetMaxBytes(n / 16) // NOTE-265: sparse trace-index samples are tiny
	parsedTraceChunkCache.SetMaxBytes(n / 16)  // NOTE-404: one decoded chunk per repeat lookup, small share
}

// ClearCaches resets all process-level caches. Intended for testing.
func ClearCaches() {
	parsedSketchCache.Clear()
	parsedSketchSummaryCache.Clear()
	parsedIntrinsicCache.Clear()

	parsedV8ColumnCache.Clear()
	blockColTypesCache.Clear()
	parsedTraceSparseCache.Clear()
	parsedTraceChunkCache.Clear()
}

// rangeIndexMeta records the byte range within metadataBytes for a
// range column index entry (lazy parsing).

// readFooter reads the footer from the end of the file.
// For 18-byte magic footers: V8 only — rejects any other version (including V7) with an error.
// Legacy v3 (22 bytes), v4 (34 bytes), v5 (46 bytes), and v6 (58 bytes) are handled
// via the legacy path when the 18-byte magic check yields no match.
//
// Detection strategy: read the last 18 bytes once; if magic matches, version must be
// readFooter reads and validates the V8 footer (the only supported format).
// Legacy formats (V3–V6) were removed 2026-06-12; all blocks must be V8 or later.
func (r *Reader) readFooter() error {
	if r.fileSize < int64(shared.FooterV8Size) {
		return fmt.Errorf("file too small for footer: %d bytes", r.fileSize)
	}
	ok, err := r.tryReadFooterMagic18()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf(
			"blockpack: unsupported or corrupt file — only FooterV8 files are supported (legacy V3–V6 formats were removed 2026-06-12; re-compact any legacy blocks before reading)",
		)
	}
	return nil
}

// tryReadFooterMagic18 reads the last 18 bytes and checks for a V8 footer.
// If magic matches but version != V8, an error is returned (V7 is not supported).
// Returns (false, nil) when magic is absent, allowing legacy detection to proceed.
func (r *Reader) tryReadFooterMagic18() (bool, error) {
	off := r.fileSize - int64(shared.FooterV8Size) // 18-byte footer
	buf, err := r.cache.GetOrFetchFooter(r.fileID, "/v78", func() ([]byte, error) {
		b := make([]byte, shared.FooterV8Size)
		n, readErr := r.provider.ReadAt(b, off, rw.DataTypeFooter)
		if readErr != nil {
			return nil, fmt.Errorf("readFooter: %w", readErr)
		}
		if n != int(shared.FooterV8Size) {
			return nil, fmt.Errorf("readFooter: short read: %d bytes", n)
		}
		return b, nil
	})
	if err != nil {
		return false, fmt.Errorf("readFooter: %w", err)
	}
	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != shared.MagicNumber {
		return false, nil
	}
	ver := binary.LittleEndian.Uint16(buf[footerV7OffVersion:])
	if ver != shared.FooterV8Version {
		return false, fmt.Errorf("readFooter: unsupported footer version %d", ver)
	}
	// footerVersion removed 2026-06-12: always V8
	r.v8ToCOffset = binary.LittleEndian.Uint64(buf[footerV7OffDirOff:])
	r.v8ToCLen = binary.LittleEndian.Uint32(buf[footerV7OffDirLen:])
	return true, nil
}

// V7/V8 footer field offsets.
// Wire format: magic[4] · version[2] · dir_offset[8] · dir_len[4] = 18 bytes.
const (
	footerV7OffVersion = 4  // uint16 version field within 18-byte V7/V8 footer
	footerV7OffDirOff  = 6  // uint64 dir_offset field
	footerV7OffDirLen  = 14 // uint32 dir_len field
)

// decodeBoundedSnappy snappy-decodes compressed, rejecting inputs whose
// decoded size would exceed MaxMetadataSize (decompression-bomb guard).
// NOTE-366: pooled scratch for the read-compressed-then-snappy-decode-then-discard pattern.
//
// readV14Section / parseV8ToCBlob / fetchToCSection / ensureV8TraceSection / chunkBytes /
// ensureTraceIndexRaw all do the same thing: r.readRange(...) allocates a fresh
// make([]byte, length) (NOTE: readRange), the bytes are fed straight to decodeBoundedSnappy,
// and the compressed buffer is then unreferenced. The decoded output escapes (to r.cache or
// a parsed struct), but the *compressed* buffer is pure transient scratch. Under heavy
// concurrent metadata loading these transient buffers piled up as live bytes attributed to
// readRange (the #3 inuse_space self-frame at ~188 MB warm). Routing them through a pool
// keyed only by the 8 MiB cap (mirroring decompBufPool / NOTE-346) keeps the steady-state
// scratch footprint bounded instead of one-shot-allocating each compressed blob.
var rangeScratchPool = sync.Pool{New: func() any { b := make([]byte, 0, 64<<10); return &b }}

// rangeScratchMaxPooledCap bounds the backing capacity any buffer may carry back into
// rangeScratchPool, so a rare large compressed section (e.g. a tens-of-MB legacy V8 trace
// index) does not pin a giant array in the pool for the process lifetime. Mirrors
// decompBufMaxPooledCap (NOTE-346). Metadata/section compressed blobs are typically KB to a
// few MB, well under this cap, so the common case recycles its buffer with no churn.
const rangeScratchMaxPooledCap = 8 << 20 // 8 MiB

// putRangeScratch returns a read-scratch buffer to rangeScratchPool, dropping any buffer
// grown beyond rangeScratchMaxPooledCap. ptr must be non-nil.
func putRangeScratch(ptr *[]byte) {
	if cap(*ptr) > rangeScratchMaxPooledCap {
		return // drop oversized buffer: let it GC rather than pin RSS in the pool
	}
	rangeScratchPool.Put(ptr)
}

// readRangeDecodeSnappy reads [offset, offset+length) into a pooled scratch buffer, snappy-
// decodes it, and returns the freshly-allocated decoded bytes. The pooled scratch (the
// compressed source) is returned to rangeScratchPool before return; the decoded output is
// independently allocated by decodeBoundedSnappy and does NOT alias the scratch, so the
// buffer is safe to recycle (NOTE-366). Returns (nil, nil) for length == 0.
func (r *Reader) readRangeDecodeSnappy(offset, length uint64, dt rw.DataType) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	scratchPtr := rangeScratchPool.Get().(*[]byte)
	scratch := *scratchPtr
	if uint64(cap(scratch)) < length { //nolint:gosec // length is a section size, bounded by MaxMetadataSize
		scratch = make([]byte, length)
	} else {
		scratch = scratch[:length]
	}
	defer func() {
		*scratchPtr = scratch[:0]
		putRangeScratch(scratchPtr)
	}()
	off := int64(offset) //nolint:gosec // safe: offset is a file offset, fits in int64
	n, err := r.provider.ReadAt(scratch, off, dt)
	if err != nil {
		return nil, fmt.Errorf("readRangeDecodeSnappy offset=%d length=%d: %w", offset, length, err)
	}
	if uint64(n) != length { //nolint:gosec // safe: n is bytes read, always non-negative
		return nil, fmt.Errorf("readRangeDecodeSnappy offset=%d: short read %d/%d", offset, n, length)
	}
	return decodeBoundedSnappy(scratch)
}

func decodeBoundedSnappy(compressed []byte) ([]byte, error) {
	decodedLen, lenErr := snappy.DecodedLen(compressed)
	if lenErr != nil {
		return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
	}
	if uint64(decodedLen) > shared.MaxMetadataSize { //nolint:gosec // safe: decodedLen is non-negative
		return nil, fmt.Errorf("snappy decoded size %d exceeds MaxMetadataSize %d", decodedLen, shared.MaxMetadataSize)
	}
	// NOTE-259: pass a pre-sized, unzeroed dst of exactly decodedLen. snappy.Decode reslices
	// dst to [:decodedLen] (since decodedLen <= len(dst)) and overwrites every byte, so the
	// memclr that snappy.Decode(nil, …)'s internal make([]byte, dLen) emits is pure waste.
	// The decoded bytes escape to r.cache so the buffer can't be pooled, but []byte is
	// pointer-free, so the unzeroed backing array is GC-safe before the full overwrite.
	return snappy.Decode(shared.MakeNoZeroBytes(decodedLen), compressed)
}

// readV14Section reads and snappy-decodes one type-keyed section from the section directory.
// Returns (nil, nil) if the section is not present in the directory.
// Decompressed bytes are cached via r.cache (key: fileID+"/v14/sec/<hex>/dec") so repeated
// reader creation for the same file avoids re-reading and re-decompressing the section.
func (r *Reader) readV14Section(sectionType uint8) ([]byte, error) {
	e, ok := r.sectionDir.TypeEntries[sectionType]
	if !ok {
		return nil, nil
	}
	raw, err := r.cache.GetOrFetchV14Section(r.fileID, sectionType, func() ([]byte, error) {
		// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
		dec, decErr := r.readRangeDecodeSnappy(e.Offset, uint64(e.CompressedLen), rw.DataTypeMetadata) //nolint:gosec
		if decErr != nil {
			return nil, fmt.Errorf("section 0x%02X read/snappy: %w", sectionType, decErr)
		}
		return dec, nil
	})
	return raw, err
}

// parseV8ToCBlob reads, decompresses, and parses the V8 unified ToC blob.
// Returns a map of ToCKey → ToCEntry and the file's signal type.
func (r *Reader) parseV8ToCBlob() (map[shared.ToCKey]shared.ToCEntry, uint8, error) {
	if r.v8ToCLen == 0 {
		return make(map[shared.ToCKey]shared.ToCEntry), shared.SignalTypeTrace, nil
	}
	raw, err := r.cache.GetOrFetchV8TOC(r.fileID, func() ([]byte, error) {
		// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
		return r.readRangeDecodeSnappy(r.v8ToCOffset, uint64(r.v8ToCLen), rw.DataTypeMetadata) //nolint:gosec
	})
	if err != nil {
		return nil, 0, fmt.Errorf("parseV8ToCBlob: %w", err)
	}

	if len(raw) < shared.ToCBlobHeaderSize {
		return nil, 0, fmt.Errorf("parseV8ToCBlob: blob too short: %d bytes", len(raw))
	}
	entryCount := binary.LittleEndian.Uint32(raw[0:])
	signalType := raw[4] // signal_type[1]; reserved[3] at raw[5:8]
	if signalType == 0 {
		signalType = shared.SignalTypeTrace
	}
	pos := shared.ToCBlobHeaderSize

	tocMap := make(map[shared.ToCKey]shared.ToCEntry, int(entryCount)) //nolint:gosec
	for i := range entryCount {
		e, n, parseErr := shared.UnmarshalToCEntry(raw[pos:])
		if parseErr != nil {
			return nil, 0, fmt.Errorf("parseV8ToCBlob: entry[%d]: %w", i, parseErr)
		}
		pos += n
		tocMap[e.Key] = e
	}
	return tocMap, signalType, nil
}

// fetchToCSection reads and snappy-decodes the section identified by key from the V8 ToC.
// Returns (nil, nil) when key is absent from r.tocMap (graceful degradation).
// Results are cached via r.cache.
func (r *Reader) fetchToCSection(key shared.ToCKey) ([]byte, error) {
	e, ok := r.tocMap[key]
	if !ok {
		return nil, nil
	}
	raw, err := r.cache.GetOrFetchV8Section(r.fileID, key.Type, key.SubType, key.Name, func() ([]byte, error) {
		// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
		dec, decErr := r.readRangeDecodeSnappy(e.Offset, uint64(e.Length), rw.DataTypeMetadata) //nolint:gosec
		if decErr != nil {
			return nil, fmt.Errorf("fetchToCSection(%v): read/snappy: %w", key, decErr)
		}
		return dec, nil
	})
	return raw, err
}

// parseSectionsV8 initializes the V8 reader by:
// 1. Parsing the ToC blob to build r.tocMap.
// 2. Eagerly loading the block index (required by all block-access methods).
// 3. Populating r.intrinsicIndex from intrinsic ToCEntries (zero I/O; offsets only).
func (r *Reader) parseSectionsV8() error {
	tocMap, signalType, err := r.parseV8ToCBlob()
	if err != nil {
		return fmt.Errorf("parseSectionsV8: ToC: %w", err)
	}
	r.tocMap = tocMap
	r.signalType = signalType
	r.fileVersion = shared.VersionBlockV14 // block format is still V14; reuse block parser

	// Block index: eager (required by all block-access methods).
	blockIdxRaw, err := r.fetchToCSection(shared.ToCKey{
		Type:    shared.ToCTypeIndex,
		SubType: shared.ToCSubTypeBlockIndex,
	})
	if err != nil {
		return fmt.Errorf("parseSectionsV8: block_index: %w", err)
	}
	if len(blockIdxRaw) >= 4 {
		blockCount := int(binary.LittleEndian.Uint32(blockIdxRaw[0:]))
		metas, _, parseErr := parseBlockIndex(blockIdxRaw[4:], blockCount)
		if parseErr != nil {
			return fmt.Errorf("parseSectionsV8: block_index parse: %w", parseErr)
		}
		r.blockMetas = metas
	}

	// Intrinsic index: zero I/O — record offsets from ToC.
	for key, e := range r.tocMap {
		if key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeIntrinsic {
			if r.intrinsicIndex == nil {
				r.intrinsicIndex = make(map[string]shared.IntrinsicColMeta)
			}
			r.intrinsicIndex[key.Name] = shared.IntrinsicColMeta{
				Name:   key.Name,
				Offset: e.Offset,
				Length: e.Length,
			}
		}
	}

	return nil
}

// ensureV8TraceSection lazily loads the V8 compact trace index on first call.
func (r *Reader) ensureV8TraceSection() error {
	r.v8TraceOnce.Do(func() {
		// Issue #340: prefer the range-readable chunked trace index when present. Only the
		// fixed header + directory are read here (range reads); chunk bodies are fetched
		// lazily per lookup. Falls back to the legacy compact section below when absent.
		if entry, ok := r.tocMap[shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeTraceChunked}]; ok {
			ci, perr := r.parseChunkedTraceIndex(entry)
			if perr != nil {
				r.v8TraceErr = fmt.Errorf("ensureV8TraceSection: chunked: %w", perr)
				return
			}
			r.chunkedTrace = ci
			return
		}
		// NOTE-349: TRUE two-phase loading for the legacy V8 snappy trace section.
		// Phase 1 here parses ONLY the header (bloom filter + block table) and records
		// the compressed section's file location; it does NOT retain the decompressed
		// trace-index bytes. Phase 2 (ensureTraceIndexRaw) re-reads + re-splits the
		// section lazily on a bloom HIT — the vast majority of trace lookups reject at
		// the bloom and never need the tens-of-MB trace-index body.
		//
		// Before NOTE-349 this method eagerly aliased the trace-index sub-slice into
		// r.compactParsed.traceIndexRaw (NOTE-257), pinning the entire decompressed
		// section in every live Reader. Under concurrent trace-lookup load that was the
		// single largest retained object on the querier heap (~1.9 GB in the inuse_space
		// profile, frame fetchToCSection<-ensureV8TraceSection). Deferring the body fetch
		// removes that retention; the bloom + block table (the only fields phase-1
		// callers — MayContainTraceID, BlocksForTraceID bloom-reject, TraceCount — need)
		// are deep-copied out of the transient blob by parseCompactIndexBytesV14Header.
		entry, ok := r.tocMap[shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeTrace}]
		if !ok {
			return
		}
		// NOTE-366: read into pooled scratch, decode, recycle the compressed buffer.
		raw, decErr := r.readRangeDecodeSnappy(entry.Offset, uint64(entry.Length), rw.DataTypeMetadata) //nolint:gosec
		if decErr != nil {
			r.v8TraceErr = fmt.Errorf("ensureV8TraceSection: read/snappy: %w", decErr)
			return
		}
		if len(raw) == 0 {
			return
		}
		header, _, splitErr := splitV14CompactSection(raw)
		if splitErr != nil {
			r.v8TraceErr = fmt.Errorf("ensureV8TraceSection: split: %w", splitErr)
			return
		}
		if parseErr := r.parseCompactIndexBytesV14Header(header); parseErr != nil {
			r.v8TraceErr = fmt.Errorf("ensureV8TraceSection: parse: %w", parseErr)
			return
		}
		// Record the section location so ensureTraceIndexRaw can re-fetch the body
		// on a bloom hit. The transient `raw` blob is now unreferenced and becomes
		// collectable as soon as this closure returns; the compressed source was read
		// into pooled scratch and already recycled by readRangeDecodeSnappy (NOTE-366).
		if r.compactParsed != nil {
			r.compactParsed.v8SectionOffset = entry.Offset
			r.compactParsed.v8SectionLen = uint64(entry.Length) //nolint:gosec
			r.compactParsed.isV8SnappyTraceIndex = true
		}
	})
	return r.v8TraceErr
}

// ensureV8TSSection lazily loads the V8 timestamp index on first call.
func (r *Reader) ensureV8TSSection() error {
	r.v8TSOnce.Do(func() {
		raw, err := r.fetchToCSection(shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeTS})
		if err != nil {
			r.v8TSErr = fmt.Errorf("ensureV8TSSection: %w", err)
			return
		}
		if len(raw) == 0 {
			return
		}
		rawEntries, tsCount, _, tsErr := parseTSIndex(raw)
		if tsErr != nil {
			r.v8TSErr = fmt.Errorf("ensureV8TSSection: parse: %w", tsErr)
			return
		}
		r.tsRaw = rawEntries
		r.tsCount = tsCount
	})
	return r.v8TSErr
}

// ensureV8BloomSection lazily loads the V8 file bloom filter on first call.
func (r *Reader) ensureV8BloomSection() error {
	r.v8BloomOnce.Do(func() {
		raw, err := r.fetchToCSection(shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeBloom})
		if err != nil {
			r.v8BloomErr = fmt.Errorf("ensureV8BloomSection: %w", err)
			return
		}
		if len(raw) == 0 {
			return
		}
		fb, _, fbErr := parseFileBloomSection(raw)
		if fbErr != nil {
			r.v8BloomErr = fmt.Errorf("ensureV8BloomSection: parse: %w", fbErr)
			return
		}
		if fb != nil {
			r.fileBloomRaw = raw
			r.fileBloomParsed = fb
		}
	})
	return r.v8BloomErr
}

// ensureV14RangeSection lazily loads the V14 range index section on first call.
// Populates r.rangeOffsets and r.metadataBytes (which ensureRangeColumnParsed indexes into).
// ensureV14TraceSection lazily loads the V14 trace index section on first call.
// Populates r.compactParsed so TraceEntries, BlocksForTraceID, and TraceCount work.
// No-op for non-V14 files (compactParsed is populated by ensureCompactIndexParsed).
//
// Two-phase loading: phase 1 reads only the bloom filter + block table (small header, ~KB)
// so most FindTraceByID lookups pay only that cost. Phase 2 (the full ~50 MB trace index)
// is deferred to ensureTraceIndexRaw, which is invoked only on a bloom hit.
//
// Cache keys:
//   - Phase 1 header: fileID+"/v14/compact-header"
//   - Phase 2 trace index bytes: fileID+"/compact-trace-index" (shared with V3/V4 lean path)
func (r *Reader) ensureV14TraceSection() error {
	return r.ensureV8TraceSection()
}

// ensureV14TSSection lazily loads the V14 timestamp index section on first call.
// Populates r.tsRaw and r.tsCount so BlocksInTimeRange works.
// No-op for non-V14 files (tsRaw/tsCount are populated by parseV5MetadataLazy).
func (r *Reader) ensureV14TSSection() error {
	return r.ensureV8TSSection()
}

// ensureV14SketchSection lazily loads the V14 sketch index section on first call.
// Populates r.sketchIdx so ColumnSketch and FileSketchSummary work.
// No-op for non-V14 files (sketchIdx is populated by parseV5MetadataLazy).
func (r *Reader) ensureV14SketchSection() error {
	// V8: per-column sketch blobs handled directly in ColumnSketch.
	return nil
}

// ensureV14BloomSection lazily loads the V14 file bloom section on first call.
// Populates r.fileBloomRaw and r.fileBloomParsed so FileBloom and FileBloomRaw work.
// No-op for non-V14 files (fileBloomRaw/fileBloomParsed are populated by parseV5MetadataLazy).
func (r *Reader) ensureV14BloomSection() error {
	return r.ensureV8BloomSection()
}

// parseV5MetadataLazy reads the metadata section and eagerly parses:
//   - block index entries → r.blockMetas
//   - range column index byte ranges → r.rangeOffsets (lazy)
//   - trace block index → r.traceIndex
//
// parseV5MetadataLazy was the V3/V4/V5/V6 metadata parser.
// Removed 2026-06-12 when legacy footer support was dropped.
// The function body is gone; this stub preserves the call site in reader.go during transition.
// parseAndCacheSketchSection parses the sketch index section from data, caches the result,
// and returns the number of bytes consumed. Returns (0, nil) when no sketch section is present.
func parseBlockIndexEntry(data []byte, pos int) (shared.BlockMeta, int, error) {
	var meta shared.BlockMeta

	// offset[8] + length[8]
	if pos+16 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for offset/length")
	}

	meta.Offset = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.Length = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// kind[1]
	if pos+1 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for kind")
	}

	meta.Kind = shared.BlockKind(data[pos])
	pos++

	// span_count[4] + min_start[8] + max_start[8]
	if pos+20 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for span_count/timestamps")
	}

	meta.SpanCount = binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	meta.MinStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.MaxStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// V13: MinTraceID/MaxTraceID omitted from block index entries.

	return meta, pos, nil
}

// parseBlockIndex parses block_count block index entries from data.
func parseBlockIndex(data []byte, blockCount int) ([]shared.BlockMeta, int, error) {
	metas := make([]shared.BlockMeta, 0, blockCount)
	pos := 0

	for i := range blockCount {
		meta, newPos, err := parseBlockIndexEntry(data, pos)
		if err != nil {
			return nil, pos, fmt.Errorf("block[%d]: %w", i, err)
		}

		metas = append(metas, meta)
		pos = newPos
	}

	return metas, pos, nil
}

// parseTraceBlockIndex parses the trace block index section.
// Supports fmt_version 0x01 (v1: with per-block span indices, discarded on read)
// and fmt_version 0x02 (v2: block IDs only).
// Returns the parsed map and bytes consumed.
func parseTraceBlockIndex(data []byte) (map[[16]byte][]uint16, int, error) {
	if len(data) < 5 {
		return nil, 0, nil
	}

	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion && fmtVersion != shared.TraceIndexFmtVersion2 {
		return nil, 0, fmt.Errorf("trace_index: unsupported fmt_version %d", fmtVersion)
	}

	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5

	result := make(map[[16]byte][]uint16, traceCount)

	for t := range traceCount {
		if pos+18 > len(data) {
			return nil, pos, fmt.Errorf("trace_index: trace[%d]: short for trace_id+block_count", t)
		}

		var tid [16]byte
		copy(tid[:], data[pos:pos+16])
		pos += 16

		blockRefCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		blockIDs := make([]uint16, 0, blockRefCount)

		if fmtVersion == shared.TraceIndexFmtVersion {
			// v1: block_id[2] + span_count[2] + span_indices[N×2] — discard span indices.
			for b := range blockRefCount {
				if pos+4 > len(data) {
					return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id+span_count", t, b)
				}
				blockID := binary.LittleEndian.Uint16(data[pos:])
				pos += 2
				spanCount := int(binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
				if pos+spanCount*2 > len(data) {
					return nil, pos, fmt.Errorf(
						"trace_index: trace[%d] block[%d]: short for span_indices (%d × 2 bytes)",
						t, b, spanCount,
					)
				}
				pos += spanCount * 2
				blockIDs = append(blockIDs, blockID)
			}
		} else {
			// v2: block_id[2] only.
			for b := range blockRefCount {
				if pos+2 > len(data) {
					return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id", t, b)
				}
				blockIDs = append(blockIDs, binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
			}
		}

		result[tid] = blockIDs
	}

	return result, pos, nil
}

// skipTraceBlockIndex advances past a trace block index section without building
// the map. This avoids O(N) allocations for search queries that never use the index.
func (r *Reader) readRange(offset, length uint64, dt rw.DataType) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	n, err := r.provider.ReadAt(buf, int64(offset), dt) //nolint:gosec // safe: offset is a file offset, fits in int64
	if err != nil {
		return nil, fmt.Errorf("readRange offset=%d length=%d: %w", offset, length, err)
	}

	if uint64(n) != length { //nolint:gosec // safe: n is bytes read, always non-negative
		return nil, fmt.Errorf("readRange offset=%d: short read %d/%d", offset, n, length)
	}

	return buf, nil
}
