package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/klauspost/compress/zstd"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// internMapPool holds reusable string intern maps for block parsing.
// Each map is cleared before return to prevent cross-block string retention.
//
// NOTE-006: The pool eliminates per-call make(map[string]string) allocations.
// Maps must be acquired at the scanBlocks level (not inside ParseBlockFromBytes) because
// lazy columns store an internMap reference that outlives the ParseBlockFromBytes call —
// decodeNow() may be called during the row loop after ParseBlockFromBytes returns.
// The caller (scanBlocks) must hold the pooled map alive until after streamSortedRows
// completes (all lazy decodes done), then release it.
//
// Strings interned during parsing escape into Column.StringDict entries (heap-allocated),
// so clearing the map does not corrupt previously interned string data.
var internMapPool = &sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		m := make(map[string]string, 64)
		return &m
	},
}

// internMapMaxPooledEntries caps the entry count a pooled intern map may have grown to
// before ReleaseInternMap will return it to the pool (NOTE-368). Go's map never shrinks its
// bucket array on clear() — a map that grew to N entries for one high-cardinality block keeps
// that ~N-bucket backing array for the lifetime of the pooled handle, even though clear()
// empties it. The querier scans one block per goroutine through this pool; a single wide
// high-cardinality block (e.g. a unique-string attribute column) inflates a map to tens of
// thousands of buckets, then that bloated handle is reused for every subsequent (typically
// small) block, pinning the large array permanently. internString was a top querier
// inuse_space self-frame (~332 MB) — partly the interned strings (which legitimately escape
// to StringDict) but also these never-reclaimed grown bucket arrays. Dropping an oversized
// map lets the big bucket array be GC'd; the pool re-seeds a fresh 64-entry map on next Get.
const internMapMaxPooledEntries = 4096

// AcquireInternMap returns a pooled intern map, cleared and ready for use.
func AcquireInternMap() *map[string]string {
	return internMapPool.Get().(*map[string]string)
}

// ReleaseInternMap clears the map and returns it to the pool, UNLESS it grew past
// internMapMaxPooledEntries (NOTE-368) — an oversized map's bucket array is dropped (left for
// GC) rather than pooled, so a single high-cardinality block can't pin a large map indefinitely.
func ReleaseInternMap(mp *map[string]string) {
	if len(*mp) > internMapMaxPooledEntries {
		// Drop the bloated backing array; do not return it to the pool. The next Get
		// allocates a fresh 64-entry map.
		return
	}
	clear(*mp)
	internMapPool.Put(mp)
}

// presentRowsScratchPool holds reusable []int scratch slices for collectPresentRowsInto.
// Each call to decodeXORBytes / decodePrefixBytes acquires one slice, resets it to [:0],
// appends present-row indices, then releases it back after the per-row decode loop.
//
// Pool cap guard constants for presentRowsScratchPool.
// NOTE-007: initial pool capacity and cap-guard threshold.
const (
	presentRowsScratchInitCap = 2048  // initial pool capacity
	presentRowsScratchMaxCap  = 65536 // above this, return a fresh small slice instead
)

// VectorF32 column header field offsets.
// Header layout: enc_version[1]+kind[1]+dim[2]+row_count[4]+rle_len[4] = 12 bytes.
const (
	vf32HdrSize   = 1 + 1 + 2 + 4 + 4 // VectorF32 column header: enc_version[1]+kind[1]+dim[2]+row_count[4]+rle_len[4] = 12 bytes
	vf32RleLenOff = 8                 // byte offset of rle_len field within VectorF32 header
)

// NOTE-007: Eliminates per-call make([]int, 0, presentCount) in collectPresentRows.
// Cap guard: slices larger than presentRowsScratchMaxCap entries are replaced with a fresh small slice before
// pool return to avoid retaining large backing arrays indefinitely.
var presentRowsScratchPool = &sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		s := make([]int, 0, presentRowsScratchInitCap)
		return &s
	},
}

func acquirePresentRowsScratch() *[]int {
	return presentRowsScratchPool.Get().(*[]int)
}

func releasePresentRowsScratch(sp *[]int) {
	if cap(*sp) > presentRowsScratchMaxCap {
		// Replace the oversized backing array with a fresh small slice.
		// Assign through the pointer so the pool receives the new small slice,
		// not a pointer to a local stack variable.
		*sp = make([]int, 0, presentRowsScratchInitCap)
	} else {
		*sp = (*sp)[:0]
	}
	presentRowsScratchPool.Put(sp)
}

// getZstdDecoder returns the package-level zstd decoder shared across calls.
// Used only for the vectorF32 column decoder path.
// SPEC-ROOT-001: panic exempt — sync.OnceValue provides init()-equivalent lazy initialization
// semantics. zstd.NewReader cannot return an error with valid options (WithDecoderConcurrency(0)
// is always valid), so the panic path is unreachable dead code in practice.
var getZstdDecoder = sync.OnceValue(func() *zstd.Decoder { //nolint:gochecknoglobals
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic(fmt.Sprintf("reader: zstd.NewReader: %v", err))
	}
	return dec
})

// decompressZstdScratch decompresses a length-prefixed zstd-compressed segment from data[pos:].
// The returned slice is valid only until the next call with the same scratch pointer.
// Used only for the vectorF32 column decoder path.
func decompressZstdScratch(data []byte, pos int, scratch *[]byte) ([]byte, int, error) {
	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("decompressZstd: need 4 bytes for length at pos %d, have %d", pos, len(data))
	}
	cLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if pos+cLen > len(data) {
		return nil, pos, fmt.Errorf(
			"decompressZstd: need %d compressed bytes at pos %d, have %d",
			cLen, pos, len(data),
		)
	}
	*scratch = (*scratch)[:0]
	result, err := getZstdDecoder().DecodeAll(data[pos:pos+cLen], *scratch)
	if err != nil {
		return nil, pos, fmt.Errorf("decompressZstd: %w", err)
	}
	*scratch = result
	return result, pos + cLen, nil
}

// readRawSegment reads a length-prefixed raw segment (raw_len[4]+raw_data) from data[pos:].
// Returns the raw bytes slice and the new position after the segment.
func readRawSegment(data []byte, pos int) ([]byte, int, error) {
	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("readRawSegment: need 4 bytes for length at pos %d, have %d", pos, len(data))
	}

	rLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+rLen > len(data) {
		return nil, pos, fmt.Errorf(
			"readRawSegment: need %d bytes at pos %d, have %d",
			rLen, pos, len(data),
		)
	}

	return data[pos : pos+rLen], pos + rLen, nil
}

// decodeCtx bundles per-parse and per-reader state threaded through column decoders.
// intern is the owning Reader's string intern table and must not be nil.
// scratch is used only by the vectorF32 decoder path (zstd-compressed float data).

// per-reader string intern map, single-goroutine only
// reusable zstd output buffer for vectorF32 decoder; may be nil

// internString looks up b in ctx.intern and returns the interned string.
// On the first occurrence a heap copy is made and stored; subsequent calls with
// equal content return the cached copy without allocating.
func internString(b []byte, ctx *decodeCtx) string {
	if len(b) == 0 {
		return ""
	}
	if ctx.intern == nil {
		return string(b) // no intern map — safe for concurrent use
	}
	// Zero-alloc lookup: temporary string header pointing into b (stack-scoped).
	key := unsafe.String(unsafe.SliceData(b), len(b)) //nolint:gosec // safe: key never escapes this func
	if s, ok := ctx.intern[key]; ok {
		return s
	}
	s := string(b) // heap-allocate only on first occurrence
	ctx.intern[s] = s
	return s
}

// decodePresenceRLEFromSlice reads rle_len[4] + rle_data starting at pos.
// Returns decoded presence bitset, new position, present count, and any error.
func decodePresenceRLEFromSlice(data []byte, pos, nBits int) ([]byte, int, int, error) {
	if pos+4 > len(data) {
		return nil, pos, 0, fmt.Errorf("presence_rle: need 4 bytes for rle_len at pos %d, have %d", pos, len(data))
	}

	rleLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+rleLen > len(data) {
		return nil, pos, 0, fmt.Errorf(
			"presence_rle: need %d rle bytes at pos %d, have %d",
			rleLen, pos, len(data),
		)
	}

	present, err := shared.DecodePresenceRLE(data[pos:pos+rleLen], nBits)
	if err != nil {
		return nil, pos, 0, fmt.Errorf("presence_rle: %w", err)
	}

	presentCount := shared.CountPresent(present, nBits)
	return present, pos + rleLen, presentCount, nil
}

// decodePresenceMaybe reads the presence section starting at pos, or — when allPresent is true
// (an AllPresent encoding kind, NOTE-AP-001) — reports a fully-present column WITHOUT allocating
// a bitset. Returns the presence bitset, the new position (unchanged when allPresent), and the
// present count (== nBits when allPresent).
//
// NOTE-360: for an AllPresent column the returned bitset is nil, not a freshly-allocated
// ceil(nBits/8)-byte all-1s slab. A nil Present field already means "every span present"
// throughout the reader (see Column.IsPresent / PresenceView), so the all-1s bitmap carried
// zero information yet was retained per-column both per-query AND in the parsedV8ColumnCache
// snapshot — for a 1M-row column that is ~125 KB of pure redundant live memory. The decode
// loops that previously walked shared.IsPresent(present, i) must instead consult the returned
// presentCount (== nBits ⇒ every row present): callers that build a dense per-row index from
// the bitset use the allPresentRows fast path below.
func decodePresenceMaybe(data []byte, pos, nBits int, allPresent bool) ([]byte, int, int, error) {
	if allPresent {
		return nil, pos, nBits, nil
	}
	return decodePresenceRLEFromSlice(data, pos, nBits)
}

// readIndexArray reads count index values of indexWidth bytes each from data[pos:].
// indexWidth must be 1, 2, or 4.
func readIndexArray(data []byte, pos, count int, indexWidth uint8) ([]uint32, int, error) {
	if count == 0 {
		return nil, pos, nil
	}

	stride := int(indexWidth)
	if stride != 1 && stride != 2 && stride != 4 {
		return nil, pos, fmt.Errorf("readIndexArray: invalid index_width %d", indexWidth)
	}

	need := count * stride
	if pos+need > len(data) {
		return nil, pos, fmt.Errorf(
			"readIndexArray: need %d bytes at pos %d, have %d",
			need, pos, len(data),
		)
	}

	out := make([]uint32, count)
	switch stride {
	case 1:
		for i := range count {
			out[i] = uint32(data[pos+i])
		}
	case 2:
		for i := range count {
			out[i] = uint32(binary.LittleEndian.Uint16(data[pos+i*2:]))
		}
	case 4:
		for i := range count {
			out[i] = binary.LittleEndian.Uint32(data[pos+i*4:])
		}
	}

	return out, pos + need, nil
}

// readPackedIndexArray copies count index values of indexWidth bytes (1 or 2) into a fresh
// []byte at native width — no 4x/2x expansion to []uint32. The returned slice is a copy
// (not an alias of data, which is the reused decompBuf and will be overwritten by the next
// column's decode). Width 4 is not supported here (no blowup to remove — callers use
// readIndexArray). NOTE-369.
func readPackedIndexArray(data []byte, pos, count int, indexWidth uint8) ([]byte, int, error) {
	if count == 0 {
		return nil, pos, nil
	}
	stride := int(indexWidth)
	if stride != 1 && stride != 2 {
		return nil, pos, fmt.Errorf("readPackedIndexArray: invalid index_width %d", indexWidth)
	}
	need := count * stride
	if pos+need > len(data) {
		return nil, pos, fmt.Errorf(
			"readPackedIndexArray: need %d bytes at pos %d, have %d",
			need, pos, len(data),
		)
	}
	out := make([]byte, need)
	copy(out, data[pos:pos+need])
	return out, pos + need, nil
}

// readColumnEncoding reads enc_version[1] + encoding_kind[1] then dispatches.
// colType is passed through so dictionary decoders can populate the correct typed fields.
// ctx carries the per-reader intern table.
func readColumnEncoding(data []byte, spanCount int, colType shared.ColumnType, ctx *decodeCtx) (*Column, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("column encoding: data too short (%d bytes)", len(data))
	}

	// NOTE-011 (shared/NOTES.md): ColumnTypeVectorF32 uses a custom wire format where
	// only the float data is zstd-compressed. Dispatch by colType so decodeVectorF32 can
	// handle the mixed compressed/uncompressed format directly.
	if colType == shared.ColumnTypeVectorF32 {
		return decodeVectorF32(data, spanCount, ctx)
	}

	encVersion := data[0]
	if encVersion != shared.VersionBlockEncV3 {
		return nil, fmt.Errorf("column encoding: unsupported version %d (only enc_version=3 supported)", encVersion)
	}

	kind := data[1]

	// NOTE-AP-001: AllPresent kinds are wire-identical to their base dense kind except they
	// omit the presence_rle segment. Map them back to the base kind for dispatch and signal
	// the decoders to synthesize a fully-present presence vector instead of reading the (absent)
	// presence bytes.
	baseKind, allPresent := shared.BaseKindFor(kind)

	switch baseKind {
	case shared.KindDictionary, shared.KindSparseDictionary:
		return decodeDictionary(data[2:], baseKind, spanCount, colType, ctx, allPresent)
	case shared.KindInlineBytes, shared.KindSparseInlineBytes:
		return decodeInlineBytes(data[2:], baseKind, spanCount, allPresent)
	case shared.KindDeltaUint64:
		return decodeDeltaUint64(data[2:], spanCount, colType, ctx, allPresent)
	case shared.KindDeltaUint64BitPacked:
		return decodeDeltaUint64BitPacked(data[2:], spanCount, colType, allPresent)
	case shared.KindDeltaUint64Paged:
		return decodeDeltaUint64Paged(data[2:], spanCount, colType)
	case shared.KindRLEIndexes, shared.KindSparseRLEIndexes:
		return decodeRLEIndexes(data[2:], baseKind, spanCount, colType, ctx, allPresent)
	case shared.KindXORBytes, shared.KindSparseXORBytes:
		return decodeXORBytes(data[2:], baseKind, spanCount, ctx, allPresent)
	case shared.KindXORBytesUniform, shared.KindSparseXORBytesUniform:
		return decodeXORBytesUniform(data[2:], baseKind, spanCount, allPresent)
	case shared.KindInlineBytesUniform, shared.KindSparseInlineBytesUniform:
		return decodeInlineBytesUniform(data[2:], baseKind, spanCount, allPresent)
	case shared.KindPrefixBytes, shared.KindSparsePrefixBytes:
		return decodePrefixBytes(data[2:], baseKind, spanCount, ctx, allPresent)
	case shared.KindDeltaDictionary, shared.KindSparseDeltaDictionary:
		return decodeDeltaDictionary(data[2:], baseKind, spanCount, ctx, allPresent)
	case shared.KindGorillaFloat64:
		return decodeGorillaFloat64(data[2:], spanCount, allPresent)
	default:
		return nil, fmt.Errorf("column encoding: unknown kind %d", kind)
	}
}

// ensureDecompressed snappy-decompresses compressedEncoding into rawEncoding on first call.
// SPEC-V14-002: decompression is deferred to first column access so that non-wanted lazy
// columns never pay the CPU/memory cost of snappy decode.
// decompressOnce ensures at most one goroutine decompresses; concurrent callers block until done.
// NOTE-CONC-001: no outer compressedEncoding check — that read would race with the write inside
// the closure. decompressOnce.Do is idempotent: the inner guard handles the already-done case.
func (c *Column) ensureDecompressed() {
	c.decompressOnce.Do(func() {
		if c.compressedEncoding == nil {
			return // not a V14 lazy column or already handled
		}
		// NOTE-209: decompress into a pooled buffer instead of allocating a fresh slice
		// per lazy column. The decompressed bytes (rawEncoding) live only until decodeNow's
		// readColumnEncoding has copied every decoded slice out; decodeNow then returns the
		// buffer via releaseDecompPooled. decompressV14ColumnDataInto grows the buffer when
		// the pooled capacity is too small (recorded as a fresh allocation carried in the
		// pool handle, so the release still recycles whatever buffer ended up holding the
		// data). On error the handle is returned immediately so a failed decode never leaks
		// a pooled buffer.
		bp := decompBufPool.Get().(*[]byte)
		decompressed, grown, err := decompressV14ColumnDataInto(
			(*bp)[:0], c.Name, c.compressedEncoding, c.uncompressedLen,
		)
		if err != nil {
			// SPEC-ROOT-010: log decompression failures; silent skips hide data corruption.
			slog.Warn("block_parser: V14 lazy column decompression failed",
				"column", c.Name, "err", err,
				"uncompressed_len", c.uncompressedLen,
				"max_block_size", shared.MaxBlockSize)
			*bp = grown[:0]
			putDecompBuf(bp)
		} else {
			c.rawEncoding = decompressed
			*bp = grown[:0]
			c.decompPooledPtr = bp
		}
		c.compressedEncoding = nil
		c.uncompressedLen = 0
	})
}

// releaseDecompPooled returns the transient lazy-decode decompression buffer (if any) to
// decompBufPool and clears rawEncoding. NOTE-209: safe to call only after readColumnEncoding
// has copied every decoded slice out of rawEncoding (the "decoders copy data out" invariant);
// after this call rawEncoding must not be read again. Idempotent: a nil handle is a no-op.
func (c *Column) releaseDecompPooled() {
	if c.decompPooledPtr != nil {
		putDecompBuf(c.decompPooledPtr)
		c.decompPooledPtr = nil
	}
	c.rawEncoding = nil
}

// decodeNow performs full decode of this column from rawEncoding.
// Called by value accessors (StringValue, Int64Value, …) and IsPresent on first access.
// decodeOnce ensures at most one goroutine runs the decode; concurrent callers block until done.
// On success, Present and all Dict/Idx slices are populated and rawEncoding is cleared.
// On failure (decompression error or corrupt data), Present is set to []byte{} so that
// IsPresent returns false for every span, and decoded is set to true to prevent retries.
// NOTE-CONC-001: no outer rawEncoding check — that read would race with the write inside
// the closure. decodeOnce.Do is idempotent: the inner guard handles the already-done case.
func (c *Column) decodeNow() {
	// NOTE-201: consult the process-level decoded-column cache BEFORE decompressing.
	// The lazy path defers snappy + readColumnEncoding to first access; without this check the
	// identical on-disk block column is re-decoded on every warm query that filters/scans it
	// lazily (e.g. high-cardinality intrinsic group-bys, predicate-filtered block columns that
	// were not in the eager wantColumns set). On a hit we copy the immutable decoded slices in
	// and skip decompression entirely. The per-query Column keeps its own fresh denseOnce /
	// sparseDictIdx so NOTE-PERF-1 dense expansion runs per query and never mutates the snapshot.
	if c.v8CacheKey != "" {
		if cached := parsedV8ColumnCache.Get(c.v8CacheKey); cached != nil {
			c.decodeOnce.Do(func() {
				if c.Present == nil {
					c.Present = cached.Present
				}
				c.StringDict = cached.StringDict
				c.StringIdx = cached.StringIdx
				c.Int64Dict = cached.Int64Dict
				c.Int64Idx = cached.Int64Idx
				c.Uint64Dict = cached.Uint64Dict
				c.Uint64Idx = cached.Uint64Idx
				c.Float64Dict = cached.Float64Dict
				c.Float64Idx = cached.Float64Idx
				c.BoolDict = cached.BoolDict
				c.BoolIdx = cached.BoolIdx
				c.BytesDict = cached.BytesDict
				c.BytesIdx = cached.BytesIdx
				c.BytesInline = cached.BytesInline
				c.uniformSlab = cached.uniformSlab     // NOTE-351
				c.uniformStride = cached.uniformStride // NOTE-351
				c.sparseDictIdx = cached.sparseDictIdx
				c.denseFlatIdx = cached.denseFlatIdx     // NOTE-358
				c.packedIdx = cached.packedIdx           // NOTE-369
				c.packedIdxWidth = cached.packedIdxWidth // NOTE-369
				c.rawEncoding = nil
				c.compressedEncoding = nil
				c.internMap = nil
				c.decoded.Store(true)
			})
			return
		}
	}

	c.ensureDecompressed()
	c.decodeOnce.Do(func() {
		if c.rawEncoding == nil {
			// Decompression failed or this is an eagerly-decoded column being re-entered;
			// mark as done so subsequent calls skip immediately.
			c.Present = []byte{} // no data → treat all spans as absent
			c.decoded.Store(true)
			return
		}
		ctx := &decodeCtx{intern: c.internMap}
		dec, err := readColumnEncoding(c.rawEncoding, c.SpanCount, c.Type, ctx)
		if err != nil {
			// SPEC-ROOT-010: log decode errors; silent drops hide data corruption.
			slog.Warn("column decode failed", "column", c.Name, "type", c.Type, "err", err)
			c.Present = []byte{} // corrupt data → treat all spans as absent
			c.releaseDecompPooled()
			c.internMap = nil
			c.decoded.Store(true)
			return
		}

		if c.Present == nil {
			c.Present = dec.Present
		}
		c.StringDict = dec.StringDict
		c.StringIdx = dec.StringIdx
		c.Int64Dict = dec.Int64Dict
		c.Int64Idx = dec.Int64Idx
		c.Uint64Dict = dec.Uint64Dict
		c.Uint64Idx = dec.Uint64Idx
		c.Float64Dict = dec.Float64Dict
		c.Float64Idx = dec.Float64Idx
		c.BoolDict = dec.BoolDict
		c.BoolIdx = dec.BoolIdx
		c.BytesDict = dec.BytesDict
		c.BytesIdx = dec.BytesIdx
		c.BytesInline = dec.BytesInline
		c.uniformSlab = dec.uniformSlab     // NOTE-351
		c.uniformStride = dec.uniformStride // NOTE-351
		c.sparseDictIdx = dec.sparseDictIdx
		c.denseFlatIdx = dec.denseFlatIdx     // NOTE-358
		c.packedIdx = dec.packedIdx           // NOTE-369
		c.packedIdxWidth = dec.packedIdxWidth // NOTE-369

		// NOTE-201: store a snapshot of the freshly decoded slices so subsequent warm
		// queries that lazily access the identical on-disk block column skip the decode.
		// dec was allocated fresh by readColumnEncoding; the snapshot shares those read-only
		// slices (dense expansion builds a new Idx on the per-query col, never mutating these).
		if c.v8CacheKey != "" {
			snap := snapshotDecodedColumn(dec, c.Name, c.Type)
			snap.Present = c.Present
			snap.SpanCount = c.SpanCount
			_ = parsedV8ColumnCache.Put(c.v8CacheKey, snap)
		}

		// NOTE-209: readColumnEncoding has copied every decoded slice out of rawEncoding,
		// so the pooled decompression buffer can be recycled now.
		c.releaseDecompPooled()
		c.internMap = nil
		c.decoded.Store(true)
	})
}

// decodeDictBody decodes the zstd-compressed dictionary body and returns typed slices.
// colType determines which typed fields of Column are populated.
func decodeDictBody(dictBytes []byte, col *Column, ctx *decodeCtx) error {
	if len(dictBytes) < 4 {
		return fmt.Errorf("dict body: too short (%d bytes)", len(dictBytes))
	}

	entryCnt := int(binary.LittleEndian.Uint32(dictBytes[0:]))
	pos := 4

	switch col.Type {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		col.StringDict = make([]string, 0, entryCnt)
		for range entryCnt {
			if pos+4 > len(dictBytes) {
				return fmt.Errorf("dict body(string): short at entry")
			}

			sLen := int(binary.LittleEndian.Uint32(dictBytes[pos:]))
			pos += 4
			if pos+sLen > len(dictBytes) {
				return fmt.Errorf("dict body(string): string data overrun")
			}

			col.StringDict = append(col.StringDict, internString(dictBytes[pos:pos+sLen], ctx))
			pos += sLen
		}

	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		// NOTE-216: fixed-width 8B entries are already unframed on the wire
		// (count[4] + N×8, no per-entry length). Validate the full payload
		// length once up front, then read with a tight strided loop — no
		// per-entry bounds branch and no per-entry length read.
		if pos+entryCnt*8 > len(dictBytes) {
			return fmt.Errorf("dict body(int64): payload short for %d entries", entryCnt)
		}

		col.Int64Dict = make([]int64, entryCnt)
		for i := range entryCnt {
			//nolint:gosec // reinterpreting uint64 bits as int64
			col.Int64Dict[i] = int64(binary.LittleEndian.Uint64(dictBytes[pos:]))
			pos += 8
		}

	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		// NOTE-216: see Int64 case.
		if pos+entryCnt*8 > len(dictBytes) {
			return fmt.Errorf("dict body(uint64): payload short for %d entries", entryCnt)
		}

		col.Uint64Dict = make([]uint64, entryCnt)
		for i := range entryCnt {
			col.Uint64Dict[i] = binary.LittleEndian.Uint64(dictBytes[pos:])
			pos += 8
		}

	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		// NOTE-216: see Int64 case.
		if pos+entryCnt*8 > len(dictBytes) {
			return fmt.Errorf("dict body(float64): payload short for %d entries", entryCnt)
		}

		col.Float64Dict = make([]float64, entryCnt)
		for i := range entryCnt {
			bits := binary.LittleEndian.Uint64(dictBytes[pos:])
			pos += 8
			col.Float64Dict[i] = math.Float64frombits(bits)
		}

	case shared.ColumnTypeBool:
		// NOTE-216: 1B entries are unframed (count[4] + N×1). Validate once,
		// then copy the contiguous run in a single operation.
		if pos+entryCnt > len(dictBytes) {
			return fmt.Errorf("dict body(bool): payload short for %d entries", entryCnt)
		}

		col.BoolDict = make([]uint8, entryCnt)
		// pos is not read after this case (the dict body ends here), so no advance needed.
		copy(col.BoolDict, dictBytes[pos:pos+entryCnt])

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes, shared.ColumnTypeUUID:
		// NOTE-356: copy every dict value into ONE contiguous arena, then sub-slice each
		// entry's BytesDict view from it — instead of a per-entry make([]byte, bLen). The old
		// code did entryCnt separate small heap allocations: each rounds up to a malloc
		// size class (a 12-byte value lands in the 16-byte class, ~25% slop) and carries
		// per-object allocator metadata, and the resulting many small objects fragment the
		// heap. A bytes-dict column is retained by parsedV8ColumnCache for the lifetime of
		// the cache entry, so that per-entry rounding + fragmentation is RETAINED inuse_space,
		// not just transient churn. The dict body is read raw (readRawSegment sub-slice of the
		// pooled, soon-recycled decompression buffer), so the values must be copied out; doing
		// it once into a single right-sized arena keeps the same ownership contract with O(1)
		// allocations instead of O(entries). Mirrors the NOTE-150 intrinsic value/ref arenas.
		//
		// Pass 1: validate every entry's length prefix and sum the total value bytes so the
		// arena is sized exactly (no over-allocation, no growth reallocations).
		total := 0
		p := pos
		for range entryCnt {
			if p+4 > len(dictBytes) {
				return fmt.Errorf("dict body(bytes): short at entry")
			}
			bLen := int(binary.LittleEndian.Uint32(dictBytes[p:]))
			p += 4
			if p+bLen > len(dictBytes) {
				return fmt.Errorf("dict body(bytes): data overrun")
			}
			total += bLen
			p += bLen
		}
		// Pass 2: one arena copy, then carve exact-length sub-slices. Each view uses a
		// three-index slice (cap == len) so a stray future append on one entry reallocates
		// rather than overwriting its neighbor in the shared arena.
		col.BytesDict = make([][]byte, entryCnt)
		arena := make([]byte, total)
		off := 0
		for i := range entryCnt {
			bLen := int(binary.LittleEndian.Uint32(dictBytes[pos:]))
			pos += 4
			copy(arena[off:off+bLen], dictBytes[pos:pos+bLen])
			col.BytesDict[i] = arena[off : off+bLen : off+bLen]
			off += bLen
			pos += bLen
		}

	default:
		return fmt.Errorf("dict body: unsupported column type %d", col.Type)
	}

	return nil
}

// decodeDictionary decodes kind 1/2 (Dictionary/SparseDictionary).
// data starts after enc_version + kind bytes.
func decodeDictionary(
	data []byte,
	kind uint8,
	spanCount int,
	colType shared.ColumnType,
	ctx *decodeCtx,
	allPresent bool,
) (*Column, error) {
	col := &Column{SpanCount: spanCount, Type: colType}

	if len(data) < 1 {
		return nil, fmt.Errorf("dictionary: data too short")
	}

	indexWidth := data[0]
	pos := 1

	// dict_len[4] + dict_zstd — decompressed into scratch; data extracted before next scratch use.
	dictBytes, newPos, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}

	pos = newPos

	if err = decodeDictBody(dictBytes, col, ctx); err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}

	// row_count[4]
	if pos+4 > len(data) {
		return nil, fmt.Errorf("dictionary: missing row_count")
	}

	rowCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if rowCount != spanCount {
		return nil, fmt.Errorf("dictionary: row_count %d != spanCount %d", rowCount, spanCount)
	}

	// presence_rle[4+N]
	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}

	pos = newPos
	col.Present = present

	// indexes
	switch kind {
	case shared.KindDictionary: // dense: rowCount indexes
		// NOTE-369: for indexWidth 1/2 keep the index packed at native width instead of
		// expanding 4x/2x into a []uint32 — readers resolve it via dictIdxAt. Width 4 has
		// no blowup to remove and falls back to the materialized []uint32 path.
		if indexWidth == 1 || indexWidth == 2 {
			packed, _, perr := readPackedIndexArray(data, pos, rowCount, indexWidth)
			if perr != nil {
				return nil, fmt.Errorf("dictionary(dense): %w", perr)
			}
			col.packedIdx = packed
			col.packedIdxWidth = indexWidth
		} else {
			idx, _, err := readIndexArray(data, pos, rowCount, indexWidth)
			if err != nil {
				return nil, fmt.Errorf("dictionary(dense): %w", err)
			}
			assignDictIdx(col, idx)
		}

	case shared.KindSparseDictionary: // sparse: present_count[4] + presentCount indexes
		if pos+4 > len(data) {
			return nil, fmt.Errorf("dictionary(sparse): missing present_count")
		}

		sparseCnt := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if sparseCnt != presentCount {
			return nil, fmt.Errorf(
				"dictionary(sparse): present_count %d != presentCount %d",
				sparseCnt, presentCount,
			)
		}

		sparseIdx, _, err := readIndexArray(data, pos, sparseCnt, indexWidth)
		if err != nil {
			return nil, fmt.Errorf("dictionary(sparse): %w", err)
		}

		// Defer dense expansion to first value access (NOTE-PERF-1).
		col.sparseDictIdx = sparseIdx
	}

	return col, nil
}

// assignDictIdx sets the appropriate typed index slice on col based on its Type.
func assignDictIdx(col *Column, idx []uint32) {
	switch col.Type {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		col.StringIdx = idx
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		col.Int64Idx = idx
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		col.Uint64Idx = idx
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		col.Float64Idx = idx
	case shared.ColumnTypeBool:
		col.BoolIdx = idx
	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes, shared.ColumnTypeUUID:
		col.BytesIdx = idx
	}
}

// expandSparseIndexes builds a dense []uint32 of length spanCount from sparse indexes.
// Absent rows get index 0.
func expandSparseIndexes(sparse []uint32, present []byte, spanCount int) []uint32 {
	dense := make([]uint32, spanCount)
	si := 0

	for i := range spanCount {
		if shared.IsPresent(present, i) && si < len(sparse) {
			dense[i] = sparse[si]
			si++
		}
	}

	return dense
}

// decodeInlineBytes decodes kind 3/4 (InlineBytes/SparseInlineBytes).
// data starts after enc_version + kind bytes.
func decodeInlineBytes(data []byte, kind uint8, spanCount int, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("inline_bytes: data too short")
	}

	rowCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if rowCount != spanCount {
		return nil, fmt.Errorf("inline_bytes: row_count %d != spanCount %d", rowCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("inline_bytes: %w", err)
	}

	pos = newPos
	col.Present = present

	// For inline bytes we need a dense slice: nil for absent rows.
	col.BytesInline = make([][]byte, spanCount)

	switch kind {
	case shared.KindInlineBytes: // dense: rowCount × {len[4] + bytes}
		for i := range rowCount {
			if pos+4 > len(data) {
				return nil, fmt.Errorf("inline_bytes(dense): short at row %d", i)
			}

			bLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+bLen > len(data) {
				return nil, fmt.Errorf("inline_bytes(dense): data overrun at row %d", i)
			}

			b := make([]byte, bLen)
			copy(b, data[pos:pos+bLen])
			col.BytesInline[i] = b
			pos += bLen
		}

	case shared.KindSparseInlineBytes: // sparse: present_count[4] + presentCount × {len[4] + bytes}
		if pos+4 > len(data) {
			return nil, fmt.Errorf("inline_bytes(sparse): missing present_count")
		}

		sparseCnt := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if sparseCnt != presentCount {
			return nil, fmt.Errorf(
				"inline_bytes(sparse): present_count %d != presentCount %d",
				sparseCnt, presentCount,
			)
		}

		si := 0
		for i := range spanCount {
			if !shared.IsPresent(present, i) {
				continue
			}

			if si >= sparseCnt {
				break
			}

			if pos+4 > len(data) {
				return nil, fmt.Errorf("inline_bytes(sparse): short at present row %d", si)
			}

			bLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+bLen > len(data) {
				return nil, fmt.Errorf("inline_bytes(sparse): data overrun at present row %d", si)
			}

			b := make([]byte, bLen)
			copy(b, data[pos:pos+bLen])
			col.BytesInline[i] = b
			pos += bLen
			si++
		}
	}

	return col, nil
}

// decodeDeltaUint64 decodes kind 5 (DeltaUint64).
// data starts after enc_version + kind bytes.
// isDeltaInt64ColType returns true for column types that store signed int64 values via
// the delta uint64 encoders (NOTE-222). The bit patterns are identical; only the
// interpretation (and the output field — Int64Dict vs Uint64Dict) differs.
func isDeltaInt64ColType(ct shared.ColumnType) bool {
	return ct == shared.ColumnTypeInt64 ||
		ct == shared.ColumnTypeRangeInt64 ||
		ct == shared.ColumnTypeRangeDuration
}

// promoteToInt64Dict reinterprets a uint64 dict as int64 in-place and moves it to
// col.Int64Dict, sharing the Idx slice. Used by delta decoders for signed int64 columns.
func promoteToInt64Dict(col *Column) {
	col.Int64Dict = make([]int64, len(col.Uint64Dict))
	for i, v := range col.Uint64Dict {
		col.Int64Dict[i] = int64(v) //nolint:gosec // bit-pattern reinterpret; NOTE-222
	}
	col.Int64Idx = col.Uint64Idx
	col.Uint64Dict = nil
	col.Uint64Idx = nil
}

func decodeDeltaUint64(
	data []byte,
	spanCount int,
	colType shared.ColumnType,
	ctx *decodeCtx,
	allPresent bool,
) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("delta_uint64: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("delta_uint64: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("delta_uint64: %w", err)
	}

	pos = newPos
	col.Present = present

	// base[8] + width[1]
	if pos+9 > len(data) {
		return nil, fmt.Errorf("delta_uint64: missing base/width at pos %d", pos)
	}

	base := binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	width := data[pos]
	pos++

	// Build dense Uint64Dict (one entry per present row, in row order).
	col.Uint64Dict = make([]uint64, presentCount)

	if width == 0 {
		// All present values equal base.
		for i := range presentCount {
			col.Uint64Dict[i] = base
		}
	} else {
		// Read compressed offset array — decompressed into scratch; values extracted before next scratch use.
		offsetBytes, _, err := readRawSegment(data, pos)
		if err != nil {
			return nil, fmt.Errorf("delta_uint64: offsets: %w", err)
		}

		stride := int(width)
		need := presentCount * stride
		if len(offsetBytes) < need {
			return nil, fmt.Errorf(
				"delta_uint64: offsets: need %d bytes, got %d",
				need, len(offsetBytes),
			)
		}

		for i := range presentCount {
			var off uint64
			switch width {
			case 1:
				off = uint64(offsetBytes[i])
			case 2:
				off = uint64(binary.LittleEndian.Uint16(offsetBytes[i*2:]))
			case 4:
				off = uint64(binary.LittleEndian.Uint32(offsetBytes[i*4:]))
			case 8:
				off = binary.LittleEndian.Uint64(offsetBytes[i*8:])
			default:
				return nil, fmt.Errorf("delta_uint64: unsupported width %d", width)
			}

			col.Uint64Dict[i] = base + off
		}
	}

	// Build dense index array.
	// NOTE-361: when every row is present this column's index is the pure identity
	// permutation [0,1,…,spanCount-1] (Dict has one entry per present row, in row order, so
	// present-rank(i) == i). Set denseFlatIdx and skip the make([]uint32, spanCount) +
	// per-row fill entirely — readers resolve the dict index arithmetically via dictIdxAt.
	// This extends the NOTE-358 optimization (already applied to decodeDeltaUint64BitPacked
	// and decodeGorillaFloat64, which produce the byte-identical dense-flat layout) to the
	// non-bit-packed delta kind 21 — the still-materializing decodeDeltaUint64 frame. Only
	// the all-present case qualifies; a partial-presence column needs the real present-rank
	// table, so it falls through to materialize Uint64Idx (denseFlatIdx == false).
	// present == nil is the AllPresent kind (NOTE-360, no presence bitmap) and is always
	// fully present; presentCount == spanCount covers both that and a materialized all-1s
	// bitmap.
	if presentCount == spanCount {
		col.denseFlatIdx = true
	} else {
		col.Uint64Idx = make([]uint32, spanCount)
		dictIdx := 0
		for i := range spanCount {
			if shared.IsPresent(present, i) {
				col.Uint64Idx[i] = uint32(dictIdx) //nolint:gosec
				dictIdx++
			}
		}
	}

	// NOTE-222: if the column type is a signed int64 family, reinterpret the uint64 bit
	// patterns as int64 and move to Int64Dict/Int64Idx so the executor finds them correctly.
	if isDeltaInt64ColType(colType) {
		promoteToInt64Dict(col)
	}

	return col, nil
}

// decodeDeltaUint64BitPacked decodes kind 22 (DeltaUint64BitPacked, NOTE-215).
// data starts after enc_version + kind bytes.
//
// Wire: span_count[4] + presence + base[8] + bit_width[1] + packed_len[4] + packed_offsets.
// Offsets are an LSB-first bit stream of presentCount values, each bit_width bits wide.
func decodeDeltaUint64BitPacked(
	data []byte,
	spanCount int,
	colType shared.ColumnType,
	allPresent bool,
) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("delta_uint64_bitpacked: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("delta_uint64_bitpacked: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("delta_uint64_bitpacked: %w", err)
	}

	pos = newPos
	col.Present = present

	// base[8] + bit_width[1]
	if pos+9 > len(data) {
		return nil, fmt.Errorf("delta_uint64_bitpacked: missing base/bit_width at pos %d", pos)
	}

	base := binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	bitWidth := data[pos]
	pos++

	if bitWidth > 64 {
		return nil, fmt.Errorf("delta_uint64_bitpacked: invalid bit_width %d", bitWidth)
	}

	col.Uint64Dict = make([]uint64, presentCount)

	// packed_len[4] + packed_offsets — always present (zero-length when bit_width==0).
	packed, _, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("delta_uint64_bitpacked: offsets: %w", err)
	}

	if bitWidth == 0 {
		for i := range presentCount {
			col.Uint64Dict[i] = base
		}
	} else {
		need := (presentCount*int(bitWidth) + 7) / 8
		if len(packed) < need {
			return nil, fmt.Errorf(
				"delta_uint64_bitpacked: offsets: need %d bytes, got %d",
				need, len(packed),
			)
		}
		// NOTE-338: bulk-unpack the contiguous equal-width offsets in one pass.
		unpackDeltaBitsLE(packed, bitWidth, base, col.Uint64Dict[:presentCount])
	}

	// NOTE-358: when every row is present the dict-index slice would be the pure identity
	// permutation [0,1,…,spanCount-1] (Dict has one entry per row in row order). Skip the
	// make([]uint32, spanCount) and resolve the index arithmetically via dictIdxAt. Only
	// the all-present case qualifies; a partial-presence column needs the real present-rank
	// table, so it falls through to materialize Uint64Idx below.
	if presentCount == spanCount {
		col.denseFlatIdx = true
	} else {
		col.Uint64Idx = make([]uint32, spanCount)
		dictIdx := 0
		for i := range spanCount {
			if shared.IsPresent(present, i) {
				col.Uint64Idx[i] = uint32(dictIdx) //nolint:gosec
				dictIdx++
			}
		}
	}

	// NOTE-222: signed int64 columns stored via the uint64 delta encoder.
	if isDeltaInt64ColType(colType) {
		promoteToInt64Dict(col)
	}

	return col, nil
}

// decodeGorillaFloat64 decodes kind 40 (GorillaFloat64, NOTE-219, SPECS §9.8).
// data starts after enc_version + kind bytes. allPresent signals the kind-41 AllPresent
// variant whose presence_rle segment is omitted.
//
// Wire: span_count[4] + presence(maybe) + stream_bit_len[8] + stream_len[4] + stream_bytes.
//
// The decoded present values are stored as a flat Float64Dict (one entry per present row) with
// an identity-by-present Float64Idx — the same dense layout decodeDeltaUint64BitPacked uses for
// uint64. Each present value is reconstructed by XOR-folding the Gorilla stream against the
// running predecessor; the exact IEEE-754 bit pattern is preserved (NaN payloads, ±0.0).
func decodeGorillaFloat64(data []byte, spanCount int, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("gorilla_float64: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("gorilla_float64: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("gorilla_float64: %w", err)
	}
	pos = newPos
	col.Present = present

	if pos+8 > len(data) {
		return nil, fmt.Errorf("gorilla_float64: missing stream_bit_len at pos %d", pos)
	}
	streamBitLen := binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	stream, _, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("gorilla_float64: stream: %w", err)
	}
	if uint64(len(stream))*8 < streamBitLen {
		return nil, fmt.Errorf(
			"gorilla_float64: stream %d bytes too short for bit_len %d",
			len(stream), streamBitLen,
		)
	}

	col.Float64Dict = make([]float64, presentCount)

	if err := decodeGorillaStream(stream, streamBitLen, col.Float64Dict); err != nil {
		return nil, fmt.Errorf("gorilla_float64: %w", err)
	}

	// NOTE-358: all-present ⇒ the index slice is the identity permutation. Skip the
	// make([]uint32, spanCount) and resolve the dict index via dictIdxAt (see the matching
	// branch in decodeDeltaUint64BitPacked). Partial-presence keeps the materialized slice.
	if presentCount == spanCount {
		col.denseFlatIdx = true
	} else {
		col.Float64Idx = make([]uint32, spanCount)
		dictIdx := 0
		for i := range spanCount {
			if shared.IsPresent(present, i) {
				col.Float64Idx[i] = uint32(dictIdx) //nolint:gosec
				dictIdx++
			}
		}
	}

	return col, nil
}

// decodeGorillaStream unpacks the Gorilla-XOR bit stream into out (len == present count). It is
// the exact inverse of the writer's encodeGorillaFloat64 stream loop. streamBitLen bounds the
// readable bits so trailing zero padding in the final byte is never misread as a control bit.
func decodeGorillaStream(stream []byte, streamBitLen uint64, out []float64) error {
	if len(out) == 0 {
		return nil
	}

	bitPos := 0
	readBit := func() (uint64, error) {
		if uint64(bitPos)+1 > streamBitLen { //nolint:gosec // bitPos >= 0
			return 0, fmt.Errorf("stream underrun at bit %d", bitPos)
		}
		b := readBitsLE(stream, bitPos, 1)
		bitPos++
		return b, nil
	}
	readN := func(width uint8) (uint64, error) {
		if uint64(bitPos)+uint64(width) > streamBitLen { //nolint:gosec // bitPos >= 0
			return 0, fmt.Errorf("stream underrun reading %d bits at %d", width, bitPos)
		}
		v := readBitsLE(stream, bitPos, width)
		bitPos += int(width)
		return v, nil
	}

	// First value is stored verbatim as a 64-bit word.
	firstBits, err := readN(64)
	if err != nil {
		return err
	}
	out[0] = math.Float64frombits(firstBits)
	prevBits := firstBits

	var prevLeading, prevTrailing uint8
	for i := 1; i < len(out); i++ {
		ctrl, err := readBit()
		if err != nil {
			return err
		}
		if ctrl == 0 {
			out[i] = math.Float64frombits(prevBits)
			continue
		}
		block, err := readBit()
		if err != nil {
			return err
		}
		if block == 0 {
			// Reuse previous window.
			meaningfulLen := 64 - int(prevLeading) - int(prevTrailing)
			if meaningfulLen < 1 || meaningfulLen > 64 {
				return fmt.Errorf("invalid reused window len %d at value %d", meaningfulLen, i)
			}
			m, mErr := readN(uint8(meaningfulLen)) //nolint:gosec // 1..64
			if mErr != nil {
				return mErr
			}
			xor := m << uint(prevTrailing)
			curBits := prevBits ^ xor
			out[i] = math.Float64frombits(curBits)
			prevBits = curBits
			continue
		}
		// New window: leading[5] + meaningful_len_minus_1[6] + meaningful[len].
		leadingV, err := readN(5)
		if err != nil {
			return err
		}
		lenV, err := readN(6)
		if err != nil {
			return err
		}
		leading := uint8(leadingV)                      //nolint:gosec // 0..31
		meaningfulLen := int(lenV) + 1                  //nolint:gosec // lenV 0..63 -> 1..64
		trailing := 64 - leading - uint8(meaningfulLen) //nolint:gosec // bounded below
		if int(leading)+meaningfulLen > 64 {
			return fmt.Errorf("invalid new window leading=%d len=%d at value %d", leading, meaningfulLen, i)
		}
		m, mErr := readN(uint8(meaningfulLen)) //nolint:gosec // 1..64
		if mErr != nil {
			return mErr
		}
		xor := m << uint(trailing)
		curBits := prevBits ^ xor
		out[i] = math.Float64frombits(curBits)
		prevBits = curBits
		prevLeading = leading
		prevTrailing = trailing
	}

	return nil
}

// deltaPageSizeReader is the number of present rows per page in the per-page DeltaUint64
// encoding (kind 39, NOTE-218). It MUST match the writer's deltaPageSize: the wire format does
// not store the per-page row count (it is derived from the global page ordering), so the reader
// reconstructs page boundaries from this constant. Changing it on only one side corrupts decode.
const deltaPageSizeReader = 1024

// decodeDeltaUint64Paged decodes kind 39 (DeltaUint64Paged, NOTE-218).
// data starts after enc_version + kind bytes.
//
// Wire: span_count[4] + presence + page_count[2]
//
//   - page_count × (page_first_row[4] + page_base[8] + page_bit_width[1] + page_payload_bytes[4])
//   - page_count × page_payload
//
// Each page holds up to deltaPageSize present rows, packed LSB-first at the page's own bit_width
// over the page's own base. There is no AllPresent variant: the presence segment is always read.
func decodeDeltaUint64Paged(data []byte, spanCount int, colType shared.ColumnType) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("delta_uint64_paged: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("delta_uint64_paged: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("delta_uint64_paged: %w", err)
	}
	pos = newPos
	col.Present = present

	if pos+2 > len(data) {
		return nil, fmt.Errorf("delta_uint64_paged: missing page_count at pos %d", pos)
	}
	pageCount := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// Read the page index.
	type pageHeader struct {
		base         uint64
		firstRow     int
		payloadBytes int
		bitWidth     uint8
	}
	headers := make([]pageHeader, pageCount)
	for p := range pageCount {
		if pos+17 > len(data) {
			return nil, fmt.Errorf("delta_uint64_paged: truncated page index at page %d", p)
		}
		fr := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		base := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		bw := data[pos]
		pos++
		pb := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		if bw > 64 {
			return nil, fmt.Errorf("delta_uint64_paged: page %d invalid bit_width %d", p, bw)
		}
		headers[p] = pageHeader{
			firstRow:     int(fr),
			base:         base,
			bitWidth:     bw,
			payloadBytes: pb,
		}
	}

	col.Uint64Dict = make([]uint64, presentCount)
	col.Uint64Idx = make([]uint32, spanCount)

	// Precompute the present-row indices so each page's stored page_first_row can be validated
	// against the actual first present row of that page (integrity check).
	presentRowIdx := make([]int, 0, presentCount)
	for i := range spanCount {
		if shared.IsPresent(present, i) {
			presentRowIdx = append(presentRowIdx, i)
		}
	}

	// Stream each page's payload directly into the dict.
	dictIdx := 0
	for p := range pageCount {
		hdr := headers[p]
		pageRows := deltaPageSizeReader
		remaining := presentCount - dictIdx
		if pageRows > remaining {
			pageRows = remaining
		}

		if pageRows > 0 && hdr.firstRow != presentRowIdx[dictIdx] {
			return nil, fmt.Errorf(
				"delta_uint64_paged: page %d first_row %d != actual %d",
				p, hdr.firstRow, presentRowIdx[dictIdx],
			)
		}

		if pos+hdr.payloadBytes > len(data) {
			return nil, fmt.Errorf("delta_uint64_paged: page %d payload truncated", p)
		}
		payload := data[pos : pos+hdr.payloadBytes]
		pos += hdr.payloadBytes

		if hdr.bitWidth == 0 {
			for i := 0; i < pageRows; i++ {
				col.Uint64Dict[dictIdx] = hdr.base
				dictIdx++
			}
			continue
		}

		need := (pageRows*int(hdr.bitWidth) + 7) / 8
		if len(payload) < need {
			return nil, fmt.Errorf(
				"delta_uint64_paged: page %d offsets: need %d bytes, got %d",
				p, need, len(payload),
			)
		}
		// NOTE-338: bulk-unpack this page's contiguous equal-width offsets in one pass.
		unpackDeltaBitsLE(payload, hdr.bitWidth, hdr.base, col.Uint64Dict[dictIdx:dictIdx+pageRows])
		dictIdx += pageRows
	}

	if dictIdx != presentCount {
		return nil, fmt.Errorf(
			"delta_uint64_paged: decoded %d present values, expected %d",
			dictIdx, presentCount,
		)
	}

	for pi, row := range presentRowIdx {
		col.Uint64Idx[row] = uint32(pi) //nolint:gosec
	}

	// NOTE-222: signed int64 columns stored via the uint64 paged delta encoder.
	if isDeltaInt64ColType(colType) {
		promoteToInt64Dict(col)
	}

	return col, nil
}

// unpackDeltaBitsLE decodes count contiguous, equal-width (width bits, 1..64) LSB-first
// values from packed and writes base+value into dst[0:count]. It is the bulk equivalent of
// the per-row `readBitsLE(packed, i*width, width)` loops in the delta-uint64 decoders.
//
// NOTE-338: the bit-packed delta-uint64 decoders (decodeDeltaUint64BitPacked and the per-page
// variant in decodeDeltaUint64Paged) called the non-inlinable readBitsLE (cost 87 > budget 80)
// once per present row, re-deriving byteIdx/bitOff from a fresh bitPos and running its inner
// 1–9-iteration chunk loop every value — readBitsLE was ~0.46% of querier self-time (profile
// 2026-06-14) and these two loops were its only hot callers. This helper instead carries a
// 64-bit little-endian accumulator (`acc`, holding `nbits` buffered bits) and refills it from
// `packed` one byte at a time from a monotonically advancing cursor, so each value is one
// mask-and-shift with no per-value index math and no function-call overhead, and the source is
// read strictly sequentially (cache-friendly). The caller must guarantee packed holds at least
// ceil(count*width/8) bytes (both callers validate `need` up front).
//
// Widths > 56 fall back to the per-value readBitsLE: their post-extraction remainder (< width)
// can be large enough that adding another byte would overflow the 64-bit window. width == 0
// never reaches here (both callers special-case the all-zero-offset case before calling).
func unpackDeltaBitsLE(packed []byte, width uint8, base uint64, dst []uint64) {
	// Wide widths (>56 bits) can leave a remainder large enough that adding another byte to
	// the 64-bit accumulator would overflow it, so the fast accumulator below is unsafe for
	// them. They are vanishingly rare for delta offsets; fall back to the per-value reader.
	if width > 56 {
		bitPos := 0
		for i := range dst {
			dst[i] = base + readBitsLE(packed, bitPos, width)
			bitPos += int(width)
		}
		return
	}
	mask := uint64(1)<<width - 1
	var (
		acc     uint64 // buffered bits, LSB-first, in the low nbits bits
		nbits   uint   // number of valid bits currently in acc
		bytePos int    // next unread byte in packed
		w       = uint(width)
	)
	// Invariant: on each value extraction the remainder nbits-w < w <= 56, so before any
	// byte add nbits <= 55 and `acc |= byte << nbits` (bits nbits..nbits+7 <= 62) never
	// overflows the 64-bit window. The byte cursor advances monotonically (sequential read).
	for i := range dst {
		for nbits < w {
			acc |= uint64(packed[bytePos]) << nbits
			bytePos++
			nbits += 8
		}
		dst[i] = base + (acc & mask)
		acc >>= w
		nbits -= w
	}
}

// readBitsLE reads width bits (1..64) from src starting at bit offset bitPos, LSB-first within
// each byte — the inverse of the writer's writeBitsLE. src must hold at least bitPos+width bits.
func readBitsLE(src []byte, bitPos int, width uint8) uint64 {
	var v uint64
	read := 0
	remaining := int(width)
	for remaining > 0 {
		byteIdx := bitPos >> 3
		bitOff := bitPos & 7
		n := 8 - bitOff
		if n > remaining {
			n = remaining
		}
		mask := uint64(1)<<uint(n) - 1
		chunk := (uint64(src[byteIdx]) >> uint(bitOff)) & mask
		v |= chunk << uint(read)
		read += n
		bitPos += n
		remaining -= n
	}
	return v
}

// decodeRLEIndexes decodes kind 6/7 (RLEIndexes/SparseRLEIndexes).
// data starts after enc_version + kind bytes.
func decodeRLEIndexes(
	data []byte,
	kind uint8,
	spanCount int,
	colType shared.ColumnType,
	ctx *decodeCtx,
	allPresent bool,
) (*Column, error) {
	col := &Column{SpanCount: spanCount, Type: colType}

	if len(data) < 1 {
		return nil, fmt.Errorf("rle_indexes: data too short")
	}

	_ = data[0] // index_width: present in wire format, not needed for RLE decode
	pos := 1

	// dict_len[4] + dict_zstd — decompressed into scratch; data extracted before next scratch use.
	dictBytes, newPos, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("rle_indexes: dict: %w", err)
	}

	pos = newPos

	if err = decodeDictBody(dictBytes, col, ctx); err != nil {
		return nil, fmt.Errorf("rle_indexes: dict body: %w", err)
	}

	// row_count[4]
	if pos+4 > len(data) {
		return nil, fmt.Errorf("rle_indexes: missing row_count")
	}

	rowCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if rowCount != spanCount {
		return nil, fmt.Errorf("rle_indexes: row_count %d != spanCount %d", rowCount, spanCount)
	}

	// presence_rle[4+N]
	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("rle_indexes: %w", err)
	}

	pos = newPos
	col.Present = present

	// index_count[4] + rle_len[4] + rle_data
	if pos+4 > len(data) {
		return nil, fmt.Errorf("rle_indexes: missing index_count")
	}

	indexCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+4 > len(data) {
		return nil, fmt.Errorf("rle_indexes: missing rle_len")
	}

	rleLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+rleLen > len(data) {
		return nil, fmt.Errorf("rle_indexes: rle_data overrun at pos %d, need %d", pos, rleLen)
	}

	rleData := data[pos : pos+rleLen]

	sparseIdx, err := shared.DecodeIndexRLE(rleData, indexCount)
	if err != nil {
		return nil, fmt.Errorf("rle_indexes: decode RLE: %w", err)
	}

	// For kind 7 (sparse): sparseIdx covers only present rows.
	// For kind 6 (dense):  sparseIdx covers all rows.
	var denseIdx []uint32
	switch kind {
	case shared.KindRLEIndexes: // dense
		if indexCount != spanCount {
			return nil, fmt.Errorf("rle_indexes(dense): index_count %d != spanCount %d", indexCount, spanCount)
		}

		denseIdx = sparseIdx

	case shared.KindSparseRLEIndexes: // sparse
		if indexCount != presentCount {
			return nil, fmt.Errorf(
				"rle_indexes(sparse): index_count %d != presentCount %d",
				indexCount, presentCount,
			)
		}

		// Defer dense expansion to first value access (NOTE-PERF-1).
		col.sparseDictIdx = sparseIdx
		return col, nil
	}

	assignDictIdx(col, denseIdx)
	return col, nil
}

// decodeXORBytes decodes kind 8/9 (XORBytes/SparseXORBytes).
// data starts after enc_version + kind bytes.
func decodeXORBytes(data []byte, kind uint8, spanCount int, ctx *decodeCtx, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("xor_bytes: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("xor_bytes: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("xor_bytes: %w", err)
	}

	pos = newPos
	col.Present = present
	_ = kind // sparse/dense distinction handled entirely by presence bitset

	// xor_len[4] + xor_data_zstd — decompressed into scratch; each row value is XOR-decoded
	// into a fresh make([]byte,...) before the next scratch use, so scratch is not retained.
	xorBytes, _, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("xor_bytes: payload: %w", err)
	}

	// Decode XOR payload: for each present row: val_len[4] + xor_bytes.
	col.BytesInline = make([][]byte, spanCount)
	var prev []byte
	xPos := 0

	// NOTE-007: Acquire pooled scratch for present-row index list; defer release covers error paths.
	presRowsBuf := acquirePresentRowsScratch()
	defer releasePresentRowsScratch(presRowsBuf)
	presentRows := collectPresentRowsInto(present, presentCount, spanCount, presRowsBuf)
	for _, presentRow := range presentRows {
		if xPos+4 > len(xorBytes) {
			return nil, fmt.Errorf("xor_bytes: short at present row %d", presentRow)
		}

		vLen := int(binary.LittleEndian.Uint32(xorBytes[xPos:]))
		xPos += 4

		if xPos+vLen > len(xorBytes) {
			return nil, fmt.Errorf("xor_bytes: data overrun at present row %d", presentRow)
		}

		xorVal := xorBytes[xPos : xPos+vLen]
		xPos += vLen

		// XOR against prev (byte-wise up to min length; extra bytes appended as-is).
		result := make([]byte, max(vLen, len(prev)))
		for i := range vLen {
			if i < len(prev) {
				result[i] = xorVal[i] ^ prev[i]
			} else {
				result[i] = xorVal[i]
			}
		}

		// Append any prev bytes beyond vLen as-is.
		if len(prev) > vLen {
			copy(result[vLen:], prev[vLen:])
		}

		col.BytesInline[presentRow] = result
		prev = result
	}

	return col, nil
}

// decodeXORBytesUniform decodes the uniform-length XOR kinds (24/25/28, NOTE-217).
// data starts after enc_version + kind bytes. The wire format drops the per-row len[4]
// prefix: a single uniform_len[4 LE] follows the presence segment, then nPresent ×
// uniform_len packed XOR bytes. allPresent signals the AllPresent variant (presence segment
// omitted).
func decodeXORBytesUniform(data []byte, kind uint8, spanCount int, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("xor_bytes_uniform: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4
	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("xor_bytes_uniform: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("xor_bytes_uniform: %w", err)
	}
	pos = newPos
	col.Present = present
	_ = kind // sparse/dense distinction handled entirely by presence bitset

	if pos+4 > len(data) {
		return nil, fmt.Errorf("xor_bytes_uniform: missing uniform_len")
	}
	uniformLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if uniformLen <= 0 {
		return nil, fmt.Errorf("xor_bytes_uniform: invalid uniform_len %d", uniformLen)
	}

	payload := data[pos:]
	if len(payload) < presentCount*uniformLen {
		return nil, fmt.Errorf(
			"xor_bytes_uniform: payload short: have %d need %d",
			len(payload), presentCount*uniformLen,
		)
	}

	// NOTE-351: store uniform-length rows ROW-INDEXED in one contiguous spanCount*uniformLen
	// slab (row idx at slab[idx*uniformLen:]) and skip the [][]byte header array entirely.
	// The old code allocated make([][]byte, spanCount) (spanCount*24 bytes of slice headers,
	// ~1.5x the actual data for 16-byte trace:id/span:id and the dominant retained footprint
	// of this path) on top of the slab. Row-indexed storage lets bytesInlineAt(idx) compute
	// the offset directly, so the header array is pure waste. Absent rows occupy a (zeroed)
	// uniformLen gap but are masked by the presence check in bytesInlineAt; for the all-present
	// trace:id case there are no gaps and this is exactly the data with zero overhead.
	// NOTE-342 (superseded): the prior single-slab fix collapsed per-row allocations; this
	// keeps that win and additionally drops the header array.
	col.uniformStride = uint32(uniformLen) //nolint:gosec // uniformLen validated > 0, << 4 GiB
	slab := make([]byte, spanCount*uniformLen)
	col.uniformSlab = slab
	var prev []byte
	xPos := 0

	presRowsBuf := acquirePresentRowsScratch()
	defer releasePresentRowsScratch(presRowsBuf)
	presentRows := collectPresentRowsInto(present, presentCount, spanCount, presRowsBuf)
	for _, presentRow := range presentRows {
		xorVal := payload[xPos : xPos+uniformLen]
		xPos += uniformLen

		rOff := presentRow * uniformLen
		result := slab[rOff : rOff+uniformLen : rOff+uniformLen]

		// All values share uniformLen, so XOR against prev is a straight uniformLen-byte loop.
		for i := range uniformLen {
			if i < len(prev) {
				result[i] = xorVal[i] ^ prev[i]
			} else {
				result[i] = xorVal[i]
			}
		}

		prev = result
	}

	return col, nil
}

// decodeInlineBytesUniform decodes the uniform-length InlineBytes kinds (26/27, NOTE-217).
// data starts after enc_version + kind bytes. These kinds are reader-only (the current writer
// never selects the InlineBytes family); they remain decodable for forward compatibility.
// The wire format mirrors the variable InlineBytes layout but replaces the per-row len[4]
// prefix with a single uniform_len[4 LE] header, then nPresent × uniform_len raw bytes.
func decodeInlineBytesUniform(data []byte, kind uint8, spanCount int, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("inline_bytes_uniform: data too short")
	}

	rowCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4
	if rowCount != spanCount {
		return nil, fmt.Errorf("inline_bytes_uniform: row_count %d != spanCount %d", rowCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("inline_bytes_uniform: %w", err)
	}
	pos = newPos
	col.Present = present
	_ = kind // sparse/dense distinction handled entirely by presence bitset

	if pos+4 > len(data) {
		return nil, fmt.Errorf("inline_bytes_uniform: missing uniform_len")
	}
	uniformLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if uniformLen <= 0 {
		return nil, fmt.Errorf("inline_bytes_uniform: invalid uniform_len %d", uniformLen)
	}

	payload := data[pos:]
	if len(payload) < presentCount*uniformLen {
		return nil, fmt.Errorf(
			"inline_bytes_uniform: payload short: have %d need %d",
			len(payload), presentCount*uniformLen,
		)
	}

	// NOTE-351: row-indexed uniform slab (same scheme as decodeXORBytesUniform) — skip the
	// [][]byte header array; bytesInlineAt(idx) computes the slab offset from uniformStride.
	col.uniformStride = uint32(uniformLen) //nolint:gosec // uniformLen validated > 0, << 4 GiB
	slab := make([]byte, spanCount*uniformLen)
	col.uniformSlab = slab
	xPos := 0

	presRowsBuf := acquirePresentRowsScratch()
	defer releasePresentRowsScratch(presRowsBuf)
	presentRows := collectPresentRowsInto(present, presentCount, spanCount, presRowsBuf)
	for _, presentRow := range presentRows {
		rOff := presentRow * uniformLen
		copy(slab[rOff:rOff+uniformLen], payload[xPos:xPos+uniformLen])
		xPos += uniformLen
	}

	return col, nil
}

// collectPresentRowsInto appends row indices where the present bit is set into *buf.
// *buf is reset to [:0] before use; the caller owns buf and must release it to the pool.
//
// NOTE-007: Replaces collectPresentRows; eliminates per-call make([]int) allocation by
// reusing a pooled scratch slice. The presentCount parameter is unused for pre-sizing
// (the slice is pre-allocated by the pool) but retained for documentation clarity.
func collectPresentRowsInto(present []byte, _ /*presentCount*/, spanCount int, buf *[]int) []int {
	*buf = (*buf)[:0]
	// NOTE-360: present == nil means every span is present (AllPresent kind, no bitmap).
	// IsPresent(nil, i) is always false, so without this branch the all-present case would
	// collect an empty row set and corrupt the decode.
	if present == nil {
		for i := range spanCount {
			*buf = append(*buf, i)
		}
		return *buf
	}
	for i := range spanCount {
		if shared.IsPresent(present, i) {
			*buf = append(*buf, i)
		}
	}

	return *buf
}

// decodePrefixBytes decodes kind 10/11 (PrefixBytes/SparsePrefixBytes).
// data starts after enc_version + kind bytes.
func decodePrefixBytes(data []byte, kind uint8, spanCount int, ctx *decodeCtx, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("prefix_bytes: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("prefix_bytes: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("prefix_bytes: %w", err)
	}

	pos = newPos
	col.Present = present
	_ = kind // sparse/dense handled by presence bitset

	// prefix_dict_len[4] + prefix_dict_zstd — decompressed into scratch; all prefix data
	// is copy()-d into fresh allocations before the next scratch use (suffix decompression).
	prefixDictBytes, newPos, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("prefix_bytes: prefix_dict: %w", err)
	}

	pos = newPos

	// Parse prefix dictionary: prefix_count[4] + prefix_count × (len[4]+bytes)
	if len(prefixDictBytes) < 4 {
		return nil, fmt.Errorf("prefix_bytes: prefix_dict too short")
	}

	prefixCount := int(binary.LittleEndian.Uint32(prefixDictBytes[0:]))
	pdPos := 4
	prefixes := make([][]byte, prefixCount)

	for i := range prefixCount {
		if pdPos+4 > len(prefixDictBytes) {
			return nil, fmt.Errorf("prefix_bytes: prefix %d short", i)
		}

		pLen := int(binary.LittleEndian.Uint32(prefixDictBytes[pdPos:]))
		pdPos += 4

		if pdPos+pLen > len(prefixDictBytes) {
			return nil, fmt.Errorf("prefix_bytes: prefix %d data overrun", i)
		}

		p := make([]byte, pLen)
		copy(p, prefixDictBytes[pdPos:pdPos+pLen])
		prefixes[i] = p
		pdPos += pLen
	}

	// suffix_data_len[4] + suffix_data_zstd — scratch reuse is safe here because
	// prefixDictBytes (first scratch use) is fully consumed above before this call.
	suffixBytes, _, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("prefix_bytes: suffix_data: %w", err)
	}

	// Parse suffix section:
	// prefix_index_width[1] + per present row: prefix_idx[piw bytes] + suffix_len[4] + suffix_bytes
	if len(suffixBytes) < 1 {
		return nil, fmt.Errorf("prefix_bytes: suffix_data too short for index width")
	}

	piw := int(suffixBytes[0])
	sPos := 1

	if piw != 1 && piw != 2 && piw != 4 {
		return nil, fmt.Errorf("prefix_bytes: invalid prefix_index_width %d", piw)
	}

	col.BytesInline = make([][]byte, spanCount)

	// NOTE-007: Acquire pooled scratch for present-row index list; defer release covers error paths.
	presRowsBuf := acquirePresentRowsScratch()
	defer releasePresentRowsScratch(presRowsBuf)
	presentRows := collectPresentRowsInto(present, presentCount, spanCount, presRowsBuf)
	for _, presentRow := range presentRows {
		if sPos+piw > len(suffixBytes) {
			return nil, fmt.Errorf("prefix_bytes: short at present row %d prefix_idx", presentRow)
		}

		var pidx uint32
		switch piw {
		case 1:
			pidx = uint32(suffixBytes[sPos])
		case 2:
			pidx = uint32(binary.LittleEndian.Uint16(suffixBytes[sPos:]))
		case 4:
			pidx = binary.LittleEndian.Uint32(suffixBytes[sPos:])
		}

		sPos += piw

		if sPos+4 > len(suffixBytes) {
			return nil, fmt.Errorf("prefix_bytes: short at present row %d suffix_len", presentRow)
		}

		sLen := int(binary.LittleEndian.Uint32(suffixBytes[sPos:]))
		sPos += 4

		if sPos+sLen > len(suffixBytes) {
			return nil, fmt.Errorf("prefix_bytes: suffix data overrun at present row %d", presentRow)
		}

		suffix := suffixBytes[sPos : sPos+sLen]
		sPos += sLen

		// noPrefix sentinel: 0xFFFFFFFF (or equivalent for smaller widths).
		noPrefix := uint32((1 << (uint(piw) * 8)) - 1) //nolint:gosec // safe: piw bounded to 1-4 bytes
		var value []byte
		if pidx == noPrefix || int(pidx) >= len(prefixes) {
			value = make([]byte, sLen)
			copy(value, suffix)
		} else {
			prefix := prefixes[pidx]
			value = make([]byte, len(prefix)+sLen)
			copy(value, prefix)
			copy(value[len(prefix):], suffix)
		}

		col.BytesInline[presentRow] = value
	}

	return col, nil
}

// decodeDeltaDictionary decodes kind 12/13 (DeltaDictionary/SparseDeltaDictionary).
// data starts after enc_version + kind bytes.
func decodeDeltaDictionary(data []byte, kind uint8, spanCount int, ctx *decodeCtx, allPresent bool) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 1 {
		return nil, fmt.Errorf("delta_dict: data too short")
	}

	// index_width[1] — present but unused for delta decoding.
	pos := 1

	// dict_len[4] + dict_zstd — decompressed into scratch; data extracted before next scratch use.
	dictBytes, newPos, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("delta_dict: dict: %w", err)
	}

	pos = newPos

	// Dictionary for delta_dict is always Bytes type.
	col.Type = shared.ColumnTypeBytes
	if err = decodeDictBody(dictBytes, col, ctx); err != nil {
		return nil, fmt.Errorf("delta_dict: dict body: %w", err)
	}

	// row_count[4]
	if pos+4 > len(data) {
		return nil, fmt.Errorf("delta_dict: missing row_count")
	}

	rowCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if rowCount != spanCount {
		return nil, fmt.Errorf("delta_dict: row_count %d != spanCount %d", rowCount, spanCount)
	}

	// presence_rle[4+N]
	present, newPos, presentCount, err := decodePresenceMaybe(data, pos, spanCount, allPresent)
	if err != nil {
		return nil, fmt.Errorf("delta_dict: %w", err)
	}

	pos = newPos
	col.Present = present

	// delta_len[4] + delta_data_zstd — scratch reuse is safe: dictBytes (first scratch use)
	// is fully consumed by decodeDictBody above before this call resets scratch.
	deltaBytes, _, err := readRawSegment(data, pos)
	if err != nil {
		return nil, fmt.Errorf("delta_dict: delta: %w", err)
	}

	// Determine how many delta values to expect.
	var nDeltas int
	switch kind {
	case shared.KindDeltaDictionary: // dense: one delta per row including nulls
		nDeltas = rowCount
	case shared.KindSparseDeltaDictionary: // sparse: one delta per present row
		nDeltas = presentCount
	}

	if len(deltaBytes) < nDeltas*4 {
		return nil, fmt.Errorf(
			"delta_dict: delta bytes: need %d bytes for %d deltas, got %d",
			nDeltas*4, nDeltas, len(deltaBytes),
		)
	}

	// Decode delta indexes.
	denseIdx := make([]uint32, spanCount)
	dictSize := len(col.BytesDict)
	var prev int32

	switch kind {
	case shared.KindDeltaDictionary: // dense
		for i := range rowCount {
			delta := int32(binary.LittleEndian.Uint32(deltaBytes[i*4:])) //nolint:gosec
			prev += delta
			if prev < 0 || int(prev) >= dictSize {
				return nil, fmt.Errorf(
					"delta_dict(dense): index %d out of range [0, %d)",
					prev, dictSize,
				)
			}

			denseIdx[i] = uint32(prev) //nolint:gosec
		}

	case shared.KindSparseDeltaDictionary: // sparse
		si := 0
		for i := range spanCount {
			if !shared.IsPresent(present, i) {
				continue
			}

			if si >= nDeltas {
				break
			}

			delta := int32(binary.LittleEndian.Uint32(deltaBytes[si*4:])) //nolint:gosec
			prev += delta
			if prev < 0 || int(prev) >= dictSize {
				return nil, fmt.Errorf(
					"delta_dict(sparse): index %d out of range [0, %d)",
					prev, dictSize,
				)
			}

			denseIdx[i] = uint32(prev) //nolint:gosec
			si++
		}
	}

	col.BytesIdx = denseIdx
	return col, nil
}

// decodeVectorF32 decodes a ColumnTypeVectorF32 column.
// data is the raw column blob (output of vectorF32ColumnBuilder.buildData):
//
//	enc_version[1] + kind[1] + dim[2 LE] + row_count[4 LE] +
//	presence_rle_len[4 LE] + presence_rle[N] +
//	float_data_compressed_len[4 LE] + zstd(flat_float32_LE[present_count * dim * 4])
//
// Returns a Column with BytesInline populated: BytesInline[i] holds the raw LE float32 bytes
// for present row i (length = dim*4). BytesInline[i] is nil for absent rows.
// NOTE-011 (shared/NOTES.md): ColumnTypeVectorF32 encoding — only float data is zstd-compressed.
func decodeVectorF32(data []byte, spanCount int, ctx *decodeCtx) (*Column, error) {
	// Header: enc_version[1] + kind[1] + dim[2] + row_count[4] + rle_len[4] = 12 bytes minimum.
	if len(data) < vf32HdrSize {
		return nil, fmt.Errorf("vectorF32: data too short: %d bytes", len(data))
	}

	dim := int(binary.LittleEndian.Uint16(data[2:4]))
	rowCount := int(binary.LittleEndian.Uint32(data[4:8]))
	rleLen := int(binary.LittleEndian.Uint32(data[vf32RleLenOff : vf32RleLenOff+4]))

	if rowCount != spanCount {
		return nil, fmt.Errorf("vectorF32: row_count %d != spanCount %d", rowCount, spanCount)
	}

	off := vf32HdrSize
	if off+rleLen > len(data) {
		return nil, fmt.Errorf("vectorF32: presence RLE truncated: need %d bytes at offset %d", rleLen, off)
	}
	rleData := data[off : off+rleLen]
	off += rleLen

	// Decode presence bitset.
	bitset, err := shared.DecodePresenceRLE(rleData, spanCount)
	if err != nil {
		return nil, fmt.Errorf("vectorF32: presence RLE: %w", err)
	}

	// SPEC-ROOT-012: compute needed size before decompression to prevent decompression bomb.
	presentCount := shared.CountPresent(bitset, spanCount)
	needed := int64(presentCount) * int64(dim) * 4
	if needed > shared.MaxBlockSize {
		return nil, fmt.Errorf(
			"vectorF32: float data would exceed MaxBlockSize: needed=%d, dim=%d, presentCount=%d",
			needed, dim, presentCount,
		)
	}

	// Read length-prefixed zstd float data (size already validated above).
	// Use ctx.scratch if set (pooled caller), else use a local scratch buffer.
	scratchPtr := ctx.scratch
	var localScratch []byte
	if scratchPtr == nil {
		scratchPtr = &localScratch
	}
	floatBytes, _, err := decompressZstdScratch(data, off, scratchPtr)
	if err != nil {
		return nil, fmt.Errorf("vectorF32: float data: %w", err)
	}
	if len(floatBytes) < int(needed) {
		return nil, fmt.Errorf("vectorF32: float data too short: need %d bytes, have %d", needed, len(floatBytes))
	}

	col := &Column{
		SpanCount:   spanCount,
		Present:     bitset,
		BytesInline: make([][]byte, spanCount),
		Type:        shared.ColumnTypeVectorF32,
	}

	presentRow := 0
	for i := range spanCount {
		if !shared.IsPresent(bitset, i) {
			continue
		}
		vecStart := int64(presentRow) * int64(dim) * 4
		raw := make([]byte, dim*4)
		copy(raw, floatBytes[vecStart:vecStart+int64(dim)*4])
		col.BytesInline[i] = raw
		presentRow++
	}

	return col, nil
}

// Column is a blockpack data type.
type Column struct {
	internMap map[string]string
	Name      string
	// NOTE-201: process-cache key for the lazy (deferred) decode path. Populated by the
	// lazy-registration loop in parseBlockColumnsReuse when a stable fileID is available.
	// decodeNow consults parsedV8ColumnCache on this key before doing snappy+readColumnEncoding
	// and stores a snapshot on miss, extending NOTE-200's eager-loop reuse to first-access decode.
	// Empty when no stable key is available (no fileID) — decode proceeds without cache.
	v8CacheKey  string
	StringDict  []string
	StringIdx   []uint32
	Int64Dict   []int64
	Int64Idx    []uint32
	Uint64Dict  []uint64
	Uint64Idx   []uint32
	Float64Dict []float64
	Float64Idx  []uint32
	BoolDict    []uint8
	BoolIdx     []uint32
	BytesDict   [][]byte
	BytesIdx    []uint32
	BytesInline [][]byte
	// NOTE-351: uniform-stride inline bytes. When uniformStride > 0 the column's inline
	// values are uniform-length (XOR/inline uniform kinds, e.g. 16-byte trace:id/span:id)
	// and are stored row-indexed in one contiguous slab (uniformSlab) instead of a
	// [][]byte header array. Row idx occupies uniformSlab[idx*uniformStride:(idx+1)*…].
	// This eliminates the spanCount*24-byte [][]byte backing array — pure overhead that
	// was ~1.5x the actual data for 16-byte IDs and the dominant retained footprint of the
	// decodeXORBytesUniform / decodeInlineBytesUniform paths (top querier inuse_space frame).
	// Reads go through bytesInlineAt(idx); BytesInline stays nil for uniform-stride columns.
	uniformSlab        []byte
	Present            []byte
	rawEncoding        []byte
	compressedEncoding []byte
	// NOTE-209: pool handle for the transient lazy-decode decompression buffer. When the
	// lazy path decompresses compressedEncoding it draws the destination buffer from
	// decompBufPool; decodeNow returns it to the pool after readColumnEncoding has copied
	// every decoded slice out (the documented "decoders copy data out" invariant), so the
	// per-lazy-column snappy.Decode(nil, …) allocation is eliminated on the warm scan path.
	// nil when decompression allocated outside the pool (e.g. pool buffer too small was grown)
	// or the column was not decompressed via the pooled path.
	decompPooledPtr *[]byte
	sparseDictIdx   []uint32
	// NOTE-369: packed native-width dictionary index. A dense dictionary column with
	// indexWidth 1 or 2 (dict cardinality <= 256 / <= 65536 — the common low-cardinality
	// case for attribute/intrinsic columns) previously materialized a []uint32 *Idx slice,
	// expanding each on-disk index 4x (width 1) or 2x (width 2). The realworld profile
	// showed readIndexArray retaining ~800 MB of these uint32 index arrays (mostly width-2
	// columns, a 2x blowup). When packedIdxWidth != 0 the type-specific *Idx slice is left
	// nil and the dict index for row i is read at native width from packedIdx via
	// dictIdxAt — packedIdx[i] (width 1) or LE-uint16 at packedIdx[2i:] (width 2). The
	// snapshot stored in parsedV8ColumnCache (NOTE-200) shares this compact slice, so the
	// retained footprint is the on-disk width, not 4 bytes/row. Width-4 columns keep the
	// []uint32 path (no blowup to remove). Only the all-present dense dict path uses this;
	// sparse / denseFlatIdx (NOTE-358) are unchanged.
	packedIdx       []byte
	SpanCount       int
	decodeOnce      sync.Once
	denseOnce       sync.Once
	decompressOnce  sync.Once
	decoded         atomic.Bool
	uncompressedLen uint32
	uniformStride   uint32 // NOTE-351: per-row stride into uniformSlab; 0 = not uniform-stride
	Type            shared.ColumnType
	// NOTE-369: native width (1 or 2 bytes/index) of packedIdx; 0 = no packed index
	// (use the materialized *Idx slice or denseFlatIdx).
	packedIdxWidth uint8
	// NOTE-358: denseFlatIdx marks a fully-present dense-flat numeric column whose dict
	// index for every row is the row index itself (Dict has one entry per present row, in
	// row order; all-present ⇒ present-rank(i) == i). For these columns the *Idx slice is a
	// pure identity permutation [0,1,2,…,SpanCount-1] — 4 bytes/row of redundant retained
	// memory (both per-query AND in the parsedV8ColumnCache snapshot). The bit-packed delta
	// (decodeDeltaUint64BitPacked) and Gorilla-float64 (decodeGorillaFloat64) decoders set
	// this flag instead of materializing the identity slice; readers resolve the dict index
	// arithmetically via dictIdxAt. Mirrors NOTE-354's flat-dense refIndex drop. Only the
	// all-present case is covered: a partial-presence column needs a real rank table, so it
	// keeps the materialized *Idx slice (denseFlatIdx == false).
	denseFlatIdx bool
}

// IsDenseFlatIdx reports whether this column uses the NOTE-358 identity-index optimization
// (all-present dense-flat numeric column with no materialized *Idx slice). Cross-package
// scan helpers (executor) consult this so they can resolve the dict index as the row index
// instead of reading the (nil) *Idx slice.
func (c *Column) IsDenseFlatIdx() bool { return c.denseFlatIdx }

// DictIdxReader returns a per-row dict-index resolver for cross-package bulk scan loops
// (executor), with the storage branch (denseFlatIdx / packed native width / materialized
// []uint32) hoisted out of the per-row loop. idx is the type-correct exported *Idx slice
// the caller selects (e.g. col.Uint64Idx); it is used only for the materialized-uint32
// fallback. The resolver returns -1 for an out-of-range row. NOTE-369.
func (c *Column) DictIdxReader(idx []uint32) func(row int) int {
	return c.packedIdxReader(idx)
}

// HasDictIdx reports whether the column exposes a usable per-row dict index via DictIdxReader
// — a packed native-width slice (NOTE-369), a materialized *Idx slice, or the denseFlatIdx
// identity (NOTE-358). idx is the type-correct *Idx slice the caller would pass to
// DictIdxReader. NOTE-369.
func (c *Column) HasDictIdx(idx []uint32) bool {
	return c.hasMaterializedIdx(idx)
}

// dictIdxAt returns the dictionary index for row idx and whether a dict lookup applies.
// For a denseFlatIdx column (NOTE-358, all-present dense-flat) the index is the row index
// itself, so no materialized *Idx slice is consulted. Otherwise it reads idx[row] from the
// passed materialized index slice (caller selects the type-correct slice). Returns ok=false
// when row is out of range for a materialized slice.
func (c *Column) dictIdxAt(idx []uint32, row int) (int, bool) {
	if c.denseFlatIdx {
		return row, true
	}
	// NOTE-369: packed native-width index supersedes the (nil) *Idx slice when set.
	if c.packedIdxWidth != 0 {
		return c.packedIdxAt(row)
	}
	if row < 0 || row >= len(idx) {
		return 0, false
	}
	return int(idx[row]), true
}

// packedIdxAt returns the dict index for row from the NOTE-369 packed native-width
// representation. Returns ok=false for an out-of-range row. The width branch is on a
// per-column field; bulk scan loops should hoist it via packedIdxReader instead of
// calling this per row.
func (c *Column) packedIdxAt(row int) (int, bool) {
	if row < 0 {
		return 0, false
	}
	switch c.packedIdxWidth {
	case 1:
		if row >= len(c.packedIdx) {
			return 0, false
		}
		return int(c.packedIdx[row]), true
	case 2:
		off := row * 2
		if off+2 > len(c.packedIdx) {
			return 0, false
		}
		return int(binary.LittleEndian.Uint16(c.packedIdx[off:])), true
	default:
		return 0, false
	}
}

// hasMaterializedIdx reports whether the column has a usable per-row dict index — either
// a packed native-width slice (NOTE-369), a materialized *Idx slice, or the denseFlatIdx
// identity (NOTE-358). idx is the type-correct *Idx slice the caller would otherwise read.
func (c *Column) hasMaterializedIdx(idx []uint32) bool {
	return c.denseFlatIdx || c.packedIdxWidth != 0 || idx != nil
}

// packedIdxReader returns a closure resolving the dict index for a row, with the storage
// branch hoisted out of the per-row loop. Bulk scan loops (executor) use this so the width
// decision is made once per column, not once per row — matching the existing denseFlat
// hoist. The returned function assumes row is in range (callers bound by SpanCount and
// check presence first). NOTE-369.
func (c *Column) packedIdxReader(idx []uint32) func(row int) int {
	switch {
	case c.denseFlatIdx:
		return func(row int) int { return row }
	case c.packedIdxWidth == 1:
		p := c.packedIdx
		return func(row int) int {
			if row >= len(p) {
				return -1
			}
			return int(p[row])
		}
	case c.packedIdxWidth == 2:
		p := c.packedIdx
		return func(row int) int {
			off := row * 2
			if off+2 > len(p) {
				return -1
			}
			return int(binary.LittleEndian.Uint16(p[off:]))
		}
	default:
		return func(row int) int {
			if row >= len(idx) {
				return -1
			}
			return int(idx[row])
		}
	}
}

// bytesInlineAt returns the inline bytes for row idx, transparently serving uniform-stride
// columns (NOTE-351) from the contiguous uniformSlab and all other columns from the
// BytesInline header array. Returns nil for absent / out-of-range rows. The returned slice
// aliases the column's backing storage and must not be mutated by callers.
func (c *Column) bytesInlineAt(idx int) []byte {
	if c.uniformStride != 0 {
		stride := int(c.uniformStride)
		off := idx * stride
		if idx < 0 || off+stride > len(c.uniformSlab) {
			return nil
		}
		if c.Present != nil && !shared.IsPresent(c.Present, idx) {
			return nil
		}
		return c.uniformSlab[off : off+stride : off+stride]
	}
	if idx < 0 || idx >= len(c.BytesInline) {
		return nil
	}
	return c.BytesInline[idx]
}

// hasInlineBytes reports whether the column carries inline bytes (either representation).
func (c *Column) hasInlineBytes() bool {
	return c.BytesInline != nil || c.uniformSlab != nil
}
