package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/klauspost/compress/zstd"
)

// zstdDecoder is a package-level zstd decoder shared across calls.
var (
	zstdDecoderOnce sync.Once
	zstdDec         *zstd.Decoder
)

// decompScratchPool holds reusable scratch buffers for zstd decompression during
// block parsing. A single scratch buffer is acquired per parseBlockColumnsReuse call
// and reused across all column decodings within that block.
//
// INVARIANT: callers MUST NOT hold references to bytes returned by decompressZstdScratch
// after calling decompressZstdScratch again with the same scratch pointer. All data
// must be extracted into typed Go values (or copied) before the next call.
var decompScratchPool = &sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		b := make([]byte, 0, 256*1024)
		return &b
	},
}

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

// AcquireInternMap returns a pooled intern map, cleared and ready for use.
func AcquireInternMap() *map[string]string {
	return internMapPool.Get().(*map[string]string)
}

// ReleaseInternMap clears the map and returns it to the pool.
func ReleaseInternMap(mp *map[string]string) {
	clear(*mp)
	internMapPool.Put(mp)
}

// presentRowsScratchPool holds reusable []int scratch slices for collectPresentRowsInto.
// Each call to decodeXORBytes / decodePrefixBytes acquires one slice, resets it to [:0],
// appends present-row indices, then releases it back after the per-row decode loop.
//
// NOTE-007: Eliminates per-call make([]int, 0, presentCount) in collectPresentRows.
// Cap guard: slices larger than 65536 entries are replaced with a fresh small slice before
// pool return to avoid retaining large backing arrays indefinitely.
var presentRowsScratchPool = &sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		s := make([]int, 0, 2048)
		return &s
	},
}

func acquirePresentRowsScratch() *[]int {
	return presentRowsScratchPool.Get().(*[]int)
}

func releasePresentRowsScratch(sp *[]int) {
	if cap(*sp) > 65536 {
		// Replace the oversized backing array with a fresh small slice.
		// Assign through the pointer so the pool receives the new small slice,
		// not a pointer to a local stack variable.
		*sp = make([]int, 0, 2048)
	} else {
		*sp = (*sp)[:0]
	}
	presentRowsScratchPool.Put(sp)
}

func getZstdDecoder() *zstd.Decoder {
	zstdDecoderOnce.Do(func() {
		var err error
		zstdDec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
		if err != nil {
			panic(fmt.Sprintf("reader: zstd.NewReader: %v", err))
		}
	})

	return zstdDec
}

func acquireDecompScratch() *[]byte {
	return decompScratchPool.Get().(*[]byte)
}

func releaseDecompScratch(s *[]byte) {
	*s = (*s)[:0]
	decompScratchPool.Put(s)
}

// decompressZstdScratch decompresses a zstd-compressed segment into scratch,
// reusing the backing array across calls. The returned slice is valid only until
// the next call to decompressZstdScratch with the same scratch pointer.
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

// decodeCtx bundles per-parse and per-reader state threaded through column decoders.
// scratch is acquired from decompScratchPool for each block parse and must not be nil.
// intern is the owning Reader's string intern table and must not be nil.
type decodeCtx struct {
	scratch *[]byte           // pooled zstd output buffer, reused across columns in one block
	intern  map[string]string // per-reader string intern map, single-goroutine only
}

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
func decodePresenceRLEFromSlice(data []byte, pos int, nBits int) ([]byte, int, int, error) {
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

// readIndexArray reads count index values of indexWidth bytes each from data[pos:].
// indexWidth must be 1, 2, or 4.
func readIndexArray(data []byte, pos int, count int, indexWidth uint8) ([]uint32, int, error) {
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

// readColumnEncoding reads enc_version[1] + encoding_kind[1] then dispatches.
// colType is passed through so dictionary decoders can populate the correct typed fields.
// ctx carries the pooled decompression scratch buffer and the per-reader intern table.
func readColumnEncoding(data []byte, spanCount int, colType shared.ColumnType, ctx *decodeCtx) (*Column, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("column encoding: data too short (%d bytes)", len(data))
	}

	// NOTE-010 (shared/NOTES.md): ColumnTypeVectorF32 uses a custom wire format where
	// only the float data is zstd-compressed. Dispatch by colType so decodeVectorF32 can
	// handle the mixed compressed/uncompressed format directly.
	if colType == shared.ColumnTypeVectorF32 {
		return decodeVectorF32(data, spanCount, ctx)
	}

	encVersion := data[0]
	if encVersion != shared.ColumnEncodingVersion {
		return nil, fmt.Errorf("column encoding: unsupported version %d", encVersion)
	}

	kind := data[1]

	switch kind {
	case 1, 2:
		return decodeDictionary(data[2:], kind, spanCount, colType, ctx)
	case 3, 4:
		return decodeInlineBytes(data[2:], kind, spanCount)
	case 5:
		return decodeDeltaUint64(data[2:], spanCount, ctx)
	case 6, 7:
		return decodeRLEIndexes(data[2:], kind, spanCount, colType, ctx)
	case 8, 9:
		return decodeXORBytes(data[2:], kind, spanCount, ctx)
	case 10, 11:
		return decodePrefixBytes(data[2:], kind, spanCount, ctx)
	case 12, 13:
		return decodeDeltaDictionary(data[2:], kind, spanCount, ctx)
	default:
		return nil, fmt.Errorf("column encoding: unknown kind %d", kind)
	}
}

// decodePresenceOnly skips past compressed blobs using length prefixes and decodes
// only the uncompressed presence RLE. Called once per column during lazy registration.
// No zstd decompression — O(M/8) where M is span count.
// NOTE-001: lazy registration path — presence available immediately, values deferred.
// NOTE-010 (shared/NOTES.md): ColumnTypeVectorF32 columns are fully zstd-compressed;
// colType is required to dispatch correctly before reading enc_version.
func decodePresenceOnly(data []byte, spanCount int, colType shared.ColumnType) ([]byte, error) {
	// ColumnTypeVectorF32: the blob starts with enc_version[1]+kind[1]+dim[2]+row_count[4]+rle_len[4]+rle.
	// Presence RLE is stored uncompressed (only float data is zstd-compressed).
	// NOTE-010 (shared/NOTES.md): presence is in the uncompressed header region.
	if colType == shared.ColumnTypeVectorF32 {
		const hdrSize = 1 + 1 + 2 + 4 + 4 // enc_version+kind+dim+row_count+rle_len
		if len(data) < hdrSize {
			return nil, fmt.Errorf("decodePresenceOnly(vectorF32): data too short: %d", len(data))
		}
		rleLen := int(binary.LittleEndian.Uint32(data[8:12]))
		if hdrSize+rleLen > len(data) {
			return nil, fmt.Errorf("decodePresenceOnly(vectorF32): rle truncated")
		}
		present, err := shared.DecodePresenceRLE(data[hdrSize:hdrSize+rleLen], spanCount)
		if err != nil {
			return nil, fmt.Errorf("decodePresenceOnly(vectorF32): %w", err)
		}
		return present, nil
	}

	if len(data) < 2 {
		return nil, fmt.Errorf("decodePresenceOnly: data too short (%d bytes)", len(data))
	}

	encVersion := data[0]
	if encVersion != shared.ColumnEncodingVersion {
		return nil, fmt.Errorf("decodePresenceOnly: unsupported enc_version %d", encVersion)
	}

	kind := data[1]
	d := data[2:]
	pos := 0

	switch kind {
	case 1, 2, 6, 7, 12, 13:
		// dictionary kinds: index_width[1] + dict_len[4] + dict_data[N] + row_count[4]
		if pos+1 > len(d) {
			return nil, fmt.Errorf("decodePresenceOnly(kind=%d): need index_width byte", kind)
		}

		pos++ // skip index_width

		if pos+4 > len(d) {
			return nil, fmt.Errorf("decodePresenceOnly(kind=%d): need dict_len", kind)
		}

		dictLen := int(binary.LittleEndian.Uint32(d[pos:]))
		pos += 4 + dictLen // skip compressed dict bytes

		if pos+4 > len(d) {
			return nil, fmt.Errorf("decodePresenceOnly(kind=%d): need row_count", kind)
		}

		pos += 4 // skip row_count

	case 3, 4, 8, 9, 10, 11:
		// kinds with span_count[4] prefix
		if pos+4 > len(d) {
			return nil, fmt.Errorf("decodePresenceOnly(kind=%d): need span_count", kind)
		}

		pos += 4

	case 5:
		// delta_uint64: span_count[4]
		if pos+4 > len(d) {
			return nil, fmt.Errorf("decodePresenceOnly(kind=5): need span_count")
		}

		pos += 4

	default:
		return nil, fmt.Errorf("decodePresenceOnly: unknown kind %d", kind)
	}

	present, _, _, err := decodePresenceRLEFromSlice(d, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("decodePresenceOnly(kind=%d): %w", kind, err)
	}

	return present, nil
}

// decodeNow performs full decode of this column from rawEncoding.
// Called on first StringValue/Int64Value/Uint64Value/Float64Value/BoolValue/BytesValue access.
// decodeOnce ensures at most one goroutine decodes; concurrent callers block until done.
// rawEncoding is nil after this call regardless of decode outcome.
func (c *Column) decodeNow() {
	if c.rawEncoding == nil {
		return // already decoded or never registered lazily
	}
	c.decodeOnce.Do(func() {
		if c.rawEncoding == nil {
			return
		}
		scratch := acquireDecompScratch()
		ctx := &decodeCtx{scratch: scratch, intern: c.internMap}
		decoded, err := readColumnEncoding(c.rawEncoding, c.SpanCount, c.Type, ctx)
		releaseDecompScratch(scratch)

		if err != nil {
			c.rawEncoding = nil
			c.internMap = nil
			return
		}

		if c.Present == nil {
			c.Present = decoded.Present
		}
		c.StringDict = decoded.StringDict
		c.StringIdx = decoded.StringIdx
		c.Int64Dict = decoded.Int64Dict
		c.Int64Idx = decoded.Int64Idx
		c.Uint64Dict = decoded.Uint64Dict
		c.Uint64Idx = decoded.Uint64Idx
		c.Float64Dict = decoded.Float64Dict
		c.Float64Idx = decoded.Float64Idx
		c.BoolDict = decoded.BoolDict
		c.BoolIdx = decoded.BoolIdx
		c.BytesDict = decoded.BytesDict
		c.BytesIdx = decoded.BytesIdx
		c.BytesInline = decoded.BytesInline
		c.sparseDictIdx = decoded.sparseDictIdx

		c.rawEncoding = nil
		c.internMap = nil
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
		col.Int64Dict = make([]int64, 0, entryCnt)
		for range entryCnt {
			if pos+8 > len(dictBytes) {
				return fmt.Errorf("dict body(int64): short at entry")
			}

			v := int64(binary.LittleEndian.Uint64(dictBytes[pos:])) //nolint:gosec
			pos += 8
			col.Int64Dict = append(col.Int64Dict, v)
		}

	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		col.Uint64Dict = make([]uint64, 0, entryCnt)
		for range entryCnt {
			if pos+8 > len(dictBytes) {
				return fmt.Errorf("dict body(uint64): short at entry")
			}

			v := binary.LittleEndian.Uint64(dictBytes[pos:])
			pos += 8
			col.Uint64Dict = append(col.Uint64Dict, v)
		}

	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		col.Float64Dict = make([]float64, 0, entryCnt)
		for range entryCnt {
			if pos+8 > len(dictBytes) {
				return fmt.Errorf("dict body(float64): short at entry")
			}

			bits := binary.LittleEndian.Uint64(dictBytes[pos:])
			pos += 8
			col.Float64Dict = append(col.Float64Dict, math.Float64frombits(bits))
		}

	case shared.ColumnTypeBool:
		col.BoolDict = make([]uint8, 0, entryCnt)
		for range entryCnt {
			if pos+1 > len(dictBytes) {
				return fmt.Errorf("dict body(bool): short at entry")
			}

			col.BoolDict = append(col.BoolDict, dictBytes[pos])
			pos++
		}

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes, shared.ColumnTypeUUID:
		col.BytesDict = make([][]byte, 0, entryCnt)
		for range entryCnt {
			if pos+4 > len(dictBytes) {
				return fmt.Errorf("dict body(bytes): short at entry")
			}

			bLen := int(binary.LittleEndian.Uint32(dictBytes[pos:]))
			pos += 4
			if pos+bLen > len(dictBytes) {
				return fmt.Errorf("dict body(bytes): data overrun")
			}

			b := make([]byte, bLen)
			copy(b, dictBytes[pos:pos+bLen])
			col.BytesDict = append(col.BytesDict, b)
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
) (*Column, error) {
	col := &Column{SpanCount: spanCount, Type: colType}

	if len(data) < 1 {
		return nil, fmt.Errorf("dictionary: data too short")
	}

	indexWidth := data[0]
	pos := 1

	// dict_len[4] + dict_zstd — decompressed into scratch; data extracted before next scratch use.
	dictBytes, newPos, err := decompressZstdScratch(data, pos, ctx.scratch)
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
	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("dictionary: %w", err)
	}

	pos = newPos
	col.Present = present

	// indexes
	switch kind {
	case 1: // dense: rowCount indexes
		idx, _, err := readIndexArray(data, pos, rowCount, indexWidth)
		if err != nil {
			return nil, fmt.Errorf("dictionary(dense): %w", err)
		}

		assignDictIdx(col, idx)

	case 2: // sparse: present_count[4] + presentCount indexes
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
func decodeInlineBytes(data []byte, kind uint8, spanCount int) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("inline_bytes: data too short")
	}

	rowCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if rowCount != spanCount {
		return nil, fmt.Errorf("inline_bytes: row_count %d != spanCount %d", rowCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("inline_bytes: %w", err)
	}

	pos = newPos
	col.Present = present

	// For inline bytes we need a dense slice: nil for absent rows.
	col.BytesInline = make([][]byte, spanCount)

	switch kind {
	case 3: // dense: rowCount × {len[4] + bytes}
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

	case 4: // sparse: present_count[4] + presentCount × {len[4] + bytes}
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
func decodeDeltaUint64(data []byte, spanCount int, ctx *decodeCtx) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("delta_uint64: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("delta_uint64: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
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

	// Build dense Uint64Dict + Uint64Idx.
	col.Uint64Dict = make([]uint64, presentCount)
	col.Uint64Idx = make([]uint32, spanCount)

	if width == 0 {
		// All present values equal base.
		for i := range presentCount {
			col.Uint64Dict[i] = base
		}
	} else {
		// Read compressed offset array — decompressed into scratch; values extracted before next scratch use.
		offsetBytes, _, err := decompressZstdScratch(data, pos, ctx.scratch)
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
	dictIdx := 0
	for i := range spanCount {
		if shared.IsPresent(present, i) {
			col.Uint64Idx[i] = uint32(dictIdx) //nolint:gosec
			dictIdx++
		}
	}

	return col, nil
}

// decodeRLEIndexes decodes kind 6/7 (RLEIndexes/SparseRLEIndexes).
// data starts after enc_version + kind bytes.
func decodeRLEIndexes(
	data []byte,
	kind uint8,
	spanCount int,
	colType shared.ColumnType,
	ctx *decodeCtx,
) (*Column, error) {
	col := &Column{SpanCount: spanCount, Type: colType}

	if len(data) < 1 {
		return nil, fmt.Errorf("rle_indexes: data too short")
	}

	_ = data[0] // index_width: present in wire format, not needed for RLE decode
	pos := 1

	// dict_len[4] + dict_zstd — decompressed into scratch; data extracted before next scratch use.
	dictBytes, newPos, err := decompressZstdScratch(data, pos, ctx.scratch)
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
	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
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
	case 6: // dense
		if indexCount != spanCount {
			return nil, fmt.Errorf("rle_indexes(dense): index_count %d != spanCount %d", indexCount, spanCount)
		}

		denseIdx = sparseIdx

	case 7: // sparse
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
func decodeXORBytes(data []byte, kind uint8, spanCount int, ctx *decodeCtx) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("xor_bytes: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("xor_bytes: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("xor_bytes: %w", err)
	}

	pos = newPos
	col.Present = present
	_ = kind // sparse/dense distinction handled entirely by presence bitset

	// xor_len[4] + xor_data_zstd — decompressed into scratch; each row value is XOR-decoded
	// into a fresh make([]byte,...) before the next scratch use, so scratch is not retained.
	xorBytes, _, err := decompressZstdScratch(data, pos, ctx.scratch)
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

// collectPresentRowsInto appends row indices where the present bit is set into *buf.
// *buf is reset to [:0] before use; the caller owns buf and must release it to the pool.
//
// NOTE-007: Replaces collectPresentRows; eliminates per-call make([]int) allocation by
// reusing a pooled scratch slice. The presentCount parameter is unused for pre-sizing
// (the slice is pre-allocated by the pool) but retained for documentation clarity.
func collectPresentRowsInto(present []byte, _ /*presentCount*/, spanCount int, buf *[]int) []int {
	*buf = (*buf)[:0]
	for i := range spanCount {
		if shared.IsPresent(present, i) {
			*buf = append(*buf, i)
		}
	}

	return *buf
}

// decodePrefixBytes decodes kind 10/11 (PrefixBytes/SparsePrefixBytes).
// data starts after enc_version + kind bytes.
func decodePrefixBytes(data []byte, kind uint8, spanCount int, ctx *decodeCtx) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 4 {
		return nil, fmt.Errorf("prefix_bytes: data too short")
	}

	storedSpanCount := int(binary.LittleEndian.Uint32(data[0:]))
	pos := 4

	if storedSpanCount != spanCount {
		return nil, fmt.Errorf("prefix_bytes: span_count %d != spanCount %d", storedSpanCount, spanCount)
	}

	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("prefix_bytes: %w", err)
	}

	pos = newPos
	col.Present = present
	_ = kind // sparse/dense handled by presence bitset

	// prefix_dict_len[4] + prefix_dict_zstd — decompressed into scratch; all prefix data
	// is copy()-d into fresh allocations before the next scratch use (suffix decompression).
	prefixDictBytes, newPos, err := decompressZstdScratch(data, pos, ctx.scratch)
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
	suffixBytes, _, err := decompressZstdScratch(data, pos, ctx.scratch)
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
func decodeDeltaDictionary(data []byte, kind uint8, spanCount int, ctx *decodeCtx) (*Column, error) {
	col := &Column{SpanCount: spanCount}

	if len(data) < 1 {
		return nil, fmt.Errorf("delta_dict: data too short")
	}

	// index_width[1] — present but unused for delta decoding.
	pos := 1

	// dict_len[4] + dict_zstd — decompressed into scratch; data extracted before next scratch use.
	dictBytes, newPos, err := decompressZstdScratch(data, pos, ctx.scratch)
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
	present, newPos, presentCount, err := decodePresenceRLEFromSlice(data, pos, spanCount)
	if err != nil {
		return nil, fmt.Errorf("delta_dict: %w", err)
	}

	pos = newPos
	col.Present = present

	// delta_len[4] + delta_data_zstd — scratch reuse is safe: dictBytes (first scratch use)
	// is fully consumed by decodeDictBody above before this call resets scratch.
	deltaBytes, _, err := decompressZstdScratch(data, pos, ctx.scratch)
	if err != nil {
		return nil, fmt.Errorf("delta_dict: delta: %w", err)
	}

	// Determine how many delta values to expect.
	var nDeltas int
	switch kind {
	case 12: // dense: one delta per row including nulls
		nDeltas = rowCount
	case 13: // sparse: one delta per present row
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
	case 12: // dense
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

	case 13: // sparse
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
// NOTE-010 (shared/NOTES.md): ColumnTypeVectorF32 encoding — only float data is zstd-compressed.
func decodeVectorF32(data []byte, spanCount int, ctx *decodeCtx) (*Column, error) {
	// Header: enc_version[1] + kind[1] + dim[2] + row_count[4] + rle_len[4] = 12 bytes minimum.
	const hdrSize = 1 + 1 + 2 + 4 + 4
	if len(data) < hdrSize {
		return nil, fmt.Errorf("vectorF32: data too short: %d bytes", len(data))
	}

	dim := int(binary.LittleEndian.Uint16(data[2:4]))
	rowCount := int(binary.LittleEndian.Uint32(data[4:8]))
	rleLen := int(binary.LittleEndian.Uint32(data[8:12]))

	if rowCount != spanCount {
		return nil, fmt.Errorf("vectorF32: row_count %d != spanCount %d", rowCount, spanCount)
	}

	off := hdrSize
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

	// Read length-prefixed zstd float data.
	floatBytes, _, err := decompressZstdScratch(data, off, ctx.scratch)
	if err != nil {
		return nil, fmt.Errorf("vectorF32: float data: %w", err)
	}

	presentCount := shared.CountPresent(bitset, spanCount)
	needed := presentCount * dim * 4
	if len(floatBytes) < needed {
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
		vecStart := presentRow * dim * 4
		raw := make([]byte, dim*4)
		copy(raw, floatBytes[vecStart:vecStart+dim*4])
		col.BytesInline[i] = raw
		presentRow++
	}

	return col, nil
}
