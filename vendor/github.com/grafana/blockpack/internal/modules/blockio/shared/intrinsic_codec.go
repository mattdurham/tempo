package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides the decode-side of the intrinsic columns wire format.
// Placed in shared (rather than reader or writer) to break the writer→reader import cycle:
//   writer → shared  (OK)
//   reader → shared  (OK)
//   reader ↛ writer  (would be cyclic since writer → reader)

import (
	"bytes"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"unsafe"

	// NOTE-283: github.com/klauspost/compress/snappy is an API-compatible drop-in for
	// github.com/golang/snappy whose Decode dispatches to the s2 assembly decoder
	// (optimized amd64/arm64 routines). The 2026-06-13 querier CPU profile showed
	// snappy.Decode at ~10% inclusive / ~5% self — the single largest blockpack-attributable
	// CPU sink — driven by per-page paged-column decompression on the metrics group-by and
	// search predicate-scan hot paths. Microbenchmark on representative column-page sizes:
	// 64KiB +24%, 256KiB +14%, 1MiB +9% decode throughput vs golang/snappy, 0 allocs both.
	// Both packages implement the standard Snappy BLOCK format (not the stream/framing
	// format), so blocks written by one decode losslessly with the other (verified
	// round-trip both directions) — no on-disk format change, fully backward/forward
	// compatible with already-written blocks. Decode's dst-reuse contract is identical
	// (reuses dst when DecodedLen <= cap(dst), else allocates), so snappyDecodeReuse's
	// NOTE-262 in-place reuse assumption is preserved.
	"github.com/klauspost/compress/snappy"
)

// decodeBoundedSnappyColumn snappy-decodes compressed, rejecting inputs whose
// decoded size would exceed MaxBlockSize (decompression-bomb guard for column blobs).
func decodeBoundedSnappyColumn(compressed []byte) ([]byte, error) {
	decodedLen, lenErr := snappy.DecodedLen(compressed)
	if lenErr != nil {
		return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
	}
	if decodedLen > MaxBlockSize {
		return nil, fmt.Errorf("snappy decoded size %d exceeds MaxBlockSize %d", decodedLen, MaxBlockSize)
	}
	return snappy.Decode(nil, compressed)
}

// NOTE-012: Snappy decode buffer pool — 64KB default cap, 4MB cap guard.
// AcquireIntrinsicBuf / ReleaseIntrinsicBuf are used in decodePagedColumnBlob
// to reuse decode scratch buffers across calls, avoiding per-page allocations
// in the common case where page blobs are ≤64KB.
// BytesValues returned from DecodeFlatPage / decodeLegacyFlatBlob must be copied
// (make+copy) before appending to IntrinsicColumn so the pool buffer can be safely
// reused on the next call without corrupting live column data.
const (
	intrinsicBufDefaultCap = 64 * 1024       // 64KB default
	intrinsicBufMaxCap     = 4 * 1024 * 1024 // 4MB cap guard

	// v1 blob field offsets: format_version[1]+format[1]+col_type[1]+row_count[4]
	intrinsicV1BlobOffFormat   = 1 // format byte offset within v1 blob
	intrinsicV1BlobOffColType  = 2 // col_type byte offset within v1 blob
	intrinsicV1BlobOffRowCount = 3 // row_count uint32 LE offset within v1 blob
)

var intrinsicBufPool = &sync.Pool{
	New: func() any { b := make([]byte, 0, intrinsicBufDefaultCap); return &b },
}

// AcquireIntrinsicBuf returns a pooled *[]byte for snappy decode scratch space.
func AcquireIntrinsicBuf() *[]byte { return intrinsicBufPool.Get().(*[]byte) }

// snappyDecodeReuse decompresses src into the pooled buffer *bp, reusing its retained
// backing array whenever it is large enough.
//
// NOTE-262: snappy.Decode reuses dst's backing array only when the decoded length is
// <= len(dst) — it checks len(dst), NOT cap(dst). Every caller here held a pooled buffer
// reset to length 0 by ReleaseIntrinsicBuf, so snappy.Decode(*bp, src) always took the
// `make([]byte, dLen)` branch and allocated a fresh zeroed buffer per decode, completely
// defeating the intrinsicBufPool — the pool's retained capacity was never used because
// the slice handed to snappy was always len 0. A querier CPU profile (2026-06-13) showed
// snappy.decode at 6.50% self-time (the largest blockpack-controllable frame) with the
// per-page make+memclr riding on top of it on the M1/M4 paged-column decode path.
//
// Growing the slice to its full capacity (*bp)[:cap(*bp)] before the call lets snappy take
// the reuse branch whenever dLen <= cap, so a warmed pooled buffer is reused in place with
// no allocation and no memclr (snappy.decode overwrites exactly dLen bytes). When the page
// is larger than the pooled capacity snappy still reallocates (unavoidable), and *bp is
// updated to the larger buffer so it is retained for the next, larger page. Behavior is
// otherwise identical to snappy.Decode(*bp, src): the returned slice has len == dLen.
//
// NOTE-263: the MaxBlockSize decompression-bomb guard is applied here so every
// pooled-buffer decode — including the search-path paged scanners that adopted this helper
// in NOTE-263 — rejects an obviously-corrupt blob before snappy allocates a multi-megabyte
// dst. The guard is cheap: snappy.DecodedLen reads only the varint length prefix.
func snappyDecodeReuse(bp *[]byte, src []byte) ([]byte, error) {
	decodedLen, lenErr := snappy.DecodedLen(src)
	if lenErr != nil {
		return nil, fmt.Errorf("snappy decoded length: %w", lenErr)
	}
	if decodedLen > MaxBlockSize {
		return nil, fmt.Errorf("snappy decoded size %d exceeds MaxBlockSize %d", decodedLen, MaxBlockSize)
	}
	dst, err := snappy.Decode((*bp)[:cap(*bp)], src)
	if err != nil {
		return nil, err
	}
	*bp = dst
	return dst, nil
}

// ReleaseIntrinsicBuf returns bp to the pool. Resets length; replaces oversized buffers.
func ReleaseIntrinsicBuf(bp *[]byte) {
	if cap(*bp) > intrinsicBufMaxCap {
		*bp = make([]byte, 0, intrinsicBufDefaultCap)
	} else {
		*bp = (*bp)[:0]
	}
	intrinsicBufPool.Put(bp)
}

// NOTE-151: pool the cross-page dict-dedup map. decodePagedColumnBlob built a fresh
// map[string]int sized valueCount*numPages on every dict-column decode (appendDictPage).
// Dict columns repeat their distinct value set on every page, so the map only ever holds
// ~valueCount distinct keys — valueCount*numPages was a ~numPages× over-allocation thrown
// away per call. Group-by columns (resource.service.name, span.http.request.method) are
// dict-encoded and decoded once per block per goroutine on M4/M6/M9/M10, so this allocated
// and GC'd one oversized map per block. A cleared pooled map starts at its warmed capacity
// (the real distinct count) — no per-call allocation, no rehash in steady state.
const dictIdxMapMaxEntries = 1 << 16 // drop pathologically large pooled maps back to GC

var dictIdxMapPool = &sync.Pool{
	New: func() any { return make(map[string]int, 256) },
}

// acquireDictIdxMap returns a cleared pooled map[string]int for cross-page dict dedup.
func acquireDictIdxMap() map[string]int { return dictIdxMapPool.Get().(map[string]int) }

// releaseDictIdxMap clears m and returns it to the pool. A map that grew past
// dictIdxMapMaxEntries (a high-cardinality outlier) is dropped so the pool never pins a
// huge backing array; the next Get allocates a fresh small map.
func releaseDictIdxMap(m map[string]int) {
	if len(m) > dictIdxMapMaxEntries {
		return
	}
	clear(m)
	dictIdxMapPool.Put(m)
}

// NOTE-280: pool the per-occurrence dict entry-index slice used to skip the pass-2 dedup
// map probe in decodeDictPagesArena. Pass 1 records, in value-visitation order, the entry
// index each (page, value) occurrence resolves to; pass 2 reads them back sequentially
// instead of re-hashing the value bytes (or rebuilding the int64 key) into the dedup map.
// A multi-page group-by column (e.g. M4 rate-by-service) repeats its full distinct value
// set on every page, so this slice has valueCount*numPages entries — sized once here and
// reused across decodes, avoiding a per-call allocation.
const dictOccIdxMaxLen = 1 << 20 // drop pathologically large pooled slices back to GC

var dictOccIdxPool = &sync.Pool{
	New: func() any { s := make([]int32, 0, 4096); return &s },
}

func acquireDictOccIdx() *[]int32 { return dictOccIdxPool.Get().(*[]int32) }

func releaseDictOccIdx(sp *[]int32) {
	if cap(*sp) > dictOccIdxMaxLen {
		return
	}
	*sp = (*sp)[:0]
	dictOccIdxPool.Put(sp)
}

// DecodeTOC decompresses a TOC blob and parses it into a slice of IntrinsicColMeta.
func DecodeTOC(blob []byte) ([]IntrinsicColMeta, error) {
	raw, err := decodeBoundedSnappyColumn(blob)
	if err != nil {
		return nil, fmt.Errorf("DecodeTOC: snappy: %w", err)
	}
	if len(raw) < 5 {
		return nil, fmt.Errorf("DecodeTOC: too short: %d bytes", len(raw))
	}
	pos := 0
	// toc_version[1]
	pos++
	// col_count[4 LE]
	count := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	entries := make([]IntrinsicColMeta, 0, count)
	for range count {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at entry name_len")
		}
		nameLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+nameLen > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at entry name")
		}
		name := string(raw[pos : pos+nameLen])
		pos += nameLen

		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at col_type/format")
		}
		colType := ColumnType(raw[pos])
		pos++
		format := raw[pos]
		pos++

		if pos+20 > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at offset/length/count")
		}
		offset := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8
		length := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4
		entryCount := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4

		// min
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at min_len")
		}
		minLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+minLen > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at min value")
		}
		minVal := string(raw[pos : pos+minLen])
		pos += minLen

		// max
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at max_len")
		}
		maxLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+maxLen > len(raw) {
			return nil, fmt.Errorf("DecodeTOC: truncated at max value")
		}
		maxVal := string(raw[pos : pos+maxLen])
		pos += maxLen

		entries = append(entries, IntrinsicColMeta{
			Name: name, Type: colType, Format: format,
			Offset: offset, Length: length, Count: entryCount,
			Min: minVal, Max: maxVal,
		})
	}
	return entries, nil
}

// EncodePageTOC encodes a PagedIntrinsicTOC to a snappy-compressed blob.
//
// Wire format (uncompressed):
//
//	page_toc_version[1] = 0x01
//	page_count[4 LE]
//	block_idx_width[1]
//	row_idx_width[1]
//	format[1]
//	col_type[1]
//	per page:
//	  offset[4 LE]
//	  length[4 LE]
//	  row_count[4 LE]
//	  min_len[2 LE] + min[min_len]
//	  max_len[2 LE] + max[max_len]
//	  bloom_len[2 LE] + bloom[bloom_len]
func EncodePageTOC(toc PagedIntrinsicTOC) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(PageTOCVersion) // PageTOCVersion

	var tmp4 [4]byte
	var tmp2 [2]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(toc.Pages))) //nolint:gosec
	buf.Write(tmp4[:])

	buf.WriteByte(toc.BlockIdxWidth)
	buf.WriteByte(toc.RowIdxWidth)
	buf.WriteByte(toc.Format)
	buf.WriteByte(byte(toc.ColType))

	for _, p := range toc.Pages {
		binary.LittleEndian.PutUint32(tmp4[:], p.Offset)
		buf.Write(tmp4[:])
		binary.LittleEndian.PutUint32(tmp4[:], p.Length)
		buf.Write(tmp4[:])
		binary.LittleEndian.PutUint32(tmp4[:], p.RowCount)
		buf.Write(tmp4[:])

		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(p.Min))) //nolint:gosec
		buf.Write(tmp2[:])
		buf.WriteString(p.Min)

		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(p.Max))) //nolint:gosec
		buf.Write(tmp2[:])
		buf.WriteString(p.Max)

		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(p.Bloom))) //nolint:gosec
		buf.Write(tmp2[:])
		buf.Write(p.Bloom)
	}

	return snappy.Encode(nil, buf.Bytes()), nil
}

// leUint64FromString reads a little-endian uint64 from the first 8 bytes of s without the
// allocation that []byte(s) would incur. Callers guarantee len(s) == 8 (NOTE-274). The
// PageMeta Min/Max range bytes are exactly 8 bytes for uint64 columns.
func leUint64FromString(s string) uint64 {
	_ = s[7] // bounds-check hint
	return uint64(s[0]) | uint64(s[1])<<8 | uint64(s[2])<<16 | uint64(s[3])<<24 |
		uint64(s[4])<<32 | uint64(s[5])<<40 | uint64(s[6])<<48 | uint64(s[7])<<56
}

// DecodePageTOC decompresses a page TOC blob and parses it into a PagedIntrinsicTOC,
// including each page's Min/Max range bytes and Bloom filter. Use this on the scan paths
// (parsePagedBlobHeaderInto) that consult those per-page stats for min/max and bloom page skip.
func DecodePageTOC(blob []byte) (PagedIntrinsicTOC, error) {
	return decodePageTOC(blob, true)
}

// DecodePageTOCNoStats is the lighter variant of DecodePageTOC for callers that only read the
// header (format/colType/widths) and per-page Offset/Length/RowCount and never consult a page's
// Min/Max/Bloom. It advances past those fields without materializing the per-page Min/Max
// string copies or the Bloom make+copy.
//
// NOTE-274: the full-column decode path (decodePagedColumnBlob) and the header-only peek
// (PeekIntrinsicBlobHeader) never touch Min/Max/Bloom — only the predicate-pruning scan paths
// do. On a Delta span:start column (hundreds of pages on real files, the hot M1/M4/M8/M9 decode
// target) the old shared DecodePageTOC allocated two Min/Max strings per page that were
// immediately discarded. Skipping them removes those per-page allocations from the decode path.
func DecodePageTOCNoStats(blob []byte) (PagedIntrinsicTOC, error) {
	return decodePageTOC(blob, false)
}

// DecodePageTOCInto is the zero-copy stats-bearing variant of DecodePageTOC for the
// predicate-pruning scan paths. It decompresses the TOC blob into the caller-owned pooled
// buffer *bp (which the caller MUST keep alive — not release to the pool — for as long as
// the returned PageMeta Min/Max/Bloom fields are read) and points each page's Min/Max/Bloom
// at sub-ranges of *bp WITHOUT copying.
//
// NOTE-282: the former stats-bearing TOC decode used by every paged scan (via
// parsePagedBlobHeader → DecodePageTOC) materialized two Min/Max string copies plus a Bloom
// make+copy PER PAGE — ~200 allocs on a hundreds-of-page Delta/dict column — only because it
// decoded into a scratch buffer it released before returning, so the stats could not alias the
// scratch. The search-path scanners (scanDictPagedBlob / scanFlatPagedBlob /
// scanDeltaUint64PagedBlob) consume every
// Min/Max/Bloom strictly within their own scan loop and never retain the TOC past the
// release of their pooled snappy scratch. By having the scanner OWN the TOC decode buffer
// for the full scan, those per-page copies collapse to zero-copy sub-slices: Min/Max via
// unsafe.String over the decode buffer, Bloom as a plain sub-slice. The buffer outlives every
// read (released by the scanner's defer after the loop), so no copy is needed for correctness.
// Cold/decode-only callers (decodePagedColumnBlob, the header peek) keep using the no-stats
// path; this purely removes warm-scan allocation, the standing lever.
func DecodePageTOCInto(blob []byte, bp *[]byte) (PagedIntrinsicTOC, error) {
	raw, err := snappyDecodeReuse(bp, blob)
	if err != nil {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOCInto: snappy: %w", err)
	}
	return parsePageTOCFields(raw, pageStatsZeroCopy)
}

// pageStatsMode selects how decodePageTOC materializes per-page Min/Max/Bloom.
type pageStatsMode uint8

const (
	pageStatsNone     pageStatsMode = iota // skip Min/Max/Bloom entirely (decode-only callers)
	pageStatsCopy                          // copy Min/Max/Bloom out (scratch released before use)
	pageStatsZeroCopy                      // alias Min/Max/Bloom into the caller-owned buffer
)

// decodePageTOC decompresses a page TOC blob and parses it into a PagedIntrinsicTOC. When
// withPageStats is false the per-page Min/Max strings and Bloom slices are left zero (their
// encoded bytes are skipped without allocation) — see DecodePageTOCNoStats.
func decodePageTOC(blob []byte, withPageStats bool) (PagedIntrinsicTOC, error) {
	// NOTE-273: decode the TOC blob into a pooled scratch buffer instead of a fresh
	// snappy.Decode(nil, …) allocation. DecodePageTOC runs once per paged-column decode
	// (decodePagedColumnBlob and the search-path header parsers) and was the last hot-path
	// caller still routing through decodeBoundedSnappyColumn, which always took snappy's
	// make([]byte, dLen) branch. Every field DecodePageTOC reads out of the decoded buffer
	// is copied before return — Min/Max via string(raw[…]) (which allocates a fresh copy)
	// and Bloom via make+copy — so nothing aliases the scratch and it is safe to release
	// back to intrinsicBufPool. snappyDecodeReuse (NOTE-262) reuses the pooled backing array
	// in place whenever the decoded TOC fits, eliminating the per-decode TOC allocation and
	// its memclr, and carries the same MaxBlockSize decompression-bomb guard.
	bp := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(bp)
	raw, err := snappyDecodeReuse(bp, blob)
	if err != nil {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: snappy: %w", err)
	}
	mode := pageStatsCopy
	if !withPageStats {
		mode = pageStatsNone
	}
	return parsePageTOCFields(raw, mode)
}

// parsePageTOCFields parses the decompressed page-TOC buffer raw into a PagedIntrinsicTOC.
// The mode controls per-page Min/Max/Bloom materialization (NOTE-282):
//   - pageStatsNone:     skip them (decode-only callers — DecodePageTOCNoStats)
//   - pageStatsCopy:     copy them out (raw is scratch released before the stats are read)
//   - pageStatsZeroCopy: alias them into raw (caller owns raw for the stats' full lifetime)
//
// For pageStatsZeroCopy the caller MUST keep raw (the pooled decode buffer) alive for as long
// as any returned Min/Max/Bloom is read; the strings/slices point directly into raw.
func parsePageTOCFields(raw []byte, mode pageStatsMode) (PagedIntrinsicTOC, error) {
	if len(raw) < 8 {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: too short: %d bytes", len(raw))
	}
	pos := 0
	tocVer := raw[pos]
	pos++
	if tocVer != PageTOCVersion {
		return PagedIntrinsicTOC{}, fmt.Errorf(
			"DecodePageTOC: unsupported page_toc_version %d (want %d)",
			tocVer,
			PageTOCVersion,
		)
	}

	pageCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	if pos+4 > len(raw) {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at header fields")
	}
	blockW := raw[pos]
	pos++
	rowW := raw[pos]
	pos++
	format := raw[pos]
	pos++
	colType := ColumnType(raw[pos])
	pos++

	pages := make([]PageMeta, 0, pageCount)
	for range pageCount {
		if pos+12 > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at page scalars")
		}
		offset := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4
		length := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4
		rowCount := binary.LittleEndian.Uint32(raw[pos:])
		pos += 4

		if pos+2 > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at min_len")
		}
		minLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+minLen > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at min value")
		}
		var minVal string
		switch mode {
		case pageStatsCopy:
			minVal = string(raw[pos : pos+minLen])
		case pageStatsZeroCopy:
			//nolint:gosec // G103: zero-copy alias into the caller-owned, read-only TOC buffer (NOTE-282); the caller holds it for the stats' full lifetime
			minVal = unsafe.String(unsafe.SliceData(raw[pos:pos+minLen]), minLen)
		case pageStatsNone:
		}
		pos += minLen

		if pos+2 > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at max_len")
		}
		maxLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+maxLen > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at max value")
		}
		var maxVal string
		switch mode {
		case pageStatsCopy:
			maxVal = string(raw[pos : pos+maxLen])
		case pageStatsZeroCopy:
			//nolint:gosec // G103: zero-copy alias into the caller-owned, read-only TOC buffer (NOTE-282); the caller holds it for the stats' full lifetime
			maxVal = unsafe.String(unsafe.SliceData(raw[pos:pos+maxLen]), maxLen)
		case pageStatsNone:
		}
		pos += maxLen

		if pos+2 > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at bloom_len")
		}
		bloomLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		var bloom []byte
		if bloomLen > 0 {
			if pos+bloomLen > len(raw) {
				return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at bloom")
			}
			switch mode {
			case pageStatsCopy:
				bloom = make([]byte, bloomLen)
				copy(bloom, raw[pos:pos+bloomLen])
			case pageStatsZeroCopy:
				bloom = raw[pos : pos+bloomLen : pos+bloomLen]
			case pageStatsNone:
			}
			pos += bloomLen
		}

		pages = append(pages, PageMeta{
			Offset:   offset,
			Length:   length,
			RowCount: rowCount,
			Min:      minVal,
			Max:      maxVal,
			Bloom:    bloom,
		})
	}

	return PagedIntrinsicTOC{
		Pages:         pages,
		BlockIdxWidth: blockW,
		RowIdxWidth:   rowW,
		Format:        format,
		ColType:       colType,
	}, nil
}

// DecodeFlatPage decodes a flat page blob (no header — format info comes from the TOC).
// rowCount must be provided from the PageMeta.RowCount field.
//
// For uint64 columns (v2 paged format):
//
//	values_len[4 LE] + varint_delta_values[values_len] + refs[rowCount × refSize]
//
// For bytes columns: length-prefixed bytes[rowCount] + refs[rowCount × refSize].
func DecodeFlatPage(raw []byte, blockW, rowW, rowCount int, colType ColumnType) (*IntrinsicColumn, error) {
	col := &IntrinsicColumn{Type: colType, Format: IntrinsicFormatFlat}
	if colType == ColumnTypeBytes {
		col.BytesValues = make([][]byte, 0, rowCount)
	} else {
		col.Uint64Values = make([]uint64, 0, rowCount)
	}
	col.BlockRefs = make([]BlockRef, 0, rowCount)
	if err := appendFlatPage(raw, blockW, rowW, rowCount, colType, col); err != nil {
		return nil, err
	}
	return col, nil
}

// appendFlatPage decodes a flat page blob and appends its values and refs directly
// into dst's (caller-pre-sized) slices, avoiding a per-page intermediate column.
// NOTE-145.
func appendFlatPage(raw []byte, blockW, rowW, rowCount int, colType ColumnType, dst *IntrinsicColumn) error {
	return appendFlatPageOpt(raw, blockW, rowW, rowCount, colType, dst, true)
}

// appendFlatPageOpt decodes a flat page's values and, when wantRefs is true, its refs.
// NOTE-340: wantRefs == false decodes only the value side, leaving BlockRefs untouched, for
// the lazy-ref decode path (the unfiltered no-group-by rate path never reads BlockRefs).
func appendFlatPageOpt(
	raw []byte,
	blockW, rowW, rowCount int,
	colType ColumnType,
	dst *IntrinsicColumn,
	wantRefs bool,
) error {
	isBytes := colType == ColumnTypeBytes
	refSize := blockW + rowW
	pos := 0

	if isBytes {
		// NOTE-356: reconstruct all of this page's values into ONE page-sized arena instead
		// of one make([]byte, vLen) per value (mirrors the NOTE-147 XOR-bytes arena). The
		// per-value alloc was the dominant allocator on the flat-bytes intrinsic decode path:
		// each small value rounds up to a malloc size class and carries per-object metadata,
		// and the resulting many small objects are RETAINED (the decoded column is held by
		// parsedIntrinsicCache for the cache-entry lifetime), inflating inuse_space with
		// rounding slop + fragmentation. A cheap pre-scan over the len[2] prefixes sizes the
		// arena exactly; each value is a non-overlapping, cap-bounded sub-slice. The arena is
		// freshly allocated and never aliases the pooled raw page buffer, so the NOTE-012/013
		// "values are independent copies" invariant holds.
		valBytes, scanErr := flatBytesPageValueSize(raw, rowCount)
		if scanErr != nil {
			return scanErr
		}
		arena := make([]byte, valBytes)
		arenaOff := 0
		for range rowCount {
			vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
			pos += 2
			v := arena[arenaOff : arenaOff+vLen : arenaOff+vLen]
			copy(v, raw[pos:pos+vLen])
			dst.BytesValues = append(dst.BytesValues, v)
			arenaOff += vLen
			pos += vLen
		}
	} else {
		// Varint delta encoding: values_len[4 LE] + varint deltas.
		if pos+4 > len(raw) {
			return fmt.Errorf("DecodeFlatPage: truncated at values_len")
		}
		valuesLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4

		var acc uint64
		valEnd := pos + valuesLen
		for range rowCount {
			if pos >= valEnd {
				return fmt.Errorf("DecodeFlatPage: truncated at varint value")
			}
			delta, n := binary.Uvarint(raw[pos:valEnd])
			if n <= 0 {
				return fmt.Errorf("DecodeFlatPage: invalid varint at pos %d", pos)
			}
			acc += delta
			dst.Uint64Values = append(dst.Uint64Values, acc)
			pos += n
		}
		pos = valEnd // ensure we're at refs start
	}

	if wantRefs {
		for range rowCount {
			if pos+refSize > len(raw) {
				return fmt.Errorf("DecodeFlatPage: truncated at refs")
			}
			dst.BlockRefs = append(dst.BlockRefs, decodeRef(raw, pos, blockW, rowW))
			pos += refSize
		}
	}
	dst.Count += uint32(rowCount) //nolint:gosec
	return nil
}

// DecodeDictPage decodes a dict page blob (no header — format info comes from the TOC).
// Page blob format (uncompressed):
//
//	value_count[4 LE]
//	per value:
//	  value_len[2 LE] + value[value_len]  (0-sentinel + 8 bytes for int64)
//	  ref_count[4 LE]
//	  refs[ref_count × refSize]
func DecodeDictPage(raw []byte, blockW, rowW int, colType ColumnType) (*IntrinsicColumn, error) {
	isInt64 := colType == ColumnTypeInt64 || colType == ColumnTypeRangeInt64
	refSize := blockW + rowW
	pos := 0

	if pos+4 > len(raw) {
		return nil, fmt.Errorf("DecodeDictPage: too short")
	}
	valueCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	col := &IntrinsicColumn{Type: colType, Format: IntrinsicFormatDict}
	col.DictEntries = make([]IntrinsicDictEntry, 0, valueCount)

	for range valueCount {
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeDictPage: truncated at value_len")
		}
		vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2

		var entry IntrinsicDictEntry
		if isInt64 && vLen == 0 {
			if pos+8 > len(raw) {
				return nil, fmt.Errorf("DecodeDictPage: truncated at int64 value")
			}
			entry.Int64Val = int64(binary.LittleEndian.Uint64(raw[pos:])) //nolint:gosec
			pos += 8
		} else {
			if pos+vLen > len(raw) {
				return nil, fmt.Errorf("DecodeDictPage: truncated at string value")
			}
			entry.Value = string(raw[pos : pos+vLen])
			pos += vLen
		}

		if pos+4 > len(raw) {
			return nil, fmt.Errorf("DecodeDictPage: truncated at ref_count")
		}
		refCount := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4

		entry.BlockRefs = make([]BlockRef, 0, refCount)
		for range refCount {
			if pos+refSize > len(raw) {
				return nil, fmt.Errorf("DecodeDictPage: truncated at refs")
			}
			entry.BlockRefs = append(entry.BlockRefs, decodeRef(raw, pos, blockW, rowW))
			pos += refSize
		}
		col.DictEntries = append(col.DictEntries, entry)
	}
	return col, nil
}

// forEachDictPageValue walks the value records of one dict page, invoking fn once per value
// with the value's bytes (nil for an int64/empty value), its int64 payload, and the byte
// offset+count of its ref block within raw. It does NOT decode the refs — callers decide
// whether to count (pass 1) or materialize (pass 2). Sharing one parser keeps the two
// decodeDictPagesArena passes byte-identical by construction (NOTE-152).
func forEachDictPageValue(
	raw []byte, refSize int, isInt64 bool,
	fn func(valBytes []byte, int64Val int64, refStart, refCount int) error,
) error {
	pos := 0
	if pos+4 > len(raw) {
		return fmt.Errorf("dict page: too short")
	}
	valueCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	for range valueCount {
		if pos+2 > len(raw) {
			return fmt.Errorf("dict page: truncated at value_len")
		}
		vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2

		var int64Val int64
		var valBytes []byte
		if isInt64 && vLen == 0 {
			if pos+8 > len(raw) {
				return fmt.Errorf("dict page: truncated at int64 value")
			}
			int64Val = int64(binary.LittleEndian.Uint64(raw[pos:])) //nolint:gosec
			pos += 8
		} else {
			if pos+vLen > len(raw) {
				return fmt.Errorf("dict page: truncated at string value")
			}
			valBytes = raw[pos : pos+vLen]
			pos += vLen
		}

		if pos+4 > len(raw) {
			return fmt.Errorf("dict page: truncated at ref_count")
		}
		refCount := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		if refCount < 0 || refCount > (len(raw)-pos)/refSize {
			return fmt.Errorf("dict page: truncated at refs")
		}
		if err := fn(valBytes, int64Val, pos, refCount); err != nil {
			return err
		}
		pos += refCount * refSize
	}
	return nil
}

// decodeDictPagesArena decodes all pages of a multi-page dict column into merged using a
// single contiguous BlockRefs arena (NOTE-152). Multi-page group-by columns (e.g.
// resource.service.name) repeat their distinct value set on every page, so the legacy
// appendDictPage extended each duplicate entry's BlockRefs with slices.Grow once per page —
// geometric reallocation that, summed over ~100 pages, allocated ~2.5x the final ref bytes
// as transient garbage (13.5% of querier alloc_space, the single largest leaf — pprof
// 2026-06-09). Two passes eliminate it: pass 1 sums each value's ref count across all pages
// (header-only walk, refs skipped); then one arena of exactly totalRefs BlockRefs is carved
// into per-entry sub-slices; pass 2 fills each entry's exact-capacity sub-slice, so no append
// ever reallocates. Entry order, Value/Int64Val, and per-entry ref order are byte-identical
// to the legacy path (entries created in first-appearance order; refs appended in page order).
//
// NOTE-171: pass-1 retains each decompressed page in a pooled buffer (up to a 4 MiB total
// budget) and pass 2 reuses it, so each page is snappy-decoded once in the common case.
// The earlier double-decode (NOTE-152) traded a second snappy pass to bound memory under
// "I/O-bound, CPU headroom" assumptions; the 2026-06-10 profile shows the querier is now
// CPU-bound (~5 cores pegged, snappy.Decode on the M4 group-by hot path), so the second
// decode is wasted work. Pages past the retain budget fall back to re-decoding in pass 2,
// preserving the bounded-memory guarantee under GOMEMLIMIT.
func decodeDictPagesArena(
	blob []byte, pageDataStart int, toc PagedIntrinsicTOC,
	blockW, rowW int, merged *IntrinsicColumn,
) error {
	isInt64 := toc.ColType == ColumnTypeInt64 || toc.ColType == ColumnTypeRangeInt64
	refSize := blockW + rowW
	if refSize <= 0 {
		return fmt.Errorf("decodeDictPagesArena: invalid ref size %d", refSize)
	}

	// NOTE-151: pooled, cleared dedup map (retains warmed capacity, no per-call alloc).
	idx := acquireDictIdxMap()
	defer releaseDictIdxMap(idx)

	// NOTE-171: single-decode page retention. The legacy two-pass design (NOTE-152)
	// snappy-decompressed every page TWICE — once in the sizing pass, once in the fill
	// pass — to avoid holding all decompressed pages in memory at once. The 2026-06-10
	// querier CPU profile shows the cluster is now CPU-bound (~5 cores pegged on heavy
	// metrics; snappy.Decode + decodeDictPagesArena are on the M4 group-by hot path),
	// so the redundant second decompression is real wasted work. Here we retain each
	// page's decompressed bytes from pass 1 in pooled buffers and reuse them in pass 2,
	// eliminating the second snappy decode — but only while the total retained size stays
	// under retainBudget. Past the budget we release that page's buffer and re-decode it
	// in pass 2 (the old behavior), preserving the bounded-memory guarantee under
	// GOMEMLIMIT. retained[i]==nil means "re-decode page i in pass 2".
	const retainBudget = intrinsicBufMaxCap // 4 MiB total decompressed bytes retained

	// scratchBuf is the fallback decode buffer for pages that were not retained (over
	// budget). NOTE-012: pooled, reused across all such pages in pass 2.
	scratchBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(scratchBuf)

	retained := make([]*[]byte, len(toc.Pages))
	defer func() {
		for _, bp := range retained {
			if bp != nil {
				ReleaseIntrinsicBuf(bp)
			}
		}
	}()
	retainedBytes := 0

	// decodePass1 decodes page i and, when under budget, retains its decompressed bytes
	// in a pooled buffer for reuse in pass 2.
	decodePass1 := func(i int, pm PageMeta) ([]byte, error) {
		pageStart := pageDataStart + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil, fmt.Errorf("decodeDictPagesArena: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		bp := AcquireIntrinsicBuf()
		pageRaw, decErr := snappyDecodeReuse(bp, blob[pageStart:pageEnd]) // NOTE-262
		if decErr != nil {
			ReleaseIntrinsicBuf(bp)
			return nil, fmt.Errorf("decodeDictPagesArena: page %d snappy: %w", i, decErr)
		}
		if retainedBytes+len(pageRaw) <= retainBudget {
			retained[i] = bp
			retainedBytes += len(pageRaw)
		} else {
			ReleaseIntrinsicBuf(bp)
		}
		return pageRaw, nil
	}

	// decodePass2 returns page i's decompressed bytes, reusing the buffer retained in
	// pass 1 when available, otherwise re-decoding into scratchBuf.
	decodePass2 := func(i int, pm PageMeta) ([]byte, error) {
		if retained[i] != nil {
			return *retained[i], nil
		}
		pageStart := pageDataStart + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil, fmt.Errorf("decodeDictPagesArena: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageRaw, decErr := snappyDecodeReuse(scratchBuf, blob[pageStart:pageEnd]) // NOTE-262
		if decErr != nil {
			return nil, fmt.Errorf("decodeDictPagesArena: page %d snappy: %w", i, decErr)
		}
		return pageRaw, nil
	}

	// NOTE-280: record, in value-visitation order, the entry index each (page, value)
	// occurrence resolves to, so pass 2 can read it back sequentially instead of re-probing
	// the dedup map (re-hashing valBytes / rebuilding the int64 key) for every occurrence on
	// every page. occIdx is appended in exactly the order forEachDictPageValue visits values
	// in pass 1, and pass 2 walks the same toc.Pages in the same order, so a monotonic counter
	// indexes them 1:1.
	occIdxP := acquireDictOccIdx()
	defer releaseDictOccIdx(occIdxP)
	occIdx := (*occIdxP)[:0]

	// Pass 1: materialize entries (Value/Int64Val) in first-appearance order and sum each
	// entry's total ref count across all pages. refTotals is parallel to merged.DictEntries.
	merged.DictEntries = merged.DictEntries[:0]
	refTotals := make([]int, 0, 64)
	var keyScratch []byte
	for i, pm := range toc.Pages {
		pageRaw, err := decodePass1(i, pm)
		if err != nil {
			return err
		}
		err = forEachDictPageValue(pageRaw, refSize, isInt64,
			func(valBytes []byte, int64Val int64, _, refCount int) error {
				if len(valBytes) > 0 {
					if j, ok := idx[string(valBytes)]; ok {
						refTotals[j] += refCount
						occIdx = append(occIdx, int32(j)) //nolint:gosec
						return nil
					}
					newIdx := len(merged.DictEntries)
					merged.DictEntries = append(merged.DictEntries,
						IntrinsicDictEntry{Value: string(valBytes), Int64Val: int64Val})
					idx[merged.DictEntries[newIdx].Value] = newIdx // reuse kept string, no 2nd alloc
					refTotals = append(refTotals, refCount)
					occIdx = append(occIdx, int32(newIdx)) //nolint:gosec
					return nil
				}
				keyScratch = append(keyScratch[:0], 0)
				keyScratch = binary.LittleEndian.AppendUint64(keyScratch, uint64(int64Val)) //nolint:gosec
				if j, ok := idx[string(keyScratch)]; ok {
					refTotals[j] += refCount
					occIdx = append(occIdx, int32(j)) //nolint:gosec
					return nil
				}
				newIdx := len(merged.DictEntries)
				merged.DictEntries = append(merged.DictEntries, IntrinsicDictEntry{Int64Val: int64Val})
				idx[string(keyScratch)] = newIdx
				refTotals = append(refTotals, refCount)
				occIdx = append(occIdx, int32(newIdx)) //nolint:gosec
				return nil
			})
		if err != nil {
			return err
		}
	}
	*occIdxP = occIdx // retain grown backing array for pooling
	if len(merged.DictEntries) == 0 {
		return nil
	}

	// Carve one contiguous arena into per-entry sub-slices of exact capacity (len 0, cap
	// refTotals[j]). Exact cap means a later append on any entry reallocates rather than
	// overwriting its neighbor — same safety contract as the NOTE-150 value/ref arenas.
	total := 0
	for _, c := range refTotals {
		total += c
	}
	// NOTE-258: the arena is carved into exact-capacity per-entry sub-slices that pass 2
	// fills completely (the summed refCount equals total), so every BlockRef slot is written
	// before any read — skip the memclr (BlockRef is pointer-free, GC-safe unscanned).
	arena := makeNoZeroBlockRef(total)
	off := 0
	for j := range merged.DictEntries {
		c := refTotals[j]
		merged.DictEntries[j].BlockRefs = arena[off : off : off+c]
		off += c
	}

	// Pass 2: decode each value's refs into its entry's exact-capacity sub-slice. Appends
	// happen in page order (same as legacy), so per-entry ref order is byte-identical.
	// NOTE-280: the entry index j for each occurrence is read sequentially from occIdx
	// (recorded in pass 1) via a monotonic counter, skipping the per-occurrence dedup-map
	// re-probe (valBytes re-hash / int64-key rebuild). occCursor advances in lockstep with
	// the identical value-visitation order of the two passes.
	occCursor := 0
	for i, pm := range toc.Pages {
		pageRaw, err := decodePass2(i, pm)
		if err != nil {
			return err
		}
		err = forEachDictPageValue(pageRaw, refSize, isInt64,
			func(_ []byte, _ int64, refStart, refCount int) error {
				j := int(occIdx[occCursor])
				occCursor++
				// NOTE-186: index-based ref store into the entry's pre-sized arena
				// sub-slice instead of a per-ref append. Each entry's BlockRefs was
				// carved with exact capacity refTotals[j] (arena[off:off:off+c]) and the
				// per-occurrence append paid a bounds-vs-cap check per ref. The summed
				// refCount across all page occurrences of an entry equals its capacity
				// (refTotals is the same sum computed in pass 1), so extending by refCount
				// here never exceeds cap and never reallocates — keeping the exact-capacity
				// arena contract (a stray future append would still reallocate, not clobber
				// a neighbor). Per-entry ref order is unchanged (page order preserved).
				//
				// NOTE-204: decode the contiguous refCount-ref run via appendVariableWidthRefs
				// (NOTE-163) instead of a per-ref decodeRef loop. decodeRef re-evaluated the
				// blockW==1 / rowW==1 width branches on EVERY ref even though both widths are
				// constant for the whole column; appendVariableWidthRefs hoists the width
				// dispatch out of the loop (one specialised branch-free copy loop per width
				// combination) and does a single up-front bounds check for the whole run.
				// The dict arena's high-cardinality group-by columns (e.g. M4's rate-by-service
				// path) decode millions of refs through this loop — decodeRef was ~0.6% of
				// querier self-time (profile 2026-06-11). appendVariableWidthRefs writes by
				// index into the entry's pre-sized BlockRefs (cap == refTotals[j] >= the running
				// total), so the exact-capacity arena contract and page-order ref layout are
				// preserved byte-for-byte.
				e := &merged.DictEntries[j]
				if _, refErr := appendVariableWidthRefs(
					pageRaw, refStart, blockW, rowW, refCount, &e.BlockRefs,
				); refErr != nil {
					return fmt.Errorf("decodeDictPagesArena: page %d refs: %w", i, refErr)
				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// decodePagedColumnBlobOpt decodes a v2 paged column blob into a merged IntrinsicColumn.
// blob[0] must already be verified to be IntrinsicPagedVersion (0x02).
//
// Wire format:
//
//	sentinel[1] = 0x02
//	toc_len[4 LE]
//	toc_blob[toc_len]  (snappy-compressed PagedIntrinsicTOC)
//	page_blob_0[pages[0].Length]
//	page_blob_1[pages[1].Length]
//	...
//
// When eagerRefs is true the BlockRefs
// are decoded in the SAME page-decompression pass as the values, instead of deferring them to
// EnsureBlockRefs -> decodePagedColumnRefs (which snappy-decompresses every page a SECOND time).
//
// NOTE-390: the ref-needing metrics paths (predicate-filtered span:start, every `... by (...)`
// group-by column and agg field — M6/M8/M9 and any filtered/grouped query) reach refs via
// EnsureBlockRefs, which re-walks the blob and re-decompresses each page solely to recover the
// refs section that sits at the tail of bytes the eager value decode already decompressed.
// decodePagedColumnRefs was ~6% of querier cumulative CPU (profile 2026-06-15 24h M8 window),
// dominated by the redundant snappy pass. When the caller knows up front it needs refs (the
// executor computes needsRefs before any decode, metrics_trace_intrinsic.go), eagerRefs=true
// folds the ref decode into the existing wantRefs=true page path so each page is decompressed
// exactly once. The unfiltered no-group-by rate fast path (M1/M4) still passes eagerRefs=false
// and keeps the deferred-ref behavior (it never reads BlockRefs). The decoded column is
// byte-identical either way — eager refs just materializes col.BlockRefs during decode rather
// than on first read.
func decodePagedColumnBlobOpt(blob []byte, eagerRefs bool) (*IntrinsicColumn, error) {
	if len(blob) < 5 {
		return nil, fmt.Errorf("decodePagedColumnBlob: too short")
	}
	pos := 1 // skip sentinel byte

	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4

	if pos+tocLen > len(blob) {
		return nil, fmt.Errorf("decodePagedColumnBlob: truncated at toc_blob")
	}
	// NOTE-274: the full-column decode never reads any page's Min/Max/Bloom — those are only
	// consulted by the predicate-pruning scan paths (parsePagedBlobHeaderInto). Skip materializing
	// the per-page Min/Max string copies (a Delta span:start column has hundreds of pages).
	toc, err := DecodePageTOCNoStats(blob[pos : pos+tocLen])
	if err != nil {
		return nil, fmt.Errorf("decodePagedColumnBlob: %w", err)
	}
	pos += tocLen // pos now points to first page blob

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)

	// Pre-allocate merged output from page TOC totals — eliminates repeated
	// append reallocations as pages are merged one by one.
	var totalRows int
	for _, pm := range toc.Pages {
		totalRows += int(pm.RowCount)
	}
	merged := &IntrinsicColumn{Type: toc.ColType, Format: toc.Format}
	// NOTE-340: defer BlockRefs decode for value-decoupled paged formats (Flat/Delta/XOR).
	// The value side decodes eagerly; refs are filled on first read via EnsureBlockRefs ->
	// decodePagedColumnRefs. The unfiltered no-group-by rate path (M1/M4) reads only values +
	// Count, so it never triggers the ref decode (appendVariableWidthRefs was ~10.7s of querier
	// self-time). Dict columns keep eager refs (their refs share a cross-page arena, NOTE-152).
	// NOTE-390: eagerRefs forces the refs to be decoded in the same page pass as the values,
	// avoiding the second snappy decompression in decodePagedColumnRefs. Only the lazy-ref
	// formats (Flat/Delta/XOR) defer refs; eagerRefs makes them decode refs inline instead.
	lazyRefs := isParallelPageDecodeFormat(toc.Format) && !eagerRefs
	// NOTE-145: pre-size only the value slice this column type actually uses (a column
	// is uint64 OR bytes, never both) — the old code allocated a full totalRows-sized
	// slice for the unused type on every flat/xor/delta column.
	if toc.Format == IntrinsicFormatFlat || toc.Format == IntrinsicFormatXORBytes ||
		toc.Format == IntrinsicFormatDeltaUint64 {
		if toc.ColType == ColumnTypeBytes {
			// []byte elements carry pointers — must be zeroed (NOTE-258).
			merged.BytesValues = make([][]byte, 0, totalRows)
		} else {
			// NOTE-258: pointer-free, fully overwritten by the page decode — skip the memclr.
			merged.Uint64Values = makeNoZeroUint64(totalRows)[:0]
		}
		if !lazyRefs {
			// NOTE-258: BlockRef is pointer-free and every slot is written by the decode.
			// NOTE-390: also reached on the eagerRefs path for Flat/Delta/XOR (lazyRefs is
			// false because eagerRefs overrode it) — pre-size the refs backing so the page
			// decode writes refs inline alongside the values.
			merged.BlockRefs = makeNoZeroBlockRef(totalRows)[:0]
		}
	}
	if lazyRefs {
		blobRef := blob // capture for the deferred ref decode (immutable cached blob)
		merged.refsDecode = func() []BlockRef { return decodePagedColumnRefs(blobRef) }
		// NOTE-344: the closure pins blobRef alive — record its length so the LRU budget
		// (IntrinsicColumn.SizeBytes) accounts for the retained blob and the eventual refs.
		// Column blobs are far below 4 GiB, so the uint32 conversion never overflows.
		blobLen := len(blobRef)
		merged.refsBlobLen = uint32(blobLen) //nolint:gosec // G115: column blob len << 4 GiB
	}

	// NOTE-150: Flat/XOR/Delta pages are self-contained (delta acc and XOR prev reset to
	// zero/nil at the start of every page — see appendDeltaUint64Page/appendXORBytesPage),
	// so they decode independently of each other. For multiple sizable pages, decode them
	// in parallel: each page writes into a disjoint, capacity-capped sub-slice of merged's
	// pre-sized backing arrays, so there is no cross-goroutine aliasing or reduction step.
	// Dict pages share dictIdx (cross-page value dedup) and stay on the serial path below.
	// span:start (Delta, hundreds of pages on real files) and trace:id/span:id (XORBytes)
	// are the hot decode targets — ~27% of querier allocs and the residual cost of M1.
	if isParallelPageDecodeFormat(toc.Format) && len(toc.Pages) >= 2 &&
		totalRows >= parallelPageDecodeMinRows {
		// NOTE-340: when lazyRefs is true the parallel decode fills only values; refs are
		// deferred to EnsureBlockRefs. NOTE-390: when eagerRefs forced lazyRefs=false the
		// parallel decode also fills refs inline (wantRefs == !lazyRefs), so each page is
		// decompressed once for both values and refs.
		if err = decodePagesParallel(blob, pos, toc, blockW, rowW, totalRows, merged, !lazyRefs); err != nil {
			return nil, err
		}
		return merged, nil
	}

	// NOTE-152: dict columns decode through a single contiguous BlockRefs arena (two passes:
	// count, then fill exact-capacity per-entry sub-slices) instead of growing each duplicate
	// entry's refs page by page. This eliminates the slices.Grow geometric reallocation that
	// was the largest single querier alloc_space leaf (13.5%, pprof 2026-06-09). Supersedes the
	// per-page appendDictPage merge (NOTE-146) and its pooled dedup map (NOTE-151, reused here).
	if toc.Format == IntrinsicFormatDict {
		if err = decodeDictPagesArena(blob, pos, toc, blockW, rowW, merged); err != nil {
			return nil, err
		}
		return merged, nil
	}

	// NOTE-012: reuse a pooled decode buffer across page decodes.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	for i, pm := range toc.Pages {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageCompressed := blob[pageStart:pageEnd]
		pageRaw, decErr := snappyDecodeReuse(pageBuf, pageCompressed) // NOTE-262
		if decErr != nil {
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d snappy: %w", i, decErr)
		}

		// NOTE-145: Flat/XOR/Delta pages decode directly into merged's pre-allocated
		// slices — the append helpers skip the per-page *IntrinsicColumn struct and
		// intermediate slices that the old code allocated only to copy-and-discard.
		// Reached only for single-page or sub-threshold columns (the parallel path above
		// handles the multi-page case); dict is handled by the arena path above.
		// NOTE-340: wantRefs == !lazyRefs. For the value-decoupled formats lazyRefs is true,
		// so the serial single-page / sub-threshold path also decodes values only and defers
		// refs to EnsureBlockRefs (decodePagedColumnRefs).
		wantRefs := !lazyRefs
		switch toc.Format {
		case IntrinsicFormatFlat:
			err = appendFlatPageOpt(pageRaw, blockW, rowW, int(pm.RowCount), toc.ColType, merged, wantRefs)
		case IntrinsicFormatXORBytes:
			err = appendXORBytesPageOpt(pageRaw, blockW, rowW, int(pm.RowCount), merged, wantRefs)
		case IntrinsicFormatDeltaUint64:
			err = appendDeltaUint64PageOpt(pageRaw, blockW, rowW, int(pm.RowCount), merged, wantRefs)
		default:
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d: unknown format %d", i, toc.Format)
		}
		if err != nil {
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d: %w", i, err)
		}
	}
	return merged, nil
}

// decodePagedColumnRefs re-walks a paged Flat/Delta/XOR column blob decoding ONLY its
// BlockRefs, returning a fully populated slice of length totalRows. NOTE-340: this is the
// lazy ref decode invoked by IntrinsicColumn.EnsureBlockRefs the first time a query reads
// the refs of a column whose value side was decoded eagerly with refs deferred. It mirrors
// decodePagedColumnBlob's page-walk and width parsing exactly; refs of a page begin after
// that page's value section, so the value section is skipped (delta/flat: varint scan;
// XOR-bytes: length-prefix scan) to locate each page's refs offset. Errors during the
// re-walk fall back to an empty slice (the caller's column is then ref-empty for this
// process, which is safe: a malformed blob would also have failed the eager value decode).
func decodePagedColumnRefs(blob []byte) []BlockRef {
	if len(blob) < 5 || blob[0] != IntrinsicPagedVersion {
		return nil
	}
	pos := 1
	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4
	if pos+tocLen > len(blob) {
		return nil
	}
	toc, err := DecodePageTOCNoStats(blob[pos : pos+tocLen])
	if err != nil {
		return nil
	}
	pos += tocLen

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	refSize := blockW + rowW

	var totalRows int
	for _, pm := range toc.Pages {
		totalRows += int(pm.RowCount)
	}
	refs := makeNoZeroBlockRef(totalRows)[:0]

	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	for _, pm := range toc.Pages {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd])
		if decErr != nil {
			return nil
		}
		rowCount := int(pm.RowCount)
		// NOTE-353: locate the refs section by subtraction, not by re-scanning the value
		// section. Every value-decoupled page (Delta/Flat/XOR) writes its refs section LAST
		// and contiguous, exactly count*(blockW+rowW) bytes, with NO trailing bytes (see the
		// writer's encodeDeltaUint64Intrinsic / encodeFlatPageBlob / encodeXORBytesIntrinsic:
		// values then refs, snappy'd as a unit). So refsStart == len(pageRaw) - count*refSize.
		// pageRefsOffset re-walked the entire value section (every delta uvarint / every
		// length prefix) on each page solely to arrive at this same offset — that re-scan was
		// the dominant cost of the deferred-ref decode (~5.7s querier self-time, profile
		// 2026-06-14, the #2 blockpack frame), and it duplicated work the eager value decode
		// already performed. The refs section length is invariant of the value encoding, so
		// the offset is computable in O(1). A negative/oversized result means the page is
		// shorter than its declared refs section (malformed blob); bail like the old !ok path.
		refsBytes := rowCount * refSize
		refsStart := len(pageRaw) - refsBytes
		if refsStart < 0 {
			return nil
		}
		if _, rerr := appendVariableWidthRefs(pageRaw, refsStart, blockW, rowW, rowCount, &refs); rerr != nil {
			return nil
		}
	}
	return refs
}

// NOTE-150: parallel page-decode tuning.
const (
	// parallelPageDecodeMinRows gates the parallel path: only worth the goroutine spawn
	// overhead once there are at least two full pages of work. IntrinsicPageSize=10_000.
	parallelPageDecodeMinRows = 2 * IntrinsicPageSize
	// maxPageDecodeWorkers caps fan-out so a single column decode cannot monopolize all
	// cores (a querier runs many concurrent block/file decodes). Matches the W=8 cap used
	// by the I/O pipeline (NOTE-058) and the group-by scan (NOTE-143).
	maxPageDecodeWorkers = 8
)

// isParallelPageDecodeFormat reports whether a paged format's pages decode independently
// (no cross-page state), making them safe to decode concurrently. NOTE-150.
func isParallelPageDecodeFormat(format uint8) bool {
	return format == IntrinsicFormatFlat || format == IntrinsicFormatXORBytes ||
		format == IntrinsicFormatDeltaUint64
}

// decodePagesParallel decodes the pages of a Flat/XOR/Delta paged column concurrently into
// merged. Each page i writes its values/refs into the disjoint backing region
// [rowOffset_i, rowOffset_i+RowCount_i) via capacity-capped sub-slices, so the existing
// append* helpers reuse unchanged while every goroutine touches a distinct memory range.
//
// pageDataStart is the offset in blob of the first page blob (== pos after the TOC).
// merged's value and ref slices must already be allocated with cap == totalRows (done by
// decodePagedColumnBlob for these formats). NOTE-150.
func decodePagesParallel(
	blob []byte,
	pageDataStart int,
	toc PagedIntrinsicTOC,
	blockW, rowW, totalRows int,
	merged *IntrinsicColumn,
	wantRefs bool, // NOTE-390: when true, refs are decoded inline (single snappy pass per page)
) error {
	// rowOffsets[i] = first output row index for page i (cumulative RowCount).
	rowOffsets := make([]int, len(toc.Pages))
	acc := 0
	for i, pm := range toc.Pages {
		rowOffsets[i] = acc
		acc += int(pm.RowCount)
	}

	// Expose the full backing arrays so per-page capped sub-slices land in the right slot.
	// The append helpers fill [off:off+RowCount); the union covers [0:totalRows) exactly.
	// NOTE-340: refs are deferred (lazyRefs is always true for these formats), so merged.BlockRefs
	// is nil here; the parallel decode fills only values via the *Opt helpers with wantRefs=false.
	isBytes := toc.ColType == ColumnTypeBytes
	if isBytes {
		merged.BytesValues = merged.BytesValues[:totalRows]
	} else {
		merged.Uint64Values = merged.Uint64Values[:totalRows]
	}
	// NOTE-390: when refs are decoded inline, expose the full pre-sized refs backing so each
	// page's capped sub-slice [off:off:off+rc] writes into its disjoint slot, mirroring the
	// value-slice discipline above. merged.BlockRefs was pre-sized to cap==totalRows by
	// decodePagedColumnBlobOpt's !lazyRefs branch.
	if wantRefs {
		merged.BlockRefs = merged.BlockRefs[:totalRows]
	}

	workers := min(len(toc.Pages), maxPageDecodeWorkers)

	var (
		next     atomic.Int64 // next page index to claim (work-stealing for balanced load)
		failed   atomic.Bool  // NOTE-374: lock-free per-page abort gate (was a mutex-guarded read)
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	// NOTE-374: split the abort signal (atomic.Bool, polled once per page) from the error
	// capture (mutex, hit at most once per worker on the error path). decodePagesParallel
	// runs over hundreds of small Delta/XOR pages per hot column; the previous hasErr()
	// acquired and released a sync.Mutex on EVERY page iteration purely to read whether any
	// worker had failed — a per-page lock on the happy path where firstErr is always nil. A
	// single atomic.Bool load is branch-light and contention-free, so the abort poll no longer
	// serializes the workers. The mutex now guards only the rare first-error store.
	setErr := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		mu.Unlock()
		failed.Store(true)
	}

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			// SPEC-ROOT-001: a worker panic must not crash the process.
			defer func() {
				if rec := recover(); rec != nil {
					setErr(fmt.Errorf("decodePagesParallel: worker panic: %v", rec))
				}
			}()
			pageBuf := AcquireIntrinsicBuf()
			defer ReleaseIntrinsicBuf(pageBuf)

			// NOTE-374: one worker-local slot struct reused across this worker's pages instead
			// of a fresh &IntrinsicColumn per page. Each worker decodes its claimed pages
			// serially, so re-pointing slot's value slice each iteration is safe — the append
			// helpers only read Type/Format and write into the value slice + Count, and the
			// regions are disjoint across pages. Eliminates one heap alloc per page (hundreds
			// per hot column per block).
			slot := &IntrinsicColumn{Type: toc.ColType, Format: toc.Format}

			for {
				i := int(next.Add(1)) - 1
				if i >= len(toc.Pages) || failed.Load() {
					return
				}
				pm := toc.Pages[i]
				pageStart := pageDataStart + int(pm.Offset)
				pageEnd := pageStart + int(pm.Length)
				if pageEnd > len(blob) || pageStart < 0 {
					setErr(fmt.Errorf("decodePagesParallel: page %d out of bounds "+
						"(offset=%d len=%d blobLen=%d)", i, pm.Offset, pm.Length, len(blob)))
					return
				}
				pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-262
				if decErr != nil {
					setErr(fmt.Errorf("decodePagesParallel: page %d snappy: %w", i, decErr))
					return
				}

				off := rowOffsets[i]
				rc := int(pm.RowCount)
				// slot aliases merged's disjoint [off:off+rc) region with len 0, cap rc.
				// The append helpers fill exactly rc entries — never exceeding cap, so they
				// stay within this goroutine's region and never reallocate or alias others.
				// NOTE-374: slot is reused across this worker's pages; re-point and reset its
				// value slice + Count before each page decode.
				slot.Count = 0
				if isBytes {
					slot.BytesValues = merged.BytesValues[off : off : off+rc]
				} else {
					slot.Uint64Values = merged.Uint64Values[off : off : off+rc]
				}
				// NOTE-340: when wantRefs is false refs are deferred to EnsureBlockRefs and
				// slot.BlockRefs stays nil (the *Opt helpers leave it untouched).
				// NOTE-390: when wantRefs is true, alias this page's disjoint refs slot
				// [off:off:off+rc] so the inline ref decode writes into merged's backing.
				if wantRefs {
					slot.BlockRefs = merged.BlockRefs[off : off : off+rc]
				}

				var aerr error
				switch toc.Format {
				case IntrinsicFormatFlat:
					aerr = appendFlatPageOpt(pageRaw, blockW, rowW, rc, toc.ColType, slot, wantRefs)
				case IntrinsicFormatXORBytes:
					aerr = appendXORBytesPageOpt(pageRaw, blockW, rowW, rc, slot, wantRefs)
				case IntrinsicFormatDeltaUint64:
					aerr = appendDeltaUint64PageOpt(pageRaw, blockW, rowW, rc, slot, wantRefs)
				}
				if aerr != nil {
					setErr(fmt.Errorf("decodePagesParallel: page %d: %w", i, aerr))
					return
				}
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	merged.Count = uint32(totalRows) //nolint:gosec
	return nil
}

// PeekIntrinsicBlobHeader reads the format, column type, and row count from a V14
// intrinsic column blob as stored on disk by writeV14Sections.
//
// V14 on-disk layout (writeV14Sections writes encodeColumn output directly):
//   - Non-paged: snappy(format_version[1]+format[1]+col_type[1]+row_count[4]+...)
//     encodeFlatColumn/encodeDictColumn return snappy-compressed v1 blobs.
//   - Paged: IntrinsicPagedVersion[1]+toc_len[4]+toc_blob+page_blobs
//     encodePagedFlatColumn/encodePagedDictColumn return raw paged blobs (not snappy-wrapped).
//
// Returns format, colType, count. Returns zeros without error for empty blobs.
func PeekIntrinsicBlobHeader(blob []byte) (format uint8, colType ColumnType, count uint32, err error) {
	if len(blob) == 0 {
		return 0, 0, 0, nil
	}
	// Paged format: starts with IntrinsicPagedVersion (0x02) sentinel byte, stored raw.
	if blob[0] == IntrinsicPagedVersion {
		if len(blob) < 5 {
			return 0, 0, 0, fmt.Errorf("PeekIntrinsicBlobHeader: paged blob too short")
		}
		tocLen := int(binary.LittleEndian.Uint32(blob[1:5]))
		if len(blob) < 5+tocLen {
			return 0, 0, 0, fmt.Errorf("PeekIntrinsicBlobHeader: paged toc truncated")
		}
		// NOTE-274: the header peek reads only format/colType/count, never Min/Max/Bloom.
		toc, tocErr := DecodePageTOCNoStats(blob[5 : 5+tocLen])
		if tocErr != nil {
			return 0, 0, 0, fmt.Errorf("PeekIntrinsicBlobHeader: %w", tocErr)
		}
		var totalRows uint32
		for _, p := range toc.Pages {
			totalRows += p.RowCount
		}
		return toc.Format, toc.ColType, totalRows, nil
	}
	// Non-paged: snappy-compressed v1 blob. Decode to get the header bytes.
	// NOTE: snappy is a block format with no streaming/partial-decode API. There is no
	// way to retrieve only the first N decoded bytes without decompressing the full blob.
	// snappy.DecodedLen can be used as a pre-check guard (reject obviously-corrupt blobs
	// before allocating) but the full decode cannot be avoided to read any content.
	// TODO: if this proves expensive at scale, consider storing a tiny uncompressed prefix
	// (format_version+format+col_type+row_count) alongside the compressed blob so that
	// PeekIntrinsicBlobHeader can read 7 bytes without decompressing anything.
	raw, decErr := decodeBoundedSnappyColumn(blob)
	if decErr != nil {
		return 0, 0, 0, fmt.Errorf("PeekIntrinsicBlobHeader: snappy: %w", decErr)
	}
	// v1 wire format: format_version[1]+format[1]+col_type[1]+row_count[4].
	if len(raw) < 7 {
		return 0, 0, 0, fmt.Errorf("PeekIntrinsicBlobHeader: v1 blob too short (%d bytes)", len(raw))
	}
	f := raw[intrinsicV1BlobOffFormat]
	ct := ColumnType(raw[intrinsicV1BlobOffColType])
	c := binary.LittleEndian.Uint32(raw[intrinsicV1BlobOffRowCount : intrinsicV1BlobOffRowCount+4])
	return f, ct, c, nil
}

// DecodeIntrinsicColumnBlob decompresses and decodes a column data blob into an IntrinsicColumn.
func DecodeIntrinsicColumnBlob(blob []byte) (*IntrinsicColumn, error) {
	return decodeIntrinsicColumnBlobOpt(blob, false)
}

// DecodeIntrinsicColumnBlobEagerRefs decodes a column blob with its BlockRefs materialized in
// the same page-decompression pass as the values (NOTE-390). Callers that know up front they
// will read col.BlockRefs (predicate-filtered / group-by metrics paths) should use this instead
// of DecodeIntrinsicColumnBlob followed by col.EnsureBlockRefs(): the latter re-decompresses
// every page a second time in decodePagedColumnRefs just to recover the refs section the value
// decode already had in hand. The non-paged (legacy v1) and Dict formats already decode refs
// eagerly, so eagerRefs is a no-op for them and the result is byte-identical to Decode
// IntrinsicColumnBlob.
func DecodeIntrinsicColumnBlobEagerRefs(blob []byte) (*IntrinsicColumn, error) {
	return decodeIntrinsicColumnBlobOpt(blob, true)
}

func decodeIntrinsicColumnBlobOpt(blob []byte, eagerRefs bool) (*IntrinsicColumn, error) {
	// v2 paged format: first byte is IntrinsicPagedVersion (0x02).
	// The blob is NOT snappy-compressed as a whole; it contains the page TOC + page blobs.
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return decodePagedColumnBlobOpt(blob, eagerRefs)
	}

	raw, err := decodeBoundedSnappyColumn(blob)
	if err != nil {
		return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: snappy: %w", err)
	}
	if len(raw) < 3 {
		return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: too short")
	}
	pos := 0
	// format_version[1]
	pos++
	format := raw[pos]
	pos++
	colType := ColumnType(raw[pos])
	pos++

	col := &IntrinsicColumn{Type: colType, Format: format}

	if pos+4 > len(raw) {
		return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at row_count")
	}
	rowCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4
	col.Count = uint32(rowCount) //nolint:gosec

	if format == IntrinsicFormatFlat {
		if err := decodeLegacyFlatBlob(raw, pos, colType, rowCount, col); err != nil {
			return nil, err
		}
	} else { // IntrinsicFormatDict
		if err := decodeLegacyDictBlob(raw, pos, colType, rowCount, col); err != nil {
			return nil, err
		}
	}
	return col, nil
}

// decodeLegacyFlatBlob decodes the flat-format section of a legacy (v1) intrinsic column blob.
// raw is the snappy-decoded buffer; pos is the offset after the row_count field.
func decodeLegacyFlatBlob(raw []byte, pos int, colType ColumnType, rowCount int, col *IntrinsicColumn) error {
	// block_idx_width[1] and row_idx_width[1] — variable-width ref encoding.
	if pos+2 > len(raw) {
		return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at ref widths")
	}
	blockW := int(raw[pos])
	rowW := int(raw[pos+1])
	pos += 2

	isBytes := colType == ColumnTypeBytes
	if isBytes {
		// NOTE-356: single value arena instead of one make([]byte) per value (same rationale
		// and safety contract as the appendFlatPageOpt bytes path). raw is the decoded blob;
		// the arena is a fresh, non-aliasing allocation, preserving the NOTE-012 copy invariant.
		valBytes, scanErr := flatBytesPageValueSize(raw[pos:], rowCount)
		if scanErr != nil {
			return fmt.Errorf("DecodeIntrinsicColumnBlob: %w", scanErr)
		}
		col.BytesValues = make([][]byte, 0, rowCount)
		arena := make([]byte, valBytes)
		arenaOff := 0
		for range rowCount {
			vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
			pos += 2
			v := arena[arenaOff : arenaOff+vLen : arenaOff+vLen]
			copy(v, raw[pos:pos+vLen])
			col.BytesValues = append(col.BytesValues, v)
			arenaOff += vLen
			pos += vLen
		}
	} else {
		// Delta-encoded uint64: each value is a delta from the previous.
		col.Uint64Values = make([]uint64, 0, rowCount)
		var acc uint64
		for range rowCount {
			if pos+8 > len(raw) {
				return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at uint64 value")
			}
			acc += binary.LittleEndian.Uint64(raw[pos:])
			col.Uint64Values = append(col.Uint64Values, acc)
			pos += 8
		}
	}
	// Refs parallel to values, using variable-width encoding.
	col.BlockRefs = make([]BlockRef, 0, rowCount)
	if _, err := appendVariableWidthRefs(raw, pos, blockW, rowW, rowCount, &col.BlockRefs); err != nil {
		return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at refs")
	}
	return nil
}

// decodeLegacyDictBlob decodes the dict-format section of a legacy (v1) intrinsic column blob.
// raw is the snappy-decoded buffer; pos is the offset after the row_count field.
func decodeLegacyDictBlob(raw []byte, pos int, colType ColumnType, rowCount int, col *IntrinsicColumn) error {
	isInt64 := colType == ColumnTypeInt64 || colType == ColumnTypeRangeInt64
	valueCount := rowCount // for dict: row_count field holds value_count

	// block_idx_width[1] and row_idx_width[1] — variable-width ref encoding.
	if pos+2 > len(raw) {
		return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at dict ref widths")
	}
	blockW := int(raw[pos])
	rowW := int(raw[pos+1])
	pos += 2

	col.DictEntries = make([]IntrinsicDictEntry, 0, valueCount)
	for range valueCount {
		if pos+2 > len(raw) {
			return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at dict value_len")
		}
		vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		var entry IntrinsicDictEntry
		if isInt64 && vLen == 0 {
			if pos+8 > len(raw) {
				return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at int64 value")
			}
			entry.Int64Val = int64(binary.LittleEndian.Uint64(raw[pos:])) //nolint:gosec
			pos += 8
		} else {
			if pos+vLen > len(raw) {
				return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at string value")
			}
			entry.Value = string(raw[pos : pos+vLen])
			pos += vLen
		}
		if pos+4 > len(raw) {
			return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at ref_count")
		}
		refCount := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		entry.BlockRefs = make([]BlockRef, 0, refCount)
		newPos, err := appendVariableWidthRefs(raw, pos, blockW, rowW, refCount, &entry.BlockRefs)
		if err != nil {
			return fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at dict refs")
		}
		pos = newPos
		col.DictEntries = append(col.DictEntries, entry)
	}
	return nil
}

// appendVariableWidthRefs decodes a contiguous run of `count` variable-width
// BlockRefs starting at raw[pos] and appends them to *dst, returning the new
// position after the last ref.
//
// NOTE-163: this is the batch form of decodeVariableWidthRef, which was called
// once per row across all four intrinsic decode paths (appendDeltaUint64Page,
// appendXORBytesPage, the v1 flat/delta tail, and the v1 legacy dict tail). The
// per-call form re-validated blockW/rowW and re-derived refSize on every single
// row, and the (blockW==1?:) / (rowW==1?:) width branches were re-evaluated per
// row even though both widths are constant for the entire page. A querier CPU
// profile (2026-06-10) showed decodeVariableWidthRef at ~0.44% self time on the
// hot delta/XOR decode paths (span:start, trace:id, span:id), which dominate the
// residual M1/M4 decode cost. Hoisting the width validation + bounds check out of
// the loop and specialising the inner loop per width combination removes those
// redundant per-row branches; the four combinations are unrolled by hand so the
// compiler emits a flat copy-and-advance loop with no per-iteration width test.
//
// The total refs section is exactly count*(blockW+rowW) bytes, so a single
// up-front bounds check replaces count separate checks. Behavior is identical to
// calling decodeVariableWidthRef count times.
func appendVariableWidthRefs(raw []byte, pos, blockW, rowW, count int, dst *[]BlockRef) (int, error) {
	if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) {
		return pos, fmt.Errorf("invalid ref width: blockW=%d rowW=%d", blockW, rowW)
	}
	refSize := blockW + rowW
	end := pos + count*refSize
	if end > len(raw) {
		return pos, fmt.Errorf("truncated ref")
	}
	// NOTE-186: index-based ref store into a pre-extended slice region instead of a
	// per-row append. appendVariableWidthRefs runs once per page after every delta/xor
	// page decode (~1.4% of querier CPU, profile 2026-06-09) and its per-row append paid
	// a bounds-vs-cap check and a length update on every BlockRef even though callers
	// guarantee capacity: the serial path pre-sizes BlockRefs to totalRows
	// (decodePagedColumnBlob, NOTE-145), and the parallel path hands each page a
	// capacity-capped sub-slice [off:off:off+rc] (decodePagesParallel, NOTE-150). So
	// cap(refs)-len(refs) >= count always holds; we extend the slice once and write each
	// ref by index, mirroring the NOTE-169 store discipline on the value side. The rare
	// short-capacity caller (none in the current tree) is handled by a single growing
	// append before the index loop.
	refs := *dst
	base := len(refs)
	if cap(refs)-base >= count {
		refs = refs[:base+count]
	} else {
		refs = append(refs, make([]BlockRef, count)...)
	}
	// NOTE-236: hoist all bounds checks out of the per-ref scatter loops. The previous
	// `for i := 0; pos < end; pos += stride { refs[base+i] = ...; i++ }` form could not be
	// bounds-check-eliminated: the compiler could prove neither that refs[base+i] was in
	// range nor that raw[pos], raw[pos+1], ... were, so every BlockRef store and every
	// source byte load paid an IsInBounds/IsSliceInBounds check (appendVariableWidthRefs was
	// ~1.2% of querier self-time, profile 2026-06-12, the second-largest blockpack frame on
	// the M1/M4 flat/delta decode path). Reslicing the destination to exactly the count slots
	// (out := refs[base : base+count]) lets the compiler discharge the store check from the
	// loop bound (i < count), and taking a tight source window src := raw[pos:end] of length
	// count*refSize lets it discharge every src[i*refSize+k] read from the same bound. The
	// indexed `for i := range count` form makes both lengths statically related to i, so the
	// loop bodies become check-free byte loads + a single struct store.
	out := refs[base : base+count]
	src := raw[pos:end] // len(src) == count*refSize, validated above
	switch {
	case blockW == 1 && rowW == 1:
		for i := range out {
			// Consuming src by exactly refSize each iteration keeps len(src) ==
			// (count-i)*refSize, so s[0..refSize-1] are provably in range and the
			// compiler discharges the loads with no per-row bounds check.
			s := src[:2:2]
			src = src[2:]
			out[i] = BlockRef{
				BlockIdx: uint16(s[0]),
				RowIdx:   uint16(s[1]),
			}
		}
	case blockW == 1 && rowW == 2:
		for i := range out {
			s := src[:3:3]
			src = src[3:]
			out[i] = BlockRef{
				BlockIdx: uint16(s[0]),
				RowIdx:   binary.LittleEndian.Uint16(s[1:]),
			}
		}
	case blockW == 2 && rowW == 1:
		for i := range out {
			s := src[:3:3]
			src = src[3:]
			out[i] = BlockRef{
				BlockIdx: binary.LittleEndian.Uint16(s),
				RowIdx:   uint16(s[2]),
			}
		}
	default: // blockW == 2 && rowW == 2
		for i := range out {
			s := src[:4:4]
			src = src[4:]
			out[i] = BlockRef{
				BlockIdx: binary.LittleEndian.Uint16(s),
				RowIdx:   binary.LittleEndian.Uint16(s[2:]),
			}
		}
	}
	*dst = refs
	return end, nil
}

// appendXORBytesPage decodes a single XOR-encoded bytes page blob (already snappy-decoded)
// and appends its values and refs directly into dst. NOTE-145.
//
// Wire format (see encodeXORBytesIntrinsic):
//
//	for each row i:
//	  xor_data_len[4 LE] + xor_data
//	refs[rowCount × refSize]
//
// NOTE-013: BytesValues must be independent copies (NOTE-012 invariant) — they cannot alias
// the pageBuf pool buffer that decodePagedColumnBlob reuses across page decodes.
//
// appendXORBytesPageOpt decodes an XOR-bytes page's values and, when wantRefs is true, its
// refs. NOTE-340: wantRefs == false skips the BlockRefs decode for the lazy-ref path.
func appendXORBytesPageOpt(raw []byte, blockW, rowW, rowCount int, dst *IntrinsicColumn, wantRefs bool) error {
	// NOTE-147: reconstruct all values into one page-sized arena instead of one
	// make([]byte) per value. xorInvert previously allocated a fresh slice per row
	// (~one alloc/value = the dominant cost on this path, ~27% of querier allocs per
	// Pyroscope). A cheap pre-scan over the length prefixes sizes the arena exactly,
	// so the whole page's values share a single allocation with zero waste. Each value
	// is a non-overlapping subslice; the arena is never reallocated, so prev (which
	// points into it) stays valid across iterations. The arena is freshly allocated and
	// never aliases the pool buffer (raw), preserving NOTE-012/NOTE-013.
	valBytes, err := xorBytesPageValueSize(raw, rowCount)
	if err != nil {
		return err
	}
	arena := make([]byte, valBytes)
	arenaOff := 0

	pos := 0
	var prev []byte
	for range rowCount {
		xorLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		xorData := raw[pos : pos+xorLen]
		pos += xorLen

		// Carve xorLen bytes out of the arena. Capacity is capped (three-index slice) so a
		// stray append on a kept value can never corrupt the next value's backing.
		reconstructed := arena[arenaOff : arenaOff+xorLen : arenaOff+xorLen]
		arenaOff += xorLen
		xorInvertInto(reconstructed, xorData, prev)
		dst.BytesValues = append(dst.BytesValues, reconstructed)
		prev = reconstructed
	}

	// Refs section: rowCount × refSize bytes after all values. The decode loop above has
	// advanced pos to exactly valBytes + 4*rowCount = the refs start (validated by the
	// pre-scan), so no bounds re-check is needed here.
	if wantRefs {
		if _, err := appendVariableWidthRefs(raw, pos, blockW, rowW, rowCount, &dst.BlockRefs); err != nil {
			return fmt.Errorf("decodeXORBytesPage refs: %w", err)
		}
	}
	dst.Count += uint32(rowCount) //nolint:gosec
	return nil
}

// appendDeltaUint64Page decodes a snappy-decoded delta uint64 page blob and appends its
// values and refs directly into dst. Deltas are page-local (acc starts at 0 each page),
// so appending produces the same absolute values regardless of dst's prior contents. NOTE-145.
//
// Wire format (see encodeDeltaUint64Intrinsic):
//
//	for each row i:
//	  uvarint(value[i] - value[i-1])  // value[-1] = 0; all deltas >= 0 (sorted ascending)
//	refs[rowCount × refSize]  — after all varints
//
// NOTE-014: no values_len prefix. Row count comes from the TOC RowCount field.
// Do NOT call pageRefsStart here — that function assumes a values_len[4] prefix.
//
// appendDeltaUint64PageOpt decodes a delta-uint64 page's values and, when wantRefs is true,
// its refs. NOTE-340: wantRefs == false skips the BlockRefs decode entirely for the lazy-ref
// path (appendVariableWidthRefs was ~10.7s of querier self-time on the M1/M4 unfiltered rate
// path, which never reads BlockRefs — only Uint64Values + Count).
func appendDeltaUint64PageOpt(raw []byte, blockW, rowW, rowCount int, dst *IntrinsicColumn, wantRefs bool) error {
	// NOTE-169: index-based varint decode with a single-byte fast path, writing into
	// a pre-extended slice region instead of per-row append. Delta-sorted uint64 columns
	// (span:start, hundreds of pages of ~10k rows each) overwhelmingly produce deltas < 128
	// (one byte), so the common case is a plain byte load + index store with no re-slice,
	// no append cap check, and no generic 10-iteration shift loop. binary.Uvarint(raw[pos:])
	// allocated a fresh slice header and ran its full continuation-bit loop on every row;
	// this fuses the 1-byte case and only falls back to the multi-byte shift loop when the
	// continuation bit is actually set. appendDeltaUint64Page was ~2.1% of querier CPU
	// (profile 2026-06-10) and is the residual decode cost of the unfiltered rate path (M1/M4).
	vals := dst.Uint64Values
	base := len(vals)
	// Pre-extend by rowCount within existing capacity (callers pre-size to totalRows,
	// NOTE-145/150). When capacity is short (rare single-page direct callers) Go grows once.
	if cap(vals)-base >= rowCount {
		vals = vals[:base+rowCount]
	} else {
		vals = append(vals, make([]uint64, rowCount)...)
	}

	// NOTE-256: hoist both per-row bounds checks out of the decode loop, mirroring the
	// NOTE-236 discipline already applied to appendVariableWidthRefs. The previous loop
	// paid two checks per row even on the dominant single-byte fast path: an IsInBounds on
	// the source load `raw[pos]` (the compiler could not connect the `pos >= n` guard to it)
	// and an IsInBounds on the destination store `vals[base+i]`. appendDeltaUint64Page was
	// the #1 blockpack self-time frame (2.6% of querier CPU, profile 2026-06-13) — the
	// residual decode cost of the unfiltered rate path (M1/M4) over hundreds of ~10k-row
	// delta-sorted span:start pages.
	//
	// Source: consume `src` (a window into raw) by exactly the bytes used each row, so its
	// shrinking length is the bound. The single-byte fast path reads src[0] after proving
	// len(src) > 0, so the load is check-free. Destination: reslice to exactly rowCount slots
	// (out := vals[base : base+rowCount]) so the store check is discharged from the loop bound
	// i < rowCount.
	out := vals[base : base+rowCount]
	src := raw
	var acc uint64
	for i := range out {
		if len(src) == 0 {
			return fmt.Errorf("decodeDeltaUint64Page: truncated at uvarint row %d", i)
		}
		b := src[0]
		switch {
		case b < 0x80:
			// Single-byte delta (the overwhelmingly common case): check-free load + store.
			acc += uint64(b)
			src = src[1:]
		case len(src) >= 2 && src[1] < 0x80:
			// NOTE-388: inline 2-byte uvarint. After ascending sort, span:start deltas
			// cluster in [128,16383] (sub-µs to ~16µs inter-span gaps), so the 2-byte
			// case is the dominant non-single-byte path. binary.Uvarint allocated a fresh
			// slice header and ran its generic continuation-bit loop (3.6% of querier CPU,
			// profile 2026-06-15); the 2-byte decode is a single OR-shift with no call,
			// no re-slice, and no loop. The len(src) >= 2 guard plus src[1] < 0x80 prove
			// both bytes are present and the value terminates, so both loads are check-free.
			acc += uint64(b&0x7f) | uint64(src[1])<<7
			src = src[2:]
		default:
			// NOTE-389: inline the >=3-byte uvarint decode instead of calling
			// binary.Uvarint. After ascending sort, span:start gaps larger than ~16µs
			// land in the 3+ byte range; the profile (2026-06-15, 24h M8 window) shows
			// binary.Uvarint still at 2.57% of querier CPU as the only remaining
			// non-inline path here. binary.Uvarint re-slices src into a fresh header and
			// runs a generic loop bounded by binary.MaxVarintLen64 with its own per-byte
			// index checks and an overflow guard the decoder does not need (the writer
			// emits well-formed uvarints). We reach this branch with b >= 0x80
			// (continuation set on byte 0); the 2-byte case above already handled
			// len(src) >= 2 && src[1] < 0x80, so here either len(src) < 2 (truncated) or
			// src[1] >= 0x80 (3+ bytes). Decode byte 0's 7-bit group, then continue from
			// shift 7 over the remaining bytes; src shrinks by exactly the bytes consumed
			// so its length is the bound and the j >= len(src) guard catches truncation.
			acc += uint64(b & 0x7f)
			j := 1
			shift := uint(7)
			for {
				if j >= len(src) {
					return fmt.Errorf("decodeDeltaUint64Page: truncated at uvarint row %d", i)
				}
				c := src[j]
				j++
				if c < 0x80 {
					acc += uint64(c) << shift
					break
				}
				acc += uint64(c&0x7f) << shift
				shift += 7
			}
			src = src[j:]
		}
		out[i] = acc
	}
	pos := len(raw) - len(src)
	dst.Uint64Values = vals

	if wantRefs {
		if _, err := appendVariableWidthRefs(raw, pos, blockW, rowW, rowCount, &dst.BlockRefs); err != nil {
			return fmt.Errorf("decodeDeltaUint64Page refs: %w", err)
		}
	}
	dst.Count += uint32(rowCount) //nolint:gosec
	return nil
}

// xorBytesPageValueSize walks the rowCount length-prefixed values of an XOR-bytes page,
// returning the total reconstructed value bytes. It performs all truncation/bounds
// validation up front (NOTE-147) so the decode loop can size its arena exactly and then
// trust the layout. It reads only the 4-byte length prefixes — the value payloads are
// skipped, so the scan is near-free.
// flatBytesPageValueSize pre-scans a flat-bytes page's len[2]+bytes records and returns the
// total value-byte count, so appendFlatPageOpt can size its single value arena exactly
// (NOTE-356). Validates every length prefix up front; the subsequent decode loop reuses the
// same offset walk knowing the arena is large enough.
func flatBytesPageValueSize(raw []byte, rowCount int) (totalValBytes int, err error) {
	pos := 0
	for range rowCount {
		if pos+2 > len(raw) {
			return 0, fmt.Errorf("DecodeFlatPage: truncated at bytes len")
		}
		vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+vLen > len(raw) {
			return 0, fmt.Errorf("DecodeFlatPage: truncated at bytes value")
		}
		pos += vLen
		totalValBytes += vLen
	}
	return totalValBytes, nil
}

func xorBytesPageValueSize(raw []byte, rowCount int) (totalValBytes int, err error) {
	pos := 0
	for range rowCount {
		if pos+4 > len(raw) {
			return 0, fmt.Errorf("decodeXORBytesPage: truncated at xor_data_len")
		}
		xorLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		if xorLen > MaxBytesLen {
			return 0, fmt.Errorf("decodeXORBytesPage: xorLen %d exceeds MaxBytesLen", xorLen)
		}
		if pos+xorLen > len(raw) {
			return 0, fmt.Errorf("decodeXORBytesPage: truncated at xor_data (len=%d)", xorLen)
		}
		pos += xorLen
		totalValBytes += xorLen
	}
	return totalValBytes, nil
}

// xorInvertInto reconstructs the original value by XOR-inverting xored against prev,
// writing the result into dst (which must be len(xored) bytes). Since xorBytesLen always
// produces len(a) bytes, xor_data_len == original value length == len(dst). NOTE-147.
func xorInvertInto(dst, xored, prev []byte) {
	// NOTE-237: use crypto/subtle.XORBytes for the overlapping prefix instead of a per-byte
	// loop. The byte loop `dst[i] = xored[i] ^ prev[i]` carried three bounds checks per byte
	// (dst[i], xored[i], prev[i]) — for the 16-byte trace:id / 8-byte span:id columns that
	// dominate the XOR-bytes decode path that is 24-48 checks per value. subtle.XORBytes is
	// the stdlib word-wide (8-byte at a time) XOR with the bounds checks hoisted to a single
	// length argument; it is ~33% faster on the 16-byte case (6.3ns -> 4.27ns microbench) and
	// produces byte-identical output. The non-overlapping tail (xored longer than prev — the
	// first row of a page where prev resets to nil) is still a plain copy.
	minLen := min(len(xored), len(prev))
	subtle.XORBytes(dst[:minLen], xored[:minLen], prev[:minLen])
	if len(xored) > minLen {
		copy(dst[minLen:], xored[minLen:])
	}
	// if len(prev) > len(xored): trailing bytes of prev are not part of the result
}

// --- Raw-byte scanning functions ---
// These operate on decompressed column bytes directly, avoiding full struct materialization.
// They decompress the snappy blob once, then scan the raw bytes to extract only matching refs.

// scanDictPageRefs scans a single decompressed dict page blob for matching refs.
// bloomKeys is used to skip pages whose bloom filter rejects all query values.
// Returns (refs, true) when the page was scanned; (nil, false) if skipped.
func scanDictPageRaw(
	pageRaw []byte,
	blockW, rowW int,
	colType ColumnType,
	matchFn func(valueBytes []byte, int64Val int64, isInt64 bool) bool,
	maxRefs int,
	result []BlockRef,
) ([]BlockRef, bool) {
	isInt64 := colType == ColumnTypeInt64 || colType == ColumnTypeRangeInt64
	refSize := blockW + rowW
	pos := 0

	if pos+4 > len(pageRaw) {
		return result, false
	}
	valueCount := int(binary.LittleEndian.Uint32(pageRaw[pos:]))
	pos += 4

	for range valueCount {
		if pos+2 > len(pageRaw) {
			return result, false
		}
		vLen := int(binary.LittleEndian.Uint16(pageRaw[pos:]))
		pos += 2

		// NOTE-277: pass the value as a sub-slice of pageRaw (no string allocation).
		// The dict scan allocated one string(pageRaw[...]) per entry on every page on
		// every predicate scan — even for non-matching entries — only to hand it to a
		// matchFn that does a map lookup or regex match, both of which accept []byte
		// without copying (string(b) map index and re.Match are non-allocating).
		var valBytes []byte
		var i64Val int64
		if isInt64 && vLen == 0 {
			if pos+8 > len(pageRaw) {
				return result, false
			}
			i64Val = int64(binary.LittleEndian.Uint64(pageRaw[pos:])) //nolint:gosec
			pos += 8
		} else {
			if pos+vLen > len(pageRaw) {
				return result, false
			}
			valBytes = pageRaw[pos : pos+vLen]
			pos += vLen
		}

		if pos+4 > len(pageRaw) {
			return result, false
		}
		refCount := int(binary.LittleEndian.Uint32(pageRaw[pos:]))
		pos += 4

		if !matchFn(valBytes, i64Val, isInt64) {
			skip := refCount * refSize
			if skip/refSize != refCount || pos+skip > len(pageRaw) {
				return result, false // corrupt refCount
			}
			pos += skip
			continue
		}
		// NOTE-278: decode the matched value's entire ref run in one batch via
		// appendVariableWidthRefs instead of a per-ref decodeRef + append loop. The
		// matched-refs inner loop paid a per-ref slice bounds check, a per-ref append
		// cap check, a per-ref maxRefs comparison, and (in decodeRef) a per-ref
		// blockW/rowW width branch — exactly the redundant per-row work that NOTE-236
		// hoisted out of the page-decode ref scatter. Low-cardinality predicate columns
		// (e.g. an attribute with a handful of distinct values matched across thousands
		// of spans — the dominant search/metrics predicate shape) produce a single
		// matched dict entry with a very large refCount, so this run is the bulk of the
		// scan's work. appendVariableWidthRefs validates the full run's bounds once and
		// emits a check-free width-specialised copy loop.
		take := refCount
		if maxRefs > 0 {
			if remaining := maxRefs - len(result); remaining < take {
				take = remaining
			}
		}
		newPos, refErr := appendVariableWidthRefs(pageRaw, pos, blockW, rowW, take, &result)
		if refErr != nil {
			// A short final run (page truncated mid-refs) is tolerated by the legacy
			// loop, which returned the refs decoded so far; preserve that by reporting
			// success with whatever was collected.
			return result, true
		}
		if maxRefs > 0 && len(result) >= maxRefs {
			// Cap hit (take may be < refCount); the remaining refs of this run are
			// skipped — the caller stops at maxRefs so position need not advance.
			return result, true
		}
		// take == refCount here (no cap), so newPos is the end of the full run.
		pos = newPos
	}
	return result, true
}

// scanDictPagedBlob handles v2 paged dict column blobs for ScanDictColumnRefs.
func scanDictPagedBlob(
	blob []byte,
	matchFn func(valueBytes []byte, int64Val int64, isInt64 bool) bool,
	bloomKeys [][]byte,
	maxRefs int,
) []BlockRef {
	// NOTE-282: own the TOC decode buffer for the whole scan so per-page Min/Max/Bloom alias
	// it (zero-copy) instead of being copied out per page. Released after the loop.
	tocBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(tocBuf)
	toc, pos, ok := parsePagedBlobHeaderInto(blob, tocBuf)
	if !ok || toc.Format != IntrinsicFormatDict {
		return nil
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)

	// NOTE-239: reuse a pooled snappy-decode scratch buffer across pages. scanDictPageRaw
	// copies out matching values (string(...)) and BlockRef structs before the loop advances,
	// so pageRaw need not outlive the iteration.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	var result []BlockRef
	for _, pm := range toc.Pages {
		// Bloom-filter skip: if bloom keys provided and none pass the bloom, skip page.
		if len(bloomKeys) > 0 && len(pm.Bloom) > 0 {
			anyPass := false
			for _, key := range bloomKeys {
				if TestIntrinsicBloom(pm.Bloom, key) {
					anyPass = true
					break
				}
			}
			if !anyPass {
				continue
			}
		}

		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-263
		if decErr != nil {
			slog.Debug("intrinsic_codec: snappy decode failed", "err", decErr)
			return nil
		}
		var ok bool
		result, ok = scanDictPageRaw(pageRaw, blockW, rowW, toc.ColType, matchFn, maxRefs, result)
		if !ok {
			return nil // corrupt page — return nil to signal failure to caller
		}
		if maxRefs > 0 && len(result) >= maxRefs {
			return result
		}
	}
	return result
}

// ScanDictColumnRefs decompresses a dict column blob and collects BlockRefs only for
// values where matchFn returns true. Non-matching values' refs are skipped by advancing
// the position pointer without allocating BlockRef structs.
//
// Returns nil (not []BlockRef{}) when the blob is not a dict column or on decode error,
// allowing callers to distinguish "not applicable" from "no matches" (empty slice).
func ScanDictColumnRefs(
	blob []byte,
	matchFn func(valueBytes []byte, int64Val int64, isInt64 bool) bool,
	maxRefs int,
) []BlockRef {
	// v2 paged format.
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanDictPagedBlob(blob, matchFn, nil, maxRefs)
	}

	raw, err := decodeBoundedSnappyColumn(blob)
	if err != nil {
		slog.Debug("intrinsic_codec: snappy decode failed", "err", err)
		return nil
	}
	if len(raw) < 3 {
		return nil
	}
	pos := 0
	pos++ // format_version
	format := raw[pos]
	pos++
	colType := ColumnType(raw[pos])
	pos++

	if format != IntrinsicFormatDict {
		return nil
	}
	isInt64 := colType == ColumnTypeInt64 || colType == ColumnTypeRangeInt64

	if pos+4 > len(raw) {
		return nil
	}
	valueCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	if pos+2 > len(raw) {
		return nil
	}
	blockW := int(raw[pos])
	rowW := int(raw[pos+1])
	pos += 2
	refSize := blockW + rowW

	var result []BlockRef
	for range valueCount {
		if pos+2 > len(raw) {
			return nil
		}
		vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2

		// Read value. NOTE-277: pass the value as a sub-slice of raw (no string copy).
		var valBytes []byte
		var i64Val int64
		if isInt64 && vLen == 0 {
			if pos+8 > len(raw) {
				return nil
			}
			i64Val = int64(binary.LittleEndian.Uint64(raw[pos:])) //nolint:gosec
			pos += 8
		} else {
			if pos+vLen > len(raw) {
				return nil
			}
			valBytes = raw[pos : pos+vLen]
			pos += vLen
		}

		if pos+4 > len(raw) {
			return nil
		}
		refCount := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4

		if !matchFn(valBytes, i64Val, isInt64) {
			// Skip all refs for this value — no struct allocation.
			skip := refCount * refSize
			if skip/refSize != refCount || pos+skip > len(raw) {
				return nil // corrupt refCount
			}
			pos += skip
			continue
		}

		// NOTE-278: batch-decode the matched value's ref run (see scanDictPageRaw for
		// rationale) rather than a per-ref decodeRef + append loop.
		take := refCount
		if maxRefs > 0 {
			if remaining := maxRefs - len(result); remaining < take {
				take = remaining
			}
		}
		newPos, refErr := appendVariableWidthRefs(raw, pos, blockW, rowW, take, &result)
		if refErr != nil {
			return nil
		}
		if maxRefs > 0 && len(result) >= maxRefs {
			return result
		}
		// take == refCount here (no cap), so newPos is the end of the full run.
		pos = newPos
	}
	return result
}

// ScanDictColumnRefsWithBloom is like ScanDictColumnRefs but also accepts bloom filter
// keys to skip pages in v2 paged blobs whose bloom filter rejects all query values.
// For v1 monolithic blobs, bloomKeys is ignored.
func ScanDictColumnRefsWithBloom(
	blob []byte,
	matchFn func(valueBytes []byte, int64Val int64, isInt64 bool) bool,
	bloomKeys [][]byte,
	maxRefs int,
) []BlockRef {
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanDictPagedBlob(blob, matchFn, bloomKeys, maxRefs)
	}
	return ScanDictColumnRefs(blob, matchFn, maxRefs)
}

// parsePagedBlobHeaderInto parses the v2 paged blob header: sentinel[1] + toc_len[4] +
// toc_blob, decoding the page TOC into the caller-owned pooled buffer *tocBuf and aliasing
// each page's Min/Max/Bloom into it (no per-page copy — NOTE-282). Returns the decoded TOC,
// the byte offset of the first page blob, and success. The caller MUST keep *tocBuf alive
// (release only after the scan loop finishes) since the returned stats point into it.
func parsePagedBlobHeaderInto(blob []byte, tocBuf *[]byte) (PagedIntrinsicTOC, int, bool) {
	if len(blob) < 5 {
		return PagedIntrinsicTOC{}, 0, false
	}
	tocLen := int(binary.LittleEndian.Uint32(blob[1:5]))
	if 5+tocLen > len(blob) {
		return PagedIntrinsicTOC{}, 0, false
	}
	toc, err := DecodePageTOCInto(blob[5:5+tocLen], tocBuf)
	if err != nil {
		slog.Error("parsePagedBlobHeaderInto: DecodePageTOCInto failed", "err", err)
		return PagedIntrinsicTOC{}, 0, false
	}
	return toc, 5 + tocLen, true
}

// findRangeBoundaries varint-scans the values section of a flat page to locate
// the first and last row indices whose accumulated value falls within [lo, hi].
// pageRaw is the decompressed page; refsStart is the byte offset where the refs
// section begins (used as the end-of-values boundary); rowCount is the number of rows.
// Returns (startIdx, endIdx) where startIdx == -1 means no rows matched.
func findRangeBoundaries(
	pageRaw []byte,
	refsStart, rowCount int,
	lo, hi uint64,
	hasLo, hasHi bool,
) (startIdx, endIdx int) {
	var acc uint64
	startIdx = -1
	endIdx = rowCount
	valPos := 4 // skip values_len prefix
	valEnd := refsStart
	for i := range rowCount {
		if valPos >= valEnd {
			break
		}
		delta, n := binary.Uvarint(pageRaw[valPos:valEnd])
		if n <= 0 {
			break
		}
		acc += delta
		valPos += n
		if startIdx < 0 && (!hasLo || acc >= lo) {
			startIdx = i
		}
		if hasHi && acc > hi {
			endIdx = i
			break
		}
	}
	return startIdx, endIdx
}

// scanFlatPagedBlob handles v2 paged flat column blobs for range scan.
func scanFlatPagedBlob(blob []byte, lo, hi uint64, hasLo, hasHi bool, maxRefs int) []BlockRef {
	// NOTE-282: own the TOC decode buffer for the whole scan (incl. any delegated delta scan,
	// which reads the same zero-copy stats) so per-page Min/Max alias it instead of being copied.
	tocBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(tocBuf)
	toc, pos, ok := parsePagedBlobHeaderInto(blob, tocBuf)
	if !ok || toc.ColType == ColumnTypeBytes {
		return nil
	}
	if toc.Format == IntrinsicFormatDict {
		return nil
	}
	if toc.Format == IntrinsicFormatDeltaUint64 {
		return scanDeltaUint64PagedBlob(blob, toc, pos, lo, hi, hasLo, hasHi, maxRefs)
	}
	if toc.Format != IntrinsicFormatFlat {
		return nil
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	refSize := blockW + rowW

	// NOTE-239: pooled snappy scratch reused across pages. decodeRef copies each ref into
	// result, so pageRaw is not retained past the iteration.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	var result []BlockRef
	for _, pm := range toc.Pages {
		// Min/max skip: if page range doesn't overlap [lo, hi], skip page.
		if len(pm.Min) == 8 && len(pm.Max) == 8 {
			pageMin := leUint64FromString(pm.Min) // NOTE-274: avoid []byte(...) copy
			pageMax := leUint64FromString(pm.Max)
			if hasLo && pageMax < lo {
				continue
			}
			if hasHi && pageMin > hi {
				continue
			}
		}

		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-263
		if decErr != nil {
			slog.Debug("intrinsic_codec: snappy decode failed", "err", decErr)
			return nil
		}

		rowCount := int(pm.RowCount)
		refsStart := pageRefsStart(pageRaw)
		if refsStart > len(pageRaw) {
			return nil // corrupt page: values_len exceeds page size
		}

		startIdx, endIdx := findRangeBoundaries(pageRaw, refsStart, rowCount, lo, hi, hasLo, hasHi)
		if startIdx < 0 || startIdx >= endIdx {
			continue
		}
		count := endIdx - startIdx
		if maxRefs > 0 && len(result)+count > maxRefs {
			count = maxRefs - len(result)
		}
		refPos := refsStart + startIdx*refSize
		for range count {
			if refPos+refSize > len(pageRaw) {
				break
			}
			result = append(result, decodeRef(pageRaw, refPos, blockW, rowW))
			refPos += refSize
		}
		if maxRefs > 0 && len(result) >= maxRefs {
			return result
		}
	}
	if result == nil {
		// NOTE-400: distinguish "evaluable, no rows matched" (non-nil empty) from the
		// error/not-flat paths above (nil). The v1 ScanFlatColumnRefs already returns a
		// non-nil empty slice for an evaluable-but-empty scan, and the executor leaf path
		// (scanIntrinsicLeafRefs) treats a nil result as "leaf unevaluable" → abandon the
		// whole intrinsic pre-filter. A paged column whose predicate (range OR equality)
		// simply matches no row is still fully evaluable, so it must return empty, not nil,
		// or every selective query that legitimately matches zero rows in this block would
		// wrongly fall back to a full block scan.
		return []BlockRef{}
	}
	return result
}

// scanFlatPagedFiltered handles v2 paged flat column blobs for filtered scan.
// When filter is nil, all refs are accepted (equivalent to the former scanFlatPagedTopK fast path).
func scanFlatPagedFiltered(blob []byte, backward bool, limit int, filter func(BlockRef) bool) []BlockRef {
	// NOTE-282: own the TOC decode buffer for the whole scan (incl. any delegated delta scan)
	// so per-page Min/Max alias it (zero-copy) instead of being copied out per page.
	tocBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(tocBuf)
	toc, pos, ok := parsePagedBlobHeaderInto(blob, tocBuf)
	if !ok || toc.ColType == ColumnTypeBytes {
		return nil
	}
	if toc.Format == IntrinsicFormatDict {
		return nil
	}
	if toc.Format == IntrinsicFormatDeltaUint64 {
		return scanDeltaUint64PagedFiltered(blob, toc, pos, backward, limit, filter)
	}
	if toc.Format != IntrinsicFormatFlat {
		return nil
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	refSize := blockW + rowW

	result := make([]BlockRef, 0, limit)

	// NOTE-239: pooled snappy scratch reused across pages. decodeRef copies each ref into
	// result, so pageRaw is not retained past the closure call.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	scanPage := func(pm PageMeta) bool {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return false
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-263
		if decErr != nil {
			slog.Debug("intrinsic_codec: snappy decode failed", "err", decErr)
			return false
		}
		rowCount := int(pm.RowCount)
		refsStart := pageRefsStart(pageRaw)

		if backward {
			for i := rowCount - 1; i >= 0 && len(result) < limit; i-- {
				refPos := refsStart + i*refSize
				if refPos+refSize > len(pageRaw) {
					continue
				}
				ref := decodeRef(pageRaw, refPos, blockW, rowW)
				if filter == nil || filter(ref) {
					result = append(result, ref)
				}
			}
		} else {
			for i := range rowCount {
				if len(result) >= limit {
					break
				}
				refPos := refsStart + i*refSize
				if refPos+refSize > len(pageRaw) {
					break
				}
				ref := decodeRef(pageRaw, refPos, blockW, rowW)
				if filter == nil || filter(ref) {
					result = append(result, ref)
				}
			}
		}
		return true
	}

	if backward {
		for i := len(toc.Pages) - 1; i >= 0; i-- {
			if len(result) >= limit {
				break
			}
			if !scanPage(toc.Pages[i]) {
				return nil
			}
		}
	} else {
		for _, pm := range toc.Pages {
			if len(result) >= limit {
				break
			}
			if !scanPage(pm) {
				return nil
			}
		}
	}
	return result
}

// scanDeltaUint64PageRange performs a single-pass uvarint scan to find the matching
// range [startIdx, endIdx) and the refs-section byte offset p.
// NOTE-017: no allocation — values are never materialized as a slice.
// DeltaUint64 values are monotonically non-decreasing (deltas >= 0), so early
// termination on acc > hi is correct. Returns ok=false on decode error.
func scanDeltaUint64PageRange(
	pageRaw []byte,
	rowCount int,
	lo, hi uint64,
	hasLo, hasHi bool,
) (startIdx, endIdx, p int, ok bool) {
	startIdx = -1
	endIdx = rowCount
	var acc uint64
	for i := range rowCount {
		delta, n := binary.Uvarint(pageRaw[p:])
		if n <= 0 {
			return 0, 0, 0, false
		}
		acc += delta
		p += n
		if startIdx < 0 && (!hasLo || acc >= lo) {
			startIdx = i
		}
		if hasHi && acc > hi {
			endIdx = i
			// Advance p to refs section start: scan remaining uvarints.
			for j := i + 1; j < rowCount; j++ {
				_, n = binary.Uvarint(pageRaw[p:])
				if n <= 0 {
					return 0, 0, 0, false
				}
				p += n
			}
			break
		}
	}
	return startIdx, endIdx, p, true
}

// scanDeltaUint64PagedBlob handles range scan for IntrinsicFormatDeltaUint64 paged blobs.
// It decodes the uvarint stream in a single pass via scanDeltaUint64PageRange, finding
// startIdx and endIdx for the matching range [lo, hi], then reads only refs in that range.
//
// NOTE-014: must NOT call pageRefsStart — DeltaUint64 pages have no values_len prefix.
// NOTE-017: single-pass streaming decode eliminates make([]uint64, rowCount) allocation.
func scanDeltaUint64PagedBlob(
	blob []byte,
	toc PagedIntrinsicTOC,
	pos int,
	lo, hi uint64,
	hasLo, hasHi bool,
	maxRefs int,
) []BlockRef {
	if len(toc.Pages) == 0 {
		return nil
	}
	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	// BUG-13 fix: reject invalid width values from corrupt blobs.
	if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) {
		return nil
	}
	refSize := blockW + rowW

	// NOTE-239: pooled snappy scratch reused across pages. decodeRef copies each ref into
	// result, so pageRaw is not retained past the iteration.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	var result []BlockRef
	for _, pm := range toc.Pages {
		// NOTE-017: min/max page skip — mirrors scanFlatPagedBlob lines 1233-1243.
		// encodeDeltaUint64Intrinsic writes Min/Max into PageMeta; the reader was not using them.
		if len(pm.Min) == 8 && len(pm.Max) == 8 {
			pageMin := leUint64FromString(pm.Min) // NOTE-274: avoid []byte(...) copy
			pageMax := leUint64FromString(pm.Max)
			if hasLo && pageMax < lo {
				continue
			}
			if hasHi && pageMin > hi {
				continue
			}
		}

		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-263
		if decErr != nil {
			slog.Debug("intrinsic_codec: snappy decode failed", "err", decErr)
			return nil
		}

		startIdx, endIdx, p, ok := scanDeltaUint64PageRange(pageRaw, int(pm.RowCount), lo, hi, hasLo, hasHi)
		if !ok {
			return nil
		}
		// refs section starts at p.
		if startIdx < 0 || startIdx >= endIdx {
			continue
		}
		count := endIdx - startIdx
		if maxRefs > 0 && len(result)+count > maxRefs {
			count = maxRefs - len(result)
		}
		refPos := p + startIdx*refSize
		for range count {
			if refPos+refSize > len(pageRaw) {
				break
			}
			result = append(result, decodeRef(pageRaw, refPos, blockW, rowW))
			refPos += refSize
		}
		if maxRefs > 0 && len(result) >= maxRefs {
			return result
		}
	}
	if result == nil {
		// NOTE-400: mirror scanFlatPagedBlob — evaluable-but-empty must be non-nil empty so
		// the executor leaf path does not mistake a legitimate zero-match scan for an
		// unevaluable leaf (which would abandon the intrinsic pre-filter).
		return []BlockRef{}
	}
	return result
}

// scanDeltaUint64PagedFiltered handles filtered scan for IntrinsicFormatDeltaUint64 paged blobs.
// Decodes the uvarint stream to reconstruct absolute values, collects refs matching filter.
//
// NOTE-014: must NOT call pageRefsStart — DeltaUint64 pages have no values_len prefix.
func scanDeltaUint64PagedFiltered(
	blob []byte,
	toc PagedIntrinsicTOC,
	pos int,
	backward bool,
	limit int,
	filter func(BlockRef) bool,
) []BlockRef {
	if len(toc.Pages) == 0 {
		return nil
	}
	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	// BUG-13 fix: reject invalid width values from corrupt blobs.
	if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) {
		return nil
	}
	refSize := blockW + rowW

	result := make([]BlockRef, 0, limit)

	// NOTE-239: pooled snappy scratch reused across pages. decodeRef copies each ref into
	// result, so pageRaw is not retained past the closure call.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	scanPage := func(pm PageMeta) bool {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return false
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-263
		if decErr != nil {
			slog.Debug("intrinsic_codec: snappy decode failed", "err", decErr)
			return false
		}
		rowCount := int(pm.RowCount)
		p := 0
		for range rowCount {
			_, n := binary.Uvarint(pageRaw[p:])
			if n <= 0 {
				return false
			}
			p += n
		}
		// p is now the start of the refs section.
		if backward {
			for i := rowCount - 1; i >= 0 && len(result) < limit; i-- {
				refPos := p + i*refSize
				if refPos+refSize > len(pageRaw) {
					continue
				}
				ref := decodeRef(pageRaw, refPos, blockW, rowW)
				if filter == nil || filter(ref) {
					result = append(result, ref)
				}
			}
		} else {
			for i := range rowCount {
				if len(result) >= limit {
					break
				}
				refPos := p + i*refSize
				if refPos+refSize > len(pageRaw) {
					break
				}
				ref := decodeRef(pageRaw, refPos, blockW, rowW)
				if filter == nil || filter(ref) {
					result = append(result, ref)
				}
			}
		}
		return true
	}

	if backward {
		for i := len(toc.Pages) - 1; i >= 0; i-- {
			if len(result) >= limit {
				break
			}
			if !scanPage(toc.Pages[i]) {
				return nil
			}
		}
	} else {
		for _, pm := range toc.Pages {
			if len(result) >= limit {
				break
			}
			if !scanPage(pm) {
				return nil
			}
		}
	}
	return result
}

// ScanFlatColumnRefs decompresses a flat uint64 column blob and collects BlockRefs
// only for values in the range [lo, hi]. Values are delta-decoded in a streaming pass
// (no slice allocation for values). Only refs in the matching range are materialized.
//
// If hasLo is false, lo is ignored (scan from start). If hasHi is false, hi is ignored (scan to end).
// Returns nil when the blob is not a flat uint64 column or on decode error.
func ScanFlatColumnRefs(
	blob []byte,
	lo, hi uint64,
	hasLo, hasHi bool,
	maxRefs int,
) []BlockRef {
	// v2 paged format.
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanFlatPagedBlob(blob, lo, hi, hasLo, hasHi, maxRefs)
	}

	raw, err := decodeBoundedSnappyColumn(blob)
	if err != nil {
		slog.Debug("intrinsic_codec: snappy decode failed", "err", err)
		return nil
	}
	if len(raw) < 3 {
		return nil
	}
	pos := 0
	pos++ // format_version
	format := raw[pos]
	pos++
	colType := ColumnType(raw[pos])
	pos++

	if format != IntrinsicFormatFlat || colType == ColumnTypeBytes {
		return nil
	}

	if pos+4 > len(raw) {
		return nil
	}
	rowCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	if pos+2 > len(raw) {
		return nil
	}
	blockW := int(raw[pos])
	rowW := int(raw[pos+1])
	pos += 2
	// BUG-13 fix: reject invalid width values from corrupt blobs.
	if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) {
		return nil
	}
	refSize := blockW + rowW

	valuesStart := pos
	// BUG-1 fix: use division-based check to avoid int overflow on 32-bit platforms.
	// rowCount*8 overflows int when rowCount > math.MaxInt/8; (len(raw)-valuesStart)/8 is safe.
	if rowCount < 0 || rowCount > (len(raw)-valuesStart)/8 {
		return nil
	}
	refsStart := valuesStart + rowCount*8

	// Streaming delta-decode to find start and end indices.
	var acc uint64
	startIdx := -1
	endIdx := rowCount

	// Scan values to find the matching range boundaries.
	for i := range rowCount {
		if valuesStart+i*8+8 > len(raw) {
			return nil
		}
		acc += binary.LittleEndian.Uint64(raw[valuesStart+i*8:])
		if startIdx < 0 && (!hasLo || acc >= lo) {
			startIdx = i
		}
		if hasHi && acc > hi {
			endIdx = i
			break
		}
	}

	if startIdx < 0 || startIdx >= endIdx {
		return []BlockRef{} // no matches but evaluable
	}

	// Decode only refs in [startIdx, endIdx).
	count := endIdx - startIdx
	if maxRefs > 0 && count > maxRefs {
		count = maxRefs
	}
	result := make([]BlockRef, count)
	refPos := refsStart + startIdx*refSize
	for i := range count {
		if refPos+refSize > len(raw) {
			return result[:i]
		}
		result[i] = decodeRef(raw, refPos, blockW, rowW)
		refPos += refSize
	}
	return result
}

// ScanFlatColumnTopKRefs decompresses a flat uint64 column blob and returns the last
// `limit` BlockRefs (for backward/MostRecent) or first `limit` BlockRefs (for forward).
// Since flat columns are sorted ascending, the last refs correspond to the newest timestamps.
// No value decoding is performed — only the refs section is read.
//
// Returns nil when the blob is not a flat uint64 column or on decode error.
func ScanFlatColumnTopKRefs(blob []byte, limit int, backward bool) []BlockRef {
	// v2 paged format.
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanFlatPagedFiltered(blob, backward, limit, nil)
	}

	raw, err := decodeBoundedSnappyColumn(blob)
	if err != nil {
		slog.Debug("intrinsic_codec: snappy decode failed", "err", err)
		return nil
	}
	if len(raw) < 3 {
		return nil
	}
	pos := 0
	pos++ // format_version
	format := raw[pos]
	pos++
	colType := ColumnType(raw[pos])
	pos++

	if format != IntrinsicFormatFlat || colType == ColumnTypeBytes {
		return nil
	}

	if pos+4 > len(raw) {
		return nil
	}
	rowCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	if pos+2 > len(raw) {
		return nil
	}
	blockW := int(raw[pos])
	rowW := int(raw[pos+1])
	pos += 2
	// BUG-13 fix: reject invalid width values from corrupt blobs.
	if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) {
		return nil
	}
	refSize := blockW + rowW

	// BUG-1 fix: overflow-safe check — rowCount > remaining/8 without computing rowCount*8.
	remaining := len(raw) - pos
	if rowCount < 0 || rowCount > remaining/8 {
		return nil
	}
	refsStart := pos + rowCount*8 // skip values section

	count := min(limit, rowCount)
	if count <= 0 {
		return nil
	}

	result := make([]BlockRef, count)
	if backward {
		// Read last `count` refs.
		refPos := refsStart + (rowCount-count)*refSize
		for i := range count {
			if refPos+refSize > len(raw) {
				return result[:i]
			}
			result[i] = decodeRef(raw, refPos, blockW, rowW)
			refPos += refSize
		}
	} else {
		// Read first `count` refs.
		refPos := refsStart
		for i := range count {
			if refPos+refSize > len(raw) {
				return result[:i]
			}
			result[i] = decodeRef(raw, refPos, blockW, rowW)
			refPos += refSize
		}
	}
	return result
}

// ScanFlatColumnRefsFiltered decompresses a flat column blob and iterates refs
// (skipping value decode entirely) calling filter for each ref. Refs where filter
// returns true are collected. Iteration order is backward (last ref first) when
// backward=true, forward otherwise. Stops after limit matches.
//
// This is optimal for top-K timestamp scans where we need refs in timestamp order
// but don't need the actual timestamp values — the sorted order is implicit in position.
func ScanFlatColumnRefsFiltered(
	blob []byte,
	backward bool,
	limit int,
	filter func(BlockRef) bool,
) []BlockRef {
	// v2 paged format.
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanFlatPagedFiltered(blob, backward, limit, filter)
	}

	raw, err := decodeBoundedSnappyColumn(blob)
	if err != nil {
		slog.Debug("intrinsic_codec: snappy decode failed", "err", err)
		return nil
	}
	if len(raw) < 3 {
		return nil
	}
	pos := 0
	pos++ // format_version
	format := raw[pos]
	pos++
	colType := ColumnType(raw[pos])
	pos++

	if format != IntrinsicFormatFlat || colType == ColumnTypeBytes {
		return nil
	}

	if pos+4 > len(raw) {
		return nil
	}
	rowCount := int(binary.LittleEndian.Uint32(raw[pos:]))
	pos += 4

	if pos+2 > len(raw) {
		return nil
	}
	blockW := int(raw[pos])
	rowW := int(raw[pos+1])
	pos += 2
	// BUG-13 fix: reject invalid width values from corrupt blobs.
	if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) {
		return nil
	}
	refSize := blockW + rowW

	// BUG-1 fix: overflow-safe refsStart guard.
	if pos > len(raw) {
		return nil
	}
	if rowCount < 0 || rowCount > (len(raw)-pos)/8 {
		return nil
	}
	refsStart := pos + rowCount*8 // skip values section entirely

	if limit <= 0 || rowCount == 0 {
		return nil
	}

	result := make([]BlockRef, 0, limit)
	if backward {
		for i := rowCount - 1; i >= 0 && len(result) < limit; i-- {
			refPos := refsStart + i*refSize
			if refPos+refSize > len(raw) {
				continue
			}
			ref := decodeRef(raw, refPos, blockW, rowW)
			if filter(ref) {
				result = append(result, ref)
			}
		}
	} else {
		for i := range rowCount {
			if len(result) >= limit {
				break
			}
			refPos := refsStart + i*refSize
			if refPos+refSize > len(raw) {
				break
			}
			ref := decodeRef(raw, refPos, blockW, rowW)
			if filter(ref) {
				result = append(result, ref)
			}
		}
	}
	return result
}

// pageRefsStart returns the byte offset of the refs section in a flat page blob.
// For uint64 pages: values_len[4 LE] + varint_values[values_len], so refs start at 4 + values_len.
// For bytes pages (no values_len prefix): caller must compute from scanning values.
func pageRefsStart(pageRaw []byte) int {
	if len(pageRaw) < 4 {
		return 0
	}
	valuesLen := int(binary.LittleEndian.Uint32(pageRaw[:4]))
	return 4 + valuesLen
}

// DecodeRefAt reads the (blockIdx, rowIdx) ref at byte offset pos in raw using the column's
// blockW/rowW ref widths. It is the exported, struct-free counterpart of decodeRef used by the
// streaming Dict group-by scatter (NOTE-407): the executor reads each ref straight out of a
// page's raw ref run and packs it into a packKey without ever materializing a BlockRef.
func DecodeRefAt(raw []byte, pos, blockW, rowW int) (blockIdx, rowIdx uint16) {
	if blockW == 1 {
		blockIdx = uint16(raw[pos])
	} else {
		blockIdx = binary.LittleEndian.Uint16(raw[pos:])
	}
	if rowW == 1 {
		rowIdx = uint16(raw[pos+blockW])
	} else {
		rowIdx = binary.LittleEndian.Uint16(raw[pos+blockW:])
	}
	return blockIdx, rowIdx
}

func decodeRef(raw []byte, pos, blockW, rowW int) BlockRef {
	blockIdx, rowIdx := DecodeRefAt(raw, pos, blockW, rowW)
	return BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx}
}
