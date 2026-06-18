package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides the streaming (decode-time push-down) consumption path for paged
// value-decoupled intrinsic columns (Flat/XOR/Delta — IntrinsicFormatFlat /
// IntrinsicFormatXORBytes / IntrinsicFormatDeltaUint64). NOTE-406 / issue #348.
//
// The eager full-column decode (decodePagedColumnBlobOpt, reached via GetIntrinsicColumn)
// materializes col.Uint64Values / col.BytesValues / col.BlockRefs sized to the WHOLE
// column's row count — arrays that, for unfiltered full-column group-by / scatter, exist
// only to be scanned once and discarded. ScanPagedColumnBlob fuses decode and consumption:
// it decodes ONE page at a time into buffers reused across pages and hands each page to a
// visitor, so the full-column arrays are never allocated. Transient allocation is O(one
// page), reused — not O(column).
//
// Page-batch granularity (a whole DecodedPage per visit call, not a per-value callback) is
// deliberate: a per-value callback would pay an un-inlinable indirect call per row; visiting
// a whole page amortizes that indirect call to near-zero over the page's rows.

import (
	"encoding/binary"
	"fmt"
)

// DecodedPage holds one page's decoded values + refs in buffers that ScanPagedColumnBlob
// reuses across pages. A DecodedPage handed to a visit callback is valid ONLY for the
// duration of that call: the next page overwrites the same buffers. A visitor that needs to
// retain any value/ref beyond its call MUST copy it. RowBase is the row index (column
// position) of this page's first row, so the visitor can map page-local index i to the
// column position RowBase+i.
//
// Exactly one of Uint64Values / BytesValues is populated per page (a column is uint64 OR
// bytes, never both), determined by Type. BlockRefs is always populated and parallel to the
// value slice (BlockRefs[i] is the ref of value i). DictEntries is unused by the streaming
// path (Dict columns share a cross-page arena and are not streamed here, NOTE-406).
type DecodedPage struct {
	BlockRefs    []BlockRef
	Uint64Values []uint64
	BytesValues  [][]byte
	Type         ColumnType
	Format       uint8
	RowBase      uint32
}

// PageStats carries a single page's pruning statistics, presented to a ScanPagedColumnBlobWithStats
// prefilter BEFORE the page's values are decoded. Min/Max are the page's value range (valid only
// when HasMinMax — encodeDeltaUint64Intrinsic/Flat write 8-byte LE Min/Max, the Min/Max page-skip
// the scan path already uses). RowCount is the number of rows in the page; RowBase is the column
// position of the page's first row. A prefilter that can fully account for the page from these
// stats alone (e.g. count all RowCount rows into one time bucket, or skip a page entirely outside
// the query window) returns true to SKIP the per-value decode for that page (NOTE-444, issue #363).
type PageStats struct {
	Min       uint64
	Max       uint64
	RowCount  uint32
	RowBase   uint32
	HasMinMax bool
}

// IsStreamablePagedColumnBlob reports whether blob is a v2 paged column in a value-decoupled
// format (Flat/XOR/Delta) that ScanPagedColumnBlob can stream. Dict columns and legacy v1
// blobs return false — the caller must fall back to the eager DecodeIntrinsicColumnBlob path
// for those. NOTE-406.
func IsStreamablePagedColumnBlob(blob []byte) bool {
	if len(blob) < 5 || blob[0] != IntrinsicPagedVersion {
		return false
	}
	pos := 1
	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4
	if pos+tocLen > len(blob) {
		return false
	}
	toc, err := DecodePageTOCNoStats(blob[pos : pos+tocLen])
	if err != nil {
		return false
	}
	return isParallelPageDecodeFormat(toc.Format)
}

// ScanPagedColumnBlob decodes a v2 paged Flat/XOR/Delta column blob ONE PAGE AT A TIME into
// reused buffers and invokes visit on each decoded page, in row order. The full-column value
// and ref arrays are never materialized — transient allocation is O(one page), reused across
// pages (NOTE-406, issue #348).
//
// Pages are presented in ascending row order with DecodedPage.RowBase set to the page's first
// row index, so a visitor that scatters by column position is order-independent or processes
// in order — either is safe (pages are emitted in row order).
//
// blob MUST be a streamable paged column (see IsStreamablePagedColumnBlob); ScanPagedColumnBlob
// returns an error for Dict / legacy v1 blobs rather than silently misdecoding. The visitor's
// returned error short-circuits the scan and is returned to the caller.
//
// The DecodedPage and every slice it references are valid ONLY for the duration of a single
// visit call — the next page reuses the same backing buffers. A visitor that retains any value
// or ref MUST copy it.
func ScanPagedColumnBlob(blob []byte, visit func(*DecodedPage) error) error {
	if len(blob) < 5 || blob[0] != IntrinsicPagedVersion {
		return fmt.Errorf("ScanPagedColumnBlob: not a v2 paged column blob")
	}
	pos := 1
	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4
	if pos+tocLen > len(blob) {
		return fmt.Errorf("ScanPagedColumnBlob: truncated at toc_blob")
	}
	toc, err := DecodePageTOCNoStats(blob[pos : pos+tocLen])
	if err != nil {
		return fmt.Errorf("ScanPagedColumnBlob: %w", err)
	}
	pos += tocLen // pos now points to first page blob

	if !isParallelPageDecodeFormat(toc.Format) {
		return fmt.Errorf("ScanPagedColumnBlob: format %d is not streamable (Dict/legacy not supported)", toc.Format)
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)

	// Per-page scratch column: the append* helpers decode into its slices. We reset the
	// value/ref slices to length 0 before each page (retaining their backing capacity) so
	// the decode reuses the same buffers across pages — O(one page) transient, not O(column).
	// Refs are ALWAYS decoded (wantRefs == true): a streaming consumer scatters values by
	// their refs, so both sides are needed per page.
	scratch := &IntrinsicColumn{Type: toc.ColType, Format: toc.Format}
	page := &DecodedPage{Type: toc.ColType, Format: toc.Format}

	// NOTE-012: reuse a pooled snappy decode buffer across page decompressions.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	var rowBase uint32
	for i, pm := range toc.Pages {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return fmt.Errorf("ScanPagedColumnBlob: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageCompressed := blob[pageStart:pageEnd]
		pageRaw, decErr := snappyDecodeReuse(pageBuf, pageCompressed) // NOTE-262
		if decErr != nil {
			return fmt.Errorf("ScanPagedColumnBlob: page %d snappy: %w", i, decErr)
		}

		// Reset the scratch slices to length 0 (keep capacity) so this page decodes into the
		// reused backing arrays. Count is reset too — the append helpers bump it per page.
		scratch.Uint64Values = scratch.Uint64Values[:0]
		scratch.BytesValues = scratch.BytesValues[:0]
		scratch.BlockRefs = scratch.BlockRefs[:0]
		scratch.Count = 0

		rowCount := int(pm.RowCount)
		switch toc.Format {
		case IntrinsicFormatFlat:
			err = appendFlatPageOpt(pageRaw, blockW, rowW, rowCount, toc.ColType, scratch, true)
		case IntrinsicFormatXORBytes:
			err = appendXORBytesPageOpt(pageRaw, blockW, rowW, rowCount, scratch, true)
		case IntrinsicFormatDeltaUint64:
			err = appendDeltaUint64PageOpt(pageRaw, blockW, rowW, rowCount, scratch, true)
		default:
			return fmt.Errorf("ScanPagedColumnBlob: page %d: unknown format %d", i, toc.Format)
		}
		if err != nil {
			return fmt.Errorf("ScanPagedColumnBlob: page %d: %w", i, err)
		}

		page.Uint64Values = scratch.Uint64Values
		page.BytesValues = scratch.BytesValues
		page.BlockRefs = scratch.BlockRefs
		page.RowBase = rowBase

		if vErr := visit(page); vErr != nil {
			return vErr
		}
		rowBase += uint32(rowCount) //nolint:gosec // row count per page << 4 GiB
	}
	return nil
}

// ScanPagedColumnBlobWithStats is ScanPagedColumnBlob plus a page-level pruning prefilter (NOTE-444,
// issue #363). Before decoding a page's values, it presents that page's PageStats (Min/Max/RowCount/
// RowBase) to prefilter. When prefilter returns true the page is fully accounted for from its stats
// alone and the per-value decode (appendFlatPageOpt / appendXORBytesPageOpt / appendDeltaUint64PageOpt
// — the latter the 14.5%-of-querier-CPU self-time frame on the span:start rate hot path) is SKIPPED;
// visit is NOT called for that page. When prefilter returns false the page is decoded and handed to
// visit exactly as ScanPagedColumnBlob does.
//
// Unlike ScanPagedColumnBlob, this decodes the stats TOC (DecodePageTOC) so each page's Min/Max are
// available to the prefilter; that costs a per-page Min/Max string materialization (NOTE-274), paid
// only on this stats path — the plain ScanPagedColumnBlob stays on the no-stats TOC. The win is that
// for time-ordered columns whose page time span ≤ the query step (the common span:start rate case),
// most pages are pruned/bulk-counted by the prefilter and never decode a single value.
//
// prefilter may be nil, in which case every page is decoded and visited (equivalent to
// ScanPagedColumnBlob). The DecodedPage validity contract is identical to ScanPagedColumnBlob: it and
// its slices are valid only for the duration of a single visit call.
func ScanPagedColumnBlobWithStats(
	blob []byte,
	prefilter func(*PageStats) bool,
	visit func(*DecodedPage) error,
) error {
	if len(blob) < 5 || blob[0] != IntrinsicPagedVersion {
		return fmt.Errorf("ScanPagedColumnBlobWithStats: not a v2 paged column blob")
	}
	pos := 1
	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4
	if pos+tocLen > len(blob) {
		return fmt.Errorf("ScanPagedColumnBlobWithStats: truncated at toc_blob")
	}
	toc, err := DecodePageTOC(blob[pos : pos+tocLen]) // stats TOC: per-page Min/Max for the prefilter
	if err != nil {
		return fmt.Errorf("ScanPagedColumnBlobWithStats: %w", err)
	}
	pos += tocLen // pos now points to first page blob

	if !isParallelPageDecodeFormat(toc.Format) {
		return fmt.Errorf(
			"ScanPagedColumnBlobWithStats: format %d is not streamable (Dict/legacy not supported)",
			toc.Format,
		)
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)

	scratch := &IntrinsicColumn{Type: toc.ColType, Format: toc.Format}
	page := &DecodedPage{Type: toc.ColType, Format: toc.Format}
	stats := &PageStats{}

	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	var rowBase uint32
	for i, pm := range toc.Pages {
		rowCount := int(pm.RowCount)

		// Present this page's pruning stats before any value decode. A prefilter that fully
		// accounts for the page (skip / bulk-count) returns true and we never decode its values.
		if prefilter != nil {
			stats.RowCount = pm.RowCount
			stats.RowBase = rowBase
			if len(pm.Min) == 8 && len(pm.Max) == 8 {
				stats.Min = leUint64FromString(pm.Min) // NOTE-274: avoid []byte(...) copy
				stats.Max = leUint64FromString(pm.Max)
				stats.HasMinMax = true
			} else {
				stats.Min, stats.Max, stats.HasMinMax = 0, 0, false
			}
			if prefilter(stats) {
				rowBase += uint32(rowCount) //nolint:gosec // row count per page << 4 GiB
				continue
			}
		}

		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return fmt.Errorf("ScanPagedColumnBlobWithStats: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageCompressed := blob[pageStart:pageEnd]
		pageRaw, decErr := snappyDecodeReuse(pageBuf, pageCompressed) // NOTE-262
		if decErr != nil {
			return fmt.Errorf("ScanPagedColumnBlobWithStats: page %d snappy: %w", i, decErr)
		}

		scratch.Uint64Values = scratch.Uint64Values[:0]
		scratch.BytesValues = scratch.BytesValues[:0]
		scratch.BlockRefs = scratch.BlockRefs[:0]
		scratch.Count = 0

		switch toc.Format {
		case IntrinsicFormatFlat:
			err = appendFlatPageOpt(pageRaw, blockW, rowW, rowCount, toc.ColType, scratch, true)
		case IntrinsicFormatXORBytes:
			err = appendXORBytesPageOpt(pageRaw, blockW, rowW, rowCount, scratch, true)
		case IntrinsicFormatDeltaUint64:
			err = appendDeltaUint64PageOpt(pageRaw, blockW, rowW, rowCount, scratch, true)
		default:
			return fmt.Errorf("ScanPagedColumnBlobWithStats: page %d: unknown format %d", i, toc.Format)
		}
		if err != nil {
			return fmt.Errorf("ScanPagedColumnBlobWithStats: page %d: %w", i, err)
		}

		page.Uint64Values = scratch.Uint64Values
		page.BytesValues = scratch.BytesValues
		page.BlockRefs = scratch.BlockRefs
		page.RowBase = rowBase

		if vErr := visit(page); vErr != nil {
			return vErr
		}
		rowBase += uint32(rowCount) //nolint:gosec // row count per page << 4 GiB
	}
	return nil
}

// DictPageValue is one value-record encountered while streaming a paged Dict column with
// ScanDictPagedColumnBlob. ValBytes/Int64Val carry the value (ValBytes nil for an int64
// value); RawRefs is the packed ref run (RefCount refs, each BlockW+RowW bytes, little-endian
// blockIdx then rowIdx) for THIS page occurrence of the value. A value that spans multiple
// pages is visited once per page it appears in, with the per-page ref run each time — the
// caller dedups the value into a group and folds every occurrence's refs into that group.
//
// RawRefs aliases the page's pooled decode buffer and is valid ONLY for the duration of the
// visit call; the next page reuses the buffer. A visitor that retains ref bytes MUST copy them
// (the group-by scatter consumer reads them in place, so it never copies).
type DictPageValue struct {
	ValBytes []byte
	RawRefs  []byte
	Int64Val int64
	RefCount int
	BlockW   int
	RowW     int
}

// ScanDictPagedColumnBlob decodes a v2 paged Dict column blob ONE PAGE AT A TIME into a reused
// snappy buffer and invokes visit once per value-record per page, handing the raw (undecoded)
// ref run for that occurrence. Unlike the eager decodeDictPagesArena path (reached via
// GetIntrinsicColumn), it NEVER materializes the O(column) BlockRefs arena nor the per-entry
// []BlockRef slices — the dominant decode-side allocation on the M4/M6/M9 rate-by-Dict-group
// hot path (decodeDictPagesArena's makeNoZeroBlockRef(total) + appendVariableWidthRefs of
// millions of refs). A group-by scatter consumer reads each ref straight out of RawRefs and
// scatters its group index, so the BlockRef structs never exist (NOTE-407, issue #356).
//
// blob MUST be a v2 paged Dict column (blob[0] == IntrinsicPagedVersion and TOC format ==
// IntrinsicFormatDict); ScanDictPagedColumnBlob returns an error otherwise rather than silently
// misdecoding. The visitor's returned error short-circuits the scan and is returned.
//
// Per-page, per-value visitation order is byte-identical to decodeDictPagesArena's pass 2 (the
// shared forEachDictPageValue walker), so a consumer that dedups by value reproduces the same
// merged entry set and per-entry ref membership as the eager path.
func ScanDictPagedColumnBlob(blob []byte, visit func(*DictPageValue) error) error {
	if len(blob) < 5 || blob[0] != IntrinsicPagedVersion {
		return fmt.Errorf("ScanDictPagedColumnBlob: not a v2 paged column blob")
	}
	pos := 1
	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4
	if pos+tocLen > len(blob) {
		return fmt.Errorf("ScanDictPagedColumnBlob: truncated at toc_blob")
	}
	toc, err := DecodePageTOCNoStats(blob[pos : pos+tocLen])
	if err != nil {
		return fmt.Errorf("ScanDictPagedColumnBlob: %w", err)
	}
	pos += tocLen // pos now points to first page blob

	if toc.Format != IntrinsicFormatDict {
		return fmt.Errorf("ScanDictPagedColumnBlob: format %d is not a Dict column", toc.Format)
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	refSize := blockW + rowW
	if refSize <= 0 {
		return fmt.Errorf("ScanDictPagedColumnBlob: invalid ref size %d", refSize)
	}
	isInt64 := toc.ColType == ColumnTypeInt64 || toc.ColType == ColumnTypeRangeInt64

	// NOTE-012: reuse a pooled snappy decode buffer across page decompressions.
	pageBuf := AcquireIntrinsicBuf()
	defer ReleaseIntrinsicBuf(pageBuf)

	pv := &DictPageValue{BlockW: blockW, RowW: rowW}
	for i, pm := range toc.Pages {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return fmt.Errorf("ScanDictPagedColumnBlob: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageRaw, decErr := snappyDecodeReuse(pageBuf, blob[pageStart:pageEnd]) // NOTE-262
		if decErr != nil {
			return fmt.Errorf("ScanDictPagedColumnBlob: page %d snappy: %w", i, decErr)
		}

		err = forEachDictPageValue(pageRaw, refSize, isInt64,
			func(valBytes []byte, int64Val int64, refStart, refCount int) error {
				pv.ValBytes = valBytes
				pv.Int64Val = int64Val
				pv.RefCount = refCount
				pv.RawRefs = pageRaw[refStart : refStart+refCount*refSize]
				return visit(pv)
			})
		if err != nil {
			return fmt.Errorf("ScanDictPagedColumnBlob: page %d: %w", i, err)
		}
	}
	return nil
}

// IsDictPagedColumnBlob reports whether blob is a v2 paged Dict column — the streamable target
// of ScanDictPagedColumnBlob. Flat/XOR/Delta paged blobs (streamable via ScanPagedColumnBlob)
// and legacy v1 blobs return false. NOTE-407.
func IsDictPagedColumnBlob(blob []byte) bool {
	if len(blob) < 5 || blob[0] != IntrinsicPagedVersion {
		return false
	}
	pos := 1
	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4
	if pos+tocLen > len(blob) {
		return false
	}
	toc, err := DecodePageTOCNoStats(blob[pos : pos+tocLen])
	if err != nil {
		return false
	}
	return toc.Format == IntrinsicFormatDict
}
