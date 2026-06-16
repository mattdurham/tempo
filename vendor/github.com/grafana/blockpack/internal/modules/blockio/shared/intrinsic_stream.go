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
