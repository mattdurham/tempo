package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides the decode-side of the intrinsic columns wire format.
// Placed in shared (rather than reader or writer) to break the writer→reader import cycle:
//   writer → shared  (OK)
//   reader → shared  (OK)
//   reader ↛ writer  (would be cyclic since writer → reader)

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/golang/snappy"
)

// DecodeTOC decompresses a TOC blob and parses it into a slice of IntrinsicColMeta.
func DecodeTOC(blob []byte) ([]IntrinsicColMeta, error) {
	raw, err := snappy.Decode(nil, blob)
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
	buf.WriteByte(0x01) // page_toc_version = 0x01 (no ref-range fields)

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

// DecodePageTOC decompresses a page TOC blob and parses it into a PagedIntrinsicTOC.
// Only version 0x01 (no ref-range fields) is supported. Unknown versions return an error.
func DecodePageTOC(blob []byte) (PagedIntrinsicTOC, error) {
	raw, err := snappy.Decode(nil, blob)
	if err != nil {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: snappy: %w", err)
	}
	if len(raw) < 8 {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: too short: %d bytes", len(raw))
	}
	pos := 0
	version := raw[pos]
	pos++ // page_toc_version

	if version != 0x01 {
		return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: unknown version %d", version)
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
		minVal := string(raw[pos : pos+minLen])
		pos += minLen

		if pos+2 > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at max_len")
		}
		maxLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+maxLen > len(raw) {
			return PagedIntrinsicTOC{}, fmt.Errorf("DecodePageTOC: truncated at max value")
		}
		maxVal := string(raw[pos : pos+maxLen])
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
			bloom = make([]byte, bloomLen)
			copy(bloom, raw[pos:pos+bloomLen])
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
	isBytes := colType == ColumnTypeBytes
	refSize := blockW + rowW
	pos := 0

	col := &IntrinsicColumn{Type: colType, Format: IntrinsicFormatFlat}

	if isBytes {
		col.BytesValues = make([][]byte, 0, rowCount)
		for range rowCount {
			if pos+2 > len(raw) {
				return nil, fmt.Errorf("DecodeFlatPage: truncated at bytes len")
			}
			vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
			pos += 2
			if pos+vLen > len(raw) {
				return nil, fmt.Errorf("DecodeFlatPage: truncated at bytes value")
			}
			col.BytesValues = append(col.BytesValues, raw[pos:pos+vLen])
			pos += vLen
		}
	} else {
		// Varint delta encoding: values_len[4 LE] + varint deltas.
		if pos+4 > len(raw) {
			return nil, fmt.Errorf("DecodeFlatPage: truncated at values_len")
		}
		valuesLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4

		col.Uint64Values = make([]uint64, 0, rowCount)
		var acc uint64
		valEnd := pos + valuesLen
		for range rowCount {
			if pos >= valEnd {
				return nil, fmt.Errorf("DecodeFlatPage: truncated at varint value")
			}
			delta, n := binary.Uvarint(raw[pos:valEnd])
			if n <= 0 {
				return nil, fmt.Errorf("DecodeFlatPage: invalid varint at pos %d", pos)
			}
			acc += delta
			col.Uint64Values = append(col.Uint64Values, acc)
			pos += n
		}
		pos = valEnd // ensure we're at refs start
	}

	col.BlockRefs = make([]BlockRef, 0, rowCount)
	for range rowCount {
		if pos+refSize > len(raw) {
			return nil, fmt.Errorf("DecodeFlatPage: truncated at refs")
		}
		col.BlockRefs = append(col.BlockRefs, decodeRef(raw, pos, blockW, rowW))
		pos += refSize
	}
	col.Count = uint32(rowCount) //nolint:gosec
	return col, nil
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

// decodePagedColumnBlob decodes a v2 paged column blob into a merged IntrinsicColumn.
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
func decodePagedColumnBlob(blob []byte) (*IntrinsicColumn, error) {
	if len(blob) < 5 {
		return nil, fmt.Errorf("decodePagedColumnBlob: too short")
	}
	pos := 1 // skip sentinel byte

	tocLen := int(binary.LittleEndian.Uint32(blob[pos:]))
	pos += 4

	if pos+tocLen > len(blob) {
		return nil, fmt.Errorf("decodePagedColumnBlob: truncated at toc_blob")
	}
	toc, err := DecodePageTOC(blob[pos : pos+tocLen])
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
	if toc.Format == IntrinsicFormatFlat {
		merged.Uint64Values = make([]uint64, 0, totalRows)
		merged.BytesValues = make([][]byte, 0, totalRows)
		merged.BlockRefs = make([]BlockRef, 0, totalRows)
	}

	// For dict columns, maintain the value→index map across page merges so we
	// don't rebuild it from scratch on each call (O(N²) → O(N) total).
	var dictIdx map[string]int

	for i, pm := range toc.Pages {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageCompressed := blob[pageStart:pageEnd]
		pageRaw, decErr := snappy.Decode(nil, pageCompressed)
		if decErr != nil {
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d snappy: %w", i, decErr)
		}

		var page *IntrinsicColumn
		if toc.Format == IntrinsicFormatFlat {
			page, err = DecodeFlatPage(pageRaw, blockW, rowW, int(pm.RowCount), toc.ColType)
		} else {
			page, err = DecodeDictPage(pageRaw, blockW, rowW, toc.ColType)
		}
		if err != nil {
			return nil, fmt.Errorf("decodePagedColumnBlob: page %d: %w", i, err)
		}

		// Flat merge: simple appends into pre-allocated slices.
		merged.Uint64Values = append(merged.Uint64Values, page.Uint64Values...)
		merged.BytesValues = append(merged.BytesValues, page.BytesValues...)
		merged.BlockRefs = append(merged.BlockRefs, page.BlockRefs...)
		merged.Count += page.Count

		// Dict merge: use persistent idx map to avoid O(N²) rebuild.
		if len(page.DictEntries) > 0 {
			if dictIdx == nil {
				dictIdx = make(map[string]int, len(page.DictEntries)*len(toc.Pages))
			}
			for _, e := range page.DictEntries {
				key := e.Value
				if key == "" {
					key = "\x00" + string(binary.LittleEndian.AppendUint64(nil, uint64(e.Int64Val))) //nolint:gosec
				}
				if j, ok := dictIdx[key]; ok {
					merged.DictEntries[j].BlockRefs = append(merged.DictEntries[j].BlockRefs, e.BlockRefs...)
				} else {
					dictIdx[key] = len(merged.DictEntries)
					merged.DictEntries = append(merged.DictEntries, e)
				}
			}
		}
	}
	return merged, nil
}

// DecodeIntrinsicColumnBlob decodes a column data blob into an IntrinsicColumn.
// Only the v2 paged format (first byte == IntrinsicPagedVersion = 0x02) is supported.
// Returns an error for empty blobs or blobs with any other first byte (including legacy
// v1 monolithic format). V1 format support was removed; files with v1 blobs must be
// rewritten to use v2 paged format.
func DecodeIntrinsicColumnBlob(blob []byte) (*IntrinsicColumn, error) {
	if len(blob) == 0 {
		return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: empty blob")
	}
	// v2 paged format: first byte is IntrinsicPagedVersion (0x02).
	// The blob is NOT snappy-compressed as a whole; it contains the page TOC + page blobs.
	if blob[0] == IntrinsicPagedVersion {
		return decodePagedColumnBlob(blob)
	}
	return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: unsupported intrinsic column format: expected v2 paged (0x%02x), got 0x%02x", IntrinsicPagedVersion, blob[0])
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
	matchFn func(value string, int64Val int64, isInt64 bool) bool,
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

		var strVal string
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
			strVal = string(pageRaw[pos : pos+vLen])
			pos += vLen
		}

		if pos+4 > len(pageRaw) {
			return result, false
		}
		refCount := int(binary.LittleEndian.Uint32(pageRaw[pos:]))
		pos += 4

		if !matchFn(strVal, i64Val, isInt64) {
			skip := refCount * refSize
			if skip/refSize != refCount || pos+skip > len(pageRaw) {
				return result, false // corrupt refCount
			}
			pos += skip
			continue
		}
		for range refCount {
			if pos+refSize > len(pageRaw) {
				return result, true
			}
			result = append(result, decodeRef(pageRaw, pos, blockW, rowW))
			pos += refSize
			if maxRefs > 0 && len(result) >= maxRefs {
				return result, true
			}
		}
	}
	return result, true
}

// scanDictPagedBlob handles v2 paged dict column blobs for ScanDictColumnRefs.
func scanDictPagedBlob(
	blob []byte,
	matchFn func(value string, int64Val int64, isInt64 bool) bool,
	bloomKeys [][]byte,
	maxRefs int,
) []BlockRef {
	toc, pos, ok := parsePagedBlobHeader(blob)
	if !ok || toc.Format != IntrinsicFormatDict {
		return nil
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)

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
		pageRaw, decErr := snappy.Decode(nil, blob[pageStart:pageEnd])
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
	matchFn func(value string, int64Val int64, isInt64 bool) bool,
	maxRefs int,
) []BlockRef {
	// v2 paged format.
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanDictPagedBlob(blob, matchFn, nil, maxRefs)
	}

	raw, err := snappy.Decode(nil, blob)
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

		// Read value.
		var strVal string
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
			strVal = string(raw[pos : pos+vLen])
			pos += vLen
		}

		if pos+4 > len(raw) {
			return nil
		}
		refCount := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4

		if !matchFn(strVal, i64Val, isInt64) {
			// Skip all refs for this value — no struct allocation.
			skip := refCount * refSize
			if skip/refSize != refCount || pos+skip > len(raw) {
				return nil // corrupt refCount
			}
			pos += skip
			continue
		}

		// Decode only matching value's refs.
		for range refCount {
			if pos+refSize > len(raw) {
				return nil
			}
			ref := decodeRef(raw, pos, blockW, rowW)
			pos += refSize
			result = append(result, ref)
			if maxRefs > 0 && len(result) >= maxRefs {
				return result
			}
		}
	}
	return result
}

// ScanDictColumnRefsWithBloom is like ScanDictColumnRefs but also accepts bloom filter
// keys to skip pages in v2 paged blobs whose bloom filter rejects all query values.
// For v1 monolithic blobs, bloomKeys is ignored.
func ScanDictColumnRefsWithBloom(
	blob []byte,
	matchFn func(value string, int64Val int64, isInt64 bool) bool,
	bloomKeys [][]byte,
	maxRefs int,
) []BlockRef {
	if len(blob) > 0 && blob[0] == IntrinsicPagedVersion {
		return scanDictPagedBlob(blob, matchFn, bloomKeys, maxRefs)
	}
	return ScanDictColumnRefs(blob, matchFn, maxRefs)
}

// parsePagedBlobHeader parses the v2 paged blob header: sentinel[1] + toc_len[4] + toc_blob.
// Returns the decoded TOC, the byte offset of the first page blob, and success.
func parsePagedBlobHeader(blob []byte) (PagedIntrinsicTOC, int, bool) {
	if len(blob) < 5 {
		return PagedIntrinsicTOC{}, 0, false
	}
	tocLen := int(binary.LittleEndian.Uint32(blob[1:5]))
	if 5+tocLen > len(blob) {
		return PagedIntrinsicTOC{}, 0, false
	}
	toc, err := DecodePageTOC(blob[5 : 5+tocLen])
	if err != nil {
		return PagedIntrinsicTOC{}, 0, false
	}
	return toc, 5 + tocLen, true
}

// findRangeBoundaries varint-scans the values section of a flat page to locate
// the first and last row indices whose accumulated value falls within [lo, hi].
// pageRaw is the decompressed page; refsStart is the byte offset where the refs
// section begins (used as the end-of-values boundary); rowCount is the number of rows.
// Returns (startIdx, endIdx) where startIdx == -1 means no rows matched.
func findRangeBoundaries(pageRaw []byte, refsStart, rowCount int, lo, hi uint64, hasLo, hasHi bool) (startIdx, endIdx int) {
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
	toc, pos, ok := parsePagedBlobHeader(blob)
	if !ok || toc.Format != IntrinsicFormatFlat || toc.ColType == ColumnTypeBytes {
		return nil
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	refSize := blockW + rowW

	var result []BlockRef
	for _, pm := range toc.Pages {
		// Min/max skip: if page range doesn't overlap [lo, hi], skip page.
		if len(pm.Min) == 8 && len(pm.Max) == 8 {
			pageMin := binary.LittleEndian.Uint64([]byte(pm.Min))
			pageMax := binary.LittleEndian.Uint64([]byte(pm.Max))
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
		pageRaw, decErr := snappy.Decode(nil, blob[pageStart:pageEnd])
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
	return result
}

// scanFlatPagedFiltered handles v2 paged flat column blobs for filtered scan.
// When filter is nil, all refs are accepted (equivalent to the former scanFlatPagedTopK fast path).
func scanFlatPagedFiltered(blob []byte, backward bool, limit int, filter func(BlockRef) bool) []BlockRef {
	toc, pos, ok := parsePagedBlobHeader(blob)
	if !ok || toc.Format != IntrinsicFormatFlat || toc.ColType == ColumnTypeBytes {
		return nil
	}

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)
	refSize := blockW + rowW

	result := make([]BlockRef, 0, limit)

	scanPage := func(pm PageMeta) bool {
		pageStart := pos + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return false
		}
		pageRaw, decErr := snappy.Decode(nil, blob[pageStart:pageEnd])
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

	raw, err := snappy.Decode(nil, blob)
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
	refSize := blockW + rowW

	valuesStart := pos
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

	raw, err := snappy.Decode(nil, blob)
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
	refSize := blockW + rowW

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

	raw, err := snappy.Decode(nil, blob)
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
	refSize := blockW + rowW

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

// decodeRef reads a single BlockRef from raw at the given position using variable-width encoding.
func decodeRef(raw []byte, pos, blockW, rowW int) BlockRef {
	var blockIdx uint16
	if blockW == 1 {
		blockIdx = uint16(raw[pos])
	} else {
		blockIdx = binary.LittleEndian.Uint16(raw[pos:])
	}
	var rowIdx uint16
	if rowW == 1 {
		rowIdx = uint16(raw[pos+blockW])
	} else {
		rowIdx = binary.LittleEndian.Uint16(raw[pos+blockW:])
	}
	return BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx}
}
