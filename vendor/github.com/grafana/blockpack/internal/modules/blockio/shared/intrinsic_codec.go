package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides the decode-side of the intrinsic columns wire format.
// Placed in shared (rather than reader or writer) to break the writer→reader import cycle:
//   writer → shared  (OK)
//   reader → shared  (OK)
//   reader ↛ writer  (would be cyclic since writer → reader)

import (
	"encoding/binary"
	"fmt"

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

// DecodeIntrinsicColumnBlob decompresses and decodes a column data blob into an IntrinsicColumn.
func DecodeIntrinsicColumnBlob(blob []byte) (*IntrinsicColumn, error) {
	raw, err := snappy.Decode(nil, blob)
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
		// block_idx_width[1] and row_idx_width[1] — variable-width ref encoding.
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at ref widths")
		}
		blockW := int(raw[pos])
		rowW := int(raw[pos+1])
		pos += 2

		isBytes := colType == ColumnTypeBytes
		if isBytes {
			col.BytesValues = make([][]byte, 0, rowCount)
			for range rowCount {
				if pos+2 > len(raw) {
					return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at bytes len")
				}
				vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
				pos += 2
				if pos+vLen > len(raw) {
					return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at bytes value")
				}
				col.BytesValues = append(col.BytesValues, raw[pos:pos+vLen])
				pos += vLen
			}
		} else {
			// Delta-encoded uint64: each value is a delta from the previous.
			col.Uint64Values = make([]uint64, 0, rowCount)
			var acc uint64
			for range rowCount {
				if pos+8 > len(raw) {
					return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at uint64 value")
				}
				acc += binary.LittleEndian.Uint64(raw[pos:])
				col.Uint64Values = append(col.Uint64Values, acc)
				pos += 8
			}
		}
		// Refs parallel to values, using variable-width encoding.
		col.BlockRefs = make([]BlockRef, 0, rowCount)
		for range rowCount {
			refSize := blockW + rowW
			if pos+refSize > len(raw) {
				return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at refs")
			}
			var blockIdx uint16
			if blockW == 1 {
				blockIdx = uint16(raw[pos])
				pos++
			} else {
				blockIdx = binary.LittleEndian.Uint16(raw[pos:])
				pos += 2
			}
			var rowIdx uint16
			if rowW == 1 {
				rowIdx = uint16(raw[pos])
				pos++
			} else {
				rowIdx = binary.LittleEndian.Uint16(raw[pos:])
				pos += 2
			}
			col.BlockRefs = append(col.BlockRefs, BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx})
		}
	} else { // IntrinsicFormatDict
		isInt64 := colType == ColumnTypeInt64 || colType == ColumnTypeRangeInt64
		valueCount := rowCount // for dict: row_count field holds value_count

		// block_idx_width[1] and row_idx_width[1] — variable-width ref encoding.
		if pos+2 > len(raw) {
			return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at dict ref widths")
		}
		blockW := int(raw[pos])
		rowW := int(raw[pos+1])
		pos += 2

		col.DictEntries = make([]IntrinsicDictEntry, 0, valueCount)
		for range valueCount {
			if pos+2 > len(raw) {
				return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at dict value_len")
			}
			vLen := int(binary.LittleEndian.Uint16(raw[pos:]))
			pos += 2
			var entry IntrinsicDictEntry
			if isInt64 && vLen == 0 {
				if pos+8 > len(raw) {
					return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at int64 value")
				}
				entry.Int64Val = int64(binary.LittleEndian.Uint64(raw[pos:])) //nolint:gosec
				pos += 8
			} else {
				if pos+vLen > len(raw) {
					return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at string value")
				}
				entry.Value = string(raw[pos : pos+vLen])
				pos += vLen
			}
			if pos+4 > len(raw) {
				return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at ref_count")
			}
			refCount := int(binary.LittleEndian.Uint32(raw[pos:]))
			pos += 4
			entry.BlockRefs = make([]BlockRef, 0, refCount)
			for range refCount {
				refSize := blockW + rowW
				if pos+refSize > len(raw) {
					return nil, fmt.Errorf("DecodeIntrinsicColumnBlob: truncated at dict refs")
				}
				var blockIdx uint16
				if blockW == 1 {
					blockIdx = uint16(raw[pos])
					pos++
				} else {
					blockIdx = binary.LittleEndian.Uint16(raw[pos:])
					pos += 2
				}
				var rowIdx uint16
				if rowW == 1 {
					rowIdx = uint16(raw[pos])
					pos++
				} else {
					rowIdx = binary.LittleEndian.Uint16(raw[pos:])
					pos += 2
				}
				entry.BlockRefs = append(entry.BlockRefs, BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx})
			}
			col.DictEntries = append(col.DictEntries, entry)
		}
	}
	return col, nil
}
