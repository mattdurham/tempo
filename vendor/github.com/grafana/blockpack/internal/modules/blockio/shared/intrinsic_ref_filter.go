package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides page-filtered decode functions for intrinsic columns and ref-index
// lookup functions (EnsureRefIndex, LookupRefFast, LookupRef) used by lookupIntrinsicFields.

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/golang/snappy"
)

// DecodePagedColumnBlobFiltered decodes a v2 paged column blob.
//
// refFilter maps packed ref keys (blockIdx<<16|rowIdx) to an empty struct.
// Pass nil to decode all pages (equivalent to decodePagedColumnBlob / full decode).
//
// NOTE: The ref-range page-skipping optimization (MinRef/MaxRef/RefBloom) was removed in
// NOTE-007. All pages are decoded; the refFilter is retained for caller API compatibility.
// Callers must still check ref membership when consuming results.
//
// Returns an error for malformed blobs; the first byte must be IntrinsicPagedVersion (0x02).
//
// NOTE: Results are NOT cached — this is a query-scoped decode. Use GetIntrinsicColumn
// for the cached full-column path.
func DecodePagedColumnBlobFiltered(blob []byte, refFilter map[uint32]struct{}) (*IntrinsicColumn, error) {
	if len(blob) == 0 {
		return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: empty blob")
	}
	if blob[0] != IntrinsicPagedVersion {
		return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: expected paged sentinel 0x%02x, got 0x%02x", IntrinsicPagedVersion, blob[0])
	}
	if len(blob) < 5 {
		return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: too short")
	}

	tocLen := int(binary.LittleEndian.Uint32(blob[1:]))
	tocStart := 5
	if tocStart+tocLen > len(blob) {
		return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: truncated at toc_blob")
	}
	toc, err := DecodePageTOC(blob[tocStart : tocStart+tocLen])
	if err != nil {
		return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: %w", err)
	}
	pageDataStart := tocStart + tocLen

	blockW := int(toc.BlockIdxWidth)
	rowW := int(toc.RowIdxWidth)

	// Allocate merge output.
	merged := &IntrinsicColumn{Type: toc.ColType, Format: toc.Format}
	if toc.Format == IntrinsicFormatFlat {
		merged.Uint64Values = make([]uint64, 0)
		merged.BytesValues = make([][]byte, 0)
		merged.BlockRefs = make([]BlockRef, 0)
	}

	var dictIdx map[string]int

	for i, pm := range toc.Pages {
		pageStart := pageDataStart + int(pm.Offset)
		pageEnd := pageStart + int(pm.Length)
		if pageEnd > len(blob) {
			return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: page %d out of bounds (offset=%d len=%d blobLen=%d)",
				i, pm.Offset, pm.Length, len(blob))
		}
		pageCompressed := blob[pageStart:pageEnd]
		pageRaw, decErr := snappy.Decode(nil, pageCompressed)
		if decErr != nil {
			return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: page %d snappy: %w", i, decErr)
		}

		var page *IntrinsicColumn
		if toc.Format == IntrinsicFormatFlat {
			page, err = DecodeFlatPage(pageRaw, blockW, rowW, int(pm.RowCount), toc.ColType)
		} else {
			page, err = DecodeDictPage(pageRaw, blockW, rowW, toc.ColType)
		}
		if err != nil {
			return nil, fmt.Errorf("DecodePagedColumnBlobFiltered: page %d: %w", i, err)
		}

		// Flat merge: append into result slices.
		merged.Uint64Values = append(merged.Uint64Values, page.Uint64Values...)
		merged.BytesValues = append(merged.BytesValues, page.BytesValues...)
		merged.BlockRefs = append(merged.BlockRefs, page.BlockRefs...)
		merged.Count += page.Count

		// Dict merge: deduplicate values across pages.
		if len(page.DictEntries) > 0 {
			if dictIdx == nil {
				dictIdx = make(map[string]int, len(page.DictEntries))
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

	_ = refFilter // retained for API compatibility; page-skipping removed in NOTE-007
	return merged, nil
}

// EnsureRefIndex builds a sorted-by-packed-ref lookup index into this column, enabling
// O(log N) reverse lookup via LookupRefFast. Safe to call concurrently — the index is
// built at most once (sync.Once). No-op if the index is already built or col is nil.
//
// For flat columns: RefIndexEntry.Pos indexes into BlockRefs/Uint64Values/BytesValues.
// For dict columns: RefIndexEntry.Pos is the DictEntries index.
func (col *IntrinsicColumn) EnsureRefIndex() {
	if col == nil {
		return
	}
	col.refIndexOnce.Do(func() {
		switch col.Format {
		case IntrinsicFormatFlat:
			idx := make([]RefIndexEntry, len(col.BlockRefs))
			for i, ref := range col.BlockRefs {
				idx[i] = RefIndexEntry{
					Packed: uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx), //nolint:gosec
					Pos:    int32(i),                                       //nolint:gosec
				}
			}
			slices.SortFunc(idx, func(a, b RefIndexEntry) int { return cmp.Compare(a.Packed, b.Packed) })
			col.refIndex = idx
		case IntrinsicFormatDict:
			total := 0
			for _, e := range col.DictEntries {
				total += len(e.BlockRefs)
			}
			idx := make([]RefIndexEntry, 0, total)
			for entryIdx, entry := range col.DictEntries {
				for _, ref := range entry.BlockRefs {
					idx = append(idx, RefIndexEntry{
						Packed: uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx), //nolint:gosec
						Pos:    int32(entryIdx),                               //nolint:gosec
					})
				}
			}
			slices.SortFunc(idx, func(a, b RefIndexEntry) int { return cmp.Compare(a.Packed, b.Packed) })
			col.refIndex = idx
		}
	})
}

// LookupRefFast performs an O(log N) binary search for packedRef in col.refIndex.
// Must call EnsureRefIndex before calling this. Returns (nil, false) when not found.
//
// Return types mirror LookupRef: uint64 or []byte for flat, string or int64 for dict.
func (col *IntrinsicColumn) LookupRefFast(packedRef uint32) (val any, found bool) {
	if col == nil || len(col.refIndex) == 0 {
		return nil, false
	}
	pos, ok := slices.BinarySearchFunc(col.refIndex, packedRef, func(e RefIndexEntry, target uint32) int {
		return cmp.Compare(e.Packed, target)
	})
	if !ok {
		return nil, false
	}
	idx := int(col.refIndex[pos].Pos)
	switch col.Format {
	case IntrinsicFormatFlat:
		if idx < len(col.Uint64Values) {
			return col.Uint64Values[idx], true
		}
		if idx < len(col.BytesValues) {
			return col.BytesValues[idx], true
		}
	case IntrinsicFormatDict:
		if idx < len(col.DictEntries) {
			e := col.DictEntries[idx]
			if col.Type == ColumnTypeInt64 || col.Type == ColumnTypeRangeInt64 {
				return e.Int64Val, true
			}
			return e.Value, true
		}
	}
	return nil, false
}

// LookupRef searches for a packed ref (blockIdx<<16|rowIdx) in the column and returns
// the associated value and true when found, or (nil, false) when not found.
//
// For flat columns (IntrinsicFormatFlat):
//   - Returns the uint64 value as uint64 for ColumnTypeUint64 and numeric types.
//   - Returns the []byte value as []byte for ColumnTypeBytes.
//
// For dict columns (IntrinsicFormatDict):
//   - Returns the string value as string for string types.
//   - Returns the int64 value as int64 for ColumnTypeInt64 / ColumnTypeRangeInt64.
//
// This is an O(N) linear scan. After GetIntrinsicColumnForRefs has already filtered
// to relevant pages, N is the number of rows in those pages (a small subset).
func (col *IntrinsicColumn) LookupRef(packedRef uint32) (val any, found bool) {
	if col == nil {
		return nil, false
	}
	switch col.Format {
	case IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			packed := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
			if packed != packedRef {
				continue
			}
			if i < len(col.Uint64Values) {
				return col.Uint64Values[i], true
			}
			if i < len(col.BytesValues) {
				return col.BytesValues[i], true
			}
			return nil, false
		}
	case IntrinsicFormatDict:
		isInt := col.Type == ColumnTypeInt64 || col.Type == ColumnTypeRangeInt64
		for _, entry := range col.DictEntries {
			for _, ref := range entry.BlockRefs {
				packed := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				if packed != packedRef {
					continue
				}
				if isInt {
					return entry.Int64Val, true
				}
				return entry.Value, true
			}
		}
	}
	return nil, false
}
