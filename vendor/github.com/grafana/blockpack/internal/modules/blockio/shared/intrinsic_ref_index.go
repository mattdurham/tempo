package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides O(log N) reverse-lookup helpers for IntrinsicColumn.
// EnsureRefIndex builds a sorted index by packed ref once (sync.Once).
// LookupRefFast performs binary search into that index.
// LookupRef is the O(N) linear-scan fallback (for small/filtered result sets).

import (
	"cmp"
	"slices"
)

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
					Pos:    int32(i),                                      //nolint:gosec
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
// Calls EnsureRefIndex internally (sync.Once, so subsequent calls are free);
// callers no longer need a separate EnsureRefIndex call before LookupRefFast.
// Returns (nil, false) when not found.
//
// Return types mirror LookupRef: uint64 or []byte for flat, string or int64 for dict.
func (col *IntrinsicColumn) LookupRefFast(packedRef uint32) (val any, found bool) {
	if col == nil {
		return nil, false
	}
	col.EnsureRefIndex()
	if len(col.refIndex) == 0 {
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
