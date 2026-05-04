package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides O(log N) reverse-lookup helpers for IntrinsicColumn.
// EnsureRefIndex builds a sorted index by packed ref once (sync.Once).
// lookupRefIdx performs binary search into that index (used by typed accessors).

import (
	"cmp"
	"slices"
)

// EnsureRefIndex builds a sorted-by-packed-ref lookup index into this column, enabling
// O(log N) reverse lookup via the typed accessor methods. Safe to call concurrently —
// the index is built at most once (sync.Once). No-op if already built or col is nil.
//
// For flat columns: RefIndexEntry.Pos indexes into BlockRefs/Uint64Values/BytesValues.
// For dict columns: RefIndexEntry.Pos is the DictEntries index.
func (col *IntrinsicColumn) EnsureRefIndex() {
	if col == nil {
		return
	}
	col.refIndexOnce.Do(func() {
		switch col.Format {
		case IntrinsicFormatFlat, IntrinsicFormatXORBytes, IntrinsicFormatDeltaUint64:
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

// lookupRefIdx returns the position index in the value arrays for packedRef,
// or -1 if not found. Calls EnsureRefIndex internally.
func (col *IntrinsicColumn) lookupRefIdx(packedRef uint32) int {
	col.EnsureRefIndex()
	if len(col.refIndex) == 0 {
		return -1
	}
	pos, ok := slices.BinarySearchFunc(col.refIndex, packedRef, func(e RefIndexEntry, target uint32) int {
		return cmp.Compare(e.Packed, target)
	})
	if !ok {
		return -1
	}
	return int(col.refIndex[pos].Pos)
}

// LookupRefFastUint64 returns the uint64 value at packedRef for flat columns.
// Returns (0, false) if not found or if the column has no Uint64Values at that index.
// NOTE-015: zero-alloc typed accessor; eliminates interface boxing vs LookupRef.
func (col *IntrinsicColumn) LookupRefFastUint64(packedRef uint32) (uint64, bool) {
	if col == nil {
		return 0, false
	}
	idx := col.lookupRefIdx(packedRef)
	if idx < 0 || idx >= len(col.Uint64Values) {
		return 0, false
	}
	return col.Uint64Values[idx], true
}

// LookupRefFastInt64 returns the int64 value at packedRef for dict int64 columns.
// Returns (0, false) if not found or if the column type is not int64.
// NOTE-015: zero-alloc typed accessor; eliminates interface boxing vs LookupRef.
func (col *IntrinsicColumn) LookupRefFastInt64(packedRef uint32) (int64, bool) {
	if col == nil {
		return 0, false
	}
	idx := col.lookupRefIdx(packedRef)
	if idx < 0 || col.Format != IntrinsicFormatDict || idx >= len(col.DictEntries) {
		return 0, false
	}
	if col.Type != ColumnTypeInt64 && col.Type != ColumnTypeRangeInt64 {
		return 0, false
	}
	return col.DictEntries[idx].Int64Val, true
}

// LookupRefFastString returns the string value at packedRef for dict string columns.
// Returns ("", false) if not found or if the column is not a dict string column.
// NOTE-015: zero-alloc typed accessor; eliminates interface boxing vs LookupRef.
// The returned string aliases col.DictEntries[idx].Value — immutable in Go, safe to alias.
func (col *IntrinsicColumn) LookupRefFastString(packedRef uint32) (string, bool) {
	if col == nil {
		return "", false
	}
	idx := col.lookupRefIdx(packedRef)
	if idx < 0 || col.Format != IntrinsicFormatDict || idx >= len(col.DictEntries) {
		return "", false
	}
	if col.Type == ColumnTypeInt64 || col.Type == ColumnTypeRangeInt64 {
		return "", false
	}
	return col.DictEntries[idx].Value, true
}

// LookupRefFastBytes returns the []byte value at packedRef for flat bytes columns.
// Returns (nil, false) if not found or if the column has no BytesValues at that index.
// NOTE-015: zero-alloc typed accessor; eliminates interface boxing vs LookupRef.
// NOTE-012: the returned slice aliases col.BytesValues[idx] which is already an independent
// copy of any pool buffer. Callers that need their own copy must clone explicitly.
func (col *IntrinsicColumn) LookupRefFastBytes(packedRef uint32) ([]byte, bool) {
	if col == nil {
		return nil, false
	}
	idx := col.lookupRefIdx(packedRef)
	if idx < 0 || idx >= len(col.BytesValues) {
		return nil, false
	}
	return col.BytesValues[idx], true
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
	case IntrinsicFormatFlat, IntrinsicFormatXORBytes, IntrinsicFormatDeltaUint64:
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

// BlockRefRange returns the subslice of refIndex whose entries belong to blockIdx.
// Calls EnsureRefIndex internally (sync.Once, idempotent). Returns nil if col is nil
// or no entries exist for blockIdx.
//
// The returned slice is a direct alias of col.refIndex[start:end] — it is valid as
// long as col is alive. Do not append to it.
//
// NOTE-016: O(log(B×N)) to find the block boundary — one binary search to start, then
// a linear walk. For flat/XORBytes/DeltaUint64 columns, Pos indexes BytesValues or
// Uint64Values. For dict columns, Pos indexes DictEntries. This is the O(1)-per-entry
// building block for the structural scatter (NOTE-100 in executor/NOTES.md).
func (col *IntrinsicColumn) BlockRefRange(blockIdx uint16) []RefIndexEntry {
	if col == nil {
		return nil
	}
	col.EnsureRefIndex()
	if len(col.refIndex) == 0 {
		return nil
	}
	loKey := uint32(blockIdx) << 16
	// Binary search for the first entry with Packed >= loKey (= blockIdx<<16|0).
	start, _ := slices.BinarySearchFunc(col.refIndex, loKey, func(e RefIndexEntry, target uint32) int {
		return cmp.Compare(e.Packed, target)
	})
	// Linear walk: all same-blockIdx entries are contiguous because blockIdx occupies
	// the high 16 bits of Packed, so they sort together after EnsureRefIndex.
	// Using walk (not a second binary search for end) avoids overflow at blockIdx=0xFFFF:
	// (0xFFFF+1)<<16 would overflow uint32 to 0. The walk is cache-friendly for the
	// N_in_block entries that are sequentially laid out in the refIndex slice.
	end := start
	for end < len(col.refIndex) && col.refIndex[end].Packed>>16 == uint32(blockIdx) {
		end++
	}
	if start == end {
		return nil
	}
	return col.refIndex[start:end]
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
	case IntrinsicFormatFlat, IntrinsicFormatXORBytes, IntrinsicFormatDeltaUint64:
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
