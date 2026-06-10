package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides O(log N) reverse-lookup helpers for IntrinsicColumn.
// EnsureRefIndex builds a sorted index by packed ref once (sync.Once).
// lookupRefIdx performs binary search into that index (used by typed accessors).

import (
	"cmp"
	"slices"
)

// radixSortRefIndex sorts idx in ascending Packed order using an LSD radix sort over the
// 32-bit Packed key (NOTE-174). This replaces the comparator-closure-driven
// slices.SortFunc(cmp.Compare(a.Packed, b.Packed)) used on the unsorted fallback path
// (interleaved multi-value dict columns, out-of-order block merges). The querier CPU
// profile showed that fallback reaching slices.pdqsortCmpFunc / slices.partitionCmpFunc —
// the per-comparison comparator closure was ~5% of querier CPU on group-by histogram and
// rate-by queries. A radix sort is O(N) over a fixed 4-byte key with no comparator
// indirection, so it removes the comparison-sort cost entirely.
//
// Four counting passes (one per byte, least significant first) produce a stable ascending
// order on the full 32-bit key. The result is byte-for-byte identical to slices.SortFunc
// because both order solely by Packed; ties keep an arbitrary-but-consistent order, which
// the binary-search consumers (lookupRefIdx, BlockRefRange, LookupRefFast*) do not depend on.
func radixSortRefIndex(idx []RefIndexEntry) {
	const radixBits = 8
	const radixSize = 1 << radixBits
	const radixMask = radixSize - 1
	n := len(idx)
	if n < 2 {
		return
	}
	buf := make([]RefIndexEntry, n)
	src, dst := idx, buf
	var counts [radixSize]int
	for shift := 0; shift < 32; shift += radixBits {
		for i := range counts {
			counts[i] = 0
		}
		for i := range src {
			counts[(src[i].Packed>>shift)&radixMask]++
		}
		// Prefix sum: counts[b] becomes the start offset of bucket b in dst.
		sum := 0
		for b := range counts {
			c := counts[b]
			counts[b] = sum
			sum += c
		}
		for i := range src {
			b := (src[i].Packed >> shift) & radixMask
			dst[counts[b]] = src[i]
			counts[b]++
		}
		src, dst = dst, src
	}
	// After 4 (even) passes src == idx, so no final copy is needed.
}

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
			// NOTE-168: build Packed keys and detect ascending order in the same pass.
			// Flat/XOR/Delta refs are emitted in row order; within a single block RowIdx
			// rises monotonically, so the packed key (BlockIdx<<16|RowIdx) is already
			// sorted for the dominant single-block decode case. When that holds we skip
			// the O(N log N) closure-driven slices.SortFunc entirely — the comparator
			// closure (cmp.Compare on Packed) was reached via slices.partitionCmpFunc and
			// showed up as a residual CPU sink on the EnsureRefIndex path after NOTE-167.
			sorted := true
			var prev uint32
			for i, ref := range col.BlockRefs {
				p := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec
				idx[i] = RefIndexEntry{
					Packed: p,
					Pos:    int32(i), //nolint:gosec
				}
				if i > 0 && p < prev {
					sorted = false
				}
				prev = p
			}
			if !sorted {
				radixSortRefIndex(idx)
			}
			col.refIndex = idx
		case IntrinsicFormatDict:
			total := 0
			for _, e := range col.DictEntries {
				total += len(e.BlockRefs)
			}
			idx := make([]RefIndexEntry, 0, total)
			// NOTE-168: same ascending-order detection for the dict path. Each dict entry's
			// BlockRefs are emitted in row order, but entries interleave across the column,
			// so the concatenation is rarely globally sorted — the check is a cheap O(N)
			// scan that costs one comparison per entry and only skips the sort when it is
			// genuinely already ordered (e.g. single-value dict columns).
			sorted := true
			var prev uint32
			first := true
			for entryIdx, entry := range col.DictEntries {
				for _, ref := range entry.BlockRefs {
					p := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec
					idx = append(idx, RefIndexEntry{
						Packed: p,
						Pos:    int32(entryIdx), //nolint:gosec
					})
					if !first && p < prev {
						sorted = false
					}
					prev = p
					first = false
				}
			}
			if !sorted {
				radixSortRefIndex(idx)
			}
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
