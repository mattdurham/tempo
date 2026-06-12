package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides O(log N) reverse-lookup helpers for IntrinsicColumn.
// EnsureRefIndex builds a sorted index by packed ref once (sync.Once).
// lookupRefIdx performs binary search into that index (used by typed accessors).

import (
	"cmp"
	"slices"
	"sync"
)

// NOTE-192: pool the radix-sort scratch buffer to remove a per-call zeroing allocation.
// radixSortRefIndex needs an n-element double-buffer for its LSD passes. `make([]RefIndexEntry, n)`
// allocates AND zeroes n*8 bytes via runtime.memclrNoHeapPointers, which a querier CPU profile
// (2026-06-11) showed as the single largest blockpack-attributable cost (memclrNoHeapPointers ~2.86%,
// and radixSortRefIndex itself ~2.35% self-time on the M8/M9/Q9-Q10 intrinsic-decode and dict
// group-by sort path). The zeroing is pure waste: the very first radix pass scatters every source
// element into a distinct destination slot, so all n slots are unconditionally overwritten before
// any are read — the buffer's prior contents are irrelevant. The buffer is local scratch that never
// escapes the function (the sorted result is always landed back into `idx`), so a pooled,
// non-zeroed buffer is byte-for-byte equivalent and aliasing-free. sync.Pool is concurrency-safe,
// which matters because EnsureRefIndex (the sole caller) runs concurrently across columns/blocks.
var radixBufPool = sync.Pool{
	New: func() any {
		s := make([]RefIndexEntry, 0)
		return &s
	},
}

// radixBufCap bounds the backing array kept in the pool. A pathologically large block could grow
// the scratch buffer far beyond steady-state need; returning such a buffer would pin a large
// allocation. Buffers larger than this are dropped on Put so the pool's resident footprint stays
// bounded (mirrors the cap-guard discipline used for the intern/lazy-column pools).
const radixBufCap = 1 << 20 // 1Mi RefIndexEntry = 8 MiB

func getRadixBuf(n int) *[]RefIndexEntry {
	bp := radixBufPool.Get().(*[]RefIndexEntry)
	if cap(*bp) < n {
		*bp = make([]RefIndexEntry, n)
	} else {
		*bp = (*bp)[:n]
	}
	return bp
}

func putRadixBuf(bp *[]RefIndexEntry) {
	if cap(*bp) > radixBufCap {
		return // drop oversized buffer; let it be GC'd
	}
	*bp = (*bp)[:0]
	radixBufPool.Put(bp)
}

// radixSortRefIndex sorts idx in ascending Packed order using an LSD radix sort over the
// 32-bit Packed key (NOTE-174). This replaces the comparator-closure-driven
// slices.SortFunc(cmp.Compare(a.Packed, b.Packed)) used on the unsorted fallback path
// (interleaved multi-value dict columns, out-of-order block merges). The querier CPU
// profile showed that fallback reaching slices.pdqsortCmpFunc / slices.partitionCmpFunc —
// the per-comparison comparator closure was ~5% of querier CPU on group-by histogram and
// rate-by queries. A radix sort is O(N) over a fixed 4-byte key with no comparator
// indirection, so it removes the comparison-sort cost entirely.
//
// Up to four counting passes (one per byte, least significant first) produce a stable
// ascending order on the full 32-bit key. The result is byte-for-byte identical to
// slices.SortFunc because both order solely by Packed; ties keep an arbitrary-but-consistent
// order, which the binary-search consumers (lookupRefIdx, BlockRefRange, LookupRefFast*) do
// not depend on.
//
// NOTE-190: bound the pass count by the maximum key magnitude instead of always running all
// four byte passes. The Packed key is BlockIdx<<16 | RowIdx; on the unsorted dict fallback
// path the keys merge refs across the blocks in a query, but RowIdx is always 16-bit and
// BlockIdx is typically small, so the two high bytes (shift 16/24) are frequently all-zero.
// A zero-valued byte position is a degenerate radix pass: every key lands in bucket 0, so the
// pass is an identity permutation that still costs a full O(N) count + O(N) scatter. By taking
// the OR of every key once (one O(N) pass that the count pass would do anyway) we learn the
// highest non-zero byte and skip the leading zero passes entirely. This is a standard radix
// byte-skip — purely a function of the data's value range, not of any particular column or
// query shape. The buffer parity is handled below: when an odd number of passes ran, the
// sorted data is in buf and is copied back into idx so the result always lands in idx.
//
// NOTE-205: fuse each pass's count scan into the prior pass's scatter scan ("look-ahead
// histograms"), so the only standalone count scan is the first one (which doubles as the
// NOTE-190 OR scan). The classic LSD loop read N elements (2·passes+1) times: one OR scan
// plus one count scan and one scatter scan per pass. The scatter of pass k already reads every
// src[i].Packed, so it can tally pass k+1's histogram for free in the same pass. That removes
// the standalone count scan from passes 2..P, cutting source reads from (2·passes+1)·N to
// (passes+1)·N. Critically the work is bounded by the *actual* number of significant passes
// (NOTE-190 byte-skip), so the dominant 2-pass (16-bit key) case never pays to tally bytes it
// won't use — only histograms for passes that will run are computed. radixSortRefIndex was
// ~2.35% of querier self-time on the unsorted dict/merge fallback path (profile 2026-06-11).
// Purely a reorganization of when counts are tallied; the prefix-sum, scatter order, pass
// count, and odd-pass copy-back are unchanged, so output is byte-for-byte identical.
func radixSortRefIndex(idx []RefIndexEntry) {
	const radixBits = 8
	const radixSize = 1 << radixBits
	const radixMask = radixSize - 1
	const numBytes = 32 / radixBits
	n := len(idx)
	if n < 2 {
		return
	}

	// Pass 1's count scan doubles as the OR scan: it tallies the lowest byte's histogram while
	// ORing all keys to learn the highest significant byte (NOTE-190 byte-skip bound).
	var keyOr uint32
	var hist [numBytes][radixSize]int
	for i := range idx {
		k := idx[i].Packed
		keyOr |= k
		hist[0][k&radixMask]++
	}
	// keyOr==0 means all keys are 0; the slice is already trivially sorted, no passes needed.
	if keyOr == 0 {
		return
	}
	// maxShift is the start shift of the highest non-zero byte. Passes run at byte positions
	// 0..maxByte; histograms for higher (all-zero) bytes are never computed or consumed.
	maxShift := 0
	for s := 0; s < 32; s += radixBits {
		if (keyOr>>s)&radixMask != 0 {
			maxShift = s
		}
	}
	maxByte := maxShift / radixBits

	// NOTE-192: pooled, non-zeroed scratch double-buffer. The first radix pass overwrites
	// every slot before any read, so the buffer's prior contents are irrelevant.
	bufPtr := getRadixBuf(n)
	defer putRadixBuf(bufPtr)
	buf := *bufPtr
	src, dst := idx, buf
	passes := 0
	for b := 0; b <= maxByte; b++ {
		shift := b * radixBits
		counts := &hist[b]
		// Prefix sum: counts[c] becomes the start offset of bucket c in dst.
		sum := 0
		for c := range counts {
			cnt := counts[c]
			counts[c] = sum
			sum += cnt
		}
		// Scatter into dst by this byte. When a further pass follows, tally its histogram in
		// the same scan (look-ahead) so it needs no standalone count pass. maxByte < numBytes
		// always (it derives from a byte position within the 32-bit key), so b < maxByte gives
		// b+1 < numBytes; the explicit numBytes guard makes the array bound statically provable.
		if b < maxByte && b+1 < numBytes {
			next := &hist[b+1] //nolint:gosec // b+1<numBytes guarded above; bound is static
			nextShift := shift + radixBits
			for i := range src {
				k := src[i].Packed
				bucket := (k >> shift) & radixMask
				dst[counts[bucket]] = src[i]
				counts[bucket]++
				next[(k>>nextShift)&radixMask]++
			}
		} else {
			for i := range src {
				bucket := (src[i].Packed >> shift) & radixMask
				dst[counts[bucket]] = src[i]
				counts[bucket]++
			}
		}
		src, dst = dst, src
		passes++
	}
	// After an even number of passes src == idx; after an odd number the sorted data is in
	// buf (now referenced by src), so copy it back so the result always lands in idx.
	if passes%2 == 1 {
		copy(idx, src)
	}
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
