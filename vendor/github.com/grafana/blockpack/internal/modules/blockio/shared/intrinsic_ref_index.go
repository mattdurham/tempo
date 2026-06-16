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

// radixBitsRefIndex is the radix digit width (bits per LSD pass) shared by the ref-index radix
// sorters. NOTE-235: hoisted to a package const so radixSortRefIndexPrepared can declare its
// pre-built first-histogram parameter as a [1<<radixBitsRefIndex]int array whose size is tied to
// the digit width (a 256-bucket byte histogram). Changing this single value resizes every pass
// histogram consistently.
const radixBitsRefIndex = 8

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

// NOTE-253: generation-stamped claim buffer for the dense-scatter density checks. Both
// scatterDictRefIndexDense and scatterDictRefIndexMultiBlockDense need to detect, while
// scattering, whether any output slot is written twice (a duplicate (block,row) => the
// permutation is not dense). They previously did this by pre-clearing the entire pooled
// scratch buffer to a -1 sentinel Pos (a full O(N) strided write) before every scatter, then
// checking `buf[slot].Pos != unclaimed` per write. On the proven-dense path the scatter
// writes every one of the n slots exactly once, so the pre-clear is pure overhead that scales
// with n on the dominant decode path. A generation-stamped claim array removes it: each slot
// carries the generation token of the call that last claimed it, so "claimed this call" is
// `claim[slot] == gen` with NO per-call clear. We bump gen once per scatter; only when gen
// would wrap to 0 (every ~4 billion scatters) do we pay a one-time O(N) reset of the resident
// buffer, keeping the clear amortized to O(1) per call. The claim buffer is sized to the
// scatter length and pooled independently of the value double-buffer; sync.Pool makes it
// concurrency-safe across the EnsureRefIndex calls that run in parallel over columns/blocks.
type claimBuf struct {
	gen   []uint32
	token uint32
}

var claimBufPool = sync.Pool{
	New: func() any { return &claimBuf{} },
}

// getClaimBuf returns a claim buffer of length >= n together with the generation token that
// marks "claimed by this call". The buffer's gen entries are NOT cleared: any slot whose
// stored token differs from the returned token is treated as unclaimed. The token is advanced
// per acquisition; on the rare wrap to 0 the resident buffer is reset to 0 once so that a
// stale 0 from an old call cannot be mistaken for the fresh token.
func getClaimBuf(n int) (*claimBuf, uint32) {
	cb := claimBufPool.Get().(*claimBuf)
	if cap(cb.gen) < n {
		cb.gen = make([]uint32, n)
	} else {
		cb.gen = cb.gen[:n]
	}
	cb.token++
	if cb.token == 0 {
		// Wrapped: every prior stamp must be invalidated so it cannot equal the new token.
		for i := range cb.gen {
			cb.gen[i] = 0
		}
		cb.token = 1
	}
	return cb, cb.token
}

func putClaimBuf(cb *claimBuf) {
	if cap(cb.gen) > radixBufCap {
		return // drop oversized buffer; let it be GC'd
	}
	claimBufPool.Put(cb)
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
	const radixSize = 1 << radixBitsRefIndex
	const radixMask = radixSize - 1
	n := len(idx)
	if n < 2 {
		return
	}

	// Pass 1's count scan doubles as the OR scan: it tallies the lowest byte's histogram while
	// ORing all keys to learn the highest significant byte (NOTE-190 byte-skip bound).
	var keyOr uint32
	var hist0 [radixSize]int
	for i := range idx {
		k := idx[i].Packed
		keyOr |= k
		hist0[k&radixMask]++
	}
	radixSortRefIndexPrepared(idx, keyOr, &hist0)
}

// radixSortRefIndexPrepared sorts idx in ascending Packed order given a PRE-COMPUTED first
// (lowest-byte) histogram and key OR-fold. NOTE-235: EnsureRefIndex's build loop already
// scans every ref once to compute the Packed key and detect sorted/single-block shape (the
// ~0.84% EnsureRefIndex.func1 self-time, profile 2026-06-12). radixSortRefIndex then re-scanned
// the same N entries to tally hist[0] and keyOr before any scatter ran — a second full pass over
// the index. By accumulating hist0[k&0xFF]++ and keyOr|=k INSIDE the build loop (a cheap byte
// store + or per ref it already touches) and handing them here, the dedicated first count pass
// is eliminated entirely. The generic path runs up to three byte-scatter passes on multi-block
// indexes (the ~2.48% radixSortRefIndex self-time — the single largest blockpack frame), so
// removing one full N-element count scan is a measurable fraction of that cost. hist0 must be
// EXACTLY the lowest-byte histogram of idx and keyOr the OR-fold of every idx[i].Packed; the
// result is byte-for-byte identical to a self-counting radixSortRefIndex.
func radixSortRefIndexPrepared(idx []RefIndexEntry, keyOr uint32, hist0 *[1 << radixBitsRefIndex]int) {
	const radixBits = radixBitsRefIndex
	const radixSize = 1 << radixBits
	const radixMask = radixSize - 1
	const numBytes = 32 / radixBits
	n := len(idx)
	if n < 2 {
		return
	}
	// keyOr==0 means all keys are 0; the slice is already trivially sorted, no passes needed.
	if keyOr == 0 {
		return
	}
	var hist [numBytes][radixSize]int
	hist[0] = *hist0
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

// radixSortRefIndexLow16 sorts idx in ascending Packed order assuming every entry's Packed
// key shares the same high-16 bits (single-block decode: all refs have one BlockIdx). With a
// constant high half the ordering is fully determined by the low-16 RowIdx, so only the two
// low bytes need radix passes regardless of how large the (constant) BlockIdx is. This is the
// case radixSortRefIndex's NOTE-190 byte-skip cannot reach on its own: keyOr skips only
// all-zero byte positions, but a single-block column with BlockIdx>0 has non-zero — yet
// constant — high bytes, so the generic path would run up to four passes (two of them pure
// identity permutations over the constant high half). NOTE-228.
//
// Sorting only the low 16 bits is correct because a constant high half is order-preserving:
// for any a,b sharing it, a.Packed < b.Packed iff (a.Packed&0xFFFF) < (b.Packed&0xFFFF). The
// caller guarantees the single-block invariant (all high-16 equal); this routine never reads
// the high bits, so it is byte-for-byte identical to a full sort on such input. Mechanics are
// the NOTE-205 fused-histogram two-pass LSD radix over the low two bytes, with the NOTE-190
// byte-skip still applied to the low half (a single-byte RowIdx range runs only one pass).
func radixSortRefIndexLow16(idx []RefIndexEntry) {
	const radixSize = 1 << radixBitsRefIndex
	const radixMask = radixSize - 1
	n := len(idx)
	if n < 2 {
		return
	}
	// Pass 1's count scan doubles as the low-half OR scan (NOTE-190 byte-skip over 16 bits).
	var lowOr uint32
	var hist0 [radixSize]int
	for i := range idx {
		k := idx[i].Packed & 0xFFFF
		lowOr |= k
		hist0[k&radixMask]++
	}
	radixSortRefIndexLow16Prepared(idx, lowOr, &hist0)
}

// radixSortRefIndexLow16Prepared sorts idx in ascending Packed order over the low-16 RowIdx
// (single-block invariant: constant high-16), given the PRE-COMPUTED lowest-byte histogram and
// low-16 OR-fold. NOTE-235: the lowest byte of Packed and the lowest byte of (Packed&0xFFFF) are
// identical, so the SAME hist0 the EnsureRefIndex build loop accumulates for the generic sorter
// also seeds this one; lowOr is just keyOr&0xFFFF. The single-block fallback (NOTE-228) is a
// frequent EnsureRefIndex outcome, so eliminating its dedicated first count scan removes a full
// N-element pass here too. hist0 must be EXACTLY the lowest-byte histogram of idx and lowOr the
// OR-fold of every idx[i].Packed&0xFFFF; the result is byte-for-byte identical to a self-counting
// radixSortRefIndexLow16.
func radixSortRefIndexLow16Prepared(idx []RefIndexEntry, lowOr uint32, hist0 *[1 << radixBitsRefIndex]int) {
	const radixBits = radixBitsRefIndex
	const radixSize = 1 << radixBits
	const radixMask = radixSize - 1
	n := len(idx)
	if n < 2 {
		return
	}
	if lowOr == 0 {
		return // all RowIdx==0 (single row, or already trivially ordered)
	}
	var hist [2][radixSize]int
	hist[0] = *hist0
	// Run a second (high byte of the low half) pass only when RowIdx exceeds 8 bits.
	maxByte := 0
	if lowOr>>radixBits != 0 {
		maxByte = 1
	}
	bufPtr := getRadixBuf(n)
	defer putRadixBuf(bufPtr)
	buf := *bufPtr
	src, dst := idx, buf
	passes := 0
	for b := 0; b <= maxByte; b++ {
		shift := b * radixBits
		counts := &hist[b]
		sum := 0
		for c := range counts {
			cnt := counts[c]
			counts[c] = sum
			sum += cnt
		}
		if b < maxByte && b+1 < len(hist) {
			next := &hist[b+1] //nolint:gosec // b+1<len(hist) guarded above; bound is static
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
	if passes%2 == 1 {
		copy(idx, src)
	}
}

// scatterDictRefIndexDense sorts idx in place assuming it is a single-block dense RowIdx
// permutation: every entry's Packed key shares the same high-16 BlockIdx and the low-16
// RowIdx values are exactly the contiguous range [minRow, minRow+len(idx)). It scatters each
// element into its rank slot (rowIdx-minRow) of a pooled scratch buffer, then copies the
// sorted result back into idx. Density is verified while scattering: a slot is claimed at
// most once (collision => not a permutation) and every rank must fall in range; if either
// invariant is violated the function reports false WITHOUT mutating idx so the caller can
// fall back to the general radix sort. The pooled buffer is the same non-zeroed scratch used
// by radixSortRefIndex (NOTE-192); claimed slots are detected via a generation-stamped claim
// array (NOTE-253) so a stale buffer cannot be mistaken for a written slot without paying an
// O(N) pre-clear. NOTE-226.
func scatterDictRefIndexDense(idx []RefIndexEntry, minRow uint32) bool {
	n := len(idx)
	if n < 2 {
		return true
	}
	bufPtr := getRadixBuf(n)
	buf := *bufPtr
	// NOTE-253: generation-stamped claim detection replaces the O(N) sentinel pre-clear.
	cb, token := getClaimBuf(n)
	gen := cb.gen
	ok := true
	for i := range idx {
		rank := int((idx[i].Packed & 0xFFFF) - minRow)
		if rank < 0 || rank >= n || gen[rank] == token {
			ok = false
			break
		}
		gen[rank] = token
		buf[rank] = idx[i]
	}
	if ok {
		copy(idx, buf)
	}
	putClaimBuf(cb)
	putRadixBuf(bufPtr)
	return ok
}

// scatterDictRefIndexMultiBlockDense sorts idx in place when it is a block-bucketed dense
// permutation: the high-16 BlockIdx values form the contiguous range [minBlk, maxBlk], and
// within each block the low-16 RowIdx values are a dense permutation [blockMin, blockMin+cnt).
// This is the sorted shape of a merge of fully-present dict columns — the dominant multi-block
// decode case, which would otherwise pay the general four-pass radix (radixSortRefIndexPrepared,
// the largest blockpack self-time frame in querier CPU profiles). The sorted index is then a
// pure rank scatter: a block's run starts at the prefix-sum of all preceding blocks' counts,
// and within that run each ref lands at (rowIdx - blockMin) — strictly O(N), no histograms over
// the full key, no double-buffer multi-pass.
//
// Two linear passes over idx, plus O(numBlocks) bookkeeping:
//  1. count refs per block and capture each block's min row;
//  2. build prefix offsets (rejecting any empty block — a gap in the contiguous range means the
//     merge is not block-dense), then scatter into a pooled scratch buffer, placing each ref at
//     offsets[b] + (rowIdx - blockMin[b]) and claiming each global slot at most once.
//
// Density is verified WHILE scattering exactly as scatterDictRefIndexDense does: a slot collision
// rejects a duplicate (block,row), and a per-block rank >= that block's ref count rejects a RowIdx
// beyond the dense run (an in-block gap). On ANY violation — empty block, in-block gap, duplicate
// row, or span overflow — it returns false WITHOUT mutating idx so the caller falls back to the
// general radix sort, keeping the output byte-for-byte identical. In a dense permutation every
// (block,row) is unique, so the Packed ordering is total (no ties) and the scatter yields exactly
// the sorted order. The caller guarantees minBlk<=maxBlk via the build scan. NOTE-240.
func scatterDictRefIndexMultiBlockDense(idx []RefIndexEntry, minBlk, maxBlk uint32) bool {
	n := len(idx)
	if n < 2 {
		return true
	}
	// numBlocks must be small enough to allocate a per-block table cheaply; a pathological
	// block span (e.g. minBlk=0, maxBlk huge) is rejected so we never allocate O(blockSpan)
	// memory for a handful of refs. The span can be at most n blocks for a dense permutation
	// anyway (each block holds >=1 ref), so reject up front when it exceeds n.
	span := uint64(maxBlk) - uint64(minBlk) + 1
	if span > uint64(n) {
		return false
	}
	numBlocks := int(span) //nolint:gosec // span <= uint64(n) checked above; n is a slice len, fits int

	// Pass 1: per-block ref count, min row, and (computed below) prefix offset. The three
	// per-block tables share one backing allocation (counts | blkMin | offsets) to keep the
	// scatter to a single small heap allocation regardless of block span.
	const noMin = uint32(1) << 16
	tables := make([]uint32, 3*numBlocks)
	counts := tables[:numBlocks]
	blkMin := tables[numBlocks : 2*numBlocks]
	offsets := tables[2*numBlocks : 3*numBlocks]
	for i := range blkMin {
		blkMin[i] = noMin
	}
	for i := range idx {
		b := int(idx[i].Packed>>16) - int(minBlk)
		// b is in [0,numBlocks) because every Packed's high-16 is in [minBlk,maxBlk].
		counts[b]++
		if r := idx[i].Packed & 0xFFFF; r < blkMin[b] {
			blkMin[b] = r
		}
	}

	// Build prefix offsets and reject any empty block (a gap in the contiguous range means the
	// merge is not block-dense). offsets[b] is the start slot of block b's run in the output.
	var off uint32
	for b := 0; b < numBlocks; b++ {
		if counts[b] == 0 {
			return false
		}
		offsets[b] = off
		off += counts[b]
	}

	bufPtr := getRadixBuf(n)
	buf := *bufPtr
	// NOTE-253: generation-stamped claim detection replaces the O(N) sentinel pre-clear.
	cb, token := getClaimBuf(n)
	gen := cb.gen
	ok := true
	for i := range idx {
		b := int(idx[i].Packed>>16) - int(minBlk)
		// row >= blkMin[b] always holds (blkMin[b] is the per-block minimum from pass 1), so
		// rank is non-negative. A rank >= the block's ref count means a RowIdx beyond the dense
		// run — i.e. a gap — so the block is not a dense permutation; reject.
		rank := (idx[i].Packed & 0xFFFF) - blkMin[b]
		if rank >= counts[b] {
			ok = false
			break
		}
		slot := offsets[b] + rank
		if gen[slot] == token {
			ok = false
			break
		}
		gen[slot] = token
		buf[slot] = idx[i]
	}
	if ok {
		copy(idx, buf)
	}
	putClaimBuf(cb)
	putRadixBuf(bufPtr)
	return ok
}

// markDenseIfContiguous inspects the freshly-built (sorted) refIndex and enables the
// NOTE-229 O(1) reverse-lookup fast path when the index is a dense contiguous single-block
// RowIdx permutation: every entry shares one high-16 BlockIdx and the low-16 RowIdx values
// are exactly [minRow, minRow+n). In that case refIndex[k].Packed == hi16<<16 | (minRow+k)
// for all k, so a lookup of packedRef maps directly to rank = (packedRef&0xFFFF)-minRow with
// no binary search. The check is a single O(N) scan over the already-built index; it never
// mutates the index and only sets the col.refDense fields, which are published together with
// refIndex under the EnsureRefIndex sync.Once. It is purely a function of the data's ref
// shape (the dominant single-block dense decode), not of any query or column identity.
func (col *IntrinsicColumn) markDenseIfContiguous() {
	idx := col.refIndex
	n := len(idx)
	if n == 0 {
		return
	}
	hi16 := idx[0].Packed >> 16
	minRow := idx[0].Packed & 0xFFFF
	// A dense permutation of n rows occupies exactly [minRow, minRow+n) within the 16-bit
	// RowIdx space; reject up front if that range would overflow 16 bits.
	if int(minRow)+n > 1<<16 {
		return
	}
	want := idx[0].Packed
	for k := range idx {
		if idx[k].Packed != want {
			return
		}
		want++
	}
	col.setRefDense(hi16, minRow)
}

// setRefDense records that col.refIndex is a dense contiguous single-block RowIdx permutation
// rooted at (hi16, minRow), enabling the NOTE-229 O(1) reverse-lookup fast path. NOTE-252:
// factored out of markDenseIfContiguous so the EnsureRefIndex build paths that already PROVE
// this shape (the flat single-block contiguous case and the dict NOTE-226 dense scatter) can
// set the fields directly without paying markDenseIfContiguous's full O(N) re-scan.
func (col *IntrinsicColumn) setRefDense(hi16, minRow uint32) {
	col.refDense = true
	col.refDenseHi16 = hi16
	col.refDenseMin = minRow
}

// setRefDenseFlat records a flat-dense column whose refIndex is the identity permutation
// (Pos == rank), enabling the NOTE-354 refIndex-slice drop. It drops col.refIndex (so the
// 8-byte/row slice is GC'd and stops counting against the LRU budget) and records the count
// so denseLookupPos / DenseFlatRange can synthesize positions arithmetically. The caller must
// have proven Pos == rank for every entry (refs already in ascending row order AND a dense
// contiguous single-block permutation).
func (col *IntrinsicColumn) setRefDenseFlat(hi16, minRow, count uint32) {
	col.refDense = true
	col.refDenseFlat = true
	col.refDenseHi16 = hi16
	col.refDenseMin = minRow
	col.refDenseCount = count
	col.refIndex = nil // drop the redundant identity slice (NOTE-354)
}

// DenseFlatRange reports whether col is a flat-dense column (NOTE-354) whose refIndex slice
// was dropped, returning the (rowIdx, pos) synthesis parameters for blockIdx: rowIdx ==
// refDenseMin + i and pos == i for i in [0, count). Returns ok == false for any other column
// shape (dict-dense, sparse, multi-block), in which case callers must use BlockRefRange.
// Calls EnsureRefIndex internally so the dense fields are populated.
func (col *IntrinsicColumn) DenseFlatRange(blockIdx uint16) (minRow uint32, count int, ok bool) {
	if col == nil {
		return 0, 0, false
	}
	col.EnsureRefIndex()
	if !col.refDenseFlat || uint32(blockIdx) != col.refDenseHi16 {
		return 0, 0, false
	}
	return col.refDenseMin, int(col.refDenseCount), true
}

// denseLookupPos returns the value-array position for packedRef using the NOTE-229 dense
// fast path, or (-1, false) when the fast path does not apply. When col.refDense holds, the
// index is a dense contiguous single-block permutation, so rank = (RowIdx-minRow) is the
// position directly — provided the high-16 matches and the rank is in range. A packedRef that
// fails either guard genuinely is not in the index (the dense range is exhaustive), so a
// false ok with a valid index means "not found" without any further search.
func (col *IntrinsicColumn) denseLookupPos(packedRef uint32) (pos int, ok bool) {
	if !col.refDense {
		return -1, false
	}
	if packedRef>>16 != col.refDenseHi16 {
		return -1, true
	}
	rank := int((packedRef & 0xFFFF) - col.refDenseMin)
	// NOTE-354: a flat-dense column dropped its refIndex slice (Pos == rank), so the bound is
	// refDenseCount and the position IS the rank. A dict-dense column keeps refIndex (Pos ==
	// entryIdx) and is bounded/answered by the slice exactly as before.
	if col.refDenseFlat {
		if rank < 0 || rank >= int(col.refDenseCount) {
			return -1, true
		}
		return rank, true
	}
	if rank < 0 || rank >= len(col.refIndex) {
		return -1, true
	}
	return int(col.refIndex[rank].Pos), true
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
	// NOTE-340: buildRefIndexFlat reads col.BlockRefs, so materialize any deferred refs first.
	col.EnsureBlockRefs()
	col.refIndexOnce.Do(func() {
		switch col.Format {
		case IntrinsicFormatFlat, IntrinsicFormatXORBytes, IntrinsicFormatDeltaUint64:
			col.buildRefIndexFlat()
		case IntrinsicFormatDict:
			col.buildRefIndexDict()
		}
	})
}

// detectFlatDense reports whether refs form an in-order, single-block, dense contiguous
// permutation: every ref shares one BlockIdx, RowIdx rises by exactly 1 each step, and the
// span is [minRow, minRow+len(refs)). For a flat column whose refs are emitted in row order
// (the dominant single-block fully-present decode) this is the identity index, so the refIndex
// slice is redundant (Pos == rank). The scan is O(N) with NO allocation. Returns ok == false
// for any out-of-order, multi-block, or gapped layout, in which case the caller builds the
// full refIndex slice. NOTE-354.
func detectFlatDense(refs []BlockRef) (hi16, minRow, count uint32, ok bool) {
	n := len(refs)
	if n == 0 {
		return 0, 0, 0, false
	}
	hi16 = uint32(refs[0].BlockIdx)
	minRow = uint32(refs[0].RowIdx)
	// A dense permutation occupies exactly [minRow, minRow+n) within the 16-bit RowIdx space;
	// reject up front if that range would overflow 16 bits.
	if int(minRow)+n > 1<<16 {
		return 0, 0, 0, false
	}
	want := refs[0].RowIdx
	for i := range refs {
		if uint32(refs[i].BlockIdx) != hi16 || refs[i].RowIdx != want {
			return 0, 0, 0, false
		}
		want++ // monotone +1; the +n overflow guard above keeps this in range
	}
	return hi16, minRow, uint32(n), true //nolint:gosec
}

// buildRefIndexFlat builds the sorted refIndex for a flat/XOR/delta column (one BlockRef per
// row). Factored out of EnsureRefIndex (NOTE-252) to keep that dispatcher under the gocyclo
// threshold; the build/sort/dense logic is otherwise unchanged from the prior inline body.
func (col *IntrinsicColumn) buildRefIndexFlat() {
	// NOTE-354: cheap allocation-free pre-scan for the dominant flat-dense shape (single
	// block, refs already in ascending row order, dense contiguous [minRow, minRow+n)). When
	// it holds, Pos == rank so the entire refIndex slice is redundant — skip building (and
	// thus allocating) it altogether and record (refDenseMin, count) for arithmetic lookups.
	// This both eliminates the 8-byte/row RETAINED slice (the structural inuse_space win) AND
	// avoids the transient make([]RefIndexEntry, n) on the hot single-block decode path. Any
	// out-of-order ref, multi-block span, or gap fails the scan and falls through to the full
	// build below (byte-for-byte unchanged).
	if hi16, minRow, count, ok := detectFlatDense(col.BlockRefs); ok {
		col.setRefDenseFlat(hi16, minRow, count)
		return
	}
	idx := make([]RefIndexEntry, len(col.BlockRefs))
	// NOTE-168: build Packed keys and detect ascending order in the same pass.
	// Flat/XOR/Delta refs are emitted in row order; within a single block RowIdx
	// rises monotonically, so the packed key (BlockIdx<<16|RowIdx) is already
	// sorted for the dominant single-block decode case. When that holds we skip
	// the O(N log N) closure-driven slices.SortFunc entirely — the comparator
	// closure (cmp.Compare on Packed) was reached via slices.partitionCmpFunc and
	// showed up as a residual CPU sink on the EnsureRefIndex path after NOTE-167.
	// NOTE-228: also track whether all refs share one high-16 BlockIdx
	// (single-block decode — the dominant query-frontend shard shape). When they
	// do, the unsorted fallback only needs to sort the low-16 RowIdx, so route it
	// to radixSortRefIndexLow16 (≤2 passes) instead of the general radixSortRefIndex
	// (up to 4 passes when BlockIdx>0 makes the high bytes non-zero but constant).
	// NOTE-235: accumulate the lowest-byte radix histogram and the key OR-fold in
	// this same build scan so the chosen radix sorter skips its own first count pass.
	sorted := true
	singleBlock := true
	var prev uint32
	var hi16, keyOr uint32
	var minRow, maxRow uint32
	var hist0 [1 << radixBitsRefIndex]int
	for i, ref := range col.BlockRefs {
		p := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec
		idx[i] = RefIndexEntry{
			Packed: p,
			Pos:    int32(i), //nolint:gosec
		}
		keyOr |= p
		hist0[p&((1<<radixBitsRefIndex)-1)]++
		r := p & 0xFFFF
		if i == 0 {
			hi16 = p >> 16
			minRow = r
			maxRow = r
		} else {
			if p < prev {
				sorted = false
			}
			if p>>16 != hi16 {
				singleBlock = false
			}
			if r < minRow {
				minRow = r
			} else if r > maxRow {
				maxRow = r
			}
		}
		prev = p
	}
	switch {
	case sorted:
	case singleBlock:
		radixSortRefIndexLow16Prepared(idx, keyOr&0xFFFF, &hist0)
	default:
		radixSortRefIndexPrepared(idx, keyOr, &hist0)
	}
	col.refIndex = idx
	// NOTE-354: the flat-dense identity case (sorted + single-block + contiguous) is caught by
	// the allocation-free detectFlatDense pre-scan at the top of this function, which returns
	// before reaching here. Any column that reaches this point either needed a sort (Pos !=
	// rank) or is sparse/multi-block, so its refIndex slice is genuinely required.
	// NOTE-252: skip the markDenseIfContiguous re-scan when the build scan already
	// proved a dense single-block permutation. Within a single block RowIdx is unique
	// per row (refs are emitted in row order), so a single-block index whose RowIdx
	// span is exactly its length (maxRow-minRow+1 == n) is, by pigeonhole, the dense
	// contiguous range [minRow, minRow+n) — the exact condition markDenseIfContiguous
	// re-verifies with a full O(N) scan. Set the dense fields directly and skip that
	// scan on the dominant single-block decode path; fall back to the scan otherwise.
	if n := uint32(len(idx)); singleBlock && n > 0 && maxRow-minRow+1 == n { //nolint:gosec
		col.setRefDense(hi16, minRow)
	} else {
		col.markDenseIfContiguous()
	}
}

// buildRefIndexDict builds the sorted refIndex for a dict column (each entry owns a set of
// rows). Factored out of EnsureRefIndex (NOTE-252) to keep that dispatcher under the gocyclo
// threshold; the build/sort/dense logic is otherwise unchanged from the prior inline body.
func (col *IntrinsicColumn) buildRefIndexDict() {
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
	//
	// NOTE-226: dense single-block scatter sort. The dominant decode case is a single
	// block (query-frontend shards to 1 block per querier call), so every ref shares
	// the same BlockIdx and the Packed key differs only in the low-16 RowIdx. A dict
	// column assigns each present row exactly one entry, so the RowIdx values across
	// all entries form a permutation; when the column is fully present they are the
	// dense range [minRow, maxRow] with total == maxRow-minRow+1. In that case the
	// sorted index is a pure rank scatter — idx[rowIdx-minRow] = {Packed, entryIdx} —
	// which is O(N) with no histograms, no double buffer, and no comparison passes,
	// replacing radixSortRefIndex (the single largest blockpack self-time frame,
	// ~2.88%, profile 2026-06-12) on the hot path. We detect eligibility (single block
	// + dense permutation) during the build scan and only scatter when proven dense;
	// any high-16 variation, out-of-range row, gap, or duplicate falls back to the
	// general append + radix path, so the output is byte-for-byte identical.
	// NOTE-235: accumulate the lowest-byte radix histogram and the key OR-fold in
	// this same build scan so the non-dense radix branches skip their first count pass.
	// The dense rank-scatter branch (NOTE-226) ignores them; the per-ref array store is
	// negligible against the existing memory-bound append.
	// NOTE-240: also track the block-index span (minBlk/maxBlk). When the column
	// spans multiple blocks (singleBlock==false) the dense single-block scatter
	// (NOTE-226) and the low-16 sort (NOTE-228) cannot fire, so a merged dict column
	// falls all the way to the general four-pass radix. But a merge of fully-present
	// blocks is still a *block-bucketed* dense permutation: the blocks are a
	// contiguous range and within each block the RowIdx values are a dense
	// permutation, so the sorted index is a per-block rank scatter. We record the
	// block span here so scatterDictRefIndexMultiBlockDense can attempt that O(N)
	// scatter before paying the general sort.
	sorted := true
	singleBlock := true
	var prev uint32
	var hi16, minRow, maxRow, keyOr uint32
	var minBlk, maxBlk uint32
	var hist0 [1 << radixBitsRefIndex]int
	first := true
	for entryIdx, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			p := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec
			idx = append(idx, RefIndexEntry{
				Packed: p,
				Pos:    int32(entryIdx), //nolint:gosec
			})
			keyOr |= p
			hist0[p&((1<<radixBitsRefIndex)-1)]++
			b := p >> 16
			if first {
				hi16 = b
				minBlk = b
				maxBlk = b
				minRow = p & 0xFFFF
				maxRow = minRow
			} else {
				if p < prev {
					sorted = false
				}
				if b != hi16 {
					singleBlock = false
				}
				if b < minBlk {
					minBlk = b
				} else if b > maxBlk {
					maxBlk = b
				}
				if r := p & 0xFFFF; r < minRow {
					minRow = r
				} else if r > maxRow {
					maxRow = r
				}
			}
			prev = p
			first = false
		}
	}
	denseProven := false
	switch {
	case sorted:
		// already globally ordered (e.g. single-value dict columns)
	case singleBlock && uint32(total) == maxRow-minRow+1:
		// NOTE-226: dense single-block permutation — scatter by RowIdx rank into a
		// fresh buffer. scatterDictRefIndexDense verifies density (no gap/dup) as it
		// writes; if the permutation is not actually dense it returns false and we
		// fall back to the low-16 sort (NOTE-228) on the original append order —
		// this branch is guarded by singleBlock so the high-16 is constant.
		// NOTE-252: a true result PROVES the index is the dense contiguous single-block
		// permutation [minRow, minRow+total) under one high-16 (hi16) — exactly what
		// markDenseIfContiguous re-verifies. Record that so we can set the dense fields
		// directly and skip the redundant full O(N) re-scan below.
		if scatterDictRefIndexDense(idx, minRow) {
			denseProven = true
		} else {
			radixSortRefIndexLow16Prepared(idx, keyOr&0xFFFF, &hist0)
		}
	case singleBlock:
		// NOTE-228: single block but the RowIdx permutation is not the dense
		// contiguous range (an optional/sparse dict column — the attribute is
		// present on only some spans). RowIdx is still unique per row and the
		// high-16 BlockIdx is constant, so only the low-16 needs sorting.
		radixSortRefIndexLow16Prepared(idx, keyOr&0xFFFF, &hist0)
	default:
		// NOTE-240: multi-block merge. Attempt the block-bucketed dense scatter
		// (each block's rows a dense permutation, blocks a contiguous range) before
		// the general four-pass radix. scatterDictRefIndexMultiBlockDense verifies
		// every density invariant while it works and reports false (without mutating
		// idx) on any gap/duplicate/overflow, so a genuinely sparse or non-contiguous
		// merge is byte-for-byte identical to the general sort.
		if !scatterDictRefIndexMultiBlockDense(idx, minBlk, maxBlk) {
			radixSortRefIndexPrepared(idx, keyOr, &hist0)
		}
	}
	col.refIndex = idx
	// NOTE-252: skip the markDenseIfContiguous re-scan when scatterDictRefIndexDense
	// already proved a dense single-block permutation [minRow, minRow+total) under hi16.
	if denseProven {
		col.setRefDense(hi16, minRow)
	} else {
		col.markDenseIfContiguous()
	}
}

// lookupRefIdx returns the position index in the value arrays for packedRef,
// or -1 if not found. Calls EnsureRefIndex internally.
func (col *IntrinsicColumn) lookupRefIdx(packedRef uint32) int {
	col.EnsureRefIndex()
	// NOTE-229/352: O(1) dense fast path. When the index is a dense contiguous single-block
	// permutation the position is rank arithmetic; a fast-path miss is a genuine not-found
	// (the dense range is exhaustive), so there is no fall-through to the binary search. This
	// MUST run before the len(refIndex)==0 guard below: a flat-dense column (NOTE-354) dropped
	// its refIndex slice, so refIndex is nil yet the column has refDenseCount entries answered
	// arithmetically here.
	if dpos, ok := col.denseLookupPos(packedRef); ok {
		return dpos
	}
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
	// NOTE-354: a flat-dense column dropped its refIndex slice (Pos == rank). The hot consumer
	// (populateTypedColumnForBlock) routes such columns through DenseFlatRange and never calls
	// here, but reconstruct the identity []RefIndexEntry for any other caller so BlockRefRange
	// stays a correct, self-contained API rather than silently returning nil. This allocation
	// is off the dominant path (only a non-scatter caller of a flat-dense column reaches it).
	if col.refDenseFlat {
		if uint32(blockIdx) != col.refDenseHi16 {
			return nil
		}
		n := int(col.refDenseCount)
		if n == 0 {
			return nil
		}
		entries := make([]RefIndexEntry, n)
		for i := range n {
			entries[i] = RefIndexEntry{
				Packed: col.refDenseHi16<<16 | (col.refDenseMin + uint32(i)), //nolint:gosec
				Pos:    int32(i),                                             //nolint:gosec
			}
		}
		return entries
	}
	if len(col.refIndex) == 0 {
		return nil
	}
	// NOTE-231: dense single-block fast path. When markDenseIfContiguous set col.refDense
	// the index is a gapless single-block permutation whose entries all share refDenseHi16
	// as their high-16 BlockIdx. The block range is therefore the WHOLE index when blockIdx
	// matches refDenseHi16, and empty otherwise — answerable with one uint16 compare instead
	// of the slices.BinarySearchFunc + cmp.Compare comparator closure below. The dominant
	// querier decode is exactly this dense single-block shape, so this removes the comparator
	// closure (the per-call CPU sink that NOTE-229 eliminated for point lookups) from the
	// range path too. Sparse/multi-block columns keep the binary search unchanged.
	if col.refDense {
		if uint32(blockIdx) != col.refDenseHi16 {
			return nil
		}
		return col.refIndex
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
