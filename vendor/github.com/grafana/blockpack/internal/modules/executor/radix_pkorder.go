package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// radixSortByPackKey sorts a []uint64 in ascending order of its HIGH 32 bits (the packKey),
// using an LSD radix sort over those 4 key bytes. Each element is a packed (packKey<<32 | idx)
// value where the low 32 bits are a tag-along position index that travels with the key but
// does NOT participate in ordering.
//
// NOTE-175: this replaces slices.Sort(pkOrder) on the compact N=1 count/rate and histogram
// group-by scan paths (streamCountRateN1Compact, the duration/histogram variants, and the
// merge-join matched-subset reorder). slices.Sort on []uint64 dispatches to the comparison-based
// pdqsortOrdered (O(N log N)); for the full N-element pkOrder array (up to ~7.2 M entries) that
// is a large self-cost on the CONFIRMED-CPU-BOUND queriers. This is the same closure-free radix
// technique already profile-proven on the ref-index unsorted fallback (NOTE-174,
// blockio/shared.radixSortRefIndex).
//
// Ordering is by packKey only; entries with equal packKey keep an arbitrary-but-consistent
// relative order. The consumers (streamCountRateN1CompactCore via scanGroupByColCompact, and the
// merge-join output assembly) iterate the sorted slice positionally and binary-search packKeys —
// they do not depend on within-packKey tie order, exactly as with the prior non-stable
// slices.Sort. Each element's low-32 index is preserved alongside its key.
//
// Four counting passes (one byte each, least significant byte of the key first) over the
// 32-bit key leave the result in the original slice (4 is even, so src==s after the final
// pass — no copy-back). The scratch buffer is drawn from compactUint64Pool (the same pool that
// backs pkOrder), so the only extra cost is one pooled acquire/release amortized across blocks.
func radixSortByPackKey(s []uint64) {
	const radixBits = 8
	const radixSize = 1 << radixBits
	const radixMask = radixSize - 1
	// keyShiftBase is the bit offset of the LOW byte of the packKey within the uint64.
	// packKey occupies the high 32 bits, so its least-significant byte starts at bit 32.
	const keyShiftBase = 32

	n := len(s)
	if n < 2 {
		return
	}
	buf := acquireCompactUint64(n)
	defer releaseCompactUint64(buf)

	src, dst := s, buf
	var counts [radixSize]int
	for byteIdx := 0; byteIdx < 4; byteIdx++ {
		shift := keyShiftBase + byteIdx*radixBits
		for i := range counts {
			counts[i] = 0
		}
		for i := range src {
			counts[(src[i]>>shift)&radixMask]++
		}
		// Prefix sum: counts[b] becomes the start offset of bucket b in dst.
		sum := 0
		for b := range counts {
			c := counts[b]
			counts[b] = sum
			sum += c
		}
		for i := range src {
			b := (src[i] >> shift) & radixMask
			dst[counts[b]] = src[i]
			counts[b]++
		}
		src, dst = dst, src
	}
	// After 4 (even) passes src == s, so no final copy is needed.
}
