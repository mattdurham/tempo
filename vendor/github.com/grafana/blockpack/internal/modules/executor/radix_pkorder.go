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
// Up to four counting passes (one byte each, least significant byte of the key first) over
// the 32-bit key. The scratch buffer is drawn from compactUint64Pool (the same pool that
// backs pkOrder), so the only extra cost is one pooled acquire/release amortized across blocks.
//
// NOTE-193: skip leading-zero key-byte passes by key magnitude, mirroring the magnitude-skip
// already proven on the ref-index radix sort (NOTE-190, blockio/shared.radixSortRefIndex).
// The sort key is packKey = BlockIdx<<16 | RowIdx held in the high 32 bits. query-frontend
// shards to one block per querier call (mission 2026-06-09), so BlockIdx is 0 (or a tiny
// single-block value) in the dominant case — the top two key bytes are then identically zero
// and their passes are pure identity reorderings. A single O(N) OR-scan of the key bytes bounds
// how many passes are actually significant; for the single-block N=1 count/rate path (M1/M4/M9)
// this halves the four-pass cost to two. With an odd number of significant passes the sorted
// data lands in the scratch buffer, so a final copy-back into s is needed (the prior fixed
// four-pass form was always even and relied on src==s after the last pass).
//
// NOTE-206: fuse each pass's count scan into the prior pass's scatter scan ("look-ahead
// histograms"), the same reorganization proven on the ref-index radix sort (NOTE-205,
// blockio/shared.radixSortRefIndex). The classic LSD loop read every element (2·passes+1)
// times: one OR scan, then one count scan plus one scatter scan per pass. The scatter of
// pass k already reads every src[i] key, so it now tallies pass k+1's histogram for free in
// the same pass. The only standalone count scan is the first byte's, which doubles as the
// NOTE-193 OR scan. Source reads drop from (2·passes+1)·N to (passes+1)·N. Work stays bounded
// by the actual significant-pass count (NOTE-193 byte-skip), so the dominant 2-pass
// single-block case (BlockIdx==0, packKey==RowIdx) never tallies a byte it will not use.
// Pure reorganization of when counts are tallied — prefix sum, scatter order, pass count, and
// odd-pass copy-back are unchanged, so the output is byte-for-byte identical.
func radixSortByPackKey(s []uint64) {
	const radixBits = 8
	const radixSize = 1 << radixBits
	const radixMask = radixSize - 1
	const numBytes = 32 / radixBits
	// keyShiftBase is the bit offset of the LOW byte of the packKey within the uint64.
	// packKey occupies the high 32 bits, so its least-significant byte starts at bit 32.
	const keyShiftBase = 32

	n := len(s)
	if n < 2 {
		return
	}

	// Pass 0's count scan doubles as the OR scan: it tallies the lowest key byte's histogram
	// while ORing all high-32-bit keys to learn the highest significant byte (NOTE-193).
	var keyOr uint32
	var hist [numBytes][radixSize]int
	for i := range s {
		k := uint32(s[i] >> keyShiftBase) //nolint:gosec
		keyOr |= k
		hist[0][k&radixMask]++
	}
	if keyOr == 0 {
		// All keys equal 0 — already trivially sorted by key (tie order is arbitrary).
		return
	}
	// maxByte is the index (0..3) of the highest non-zero key byte.
	maxByte := 0
	for b := 0; b < numBytes; b++ {
		if (keyOr>>(b*radixBits))&radixMask != 0 {
			maxByte = b
		}
	}

	buf := acquireCompactUint64(n)
	defer releaseCompactUint64(buf)

	src, dst := s, buf
	passes := 0
	for byteIdx := 0; byteIdx <= maxByte; byteIdx++ {
		shift := keyShiftBase + byteIdx*radixBits
		counts := &hist[byteIdx]
		// Prefix sum: counts[b] becomes the start offset of bucket b in dst.
		sum := 0
		for b := range counts {
			c := counts[b]
			counts[b] = sum
			sum += c
		}
		// Scatter into dst by this byte. When a further pass follows, tally its histogram in
		// the same scan (look-ahead) so it needs no standalone count pass. maxByte < numBytes
		// always (it is a byte index within the 32-bit key), so byteIdx < maxByte gives
		// byteIdx+1 < numBytes; the explicit numBytes guard makes the array bound static.
		if byteIdx < maxByte && byteIdx+1 < numBytes {
			next := &hist[byteIdx+1] //nolint:gosec // byteIdx+1<numBytes guarded above; static bound
			nextShift := shift + radixBits
			for i := range src {
				k := src[i]
				b := (k >> shift) & radixMask
				dst[counts[b]] = k
				counts[b]++
				next[(k>>nextShift)&radixMask]++
			}
		} else {
			for i := range src {
				b := (src[i] >> shift) & radixMask
				dst[counts[b]] = src[i]
				counts[b]++
			}
		}
		src, dst = dst, src
		passes++
	}
	// After an even number of passes src == s; after an odd number the sorted data is in buf
	// (now referenced by src), so copy it back so the result always lands in s.
	if passes%2 == 1 {
		copy(s, src)
	}
}
