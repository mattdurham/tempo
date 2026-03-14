// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

package queryplanner

import "math/bits"

// blockSet is a dense bitset where each bit position i represents block index i.
// It replaces map[int]struct{} for candidate block tracking to reduce allocations
// and improve cache locality for AND/OR operations.
//
// The underlying []uint64 stores ceil(n/64) words; bit i lives in word i/64, bit i%64.
type blockSet []uint64

// allBlocks returns a blockSet with the first n bits set (blocks 0..n-1).
// Returns an empty blockSet when n == 0.
func allBlocks(n int) blockSet {
	if n == 0 {
		return blockSet(nil)
	}
	words := (n + 63) / 64
	s := make(blockSet, words)
	// Set all bits in fully-used words.
	fullWords := n / 64
	for i := range fullWords {
		s[i] = ^uint64(0)
	}
	// Set remaining bits in the last partial word, if any.
	if rem := n % 64; rem != 0 {
		s[fullWords] = (uint64(1) << rem) - 1
	}
	return s
}

// test reports whether bit i is set.
// Returns false for out-of-range indices.
func (s blockSet) test(i int) bool {
	w := i / 64
	if w >= len(s) {
		return false
	}
	return s[w]>>uint(i%64)&1 == 1 //nolint:gosec // i%64 is always in [0,63], safe shift amount
}

// set sets bit i.
// Panics if i is out of range (i >= s.numBlocks()).
func (s blockSet) set(i int) {
	s[i/64] |= uint64(1) << uint(i%64) //nolint:gosec // i%64 is always in [0,63], safe shift amount
}

// clear clears bit i.
// Panics if i is out of range (i >= s.numBlocks()).
func (s blockSet) clear(i int) {
	s[i/64] &^= uint64(1) << uint(i%64) //nolint:gosec // i%64 is always in [0,63], safe shift amount
}

// and performs an in-place bitwise AND with other (s &= other).
// Words beyond len(other) in s are cleared.
func (s blockSet) and(other blockSet) {
	for i := range s {
		if i < len(other) {
			s[i] &= other[i]
		} else {
			s[i] = 0
		}
	}
}

// or performs an in-place bitwise OR with other (s |= other).
// Words beyond len(s) in other are ignored.
func (s blockSet) or(other blockSet) {
	for i := range s {
		if i < len(other) {
			s[i] |= other[i]
		}
	}
}

// count returns the number of set bits (popcount).
func (s blockSet) count() int {
	n := 0
	for _, w := range s {
		n += bits.OnesCount64(w)
	}
	return n
}

// iter calls fn for each set bit index in ascending order.
func (s blockSet) iter(fn func(int)) {
	for wi, w := range s {
		base := wi * 64
		for w != 0 {
			tz := bits.TrailingZeros64(w)
			fn(base + tz)
			w &^= uint64(1) << uint(tz) //nolint:gosec // tz=TrailingZeros64 returns 0..63, safe shift
		}
	}
}

// numBlocks returns the maximum capacity (number of representable bit positions),
// which is len(s)*64.
func (s blockSet) numBlocks() int {
	return len(s) * 64
}
