package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// This file provides bloom filter helpers for paged intrinsic dict columns.
// Unlike the trace-ID bloom (which hashes 16-byte UUIDs directly), these functions
// accept arbitrary byte slices and use inline FNV-1a double-hashing.

// FNV-1a constants (64-bit).
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// IntrinsicBloomSize returns the byte size for a bloom filter covering itemCount unique
// values. Sized at IntrinsicPageBloomBitsPerItem bits per item, clamped to
// [IntrinsicPageBloomMinBytes, 4096].
func IntrinsicBloomSize(itemCount int) int {
	if itemCount <= 0 {
		return IntrinsicPageBloomMinBytes
	}
	b := (itemCount*IntrinsicPageBloomBitsPerItem + 7) / 8
	if b < IntrinsicPageBloomMinBytes {
		return IntrinsicPageBloomMinBytes
	}
	if b > 4096 {
		return 4096
	}
	return b
}

// intrinsicBloomHashes derives (h1, h2) for Kirsch-Mitzenmacher double-hashing
// from an arbitrary byte key using inline FNV-1a. Zero allocations.
// h2 is forced odd for good stride distribution.
func intrinsicBloomHashes(key []byte) (h1, h2 uint64) {
	// First pass: FNV-1a of key → h1.
	h := uint64(fnvOffset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	h1 = h
	// Second pass: continue hashing key again → h2.
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	h2 = h | 1 // force odd
	return h1, h2
}

// AddIntrinsicToBloom adds key to the bloom filter using FNV-1a double-hashing.
// No-op for nil or empty bloom slices.
func AddIntrinsicToBloom(bloom []byte, key []byte) {
	if len(bloom) == 0 {
		return
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := intrinsicBloomHashes(key)
	for i := range uint64(IntrinsicPageBloomK) {
		pos := (h1 + i*h2) % m
		bloom[pos/8] |= 1 << (pos % 8) //nolint:gosec // safe: pos%8 is always 0..7
	}
}

// TestIntrinsicBloom returns false only if key is definitely absent from the filter.
// Returns true for nil or empty bloom (vacuous — no false negatives).
func TestIntrinsicBloom(bloom []byte, key []byte) bool {
	if len(bloom) == 0 {
		return true
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := intrinsicBloomHashes(key)
	for i := range uint64(IntrinsicPageBloomK) {
		pos := (h1 + i*h2) % m
		if bloom[pos/8]&(1<<(pos%8)) == 0 { //nolint:gosec // safe: pos%8 is always 0..7
			return false
		}
	}
	return true
}
