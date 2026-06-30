package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// bucketbloom.go — bloom filter over canonical column values for the v2 BucketGroup
// file format (NOTE-VI-043, issue #427).
//
// Each block in a value-index file carries a bloom filter over the canonical values
// it contains. The querier tests a query value against the block's bloom before reading
// the block: a negative test definitively skips the block, a positive test (possibly a
// false positive) reads it. False positives only cost an unnecessary block read, never
// a wrong answer, so the filter is conservative-correct.
//
// Unlike the trace-ID bloom (shared.AddTraceIDToBloom) which assumes 16-byte uniformly
// random input, canonical values are variable-length and not uniformly distributed, so
// this filter derives two independent 64-bit hashes via FNV-1a over the value bytes and
// a salted second pass, then applies Kirsch-Mitzenmacher double hashing.

import "math"

// valueBloomK is the number of hash probes per value. k=7 targets ~0.8% false-positive
// rate at ~10 bits/value, matching the trace-ID bloom's tuning.
const valueBloomK = 7

// valueBloomBitsPerValue is the target number of bits allocated per distinct value.
const valueBloomBitsPerValue = 10

// valueBloomMinBytes / valueBloomMaxBytes clamp the per-block bloom size so a block with
// very few or very many distinct values still produces a sanely-sized filter.
const (
	valueBloomMinBytes = 16
	valueBloomMaxBytes = 1 << 20 // 1 MiB
)

// ValueBloomSize returns the byte size of a value bloom filter sized for distinctValues
// distinct entries, clamped to [valueBloomMinBytes, valueBloomMaxBytes].
func ValueBloomSize(distinctValues int) int {
	if distinctValues <= 0 {
		return valueBloomMinBytes
	}
	n := (distinctValues*valueBloomBitsPerValue + 7) / 8
	if n < valueBloomMinBytes {
		return valueBloomMinBytes
	}
	if n > valueBloomMaxBytes {
		return valueBloomMaxBytes
	}
	return n
}

// valueBloomHashes returns the two 64-bit base hashes (h1, h2) for a canonical value.
// h1 is FNV-1a; h2 is a salted FNV-1a forced odd so the double-hashing stride co-prime
// with the bit count for any power-of-two-ish modulus, giving good probe distribution.
func valueBloomHashes(value []byte) (uint64, uint64) {
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)
	h1 := uint64(fnvOffset)
	for _, b := range value {
		h1 ^= uint64(b)
		h1 *= fnvPrime
	}
	// Second independent hash: salt the offset basis and re-run.
	h2 := uint64(fnvOffset) ^ 0x9E3779B97F4A7C15
	for _, b := range value {
		h2 ^= uint64(b)
		h2 *= fnvPrime
	}
	return h1, h2 | 1
}

// AddValueToBloom sets the bits for value in bloom. No-op for an empty filter.
func AddValueToBloom(bloom, value []byte) {
	if len(bloom) == 0 {
		return
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := valueBloomHashes(value)
	for i := range uint64(valueBloomK) {
		pos := (h1 + i*h2) % m
		bloom[pos/8] |= 1 << (pos % 8) //nolint:gosec // pos%8 ∈ 0..7
	}
}

// TestValueBloom returns false only if value is definitely absent from the filter.
// Returns true for an empty filter (vacuous — no false negatives).
func TestValueBloom(bloom, value []byte) bool {
	if len(bloom) == 0 {
		return true
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := valueBloomHashes(value)
	for i := range uint64(valueBloomK) {
		pos := (h1 + i*h2) % m
		if bloom[pos/8]&(1<<(pos%8)) == 0 { //nolint:gosec // pos%8 ∈ 0..7
			return false
		}
	}
	return true
}

// EstimatedFalsePositiveRate returns the theoretical FP rate of a value bloom of byteLen
// bytes holding n distinct values with k probes: (1 - e^(-k*n/m))^k where m = byteLen*8.
// Used by tests and tuning; not on the query hot path.
func EstimatedFalsePositiveRate(byteLen, n int) float64 {
	if byteLen <= 0 || n <= 0 {
		return 0
	}
	m := float64(byteLen) * 8
	k := float64(valueBloomK)
	return math.Pow(1-math.Exp(-k*float64(n)/m), k)
}
