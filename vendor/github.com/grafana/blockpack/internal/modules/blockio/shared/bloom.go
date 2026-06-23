// Package shared provides common types and interfaces for the blockio packages.
package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
)

// TraceIDBloomSize returns the byte size of the trace ID bloom filter for the given trace count.
// Sized at TraceIDBloomBitsPerTrace bits per trace (≈0.8% FP with k=7), clamped to
// [TraceIDBloomMinBytes, TraceIDBloomMaxBytes].
func TraceIDBloomSize(traceCount int) int {
	if traceCount <= 0 {
		return TraceIDBloomMinBytes
	}
	bytes := (traceCount*TraceIDBloomBitsPerTrace + 7) / 8
	if bytes < TraceIDBloomMinBytes {
		return TraceIDBloomMinBytes
	}
	if bytes > TraceIDBloomMaxBytes {
		return TraceIDBloomMaxBytes
	}
	return bytes
}

// AddTraceIDToBloom adds a 16-byte trace ID to the bloom filter.
// Uses Kirsch-Mitzenmacher double-hashing: h_i = (h1 + i*h2) mod m, with h1 and h2
// derived directly from the trace ID bytes (which are already random UUIDs).
// No-op for nil or empty bloom slices.
func AddTraceIDToBloom(bloom []byte, traceID [16]byte) {
	if len(bloom) == 0 {
		return
	}
	m := uint64(len(bloom)) * 8
	h1 := binary.LittleEndian.Uint64(traceID[0:8])
	h2 := binary.LittleEndian.Uint64(traceID[8:16]) | 1 // force odd for good stride distribution
	for i := range uint64(TraceIDBloomK) {
		pos := (h1 + i*h2) % m
		bloom[pos/8] |= 1 << (pos % 8) //nolint:gosec // safe: pos%8 is always 0..7, fits in uint
	}
}

// TestTraceIDBloom returns false only if traceID is definitely absent from the filter.
// Returns true for nil or empty bloom (vacuous — no false negatives for old files).
func TestTraceIDBloom(bloom []byte, traceID [16]byte) bool {
	if len(bloom) == 0 {
		return true
	}
	m := uint64(len(bloom)) * 8
	h1 := binary.LittleEndian.Uint64(traceID[0:8])
	h2 := binary.LittleEndian.Uint64(traceID[8:16]) | 1
	for i := range uint64(TraceIDBloomK) {
		pos := (h1 + i*h2) % m
		if bloom[pos/8]&(1<<(pos%8)) == 0 { //nolint:gosec // safe: pos%8 is always 0..7, fits in uint
			return false
		}
	}
	return true
}

// NOTE-468 (issue #388): per-chunk span-ID bloom. A SpanTree chunk holds the records of many
// traces; a span-ID lookup (span→trace) needs to find which chunk(s) may contain a given 8-byte
// span ID without decoding every chunk. Each sealed chunk carries a fixed-size span-ID bloom
// (SpanTreeChunkBloomSize); SpanTreeChunksForSpan probes all chunk blooms in memory.
//
// Span IDs are only 8 bytes (one uint64), so the Kirsch-Mitzenmacher second hash is derived by
// mixing the single word rather than reading a second word. The mix constant is the
// splitmix64 finalizer's odd multiplier, forced odd so the stride covers the filter well.

// spanIDH1H2 derives the two double-hashing seeds from an 8-byte span ID.
func spanIDH1H2(spanID [8]byte) (h1, h2 uint64) {
	h1 = binary.LittleEndian.Uint64(spanID[:])
	// Mix to a well-distributed odd second hash (splitmix64-style finalizer step).
	z := h1
	z ^= z >> 30
	z *= 0xbf58476d1ce4e5b9
	z ^= z >> 27
	h2 = z | 1 // force odd for good stride distribution
	return h1, h2
}

// AddSpanIDToBloom adds an 8-byte span ID to a fixed-size per-chunk span-ID bloom filter.
// No-op for nil or empty bloom slices.
func AddSpanIDToBloom(bloom []byte, spanID [8]byte) {
	if len(bloom) == 0 {
		return
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := spanIDH1H2(spanID)
	for i := range uint64(SpanIDBloomK) {
		pos := (h1 + i*h2) % m
		bloom[pos/8] |= 1 << (pos % 8) //nolint:gosec // safe: pos%8 is always 0..7, fits in uint
	}
}

// TestSpanIDBloom returns false only if spanID is definitely absent from the filter.
// Returns true for a nil or empty bloom (vacuous — no false negatives for v1 files).
func TestSpanIDBloom(bloom []byte, spanID [8]byte) bool {
	if len(bloom) == 0 {
		return true
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := spanIDH1H2(spanID)
	for i := range uint64(SpanIDBloomK) {
		pos := (h1 + i*h2) % m
		if bloom[pos/8]&(1<<(pos%8)) == 0 { //nolint:gosec // safe: pos%8 is always 0..7, fits in uint
			return false
		}
	}
	return true
}
