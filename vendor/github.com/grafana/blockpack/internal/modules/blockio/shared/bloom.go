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
