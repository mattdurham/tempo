// Package sketch provides HyperLogLog, Count-Min Sketch, and BinaryFuse8
// implementations for per-block cardinality estimation, frequency estimation,
// and value membership testing.
//
// NOTE: SketchBloom is a fixed-size bloom filter for per-block column value membership
// testing. Unlike BinaryFuse8 it is incrementally updatable — one Add() per observed
// value, no key accumulation required. This eliminates the large keys []uint64 slice
// and the unbounded TopK.counts map from the write path, reducing peak heap during
// compaction by ~1 GB.
//
// Parameters: 2048-byte (16 384-bit) filter, k=7 Kirsch-Mitzenmacher hash functions.
// FPR at 1 000 unique values: ~0.01%.  FPR at 5 000: ~5%.
package sketch

import "fmt"

const (
	// SketchBloomBytes is the fixed byte size of a SketchBloom filter.
	// 2 KiB gives ~0.01% FPR at 1 000 unique values with k=7.
	SketchBloomBytes = 2048

	sketchBloomK    = 7
	sketchBloomBits = uint64(SketchBloomBytes * 8) // 16 384 — power of two
	sketchBloomMask = sketchBloomBits - 1          // 0x3FFF
)

// SketchBloom is a fixed-size Bloom filter keyed on uint64 fingerprints.
// It is NOT safe for concurrent use.
//
//nolint:revive // SketchBloom name is intentional: "Bloom" alone is too generic within this package
type SketchBloom struct {
	data [SketchBloomBytes]byte
}

// NewSketchBloom returns a zeroed (empty) SketchBloom.
func NewSketchBloom() *SketchBloom {
	return &SketchBloom{}
}

// Reset zeros all bits, returning the filter to its empty state for pool reuse.
func (b *SketchBloom) Reset() {
	b.data = [SketchBloomBytes]byte{}
}

// Add sets the k filter bits for fp using Kirsch-Mitzenmacher double hashing.
// fp should be a pre-computed HashForFuse fingerprint.
func (b *SketchBloom) Add(fp uint64) {
	h1 := fp
	// h2: rotate fp right 32 bits and force odd stride for full-cycle coverage.
	h2 := (fp>>32 | fp<<32) | 1
	for i := range uint64(sketchBloomK) {
		pos := (h1 + i*h2) & sketchBloomMask
		b.data[pos>>3] |= 1 << (pos & 7) //nolint:gosec // safe: pos&7 is 0..7, fits uint
	}
}

// Contains returns false only if fp is definitely absent from the filter.
// May return true for absent fingerprints (~0.01%–5% FPR depending on load).
func (b *SketchBloom) Contains(fp uint64) bool {
	h1 := fp
	h2 := (fp>>32 | fp<<32) | 1
	for i := range uint64(sketchBloomK) {
		pos := (h1 + i*h2) & sketchBloomMask
		if b.data[pos>>3]&(1<<(pos&7)) == 0 { //nolint:gosec // safe: pos&7 is 0..7
			return false
		}
	}
	return true
}

// Marshal returns the raw filter bytes. The returned slice is a view of the
// internal array; callers must not retain it across Reset or pool return.
func (b *SketchBloom) Marshal() []byte {
	return b.data[:]
}

// Unmarshal copies d into the filter. Returns an error if len(d) != SketchBloomBytes.
func (b *SketchBloom) Unmarshal(d []byte) error {
	if len(d) != SketchBloomBytes {
		return fmt.Errorf("sketch_bloom: Unmarshal: need %d bytes, got %d", SketchBloomBytes, len(d))
	}
	copy(b.data[:], d)
	return nil
}

// BloomContains returns false only if fp is definitely absent from the bloom filter
// represented by the raw byte slice data. data must be exactly SketchBloomBytes bytes.
// This zero-allocation variant allows the reader to query bloom filters without
// copying the underlying metadata buffer into a SketchBloom struct.
func BloomContains(data []byte, fp uint64) bool {
	if len(data) != SketchBloomBytes {
		return true // conservative: treat malformed data as "may contain"
	}
	h1 := fp
	h2 := (fp>>32 | fp<<32) | 1
	for i := range uint64(sketchBloomK) {
		pos := (h1 + i*h2) & sketchBloomMask
		if data[pos>>3]&(1<<(pos&7)) == 0 { //nolint:gosec // safe: pos&7 is 0..7
			return false
		}
	}
	return true
}
