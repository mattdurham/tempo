// Package sketch provides HyperLogLog, Count-Min Sketch, and BinaryFuse8
// implementations for per-block cardinality estimation, frequency estimation,
// and value membership testing.
//
// NOTE: BinaryFuse8 wraps xorfilter.BinaryFuse8 for per-block value membership testing.
// SPEC-SK-12 through SPEC-SK-16 govern this file.
// No false negatives (SPEC-SK-12). Empty filter returns false for all queries (SPEC-SK-15).
package sketch

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/FastFilter/xorfilter"
)

// BinaryFuse8 wraps xorfilter.BinaryFuse8 with MarshalBinary/UnmarshalBinary.
// SPEC-SK-12: no false negatives.
// SPEC-SK-13: ~0.39% false positive rate.
// NOTE-SK-05: chosen over bloom for lower FPR and cache-friendly lookups.
type BinaryFuse8 struct {
	inner *xorfilter.BinaryFuse8
}

// NewBinaryFuse8 builds a BinaryFuse8 from the given uint64 keys.
// Duplicate keys are removed before construction (SPEC-SK-16, Edge Case 5).
// Returns an empty filter (Contains always false) for nil or empty keys. (SPEC-SK-15)
func NewBinaryFuse8(keys []uint64) (*BinaryFuse8, error) {
	if len(keys) == 0 {
		return &BinaryFuse8{}, nil
	}
	deduped := deduplicateUint64(keys)
	f, err := xorfilter.PopulateBinaryFuse8(deduped)
	if err != nil {
		return nil, fmt.Errorf("fuse8: construction: %w", err)
	}
	return &BinaryFuse8{inner: f}, nil
}

// Contains returns true if key was in the construction set (no false negatives).
// May return true for absent keys (~0.39% FPR). (SPEC-SK-12, SPEC-SK-13)
func (f *BinaryFuse8) Contains(key uint64) bool {
	if f.inner == nil {
		return false
	}
	return f.inner.Contains(key)
}

// MarshalBinary serializes the filter using xorfilter's Save method.
// Returns empty slice for an empty filter. (SPEC-SK-15)
func (f *BinaryFuse8) MarshalBinary() ([]byte, error) {
	if f.inner == nil {
		return []byte{}, nil
	}
	var buf bytes.Buffer
	if err := f.inner.Save(&buf); err != nil {
		return nil, fmt.Errorf("fuse8: MarshalBinary: %w", err)
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary deserializes the filter from b.
// Returns error if b is too short or malformed. (SPEC-SK-14)
func (f *BinaryFuse8) UnmarshalBinary(b []byte) error {
	if len(b) < 28 { // minimum: 8(seed)+4(segLen)+4(segLenMask)+4(segCount)+4(segCountLen)+4(fpLen)
		return fmt.Errorf("fuse8: UnmarshalBinary: need at least 28 bytes, got %d", len(b))
	}
	r := bytes.NewReader(b)
	inner, err := xorfilter.LoadBinaryFuse8(r)
	if err != nil {
		return fmt.Errorf("fuse8: UnmarshalBinary: %w", err)
	}
	f.inner = inner
	return nil
}

// deduplicateUint64 returns a sorted, deduplicated slice from keys.
// A copy is made to avoid mutating the caller's slice.
// Required before NewBinaryFuse8: the peeling algorithm requires unique keys.
func deduplicateUint64(keys []uint64) []uint64 {
	if len(keys) == 0 {
		return keys
	}
	cp := make([]uint64, len(keys))
	copy(cp, keys)
	sortUint64(cp)
	out := cp[:1]
	for i := 1; i < len(cp); i++ {
		if cp[i] != cp[i-1] {
			out = append(out, cp[i])
		}
	}
	return out
}

// sortUint64 sorts a uint64 slice in ascending order using pdqsort (O(n log n)).
func sortUint64(a []uint64) {
	slices.Sort(a)
}
