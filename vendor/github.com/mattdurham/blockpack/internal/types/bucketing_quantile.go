package ondisk

import (
	"bytes"
	"sort"
)

const (
	// DefaultNumBuckets is the default number of buckets for range encoding.
	DefaultNumBuckets = 100
)

// GetBucketIDFloat64 finds the bucket ID for a float64 value using binary search.
//
// Algorithm: sort.Search finds the first boundary > value
// Returns the bucket index (0-based)
//
// Time complexity: O(log n) - typically 7 comparisons for 100 buckets
// Space complexity: O(1) - zero allocations
//
// Example:
//
//	boundaries = [0, 25, 50, 75, 100]
//	GetBucketIDFloat64(30, boundaries) → 1  (falls in bucket [25, 50))
//	GetBucketIDFloat64(50, boundaries) → 2  (falls in bucket [50, 75))
func GetBucketIDFloat64(value float64, boundaries []float64) uint16 {
	if len(boundaries) <= 1 {
		return 0
	}

	// Binary search: find first boundary > value
	idx := sort.Search(len(boundaries)-1, func(i int) bool {
		return value < boundaries[i+1]
	})

	// Clamp to valid range
	if idx >= len(boundaries)-1 {
		idx = len(boundaries) - 2
	}
	if idx < 0 {
		idx = 0
	}

	return uint16(idx) //nolint:gosec
}

// GetBucketIDBytes finds bucket ID using binary search on bytes.
func GetBucketIDBytes(value []byte, boundaries [][]byte) uint16 {
	if len(boundaries) <= 1 {
		return 0
	}

	idx := sort.Search(len(boundaries)-1, func(i int) bool {
		return bytes.Compare(value, boundaries[i+1]) < 0
	})

	if idx >= len(boundaries)-1 {
		idx = len(boundaries) - 2
	}
	if idx < 0 {
		idx = 0
	}

	return uint16(idx) //nolint:gosec
}

// GetBucketIDString finds bucket ID using binary search on strings.
func GetBucketIDString(value string, boundaries []string) uint16 {
	if len(boundaries) <= 1 {
		return 0
	}

	idx := sort.Search(len(boundaries)-1, func(i int) bool {
		return value < boundaries[i+1]
	})

	if idx >= len(boundaries)-1 {
		idx = len(boundaries) - 2
	}
	if idx < 0 {
		idx = 0
	}

	return uint16(idx) //nolint:gosec
}
