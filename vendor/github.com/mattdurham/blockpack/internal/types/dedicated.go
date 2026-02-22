package ondisk

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Note: Dedicated columns are now auto-detected at write time.
// All columns are automatically indexed with appropriate types:
// - Numeric types (int64, uint64) → Range-bucketed
// - Bytes types (UUIDs, IDs) → Range-bucketed by first 8 bytes
// - String, bool, float64 → Regular dedicated columns
// No hardcoded list needed!

// DedicatedValueKey captures a typed value used for dedicated column lookups.
type DedicatedValueKey struct {
	data []byte
	typ  ColumnType
}

// Type returns the column type of the key.
func (k DedicatedValueKey) Type() ColumnType {
	return k.typ
}

// Data returns the raw bytes for the key. Do not modify the returned slice.
func (k DedicatedValueKey) Data() []byte {
	return k.data
}

// StringValueKey builds a dedicated key for string values.
func StringValueKey(val string) DedicatedValueKey {
	return DedicatedValueKey{typ: ColumnTypeString, data: []byte(val)}
}

// BytesValueKey builds a dedicated key for byte slice values.
func BytesValueKey(val []byte) DedicatedValueKey {
	cp := make([]byte, len(val))
	copy(cp, val)
	return DedicatedValueKey{typ: ColumnTypeBytes, data: cp}
}

// IntValueKey builds a dedicated key for int64 values.
func IntValueKey(val int64) DedicatedValueKey {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(val)) //nolint:gosec
	return DedicatedValueKey{typ: ColumnTypeInt64, data: buf[:]}
}

// UintValueKey builds a dedicated key for uint64 values.
func UintValueKey(val uint64) DedicatedValueKey {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], val)
	return DedicatedValueKey{typ: ColumnTypeUint64, data: buf[:]}
}

// FloatValueKey builds a dedicated key for float64 values.
func FloatValueKey(val float64) DedicatedValueKey {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(val))
	return DedicatedValueKey{typ: ColumnTypeFloat64, data: buf[:]}
}

// BoolValueKey builds a dedicated key for boolean values.
func BoolValueKey(val bool) DedicatedValueKey {
	b := byte(0)
	if val {
		b = 1
	}
	return DedicatedValueKey{typ: ColumnTypeBool, data: []byte{b}}
}

// RangeInt64ValueKey builds a dedicated key for range-bucketed int64 values.
// This is used when a range column falls back to storing raw values (< 100 unique values).
func RangeInt64ValueKey(val int64, rangeType ColumnType) DedicatedValueKey {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(val)) //nolint:gosec
	return DedicatedValueKey{typ: rangeType, data: buf[:]}
}

// RangeFloat64ValueKey builds a dedicated key for range-bucketed float64 values.
// This is used when a range column falls back to storing raw values (< 100 unique values).
func RangeFloat64ValueKey(val float64, rangeType ColumnType) DedicatedValueKey {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(val))
	return DedicatedValueKey{typ: rangeType, data: buf[:]}
}

// RangeBytesValueKey builds a dedicated key for range-bucketed bytes values.
// This is used when a range column falls back to storing raw values (< 100 unique values).
func RangeBytesValueKey(val []byte, rangeType ColumnType) DedicatedValueKey {
	cp := make([]byte, len(val))
	copy(cp, val)
	return DedicatedValueKey{typ: rangeType, data: cp}
}

// RangeStringValueKey builds a dedicated key for range-bucketed string values.
// This is used when a range column falls back to storing raw values (< 100 unique values).
func RangeStringValueKey(val string, rangeType ColumnType) DedicatedValueKey {
	return DedicatedValueKey{typ: rangeType, data: []byte(val)}
}

func (k DedicatedValueKey) encode() string {
	out := make([]byte, 1+len(k.data))
	out[0] = byte(k.typ)
	copy(out[1:], k.data)
	return string(out)
}

// Encode returns the encoded key for use in indexes.
func (k DedicatedValueKey) Encode() string {
	return k.encode()
}

func decodeDedicatedKey(encoded string) (DedicatedValueKey, error) {
	if len(encoded) == 0 {
		return DedicatedValueKey{}, fmt.Errorf("empty dedicated key")
	}
	return DedicatedValueKey{
		typ:  ColumnType(encoded[0]),
		data: []byte(encoded[1:]),
	}, nil
}

// DecodeDedicatedKey parses an encoded key from index storage.
func DecodeDedicatedKey(encoded string) (DedicatedValueKey, error) {
	return decodeDedicatedKey(encoded)
}

// Range bucket constants
const (
	MaxRangeBuckets       = 100 // Maximum number of buckets for range columns
	MinValuesForBucketing = 100 // If fewer unique values, use normal dedicated column
)

// RangeBucketValueKey creates a dedicated key for a range bucket ID
// Bucket IDs are uint16 (0-999 for 1000 buckets, max 65535)
func RangeBucketValueKey(bucketID uint16, rangeType ColumnType) DedicatedValueKey {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], bucketID)
	return DedicatedValueKey{typ: rangeType, data: buf[:]}
}

// IsRangeColumnType returns true if the column type is a range-bucketed type
func IsRangeColumnType(typ ColumnType) bool {
	return typ == ColumnTypeRangeInt64 ||
		typ == ColumnTypeRangeUint64 ||
		typ == ColumnTypeRangeDuration ||
		typ == ColumnTypeRangeFloat64 ||
		typ == ColumnTypeRangeBytes ||
		typ == ColumnTypeRangeString
}

// GetBucketID returns the bucket ID (0 to numBuckets-1) for a given value
// Buckets are: [b[0], b[1]), [b[1], b[2]), ..., [b[n-1], b[n]]
// Last bucket is inclusive on both ends
// Uses binary search for O(log n) performance
func GetBucketID(value int64, buckets []int64) uint16 {
	if len(buckets) == 0 {
		return 0
	}
	if value <= buckets[0] {
		return 0
	}
	if value >= buckets[len(buckets)-1] {
		//nolint:gosec // Reviewed and acceptable
		return uint16(len(buckets) - 2) // Last bucket
	}

	// Binary search: find smallest index i where buckets[i] > value
	i := sort.Search(len(buckets), func(j int) bool {
		return buckets[j] > value
	})

	// value is in bucket [i-1, i), so return bucket ID i-1
	return uint16(i - 1) //nolint:gosec // Reviewed and acceptable
}

// GetBucketsForRange returns bucket IDs that intersect with [minValue, maxValue]
// minInclusive/maxInclusive control whether endpoints are included
func GetBucketsForRange(minValue, maxValue *int64, minInclusive, maxInclusive bool, buckets []int64) []uint16 {
	if len(buckets) == 0 {
		return nil
	}

	numBuckets := len(buckets) - 1
	result := make([]uint16, 0, numBuckets)

	for bucketID := 0; bucketID < numBuckets; bucketID++ {
		bucketMin := buckets[bucketID]
		bucketMax := buckets[bucketID+1]

		// Check if this bucket intersects with the query range
		intersects := rangeIntersectsBucket(
			minValue,
			maxValue,
			minInclusive,
			maxInclusive,
			bucketMin,
			bucketMax,
			bucketID == numBuckets-1,
		)
		if intersects {
			result = append(result, uint16(bucketID)) //nolint:gosec
		}
	}

	return result
}

// rangeIntersectsBucket checks if query range intersects with bucket range
func rangeIntersectsBucket(
	queryMin, queryMax *int64,
	minInclusive, maxInclusive bool,
	bucketMin, bucketMax int64,
	isLastBucket bool,
) bool {
	// Bucket range: [bucketMin, bucketMax) for all buckets except last
	// Last bucket: [bucketMin, bucketMax] (inclusive on both ends)

	// No lower bound on query - starts from -∞
	if queryMin == nil {
		// Check if bucket starts before queryMax
		if queryMax == nil {
			return true // No upper bound either - matches all
		}
		// Bucket starts before queryMax?
		if bucketMin < *queryMax {
			return true
		}
		if bucketMin == *queryMax && maxInclusive {
			return true
		}
		return false
	}

	// No upper bound on query - extends to +∞
	if queryMax == nil {
		// Check if bucket ends after queryMin
		if isLastBucket {
			// Last bucket includes bucketMax
			if bucketMax > *queryMin {
				return true
			}
			if bucketMax == *queryMin && minInclusive {
				return true
			}
		} else {
			// Regular bucket excludes bucketMax
			if bucketMax > *queryMin {
				return true
			}
		}
		return false
	}

	// Both bounds present: [queryMin, queryMax]
	// Check if ranges overlap

	// Bucket ends before query starts?
	if isLastBucket {
		if bucketMax < *queryMin {
			return false
		}
		if bucketMax == *queryMin && !minInclusive {
			return false
		}
	} else {
		if bucketMax <= *queryMin {
			return false
		}
	}

	// Bucket starts after query ends?
	if bucketMin > *queryMax {
		return false
	}
	if bucketMin == *queryMax && !maxInclusive {
		return false
	}

	return true
}
