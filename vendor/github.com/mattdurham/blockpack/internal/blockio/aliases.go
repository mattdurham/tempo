package blockio

import (
	"github.com/mattdurham/blockpack/internal/arena"
	types "github.com/mattdurham/blockpack/internal/types"
)

// BOT: What does this do? Why do we need it? Why not just import the types package directly where needed?

type Column = types.Column
type ColumnStats = types.ColumnStats
type ColumnStatsWithType = types.ColumnStatsWithType
type ColumnType = types.ColumnType
type DedicatedValueKey = types.DedicatedValueKey
type ColumnNameBloom = types.ColumnNameBloom
type ArrayValue = types.ArrayValue
type ArrayValueType = types.ArrayValueType
type MinHashSignature = types.MinHashSignature
type MinHashCache = types.MinHashCache
type MinHashCacheStats = types.MinHashCacheStats

var OTELSemanticFields = types.OTELSemanticFields

const (
	ColumnTypeString  = types.ColumnTypeString
	ColumnTypeInt64   = types.ColumnTypeInt64
	ColumnTypeUint64  = types.ColumnTypeUint64
	ColumnTypeBool    = types.ColumnTypeBool
	ColumnTypeFloat64 = types.ColumnTypeFloat64
	ColumnTypeBytes   = types.ColumnTypeBytes

	// Range-bucketed types for high-cardinality numeric columns
	ColumnTypeRangeInt64    = types.ColumnTypeRangeInt64
	ColumnTypeRangeUint64   = types.ColumnTypeRangeUint64
	ColumnTypeRangeDuration = types.ColumnTypeRangeDuration

	ArrayTypeString   = types.ArrayTypeString
	ArrayTypeInt64    = types.ArrayTypeInt64
	ArrayTypeFloat64  = types.ArrayTypeFloat64
	ArrayTypeBool     = types.ArrayTypeBool
	ArrayTypeBytes    = types.ArrayTypeBytes
	ArrayTypeDuration = types.ArrayTypeDuration
)

// DecodeDedicatedKey parses an encoded key from index storage.
func DecodeDedicatedKey(encoded string) (DedicatedValueKey, error) {
	return types.DecodeDedicatedKey(encoded)
}

// StringValueKey builds a dedicated key for string values.
func StringValueKey(val string) DedicatedValueKey {
	return types.StringValueKey(val)
}

// BytesValueKey builds a dedicated key for byte slice values.
func BytesValueKey(val []byte) DedicatedValueKey {
	return types.BytesValueKey(val)
}

// IntValueKey builds a dedicated key for int64 values.
func IntValueKey(val int64) DedicatedValueKey {
	return types.IntValueKey(val)
}

// UintValueKey builds a dedicated key for uint64 values.
func UintValueKey(val uint64) DedicatedValueKey {
	return types.UintValueKey(val)
}

// FloatValueKey builds a dedicated key for float64 values.
func FloatValueKey(val float64) DedicatedValueKey {
	return types.FloatValueKey(val)
}

// BoolValueKey builds a dedicated key for boolean values.
func BoolValueKey(val bool) DedicatedValueKey {
	return types.BoolValueKey(val)
}

// ComputeMinHash computes a MinHash signature for a set of key:value tokens.
func ComputeMinHash(tokens []string) MinHashSignature {
	return types.ComputeMinHash(tokens)
}

// NewMinHashCache creates a MinHash cache with a maximum entry count.
func NewMinHashCache(maxEntries int) *MinHashCache {
	return types.NewMinHashCache(maxEntries)
}

// CompareMinHashSigs compares two MinHash signatures lexicographically.
func CompareMinHashSigs(a, b MinHashSignature) int {
	return types.CompareMinHashSigs(a, b)
}

// ExtractOTELTokens extracts key:value tokens from span attributes for MinHash.
func ExtractOTELTokens(attributes map[string]string) []string {
	return types.ExtractOTELTokens(attributes)
}

// ExtractOTELTokensWithArena extracts key:value tokens from span attributes for MinHash,
// optionally allocating token strings on the arena.
func ExtractOTELTokensWithArena(attributes map[string]string, a *arena.Arena) []string {
	return types.ExtractOTELTokensWithArena(attributes, a)
}

// IsRangeColumnType reports whether the column type is range-bucketed.
func IsRangeColumnType(typ ColumnType) bool {
	return types.IsRangeColumnType(typ)
}

// RangeBucketValueKey creates a dedicated key for a range bucket ID.
func RangeBucketValueKey(bucketID uint16, rangeType ColumnType) DedicatedValueKey {
	return types.RangeBucketValueKey(bucketID, rangeType)
}

// CalculateBuckets computes bucket boundaries for a range of values.
func CalculateBuckets(minVal, maxVal int64, numBuckets int) []int64 {
	return types.CalculateBuckets(minVal, maxVal, numBuckets)
}

// CalculateBucketsFromValues computes quantile-based bucket boundaries for equal distribution.
func CalculateBucketsFromValues(sortedValues []int64, numBuckets int) []int64 {
	return types.CalculateBucketsFromValues(sortedValues, numBuckets)
}

// GetBucketID returns the bucket ID for a given value.
func GetBucketID(value int64, buckets []int64) uint16 {
	return types.GetBucketID(value, buckets)
}

// GetBucketsForRange returns bucket IDs that intersect with a value range.
func GetBucketsForRange(minValue, maxValue *int64, minInclusive, maxInclusive bool, buckets []int64) []uint16 {
	return types.GetBucketsForRange(minValue, maxValue, minInclusive, maxInclusive, buckets)
}

// RangeInt64ValueKey builds a dedicated key for range-bucketed int64 values.
func RangeInt64ValueKey(val int64, rangeType ColumnType) DedicatedValueKey {
	return types.RangeInt64ValueKey(val, rangeType)
}
