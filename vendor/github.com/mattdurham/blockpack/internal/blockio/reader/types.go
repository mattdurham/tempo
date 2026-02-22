package reader

import (
	"github.com/mattdurham/blockpack/internal/blockio/shared"
	types "github.com/mattdurham/blockpack/internal/types"
)

// BOT: What does this do? Why do we need it? Why not just import the types package directly where needed?

// ColumnIndexEntry is re-exported from shared for public API convenience
type ColumnIndexEntry = shared.ColumnIndexEntry

// Column is an alias for types.Column.
type Column = types.Column

// ColumnStats is an alias for types.ColumnStats.
type ColumnStats = types.ColumnStats

// ColumnStatsWithType is an alias for types.ColumnStatsWithType.
type ColumnStatsWithType = types.ColumnStatsWithType

// ColumnType is an alias for types.ColumnType.
type ColumnType = types.ColumnType

// DedicatedValueKey is an alias for types.DedicatedValueKey.
type DedicatedValueKey = types.DedicatedValueKey

// ColumnNameBloom is an alias for types.ColumnNameBloom.
type ColumnNameBloom = types.ColumnNameBloom

// ArrayValue is an alias for types.ArrayValue.
type ArrayValue = types.ArrayValue

// ArrayValueType is an alias for types.ArrayValueType.
type ArrayValueType = types.ArrayValueType

// MinHashSignature is an alias for types.MinHashSignature.
type MinHashSignature = types.MinHashSignature

// MinHashCache is an alias for types.MinHashCache.
type MinHashCache = types.MinHashCache

// MinHashCacheStats is an alias for types.MinHashCacheStats.
type MinHashCacheStats = types.MinHashCacheStats

// OTELSemanticFields is the exported semantic fields for OTEL.
var OTELSemanticFields = types.OTELSemanticFields

// ColumnType constants for column types.
const (
	// ColumnTypeString is the string column type.
	ColumnTypeString = types.ColumnTypeString
	// ColumnTypeInt64 is the int64 column type.
	ColumnTypeInt64 = types.ColumnTypeInt64
	// ColumnTypeUint64 is the uint64 column type.
	ColumnTypeUint64 = types.ColumnTypeUint64
	// ColumnTypeBool is the boolean column type.
	ColumnTypeBool = types.ColumnTypeBool
	// ColumnTypeFloat64 is the float64 column type.
	ColumnTypeFloat64 = types.ColumnTypeFloat64
	// ColumnTypeBytes is the bytes column type.
	ColumnTypeBytes = types.ColumnTypeBytes

	// Range-bucketed types for high-cardinality numeric columns
	// ColumnTypeRangeInt64 is the range-bucketed int64 column type.
	ColumnTypeRangeInt64 = types.ColumnTypeRangeInt64
	// ColumnTypeRangeUint64 is the range-bucketed uint64 column type.
	ColumnTypeRangeUint64 = types.ColumnTypeRangeUint64
	// ColumnTypeRangeDuration is the range-bucketed duration column type.
	ColumnTypeRangeDuration = types.ColumnTypeRangeDuration
	ColumnTypeRangeFloat64  = types.ColumnTypeRangeFloat64
	ColumnTypeRangeBytes    = types.ColumnTypeRangeBytes
	ColumnTypeRangeString   = types.ColumnTypeRangeString

	// ArrayType constants for array element types.
	// ArrayTypeString is the array of strings type.
	ArrayTypeString = types.ArrayTypeString
	// ArrayTypeInt64 is the array of int64 type.
	ArrayTypeInt64 = types.ArrayTypeInt64
	// ArrayTypeFloat64 is the array of float64 type.
	ArrayTypeFloat64 = types.ArrayTypeFloat64
	// ArrayTypeBool is the array of bool type.
	ArrayTypeBool = types.ArrayTypeBool
	// ArrayTypeBytes is the array of bytes type.
	ArrayTypeBytes = types.ArrayTypeBytes
	// ArrayTypeDuration is the array of duration type.
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

// IsRangeColumnType reports whether the column type is range-bucketed.
func IsRangeColumnType(typ ColumnType) bool {
	return types.IsRangeColumnType(typ)
}

// RangeBucketValueKey creates a dedicated key for a range bucket ID.
func RangeBucketValueKey(bucketID uint16, rangeType ColumnType) DedicatedValueKey {
	return types.RangeBucketValueKey(bucketID, rangeType)
}
