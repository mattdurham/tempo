// Package blockio provides blockpack I/O operations and format handling.
package blockio

import (
	"github.com/mattdurham/blockpack/internal/blockio/shared"
	types "github.com/mattdurham/blockpack/internal/types"
)

// RangeBucketMetadata is an alias to shared.RangeBucketMetadata for backward compatibility
type RangeBucketMetadata = shared.RangeBucketMetadata

// Aliases for provider wrapper types moved to shared/

// ByteCache is an alias for shared.ByteCache.
type ByteCache = shared.ByteCache

// LRUByteCache is an alias for shared.LRUByteCache.
type LRUByteCache = shared.LRUByteCache

// ShardedLRUByteCache is an alias for shared.ShardedLRUByteCache.
type ShardedLRUByteCache = shared.ShardedLRUByteCache

// CacheStats is an alias for shared.CacheStats.
type CacheStats = shared.CacheStats

// MemcacheClient is an alias for shared.MemcacheClient.
type MemcacheClient = shared.MemcacheClient

// TrackingReaderProvider is an alias for shared.TrackingReaderProvider.
type TrackingReaderProvider = shared.TrackingReaderProvider

// DefaultProvider is an alias for shared.DefaultProvider.
type DefaultProvider = shared.DefaultProvider

// DetailedTrackingReader is an alias for shared.DetailedTrackingReader.
type DetailedTrackingReader = shared.DetailedTrackingReader

// IOStats is an alias for shared.IOStats.
type IOStats = shared.IOStats

// CachingReaderProvider is an alias for shared.CachingReaderProvider.
type CachingReaderProvider = shared.CachingReaderProvider

// CachingProviderConfig is an alias for shared.CachingProviderConfig.
type CachingProviderConfig = shared.CachingProviderConfig

// MemcacheProviderConfig is an alias for shared.MemcacheProviderConfig.
type MemcacheProviderConfig = shared.MemcacheProviderConfig

// Re-export provider constructor functions
var (
	NewTrackingReaderProvider            = shared.NewTrackingReaderProvider
	NewTrackingReaderProviderWithLatency = shared.NewTrackingReaderProviderWithLatency
	NewDefaultProvider                   = shared.NewDefaultProvider
	NewDefaultProviderWithLatency        = shared.NewDefaultProviderWithLatency
	NewDetailedTrackingReader            = shared.NewDetailedTrackingReader
	NewDetailedTrackingReaderWithLatency = shared.NewDetailedTrackingReaderWithLatency
	NewCachingReaderProvider             = shared.NewCachingReaderProvider
	NewDataAwareCachingProvider          = shared.NewDataAwareCachingProvider
	NewMemcacheReaderProvider            = shared.NewMemcacheReaderProvider
	NewLRUByteCache                      = shared.NewLRUByteCache
	NewShardedLRUByteCache               = shared.NewShardedLRUByteCache
	NewInMemoryMemcacheClient            = shared.NewInMemoryMemcacheClient
)

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

// ColumnType constants.
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

// IsRangeColumnType reports whether the column type is range-bucketed.
func IsRangeColumnType(typ ColumnType) bool {
	return types.IsRangeColumnType(typ)
}
