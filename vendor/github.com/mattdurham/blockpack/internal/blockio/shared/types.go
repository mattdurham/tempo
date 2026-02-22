package shared

import types "github.com/mattdurham/blockpack/internal/types"

// ColumnType is re-exported from internal/types for convenience
type ColumnType = types.ColumnType

// ColumnNameBloom is re-exported from internal/types for convenience
type ColumnNameBloom = types.ColumnNameBloom

// StatsType identifies the type of statistics stored for an attribute
type StatsType uint8

const (
	// StatsTypeNone indicates no statistics are stored.
	StatsTypeNone StatsType = iota
	// StatsTypeString indicates string statistics are stored.
	StatsTypeString
	// StatsTypeInt64 indicates int64 statistics are stored.
	StatsTypeInt64
	// StatsTypeFloat64 indicates float64 statistics are stored.
	StatsTypeFloat64
	// StatsTypeBool indicates bool statistics are stored.
	StatsTypeBool
)

// AttributeStats holds per-block statistics for a single attribute.
// Used for query optimization by allowing block pruning before reading.
// This type is shared between blockio and reader packages.
type AttributeStats struct {
	// Numeric range bounds (only valid for Int64/Float64 types)
	MinInt   int64
	MaxInt   int64
	MinFloat float64
	MaxFloat float64

	// Approximate count of distinct values in this block
	// Used for selectivity estimation
	ApproxDistinctCount uint16

	// Bloom filter for value membership testing (256 bits = 32 bytes)
	// For strings: hash of the string value
	// For numbers: hash of the numeric value
	// ~1% false positive rate for up to 100 distinct values per block
	ValuesBloom [32]byte

	Type StatsType
}

// RangeBucketMetadata stores bucket information for a range-bucketed column
// This type is shared between blockio (writer) and reader packages.
type RangeBucketMetadata struct {
	Boundaries        []int64   // Length = numBuckets + 1 (int64 bit-cast representations)
	Float64Boundaries []float64 // Actual float64 boundaries (only for ColumnTypeRangeFloat64)
	StringBoundaries  []string  // Actual string boundaries (only for ColumnTypeRangeString)
	BytesBoundaries   [][]byte  // Actual bytes boundaries (only for ColumnTypeRangeBytes)
	Min               int64
	Max               int64
	ColumnType        ColumnType
}

// MemcacheClient defines the interface for a memcache client
// This allows for different implementations (gomemcache, custom, etc.)
// This type is shared between blockio and reader packages.
type MemcacheClient interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte, expiration int32) error
}

// CoalescedRead represents a single IO operation that reads multiple consecutive blocks
// This type is shared between blockio and reader packages.
type CoalescedRead struct {
	StartBlockIdx int    // First block index (inclusive)
	EndBlockIdx   int    // Last block index (exclusive)
	Offset        uint64 // File offset to start reading
	Length        uint64 // Total bytes to read
}

// CoalesceConfig controls how blocks are coalesced into larger reads
// This type is shared between blockio and reader packages.
type CoalesceConfig struct {
	MaxReadSize   uint64  // Maximum size of a single coalesced read (default: 4MB)
	MaxGapSize    uint64  // Maximum gap between blocks to coalesce (default: 1MB, 0 = only adjacent)
	MaxWasteRatio float64 // Maximum ratio of wasted bytes (gap / total read) (default: 0.5 = 50%)
}

// BlockIndexEntry mirrors the on-disk block index entry with exported fields.
// This type is shared between blockio and reader packages.
type BlockIndexEntry struct {
	ValueStats      map[string]AttributeStats // Per-attribute value statistics
	ColumnIndex     []ColumnIndexEntry        // Per-column offsets for selective I/O
	Offset          uint64
	Length          uint64
	MinStart        uint64
	MaxStart        uint64
	SpanCount       uint32
	ColumnNameBloom ColumnNameBloom
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
	Kind            uint8
}

// ColumnIndexEntry represents a column's location within a block
type ColumnIndexEntry struct {
	Name   string // Column name
	Offset uint64
	Length uint64
}

// DataType indicates the type of data being read for cache optimization
type DataType string

const (
	// DataTypeFooter indicates file footer data (last 41 bytes).
	DataTypeFooter DataType = "footer"
	// DataTypeMetadata indicates schema and column metadata.
	DataTypeMetadata DataType = "metadata"
	// DataTypeColumn indicates column data blocks.
	DataTypeColumn DataType = "column"
	// DataTypeRow indicates row data blocks.
	DataTypeRow DataType = "row"
	// DataTypeIndex indicates block index data.
	DataTypeIndex DataType = "index"
	// DataTypeUnknown indicates unknown or generic data.
	DataTypeUnknown DataType = "unknown"
)

// ReaderProvider supplies random access to blockpack data with explicit data type hints.
// The dataType parameter allows providers to optimize caching strategies based on access patterns.
// This interface is shared between blockio and reader packages.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}

// CloseableReaderProvider extends ReaderProvider with resource cleanup.
// Implementations that hold open file descriptors or other resources should implement this.
// This interface is shared between blockio and reader packages.
type CloseableReaderProvider interface {
	ReaderProvider
	Close() error
}
