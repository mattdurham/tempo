package ondisk

// ColumnType represents the logical type of a column in the blockpack format.
type ColumnType uint8

// ColumnType constants for column types.
const (
	// ColumnTypeString is the string column type.
	ColumnTypeString ColumnType = iota
	// ColumnTypeInt64 is the int64 column type.
	ColumnTypeInt64
	// ColumnTypeUint64 is the uint64 column type.
	ColumnTypeUint64
	// ColumnTypeBool is the boolean column type.
	ColumnTypeBool
	// ColumnTypeFloat64 is the float64 column type.
	ColumnTypeFloat64
	// ColumnTypeBytes is the bytes column type.
	ColumnTypeBytes
	// Range-bucketed types: high-cardinality numeric columns bucketed into ~100 ranges
	// ColumnTypeRangeInt64 is the range-bucketed int64 column type.
	ColumnTypeRangeInt64
	// ColumnTypeRangeUint64 is the range-bucketed uint64 column type.
	ColumnTypeRangeUint64
	// ColumnTypeRangeDuration is the range-bucketed duration column type (special case: int64 representing nanosecond duration).
	ColumnTypeRangeDuration
	// ColumnTypeRangeFloat64 is the range-bucketed float64 column type.
	ColumnTypeRangeFloat64
	// ColumnTypeRangeBytes is the range-bucketed bytes column type (16-byte lexicographic).
	ColumnTypeRangeBytes
	// ColumnTypeRangeString is the range-bucketed string column type (32-byte lexicographic).
	ColumnTypeRangeString
)

const (
	columnEncodingVersion = uint8(2)
	presenceRLEVersion    = uint8(1)
	columnNameBloomBits   = 256 // 256 bits
	columnNameBloomBytes  = 32  // 256 bits / 8 = 32 bytes
	// ColumnNameBloomBits is exported for query-time checks.
	ColumnNameBloomBits = columnNameBloomBits
)

// DedicatedColumnIndex stores value -> blocks mapping for dedicated columns.
type DedicatedColumnIndex map[string][]int

// DedicatedColumnsIndexFile groups dedicated indexes per column name.
type DedicatedColumnsIndexFile map[string]DedicatedColumnIndex

const (
	nullIndex     uint32 = ^uint32(0)
	nullStringIdx        = nullIndex
	nullBytesIdx         = nullIndex
)

// Column holds decoded blockpack data with presence tracking.
//
//nolint:govet // Field order optimized for readability over memory alignment
type Column struct {
	Name         string
	stringDict   []string
	stringValues []uint32
	intDict      []int64
	intIndexes   []uint32
	uintDict     []uint64
	uintIndexes  []uint32
	boolDict     []uint8
	boolIndexes  []uint32
	floatDict    []float64
	floatIndexes []uint32
	bytesDict    [][]byte
	bytesIndexes []uint32
	bytesInline  [][]byte
	present      []byte // bitset of present values
	Stats        ColumnStats
	Type         ColumnType
}

// ColumnStats captures per-column value metadata for block pruning.
type ColumnStats struct {
	StringMin string
	StringMax string

	BytesMin []byte
	BytesMax []byte

	IntMin  int64
	IntMax  int64
	UintMin uint64
	UintMax uint64

	FloatMin float64
	FloatMax float64

	HasValues bool

	BoolMin bool
	BoolMax bool
}

// ColumnStatsWithType bundles column stats with the column type.
type ColumnStatsWithType struct {
	Stats ColumnStats
	Type  ColumnType
}

func (c *Column) isPresent(idx int) bool {
	if idx < 0 {
		return false
	}
	byteIdx := idx / 8
	bit := uint(idx % 8) //nolint:gosec
	if byteIdx >= len(c.present) {
		return false
	}
	return (c.present[byteIdx] & (1 << bit)) != 0
}

// IsPresent checks if a value is present at the given row index.
// Exported version of isPresent for use by query executor.
func (c *Column) IsPresent(idx int) bool {
	return c.isPresent(idx)
}

// StringValues returns the string dictionary values.
