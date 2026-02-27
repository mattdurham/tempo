package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// ColumnType is the logical column type (0–11).
type ColumnType uint8

// ColumnType constants.
const (
	ColumnTypeString        ColumnType = 0
	ColumnTypeInt64         ColumnType = 1
	ColumnTypeUint64        ColumnType = 2
	ColumnTypeFloat64       ColumnType = 3
	ColumnTypeBool          ColumnType = 4
	ColumnTypeBytes         ColumnType = 5
	ColumnTypeRangeInt64    ColumnType = 6
	ColumnTypeRangeUint64   ColumnType = 7
	ColumnTypeRangeDuration ColumnType = 8
	ColumnTypeRangeFloat64  ColumnType = 9
	ColumnTypeRangeBytes    ColumnType = 10
	ColumnTypeRangeString   ColumnType = 11
)

// BlockKind is the kind byte in a v11 block index entry.
type BlockKind uint8

// BlockKindLeaf is the only defined block kind; leaf blocks contain span data.
const BlockKindLeaf BlockKind = 0

// BlockMeta holds the parsed block index entry.
type BlockMeta struct {
	ValueStats      []AttributeStatEntry
	Offset          uint64
	Length          uint64
	MinStart        uint64
	MaxStart        uint64
	SpanCount       uint32
	ColumnNameBloom [32]byte
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
	Kind            BlockKind
}

// AttributeStatEntry is one per-attribute stat in a block index entry.
type AttributeStatEntry struct {
	Name       string
	StringMin  string
	StringMax  string
	Int64Min   int64
	Int64Max   int64
	Float64Min float64
	Float64Max float64
	StatsType  uint8 // 0=None 1=String 2=Int64 3=Float64 4=Bool
	BoolMin    bool
	BoolMax    bool
}

// ColumnStats captures min/max for block pruning.
type ColumnStats struct {
	StringMin string
	StringMax string
	BytesMin  []byte
	BytesMax  []byte
	IntMin    int64
	IntMax    int64
	UintMin   uint64
	UintMax   uint64
	FloatMin  float64
	FloatMax  float64
	BoolMin   bool
	BoolMax   bool
	HasValues bool
}

// AttrValue is a typed attribute value.
type AttrValue struct {
	Str   string
	Bytes []byte
	Int   int64
	Uint  uint64
	Float float64
	Bool  bool
	Type  ColumnType
}

// AttrKV is a key-value pair for span, resource, or scope attributes.
// Using a slice of AttrKV instead of map[string]AttrValue eliminates per-span map
// allocations (map headers + hash buckets), reducing GC scan work significantly.
type AttrKV struct {
	Key string
	Val AttrValue
}

// RangeValueKey is a string key used in the range index.
type RangeValueKey = string
