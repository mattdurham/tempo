package ondisk

import (
	"bytes"
	"encoding/binary"
)

// ColumnType represents the logical type of a column in the blockpack format.
type ColumnType uint8

const (
	ColumnTypeString ColumnType = iota
	ColumnTypeInt64
	ColumnTypeUint64
	ColumnTypeBool
	ColumnTypeFloat64
	ColumnTypeBytes
	// Range-bucketed types: high-cardinality numeric columns bucketed into ~100 ranges
	ColumnTypeRangeInt64
	ColumnTypeRangeUint64
	ColumnTypeRangeDuration // Special case: int64 representing nanosecond duration
)

const (
	columnEncodingVersion = uint8(2)
	presenceRLEVersion    = uint8(1)
	columnNameBloomBits   = 256                 // 256 bits
	columnNameBloomBytes  = 32                  // 256 bits / 8 = 32 bytes
	ColumnNameBloomBits   = columnNameBloomBits // exported for query-time checks
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
type Column struct {
	Name         string
	Type         ColumnType
	Stats        ColumnStats
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
}

// ColumnStats captures per-column value metadata for block pruning.
type ColumnStats struct {
	HasValues bool

	StringMin string
	StringMax string

	IntMin  int64
	IntMax  int64
	UintMin uint64
	UintMax uint64

	FloatMin float64
	FloatMax float64

	BoolMin bool
	BoolMax bool

	BytesMin []byte
	BytesMax []byte
}

// ColumnStatsWithType bundles column stats with the column type.
type ColumnStatsWithType struct {
	Type  ColumnType
	Stats ColumnStats
}

func (c *Column) isPresent(idx int) bool {
	if idx < 0 {
		return false
	}
	byteIdx := idx / 8
	bit := uint(idx % 8)
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

func isBitSet(bits []byte, idx int) bool {
	if idx < 0 {
		return false
	}
	byteIdx := idx / 8
	if byteIdx >= len(bits) {
		return false
	}
	bit := uint(idx % 8)
	return (bits[byteIdx] & (1 << bit)) != 0
}

// EncodePresenceRLE compresses presence bits into alternating runs of false/true values.
// It is used by column encodings to avoid storing one bit per row when nulls are clustered.
func EncodePresenceRLE(bits []byte, rows int) []byte {
	var buf bytes.Buffer
	runCount := uint32(0)
	last := false
	for i := 0; i < rows; i++ {
		val := isBitSet(bits, i)
		if i == 0 || val != last {
			runCount++
		}
		last = val
	}
	mustWriteByte(&buf, presenceRLEVersion)
	mustWriteBinary(&buf, runCount)

	currentVal := isBitSet(bits, 0)
	currentLen := uint32(0)
	for i := 0; i < rows; i++ {
		val := isBitSet(bits, i)
		if val == currentVal {
			currentLen++
			continue
		}
		mustWriteBinary(&buf, currentLen)
		mustWriteByte(&buf, boolToByte(currentVal))
		currentVal = val
		currentLen = 1
	}
	mustWriteBinary(&buf, currentLen)
	mustWriteByte(&buf, boolToByte(currentVal))
	return buf.Bytes()
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// mustWriteByte writes a byte to the buffer and panics on error.
// Buffer errors only occur on OOM, which is a fatal condition.
func mustWriteByte(buf *bytes.Buffer, b byte) {
	if err := buf.WriteByte(b); err != nil {
		panic(err)
	}
}

// mustWriteBinary writes binary data to the buffer and panics on error.
// Buffer errors only occur on OOM, which is a fatal condition.
func mustWriteBinary(buf *bytes.Buffer, data any) {
	if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
		panic(err)
	}
}

// EncodeIndexRLE compresses uint32 indexes into runs of the same value.
// Used when dictionary indices are larger than the dictionary itself.
// Format: version(1) + runCount(4) + [length(4) + value(4)]...
func EncodeIndexRLE(indexes []uint32) []byte {
	if len(indexes) == 0 {
		var buf bytes.Buffer
		mustWriteByte(&buf, 1) // version
		mustWriteBinary(&buf, uint32(0))
		return buf.Bytes()
	}

	var buf bytes.Buffer
	runCount := uint32(0)
	last := indexes[0]
	for i := 1; i < len(indexes); i++ {
		if indexes[i] != last {
			runCount++
			last = indexes[i]
		}
	}
	runCount++ // count the final run

	mustWriteByte(&buf, 1) // version
	mustWriteBinary(&buf, runCount)

	currentVal := indexes[0]
	currentLen := uint32(1)
	for i := 1; i < len(indexes); i++ {
		if indexes[i] == currentVal {
			currentLen++
			continue
		}
		mustWriteBinary(&buf, currentLen)
		mustWriteBinary(&buf, currentVal)
		currentVal = indexes[i]
		currentLen = 1
	}
	mustWriteBinary(&buf, currentLen)
	mustWriteBinary(&buf, currentVal)
	return buf.Bytes()
}
