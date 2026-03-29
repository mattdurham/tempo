package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// NewDictAccumForTest creates an empty dictAccum for white-box testing.
func NewDictAccumForTest(colType shared.ColumnType) *dictAccum {
	return &dictAccum{
		index:   make(map[string]int),
		colType: colType,
	}
}

// EncodeDictColumnForTest exposes encodePagedDictColumn for white-box testing.
// (encodeDictColumn v1 monolithic format was removed; encodePagedDictColumn is always used.)
var EncodeDictColumnForTest = encodePagedDictColumn

// NewIntrinsicAccumulatorForTest creates an empty intrinsicAccumulator for testing.
func NewIntrinsicAccumulatorForTest() *intrinsicAccumulator {
	return newIntrinsicAccumulator()
}

// OverCapForTest exposes overCap for testing.
func (a *intrinsicAccumulator) OverCapForTest() bool {
	return a.overCap()
}

// FeedUint64ForTest exposes feedUint64 for testing.
func (a *intrinsicAccumulator) FeedUint64ForTest(
	name string,
	colType shared.ColumnType,
	val uint64,
	blockIdx uint16,
	rowIdx int,
) {
	a.feedUint64(name, colType, val, blockIdx, rowIdx)
}

// ComputeMinMaxForTest exposes computeMinMax for testing.
func (a *intrinsicAccumulator) ComputeMinMaxForTest(name string) (minVal, maxVal string) {
	return a.computeMinMax(name)
}

// NewFlatAccumForTest creates an empty flatAccum for white-box testing.
func NewFlatAccumForTest(colType shared.ColumnType) *flatAccum {
	return &flatAccum{colType: colType}
}

// EncodePagedFlatColumnForTest exposes encodePagedFlatColumn for white-box testing.
var EncodePagedFlatColumnForTest = encodePagedFlatColumn
