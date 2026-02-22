package writer

import (
	"bytes"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"

	"github.com/mattdurham/blockpack/internal/encodings"
)

// setFloat64 sets a float64 value at the given index in the column builder.
func (cb *columnBuilder) setFloat64(idx int, value float64) error {
	if cb.typ != ColumnTypeFloat64 {
		return fmt.Errorf("column %s expects %v got float64", cb.name, cb.typ)
	}
	cb.ensureIndex(idx)
	bits := math.Float64bits(value)
	dictIdx, ok := cb.floatDict[bits]
	if !ok {
		dictIdx = uint32(len(cb.floatDictVals)) //nolint:gosec
		cb.floatDict[bits] = dictIdx
		cb.floatDictVals = append(cb.floatDictVals, value)
	}
	cb.floatIndexes[idx] = dictIdx
	setBit(cb.present, idx)
	// Update stats
	cb.stats.recordFloat(value)
	return nil
}

// buildFloat64Data builds the encoded data for a float64 column.
func (cb *columnBuilder) buildFloat64Data(
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	encoder *zstd.Encoder,
) error {
	return encodings.BuildFloat64Dictionary(
		encoder,
		buf,
		spanCount, presentCount,
		useSparse,
		presenceRLE,
		cb.floatDictVals,
		cb.floatIndexes,
		cb.present,
	)
}
