package blockio

import (
	"bytes"
	"fmt"
	"github.com/klauspost/compress/zstd"

	"github.com/mattdurham/blockpack/internal/encodings"
)

// setBool sets a bool value at the given index in the column builder.
func (cb *columnBuilder) setBool(idx int, value bool) error {
	if cb.typ != ColumnTypeBool {
		return fmt.Errorf("column %s expects %v got bool", cb.name, cb.typ)
	}
	cb.ensureIndex(idx)
	var v uint8
	if value {
		v = 1
	}
	dictIdx, ok := cb.boolDict[v]
	if !ok {
		dictIdx = uint32(len(cb.boolDictVals))
		cb.boolDict[v] = dictIdx
		cb.boolDictVals = append(cb.boolDictVals, v)
	}
	cb.boolIndexes[idx] = dictIdx
	setBit(cb.present, idx)
	// Update stats
	cb.stats.recordBool(value)
	return nil
}

// buildBoolData builds the encoded data for a bool column.
func (cb *columnBuilder) buildBoolData(buf *bytes.Buffer, spanCount, presentCount int, useSparse bool, presenceRLE []byte, encoder *zstd.Encoder) error {
	return encodings.BuildBoolDictionary(
		encoder,
		buf,
		spanCount, presentCount,
		useSparse,
		presenceRLE,
		cb.boolDictVals,
		cb.boolIndexes,
		cb.present,
	)
}
