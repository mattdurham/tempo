package blockio

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/mattdurham/blockpack/internal/encodings"
)

// setString sets a string value at the given index in the column builder.
func (cb *columnBuilder) setString(idx int, value string) error {
	if cb.typ != ColumnTypeString {
		return fmt.Errorf("column %s expects %v got string", cb.name, cb.typ)
	}
	cb.ensureIndex(idx)
	dictIdx, ok := cb.stringDict[value]
	if !ok {
		dictIdx = uint32(len(cb.stringDictVals))
		cb.stringDict[value] = dictIdx
		cb.stringDictVals = append(cb.stringDictVals, value)
	}
	cb.stringIndexes[idx] = dictIdx
	setBit(cb.present, idx)
	// Update stats
	cb.stats.recordString(value)
	return nil
}

// buildStringData builds the encoded data for a string column.
// The encoder is passed through to BuildStringDictionary for dictionary compression.
func (cb *columnBuilder) buildStringData(buf *bytes.Buffer, spanCount, presentCount int, useSparse bool, presenceRLE []byte, encoder *zstd.Encoder) error {
	return encodings.BuildStringDictionary(
		encoder,
		buf,
		spanCount, presentCount,
		useSparse,
		presenceRLE,
		cb.stringDictVals,
		cb.stringIndexes,
		cb.present,
	)
}
