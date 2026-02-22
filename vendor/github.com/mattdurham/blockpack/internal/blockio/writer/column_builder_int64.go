//nolint:dupl // Type-specific column builders are intentionally similar
package writer

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/mattdurham/blockpack/internal/encodings"
)

// setInt64 sets an int64 value at the given index in the column builder.
func (cb *columnBuilder) setInt64(idx int, value int64) error {
	if cb.typ != ColumnTypeInt64 {
		return fmt.Errorf("column %s expects %v got int64", cb.name, cb.typ)
	}
	cb.ensureIndex(idx)
	dictIdx, ok := cb.intDict[value]
	if !ok {
		dictIdx = uint32(len(cb.intDictVals)) //nolint:gosec
		cb.intDict[value] = dictIdx
		cb.intDictVals = append(cb.intDictVals, value)
	}
	cb.intIndexes[idx] = dictIdx
	setBit(cb.present, idx)
	// Update stats
	cb.stats.recordInt(value)
	return nil
}

// buildInt64Data builds the encoded data for an int64 column.
func (cb *columnBuilder) buildInt64Data(
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	encoder *zstd.Encoder,
) error {
	return encodings.BuildInt64Dictionary(
		encoder,
		buf,
		spanCount, presentCount,
		useSparse,
		presenceRLE,
		cb.intDictVals,
		cb.intIndexes,
		cb.present,
	)
}
