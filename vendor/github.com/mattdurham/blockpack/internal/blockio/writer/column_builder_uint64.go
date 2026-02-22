package writer

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/mattdurham/blockpack/internal/encodings"
)

// setUint64 sets a uint64 value at the given index in the column builder.
func (cb *columnBuilder) setUint64(idx int, value uint64) error {
	if cb.typ != ColumnTypeUint64 {
		return fmt.Errorf("column %s expects %v got uint64", cb.name, cb.typ)
	}
	cb.ensureIndex(idx)
	// Always store raw value - we'll decide encoding at build time
	cb.uintValues[idx] = value
	setBit(cb.present, idx)
	// Update stats
	cb.stats.recordUint(value)
	return nil
}

// buildUint64Data builds the encoded data for a uint64 column.
// It analyzes the data to decide between delta encoding and dictionary encoding.
func (cb *columnBuilder) buildUint64Data(
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	encoder *zstd.Encoder,
) error {
	// Analyze whether delta encoding would be beneficial using pre-computed min/max from stats
	if shouldUseDeltaEncoding(cb.uintValues, cb.present, spanCount, cb.stats.uintMin, cb.stats.uintMax) {
		return encodings.BuildDeltaUint64(
			encoder,
			buf,
			spanCount,
			presenceRLE,
			cb.uintValues,
			cb.present,
		)
	}

	// Use dictionary encoding - build dictionary from raw values
	uintDict := make(map[uint64]uint32)
	uintDictVals := make([]uint64, 0, 16)
	uintIndexes := make([]uint32, spanCount)

	for i := range spanCount {
		if isBitSet(cb.present, i) {
			value := cb.uintValues[i]
			dictIdx, ok := uintDict[value]
			if !ok {
				dictIdx = uint32(len(uintDictVals)) //nolint:gosec
				uintDict[value] = dictIdx
				uintDictVals = append(uintDictVals, value)
			}
			uintIndexes[i] = dictIdx
		}
	}

	return encodings.BuildUint64Dictionary(
		encoder,
		buf,
		spanCount, presentCount,
		useSparse,
		presenceRLE,
		uintDictVals,
		uintIndexes,
		cb.present,
	)
}
