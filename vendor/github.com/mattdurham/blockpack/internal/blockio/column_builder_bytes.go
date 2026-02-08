package blockio

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/mattdurham/blockpack/internal/encodings"
)

const (
	// columnNameTraceID is the trace ID column that benefits from delta-dictionary encoding
	// due to sorted spans with repeated trace IDs (multiple spans per trace).
	columnNameTraceID = "trace:id"
)

// setBytes sets a bytes value at the given index in the column builder.
func (cb *columnBuilder) setBytes(idx int, value []byte) error {
	if cb.typ != ColumnTypeBytes {
		return fmt.Errorf("column %s expects %v got bytes", cb.name, cb.typ)
	}
	cb.ensureIndex(idx)
	if value == nil {
		return nil
	}

	if cb.useXOR || cb.usePrefix {
		// Store raw value for XOR or prefix encoding
		cp := make([]byte, len(value))
		copy(cp, value)
		cb.bytesValues[idx] = cp
	} else {
		// Use dictionary encoding
		cp := make([]byte, len(value))
		copy(cp, value)
		key := string(cp)
		dictIdx, ok := cb.bytesDict[key]
		if !ok {
			dictIdx = uint32(len(cb.bytesDictVals))
			cb.bytesDict[key] = dictIdx
			cb.bytesDictVals = append(cb.bytesDictVals, cp)
		}
		cb.bytesIndexes[idx] = dictIdx
	}
	setBit(cb.present, idx)
	// Update stats
	cb.stats.recordBytes(cb.name, value)
	return nil
}

// buildBytesData builds the encoded data for a bytes column.
// The encoder parameter is used for dictionary compression. Using a per-Writer encoder
// instead of sync.Pool prevents memory accumulation - pools retain all encoders until GC runs.
func (cb *columnBuilder) buildBytesData(buf *bytes.Buffer, spanCount, presentCount int, useSparse bool, presenceRLE []byte, encoder *zstd.Encoder) error {
	if cb.useXOR {
		return encodings.BuildXORBytes(encoder, buf, spanCount, presentCount, useSparse, presenceRLE, cb.bytesValues, cb.present)
	}
	if cb.usePrefix {
		return encodings.BuildPrefixBytes(encoder, buf, spanCount, presentCount, useSparse, presenceRLE, cb.bytesValues, cb.present)
	}

	// Dictionary or inline encoding
	width := encodings.ChooseIndexWidth(len(cb.bytesDictVals))
	var dictBuf bytes.Buffer
	_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(cb.bytesDictVals)))
	for _, val := range cb.bytesDictVals {
		_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(val)))
		if len(val) > 0 {
			_, _ = dictBuf.Write(val)
		}
	}
	// Compress dictionary with zstd using per-Writer encoder (not pool).
	// This prevents memory accumulation - sync.Pool retains all encoders until GC runs,
	// causing OOM with large conversions (128 MB per encoder × hundreds of blocks).
	encoder.Reset(nil)
	compressedDict := encoder.EncodeAll(dictBuf.Bytes(), make([]byte, 0, dictBuf.Len()))

	// Use delta-dictionary encoding for trace:id (optimized for sorted, repeated values)
	// This provides 11-16% compression improvement compared to standard dictionary encoding
	// (validated with real-world data: 191K traces, 670K spans)
	if cb.name == columnNameTraceID {
		return encodings.BuildBytesDictionaryDelta(encoder, buf, spanCount, presentCount, useSparse, presenceRLE, width, compressedDict, cb.bytesIndexes, cb.present)
	}

	encodingKind, err := encodings.ChooseBytesEncodingKind(spanCount, presentCount, useSparse, presenceRLE, width, compressedDict, cb.bytesDictVals, cb.bytesIndexes, cb.present)
	if err != nil {
		return err
	}

	if encodingKind == encodingKindInlineBytes || encodingKind == encodingKindSparseInlineBytes {
		return encodings.BuildBytesInline(buf, spanCount, presentCount, useSparse, presenceRLE, cb.bytesDictVals, cb.bytesIndexes, cb.present)
	}
	return encodings.BuildBytesDictionary(encoder, buf, spanCount, presentCount, useSparse, presenceRLE, width, compressedDict, cb.bytesIndexes, cb.present)
}
