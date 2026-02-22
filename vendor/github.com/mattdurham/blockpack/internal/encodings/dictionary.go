package encodings

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

// unsafeStringToBytes converts a string to []byte without allocation.
// WARNING: This is only safe when the returned slice is used immediately
// and not stored, as the string data is immutable.
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s)) //nolint:gosec // Reviewed and acceptable
}

// writeUint32LE writes a uint32 in little-endian format without allocation.
// Uses a stack-allocated buffer instead of binary.Write which allocates.
func writeUint32LE(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

// writeUint64LE writes a uint64 in little-endian format without allocation.
func writeUint64LE(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

// writeInt64LE writes an int64 in little-endian format without allocation.
func writeInt64LE(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v)) //nolint:gosec
	buf.Write(b[:])
}

// writeFloat64LE writes a float64 in little-endian format without allocation.
func writeFloat64LE(buf *bytes.Buffer, v float64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	buf.Write(b[:])
}

// BuildStringDictionary encodes a string column using dictionary encoding.
// It compresses the dictionary with zstd using the provided encoder.
// The encoder parameter should be a per-Writer encoder, not from a pool.
func BuildStringDictionary(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	dictVals []string,
	indexes []uint32,
	present []byte,
) error {
	width := ChooseIndexWidth(len(dictVals))

	// Build and compress dictionary
	var dictBuf bytes.Buffer
	// Use stack buffer to avoid binary.Write allocation
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(dictVals))) //nolint:gosec
	dictBuf.Write(lenBuf[:])

	for _, val := range dictVals {
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(val))) //nolint:gosec
		dictBuf.Write(lenBuf[:])
		// Use unsafe conversion to avoid allocation when writing string to buffer
		// Safe because we're immediately writing to buffer, not storing the reference
		if _, err := dictBuf.Write(unsafeStringToBytes(val)); err != nil {
			return err
		}
	}
	compressedDict := CompressZstd(dictBuf.Bytes(), encoder)

	// Decide if RLE indices would be smaller
	var indexCount int
	if useSparse {
		indexCount = presentCount
	} else {
		indexCount = spanCount
	}
	fixedWidthSize := indexCount * int(width)

	// Try RLE encoding
	var indexesToEncode []uint32
	if useSparse {
		// Use optimized word-level bitset scanning for sparse data
		indexesToEncode = extractSetIndices(present, indexes, spanCount, presentCount)
	} else {
		indexesToEncode = indexes
	}
	rleIndexes := EncodeIndexRLE(indexesToEncode)
	// Only use RLE if it saves at least 25% of space to account for overhead
	useRLE := len(rleIndexes) < (fixedWidthSize * 3 / 4)

	// Choose encoding kind
	encodingKind := encodingKindDictionary
	if useSparse && useRLE {
		encodingKind = encodingKindSparseRLEIndexes
	} else if useSparse {
		encodingKind = encodingKindSparseDictionary
	} else if useRLE {
		encodingKind = encodingKindRLEIndexes
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	writeUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec
	_, _ = buf.Write(compressedDict)
	writeUint32LE(buf, uint32(spanCount))        //nolint:gosec
	writeUint32LE(buf, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		writeUint32LE(buf, uint32(len(indexesToEncode))) //nolint:gosec
		writeUint32LE(buf, uint32(len(rleIndexes)))      //nolint:gosec
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		writeUint32LE(buf, uint32(presentCount)) //nolint:gosec
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				_ = WriteFixedWidth(buf, indexes[i], width)
			}
		}
	} else {
		for _, val := range indexes {
			_ = WriteFixedWidth(buf, val, width)
		}
	}
	return nil
}

// BuildInt64Dictionary encodes an int64 column using dictionary encoding.
// BOT: Are there commonalities here that would be shared between these two build functions?
//
//nolint:dupl // Similar to BuildStringDictionary but type-specific optimizations differ
func BuildInt64Dictionary(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	dictVals []int64,
	indexes []uint32,
	present []byte,
) error {
	width := ChooseIndexWidth(len(dictVals))

	// Build and compress dictionary
	var dictBuf bytes.Buffer
	writeUint32LE(&dictBuf, uint32(len(dictVals))) //nolint:gosec
	for _, v := range dictVals {
		writeInt64LE(&dictBuf, v)
	}
	compressedDict := CompressZstd(dictBuf.Bytes(), encoder)

	// Decide if RLE indices would be smaller
	var indexCount int
	if useSparse {
		indexCount = presentCount
	} else {
		indexCount = spanCount
	}
	fixedWidthSize := indexCount * int(width)

	// Try RLE encoding
	var indexesToEncode []uint32
	if useSparse {
		// Use optimized word-level bitset scanning for sparse data
		indexesToEncode = extractSetIndices(present, indexes, spanCount, presentCount)
	} else {
		indexesToEncode = indexes
	}
	rleIndexes := EncodeIndexRLE(indexesToEncode)
	// Only use RLE if it saves at least 25% of space to account for overhead
	useRLE := len(rleIndexes) < (fixedWidthSize * 3 / 4)

	// Choose encoding kind
	encodingKind := encodingKindDictionary
	if useSparse && useRLE {
		encodingKind = encodingKindSparseRLEIndexes
	} else if useSparse {
		encodingKind = encodingKindSparseDictionary
	} else if useRLE {
		encodingKind = encodingKindRLEIndexes
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	writeUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec
	_, _ = buf.Write(compressedDict)
	writeUint32LE(buf, uint32(spanCount))        //nolint:gosec
	writeUint32LE(buf, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		writeUint32LE(buf, uint32(len(indexesToEncode))) //nolint:gosec
		writeUint32LE(buf, uint32(len(rleIndexes)))      //nolint:gosec
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		writeUint32LE(buf, uint32(presentCount)) //nolint:gosec
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				_ = WriteFixedWidth(buf, indexes[i], width)
			}
		}
	} else {
		for _, idx := range indexes {
			_ = WriteFixedWidth(buf, idx, width)
		}
	}
	return nil
}

// BuildUint64Dictionary encodes a uint64 column using dictionary encoding.
//
//nolint:dupl // Type-specific dictionary encoding; duplication is intentional for performance
func BuildUint64Dictionary(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	dictVals []uint64,
	indexes []uint32,
	present []byte,
) error {
	width := ChooseIndexWidth(len(dictVals))

	// Build and compress dictionary
	var dictBuf bytes.Buffer
	writeUint32LE(&dictBuf, uint32(len(dictVals))) //nolint:gosec
	for _, v := range dictVals {
		writeUint64LE(&dictBuf, v)
	}
	compressedDict := CompressZstd(dictBuf.Bytes(), encoder)

	// Decide if RLE indices would be smaller
	var indexCount int
	if useSparse {
		indexCount = presentCount
	} else {
		indexCount = spanCount
	}
	fixedWidthSize := indexCount * int(width)

	// Try RLE encoding
	var indexesToEncode []uint32
	if useSparse {
		// Use optimized word-level bitset scanning for sparse data
		indexesToEncode = extractSetIndices(present, indexes, spanCount, presentCount)
	} else {
		indexesToEncode = indexes
	}
	rleIndexes := EncodeIndexRLE(indexesToEncode)
	// Only use RLE if it saves at least 25% of space to account for overhead
	useRLE := len(rleIndexes) < (fixedWidthSize * 3 / 4)

	// Choose encoding kind
	encodingKind := encodingKindDictionary
	if useSparse && useRLE {
		encodingKind = encodingKindSparseRLEIndexes
	} else if useSparse {
		encodingKind = encodingKindSparseDictionary
	} else if useRLE {
		encodingKind = encodingKindRLEIndexes
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	writeUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec
	_, _ = buf.Write(compressedDict)
	writeUint32LE(buf, uint32(spanCount))        //nolint:gosec
	writeUint32LE(buf, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		writeUint32LE(buf, uint32(len(indexesToEncode))) //nolint:gosec
		writeUint32LE(buf, uint32(len(rleIndexes)))      //nolint:gosec
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		writeUint32LE(buf, uint32(presentCount)) //nolint:gosec
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				_ = WriteFixedWidth(buf, indexes[i], width)
			}
		}
	} else {
		for _, idx := range indexes {
			_ = WriteFixedWidth(buf, idx, width)
		}
	}
	return nil
}

// BuildBoolDictionary encodes a bool column using dictionary encoding.
func BuildBoolDictionary(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	dictVals []uint8,
	indexes []uint32,
	present []byte,
) error {
	width := ChooseIndexWidth(len(dictVals))

	// Build and compress dictionary
	var dictBuf bytes.Buffer
	writeUint32LE(&dictBuf, uint32(len(dictVals))) //nolint:gosec
	for _, v := range dictVals {
		_ = dictBuf.WriteByte(v)
	}
	compressedDict := CompressZstd(dictBuf.Bytes(), encoder)

	// Decide if RLE indices would be smaller
	var indexCount int
	if useSparse {
		indexCount = presentCount
	} else {
		indexCount = spanCount
	}
	fixedWidthSize := indexCount * int(width)

	// Try RLE encoding
	var indexesToEncode []uint32
	if useSparse {
		// Use optimized word-level bitset scanning for sparse data
		indexesToEncode = extractSetIndices(present, indexes, spanCount, presentCount)
	} else {
		indexesToEncode = indexes
	}
	rleIndexes := EncodeIndexRLE(indexesToEncode)
	// Only use RLE if it saves at least 25% of space to account for overhead
	useRLE := len(rleIndexes) < (fixedWidthSize * 3 / 4)

	// Choose encoding kind
	encodingKind := encodingKindDictionary
	if useSparse && useRLE {
		encodingKind = encodingKindSparseRLEIndexes
	} else if useSparse {
		encodingKind = encodingKindSparseDictionary
	} else if useRLE {
		encodingKind = encodingKindRLEIndexes
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	writeUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec
	_, _ = buf.Write(compressedDict)
	writeUint32LE(buf, uint32(spanCount))        //nolint:gosec
	writeUint32LE(buf, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		writeUint32LE(buf, uint32(len(indexesToEncode))) //nolint:gosec
		writeUint32LE(buf, uint32(len(rleIndexes)))      //nolint:gosec
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		writeUint32LE(buf, uint32(presentCount)) //nolint:gosec
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				_ = WriteFixedWidth(buf, indexes[i], width)
			}
		}
	} else {
		for _, idx := range indexes {
			_ = WriteFixedWidth(buf, idx, width)
		}
	}
	return nil
}

// BuildFloat64Dictionary encodes a float64 column using dictionary encoding.
//
//nolint:dupl // Type-specific dictionary encoding; duplication is intentional for performance
func BuildFloat64Dictionary(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	dictVals []float64,
	indexes []uint32,
	present []byte,
) error {
	width := ChooseIndexWidth(len(dictVals))

	// Build and compress dictionary
	var dictBuf bytes.Buffer
	writeUint32LE(&dictBuf, uint32(len(dictVals))) //nolint:gosec
	for _, v := range dictVals {
		writeFloat64LE(&dictBuf, v)
	}
	compressedDict := CompressZstd(dictBuf.Bytes(), encoder)

	// Decide if RLE indices would be smaller
	var indexCount int
	if useSparse {
		indexCount = presentCount
	} else {
		indexCount = spanCount
	}
	fixedWidthSize := indexCount * int(width)

	// Try RLE encoding
	var indexesToEncode []uint32
	if useSparse {
		// Use optimized word-level bitset scanning for sparse data
		indexesToEncode = extractSetIndices(present, indexes, spanCount, presentCount)
	} else {
		indexesToEncode = indexes
	}
	rleIndexes := EncodeIndexRLE(indexesToEncode)
	// Only use RLE if it saves at least 25% of space to account for overhead
	useRLE := len(rleIndexes) < (fixedWidthSize * 3 / 4)

	// Choose encoding kind
	encodingKind := encodingKindDictionary
	if useSparse && useRLE {
		encodingKind = encodingKindSparseRLEIndexes
	} else if useSparse {
		encodingKind = encodingKindSparseDictionary
	} else if useRLE {
		encodingKind = encodingKindRLEIndexes
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	writeUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec
	_, _ = buf.Write(compressedDict)
	writeUint32LE(buf, uint32(spanCount))        //nolint:gosec
	writeUint32LE(buf, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		writeUint32LE(buf, uint32(len(indexesToEncode))) //nolint:gosec
		writeUint32LE(buf, uint32(len(rleIndexes)))      //nolint:gosec
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		writeUint32LE(buf, uint32(presentCount)) //nolint:gosec
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				_ = WriteFixedWidth(buf, indexes[i], width)
			}
		}
	} else {
		for _, idx := range indexes {
			_ = WriteFixedWidth(buf, idx, width)
		}
	}
	return nil
}
