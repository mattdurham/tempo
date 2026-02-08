package encodings

import (
	"bytes"
	"encoding/binary"

	"github.com/klauspost/compress/zstd"
)

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
	_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(dictVals)))
	for _, val := range dictVals {
		_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(val)))
		// BOT: Can we use unsafe here to avoid the allocation?
		if _, err := dictBuf.Write([]byte(val)); err != nil {
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
		indexesToEncode = make([]uint32, 0, presentCount)
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				indexesToEncode = append(indexesToEncode, indexes[i])
			}
		}
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
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict)))
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(indexesToEncode)))
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(rleIndexes)))
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount))
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
	_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(dictVals)))
	for _, v := range dictVals {
		_ = binary.Write(&dictBuf, binary.LittleEndian, v)
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
		indexesToEncode = make([]uint32, 0, presentCount)
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				indexesToEncode = append(indexesToEncode, indexes[i])
			}
		}
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
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict)))
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(indexesToEncode)))
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(rleIndexes)))
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount))
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
	_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(dictVals)))
	for _, v := range dictVals {
		_ = binary.Write(&dictBuf, binary.LittleEndian, v)
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
		indexesToEncode = make([]uint32, 0, presentCount)
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				indexesToEncode = append(indexesToEncode, indexes[i])
			}
		}
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
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict)))
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(indexesToEncode)))
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(rleIndexes)))
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount))
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
	_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(dictVals)))
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
		indexesToEncode = make([]uint32, 0, presentCount)
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				indexesToEncode = append(indexesToEncode, indexes[i])
			}
		}
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
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict)))
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(indexesToEncode)))
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(rleIndexes)))
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount))
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
	_ = binary.Write(&dictBuf, binary.LittleEndian, uint32(len(dictVals)))
	for _, v := range dictVals {
		_ = binary.Write(&dictBuf, binary.LittleEndian, v)
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
		indexesToEncode = make([]uint32, 0, presentCount)
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				indexesToEncode = append(indexesToEncode, indexes[i])
			}
		}
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
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict)))
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	if useRLE {
		// Write RLE-encoded indices
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(indexesToEncode)))
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(rleIndexes)))
		_, _ = buf.Write(rleIndexes)
	} else if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount))
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
