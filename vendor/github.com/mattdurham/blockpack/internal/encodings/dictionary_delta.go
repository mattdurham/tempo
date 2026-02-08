package encodings

import (
	"bytes"
	"encoding/binary"
	"github.com/klauspost/compress/zstd"
)

// BuildBytesDictionaryDelta encodes a bytes column using dictionary encoding with delta-compressed indices.
// This is optimized for columns like trace:id where:
// 1. Many spans share the same value (dictionary reduces 16 bytes to index)
// 2. Data is sorted by value (delta encoding compresses runs of same index to 0)
//
// Format:
// - encodingKind (1 byte)
// - dictWidth (1 byte) - width of dictionary indices
// - compressedDictLen (4 bytes) + compressedDict - zstd-compressed dictionary
// - spanCount (4 bytes)
// - presenceRLELen (4 bytes) + presenceRLE - presence bitmap
// - deltaIndicesLen (4 bytes) + deltaIndices - zstd-compressed delta-encoded indices
func BuildBytesDictionaryDelta(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	width uint8,
	compressedDict []byte,
	indexes []uint32,
	present []byte,
) error {
	encodingKind := encodingKindDeltaDictionary
	if useSparse {
		encodingKind = encodingKindSparseDeltaDictionary
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict)))
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	// Delta-encode the indices
	var deltaIndexes []int32
	if useSparse {
		// Sparse: only encode present values
		prevIdx := int32(0)
		for i := 0; i < spanCount; i++ {
			if isBitSet(present, i) {
				currIdx := int32(indexes[i])
				delta := currIdx - prevIdx
				deltaIndexes = append(deltaIndexes, delta)
				prevIdx = currIdx
			}
		}
	} else {
		// Dense: encode all values (nulls have index 0)
		prevIdx := int32(0)
		for i := 0; i < spanCount; i++ {
			currIdx := int32(indexes[i])
			delta := currIdx - prevIdx
			deltaIndexes = append(deltaIndexes, delta)
			prevIdx = currIdx
		}
	}

	// Serialize delta indices
	var deltaBuf bytes.Buffer
	for _, delta := range deltaIndexes {
		_ = binary.Write(&deltaBuf, binary.LittleEndian, delta)
	}

	// Compress with zstd (deltas compress very well - many zeros)
	compressedDeltas := CompressZstd(deltaBuf.Bytes(), encoder)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDeltas)))
	_, _ = buf.Write(compressedDeltas)

	return nil
}
