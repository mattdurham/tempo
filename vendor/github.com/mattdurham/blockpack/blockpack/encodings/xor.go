package encodings

import (
	"bytes"
	"encoding/binary"
)

// BuildXORBytes encodes a bytes column using XOR encoding.
// XOR encoding stores the first value and then XORs subsequent values with previous ones.
// This is efficient for ID columns where values have common patterns.
func BuildXORBytes(
	buf *bytes.Buffer,
	spanCount, _ int,
	useSparse bool,
	presenceRLE []byte,
	bytesValues [][]byte,
	present []byte,
) error {
	// XOR encoding for ID columns
	encodingKind := encodingKindXORBytes
	if useSparse {
		encodingKind = encodingKindSparseXORBytes
	}

	_ = buf.WriteByte(encodingKind)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	// Encode XOR deltas
	var xorBuf bytes.Buffer
	var prevValue []byte

	for i := 0; i < spanCount; i++ {
		if !isBitSet(present, i) {
			continue
		}

		value := bytesValues[i]
		if prevValue == nil {
			// First value - store as-is
			_ = binary.Write(&xorBuf, binary.LittleEndian, uint32(len(value)))
			_, _ = xorBuf.Write(value)
			prevValue = value
		} else {
			// XOR with previous value
			xorResult := make([]byte, len(value))
			for j := 0; j < len(value); j++ {
				if j < len(prevValue) {
					xorResult[j] = value[j] ^ prevValue[j]
				} else {
					xorResult[j] = value[j]
				}
			}
			_ = binary.Write(&xorBuf, binary.LittleEndian, uint32(len(xorResult)))
			_, _ = xorBuf.Write(xorResult)
			prevValue = value
		}
	}

	// Compress XOR-encoded data
	compressedXOR := CompressZstd(xorBuf.Bytes())
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedXOR)))
	_, _ = buf.Write(compressedXOR)

	return nil
}
