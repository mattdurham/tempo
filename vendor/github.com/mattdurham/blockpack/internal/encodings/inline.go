package encodings

import (
	"bytes"
	"encoding/binary"

	"github.com/klauspost/compress/zstd"
)

// BuildBytesInline encodes a bytes column using inline encoding.
// Inline encoding stores each value directly with its length prefix.
// This is efficient for small or highly unique values where dictionary overhead isn't worth it.
func BuildBytesInline(
	buf *bytes.Buffer,
	spanCount, presentCount int,
	useSparse bool,
	presenceRLE []byte,
	dictVals [][]byte,
	indexes []uint32,
	present []byte,
) error {
	encodingKind := encodingKindInlineBytes
	if useSparse {
		encodingKind = encodingKindSparseInlineBytes
	}

	_ = buf.WriteByte(encodingKind)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))        //nolint:gosec
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount)) //nolint:gosec
		for i := 0; i < spanCount; i++ {
			if !isBitSet(present, i) {
				continue
			}
			val := dictVals[indexes[i]]
			_ = binary.Write(buf, binary.LittleEndian, uint32(len(val))) //nolint:gosec
			if len(val) > 0 {
				_, _ = buf.Write(val)
			}
		}
	} else {
		for i := 0; i < spanCount; i++ {
			if !isBitSet(present, i) {
				_ = binary.Write(buf, binary.LittleEndian, uint32(0))
				continue
			}
			val := dictVals[indexes[i]]
			_ = binary.Write(buf, binary.LittleEndian, uint32(len(val))) //nolint:gosec
			if len(val) > 0 {
				_, _ = buf.Write(val)
			}
		}
	}

	return nil
}

// BuildBytesDictionary encodes a bytes column using dictionary encoding.
// Dictionary encoding stores unique values in a dictionary and uses indexes to reference them.
// This is efficient for values with moderate cardinality.
func BuildBytesDictionary(
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
	encodingKind := encodingKindDictionary
	if useSparse {
		encodingKind = encodingKindSparseDictionary
	}

	_ = buf.WriteByte(encodingKind)
	_ = buf.WriteByte(width)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedDict))) //nolint:gosec
	_, _ = buf.Write(compressedDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))        //nolint:gosec
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE))) //nolint:gosec
	_, _ = buf.Write(presenceRLE)

	if useSparse {
		_ = binary.Write(buf, binary.LittleEndian, uint32(presentCount)) //nolint:gosec
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
