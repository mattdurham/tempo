package encodings

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/zstd"
)

// BuildDeltaUint64 encodes a uint64 column using block-relative encoding.
// Stores all timestamps as offsets from the block's minimum timestamp.
// This eliminates the "backwards time" problem and keeps all offsets small.
// Format: [encoding_kind: 1 byte] [span_count: 4 bytes] [presence_len: 4 bytes] [presence_rle: N bytes]
//
//	[base_timestamp: 8 bytes] [width: 1 byte] [compressed_offsets: variable]
//
// AGENT: This is an example of a solid great well commented function.
func BuildDeltaUint64(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount int,
	presenceRLE []byte,
	uintValues []uint64,
	present []byte,
) error {
	// Block-relative encoding for timestamps
	// bytes.Buffer.WriteByte never fails - it only panics on out-of-memory
	_ = buf.WriteByte(encodingKindDeltaUint64)

	// Write span count and presence
	// binary.Write to bytes.Buffer never fails - it only panics on out-of-memory
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	// Find min and max values to determine base timestamp and required width
	var minVal, maxVal uint64
	firstValue := true
	for i := 0; i < spanCount; i++ {
		if isBitSet(present, i) {
			value := uintValues[i]
			if firstValue {
				minVal = value
				maxVal = value
				firstValue = false
			} else {
				if value < minVal {
					minVal = value
				}
				if value > maxVal {
					maxVal = value
				}
			}
		}
	}

	// If no values, write empty data
	if firstValue {
		// No values present - write zero base and width
		_ = binary.Write(buf, binary.LittleEndian, uint64(0))
		_ = buf.WriteByte(0)
		// No offset data needed
		return nil
	}

	// Write base timestamp (minimum value in block)
	_ = binary.Write(buf, binary.LittleEndian, minVal)

	// Determine width based on maximum offset
	maxOffset := maxVal - minVal
	var width uint8
	if maxOffset <= 0xFF {
		width = 1
	} else if maxOffset <= 0xFFFF {
		width = 2
	} else if maxOffset <= 0xFFFFFFFF {
		width = 4
	} else {
		width = 8
	}
	_ = buf.WriteByte(width)

	// Build offset data
	var offsetBuf bytes.Buffer
	for i := 0; i < spanCount; i++ {
		if isBitSet(present, i) {
			offset := uintValues[i] - minVal
			switch width {
			case 1:
				if offset > 0xFF {
					return fmt.Errorf("delta encoding: offset %d exceeds width 1 (max 255)", offset)
				}
				_ = offsetBuf.WriteByte(uint8(offset))
			case 2:
				if offset > 0xFFFF {
					return fmt.Errorf("delta encoding: offset %d exceeds width 2 (max 65535)", offset)
				}
				_ = binary.Write(&offsetBuf, binary.LittleEndian, uint16(offset))
			case 4:
				if offset > 0xFFFFFFFF {
					return fmt.Errorf("delta encoding: offset %d exceeds width 4 (max 4294967295)", offset)
				}
				_ = binary.Write(&offsetBuf, binary.LittleEndian, uint32(offset))
			case 8:
				_ = binary.Write(&offsetBuf, binary.LittleEndian, offset)
			}
		}
	}

	// Compress offsets
	compressedOffsets := CompressZstd(offsetBuf.Bytes(), encoder)
	// binary.Write to bytes.Buffer never fails - it only panics on out-of-memory
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedOffsets)))
	_, _ = buf.Write(compressedOffsets)

	return nil
}
