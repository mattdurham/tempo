package shared

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// PresenceRLEVersion is the version number for RLE-encoded presence bitmaps.
	PresenceRLEVersion = uint8(1)
)

// SetBit sets the bit at the specified index to 1.
func SetBit(bits []byte, idx int) {
	byteIdx := idx / 8
	bit := uint(idx % 8) //nolint:gosec
	if byteIdx >= len(bits) {
		return
	}
	bits[byteIdx] |= 1 << bit
}

// IsBitSet returns true if the bit at the specified index is set to 1.
func IsBitSet(bits []byte, idx int) bool {
	if idx < 0 {
		return false
	}
	byteIdx := idx / 8
	if byteIdx >= len(bits) {
		return false
	}
	bit := uint(idx % 8) //nolint:gosec
	return (bits[byteIdx] & (1 << bit)) != 0
}

// BitsLen returns the number of bytes needed to store the given number of bits.
func BitsLen(rows int) int {
	return (rows + 7) / 8
}

// EncodePresenceRLE compresses presence bits into alternating runs of false/true values.
func EncodePresenceRLE(bits []byte, rows int) []byte {
	var buf bytes.Buffer
	runCount := uint32(0)
	last := false
	for i := 0; i < rows; i++ {
		val := IsBitSet(bits, i)
		if i == 0 || val != last {
			runCount++
		}
		last = val
	}
	_ = buf.WriteByte(PresenceRLEVersion)
	_ = binary.Write(&buf, binary.LittleEndian, runCount)

	currentVal := IsBitSet(bits, 0)
	currentLen := uint32(0)
	for i := 0; i < rows; i++ {
		val := IsBitSet(bits, i)
		if val == currentVal {
			currentLen++
			continue
		}
		_ = binary.Write(&buf, binary.LittleEndian, currentLen)
		_ = buf.WriteByte(BoolToByte(currentVal))
		currentVal = val
		currentLen = 1
	}
	_ = binary.Write(&buf, binary.LittleEndian, currentLen)
	_ = buf.WriteByte(BoolToByte(currentVal))
	return buf.Bytes()
}

// DecodePresenceRLE decompresses presence bits from alternating runs of false/true values.
func DecodePresenceRLE(data []byte, rows int) ([]byte, error) {
	rd := bytes.NewReader(data)
	versionByte, err := rd.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read presence RLE version: %w", err)
	}
	if versionByte != PresenceRLEVersion {
		return nil, fmt.Errorf("unsupported presence RLE version %d", versionByte)
	}
	var runCount uint32
	if err := binary.Read(rd, binary.LittleEndian, &runCount); err != nil {
		return nil, fmt.Errorf("read presence RLE run count: %w", err)
	}
	out := make([]byte, BitsLen(rows))
	total := 0
	for i := uint32(0); i < runCount; i++ {
		var length uint32
		if err := binary.Read(rd, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("read presence RLE run %d length: %w", i, err)
		}
		b, err := rd.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read presence RLE run %d value: %w", i, err)
		}
		val := b != 0
		for j := 0; j < int(length) && total < rows; j++ {
			if val {
				SetBit(out, total)
			}
			total++
		}
	}
	if total != rows {
		return nil, fmt.Errorf("presence RLE decoded %d rows, expected %d", total, rows)
	}
	return out, nil
}

// BoolToByte converts a boolean to a byte (0 or 1).
func BoolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
