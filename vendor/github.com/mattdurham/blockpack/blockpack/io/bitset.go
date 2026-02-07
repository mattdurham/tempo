package ondiskio

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func setBit(bits []byte, idx int) {
	byteIdx := idx / 8
	bit := uint(idx % 8)
	if byteIdx >= len(bits) {
		return
	}
	bits[byteIdx] |= 1 << bit
}

func bitsLen(rows int) int {
	return (rows + 7) / 8
}

func isBitSet(bits []byte, idx int) bool {
	if idx < 0 {
		return false
	}
	byteIdx := idx / 8
	if byteIdx >= len(bits) {
		return false
	}
	bit := uint(idx % 8)
	return (bits[byteIdx] & (1 << bit)) != 0
}

// EncodePresenceRLE compresses presence bits into alternating runs of false/true values.
func EncodePresenceRLE(bits []byte, rows int) []byte {
	var buf bytes.Buffer
	runCount := uint32(0)
	last := false
	for i := 0; i < rows; i++ {
		val := isBitSet(bits, i)
		if i == 0 || val != last {
			runCount++
		}
		last = val
	}
	_ = buf.WriteByte(presenceRLEVersion)
	_ = binary.Write(&buf, binary.LittleEndian, runCount)

	currentVal := isBitSet(bits, 0)
	currentLen := uint32(0)
	for i := 0; i < rows; i++ {
		val := isBitSet(bits, i)
		if val == currentVal {
			currentLen++
			continue
		}
		_ = binary.Write(&buf, binary.LittleEndian, currentLen)
		_ = buf.WriteByte(boolToByte(currentVal))
		currentVal = val
		currentLen = 1
	}
	_ = binary.Write(&buf, binary.LittleEndian, currentLen)
	_ = buf.WriteByte(boolToByte(currentVal))
	return buf.Bytes()
}

func decodePresenceRLE(data []byte, rows int) ([]byte, error) {
	rd := bytes.NewReader(data)
	versionByte, err := rd.ReadByte()
	if err != nil {
		return nil, err
	}
	if versionByte != presenceRLEVersion {
		return nil, fmt.Errorf("unsupported presence RLE version %d", versionByte)
	}
	var runCount uint32
	if err := binary.Read(rd, binary.LittleEndian, &runCount); err != nil {
		return nil, err
	}
	out := make([]byte, bitsLen(rows))
	total := 0
	for i := uint32(0); i < runCount; i++ {
		var length uint32
		if err := binary.Read(rd, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		b, err := rd.ReadByte()
		if err != nil {
			return nil, err
		}
		val := b != 0
		for j := 0; j < int(length) && total < rows; j++ {
			if val {
				setBit(out, total)
			}
			total++
		}
	}
	if total != rows {
		return nil, fmt.Errorf("presence RLE decoded %d rows, expected %d", total, rows)
	}
	return out, nil
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
