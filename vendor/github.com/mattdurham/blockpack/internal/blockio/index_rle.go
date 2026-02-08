package blockio

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// decodeIndexRLE decompresses RLE-encoded uint32 indexes.
func decodeIndexRLE(data []byte, count int) ([]uint32, error) {
	rd := bytes.NewReader(data)
	versionByte, err := rd.ReadByte()
	if err != nil {
		return nil, err
	}
	if versionByte != 1 {
		return nil, fmt.Errorf("unsupported index RLE version %d", versionByte)
	}
	var runCount uint32
	if err := binary.Read(rd, binary.LittleEndian, &runCount); err != nil {
		return nil, err
	}

	out := make([]uint32, count)
	pos := 0
	for i := uint32(0); i < runCount; i++ {
		var length uint32
		if err := binary.Read(rd, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		var value uint32
		if err := binary.Read(rd, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		for j := uint32(0); j < length && pos < count; j++ {
			out[pos] = value
			pos++
		}
	}
	if pos != count {
		return nil, fmt.Errorf("index RLE decoded %d values, expected %d", pos, count)
	}
	return out, nil
}
