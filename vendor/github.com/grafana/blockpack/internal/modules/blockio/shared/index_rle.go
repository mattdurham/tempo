package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
)

const indexRLEVersion uint8 = 1

// EncodeIndexRLE encodes a []uint32 index array to rle_data bytes.
// Wire format: version uint8, run_count uint32 LE, then run_count × {length uint32 LE, value uint32 LE}.
// value is the dictionary index (0-based).
func EncodeIndexRLE(indexes []uint32) ([]byte, error) {
	type run struct {
		length uint32
		value  uint32
	}

	var runs []run

	if len(indexes) > 0 {
		curVal := indexes[0]
		var curLen uint32 = 1

		for i := 1; i < len(indexes); i++ {
			if indexes[i] == curVal {
				curLen++
			} else {
				runs = append(runs, run{length: curLen, value: curVal})
				curVal = indexes[i]
				curLen = 1
			}
		}

		runs = append(runs, run{length: curLen, value: curVal})
	}

	// Encode: 1 (version) + 4 (run_count) + len(runs)*8
	buf := make([]byte, 1+4+len(runs)*8)
	buf[0] = indexRLEVersion
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(runs))) //nolint:gosec // safe: run count bounded by MaxSpans

	off := 5
	for _, r := range runs {
		binary.LittleEndian.PutUint32(buf[off:], r.length)
		binary.LittleEndian.PutUint32(buf[off+4:], r.value)
		off += 8
	}

	return buf, nil
}

// DecodeIndexRLE decodes rle_data to a []uint32 of length nIndexes.
func DecodeIndexRLE(data []byte, nIndexes int) ([]uint32, error) {
	if nIndexes < 0 {
		return nil, fmt.Errorf("index_rle: nIndexes must be non-negative, got %d", nIndexes)
	}

	if len(data) < 5 {
		return nil, fmt.Errorf("index_rle: data too short: %d bytes", len(data))
	}

	if data[0] != indexRLEVersion {
		return nil, fmt.Errorf("index_rle: unsupported version %d", data[0])
	}

	runCount := binary.LittleEndian.Uint32(data[1:5])
	expectedLen := 5 + int(runCount)*8
	if len(data) < expectedLen {
		return nil, fmt.Errorf(
			"index_rle: data too short for %d runs: need %d bytes, got %d",
			runCount, expectedLen, len(data),
		)
	}

	out := make([]uint32, 0, nIndexes)
	off := 5

	for range runCount {
		runLen := int(binary.LittleEndian.Uint32(data[off:]))
		runVal := binary.LittleEndian.Uint32(data[off+4:])
		off += 8

		for range runLen {
			if len(out) >= nIndexes {
				break
			}

			out = append(out, runVal)
		}
	}

	if len(out) != nIndexes {
		return nil, fmt.Errorf(
			"index_rle: decoded %d indexes but expected %d",
			len(out), nIndexes,
		)
	}

	return out, nil
}
