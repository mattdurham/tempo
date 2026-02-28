package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

const presenceRLEVersion uint8 = 1

// EncodePresenceRLE encodes a presence bitset (ceil(nBits/8) bytes) to rle_data bytes.
// Wire format: version uint8, run_count uint32 LE, then run_count × {length uint32 LE, value uint32 LE}.
// value=1 means present, value=0 means absent.
func EncodePresenceRLE(present []byte, nBits int) ([]byte, error) {
	if nBits < 0 {
		return nil, fmt.Errorf("presence_rle: nBits must be non-negative, got %d", nBits)
	}

	type run struct {
		length uint32
		value  uint32
	}

	var runs []run

	if nBits > 0 {
		// Determine the value of bit 0.
		var curVal uint32
		if IsPresent(present, 0) {
			curVal = 1
		}

		var curLen uint32 = 1

		for i := 1; i < nBits; i++ {
			var v uint32
			if IsPresent(present, i) {
				v = 1
			}

			if v == curVal {
				curLen++
			} else {
				runs = append(runs, run{length: curLen, value: curVal})
				curVal = v
				curLen = 1
			}
		}

		runs = append(runs, run{length: curLen, value: curVal})
	}

	// Encode: 1 (version) + 4 (run_count) + len(runs)*8
	buf := make([]byte, 1+4+len(runs)*8)
	buf[0] = presenceRLEVersion
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(runs))) //nolint:gosec // safe: run count bounded by MaxSpans

	off := 5
	for _, r := range runs {
		binary.LittleEndian.PutUint32(buf[off:], r.length)
		binary.LittleEndian.PutUint32(buf[off+4:], r.value)
		off += 8
	}

	return buf, nil
}

// DecodePresenceRLE decodes rle_data to a ceil(nBits/8)-byte bitset.
func DecodePresenceRLE(data []byte, nBits int) ([]byte, error) {
	if nBits < 0 {
		return nil, fmt.Errorf("presence_rle: nBits must be non-negative, got %d", nBits)
	}

	if len(data) < 5 {
		return nil, fmt.Errorf("presence_rle: data too short: %d bytes", len(data))
	}

	if data[0] != presenceRLEVersion {
		return nil, fmt.Errorf("presence_rle: unsupported version %d", data[0])
	}

	runCount := binary.LittleEndian.Uint32(data[1:5])
	expectedLen := 5 + int(runCount)*8
	if len(data) < expectedLen {
		return nil, fmt.Errorf(
			"presence_rle: data too short for %d runs: need %d bytes, got %d",
			runCount, expectedLen, len(data),
		)
	}

	byteLen := (nBits + 7) / 8
	out := make([]byte, byteLen)

	bitIdx := 0
	off := 5
	for range runCount {
		runLen := int(binary.LittleEndian.Uint32(data[off:]))
		runVal := binary.LittleEndian.Uint32(data[off+4:])
		off += 8

		if runVal == 1 {
			for range runLen {
				if bitIdx >= nBits {
					break
				}

				out[bitIdx/8] |= 1 << uint(bitIdx%8)
				bitIdx++
			}
		} else {
			bitIdx += runLen
			if bitIdx > nBits {
				bitIdx = nBits
			}
		}
	}

	return out, nil
}

// CountPresent returns the number of set bits in the presence bitset.
func CountPresent(present []byte, nBits int) int {
	if nBits <= 0 {
		return 0
	}

	fullBytes := nBits / 8
	count := 0

	for i := range fullBytes {
		count += bits.OnesCount8(present[i])
	}

	rem := nBits % 8
	if rem > 0 && fullBytes < len(present) {
		// Mask off bits beyond nBits in the last byte.
		mask := byte((1 << uint(rem)) - 1)
		count += bits.OnesCount8(present[fullBytes] & mask)
	}

	return count
}

// IsPresent tests whether bit idx is set in the presence bitset.
func IsPresent(present []byte, idx int) bool {
	byteIdx := idx / 8
	if byteIdx >= len(present) {
		return false
	}

	return present[byteIdx]&(1<<uint(idx%8)) != 0 //nolint:gosec // safe: idx%8 is always 0-7
}
