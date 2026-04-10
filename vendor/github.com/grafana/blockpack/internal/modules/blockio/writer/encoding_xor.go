package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// encodeXORBytes encodes a []byte column using XOR-against-previous encoding (kinds 8/9).
//
// Wire format (V14 enc_version=3):
//
//	enc_version[1] + kind[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ xor_data_len[4 LE] + raw_xor_payload  (no zstd — outer snappy per column)
//
// XOR payload (for each present row in order):
//
//	val_len[4 LE] + xor_bytes
func encodeXORBytes(kind uint8, values [][]byte, present []bool, nRows int) ([]byte, error) {
	// Build presence bitset.
	bitsetLen := (nRows + 7) / 8
	bitset := make([]byte, bitsetLen)

	for i := range nRows {
		if i < len(present) && present[i] {
			bitset[i/8] |= 1 << uint(i%8)
		}
	}

	rleData, err := shared.EncodePresenceRLE(bitset, nRows)
	if err != nil {
		return nil, err
	}

	// Build XOR payload for present rows.
	var prev []byte
	xorPayload := make([]byte, 0, nRows*20)

	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var cur []byte
		if i < len(values) {
			cur = values[i]
		}

		xored := xorBytes(cur, prev)
		xorPayload = appendUint32LE(
			xorPayload,
			uint32(len(xored)), //nolint:gosec // safe: entry length bounded by MaxBytesLen
		)
		xorPayload = append(xorPayload, xored...)
		prev = cur
	}

	// V14: XOR payload is raw (no zstd). Outer snappy applied per-column by block writer.
	buf := make([]byte, 0, 2+4+4+len(rleData)+4+len(xorPayload))
	buf = append(buf, shared.VersionBlockEncV3, kind)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)
	buf = appendUint32LE(buf, uint32(len(xorPayload))) //nolint:gosec // safe: raw data bounded by block size
	buf = append(buf, xorPayload...)

	return buf, nil
}

// xorBytes returns a XOR b byte-by-byte. If lengths differ, XOR up to min length;
// the remainder of the longer slice is appended unchanged.
func xorBytes(a, b []byte) []byte {
	minLen := min(len(a), len(b))
	result := make([]byte, max(len(a), len(b)))

	for i := range minLen {
		result[i] = a[i] ^ b[i]
	}

	if len(a) > len(b) {
		copy(result[minLen:], a[minLen:])
	} else if len(b) > len(a) {
		copy(result[minLen:], b[minLen:])
	}

	return result
}
