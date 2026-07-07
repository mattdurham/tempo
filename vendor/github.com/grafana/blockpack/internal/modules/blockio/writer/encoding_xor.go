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
//
// NOTE-AP-001: for the dense kind (shared.KindXORBytes) when every row is present,
// shared.KindXORBytesAllPresent is emitted and the presence_rle segment is omitted.
//
// NOTE-217: when every present value shares the same byte length (uniformLen > 0), the
// uniform variant (encodeXORBytesUniform) is selected instead — it drops the per-row len[4]
// prefix and packs the XOR payload as a fixed-width array.
func encodeXORBytes(kind uint8, values [][]byte, present []bool, nRows int) ([]byte, error) {
	bitset, presentCount := buildPresenceBitset(present, nRows)

	// NOTE-217: route to the uniform-length variant when all present values share one length.
	if uniformBytesEnabled() {
		if uLen, ok := uniformValueLen(values, present, nRows, presentCount); ok {
			uniformKind := shared.KindXORBytesUniform
			if kind == shared.KindSparseXORBytes {
				uniformKind = shared.KindSparseXORBytesUniform
			}
			return encodeXORBytesUniform(uniformKind, values, present, nRows, bitset, presentCount, uLen)
		}
	}

	kind, allPresent := selectAllPresent(kind, presentCount, nRows)

	var rleData []byte
	if !allPresent {
		var err error
		rleData, err = shared.EncodePresenceRLE(bitset, nRows)
		if err != nil {
			return nil, err
		}
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
	buf = appendUint32LE(buf, uint32(nRows)) //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendPresenceSegment(buf, rleData, allPresent)
	buf = appendUint32LE(buf, uint32(len(xorPayload))) //nolint:gosec // safe: raw data bounded by block size
	buf = append(buf, xorPayload...)

	return buf, nil
}

// uniformValueLen reports the common byte length shared by every present value, and true,
// when the uniform-length XOR variant is applicable (NOTE-217). It is applicable only when:
//   - presentCount > 1 (a single present value gains nothing — the per-row len prefix is
//     paid once either way, and the uniform header adds uniform_len[4]); and
//   - every present value has the same non-zero length (firstLen > 0).
//
// Zero-length values fall through to the variable form (a uniform_len of 0 would make the
// payload empty and indistinguishable from "no rows", and zero-length IDs are not a real
// case worth a dedicated kind). Rows beyond len(values) are treated as nil (length 0), which
// also disqualifies the uniform form.
func uniformValueLen(values [][]byte, present []bool, nRows, presentCount int) (int, bool) {
	if presentCount <= 1 {
		return 0, false
	}
	firstLen := -1
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var l int
		if i < len(values) {
			l = len(values[i])
		}
		if firstLen < 0 {
			firstLen = l
			continue
		}
		if l != firstLen {
			return 0, false
		}
	}
	if firstLen <= 0 {
		return 0, false
	}
	return firstLen, true
}

// encodeXORBytesUniform encodes a uniform-length []byte column (NOTE-217, kinds 24/25/28).
//
// Wire format (V14 enc_version=3):
//
//	enc_version[1] + kind[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data   (omitted when AllPresent)
//	+ uniform_len[4 LE]
//	+ packed_xor_payload                            (nPresent × uniform_len bytes)
//
// The XOR computation is identical to encodeXORBytes; only the framing differs (no per-row
// length prefix). uniformLen is guaranteed > 0 and equal for every present row by the caller
// (uniformValueLen). The dense kind composes with NOTE-AP-001: a fully-present column emits
// shared.KindXORBytesUniformAllPresent and omits the presence segment.
func encodeXORBytesUniform(
	kind uint8,
	values [][]byte,
	present []bool,
	nRows int,
	bitset []byte,
	presentCount, uniformLen int,
) ([]byte, error) {
	kind, allPresent := selectAllPresent(kind, presentCount, nRows)

	var rleData []byte
	if !allPresent {
		var err error
		rleData, err = shared.EncodePresenceRLE(bitset, nRows)
		if err != nil {
			return nil, err
		}
	}

	// Packed XOR payload: each present row contributes exactly uniformLen bytes.
	xorPayload := make([]byte, 0, presentCount*uniformLen)
	var prev []byte
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var cur []byte
		if i < len(values) {
			cur = values[i]
		}
		// All present values share uniformLen, so xorBytesLen produces exactly uniformLen bytes.
		xored := xorBytesLen(cur, prev)
		xorPayload = append(xorPayload, xored...)
		prev = cur
	}

	buf := make([]byte, 0, 2+4+4+len(rleData)+4+len(xorPayload))
	buf = append(buf, shared.VersionBlockEncV3, kind)
	buf = appendUint32LE(buf, uint32(nRows)) //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendPresenceSegment(buf, rleData, allPresent)
	// NOTE-217: like the non-uniform XOR kinds (8/9), there is no present_count field — the
	// decoder walks the presence bitset directly. uniform_len[4] is the only payload header.
	buf = appendUint32LE(buf, uint32(uniformLen)) //nolint:gosec // safe: value length bounded by MaxBytesLen
	buf = append(buf, xorPayload...)

	return buf, nil
}

// xorBytesLen XORs a against b producing exactly len(a) bytes.
// Unlike xorBytes, the result length always equals len(a), making the wire format
// self-describing: xor_data_len == original value length.
func xorBytesLen(a, b []byte) []byte {
	result := make([]byte, len(a))
	minLen := min(len(a), len(b))
	for i := range minLen {
		result[i] = a[i] ^ b[i]
	}
	if len(a) > len(b) {
		copy(result[minLen:], a[minLen:])
	}
	// if len(b) > len(a): trailing bytes of b are simply not included
	return result
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
