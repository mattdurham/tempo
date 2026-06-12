package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math/bits"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// bitPackedDeltaMinSavedBits is the minimum number of bits per offset the bit-packed form must
// save over the byte-width form (kind 5) for the writer to select it. 4 bits ≈ 12.5% of a 1-byte
// width, the smallest threshold worth the per-column 5-bit width header. Below this the byte-width
// form (kind 5) is kept.
const bitPackedDeltaMinSavedBits = 4

// bitPackedDeltaMinPresent is the minimum present-row count required to select the bit-packed
// form. Below this the fixed per-column header (base[8] + bit_width[1] + packed_len[4]) is too
// large relative to the packed payload, so kind 5 is kept.
const bitPackedDeltaMinPresent = 64

// pickDeltaBitWidth returns the minimum number of bits needed to represent maxOffset (0..64).
func pickDeltaBitWidth(maxOffset uint64) uint8 {
	if maxOffset == 0 {
		return 0
	}
	return uint8(bits.Len64(maxOffset)) //nolint:gosec // bits.Len64 <= 64
}

// shouldUseBitPackedDelta reports whether the bit-packed DeltaUint64 form (kind 22) should be
// selected over the byte-width form (kind 5) for a column with the given range and present-row
// count. Selects when bit packing saves at least bitPackedDeltaMinSavedBits bits per offset
// versus the byte width AND there are at least bitPackedDeltaMinPresent present rows to amortize
// the header. The rollout flag (NOTE-215) gates the whole decision.
func shouldUseBitPackedDelta(maxOffset uint64, presentCount int) bool {
	if !bitPackedDeltaEnabled() {
		return false
	}
	if presentCount < bitPackedDeltaMinPresent {
		return false
	}
	byteWidth := pickDeltaWidth(maxOffset)
	if byteWidth == 0 {
		// All offsets are zero: kind 5 already stores no payload (width=0). Bit packing adds
		// only header overhead, so keep kind 5.
		return false
	}
	bitWidth := pickDeltaBitWidth(maxOffset)
	byteWidthBits := uint16(byteWidth) * 8
	return byteWidthBits-uint16(bitWidth) >= bitPackedDeltaMinSavedBits
}

// encodeDeltaUint64BitPacked encodes a uint64 column using bit-packed delta-from-base encoding
// (kind 22, NOTE-215).
//
// Wire format (V14 enc_version=3):
//
//	enc_version[1] + kind(22)[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ base[8 LE] + bit_width[1]   // 0..64
//	+ packed_len[4 LE] + packed_offsets   // LSB-first bit stream, ceil(nPresent*bit_width/8) bytes
//
// NOTE-AP-001: when every row is present, kind KindDeltaUint64BitPackedAllPresent is emitted and
// the presence_rle_len[4] + presence_rle_data segment is omitted entirely.
//
// Callers select this encoder via shouldUseBitPackedDelta; it does not re-validate that the
// bit-packed form is profitable, so it remains usable for round-trip tests of any column.
func encodeDeltaUint64BitPacked(values []uint64, present []bool, nRows int) ([]byte, error) {
	bitset, presentCount := buildPresenceBitset(present, nRows)

	kind, allPresent := selectAllPresent(KindDeltaUint64BitPacked, presentCount, nRows)

	var rleData []byte
	if !allPresent {
		var err error
		rleData, err = shared.EncodePresenceRLE(bitset, nRows)
		if err != nil {
			return nil, err
		}
	}

	base, maxOffset, _ := deltaBaseAndMaxOffset(values, present, nRows)
	bitWidth := pickDeltaBitWidth(maxOffset)

	packedLen := 0
	if bitWidth > 0 {
		packedLen = (presentCount*int(bitWidth) + 7) / 8
	}

	buf := make([]byte, 0, 2+4+4+len(rleData)+8+1+4+packedLen)
	buf = append(buf, shared.VersionBlockEncV3, kind)
	buf = appendUint32LE(buf, uint32(nRows)) //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendPresenceSegment(buf, rleData, allPresent)

	var baseBytes [8]byte
	binary.LittleEndian.PutUint64(baseBytes[:], base)
	buf = append(buf, baseBytes[:]...)
	buf = append(buf, bitWidth)

	if bitWidth == 0 {
		// All present offsets are zero; no payload, but emit an explicit zero-length segment
		// so the reader's readRawSegment finds a well-formed (empty) packed array.
		buf = appendUint32LE(buf, 0)
		return buf, nil
	}

	packed := make([]byte, packedLen)
	bitPos := 0
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var v uint64
		if i < len(values) {
			v = values[i]
		}
		writeBitsLE(packed, bitPos, v-base, bitWidth)
		bitPos += int(bitWidth)
	}

	buf = appendUint32LE(buf, uint32(len(packed))) //nolint:gosec // safe: packed bounded by block size
	buf = append(buf, packed...)

	return buf, nil
}

// writeBitsLE writes the low width bits of v into dst starting at bit offset bitPos, LSB-first
// within each byte (bit 0 of the first value occupies bit 0 of dst[0]). dst must have room for
// bitPos+width bits. width must be in 1..64.
func writeBitsLE(dst []byte, bitPos int, v uint64, width uint8) {
	remaining := int(width)
	for remaining > 0 {
		byteIdx := bitPos >> 3
		bitOff := bitPos & 7
		n := 8 - bitOff
		if n > remaining {
			n = remaining
		}
		mask := uint64(1)<<uint(n) - 1
		chunk := byte((v & mask) << uint(bitOff)) //nolint:gosec // chunk fits in the masked bit window
		dst[byteIdx] |= chunk
		v >>= uint(n)
		bitPos += n
		remaining -= n
	}
}
