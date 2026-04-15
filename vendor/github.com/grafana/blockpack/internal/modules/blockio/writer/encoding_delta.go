package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// encodeDeltaUint64 encodes a uint64 column using delta-from-base encoding (kind 5).
//
// Wire format (V14 enc_version=3):
//
//	enc_version[1] + kind(5)[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ base[8 LE] + width[1]
//	  if width > 0: offset_data_len[4 LE] + raw_offsets  (no zstd — outer snappy per column)
func encodeDeltaUint64(values []uint64, present []bool, nRows int) ([]byte, error) {
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

	// Compute base (min of present values) and maxOffset.
	var base uint64
	var maxOffset uint64
	hasAny := false

	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var v uint64
		if i < len(values) {
			v = values[i]
		}
		if !hasAny {
			base = v
			hasAny = true
		} else if v < base {
			base = v
		}
	}

	if hasAny {
		for i := range nRows {
			if i >= len(present) || !present[i] {
				continue
			}
			var v uint64
			if i < len(values) {
				v = values[i]
			}
			offset := v - base
			if offset > maxOffset {
				maxOffset = offset
			}
		}
	}

	width := pickDeltaWidth(maxOffset)

	buf := make([]byte, 0, 2+4+4+len(rleData)+8+1+4+nRows*int(width)+1)
	buf = append(buf, shared.VersionBlockEncV3, KindDeltaUint64)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)

	var baseBytes [8]byte
	binary.LittleEndian.PutUint64(baseBytes[:], base)
	buf = append(buf, baseBytes[:]...)
	buf = append(buf, width)

	if width == 0 {
		return buf, nil
	}

	// Build offset byte slice for present rows.
	offsetBuf := make([]byte, 0, nRows*int(width))
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var v uint64
		if i < len(values) {
			v = values[i]
		}
		offset := v - base
		offsetBuf = appendUintLE(offsetBuf, offset, width)
	}

	// V14: offsets are raw (no zstd). Outer snappy applied per-column by block writer.
	buf = appendUint32LE(buf, uint32(len(offsetBuf))) //nolint:gosec // safe: raw data bounded by block size
	buf = append(buf, offsetBuf...)

	return buf, nil
}

// pickDeltaWidth returns the minimum byte width needed to represent maxOffset.
func pickDeltaWidth(maxOffset uint64) uint8 {
	switch {
	case maxOffset == 0:
		return 0
	case maxOffset <= 255:
		return 1
	case maxOffset <= deltaRangeThreshold16:
		return 2
	case maxOffset <= deltaRangeThreshold32:
		return 4
	default:
		return 8
	}
}

// appendUintLE appends v as width little-endian bytes to buf.
func appendUintLE(buf []byte, v uint64, width uint8) []byte {
	switch width {
	case 1:
		return append(buf, byte(v)) //nolint:gosec // safe: truncating to width bytes as designed
	case 2:
		return append(buf, byte(v), byte(v>>8)) //nolint:gosec // safe: truncating to width bytes as designed
	case 4:
		return append(
			buf,
			byte(v),     //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>8),  //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>16), //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>24), //nolint:gosec // safe: truncating to width bytes as designed
		)
	default: // 8
		return append(
			buf,
			byte(v),     //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>8),  //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>16), //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>24), //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>32), //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>40), //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>48), //nolint:gosec // safe: truncating to width bytes as designed
			byte(v>>56), //nolint:gosec // safe: truncating to width bytes as designed
		)
	}
}
