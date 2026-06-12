package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// deltaPageSize is the number of present rows per page in the per-page DeltaUint64 encoding
// (kind 39, NOTE-218). Each page picks its own base + bit_width. 1024 is large enough that the
// fixed per-page header (page_first_row[4] + page_base[8] + page_bit_width[1] +
// page_payload_bytes[4] = 17 B) is small relative to the page payload, yet small enough that a
// bursty-then-trickle timestamp distribution lands its burst and its trickle in separate pages.
const deltaPageSize = 1024

// pagedDeltaMinSavedBitFraction is the minimum fraction of the column-wide bit-packed payload the
// per-page form must save to be selected over kind 22. 12.5% (= 1/8) is the smallest saving worth
// the extra per-page headers. Below this, the single-page bit-packed form (kind 22) is kept.
const pagedDeltaMinSavedBitFraction = 8 // saved*pagedDeltaMinSavedBitFraction >= columnWide

// pagedDeltaMinPages is the minimum number of pages required to select the per-page form. With
// fewer than 2 pages there is nothing to adapt per-page, so kind 22 (or kind 5) is kept.
const pagedDeltaMinPages = 2

// shouldUsePagedDelta reports whether the per-page DeltaUint64 form (kind 39) should be selected
// over the single-page bit-packed form (kind 22) for the given column. It computes the would-be
// per-page bit-width sum and compares it against the column-wide bit-packed payload size.
//
// Selection requires (SPEC-006):
//  1. the rollout flag is on,
//  2. there are at least pagedDeltaMinPages pages of present rows (i.e. present_count >=
//     pagedDeltaMinPages * deltaPageSize), and
//  3. the per-page packed payload is at least 1/pagedDeltaMinSavedBitFraction (12.5%) smaller than
//     the column-wide bit-packed payload.
//
// presentValues is the in-order slice of present row values (length presentCount). columnMaxOffset
// is the column-wide max offset over base, used to size the kind-22 comparison baseline.
func shouldUsePagedDelta(presentValues []uint64, columnBase, columnMaxOffset uint64) bool {
	if !pagedDeltaEnabled() {
		return false
	}
	presentCount := len(presentValues)
	if presentCount < pagedDeltaMinPages*deltaPageSize {
		return false
	}
	if columnMaxOffset == 0 {
		// All present values are equal: kind 5 stores no payload; paging cannot beat that.
		return false
	}

	columnWideBitWidth := pickDeltaBitWidth(columnMaxOffset)
	columnWideBits := presentCount * int(columnWideBitWidth)

	pagedBits := 0
	for start := 0; start < presentCount; start += deltaPageSize {
		end := start + deltaPageSize
		if end > presentCount {
			end = presentCount
		}
		_, pageMaxOffset := pageBaseAndMaxOffset(presentValues[start:end])
		pageBitWidth := pickDeltaBitWidth(pageMaxOffset)
		pagedBits += (end - start) * int(pageBitWidth)
	}

	// Require pagedBits to be at least 12.5% smaller than columnWideBits.
	saved := columnWideBits - pagedBits
	if saved <= 0 {
		return false
	}
	return saved*pagedDeltaMinSavedBitFraction >= columnWideBits
}

// pageBaseAndMaxOffset returns the minimum value (page base) and the maximum offset over that
// base for a contiguous run of present values.
func pageBaseAndMaxOffset(vals []uint64) (base, maxOffset uint64) {
	if len(vals) == 0 {
		return 0, 0
	}
	base = vals[0]
	for _, v := range vals[1:] {
		if v < base {
			base = v
		}
	}
	for _, v := range vals {
		off := v - base
		if off > maxOffset {
			maxOffset = off
		}
	}
	return base, maxOffset
}

// collectPresentValues returns the in-order slice of present row values and the index of the
// first present row for each page boundary. Reused by the selector and the encoder so both agree
// on the same present-row ordering.
func collectPresentValues(values []uint64, present []bool, nRows int) (presentValues []uint64, presentRowIdx []int) {
	presentValues = make([]uint64, 0, nRows)
	presentRowIdx = make([]int, 0, nRows)
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var v uint64
		if i < len(values) {
			v = values[i]
		}
		presentValues = append(presentValues, v)
		presentRowIdx = append(presentRowIdx, i)
	}
	return presentValues, presentRowIdx
}

// encodeDeltaUint64Paged encodes a uint64 column using per-page bit-packed delta-from-base
// encoding (kind 39, NOTE-218).
//
// Wire format (V14 enc_version=3):
//
//	enc_version[1] + kind(39)[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ page_count[2 LE]
//	+ page_count × (
//	    page_first_row[4 LE]      // row index of the first present row in this page
//	  + page_base[8 LE]           // minimum present value within this page
//	  + page_bit_width[1]         // 0..64
//	  + page_payload_bytes[4 LE]  // ceil(page_rows * page_bit_width / 8)
//	  )
//	+ page_count × page_payload   // concatenated LSB-first bit-packed offsets
//
// There is no AllPresent variant: the presence_rle segment is always emitted. The caller selects
// this encoder via shouldUsePagedDelta; it does not re-validate that the per-page form is
// profitable, so it remains usable for round-trip tests of any column with >= 1 present row.
func encodeDeltaUint64Paged(values []uint64, present []bool, nRows int) ([]byte, error) {
	bitset, _ := buildPresenceBitset(present, nRows)

	rleData, err := shared.EncodePresenceRLE(bitset, nRows)
	if err != nil {
		return nil, err
	}

	presentValues, presentRowIdx := collectPresentValues(values, present, nRows)
	presentCount := len(presentValues)

	pageCount := (presentCount + deltaPageSize - 1) / deltaPageSize

	// Build per-page headers and payloads.
	type pageMeta struct {
		payload  []byte
		base     uint64
		firstRow uint32
		bitWidth uint8
	}
	pages := make([]pageMeta, 0, pageCount)
	for start := 0; start < presentCount; start += deltaPageSize {
		end := start + deltaPageSize
		if end > presentCount {
			end = presentCount
		}
		pageVals := presentValues[start:end]
		base, maxOffset := pageBaseAndMaxOffset(pageVals)
		bitWidth := pickDeltaBitWidth(maxOffset)

		var payload []byte
		if bitWidth > 0 {
			payloadLen := (len(pageVals)*int(bitWidth) + 7) / 8
			payload = make([]byte, payloadLen)
			bitPos := 0
			for _, v := range pageVals {
				writeBitsLE(payload, bitPos, v-base, bitWidth)
				bitPos += int(bitWidth)
			}
		}

		pages = append(pages, pageMeta{
			firstRow: uint32(presentRowIdx[start]), //nolint:gosec // row index bounded by MaxBlockSpans
			base:     base,
			bitWidth: bitWidth,
			payload:  payload,
		})
	}

	// Size: header + presence + page_count[2] + per-page headers + payloads.
	headerBytes := 2 + 4 + 4 + len(rleData) + 2
	perPageHeader := pageCount * (4 + 8 + 1 + 4)
	payloadBytes := 0
	for i := range pages {
		payloadBytes += len(pages[i].payload)
	}

	buf := make([]byte, 0, headerBytes+perPageHeader+payloadBytes)
	buf = append(buf, shared.VersionBlockEncV3, shared.KindDeltaUint64Paged)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)

	pc := uint16(pageCount) //nolint:gosec // pageCount bounded by MaxBlockSpans/pageSize
	var pageCountBytes [2]byte
	binary.LittleEndian.PutUint16(pageCountBytes[:], pc)
	buf = append(buf, pageCountBytes[:]...)

	// Page index.
	for i := range pages {
		buf = appendUint32LE(buf, pages[i].firstRow)
		var baseBytes [8]byte
		binary.LittleEndian.PutUint64(baseBytes[:], pages[i].base)
		buf = append(buf, baseBytes[:]...)
		buf = append(buf, pages[i].bitWidth)
		buf = appendUint32LE(buf, uint32(len(pages[i].payload))) //nolint:gosec // payload bounded by block size
	}

	// Concatenated payloads.
	for i := range pages {
		buf = append(buf, pages[i].payload...)
	}

	return buf, nil
}
