package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// gorillaMinPresent is the minimum present-row count required to select the Gorilla-XOR form.
// Below this the per-column header (stream_bit_len[8] + first value[8]) is too large relative to
// the packed payload, so the Dictionary path is kept.
const gorillaMinPresent = 64

// gorillaCardinalityFloorFraction sets the cardinality threshold as a fraction of present rows.
// A float column is routed to Gorilla only when its distinct-value count exceeds
// max(gorillaCardinalityFloor, presentCount/gorillaCardinalityFloorFraction). This is the
// critical "don't regress low-cardinality columns" guard (NOTE-219, two-population analysis):
// columns with few distinct values (HTTP sampling ratios, rounded utilization gauges, etc.)
// dedup superbly under Dictionary+RLE and MUST stay there. Only columns whose values are mostly
// distinct — where the dictionary provides no real deduplication — are candidates for Gorilla.
const gorillaCardinalityFloorFraction = 4

// gorillaCardinalityFloor is the absolute minimum distinct-value count below which a column is
// always kept on Dictionary regardless of present-row count. Mirrors the issue's
// `cardinality > max(64, nRows/4)` rule.
const gorillaCardinalityFloor = 64

// shouldUseGorillaFloat64 reports whether the Gorilla-XOR form (kind 40/41) should be selected
// over the Dictionary path (kinds 1/2) for a float64 column. The decision is purely
// data-driven (cardinality vs present-row count), never name- or type-based, so it generalizes
// across the whole float column population. Selection requires the rollout flag enabled, at
// least gorillaMinPresent present rows, and a distinct-value count exceeding
// max(gorillaCardinalityFloor, presentCount/gorillaCardinalityFloorFraction).
func shouldUseGorillaFloat64(values []float64, present []bool, nRows int) bool {
	if !gorillaFloat64Enabled() {
		return false
	}
	presentCount, cardinality := float64PresenceAndCardinality(values, present, nRows)
	if presentCount < gorillaMinPresent {
		return false
	}
	threshold := gorillaCardinalityFloor
	if frac := presentCount / gorillaCardinalityFloorFraction; frac > threshold {
		threshold = frac
	}
	return cardinality > threshold
}

// float64PresenceAndCardinality returns the present-row count and the number of distinct present
// float64 values for the column. Distinct counting is done on the raw IEEE-754 bit pattern so
// that -0.0 and +0.0 count as distinct and every NaN payload counts as distinct — matching the
// exact-roundtrip contract of the Gorilla encoder. The map is bounded by presentCount, which is
// itself bounded by MaxBlockSpans (65535).
func float64PresenceAndCardinality(values []float64, present []bool, nRows int) (presentCount, cardinality int) {
	seen := make(map[uint64]struct{})
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		presentCount++
		var v float64
		if i < len(values) {
			v = values[i]
		}
		seen[math.Float64bits(v)] = struct{}{}
	}
	return presentCount, len(seen)
}

// encodeGorillaFloat64 encodes a float64 column using Gorilla-XOR delta encoding (kind 40,
// NOTE-219, SPECS §9.8).
//
// Wire format (V14 enc_version=3):
//
//	enc_version[1] + kind(40)[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data    // omitted for kind 41 (AllPresent)
//	+ stream_bit_len[8 LE]                           // total meaningful bit count of the stream
//	+ stream_len[4 LE] + stream_bytes                // LSB-first Gorilla bit stream
//
// The bit stream encodes the present values in row order. The first present value is stored
// verbatim as a 64-bit IEEE-754 word. Each subsequent present value is XORed against its
// predecessor:
//   - XOR == 0          → emit control bit 0.
//   - XOR != 0          → emit control bit 1, then a block-reuse bit:
//   - 0  → the meaningful bits fit in the previous (leading,len) window: emit meaningful[len].
//   - 1  → new window: emit leading[5] + meaningful_len[6] + meaningful[meaningful_len].
//
// Bit values are written LSB-first via writeBitsLE (shared with kind 22). Callers select this
// encoder via shouldUseGorillaFloat64; it does not re-validate that the Gorilla form is
// profitable, so it remains usable for round-trip tests of any column.
func encodeGorillaFloat64(values []float64, present []bool, nRows int) ([]byte, error) {
	bitset, presentCount := buildPresenceBitset(present, nRows)

	kind, allPresent := selectAllPresent(KindGorillaFloat64, presentCount, nRows)

	var rleData []byte
	if !allPresent {
		var err error
		rleData, err = shared.EncodePresenceRLE(bitset, nRows)
		if err != nil {
			return nil, err
		}
	}

	w := &gorillaBitWriter{}
	firstValue := true
	haveWindow := false // true once a (leading,trailing) window has been emitted
	var prevBits uint64
	var prevLeading, prevTrailing uint8
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var v float64
		if i < len(values) {
			v = values[i]
		}
		curBits := math.Float64bits(v)

		if firstValue {
			w.writeBits(curBits, 64)
			prevBits = curBits
			firstValue = false
			continue
		}

		xor := curBits ^ prevBits
		if xor == 0 {
			w.writeBit(0)
			prevBits = curBits
			continue
		}
		w.writeBit(1)

		leading := uint8(bits.LeadingZeros64(xor))   //nolint:gosec // 0..64
		trailing := uint8(bits.TrailingZeros64(xor)) //nolint:gosec // 0..64
		// Gorilla clamps leading at 31 so it fits in 5 bits.
		if leading >= 32 {
			leading = 31
		}

		// Reuse the previous window only when the current XOR's meaningful bits fit entirely
		// inside it (leading >= prevLeading && trailing >= prevTrailing). The first XORed value
		// has no previous window, so it always emits a new one.
		if haveWindow && leading >= prevLeading && trailing >= prevTrailing {
			w.writeBit(0)
			meaningfulLen := 64 - int(prevLeading) - int(prevTrailing)
			w.writeBits(xor>>uint(prevTrailing), uint8(meaningfulLen)) //nolint:gosec // 1..64
		} else {
			w.writeBit(1)
			meaningfulLen := 64 - int(leading) - int(trailing)
			if meaningfulLen < 1 {
				meaningfulLen = 1
			}
			w.writeBits(uint64(leading), 5)
			// meaningful_len is stored as len-1 in 6 bits (1..64 → 0..63).
			w.writeBits(uint64(meaningfulLen-1), 6)                //nolint:gosec // meaningfulLen 1..64
			w.writeBits(xor>>uint(trailing), uint8(meaningfulLen)) //nolint:gosec // 1..64
			prevLeading = leading
			prevTrailing = trailing
			haveWindow = true
		}
		prevBits = curBits
	}

	stream := w.bytes()
	streamBitLen := w.bitLen()

	buf := make([]byte, 0, 2+4+4+len(rleData)+8+4+len(stream))
	buf = append(buf, shared.VersionBlockEncV3, kind)
	buf = appendUint32LE(buf, uint32(nRows)) //nolint:gosec // safe: nRows bounded by MaxBlockSpans
	buf = appendPresenceSegment(buf, rleData, allPresent)

	var bitLenBytes [8]byte
	binary.LittleEndian.PutUint64(bitLenBytes[:], streamBitLen)
	buf = append(buf, bitLenBytes[:]...)

	buf = appendUint32LE(buf, uint32(len(stream))) //nolint:gosec // safe: stream bounded by block size
	buf = append(buf, stream...)

	return buf, nil
}

// gorillaBitWriter accumulates an LSB-first bit stream using the shared writeBitsLE primitive.
// It grows its backing slice as needed and tracks the exact meaningful bit count so the reader
// knows where the stream ends (the final byte may have trailing zero padding).
type gorillaBitWriter struct {
	buf    []byte
	bitPos int
}

// ensure grows buf so that bit offset bitPos+width fits.
func (w *gorillaBitWriter) ensure(width int) {
	need := (w.bitPos + width + 7) / 8
	for len(w.buf) < need {
		w.buf = append(w.buf, 0)
	}
}

// writeBit writes a single bit (0 or 1).
func (w *gorillaBitWriter) writeBit(b uint64) {
	w.ensure(1)
	writeBitsLE(w.buf, w.bitPos, b&1, 1)
	w.bitPos++
}

// writeBits writes the low width bits of v (width 1..64).
func (w *gorillaBitWriter) writeBits(v uint64, width uint8) {
	w.ensure(int(width))
	writeBitsLE(w.buf, w.bitPos, v, width)
	w.bitPos += int(width)
}

// bytes returns the accumulated stream bytes.
func (w *gorillaBitWriter) bytes() []byte { return w.buf }

// bitLen returns the total meaningful bit count written.
func (w *gorillaBitWriter) bitLen() uint64 { return uint64(w.bitPos) } //nolint:gosec // bitPos >= 0
