package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// buildPresenceBitset builds the ceil(nRows/8)-byte presence bitset for present and returns
// it together with the count of present rows. Rows beyond len(present) are treated as absent.
func buildPresenceBitset(present []bool, nRows int) (bitset []byte, presentCount int) {
	bitset = make([]byte, (nRows+7)/8)
	for i := range nRows {
		if i < len(present) && present[i] {
			bitset[i/8] |= 1 << uint(i%8)
			presentCount++
		}
	}
	return bitset, presentCount
}

// selectAllPresent decides whether the AllPresent variant of baseKind should be used for a
// column with presentCount present rows out of nRows. AllPresent is chosen only when every
// row is present (presentCount == nRows), nRows > 0, the rollout flag is enabled, and baseKind
// actually has an AllPresent variant. Returns the kind to emit and whether the presence_rle
// segment must be omitted (allPresent == true).
//
// NOTE-AP-001: omitting the presence segment for fully-present columns saves ~nRows/8 bytes
// plus the per-column RLE encode, and makes the reader's presence synthesis allocation-only.
func selectAllPresent(baseKind uint8, presentCount, nRows int) (kind uint8, allPresent bool) {
	if !allPresentEnabled() || nRows == 0 || presentCount != nRows {
		return baseKind, false
	}
	variant, ok := shared.AllPresentKindFor(baseKind)
	if !ok {
		return baseKind, false
	}
	return variant, true
}

// appendPresenceSegment appends the presence_rle_len[4 LE] + presence_rle_data segment to buf
// unless allPresent is true, in which case nothing is appended (the AllPresent kind byte already
// signals full presence — NOTE-AP-001). When not all-present, rleData must be the pre-encoded
// presence RLE bytes for the column.
func appendPresenceSegment(buf, rleData []byte, allPresent bool) []byte {
	if allPresent {
		return buf
	}
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)
	return buf
}
