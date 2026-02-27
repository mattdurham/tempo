package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// encodePrefixBytes encodes a []byte column using prefix-dictionary compression (kinds 10/11).
//
// Wire format:
//
//	enc_version[1] + kind[1] + span_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ prefix_dict_len[4 LE] + zstd(prefix_dict_payload)
//	+ suffix_data_len[4 LE] + zstd(suffix_data_payload)
//
// Prefix dict payload:
//
//	prefix_index_width[1] + n_prefixes[4 LE] + n_prefixes × { prefix_len[4 LE] + prefix_bytes }
//
// Suffix data payload (for each present row):
//
//	prefix_idx[prefix_index_width bytes LE] + suffix_len[4 LE] + suffix_bytes
func encodePrefixBytes(
	kind uint8,
	values [][]byte,
	present []bool,
	nRows int,
	enc *zstdEncoder,
) ([]byte, error) {
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

	// Collect present values for dictionary building.
	presentValues := make([][]byte, 0, nRows)
	for i := range nRows {
		if i < len(present) && present[i] {
			var v []byte
			if i < len(values) {
				v = values[i]
			}
			presentValues = append(presentValues, v)
		}
	}

	prefixes, prefixIdx, suffixes := buildPrefixDictionary(presentValues)
	indexWidth := pickPrefixIndexWidth(len(prefixes))

	// Encode prefix dict payload: prefix_count[4] + n × (len[4] + bytes).
	dictPayload := make([]byte, 0, 4+len(prefixes)*8)
	dictPayload = appendUint32LE(dictPayload, uint32(len(prefixes))) //nolint:gosec // safe: prefix count bounded by MaxDictionarySize
	for _, p := range prefixes {
		dictPayload = appendUint32LE(dictPayload, uint32(len(p))) //nolint:gosec // safe: prefix length bounded by MaxStringLen
		dictPayload = append(dictPayload, p...)
	}

	compressedDict, err := enc.compress(dictPayload)
	if err != nil {
		return nil, err
	}

	// Encode suffix data payload: prefix_index_width[1] + per present row × SuffixEntry.
	suffixPayload := make([]byte, 0, 1+len(presentValues)*12)
	suffixPayload = append(suffixPayload, indexWidth)
	for i, suf := range suffixes {
		suffixPayload = appendUintLE(suffixPayload, uint64(prefixIdx[i]), indexWidth)
		suffixPayload = appendUint32LE(suffixPayload, uint32(len(suf))) //nolint:gosec // safe: suffix length bounded by MaxStringLen
		suffixPayload = append(suffixPayload, suf...)
	}

	compressedSuffix, err := enc.compress(suffixPayload)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 2+4+4+len(rleData)+4+len(compressedDict)+4+len(compressedSuffix))
	buf = append(buf, shared.ColumnEncodingVersion, kind)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)
	buf = appendUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec // safe: compressed data bounded by block size
	buf = append(buf, compressedDict...)
	buf = appendUint32LE(buf, uint32(len(compressedSuffix))) //nolint:gosec // safe: compressed data bounded by block size
	buf = append(buf, compressedSuffix...)

	return buf, nil
}

// buildPrefixDictionary builds a prefix dictionary for the given present values.
// It returns the prefix list, per-row prefix index (into prefixes), and per-row suffix.
func buildPrefixDictionary(values [][]byte) (prefixes [][]byte, prefixIdx []uint32, suffixes [][]byte) {
	if len(values) == 0 {
		return [][]byte{{}}, nil, nil
	}

	// Sort copies for LCP analysis.
	sorted := make([][]byte, len(values))
	copy(sorted, values)
	slices.SortFunc(sorted, func(a, b []byte) int {
		minLen := min(len(a), len(b))
		for i := range minLen {
			if a[i] < b[i] {
				return -1
			}
			if a[i] > b[i] {
				return 1
			}
		}
		return len(a) - len(b)
	})

	// Find LCP lengths between adjacent sorted values.
	type lcpEntry struct {
		prefix []byte
		count  int
	}
	lcpMap := make(map[string]*lcpEntry)

	for i := 1; i < len(sorted); i++ {
		lcp := commonPrefixLen(sorted[i-1], sorted[i])
		if lcp > 0 {
			p := sorted[i][:lcp]
			key := string(p)
			if e, ok := lcpMap[key]; ok {
				e.count++
			} else {
				lcpMap[key] = &lcpEntry{prefix: p, count: 1}
			}
		}
	}

	// Pick the top 5 most-common LCP prefixes.
	type scored struct {
		prefix []byte
		count  int
	}
	candidates := make([]scored, 0, len(lcpMap))
	for _, e := range lcpMap {
		candidates = append(candidates, scored{prefix: e.prefix, count: e.count})
	}
	slices.SortFunc(candidates, func(a, b scored) int {
		return b.count - a.count // descending by count
	})
	const maxPrefixes = 5
	if len(candidates) > maxPrefixes {
		candidates = candidates[:maxPrefixes]
	}

	prefixes = make([][]byte, 0, len(candidates)+1)
	// Index 0 is always the empty prefix (no-match sentinel).
	prefixes = append(prefixes, []byte{})
	for _, c := range candidates {
		prefixes = append(prefixes, c.prefix)
	}

	// For each value, find the longest matching prefix.
	prefixIdx = make([]uint32, len(values))
	suffixes = make([][]byte, len(values))

	for i, v := range values {
		bestIdx := uint32(0)
		bestLen := 0
		for pi, p := range prefixes {
			if len(p) > 0 && len(p) <= len(v) && bytes.Equal(v[:len(p)], p) {
				if len(p) > bestLen {
					bestLen = len(p)
					bestIdx = uint32(pi)
				}
			}
		}
		prefixIdx[i] = bestIdx
		if bestLen > 0 {
			suffixes[i] = v[bestLen:]
		} else {
			suffixes[i] = v
		}
	}

	return prefixes, prefixIdx, suffixes
}

// commonPrefixLen returns the length of the longest common prefix of a and b.
func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// pickPrefixIndexWidth returns 1, 2, or 4 depending on the number of prefix entries.
func pickPrefixIndexWidth(prefixCount int) uint8 {
	switch {
	case prefixCount <= 255:
		return 1
	case prefixCount <= 65535:
		return 2
	default:
		return 4
	}
}
