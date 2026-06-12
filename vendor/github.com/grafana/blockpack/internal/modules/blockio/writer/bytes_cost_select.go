package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.
//
// NOTE-221 (issue #333): data-driven encoding selector for the bytes column path.
//
// The legacy bytes selector (encoding_select.go isIDColumn/isURLColumn) dispatched purely on the
// column NAME suffix with zero inspection of the actual values: customer.duration_id forced XOR
// onto a duration value, config.file.path forced Prefix onto paths with no shared prefix. This
// file replaces that name-suffix dispatch with a two-tier system:
//
//  1. Tier 1 — semantic overrides (shared.SemanticBytesOverride): a small, deliberate allow-list
//     of intrinsic columns (trace:id, span:id, ...) whose best encoding is known a-priori. Only
//     intrinsic names are eligible; user attribute names never match.
//  2. Tier 2 — cost-based selection: a single streaming stats pass over the present values, then
//     a pure cost estimate (estimated wire bytes) per candidate encoding family; the cheapest
//     wins. The demoted name-suffix heuristics (isIDColumn/isURLColumn) survive only as a tiebreak
//     hint when the two cheapest estimates are within bytesCostTiebreakFraction of each other.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// bytesCostTiebreakFraction: when the best two cost estimates are within this fraction of each
// other the decision is a near-tie, so the demoted name-suffix hint (isIDColumn/isURLColumn) is
// allowed to break it. 0.05 = within 5%.
const bytesCostTiebreakFraction = 0.05

// bytesEncoding enumerates the candidate dense encoding families for a bytes column. The sparse
// and all-present variants are derived downstream by the encoders; the cost model compares
// families, not concrete kind IDs.
type bytesEncoding uint8

const (
	bytesEncDictionary bytesEncoding = iota
	bytesEncXOR
	bytesEncPrefix
	bytesEncDeltaDictionary
)

// bytesStats is the alloc-light summary of a bytes column gathered in a single pass. All fields
// are derived from the present values only.
type bytesStats struct {
	presentCount int
	totalBytes   int // sum of len(v) over present values

	// distinctCount is a cap-N cardinality estimate. Once it reaches bytesCardinalityCap it stops
	// counting (the exact value no longer changes any encoding decision). Alloc-free: it uses a
	// fixed-size open-addressed fingerprint set on the first bytesCardinalityCap distinct values.
	distinctCount int
	distinctSat   bool // true once distinctCount hit the cap (saturated)

	commonPrefixLen int // shared byte prefix across all present values
	xorPayloadBytes int // sum of meaningful (non-leading-zero after XOR-with-prev) bytes
	uniformLen      int // common value length if all present values share one length, else -1
}

// bytesCardinalityCap bounds the cap-N distinct estimator. The dictionary cost model only needs
// to distinguish "few" from "many" distinct values; once the count exceeds this cap the exact
// figure does not change which encoding wins, so we stop tracking.
const bytesCardinalityCap = 256

// gatherBytesStats performs the single streaming pass over the present values of a bytes column.
func gatherBytesStats(values [][]byte, present []bool, nRows int) bytesStats {
	st := bytesStats{uniformLen: -1, commonPrefixLen: -1}

	// Open-addressed fingerprint set for the cap-N distinct estimator. Sized 2× the cap so the
	// load factor stays low and probing is short. Zero is a sentinel for "empty slot"; the rare
	// genuine zero-fingerprint value is folded into the +1 below.
	const slots = bytesCardinalityCap * 2
	var fps [slots]uint64
	var hasZeroFP bool

	var first []byte
	for i := range nRows {
		if i >= len(present) || !present[i] {
			continue
		}
		var v []byte
		if i < len(values) {
			v = values[i]
		}
		st.presentCount++
		st.totalBytes += len(v)

		// Uniform length tracking.
		if st.presentCount == 1 {
			st.uniformLen = len(v)
		} else if st.uniformLen != len(v) {
			st.uniformLen = -1
		}

		// Common prefix across all present values, folded against the first present value.
		if st.commonPrefixLen < 0 {
			st.commonPrefixLen = len(v)
			first = v
		} else {
			st.commonPrefixLen = sharedPrefixLen(first, v, st.commonPrefixLen)
		}

		// XOR payload proxy: the format stores len(v) bytes of XOR result per row (leading
		// shared bytes become zero but are still emitted; the outer snappy squashes them). Using
		// len(v) keeps the estimate conservative and comparable across encodings.
		st.xorPayloadBytes += len(v)

		// Cap-N distinct fingerprint counting.
		if !st.distinctSat {
			fp := fnv64(v)
			if fp == 0 {
				if !hasZeroFP {
					hasZeroFP = true
					st.distinctCount++
				}
			} else if insertFingerprint(fps[:], fp) {
				st.distinctCount++
				if st.distinctCount >= bytesCardinalityCap {
					st.distinctSat = true
				}
			}
		}
	}

	if st.commonPrefixLen < 0 {
		st.commonPrefixLen = 0
	}
	return st
}

// sharedPrefixLen returns min(limit, longest common byte prefix of a and b).
func sharedPrefixLen(a, b []byte, limit int) int {
	n := limit
	if len(a) < n {
		n = len(a)
	}
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// fnv64 is a tiny FNV-1a hash used only for the cap-N distinct fingerprint estimator.
func fnv64(b []byte) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// insertFingerprint inserts fp into the open-addressed set and reports whether it was new.
func insertFingerprint(fps []uint64, fp uint64) bool {
	n := uint64(len(fps))
	idx := fp % n
	for {
		switch fps[idx] {
		case 0:
			fps[idx] = fp
			return true
		case fp:
			return false
		}
		idx++
		if idx == n {
			idx = 0
		}
	}
}

// --- cost estimators (estimated wire bytes per candidate family) ---
//
// Each estimator returns an approximate serialized byte count for the family on the gathered
// stats. They share a common presence/header overhead so only the relative ordering matters; the
// estimates are deliberately simple (single pass already done) and need only rank correctly.

// estimateDictBytesCost: dict payload (each distinct value stored once with a len prefix) + one
// index per present row. Index width grows with distinct count.
func estimateDictBytesCost(st bytesStats) int {
	distinct := st.distinctCount
	if st.distinctSat {
		// Saturated estimator: assume distinct ≈ presentCount (worst case for dict — no dedup).
		distinct = st.presentCount
	}
	if distinct < 1 {
		distinct = 1
	}
	// Average value length, used to estimate the dict payload when values are de-duplicated.
	avgLen := 0
	if st.presentCount > 0 {
		avgLen = st.totalBytes / st.presentCount
	}
	dictPayload := distinct * (4 + avgLen) // len[4] + bytes per distinct entry
	indexWidth := int(pickIndexWidth(distinct))
	indexBytes := st.presentCount * indexWidth
	return dictPayload + indexBytes
}

// estimateXORBytesCost: per-row len[4] + xor payload (≈ raw bytes; outer snappy squashes the zero
// runs that XOR creates). The uniform variant drops the per-row len[4], so when uniformLen > 0 we
// charge a single shared length header instead.
func estimateXORBytesCost(st bytesStats) int {
	if st.uniformLen > 0 && st.presentCount > 1 {
		return 4 + st.xorPayloadBytes // single uniform_len[4] + payload
	}
	return st.presentCount*4 + st.xorPayloadBytes
}

// estimatePrefixBytesCost: shared prefix stored once, each row stores prefix_idx + suffix. The win
// scales with commonPrefixLen; with no shared prefix this degrades to dict-of-prefixes overhead.
func estimatePrefixBytesCost(st bytesStats) int {
	if st.presentCount == 0 {
		return 0
	}
	prefixDict := 4 + st.commonPrefixLen // one shared prefix entry: len[4] + prefix bytes
	// Each present row: prefix_idx[1] + suffix_len[4] + suffix bytes (suffix = value minus prefix).
	suffixBytes := st.totalBytes - st.presentCount*st.commonPrefixLen
	if suffixBytes < 0 {
		suffixBytes = 0
	}
	perRow := st.presentCount * (1 + 4)
	return prefixDict + perRow + suffixBytes
}

// pickBytesEncoding selects the dense bytes encoding family for a column.
//
// Tier 1: semantic override (intrinsic columns only). Tier 2: cheapest cost estimate, with the
// demoted name-suffix hint breaking near-ties (within bytesCostTiebreakFraction).
func pickBytesEncoding(name string, st bytesStats) bytesEncoding {
	// Tier 1 — semantic overrides for intrinsic columns.
	switch shared.SemanticBytesOverride(name) {
	case shared.SemanticBytesDeltaDictionary:
		return bytesEncDeltaDictionary
	case shared.SemanticBytesXOR:
		return bytesEncXOR
	case shared.SemanticBytesPrefix:
		return bytesEncPrefix
	case shared.SemanticBytesNone:
		// fall through to cost-based selection
	}

	// Tier 2 — cost-based selection over the candidate families.
	//
	// DeltaDictionary is intentionally NOT a cost-based candidate: its only advantage over plain
	// Dictionary is a delta-coded index stream, which shrinks bytes only when both the dictionary
	// is sorted AND the per-row indexes are clustered (the sorted-trace-ID case). That property
	// can't be established cheaply from the streaming stats, and assuming it (always crediting a
	// smaller index stream) makes DeltaDictionary spuriously beat Dictionary on unsorted
	// low-cardinality columns. It is therefore reachable only via the semantic-override table,
	// where the sortedness is known a-priori.
	type cand struct {
		enc   bytesEncoding
		bytes int
	}
	candidates := []cand{
		{bytesEncDictionary, estimateDictBytesCost(st)},
		{bytesEncXOR, estimateXORBytesCost(st)},
		{bytesEncPrefix, estimatePrefixBytesCost(st)},
	}

	best := candidates[0]
	second := cand{bytes: maxIntSentinel}
	for _, c := range candidates[1:] {
		if c.bytes < best.bytes {
			second = best
			best = c
		} else if c.bytes < second.bytes {
			second = c
		}
	}

	// Demoted name-suffix tiebreak: only when the runner-up is within the tiebreak fraction.
	if second.bytes != maxIntSentinel && best.bytes > 0 {
		gap := float64(second.bytes-best.bytes) / float64(best.bytes)
		if gap <= bytesCostTiebreakFraction {
			if hint, ok := nameSuffixHint(name); ok {
				if hint == best.enc || hint == second.enc {
					return hint
				}
			}
		}
	}

	return best.enc
}

const maxIntSentinel = int(^uint(0) >> 1)

// nameSuffixHint maps the demoted name-suffix heuristics to a candidate family. It is consulted
// only as a near-tie tiebreak (never as a primary decision). Returns ok=false for names with no
// hint.
func nameSuffixHint(name string) (bytesEncoding, bool) {
	if isIDColumn(name) {
		return bytesEncXOR, true
	}
	if isURLColumn(name) {
		return bytesEncPrefix, true
	}
	return bytesEncDictionary, false
}
