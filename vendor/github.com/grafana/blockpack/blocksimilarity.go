package blockpack

// NOTE-463: Compaction similarity scoring from intrinsic ToC dict value sets (issue #382).
//
// The blockpack sort key is (resource.service.name, span:name, minHash). When blocks that
// share a large overlap in those two value sets are compacted together, the output has
// denser runs of identical strings — better dictionary encoding, better snappy compression,
// fewer distinct dict values per output block (smaller bloom filters, faster pruning).
//
// This file exposes a cheap, ToC-only way to read each candidate block's service/span-name
// value sets and a pure Jaccard similarity score over them, so a compaction scheduler can
// group similar blocks and feed the most-similar block FIRST (establishing the writer's
// dictionary and sort baseline from the dominant data pattern). It is provider-ordering
// advice only: CompactBlocksStreaming already consumes providers in slice order, so the
// scheduler reorders its provider slice using these scores.

import (
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// BlockValueSets holds the distinct string value sets of the two intrinsic columns that
// drive the blockpack sort key and compression density: resource.service.name and span:name.
// Either set may be nil (column absent or not a paged Dict column in that block); a nil set
// contributes zero overlap and zero union to similarity, i.e. "no information" rather than
// "empty match".
type BlockValueSets struct {
	Services  map[string]struct{}
	SpanNames map[string]struct{}
}

// ReadBlockValueSets reads the resource.service.name and span:name distinct value sets from
// a block's intrinsic ToC, decoding only the two column blobs (cheap ranged GETs, no full
// block download and no ref decode). Use a lean reader (NewLeanReaderFromProvider /
// NewLeanReaderWithCache) for the lowest-I/O path: only the footer + section directory are
// read on open, and each column blob is fetched lazily here.
//
// A column that is absent or not Dict-encoded yields a nil set for that field; the function
// only returns an error on an actual decode/read failure. A block with neither column present
// returns (BlockValueSets{}, nil).
func ReadBlockValueSets(r *Reader) (BlockValueSets, error) {
	// After #433 (IntrinsicTOC removal), service.name and span:name live in block columns.
	var svc, names map[string]struct{}
	for bi := range r.BlockCount() {
		bwb, err := r.GetBlockWithBytes(bi, nil)
		if err != nil || bwb == nil {
			continue
		}
		block := bwb.Block
		if col := block.GetColumn(modules_shared.SvcNameColumnName); col != nil {
			if svc == nil {
				svc = make(map[string]struct{})
			}
			for rowIdx := range block.SpanCount() {
				if v, ok := col.StringValue(rowIdx); ok && v != "" {
					svc[v] = struct{}{}
				}
			}
		}
		if col := block.GetColumn(modules_shared.SpanNameColumnName); col != nil {
			if names == nil {
				names = make(map[string]struct{})
			}
			for rowIdx := range block.SpanCount() {
				if v, ok := col.StringValue(rowIdx); ok && v != "" {
					names[v] = struct{}{}
				}
			}
		}
	}
	return BlockValueSets{Services: svc, SpanNames: names}, nil
}

// BlockSimilarity scores how similar two blocks' contents are for compaction grouping, as the
// sum of the Jaccard coefficient on their service-name sets and the Jaccard coefficient on
// their span-name sets:
//
//	similarity(A, B) = J(A.Services, B.Services) + J(A.SpanNames, B.SpanNames)
//
// The result is in [0, 2]: 2 means both value sets are identical, 0 means no overlap on
// either column. Higher means the merged block will have denser identical-string runs and
// compress better. A nil/empty set on either side contributes 0 for that column (no
// information), so a block whose ToC lacks both columns is maximally dissimilar to everything
// and naturally sorts to the back of any similarity-ordered group.
func BlockSimilarity(a, b BlockValueSets) float64 {
	return jaccard(a.Services, b.Services) + jaccard(a.SpanNames, b.SpanNames)
}

// jaccard returns |a ∩ b| / |a ∪ b| for two string sets, treating nil/empty as the empty set.
// Returns 0 when the union is empty (no information to compare) rather than NaN.
func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	// Iterate the smaller set against the larger for the intersection count.
	small, large := a, b
	if len(large) < len(small) {
		small, large = large, small
	}
	intersection := 0
	for v := range small {
		if _, ok := large[v]; ok {
			intersection++
		}
	}
	union := len(a) + len(b) - intersection
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}
