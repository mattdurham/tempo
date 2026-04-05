package queryplanner

// NOTE: Bloom block pruning and HLL-based block scoring for the queryplanner.
// Stage 2b: pruneByFuseAll  — SketchBloom membership filter (SPEC-SK-12, SPEC-SK-16)
// Stage 3:  scoreBlocks     — freq/max(cardinality,1) selectivity scoring
// NOTE-015: Bloom gives hard binary exclusion at low FPR.
// NOTE-014: Score = freq / max(cardinality, 1); higher = more selective block.

import (
	"github.com/grafana/blockpack/internal/modules/sketch"
)

// pruneByFuseAll removes candidates that fail the BinaryFuse8 check for ANY top-level predicate.
// Top-level predicates are AND-combined: a block must pass ALL predicates to remain a candidate.
// A block fails a predicate when the fuse filter definitively excludes all queried values.
//
// Each leaf predicate fetches the full FuseContains slice once per value, then iterates
// over candidate blocks — O(predicates × values × blocks) comparisons, not allocations.
//
// NOTE-015: Skips IntervalMatch predicates (range queries need all blocks in range).
// Returns the count of pruned blocks.
func pruneByFuseAll(r BlockIndexer, candidates blockSet, predicates []Predicate) int {
	if len(predicates) == 0 {
		return 0
	}
	// AND semantics: a block must pass ALL top-level predicates to be kept.
	// Start with all candidates saved, then intersect with each predicate's passing set.
	saved := make(blockSet, len(candidates))
	candidates.iter(func(blockIdx int) { saved.set(blockIdx) })
	for _, pred := range predicates {
		predSaved := make(blockSet, len(candidates))
		pruneByFusePred(r, candidates, predSaved, pred)
		saved.and(predSaved)
	}

	// Prune blocks that do not pass all predicates.
	pruned := 0
	candidates.iter(func(blockIdx int) {
		if !saved.test(blockIdx) {
			candidates.clear(blockIdx)
			pruned++
		}
	})
	return pruned
}

// pruneByFusePred marks blocks in saved[] that pass the fuse check for pred.
// A block passes if pred returns true for that block index.
func pruneByFusePred(r BlockIndexer, candidates, saved blockSet, pred Predicate) {
	if len(pred.Children) > 0 {
		if pred.Op == LogicalOR {
			// OR: block passes if any child passes — merge all children's passing sets.
			for _, child := range pred.Children {
				pruneByFusePred(r, candidates, saved, child)
			}
			return
		}
		// LogicalAND: block passes only if ALL children pass.
		// Start with candidate set for this AND subtree; intersect with each child.
		andSaved := make(blockSet, len(candidates))
		candidates.iter(func(blockIdx int) { andSaved.set(blockIdx) })
		for _, child := range pred.Children {
			childSaved := make(blockSet, len(candidates))
			pruneByFusePred(r, candidates, childSaved, child)
			andSaved.and(childSaved)
		}
		// Merge andSaved into the outer saved.
		for i, w := range andSaved {
			saved[i] |= w
		}
		return
	}

	// Leaf node.
	if pred.IntervalMatch || len(pred.Values) == 0 || len(pred.Columns) != 1 {
		// Conservative: interval and non-single-column predicates cannot prune — mark all passing.
		candidates.iter(func(blockIdx int) { saved.set(blockIdx) })
		return
	}
	col := pred.Columns[0]
	cs := r.ColumnSketch(col)
	if cs == nil {
		// No sketch data — conservative pass for all candidates.
		candidates.iter(func(blockIdx int) { saved.set(blockIdx) })
		return
	}

	// Fetch the full fuse result slice once per value; scan candidates.
	// Block passes if ANY queried value is possibly present.
	for _, val := range pred.Values {
		h := sketch.HashForFuse(val)
		fuseResults := cs.FuseContains(h)
		candidates.iter(func(blockIdx int) {
			if blockIdx < len(fuseResults) && fuseResults[blockIdx] {
				saved.set(blockIdx)
			}
		})
	}
}

// scoreBlocks computes a selectivity score for each candidate block based on
// HLL cardinality and TopK frequency counts.
//
// score = sum(freq_i) / max(cardinality, 1)
//
// Higher score means more selective (fewer distinct values relative to frequency).
// Only populated when sketch data is available; returns nil when no predicates or
// no sketch data.
// NOTE-014: score = freq/max(card,1).
func scoreBlocks(r BlockIndexer, candidates blockSet, predicates []Predicate) map[int]float64 {
	if len(predicates) == 0 || candidates.count() == 0 {
		return nil
	}
	scores := make(map[int]float64, candidates.count())
	anyScored := false
	for _, pred := range predicates {
		scoreBlocksForPred(r, candidates, pred, scores)
	}
	for _, v := range scores {
		if v > 0 {
			anyScored = true
			break
		}
	}
	if !anyScored {
		return nil
	}
	return scores
}

// scoreBlocksForPred accumulates score contributions from one predicate into scores[].
// Uses bulk slice fetches: Distinct(), TopKMatch() called once per value.
func scoreBlocksForPred(r BlockIndexer, candidates blockSet, pred Predicate, scores map[int]float64) {
	if len(pred.Children) > 0 {
		for _, child := range pred.Children {
			scoreBlocksForPred(r, candidates, child, scores)
		}
		return
	}

	// Leaf node.
	if len(pred.Values) == 0 || len(pred.Columns) != 1 {
		return
	}
	col := pred.Columns[0]
	cs := r.ColumnSketch(col)
	if cs == nil {
		return
	}

	distinct := cs.Distinct()

	for _, val := range pred.Values {
		// Try TopK (Space-Saving approximate upper-bound count via FP lookup) — bulk fetch.
		valFP := sketch.HashForFuse(val)
		topkCounts := cs.TopKMatch(valFP)

		candidates.iter(func(blockIdx int) {
			if blockIdx >= len(distinct) {
				return
			}
			card := float64(distinct[blockIdx])
			if card < 1 {
				card = 1
			}
			var freq float64
			if blockIdx < len(topkCounts) && topkCounts[blockIdx] > 0 {
				freq = float64(topkCounts[blockIdx])
			}
			if freq > 0 {
				scores[blockIdx] += freq / card
			}
		})
	}
}
