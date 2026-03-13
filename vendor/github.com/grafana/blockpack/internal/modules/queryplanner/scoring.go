package queryplanner

// NOTE: Fuse/CMS block pruning for the queryplanner.
// Stage 2b: pruneByFuseAll  — BinaryFuse8 membership filter (SPEC-SK-12, SPEC-SK-16)
// Stage 3b: pruneByCMSAll   — Count-Min Sketch zero-estimate prune
// NOTE-015: Fuse before CMS — fuse gives hard binary exclusion at 0.39% FPR.
// NOTE-013: CMS zero means definitely absent (no false negatives for zero).

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

// pruneByCMSAll removes candidates that fail the CMS check for ANY top-level predicate.
// Top-level predicates are AND-combined: a block must pass ALL predicates to remain a candidate.
//
// A CMS estimate of 0 means the value was definitely never added to that block.
// CMS never under-counts (SPEC-SK-08), so Estimate==0 is a safe prune.
//
// Each leaf predicate fetches the full CMSEstimate slice once per value — bulk scan.
// NOTE-013: Skips IntervalMatch predicates and non-single-column predicates.
// Returns the count of pruned blocks.
func pruneByCMSAll(r BlockIndexer, candidates blockSet, predicates []Predicate) int {
	if len(predicates) == 0 {
		return 0
	}
	// AND semantics: a block must pass ALL top-level predicates to be kept.
	saved := make(blockSet, len(candidates))
	candidates.iter(func(blockIdx int) { saved.set(blockIdx) })
	for _, pred := range predicates {
		predSaved := make(blockSet, len(candidates))
		pruneByCMSPred(r, candidates, predSaved, pred)
		saved.and(predSaved)
	}

	pruned := 0
	candidates.iter(func(blockIdx int) {
		if !saved.test(blockIdx) {
			candidates.clear(blockIdx)
			pruned++
		}
	})
	return pruned
}

// pruneByCMSPred marks blocks in saved[] that pass the CMS check for pred.
func pruneByCMSPred(r BlockIndexer, candidates, saved blockSet, pred Predicate) {
	if len(pred.Children) > 0 {
		if pred.Op == LogicalOR {
			for _, child := range pred.Children {
				pruneByCMSPred(r, candidates, saved, child)
			}
			return
		}
		// LogicalAND
		andSaved := make(blockSet, len(candidates))
		candidates.iter(func(blockIdx int) { andSaved.set(blockIdx) })
		for _, child := range pred.Children {
			childSaved := make(blockSet, len(candidates))
			pruneByCMSPred(r, candidates, childSaved, child)
			andSaved.and(childSaved)
		}
		for i, w := range andSaved {
			saved[i] |= w
		}
		return
	}

	// Leaf node.
	if pred.IntervalMatch || len(pred.Values) == 0 || len(pred.Columns) != 1 {
		candidates.iter(func(blockIdx int) { saved.set(blockIdx) })
		return
	}
	col := pred.Columns[0]
	cs := r.ColumnSketch(col)
	if cs == nil {
		candidates.iter(func(blockIdx int) { saved.set(blockIdx) })
		return
	}

	// Fetch the full estimate slice once per value; scan candidates.
	// Block passes if ANY queried value has a non-zero estimate.
	for _, val := range pred.Values {
		estimates := cs.CMSEstimate(val)
		candidates.iter(func(blockIdx int) {
			if blockIdx < len(estimates) && estimates[blockIdx] > 0 {
				saved.set(blockIdx)
			}
		})
	}
}

