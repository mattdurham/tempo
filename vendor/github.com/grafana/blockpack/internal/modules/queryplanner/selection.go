package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "sort"

// insertionSortBlockSets sorts sets ascending by popcount.
// Uses insertion sort for n ≤ 8 (avoids sort.Slice closure allocation, faster for small N).
// Falls back to sort.Slice for n > 8 to avoid O(n²) cost on deep predicate trees.
// NOTE-024: insertion sort for small N (≤8); sort.Slice fallback for large N (>8).
func insertionSortBlockSets(sets []blockSet) {
	if len(sets) <= 1 {
		return
	}
	if len(sets) > 8 {
		// Fall back to sort.Slice for larger inputs to avoid O(n²) cost.
		sort.Slice(sets, func(i, j int) bool {
			return sets[i].count() < sets[j].count()
		})
		return
	}
	// Insertion sort for small n (≤8): no closure heap escape.
	for i := 1; i < len(sets); i++ {
		key := sets[i]
		keyCount := key.count()
		j := i - 1
		for j >= 0 && sets[j].count() > keyCount {
			sets[j+1] = sets[j]
			j--
		}
		sets[j+1] = key
	}
}

// intersectBySelectivity intersects a slice of (blockSet, constrained) pairs in selectivity
// order (smallest popcount first) to minimize intermediate result sizes.
//
// Returns (nil, false) when sets is empty (no constrained predicates — no pruning possible).
// Returns (intersection, true) when at least one constrained set is present.
//
// The input blockSet values are never modified, but the order of elements in sets may be
// permuted by the internal selectivity sort. Callers must not rely on the slice order
// being preserved after this call.
//
// NOTE-021: blockSet replaces map[int]struct{} for allocation reduction. The nil/false
// = unconstrained / non-nil true = constrained invariant is preserved via the bool return.
func intersectBySelectivity(sets []blockSet, blockCount int) (blockSet, bool) {
	if len(sets) == 0 {
		return nil, false
	}
	// Defensive: a nil blockSet with constrained=true should not occur in normal
	// operation, but guard against it to avoid silently zeroing the result via AND.
	// Note: a non-nil zero-length blockSet IS a valid constrained-empty result
	// (all-zero bits = index consulted, no blocks matched) and must NOT be skipped.
	filtered := sets[:0]
	for _, s := range sets {
		if s != nil {
			filtered = append(filtered, s)
		}
	}
	if len(filtered) == 0 {
		return nil, false
	}
	sets = filtered
	// Sort ascending by popcount: smallest (most selective) first.
	// NOTE-024: insertion sort replaces sort.Slice — avoids closure allocation for small N (≤8).
	insertionSortBlockSets(sets)
	// Copy sets[0] so we never mutate caller-owned sets.
	words := (blockCount + 63) / 64
	result := make(blockSet, words)
	copy(result, sets[0])
	for _, s := range sets[1:] {
		result.and(s)
		if result.count() == 0 {
			break // already empty — no further intersections can add blocks
		}
	}
	return result, true
}

// leafBlockSet returns the set of block indices that the range index indicates may
// satisfy a leaf predicate.
//
// Returns (nil, false) when the predicate cannot be evaluated against the range index:
// no Values, not exactly one column, or the column has no range index. Callers treat
// (nil, false) as "no index coverage — skip this predicate conservatively".
//
// Returns (set, true) when the index was consulted. An all-zero set means the index
// was queried and found no matching blocks — all candidates should be pruned for an AND predicate.
//
// NOTE-011: Interval match for case-insensitive regex prefix lookups.
// NOTE-021: blockSet variant; bool=false means unconstrained (replaces nil-map convention).
func leafBlockSet(r BlockIndexer, pred Predicate, blockCount int) (blockSet, bool, error) {
	if len(pred.Values) == 0 || len(pred.Columns) != 1 {
		return nil, false, nil // not indexable
	}
	col := pred.Columns[0]
	if _, ok := r.RangeColumnType(col); !ok {
		return nil, false, nil // no range index for this column
	}
	// blockCount==0: no blocks to constrain — return unconstrained rather than
	// a constrained-empty set. pruneByIndexAll is never called with an empty
	// candidate set, so this only affects explainLeaf on an empty file (harmless).
	if blockCount == 0 {
		return nil, false, nil
	}

	words := (blockCount + 63) / 64

	if pred.IntervalMatch {
		if len(pred.Values) != 2 {
			return nil, false, nil
		}
		blocks, err := r.BlocksForRangeInterval(col, pred.Values[0], pred.Values[1])
		if err != nil {
			return nil, false, err
		}
		// Return constrained=true (even when all-zero) — index was consulted, no matches found.
		result := make(blockSet, words)
		for _, b := range blocks {
			// b >= blockCount indicates a block index beyond the file's block count —
			// stale or corrupt range index entry; skip silently rather than panicking.
			// The caller's bloom/time filters provide a second defense.
			if b < blockCount {
				result.set(b)
			}
		}
		return result, true, nil
	}

	// Union all block sets for each query value.
	// Return constrained=true even when all-zero — index was consulted, no blocks matched any value.
	result := make(blockSet, words)
	for _, val := range pred.Values {
		blocks, err := r.BlocksForRange(col, val)
		if err != nil {
			return nil, false, err
		}
		for _, b := range blocks {
			// b >= blockCount indicates a block index beyond the file's block count —
			// stale or corrupt range index entry; skip silently rather than panicking.
			// The caller's bloom/time filters provide a second defense.
			if b < blockCount {
				result.set(b)
			}
		}
	}
	return result, true, nil
}

// blockSetForPred returns the block set for a predicate tree node.
// Returns (nil, false) when the node is unconstrained (no index coverage).
//
// Leaf: delegates to leafBlockSet.
// Composite LogicalOR: union of constrained children; unconstrained (false) children
// are skipped — they represent columns with no range index (absent from the file
// entirely per the writer invariant). Returns (nil, false) only when ALL children are
// unconstrained. See NOTE-012.
//
// Composite LogicalAND: intersection of children; unconstrained children are skipped
// (conservative — cannot prune what cannot be indexed).
func blockSetForPred(r BlockIndexer, pred Predicate, blockCount int) (blockSet, bool, error) {
	if len(pred.Children) == 0 {
		return leafBlockSet(r, pred, blockCount)
	}

	if pred.Op == LogicalOR {
		// NOTE-012: OR skip-nil semantics. Union constrained children, skip
		// unconstrained (false) ones. A false child means the column has no range
		// index, which (per writer invariant) means it is absent from the file
		// entirely — no block can satisfy that scope, so skipping is safe.
		// Returns (nil, false) only when ALL children are unconstrained.
		words := (blockCount + 63) / 64
		var union blockSet
		anyConstrained := false
		for _, child := range pred.Children {
			set, constrained, err := blockSetForPred(r, child, blockCount)
			if err != nil {
				return nil, false, err
			}
			if !constrained {
				continue // unconstrained child — column absent from file, skip
			}
			anyConstrained = true
			if union == nil {
				union = make(blockSet, words)
			}
			union.or(set)
		}
		if !anyConstrained {
			return nil, false, nil // all children unconstrained — no pruning possible
		}
		if union == nil {
			union = make(blockSet, words) // all constrained children found nothing
		}
		return union, true, nil
	}

	// LogicalAND: collect constrained children then intersect in selectivity order
	// (smallest popcount first) so that the most selective predicate prunes first.
	// See NOTE-019, NOTE-021.
	// NOTE-024: stack pre-alloc for ≤8 constrained children (typical case).
	var setsArr [8]blockSet
	sets := setsArr[:0]
	for _, child := range pred.Children {
		set, constrained, err := blockSetForPred(r, child, blockCount)
		if err != nil {
			return nil, false, err
		}
		if !constrained {
			continue // unconstrained AND child — skip conservatively
		}
		sets = append(sets, set)
	}
	result, constrained := intersectBySelectivity(sets, blockCount)
	return result, constrained, nil
}

// pruneByIndexAll evaluates the top-level predicates (AND-combined) via range index
// and removes candidates that cannot satisfy the combined constraint.
// blockCount is the actual number of blocks in the file (from BlockIndexer.BlockCount()),
// not the bitset capacity (candidates.numBlocks() returns len*64, which rounds up to
// the nearest 64 and is semantically incorrect as an "actual block count").
func pruneByIndexAll(r BlockIndexer, candidates blockSet, predicates []Predicate, blockCount int) (int, error) {
	// Collect constrained block sets for all predicates, then intersect in selectivity
	// order (smallest popcount first) so that the most selective predicate prunes first.
	// See NOTE-019, NOTE-021.
	// NOTE-024: stack pre-alloc for ≤8 constrained predicates (typical case),
	// eliminating the backing-array alloc from the first append.
	var setsArr [8]blockSet
	sets := setsArr[:0]
	for _, pred := range predicates {
		set, constrained, err := blockSetForPred(r, pred, blockCount)
		if err != nil {
			return 0, err
		}
		if !constrained {
			continue // unconstrained predicate — skip
		}
		sets = append(sets, set)
	}
	result, constrained := intersectBySelectivity(sets, blockCount)
	if !constrained {
		return 0, nil
	}
	pruned := 0
	candidates.iter(func(b int) {
		if !result.test(b) {
			candidates.clear(b)
			pruned++
		}
	})
	return pruned, nil
}
