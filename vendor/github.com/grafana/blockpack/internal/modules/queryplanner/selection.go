package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "sort"

// intersectBySelectivity intersects a slice of non-nil block sets in selectivity order
// (smallest set first) to minimise intermediate result sizes. The caller must filter out
// nil (unconstrained) sets before calling; nil maps in sets produce a panic.
//
// Returns nil when sets is empty (no constrained predicates → no pruning possible).
// Returns the intersection result, which may be a non-nil empty map when a constrained
// predicate matched no blocks (all candidates should be pruned for an AND predicate).
//
// NOTE-019: Intersecting in order of ascending set size (most selective first) minimises
// the work done at each step: starting from the smallest set means subsequent intersections
// iterate over fewer elements. For the common case of A && (B || C) where A is indexed on
// a selective column and OR(B,C) is wide, processing A first prunes the block list to a
// small set before OR(B,C) further intersects — rather than building the full OR union and
// then reducing it.
func intersectBySelectivity(sets []map[int]struct{}) map[int]struct{} {
	if len(sets) == 0 {
		return nil
	}
	// Sort ascending by size: smallest (most selective) first.
	sort.Slice(sets, func(i, j int) bool {
		return len(sets[i]) < len(sets[j])
	})
	result := sets[0]
	for _, set := range sets[1:] {
		for b := range result {
			if _, ok := set[b]; !ok {
				delete(result, b)
			}
		}
		if len(result) == 0 {
			break // already empty — no further intersections can add blocks
		}
	}
	return result
}

// leafBlockSet returns the set of block indices that the range index indicates may
// satisfy a leaf predicate.
//
// Returns nil (unconstrained) when the predicate cannot be evaluated against the range
// index: no Values, not exactly one column, or the column has no range index. Callers
// treat nil as "no index coverage — skip this predicate conservatively".
//
// Returns a non-nil (possibly empty) map when the index was consulted. An empty map
// means the index was queried and found no matching blocks — all candidates should be
// pruned for an AND predicate.
//
// NOTE-011: Interval match for case-insensitive regex prefix lookups.
func leafBlockSet(r BlockIndexer, pred Predicate) (map[int]struct{}, error) {
	if len(pred.Values) == 0 || len(pred.Columns) != 1 {
		return nil, nil // not indexable
	}
	col := pred.Columns[0]
	if _, ok := r.RangeColumnType(col); !ok {
		return nil, nil // no range index for this column
	}

	if pred.IntervalMatch {
		if len(pred.Values) != 2 {
			return nil, nil
		}
		blocks, err := r.BlocksForRangeInterval(col, pred.Values[0], pred.Values[1])
		if err != nil {
			return nil, err
		}
		// Return non-nil (even when empty) — index was consulted, no matches found.
		set := make(map[int]struct{}, len(blocks))
		for _, b := range blocks {
			set[b] = struct{}{}
		}
		return set, nil
	}

	// Union all block sets for each query value.
	// Return non-nil even when empty — index was consulted, no blocks matched any value.
	union := make(map[int]struct{})
	for _, val := range pred.Values {
		blocks, err := r.BlocksForRange(col, val)
		if err != nil {
			return nil, err
		}
		for _, b := range blocks {
			union[b] = struct{}{}
		}
	}
	return union, nil
}

// blockSetForPred returns the block set for a predicate tree node.
// Returns nil when the node is unconstrained (no index coverage).
//
// Leaf: delegates to leafBlockSet.
// Composite LogicalOR: union of constrained children; unconstrained (nil) children
// are skipped — they represent columns with no range index (absent from the file
// entirely per the writer invariant). Returns nil only when ALL children are
// unconstrained. See NOTE-012.
//
// Composite LogicalAND: intersection of children; unconstrained children are skipped
// (conservative — cannot prune what cannot be indexed).
func blockSetForPred(r BlockIndexer, pred Predicate) (map[int]struct{}, error) {
	if len(pred.Children) == 0 {
		return leafBlockSet(r, pred)
	}

	if pred.Op == LogicalOR {
		// NOTE-012: OR skip-nil semantics. Union constrained children, skip
		// unconstrained (nil) ones. A nil child means the column has no range
		// index, which (per writer invariant) means it is absent from the file
		// entirely — no block can satisfy that scope, so skipping is safe.
		// Returns nil only when ALL children are unconstrained.
		var union map[int]struct{}
		anyConstrained := false
		for _, child := range pred.Children {
			set, err := blockSetForPred(r, child)
			if err != nil {
				return nil, err
			}
			if set == nil {
				continue // unconstrained child — column absent from file, skip
			}
			anyConstrained = true
			for b := range set {
				if union == nil {
					union = make(map[int]struct{})
				}
				union[b] = struct{}{}
			}
		}
		if !anyConstrained {
			return nil, nil // all children unconstrained — no pruning possible
		}
		if union == nil {
			union = make(map[int]struct{}) // all constrained children found nothing
		}
		return union, nil
	}

	// LogicalAND: collect constrained children then intersect in selectivity order
	// (smallest set first) so that the most selective predicate prunes first.
	// See NOTE-019.
	var sets []map[int]struct{}
	for _, child := range pred.Children {
		set, err := blockSetForPred(r, child)
		if err != nil {
			return nil, err
		}
		if set == nil {
			continue // unconstrained AND child — skip conservatively
		}
		sets = append(sets, set)
	}
	return intersectBySelectivity(sets), nil // nil when no constrained children
}

// pruneByIndexAll evaluates the top-level predicates (AND-combined) via range index
// and removes candidates that cannot satisfy the combined constraint.
func pruneByIndexAll(r BlockIndexer, candidates blockSet, predicates []Predicate) (int, error) {
	// Collect constrained block sets for all predicates, then intersect in selectivity
	// order (smallest set first) so that the most selective predicate prunes first.
	// See NOTE-019.
	var sets []map[int]struct{}
	for _, pred := range predicates {
		set, err := blockSetForPred(r, pred)
		if err != nil {
			return 0, err
		}
		if set == nil {
			continue // unconstrained predicate — skip
		}
		sets = append(sets, set)
	}
	result := intersectBySelectivity(sets)
	if result == nil {
		return 0, nil
	}
	pruned := 0
	candidates.iter(func(b int) {
		if _, ok := result[b]; !ok {
			candidates.clear(b)
			pruned++
		}
	})
	return pruned, nil
}
