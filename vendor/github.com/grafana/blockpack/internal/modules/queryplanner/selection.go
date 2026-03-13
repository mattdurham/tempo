package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

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
// Composite LogicalOR: union of constrained children; unconstrained (nil) children are
// skipped — they contribute no pruning, but they do not invalidate other children.
// Returns nil only when ALL children are unconstrained (NOTE-012).
// Composite LogicalAND: intersection of children; unconstrained children are skipped
// (conservative — cannot prune what cannot be indexed).
func blockSetForPred(r BlockIndexer, pred Predicate) (map[int]struct{}, error) {
	if len(pred.Children) == 0 {
		return leafBlockSet(r, pred)
	}

	if pred.Op == LogicalOR {
		// Union children. Skip unconstrained children (nil set) — an OR child with no
		// range index simply cannot be pruned, but other constrained children still can.
		// If ALL children are unconstrained, the entire OR is unconstrained.
		var union map[int]struct{}
		anyConstrained := false
		for _, child := range pred.Children {
			set, err := blockSetForPred(r, child)
			if err != nil {
				return nil, err
			}
			if set == nil {
				continue // skip unconstrained child
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
			return nil, nil // all children unconstrained → OR is unconstrained
		}
		if union == nil {
			union = make(map[int]struct{}) // all constrained children found nothing
		}
		return union, nil
	}

	// LogicalAND: intersect children, skipping unconstrained ones.
	var result map[int]struct{}
	for _, child := range pred.Children {
		set, err := blockSetForPred(r, child)
		if err != nil {
			return nil, err
		}
		if set == nil {
			continue // unconstrained AND child — skip conservatively
		}
		if result == nil {
			result = set
			continue
		}
		for b := range result {
			if _, ok := set[b]; !ok {
				delete(result, b)
			}
		}
	}
	return result, nil // nil when no indexed children
}

// pruneByIndexAll evaluates the top-level predicates (AND-combined) via range index
// and removes candidates that cannot satisfy the combined constraint.
func pruneByIndexAll(r BlockIndexer, candidates blockSet, predicates []Predicate) (int, error) {
	// Top-level predicates are AND-combined: intersect all constrained block sets.
	var result map[int]struct{}
	constrained := false
	for _, pred := range predicates {
		set, err := blockSetForPred(r, pred)
		if err != nil {
			return 0, err
		}
		if set == nil {
			continue // unconstrained predicate — skip
		}
		if !constrained {
			result = set
			constrained = true
			continue
		}
		for b := range result {
			if _, ok := set[b]; !ok {
				delete(result, b)
			}
		}
	}
	if !constrained {
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
