package executor

// NOTE-099: sorted-slice BlockRef merge operations — no map allocations.
// See NOTES.md §NOTE-099 for design rationale and measured impact.

import (
	"slices"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// packRef packs a BlockRef into a single uint32 for O(1) comparison.
// BlockIdx occupies the high 16 bits; RowIdx occupies the low 16 bits.
// The ordering is BlockIdx-major, RowIdx-minor — matching the write order
// from the intrinsic accumulator (blocks are written in ascending index order).
func packRef(ref modules_shared.BlockRef) uint32 {
	return uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
}

// unpackRef unpacks a uint32 produced by packRef back into a BlockRef.
func unpackRef(packed uint32) modules_shared.BlockRef {
	return modules_shared.BlockRef{
		BlockIdx: uint16(packed >> 16), //nolint:gosec // safe: packed>>16 is in [0, 0xFFFF]
		RowIdx:   uint16(packed),       //nolint:gosec // safe: low 16 bits always in [0, 0xFFFF]
	}
}

// isSortedRefs reports whether refs is sorted in non-decreasing packed-ref order.
// Returns true for nil and empty slices.
func isSortedRefs(refs []modules_shared.BlockRef) bool {
	for i := 1; i < len(refs); i++ {
		if packRef(refs[i]) < packRef(refs[i-1]) {
			return false
		}
	}
	return true
}

// sortRefs sorts refs in-place in ascending packed-ref order.
// Uses slices.SortFunc; safe on nil and empty inputs.
func sortRefs(refs []modules_shared.BlockRef) {
	slices.SortFunc(refs, func(a, b modules_shared.BlockRef) int {
		pa, pb := packRef(a), packRef(b)
		if pa < pb {
			return -1
		}
		if pa > pb {
			return 1
		}
		return 0
	})
}

// unionSortedRefs merges two sorted BlockRef slices, deduplicating by packed value.
// Both inputs must be sorted in ascending packed-ref order (see sortRefs).
// Safe on nil and empty inputs.
func unionSortedRefs(a, b []modules_shared.BlockRef) []modules_shared.BlockRef {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	out := make([]modules_shared.BlockRef, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		pa := packRef(a[i])
		pb := packRef(b[j])
		switch {
		case pa < pb:
			out = append(out, a[i])
			i++
		case pa > pb:
			out = append(out, b[j])
			j++
		default:
			// Equal: emit once, advance both.
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// intersectTwoSortedRefs intersects two sorted BlockRef slices via two-pointer walk.
// Result length is bounded by min(len(a), len(b)).
// Both inputs must be sorted in ascending packed-ref order.
func intersectTwoSortedRefs(a, b []modules_shared.BlockRef, limit int) []modules_shared.BlockRef {
	if len(a) == 0 || len(b) == 0 {
		return []modules_shared.BlockRef{}
	}

	capHint := len(a)
	if len(b) < capHint {
		capHint = len(b)
	}
	if limit > 0 && limit < capHint {
		capHint = limit
	}
	out := make([]modules_shared.BlockRef, 0, capHint)

	i, j := 0, 0
	for i < len(a) && j < len(b) {
		pa := packRef(a[i])
		pb := packRef(b[j])
		switch {
		case pa < pb:
			i++
		case pa > pb:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
			if limit > 0 && len(out) >= limit {
				return out
			}
		}
	}
	return out
}

// intersectSortedRefSets intersects multiple sorted BlockRef slices, returning only
// refs present in all sets. Applies smallest-first strategy; each set must be sorted
// in ascending packed-ref order. Returns nil for zero sets, the single set (optionally
// truncated) for one set. limit=0 means no limit; limit<0 returns nil.
func intersectSortedRefSets(sets [][]modules_shared.BlockRef, limit int) []modules_shared.BlockRef {
	if limit < 0 {
		return nil
	}
	switch len(sets) {
	case 0:
		return nil
	case 1:
		if limit > 0 && len(sets[0]) > limit {
			return sets[0][:limit]
		}
		return sets[0]
	}

	// Sort sets by length ascending so we start from the smallest candidate set.
	slices.SortFunc(sets, func(a, b []modules_shared.BlockRef) int {
		return len(a) - len(b)
	})

	// Pairwise intersect: acc starts as the smallest set, intersected with each subsequent.
	acc := sets[0]
	for _, next := range sets[1:] {
		if len(acc) == 0 {
			return []modules_shared.BlockRef{}
		}
		// Pass limit only on the final intersection to avoid early truncation.
		acc = intersectTwoSortedRefs(acc, next, 0)
	}

	if limit > 0 && len(acc) > limit {
		return acc[:limit]
	}
	return acc
}
