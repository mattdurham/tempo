package queryplan

// NOTE: VCNTDurationPerMinuteFunc is new, additive code for issue #499 Part 1 — mirrors
// VCNTPerMinuteFunc's (perminutefrom.go) relationship to VCNTCostFunc, but for the
// histogram-eligible Min/Max leaf shape vcnt_duration_cost.go recognizes instead of equality.
// Deliberately reuses durationRangeLeafShape/durationBetweenLeafShape directly (same package,
// unexported) rather than duplicating the leaf-shape extraction, per NOTE-QP-007's
// non-duplication principle.

import (
	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/vm"
)

// VCNTDurationPerMinuteFunc is VCNTPerMinuteFunc's range-predicate sibling for the
// histogram-eligible column (#499 Part 1, mirroring #205's VCNTDurationCostFunc). It closes over
// one decoded VCNT section and query window and returns a callback suitable for BuildQueryPlan's
// perMinuteForLead parameter: given a leaf, it recognizes exactly the single-bound/between leaf
// shapes durationRangeLeafShape/durationBetweenLeafShape extract, and answers via
// valuecounts.DurationHistogramPerMinuteInRange plus the SAME EstimateThreshold/EstimateBetween
// estimators VCNTDurationCostFunc uses — a thin adapter, not a second implementation.
//
// Returns nil for any leaf shape those extractors reject (equality, non-histogram column, or any
// other non-range shape) — never an error. A minute whose estimate is unknown or <= 0 is excluded
// from the returned slice entirely (mirrors VCNTPerMinuteFunc's own "no signal is not the same as
// a zero-count entry" liveness contract, applied per-minute).
//
// Performs no object-storage I/O: data/dir are already-decoded in-memory bytes, identical to
// VCNTDurationCostFunc's own inputs.
//
// SPEC-QP-11.
func VCNTDurationPerMinuteFunc(
	data []byte, dir []valuecounts.ChunkDirEntry, minTS, maxTS uint64,
) func(leaf *vm.RangeNode) []valuecounts.MinuteCount {
	return func(leaf *vm.RangeNode) []valuecounts.MinuteCount {
		var estimate func(valuecounts.DurationHistogram) (int64, bool)
		if lo, hi, ok := durationBetweenLeafShape(leaf); ok {
			estimate = func(h valuecounts.DurationHistogram) (int64, bool) { return h.EstimateBetween(lo, hi) }
		} else if op, threshold, ok := durationRangeLeafShape(leaf); ok {
			estimate = func(h valuecounts.DurationHistogram) (int64, bool) { return h.EstimateThreshold(op, threshold) }
		} else {
			return nil
		}
		perMinuteHist, err := valuecounts.DurationHistogramPerMinuteInRange(data, dir, leaf.Column, minTS, maxTS)
		if err != nil {
			return nil
		}
		out := make([]valuecounts.MinuteCount, 0, len(perMinuteHist))
		for _, mh := range perMinuteHist {
			count, known := estimate(mh.Histogram)
			if !known || count <= 0 {
				continue
			}
			out = append(out, valuecounts.MinuteCount{Minute: mh.Minute, Count: count})
		}
		return out
	}
}

// CombinePerMinuteFuncs mirrors CombineCostFuncs (vcnt_duration_cost.go) for the
// perMinuteForLead callback shape: tries first, falls through to second only when first returns
// nil/empty — never both, never merges. Equality vs. duration-range leaf shapes are mutually
// exclusive by construction (same guarantee CombineCostFuncs relies on), so this never
// double-counts a single leaf.
//
// SPEC-QP-11.
func CombinePerMinuteFuncs(
	first, second func(*vm.RangeNode) []valuecounts.MinuteCount,
) func(*vm.RangeNode) []valuecounts.MinuteCount {
	return func(leaf *vm.RangeNode) []valuecounts.MinuteCount {
		if out := first(leaf); len(out) > 0 {
			return out
		}
		return second(leaf)
	}
}
