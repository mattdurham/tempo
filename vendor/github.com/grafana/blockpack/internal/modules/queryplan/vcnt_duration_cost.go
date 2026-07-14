package queryplan

import (
	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/vm"
)

// durationHistogramColumn is the sole histogram-eligible column Phase 1 recognizes (#205) — a
// single hardcoded string comparison, not a map, per the design's own "don't generalize
// speculatively" ruling. Extend to a small set/map if a second histogram-eligible column is
// ever added.
const durationHistogramColumn = "span:duration"

// VCNTDurationCostFunc is VCNTCostFunc's range-predicate sibling for histogram-eligible columns
// (#205). Recognizes exactly the leaf shapes leafEqualityValue rejects (Min and/or Max set,
// single column, no Values/Pattern/RequirePresent) on durationHistogramColumn, and answers via
// valuecounts.DurationHistogramInRange's EstimateThreshold/EstimateBetween. Every other leaf
// shape — including an equality leaf on the same column — returns UnknownCost(), which
// VCNTCostFunc remains free to recognize instead (mutual exclusivity by construction, SPEC-QP-9).
//
// Performs no object-storage I/O: data/dir are already-decoded in-memory bytes, identical to
// VCNTCostFunc's own inputs.
func VCNTDurationCostFunc(data []byte, dir []valuecounts.ChunkDirEntry, minTS, maxTS uint64) CostFunc {
	return func(leaf *vm.RangeNode) LeafCost {
		if loMillis, hiMillis, ok := durationBetweenLeafShape(leaf); ok {
			return durationHistogramCost(data, dir, leaf.Column, minTS, maxTS,
				func(h valuecounts.DurationHistogram) (int64, bool) {
					return h.EstimateBetween(loMillis, hiMillis)
				})
		}
		op, thresholdMillis, ok := durationRangeLeafShape(leaf)
		if !ok {
			return UnknownCost()
		}
		return durationHistogramCost(data, dir, leaf.Column, minTS, maxTS,
			func(h valuecounts.DurationHistogram) (int64, bool) {
				return h.EstimateThreshold(op, thresholdMillis)
			})
	}
}

// durationHistogramCost looks up column's duration histogram over [minTS, maxTS] and, if
// covered, applies estimate to it — the shared tail both of VCNTDurationCostFunc's branches
// (single-threshold and between) reduce to.
func durationHistogramCost(
	data []byte, dir []valuecounts.ChunkDirEntry, column string, minTS, maxTS uint64,
	estimate func(valuecounts.DurationHistogram) (int64, bool),
) LeafCost {
	h, err := valuecounts.DurationHistogramInRange(data, dir, column, minTS, maxTS)
	if err != nil || !h.Covered {
		return UnknownCost()
	}
	count, known := estimate(h)
	if !known {
		return UnknownCost()
	}
	return KnownCost(count)
}

// durationRangeLeafShape extracts (op, thresholdMillis, ok) from a single-bound (Min-XOR-Max)
// leaf on durationHistogramColumn — mirrors leafEqualityValue's gate, inverted: that function
// accepts exactly one Values entry and no Min/Max/Pattern/RequirePresent, this accepts exactly
// one of Min/Max set and no Values/Pattern/RequirePresent. A leaf with BOTH Min and Max set (a
// between leaf) is not this shape — see durationBetweenLeafShape.
func durationRangeLeafShape(n *vm.RangeNode) (op valuecounts.TimeCompareOp, thresholdMillis uint64, ok bool) {
	if !isDurationHistogramLeaf(n) {
		return 0, 0, false
	}
	switch {
	case n.Min != nil && n.Max == nil:
		millis, mok := durationValueMillis(n.Min)
		if !mok {
			return 0, 0, false
		}
		if n.MinInclusive {
			return valuecounts.OpGTE, millis, true
		}
		return valuecounts.OpGT, millis, true
	case n.Max != nil && n.Min == nil:
		millis, mok := durationValueMillis(n.Max)
		if !mok {
			return 0, 0, false
		}
		if n.MaxInclusive {
			return valuecounts.OpLTE, millis, true
		}
		return valuecounts.OpLT, millis, true
	default:
		return 0, 0, false // neither set (no range) or both set (between — a different shape)
	}
}

// durationBetweenLeafShape extracts (loMillis, hiMillis, ok) from a leaf with BOTH Min and Max
// set on durationHistogramColumn — the shape EstimateBetween expects.
func durationBetweenLeafShape(n *vm.RangeNode) (loMillis, hiMillis uint64, ok bool) {
	if !isDurationHistogramLeaf(n) || n.Min == nil || n.Max == nil {
		return 0, 0, false
	}
	lo, lok := durationValueMillis(n.Min)
	hi, hok := durationValueMillis(n.Max)
	if !lok || !hok {
		return 0, 0, false
	}
	return lo, hi, true
}

// isDurationHistogramLeaf gates both leaf-shape extractors: a range-predicate leaf (no
// Values/Pattern/RequirePresent) on the sole histogram-eligible column.
func isDurationHistogramLeaf(n *vm.RangeNode) bool {
	if n == nil || n.Column != durationHistogramColumn {
		return false
	}
	return len(n.Values) == 0 && n.Pattern == "" && !n.RequirePresent
}

// durationValueMillis converts a Min/Max bound's raw nanosecond value (the TraceQL-compiler
// convention for TypeInt/TypeDuration, mirroring valueAsColType's own handling of both types) to
// milliseconds via plain integer division — deliberately NOT vibuilder's own
// intOrDedicatedColType/decidableTimeBucketThreshold decidability logic (that solves VI's own,
// unrelated, exact-lookup decidability problem, was itself wrong twice recently — NOTE-VI-107/108
// — and this histogram's approximate, over-estimating nature has no decidability gate to
// replicate at all; copying that logic would import risk for zero benefit).
func durationValueMillis(v *vm.Value) (uint64, bool) {
	if v == nil {
		return 0, false
	}
	nanos, ok := v.Data.(int64)
	if !ok {
		return 0, false
	}
	if nanos < 0 {
		nanos = 0
	}
	return uint64(nanos) / 1_000_000, true
}

// CombineCostFuncs tries first, then second, per leaf — never both (a leaf's shape determines
// which CostFunc recognizes it; equality vs. range are mutually exclusive by construction, so
// the two never double-count a single leaf). Returns UnknownCost() only when neither recognizes
// the leaf.
func CombineCostFuncs(first, second CostFunc) CostFunc {
	return func(leaf *vm.RangeNode) LeafCost {
		if c := first(leaf); c.Known {
			return c
		}
		return second(leaf)
	}
}
