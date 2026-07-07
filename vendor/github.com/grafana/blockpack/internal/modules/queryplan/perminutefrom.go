package queryplan

// NOTE: VCNTPerMinuteFunc is new, additive code for issue #487 (holistic-review fix 1 —
// closing the composition seam that left the root public API unable to drive adaptive
// slicing). It is a NEW file, added alongside vcnt_cost.go rather than editing it, per the
// all-new-files ruling — but it deliberately calls vcnt_cost.go's unexported
// leafEqualityValue directly (same package) rather than duplicating that extraction logic,
// so the cost oracle and the per-minute oracle can never drift on which leaves are point
// lookups or how their values are canonically encoded.

import (
	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/vm"
)

// VCNTPerMinuteFunc is SelectivityPerMinute's oracle-construction sibling to VCNTCostFunc: it
// closes over one decoded VCNT section (data + dir) and query window and returns a callback
// suitable for BuildQueryPlan's perMinuteForLead parameter. Given the lead leaf Group.Lead()
// identifies, it extracts the leaf's (column, value) equality pair via the SAME
// leafEqualityValue/canonical-encoding path VCNTCostFunc itself uses (issue #485,
// NOTE-QP-001) — not a hand-rolled duplicate — so the two oracles agree on which leaves are
// point lookups and how their values are encoded.
//
// It returns nil for any leaf VCNTCostFunc would itself score UnknownCost() for (range /
// regex / multi-value / present-only / non-canonically-encodable), or when
// valuecounts.SelectivityPerMinute errors. A leaf that IS a valid equality lookup but has no
// live VCNT coverage for its column/value returns an empty (possibly non-nil) result via
// SelectivityPerMinute's own contract — never an error. Either an empty or nil result is a
// valid, expected perMinuteForLead outcome: BuildTimeSlices' own empty-perMinute uniform-width
// fallback handles both identically (len(perMinute) == 0), never a signal to propagate an
// error.
//
// Performs no object-storage I/O: data/dir are the caller's already-decoded VCNT section
// bytes, identical to VCNTCostFunc's own inputs.
func VCNTPerMinuteFunc(
	data []byte, dir []valuecounts.ChunkDirEntry, minTS, maxTS uint64,
) func(leaf *vm.RangeNode) []valuecounts.MinuteCount {
	return func(leaf *vm.RangeNode) []valuecounts.MinuteCount {
		colType, value, ok := leafEqualityValue(leaf)
		if !ok {
			return nil
		}
		canon, err := valueindex.CanonicalValue(colType, value)
		if err != nil {
			return nil
		}
		out, err := valuecounts.SelectivityPerMinute(data, dir, leaf.Column, canon, minTS, maxTS)
		if err != nil {
			return nil
		}
		return out
	}
}
