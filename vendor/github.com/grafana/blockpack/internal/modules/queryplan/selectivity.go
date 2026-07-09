package queryplan

import "github.com/grafana/blockpack/internal/vm"

// Selectivity classifies how much a query's predicate will let value-index pruning
// skip, driving the executor's choice of execution STRATEGY (issue #486, NOTE-QP-002).
// It is the recognition primitive #481 part 2 needs: "this predicate matches almost
// everything, index pruning won't help — prefer a pointed, recent-block-first,
// limit-bounded execution over building an index source across the whole window."
//
// It is deliberately three-state, mirroring LeafCost's Known/Unknown split
// (NOTE-QP-001): "no signal" and "affirmatively low-selectivity" are different
// decisions and must never be conflated.
type Selectivity int

const (
	// UnknownSelectivity means no cost signal covered the plan's lead leaf, so the
	// planner cannot say whether pruning will help. The caller applies its own
	// no-signal policy (e.g. keep the existing index-source-over-window default).
	// It is the zero value so a default-constructed classification is "no signal".
	UnknownSelectivity Selectivity = iota
	// Selective means the plan's lead (most-selective) leaf matches a small enough
	// fraction of the column's population that index pruning is expected to skip
	// meaningful work — build the index source over the window as usual.
	Selective
	// LowSelectivity means the plan's lead leaf matches most of the column's
	// population, so index pruning would skip almost nothing (the kind=server case
	// in #481/#486). A limit-bounded caller should prefer pointed, recent-block-first
	// execution that stops once it has enough matches rather than an unbounded
	// index-source build over the whole window.
	LowSelectivity
)

// String renders a Selectivity for logs/tests.
func (s Selectivity) String() string {
	switch s {
	case Selective:
		return "selective"
	case LowSelectivity:
		return "low-selectivity"
	default:
		return "unknown"
	}
}

// DefaultLowSelectivityFraction is the fraction of a column's population at or above
// which a lead leaf is treated as low-selectivity. A leaf matching >= this share of
// its column's spans lets index pruning skip < (1 - fraction) of the work, at which
// point a pointed, limit-bounded scan of recent blocks is the better strategy.
//
// 0.5 is a deliberately conservative default (a leaf must match at least HALF the
// column before we abandon the index-source path); it is not tuned to any specific
// benchmark predicate (per the "no benchmark-specific constants" rule) — it is the
// natural "the majority of spans match, pruning is a loss" threshold. Callers that
// want a different policy pass their own via ClassifyWithThreshold.
const DefaultLowSelectivityFraction = 0.5

// ColumnTotalFunc returns the total live-span population of a leaf's column over the
// query window — the DENOMINATOR the leaf's own count is a fraction of. It is the
// pluggable counterpart to CostFunc for the population side (valuecounts.
// ColumnTotalInRange is the concrete VCNT implementation, wired by the caller). It
// returns ok=false when it has no coverage for the column, in which case a leaf over
// that column cannot be classified and reads as UnknownSelectivity.
//
// Like CostFunc it must be pure and perform no leaf I/O — it informs strategy only.
type ColumnTotalFunc func(leaf *vm.RangeNode) (total int64, ok bool)

// Classify classifies g's selectivity using the default low-selectivity threshold.
// See ClassifyWithThreshold for the full contract.
func Classify(g Group, total ColumnTotalFunc) Selectivity {
	return ClassifyWithThreshold(g, total, DefaultLowSelectivityFraction)
}

// ClassifyWithThreshold classifies g's selectivity: it finds the plan's LEAD leaf
// (the most-selective leaf the executor would resolve first) and reports whether that
// leaf matches >= fraction of its column's population.
//
// Why the lead leaf and not the whole plan: the executor leads an AND with its
// cheapest leaf and narrows from there, so the lead leaf bounds how much pruning can
// possibly skip. If even the most-selective leaf matches most of its column, no leaf
// in the plan will prune well and pointed execution wins. For an OR the lead leaf is
// the cheapest disjunct; an OR can only be as selective as its most-selective branch
// (every branch's matches are unioned in), so the same lead-leaf bound is the right,
// conservative signal.
//
// The classification is:
//
//   - UnknownSelectivity: no leaf carried a Known cost, or the lead (Known,
//     lowest-count) leaf's column has no population coverage. No signal.
//   - LowSelectivity: the lead leaf's Known count is >= fraction * columnTotal.
//   - Selective: the lead leaf's Known count is < fraction * columnTotal.
//
// A lead leaf whose Known cost IsEmpty (net-zero live matches) is maximally selective
// and always classifies Selective — an empty leaf prunes everything, the exact
// opposite of low-selectivity. fraction is clamped to (0, 1]; a fraction <= 0 or > 1
// falls back to the default so a caller cannot accidentally classify everything (or
// nothing) as low-selectivity.
func ClassifyWithThreshold(g Group, total ColumnTotalFunc, fraction float64) Selectivity {
	sel, _ := classifyDetailed(g, total, fraction)
	return sel
}

// leadDetail (SPEC-QP-7, NOTE-QP-011) carries the lead leaf's own estimated matching count (the
// index side) and its column's total live population over the window (the full-scan side)
// alongside the verdict classifyDetailed already computes — both are already local variables
// inside the classification logic below; this struct just keeps them instead of discarding them
// (issue #493 Task 4b, R4: PRE-AUTHORIZED new blockpack public API — see LeadDetail, vcnt_cost.go,
// for the exported counterpart this is composed into).
type leadDetail struct {
	leadColumn    string
	leafCount     int64
	colTotal      int64
	hasLead       bool
	colTotalKnown bool
}

// classifyDetailed (SPEC-QP-7, NOTE-QP-011) is ClassifyWithThreshold's implementation,
// additionally returning the lead leaf detail its caller (ClassifyProgramVCNTWithDetail,
// vcnt_cost.go) needs. ClassifyWithThreshold itself is a thin wrapper discarding the detail —
// this split guarantees ClassifyWithThreshold's existing behavior is BYTE-IDENTICAL (parity-first,
// zero risk to its existing callers) while making the detail available to the new sibling —
// mirroring SPEC-QP-1's Group.Lead()/leadLeaf "wrapper can never diverge from its own
// implementation" pattern.
func classifyDetailed(g Group, total ColumnTotalFunc, fraction float64) (Selectivity, leadDetail) {
	if total == nil {
		return UnknownSelectivity, leadDetail{}
	}
	if fraction <= 0 || fraction > 1 {
		fraction = DefaultLowSelectivityFraction
	}
	lead, ok := leadLeaf(g)
	if !ok {
		return UnknownSelectivity, leadDetail{}
	}
	detail := leadDetail{hasLead: true, leafCount: lead.Cost.Count}
	if lead.Node != nil {
		detail.leadColumn = lead.Node.Column
	}
	if lead.Cost.IsEmpty() {
		// Net-zero live matches: maximally selective, prunes everything.
		return Selective, detail
	}
	colTotal, ok := total(lead.Node)
	if !ok || colTotal <= 0 {
		return UnknownSelectivity, detail
	}
	detail.colTotal = colTotal
	detail.colTotalKnown = true
	if float64(lead.Cost.Count) >= fraction*float64(colTotal) {
		return LowSelectivity, detail
	}
	return Selective, detail
}

// leadLeaf returns the most-selective leaf across the whole plan tree: the leaf with
// the lowest Known cost. Unknown-cost leaves are never the lead (they carry no
// signal). It recurses into sub-groups so a lead leaf nested inside an OR branch's
// AND sub-group is still considered. ok is false when no leaf in the tree carried a
// Known cost.
func leadLeaf(g Group) (PlannedLeaf, bool) {
	var (
		best     PlannedLeaf
		haveBest bool
	)
	consider := func(l PlannedLeaf) {
		if !l.Cost.Known {
			return
		}
		if !haveBest || l.Cost.less(best.Cost) {
			best = l
			haveBest = true
		}
	}
	for _, l := range g.Leaves {
		consider(l)
	}
	for _, sub := range g.SubGroup {
		if l, ok := leadLeaf(sub); ok {
			consider(l)
		}
	}
	return best, haveBest
}
