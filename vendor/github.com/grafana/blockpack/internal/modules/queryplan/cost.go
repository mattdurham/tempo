package queryplan

import "github.com/grafana/blockpack/internal/vm"

// LeafCost is a rough, comparable selectivity/cost estimate for a single leaf
// predicate. It intentionally carries a third state — Unknown — distinct from a
// known-zero count, because "no cost signal available" and "the oracle affirmatively
// says this leaf matches zero spans" are directionally OPPOSITE for ordering:
//
//   - A known-zero leaf is maximally selective: resolve it FIRST — inside an AND it
//     short-circuits the whole group to empty.
//   - An Unknown leaf carries no signal: it must sort LAST (resolve it only after
//     every leaf we do have a signal for), never masquerading as a cheap zero.
//
// Collapsing Unknown into Count==0 would make an unmeasured predicate wrongly
// short-circuit an AND that actually has matches — the exact bug NOTE-QP-001 guards
// against (see also the VCNT SelectivityEstimate.Covered contract, NOTE-VC-013).
type LeafCost struct {
	// Count is the estimated matching-span count for the leaf over the query window.
	// Only meaningful when Known is true; it is always >= 0.
	Count int64
	// Known reports whether Count carries a real cost signal. When false the leaf has
	// no coverage / no signal and sorts last regardless of Count.
	Known bool
}

// UnknownCost is the sentinel for a leaf with no cost signal. It sorts after every
// Known cost, whatever that Known cost's Count.
func UnknownCost() LeafCost { return LeafCost{Known: false} }

// KnownCost returns a LeafCost carrying a real signal. A negative count is clamped to
// zero: a net-negative estimate (an async/retention delta-accounting artifact) reads
// as "zero live matches", which is a legitimate maximally-selective signal, not
// unknown.
func KnownCost(count int64) LeafCost {
	if count < 0 {
		count = 0
	}
	return LeafCost{Count: count, Known: true}
}

// IsEmpty reports whether this cost affirmatively says the leaf matches zero spans.
// An Unknown cost is NOT empty (it carries no such affirmation), so IsEmpty can be
// used to drive AND short-circuiting safely: only a Known-zero leaf empties an AND.
func (c LeafCost) IsEmpty() bool { return c.Known && c.Count == 0 }

// less reports whether c should be resolved before other in a cost-ascending order.
// Known costs sort ahead of Unknown; among Known costs, smaller Count first. Two
// Unknown costs are equal (their relative order is left to the stable sort, which
// preserves original tree position).
func (c LeafCost) less(other LeafCost) bool {
	if c.Known != other.Known {
		// Known (has a signal) sorts before Unknown (no signal).
		return c.Known
	}
	if !c.Known {
		return false // both unknown: equal
	}
	return c.Count < other.Count
}

// CostFunc estimates the cost of a single leaf RangeNode. It is the pluggable oracle
// abstraction: #484's VCNT selectivity lookup is the first concrete implementation,
// but a per-block min/max/bloom estimator or a no-signal default (always UnknownCost)
// can plug in identically. A CostFunc must be pure and side-effect-free — it informs
// ordering only and must never itself perform the leaf's I/O.
//
// A CostFunc that cannot estimate a given leaf returns UnknownCost().
type CostFunc func(leaf *vm.RangeNode) LeafCost
