package queryplan

// Lead returns the plan's most-selective leaf: the leaf with the lowest Known cost,
// recursing into sub-groups. Unknown-cost leaves are never the lead. ok is false when
// no leaf in the tree carries a Known cost. This is the same "most-selective leaf"
// concept ClassifyWithThreshold's unexported leadLeaf (selectivity.go) already computes
// internally for selectivity classification — Lead is a thin exported wrapper around
// that same function, for callers (time-slice construction, #487) that need the lead
// leaf itself, not just a selectivity verdict derived from it. leadLeaf itself stays
// unexported; Lead is the sole exported surface.
//
// SPEC-QP-1: Group.Lead() exported lead-leaf accessor contract.
// NOTE-QP-004
func (g Group) Lead() (PlannedLeaf, bool) {
	return leadLeaf(g)
}
