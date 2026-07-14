package queryplan

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md/NOTES.md.
//
// This file is purely additive (issue #499, Phase 2): it adds collectANDConjoinedLeaves as a new,
// self-contained tree walk. It never modifies plan.go, lead.go, or selectivity.go, and it does not
// change leadLeaf/Group.Lead()'s existing behavior for either of their two existing production
// callers (queryplan.go's BuildQueryPlan lead-leaf resolution, selectivity.go's classifyDetailed).

// collectANDConjoinedLeaves returns every leaf in g's tree whose path from g down to that leaf
// passes through zero GroupOR groups — i.e. every leaf that is AND-conjoined all the way to g's
// own root. Each such leaf, independently, is sufficient to prove a minute's whole-plan match
// count is zero when its own per-minute signal is confidently zero for that minute, since an AND
// is false the moment any one conjunct is false — regardless of any sibling OR branch or any
// other AND-conjoined leaf.
//
// Explicitly checks sub.Kind at every recursion level rather than relying on Plan()'s current
// "an AND group's sub-groups are always OR groups" construction invariant (planAndNodes/
// planOrNode, plan.go) — so this stays correct even if a future refactor of that construction
// changes the incidental structure. Concretely: a GroupOR (or any group whose own Kind is not
// GroupAND) contributes nothing and recursion into it stops immediately, returning nil for that
// branch — its leaves and every leaf beneath it are excluded, no matter how deeply nested.
//
// Never changes leadLeaf/Lead()'s own behavior — this is a wholly separate, additive tree walk,
// consulted only by the new skip-computation path in BuildQueryPlan (queryplan.go, issue #499
// Phase 3), never by Classify (selectivity.go) or by BuildQueryPlan's existing lead-leaf
// dispatch-priority resolution (queryplan.go's perMinuteForLead(lead.Node) call).
//
// SPEC-QP-10
func collectANDConjoinedLeaves(g Group) []PlannedLeaf {
	if g.Kind != GroupAND {
		return nil
	}
	leaves := append([]PlannedLeaf(nil), g.Leaves...)
	for _, sub := range g.SubGroup {
		leaves = append(leaves, collectANDConjoinedLeaves(sub)...)
	}
	return leaves
}
