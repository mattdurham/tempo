package queryplan

import (
	"sort"

	"github.com/grafana/blockpack/internal/vm"
)

// PlannedLeaf is one leaf predicate paired with its estimated cost and a stable
// index recording its original position in the tree walk (used as a tie-breaker so
// equal-cost / both-Unknown leaves keep deterministic order).
type PlannedLeaf struct {
	// Node points into the caller-owned RangeNode tree; the plan never copies or
	// mutates it. The caller resolves exactly this leaf when it reaches it.
	Node *vm.RangeNode
	// Cost is the leaf's estimated cost from the CostFunc.
	Cost LeafCost
	// origin is the leaf's original discovery order, used only as a stable sort
	// tie-breaker; not exported because it carries no meaning to the caller.
	origin int
}

// Group is one node of the query plan tree. It is either an AND group or an OR
// group (Kind), holding an ordered set of leaves that live directly under it plus
// any nested sub-groups (an AND group may contain OR sub-groups and vice versa).
//
// AND vs OR changes what the ordering MEANS, not the ordering itself — both order
// Leaves cost-ascending — so the two behaviors share this one structure but are
// documented and consumed differently:
//
//   - GroupAND: Leaves are resolve-order AND short-circuit-eligible. The caller
//     resolves them in order and may stop the moment one resolves empty (the whole
//     AND is then empty). CanShortCircuit reports true.
//   - GroupOR: Leaves are an evaluation-ORDER hint only. Every branch must still be
//     resolved for correctness; the ordering only helps an early-exit-eligible
//     caller reach matches sooner. CanShortCircuit reports false.
type Group struct {
	Leaves   []PlannedLeaf
	SubGroup []Group
	Kind     GroupKind
}

// GroupKind distinguishes AND groups from OR groups.
type GroupKind int

const (
	// GroupAND is a conjunction: cost-ordered and short-circuitable.
	GroupAND GroupKind = iota
	// GroupOR is a disjunction: cost-ordered as a hint only, never short-circuited.
	GroupOR
)

// CanShortCircuit reports whether resolving a leaf in this group to empty lets the
// caller skip the group's remaining leaves. True for AND (an empty conjunct empties
// the whole AND), false for OR (every branch is independently required).
func (g Group) CanShortCircuit() bool { return g.Kind == GroupAND }

// Plan walks prog's predicate tree, scores every leaf with cost, and returns the
// root Group: the top-level AND of all prog.Predicates.Nodes, with each OR composite
// turned into a nested GroupOR whose branches are themselves recursively planned.
//
// It returns ok=false when there is nothing to plan (nil program / no predicate
// nodes / no leaves resolved a cost path) so the caller can skip cost-based ordering
// entirely and fall back to its existing unordered resolution.
//
// Plan performs no I/O; cost must be a pure estimator (see CostFunc).
func Plan(prog *vm.Program, cost CostFunc) (Group, bool) {
	if prog == nil || prog.Predicates == nil || cost == nil {
		return Group{}, false
	}
	nodes := prog.Predicates.Nodes
	if len(nodes) == 0 {
		return Group{}, false
	}
	var next int
	root := planAndNodes(nodes, cost, &next)
	if len(root.Leaves) == 0 && len(root.SubGroup) == 0 {
		return Group{}, false
	}
	return root, true
}

// ProgramLeafColumns returns the distinct column names named by prog's leaf
// predicates, in stable first-seen order. It walks the full predicate tree the same
// way Plan does — descending into every AND/OR composite — because a consumer
// deciding which VCNT columns to fetch before classifying selectivity needs the
// complete leaf-column set regardless of boolean structure (the selectivity verdict
// keys on the plan's lead leaf, but the caller cannot know which leaf that is until
// it has the VCNT data for all of them). Present-only leaves (Column set, no
// Values/Min/Max/Pattern) are still columns and are included; a caller that only
// wants point-lookup columns filters further.
//
// Returns nil when prog is nil, has no predicates, or names no columns.
func ProgramLeafColumns(prog *vm.Program) []string {
	if prog == nil || prog.Predicates == nil {
		return nil
	}
	var out []string
	seen := make(map[string]struct{})
	var walk func(ns []vm.RangeNode)
	walk = func(ns []vm.RangeNode) {
		for i := range ns {
			n := &ns[i]
			if len(n.Children) > 0 {
				walk(n.Children)
				continue
			}
			if n.Column == "" {
				continue
			}
			if _, dup := seen[n.Column]; dup {
				continue
			}
			seen[n.Column] = struct{}{}
			out = append(out, n.Column)
		}
	}
	walk(prog.Predicates.Nodes)
	return out
}

// planAndNodes builds a GroupAND from an implicitly-AND-combined node list. Leaf
// children become directly-owned Leaves; composite children recurse (an AND
// composite is flattened up into this group; an OR composite becomes a nested
// GroupOR sub-group). next threads a monotonically-increasing discovery counter for
// stable tie-breaking.
func planAndNodes(nodes []vm.RangeNode, cost CostFunc, next *int) Group {
	g := Group{Kind: GroupAND}
	for i := range nodes {
		n := &nodes[i]
		if len(n.Children) == 0 {
			if n.Column == "" {
				continue // not a resolvable leaf (e.g. an empty node)
			}
			g.Leaves = append(g.Leaves, PlannedLeaf{
				Node:   n,
				Cost:   cost(n),
				origin: takeOrigin(next),
			})
			continue
		}
		if n.IsOR {
			g.SubGroup = append(g.SubGroup, planOrNode(n, cost, next))
			continue
		}
		// Nested AND composite: maximal-AND-subtree rule — flatten it into THIS
		// group's leaves/sub-groups so a whole chain of AND parents is one orderable
		// group, not artificially split at each nesting level.
		sub := planAndNodes(n.Children, cost, next)
		g.Leaves = append(g.Leaves, sub.Leaves...)
		g.SubGroup = append(g.SubGroup, sub.SubGroup...)
	}
	orderAscending(g.Leaves)
	return g
}

// planOrNode builds a GroupOR from an OR composite. Each child branch is planned:
// a leaf child becomes an OR-owned leaf; a composite child recurses. Unlike AND, the
// resulting Leaves order is an evaluation-order hint only (see Group docs).
func planOrNode(n *vm.RangeNode, cost CostFunc, next *int) Group {
	g := Group{Kind: GroupOR}
	for i := range n.Children {
		c := &n.Children[i]
		if len(c.Children) == 0 {
			if c.Column == "" {
				continue
			}
			g.Leaves = append(g.Leaves, PlannedLeaf{
				Node:   c,
				Cost:   cost(c),
				origin: takeOrigin(next),
			})
			continue
		}
		if c.IsOR {
			// Nested OR under OR: flatten (OR of ORs is one OR) so all disjuncts sit
			// in one evaluation-order pool.
			sub := planOrNode(c, cost, next)
			g.Leaves = append(g.Leaves, sub.Leaves...)
			g.SubGroup = append(g.SubGroup, sub.SubGroup...)
			continue
		}
		// An AND-subtree that is one branch of this OR: cost-order it recursively as
		// its own GroupAND sub-group (its internal short-circuiting is valid within
		// the branch even though the OR itself cannot skip the branch).
		g.SubGroup = append(g.SubGroup, planAndNodes(c.Children, cost, next))
	}
	orderAscending(g.Leaves)
	return g
}

// orderAscending sorts leaves cost-ascending with a stable origin tie-break. Known
// costs sort ahead of Unknown; among Known, smaller Count first; ties (and
// both-Unknown pairs) keep original discovery order. This is a plain sort with an
// explicit tie-breaker rather than sort.Stable so the ordering is fully determined
// by (cost, origin) and not by the sort's stability guarantee.
func orderAscending(leaves []PlannedLeaf) {
	sort.Slice(leaves, func(i, j int) bool {
		a, b := leaves[i], leaves[j]
		if a.Cost.less(b.Cost) {
			return true
		}
		if b.Cost.less(a.Cost) {
			return false
		}
		return a.origin < b.origin
	})
}

// takeOrigin returns the current discovery counter and advances it.
func takeOrigin(next *int) int {
	o := *next
	*next++
	return o
}
