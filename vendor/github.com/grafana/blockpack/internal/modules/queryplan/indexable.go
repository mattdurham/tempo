package queryplan

// indexable.go — AllLeavesIndexable, issue #487 T5b. Additive, no existing file (plan.go,
// cost.go, selectivity.go, vcnt_cost.go, queryplan.go) is touched.
//
// BuildQueryPlan's allLeavesResolvable parameter needs an ALL-leaves verdict, but
// vibuilder.BuildSource's own ok return (the value-index query path's existing availability
// check) only requires ONE leaf in a program to have an indexable shape — see BuildSource's own
// doc comment ("at least one column resolved against the index"). A caller that reused BuildSource's
// ok verbatim as allLeavesResolvable could qualify a mixed-shape query (one indexable leaf plus
// one leaf the index architecturally cannot represent — multi-value OR, negation, a
// RequirePresent-only leaf) for DispatchTimeSliced, when in fact only part of the query is
// index-answerable — see NOTE-QP-009/SPEC-QP-5.

import (
	"github.com/grafana/blockpack/internal/modules/vibuilder"
	"github.com/grafana/blockpack/internal/vm"
)

// AllLeavesIndexable reports whether EVERY leaf in prog's predicate tree has a shape the value
// index can represent at all (single-value equality, range, or regex — vibuilder.LeafIndexable's
// exact rules, reused verbatim here rather than re-derived, so this can never drift from the
// same decision the real index-source builder makes). It performs no I/O and is independent of
// whether any value-index files currently exist (a data-presence question, not a shape one —
// see vibuilder.LeafIndexable's own doc comment); it answers purely "could the index represent
// this query's SHAPE if it had full data," which is exactly the complement of BuildSource's own
// per-leaf shape rejection that leaves a column uncovered rather than failing the build.
//
// SPEC-QP-5: allLeavesResolvable's ALL-not-ANY contract — a caller computing allLeavesResolvable
// from a data-availability check alone (e.g. vibuilder.BuildSource's ok) must additionally
// require AllLeavesIndexable(prog) before treating the query as index-answerable, since
// BuildSource's own ok is satisfied by ANY ONE indexable leaf, not ALL of them.
//
// A program with no leaves and no match-all Columns list (nothing referenced at all) returns
// false, mirroring BuildSource's own "truly nothing referenced" decline. A match-all query
// (Nodes empty, Columns populated — e.g. `{} | rate()`) returns true: BuildSource's own
// lookupColumnAll path never rejects a column's shape, so there is nothing to reject here either.
func AllLeavesIndexable(prog *vm.Program) bool {
	if prog == nil || prog.Predicates == nil {
		return false
	}
	preds := prog.Predicates
	if len(preds.Nodes) == 0 && len(preds.Columns) == 0 {
		return false
	}
	if len(preds.Nodes) == 0 {
		// Match-all over an explicit column list: BuildSource's lookupColumnAll path takes
		// every listed column unconditionally, with no per-leaf shape rejection.
		return true
	}
	leaves := collectLeafNodes(preds.Nodes)
	if len(leaves) == 0 {
		return false
	}
	for _, n := range leaves {
		if !vibuilder.LeafIndexable(n) {
			return false
		}
	}
	return true
}

// collectLeafNodes flattens nodes to its leaf RangeNodes (those naming a column), descending
// into every composite child regardless of AND/OR — mirroring vibuilder.collectLeaves' own flat
// walk (not Plan()'s AND/OR-structured walk), since AllLeavesIndexable must inspect exactly the
// same leaf set BuildSource itself would touch, independent of the query's boolean structure.
func collectLeafNodes(nodes []vm.RangeNode) []*vm.RangeNode {
	var out []*vm.RangeNode
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
			out = append(out, n)
		}
	}
	walk(nodes)
	return out
}
