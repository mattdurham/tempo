package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// NOTE-VI-087: compileStructuralPair is the 2-node-only v1 compile bridge for the index-driven
// structural query path (plan-d.md §D2, issue #489). It declines (ok=false, err=nil) on any
// chain shape other than exactly 2 filter nodes rather than erroring — the caller falls back to
// the scan path (ExecuteStructural), which already supports N-ary chains up to SPEC-STRUCT-8.
// See executor/NOTES.md NOTE-VI-087 for the full design decision (staged; lands in NOTES.md at
// team-lead's finalization signal per this phase's stage-then-finalize convention).

import (
	"fmt"

	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// compileStructuralPair compiles a 2-node structural query's two filter legs to their own
// *vm.Program, reusing traceqlparser.FlattenChain (already exported) and vm.CompileTraceQLFilter
// per leg — the SAME per-leg compile step stream_structural.go's own (unexported)
// compileStructuralPrograms already performs, just returning a typed pair instead of a slice and
// declining (not erroring) on chain lengths this phase's index-driven path doesn't yet support.
// A nil filter leg (match-all) compiles to a nil *vm.Program, mirroring compileStructuralPrograms.
//
// Compilation itself does not gate on positive-vs-negated op: a negated op (!>>, !>, !~)
// compiles successfully here — D4 declines negated ops at the execution layer, and D6 reuses
// this exact function unchanged for its own negated-structural engine.
//
// ok=false, err=nil: q flattens to other than exactly 2 filter nodes — the caller (D4/D6) must
// fall back to ExecuteStructural (this phase's scope decision, see plan-d.md).
// ok=false, err!=nil: a genuine compile error on one of the two legs.
func compileStructuralPair(q *traceqlparser.StructuralQuery) (
	leftProg, rightProg *vm.Program, op traceqlparser.StructuralOp, ok bool, err error,
) {
	filters, ops := traceqlparser.FlattenChain(q)
	if len(filters) != 2 || len(ops) != 1 {
		return nil, nil, 0, false, nil
	}
	leftProg, err = compileStructuralLeg(filters[0])
	if err != nil {
		return nil, nil, 0, false, fmt.Errorf("compileStructuralPair: left leg: %w", err)
	}
	rightProg, err = compileStructuralLeg(filters[1])
	if err != nil {
		return nil, nil, 0, false, fmt.Errorf("compileStructuralPair: right leg: %w", err)
	}
	return leftProg, rightProg, ops[0], true, nil
}

// compileStructuralLeg mirrors compileStructuralPrograms' per-node rule: nil filter -> nil
// program (match-all).
func compileStructuralLeg(f *traceqlparser.FilterExpression) (*vm.Program, error) {
	if f == nil {
		return nil, nil
	}
	return vm.CompileTraceQLFilter(f)
}

// CompileStructuralLegs is the public re-export of compileStructuralPair (D5b, plan-d.md, issue
// #489), exposed so an external caller (root's structural.go, tempo's DT1 dispatch) can classify a
// structural query's operator and build BOTH ValueIndexSources (left AND right) before calling
// ExecuteStructuralFromIndex/ExecuteNegatedStructuralFromIndex — a permanently-nil rightSource
// would disable D4's intersection prefilter and the discovery-seed cost mechanism entirely,
// defeating the seed-split ruling (D4-SEED). Same exact decline-vs-error contract as
// compileStructuralPair; this is a zero-logic passthrough, not a second implementation.
func CompileStructuralLegs(q *traceqlparser.StructuralQuery) (
	leftProg, rightProg *vm.Program, op traceqlparser.StructuralOp, ok bool, err error,
) {
	return compileStructuralPair(q)
}
