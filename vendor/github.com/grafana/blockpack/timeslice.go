package blockpack

// timeslice.go — public API surface for #487's time-slice job sharding: thin wrappers
// (no internal imports leak) over the internal queryplan package's new types, for tempo's
// frontend integration (Section 2). Mirrors vcnt.go's own wrapper convention. Naming
// deliberately avoids a VCNT prefix (brainstorm-c.md risk 6 / team-lead binding ruling) to
// stay visually distinct from vcnt.go's own VCNT-prefixed additions, which belong to a
// concurrently-edited session's namespace — this file never edits vcnt.go itself.

import (
	"github.com/grafana/blockpack/internal/modules/queryplan"
	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/vm"
)

// RangeNode identifies a single leaf predicate within a compiled Program's tree. It is
// passed to a CostFunc and to BuildQueryPlan's per-minute VCNT-signal callback so a caller
// can score or fetch signal for exactly the leaf being planned.
type RangeNode = vm.RangeNode

// LeafCost is a rough, comparable selectivity/cost estimate for a single leaf predicate,
// returned by a CostFunc. See UnknownCost/KnownCost to construct one.
type LeafCost = queryplan.LeafCost

// UnknownCost is the sentinel LeafCost for a leaf with no cost signal — it sorts after
// every Known cost during BuildQueryPlan's internal leaf-ordering pass.
func UnknownCost() LeafCost { return queryplan.UnknownCost() }

// KnownCost returns a LeafCost carrying a real signal. A negative count is clamped to
// zero (a legitimate maximally-selective "zero live matches" signal, not unknown).
func KnownCost(count int64) LeafCost { return queryplan.KnownCost(count) }

// CostFunc estimates the cost of a single leaf RangeNode for BuildQueryPlan's internal
// leaf-ordering pass. It must be pure and side-effect-free — it informs ordering only and
// must never itself perform the leaf's I/O. A CostFunc that cannot estimate a given leaf
// returns UnknownCost(). TimeSliceOracle builds a real, VCNT-backed CostFunc from a decoded
// VCNT section — the intended way for a caller outside this module to obtain one, since the
// concrete internal implementation is not itself exported.
type CostFunc = queryplan.CostFunc

// MinuteCount is one live minute-bucket's net span count for a single column=value leaf —
// the per-minute VCNT signal BuildQueryPlan's lead-leaf callback returns. TimeSliceOracle
// builds a callback that produces these from a decoded VCNT section — the intended way for a
// caller outside this module to obtain one.
type MinuteCount = valuecounts.MinuteCount

// TimeSlice is one time-bounded sub-window of a query, carrying enough information for a
// dispatch-priority-ordering caller (tempo's sharder) to reorder without re-deriving signal
// blockpack already computed. Start/End are minute-aligned unix seconds, [Start, End)
// half-open. EstMatches/EstKnown/VCNTEmpty are dispatch-priority hints only — BuildQueryPlan
// never reorders TimeSlices by them; see queryplan.TimeSlice's doc comment for the full
// three-state EstKnown/VCNTEmpty contract.
type TimeSlice = queryplan.TimeSlice

// QueryPlan is the top-level output blockpack hands tempo's frontend: the cost-ordered leaf
// plan (Root) plus, when Strategy qualifies, a set of chronologically-ordered TimeSlices.
type QueryPlan = queryplan.QueryPlan

// DispatchStrategy tells the caller (tempo's frontend) which job-construction path to use
// for a query. DispatchBlockSharded (the zero value) is today's existing per-block job path,
// unchanged. DispatchTimeSliced is the #487 opt-in narrowing.
type DispatchStrategy = queryplan.DispatchStrategy

const (
	// DispatchBlockSharded is the zero value and today's existing per-block job path.
	DispatchBlockSharded = queryplan.DispatchBlockSharded
	// DispatchTimeSliced is the #487 opt-in narrowing; see DispatchStrategy's doc comment.
	DispatchTimeSliced = queryplan.DispatchTimeSliced
	// DefaultK is the recommended default for BuildTimeSlices/BuildQueryPlan's k parameter —
	// see queryplan.DefaultK's own doc comment (SPEC-QP-2/NOTE-QP-008) for the full rationale.
	DefaultK = queryplan.DefaultK
)

// BuildQueryPlan composes a cost-based leaf plan and, when qualified, minute-aligned
// TimeSlices for prog's predicate over [minTS, maxTS] — the #487 time-slice job-sharding
// entry point for tempo's frontend (issue #487, Section 1 C4/C5).
//
// allLeavesResolvable is the caller-supplied #481 index-coverage verdict (blockpack's plan
// has no notion of index coverage itself — that's a tempo/vibuilder-side concept) and is the
// ONLY gate on Strategy: DispatchBlockSharded whenever any leaf is unresolvable, or when prog
// has nothing plannable. cost scores each leaf; TimeSliceOracle builds a real, VCNT-backed
// (cost, perMinuteForLead) pair from a decoded VCNT section for callers that want one, rather
// than requiring a hand-rolled CostFunc. perMinuteForLead is resolved AFTER the plan identifies
// its lead (most-selective) leaf, mirroring cost's own pluggable-oracle shape, so the caller
// never has to guess which leaf will be lead ahead of time; a resolvable-but-VCNT-blind lead
// leaf (perMinuteForLead returning nil/empty, or no lead leaf at all) still qualifies for
// DispatchTimeSliced with uniform-width (EstKnown=false) slices rather than falling back to
// DispatchBlockSharded. An inverted or otherwise degenerate [minTS, maxTS] (minTS > maxTS, or
// a range BuildTimeSlices cannot produce any slice for) also falls back to DispatchBlockSharded
// rather than reporting DispatchTimeSliced with zero Slices. A hand-rolled perMinuteForLead
// (as opposed to one built by TimeSliceOracle) is expected to return non-negative
// MinuteCount.Count values; a negative Count is defensively clamped to zero rather than
// corrupting the adaptive-width calculation.
func BuildQueryPlan(
	prog *Program, cost CostFunc, allLeavesResolvable bool,
	perMinuteForLead func(leaf *RangeNode) []MinuteCount,
	minTS, maxTS uint64, concurrentRequests, k int,
) QueryPlan {
	return queryplan.BuildQueryPlan(
		prog, cost, allLeavesResolvable, perMinuteForLead, minTS, maxTS, concurrentRequests, k,
	)
}

// TimeSliceOracle composes a real, VCNT-backed CostFunc and perMinuteForLead callback from one
// decoded VCNT section (data, dir) over [minTS, maxTS] — the intended way for a caller outside
// this module (tempo's frontend) to obtain the two callbacks BuildQueryPlan needs, without
// hand-rolling leaf-value canonicalization itself (a non-trivial, column-type-dependent
// encoding step that must agree exactly with how blockpack's own write path encodes values, to
// avoid drift). Mirrors ClassifyProgramVCNT's own "compose oracles over one decoded section"
// pattern (see vcnt_cost.go, NOTE-QP-003) for the #487 time-slice case.
//
// Both returned callbacks answer only single-value-equality leaves the VCNT section can
// canonically encode (the same scope as VCNTSelectivityInRange/VCNTSelectivityPerMinute) —
// every other leaf shape (range, regex, multi-value, present-only, or a leaf VCNTSelectivity*
// has no coverage for) reads as UnknownCost()/no per-minute signal, which is a safe, expected
// input to both BuildQueryPlan and BuildTimeSlices (they degrade to the block-sharded or
// uniform-width fallback respectively, never an error).
//
// It performs no object-storage I/O: data/dir are the caller's already-decoded VCNT section
// bytes (e.g. from VCNTBuildSectionFromObjects), identical to VCNTSelectivityInRange's own
// inputs.
func TimeSliceOracle(
	data []byte, dir []VCNTChunkDirEntry, minTS, maxTS uint64,
) (CostFunc, func(leaf *RangeNode) []MinuteCount) {
	return queryplan.VCNTCostFunc(data, dir, minTS, maxTS), queryplan.VCNTPerMinuteFunc(data, dir, minTS, maxTS)
}

// AllLeavesIndexable reports whether EVERY leaf in prog's predicate tree has a shape the value
// index can represent at all (single-value equality, range, or regex), independent of whether
// any value-index files currently exist for it — a data-presence question this function does
// not answer (issue #487, T5b).
//
// BuildQueryPlan's allLeavesResolvable parameter needs an ALL-leaves verdict; a caller that
// derives it purely from a value-index-availability check (e.g. whether BuildValueIndexSource
// found ANY coverage) is checking a strictly weaker "at least one leaf" condition — see
// BuildValueIndexSource's own doc comment. AllLeavesIndexable closes that gap: combine it (AND)
// with the caller's own availability/data-presence check to get a true ALL-leaves-resolvable
// verdict, rather than reusing the availability check alone.
//
// A program with nothing referenced at all (no leaves, no match-all column list) returns false.
// A match-all query (e.g. `{} | rate()`) returns true — that shape has no per-leaf value
// predicate to reject.
func AllLeavesIndexable(prog *Program) bool {
	return queryplan.AllLeavesIndexable(prog)
}
