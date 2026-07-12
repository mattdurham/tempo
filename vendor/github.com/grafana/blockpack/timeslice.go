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
// returned by a CostFunc. See queryplan.UnknownCost/queryplan.KnownCost to construct one — a
// root-level re-export of those two constructors was removed as an unconsumed public export
// (issue #481 F-11 sweep: zero callers in tempo or blockpack outside this package's own tests,
// which construct a CostFunc by calling queryplan.UnknownCost/queryplan.KnownCost directly).
type LeafCost = queryplan.LeafCost

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
// unchanged. DispatchTimeSliced is the #487 opt-in narrowing — see queryplan.DispatchStrategy's
// own doc comment for the full contract.
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

// Selectivity classifies a query plan's VCNT-estimated match volume — see
// queryplan.Selectivity's own doc comment for the full contract. Re-exported at root so
// tempo's frontend can pass a classification result (from its own ClassifyProgramVCNT call,
// #481 Task 6) to SelectSearchStrategy without importing the internal queryplan package.
type Selectivity = queryplan.Selectivity

const (
	// UnknownSelectivity means no cost signal covered the plan's lead leaf.
	UnknownSelectivity = queryplan.UnknownSelectivity
	// Selective means the plan's lead leaf matches a small enough share of the column.
	Selective = queryplan.Selective
	// LowSelectivity means the plan's lead leaf matches most of the column's live values.
	LowSelectivity = queryplan.LowSelectivity
)

// SelectSearchStrategy implements R3's ruling table for choosing a search-query dispatch
// strategy from a VCNT selectivity classification and whether the query carries a limit
// (issue #481 parts 2/3, team-lead ruling R13) — see queryplan.SelectSearchStrategy's own doc
// comment for the full five-row decision table and the planTimeDecline contract. Tempo's
// frontend (buildQueryPlanFromProgram, #481 Task 6) calls this directly for search callers
// ONLY, after gating on boundedEligible/resolvability itself — metrics callers never reach it
// (R2: metrics is never bounded-served).
func SelectSearchStrategy(sel Selectivity, hasLimit bool) (strategy DispatchStrategy, planTimeDecline bool) {
	return queryplan.SelectSearchStrategy(sel, hasLimit)
}

// ClassifyProgramVCNT classifies prog's predicate selectivity over [minTS, maxTS] from one
// already-decoded VCNT section (issue #481 part 2, Task 6) — re-exported at root so tempo's
// frontend (buildQueryPlanFromProgram) can call it directly on the SAME decoded section it
// already fetches for TimeSliceOracle, without importing the internal queryplan package. See
// queryplan.ClassifyProgramVCNT's own doc comment for the full contract (UnknownSelectivity on
// no signal; performs no object-storage I/O — data/dir are the caller's already-decoded bytes).
func ClassifyProgramVCNT(
	prog *Program, data []byte, dir []VCNTChunkDirEntry, minTS, maxTS uint64,
) Selectivity {
	return queryplan.ClassifyProgramVCNT(prog, data, dir, minTS, maxTS)
}

// LeadDetail (SPEC-QP-7, NOTE-QP-011) carries a selectivity classification's lead-leaf cost
// detail — the index side (estimated matching count) and the full-scan side (column total
// population) that ClassifyProgramVCNT computes internally then discards — re-exported at root so
// tempo's frontend (buildQueryPlanFromProgram, issue #493 Task 4b) can report WHY a query
// qualified/declined without importing the internal queryplan package. See
// queryplan.LeadDetail's own doc comment for the full field contract.
type LeadDetail = queryplan.LeadDetail

// ClassifyProgramVCNTWithDetail is ClassifyProgramVCNT plus the LeadDetail its classification
// already computes internally — see queryplan.ClassifyProgramVCNTWithDetail's own doc comment
// for the full contract (same default threshold, zero additional object-storage I/O).
func ClassifyProgramVCNTWithDetail(
	prog *Program, data []byte, dir []VCNTChunkDirEntry, minTS, maxTS uint64,
) (Selectivity, LeadDetail) {
	return queryplan.ClassifyProgramVCNTWithDetail(prog, data, dir, minTS, maxTS)
}

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
