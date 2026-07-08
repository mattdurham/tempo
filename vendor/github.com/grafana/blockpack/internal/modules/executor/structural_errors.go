package executor

// structural_errors.go — D4 (plan-d.md, issue #489): typed errors for the index-driven
// structural-query engine.

import "errors"

// ErrStructuralIndexCoverageGap mirrors NOTE-VI-072's coverage-gap-is-an-error contract for
// structural queries: BOTH the search VI (candidate discovery) and the trace-by-id VI/TraceGroup
// (whole-tree resolution) must be authoritative for a query's window before
// ExecuteStructuralFromIndex answers it. A Partial TraceGroup (an orphan span whose true parent
// lives outside the assembled data) is treated as a coverage gap for STRUCTURAL evaluation
// specifically (team-lead ruling, 2026-07-07) — unlike GetTraceByID's plain, non-structural
// Partial handling, which is not itself an error condition (SpanEntry.IsRoot/ParentSpanID
// semantics, valueindex/NOTES.md): a partial tree can produce false negatives for positive
// operators and false positives for negated ones, so labeling a narrower-than-true answer as
// authoritatively "successful" is not a safe default here.
//
// Also returned when indexOnly is true and either the search VI or the trace-by-id index has no
// coverage for the query window — a time-sliced structural job has no safe scan fallback across
// slice boundaries (mirrors #487's IndexOnly / ErrSliceIndexCoverageGap pattern and
// ErrValueIndexNoCoverage's precedent), so it must fail loudly rather than silently narrow.
var ErrStructuralIndexCoverageGap = errors.New("executor: structural index coverage gap")
