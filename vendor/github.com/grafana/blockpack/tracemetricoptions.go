package blockpack

import "errors"

// ErrValueIndexNoCoverage (issue #487, holistic-review Issue 1/fix A) is the error
// ExecuteMetricsTraceQL returns when TraceMetricOptions.IndexOnly is set and the value index
// cannot answer the query — mirrors the search path's ErrSliceIndexCoverageGap (owned by
// tempo, since the search path's decline-to-scan decision is made by its caller, not by
// blockpack itself); this sentinel exists because the metrics path's decline-to-scan fallback
// is owned by ExecuteMetricsTraceQL internally, so its caller needs a way to detect "declined,
// no scan happened" from the returned error using errors.Is.
var ErrValueIndexNoCoverage = errors.New("blockpack: value index cannot answer this metrics query under index-only mode")

// TraceMetricOptions is a blockpack data type.
type TraceMetricOptions struct {
	// ValueIndex, when non-nil, enables the zero-block-read metrics path
	// (NOTE-VI-033, issue #460): count_over_time()/rate() without group-by are
	// answered from value-index TimeSec alone. The index is authoritative for the
	// metric shapes and columns it covers (NOTE-VI-047, issue #474) — when it
	// answers, that answer is complete and correct. ExecuteMetricsTraceQL falls back
	// to a full block scan ONLY when the index genuinely cannot answer: an
	// unsupported metric shape (group-by, non-count/rate function) or a leaf column
	// with no coverage (negation/unindexable predicate, or a file predating per-span
	// timestamps) — UNLESS IndexOnly is set (see its own doc comment).
	ValueIndex ValueIndexSource

	// IndexOnly (issue #487, holistic-review Issue 1/fix A) forbids ExecuteMetricsTraceQL's
	// full-block-scan fallback: when true and the value index cannot answer the query (no
	// ValueIndex configured, or ExecuteTraceMetricsFromVI's own decline for an unsupported
	// shape or uncovered leaf), ExecuteMetricsTraceQL returns ErrValueIndexNoCoverage instead
	// of falling through to a scan. A caller running a narrowed-window, per-slice job (tempo's
	// #487 time-slice dispatch) sets this because a full, un-windowed scan is not a safe
	// fallback for that job shape — it would ignore the slice boundary at the per-span level
	// and could double-count or over-fetch across overlapping slice jobs dispatched for the
	// same block. Mirrors the search path's own IndexOnly contract (QueryTraceQLFromIndex's
	// caller, tempo's tryIndexFetch, converts an analogous decline into
	// ErrSliceIndexCoverageGap) — this field exists because ExecuteMetricsTraceQL, unlike the
	// search path, owns its VI-decline-to-scan fallback internally rather than leaving it to
	// its caller, so the caller needs a way to forbid that fallback from outside.
	IndexOnly bool

	StartNano int64
	EndNano   int64
	StepNano  int64
}
