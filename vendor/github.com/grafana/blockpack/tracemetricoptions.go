package blockpack

import (
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
)

// ErrMetricsShapeNotAnswerable (issue #481 part 3, decline category 1c) is returned by
// ExecuteMetricsTraceQL when a metrics query's aggregate function or group-by combination
// cannot be answered from the value index (only count_over_time()/rate() without group-by are
// index-answerable). There is no full-block-scan fallback — this is the PRODUCTION DEFAULT
// decline contract, not just under TraceMetricOptions.IndexOnly. Use errors.Is to detect it.
// Re-exported from modules_executor (the internal package owns the canonical definition).
//
// SPEC-ROOT-019 (revised, issue #481 part 3): metrics decline contract — every
// ExecuteMetricsTraceQL decline is now one of ErrMetricsShapeNotAnswerable/
// ErrMetricsNoCoverage/ErrMetricsLegacyTimeSecZero, the PRODUCTION DEFAULT contract (not just
// under TraceMetricOptions.IndexOnly), with no full-block-scan fallback (ExecuteTraceMetrics
// was deleted outright).
var ErrMetricsShapeNotAnswerable = modules_executor.ErrMetricsShapeNotAnswerable

// ErrMetricsNoCoverage (issue #481 part 3, decline category 1b) is returned by
// ExecuteMetricsTraceQL when a leaf column in a metrics query's predicate has no value-index
// coverage — a per-query-shape limitation with the index otherwise configured and enabled.
// Contrast with ErrMetricsValueIndexDisabled (R8's distinct config-level category, fired when
// no ValueIndexSource was supplied at all). There is no full-block-scan fallback. Use errors.Is
// to detect it.
var ErrMetricsNoCoverage = modules_executor.ErrMetricsNoCoverage

// ErrMetricsLegacyTimeSecZero (issue #481 part 3, decline category 1d) is returned by
// ExecuteMetricsTraceQL when a matched span's indexed TimeSec is 0, meaning the block predates
// per-span timestamps. There is no full-block-scan fallback. Use errors.Is to detect it.
var ErrMetricsLegacyTimeSecZero = modules_executor.ErrMetricsLegacyTimeSecZero

// ErrMetricsValueIndexDisabled (issue #481 part 3, team-lead ruling R8) is returned by
// ExecuteMetricsTraceQL when no ValueIndexSource was supplied at all (opts.ValueIndex == nil)
// — a distinct zeroth/config-level category from ErrMetricsNoCoverage's per-query-shape
// limitation: this fires for EVERY metrics query on a querier with value_index_query disabled,
// not just queries touching a specific uncovered column. There is no full-block-scan fallback
// (ExecuteTraceMetrics was deleted outright). Use errors.Is to detect it.
var ErrMetricsValueIndexDisabled = modules_executor.ErrMetricsValueIndexDisabled

// TraceMetricOptions is a blockpack data type.
type TraceMetricOptions struct {
	// ValueIndex, when non-nil, enables the zero-block-read metrics path
	// (NOTE-VI-033, issue #460): count_over_time()/rate() without group-by are
	// answered from value-index TimeSec alone. The index is authoritative for the
	// metric shapes and columns it covers (NOTE-VI-096, issue #474/#481) — when it
	// answers, that answer is complete and correct. When nil, ExecuteMetricsTraceQL
	// returns ErrMetricsValueIndexDisabled (R8's config-level category). When the index
	// genuinely cannot answer (an unsupported metric shape, an uncovered leaf column,
	// or a legacy block with no per-span timestamps), it returns one of
	// ErrMetricsShapeNotAnswerable / ErrMetricsNoCoverage / ErrMetricsLegacyTimeSecZero
	// (issue #481 part 3) — there is no full-block-scan fallback either way.
	ValueIndex ValueIndexSource

	// IndexOnly (issue #487, holistic-review Issue 1/fix A) no longer changes
	// ExecuteMetricsTraceQL's decline behavior (issue #481 part 3 removed the
	// full-block-scan fallback entirely, so there is nothing left for IndexOnly to
	// forbid): every decline returns the same typed sentinel regardless of this field's
	// value. Retained on the struct because tempo's callers (time-slice vs.
	// block-sharded dispatch, #487) still set it to describe the job's own dispatch
	// shape, mirroring the search path's own indexOnly parameter — but blockpack
	// itself now treats index-only and production-default identically for metrics.
	IndexOnly bool

	StartNano int64
	EndNano   int64
	StepNano  int64
}
