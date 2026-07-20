package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// SPEC-VIS-2 (revised, issue #481 part 3): unconditional metrics decline contract — the four
// sentinels below (production-default, not just under TraceMetricOptions.IndexOnly). Contrast
// with the root re-export's SPEC-ROOT-019 (revised) tag in tracemetricoptions.go: SPEC-ROOT-019
// is the root-level umbrella, SPEC-VIS-2 is this package's mechanism-level detail underneath it.
//
// NOTE-VI-097: design rationale for plain sentinel errors over the originally-shipped
// DeclineReason enum + ErrValueIndexDecline struct (team-lead's enum→sentinels correction), and
// for why ErrMetricsValueIndexDisabled is a distinct category from ErrMetricsNoCoverage rather
// than folded into it (R8) — see executor/NOTES.md for the full rationale.

import "errors"

// ErrMetricsShapeNotAnswerable (issue #481 part 3, decline category 1c) is returned when a
// metrics query uses an aggregate function or group-by combination the value index cannot
// answer (only count_over_time()/rate() without group-by are index-answerable —
// vm.MetricsShapeIsVIAnswerable). Deterministic per query text, not per block: SUM/AVG/MIN/MAX/
// HISTOGRAM/QUANTILE/STDDEV and any group-by always decline for this reason, on every block.
// There is no full-block-scan fallback (ExecuteTraceMetrics, the scan engine, was deleted
// outright) — this is the PRODUCTION DEFAULT decline contract, not just under
// TraceMetricOptions.IndexOnly. Mirrors ErrStructuralIndexCoverageGap's plain-sentinel style
// (structural_errors.go); use errors.Is to detect it.
var ErrMetricsShapeNotAnswerable = errors.New("executor: metrics shape not answerable from value index")

// ErrMetricsNoCoverage (issue #481 part 3, decline category 1b) is returned when a leaf column
// in a metrics query's predicate has no value-index coverage — an unindexable/negation
// predicate, SPEC-ROOT-019's documented exception. This is a per-QUERY-SHAPE limitation: the
// value index is enabled and configured, but this specific predicate/column isn't covered by
// it. Contrast with ErrMetricsValueIndexDisabled (R8's distinct zeroth/config-level category):
// that one fires when the value index isn't configured AT ALL for this call, an operator
// config action affecting every metrics query, not a per-query-shape limitation. There is no
// full-block-scan fallback for either. Use errors.Is to detect it.
var ErrMetricsNoCoverage = errors.New("executor: metrics query has no value-index coverage")

// ErrMetricsLegacyTimeSecZero (issue #481 part 3, decline category 1d) is returned when a
// matched span's indexed TimeSec is 0, meaning the block predates per-span timestamps — its
// time bucket is unknown and the span cannot be safely placed. Per-block-data-driven (not
// query-shape-driven): heterogeneous across blocks written before vs. after per-span
// timestamps were introduced. There is no full-block-scan fallback. Use errors.Is to detect it.
var ErrMetricsLegacyTimeSecZero = errors.New("executor: metrics query matched a legacy block with no per-span timestamps")

// ErrMetricsValueIndexDisabled (issue #481 part 3, team-lead ruling R8) is returned when
// ExecuteMetricsTraceQL is called with no ValueIndexSource supplied at all (opts.ValueIndex ==
// nil) — the value index is not configured for this call, a DISTINCT zeroth/config-level
// category from ErrMetricsNoCoverage's per-query-shape "this leaf isn't covered" limitation.
// Since ExecuteTraceMetrics (the full-block-scan fallback) was deleted outright (R4), there is
// no scan left to fall back to, so this is now a typed error rather than a silent scan. An
// operator sees this on every metrics query when value_index_query is disabled for a querier —
// contrast with ErrMetricsNoCoverage, which only affects queries touching a specific
// uncovered column. Use errors.Is to detect it.
var ErrMetricsValueIndexDisabled = errors.New("executor: metrics requires value_index_query.enabled")
