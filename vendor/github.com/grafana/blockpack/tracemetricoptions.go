package blockpack

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
	// timestamps). Wired by the querier (issue #461).
	ValueIndex ValueIndexSource

	StartNano int64
	EndNano   int64
	StepNano  int64
}
