package blockpack

// TraceMetricOptions is a blockpack data type.
type TraceMetricOptions struct {
	// ValueIndex, when non-nil, enables the zero-block-read metrics path
	// (NOTE-VI-033, issue #460): count_over_time()/rate() without group-by are
	// answered from value-index TimeSec alone. ExecuteMetricsTraceQL tries this
	// path first and falls back to a full block scan when the query is unsupported
	// or the index lacks coverage. Wired by the querier (issue #461).
	ValueIndex ValueIndexSource

	StartNano int64
	EndNano   int64
	StepNano  int64
}
