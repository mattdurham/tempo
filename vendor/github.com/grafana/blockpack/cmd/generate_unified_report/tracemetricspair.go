package main

type traceMetricsPair struct {
	Blockpack *TraceMetricsComparisonResult
	Parquet   *TraceMetricsComparisonResult
	QueryName string
}
