package main

type BenchmarkReport struct {
	FileSizes    map[string]int64               `json:"fileSizes,omitempty"`
	Metadata     BenchmarkMetadata              `json:"metadata"`
	OTELDemo     []OTELDemoResult               `json:"otelDemo"`
	Comparisons  []ComparisonResult             `json:"comparisons"`
	Aggregations []AggregationComparisonResult  `json:"aggregations"`
	WritePath    []WritePathResult              `json:"writePath,omitempty"`
	TraceMetrics []TraceMetricsComparisonResult `json:"traceMetrics,omitempty"`
}
