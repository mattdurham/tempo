package main

type AggregationComparison struct {
	Aggregation *AggregationComparisonResult
	Parquet     *AggregationComparisonResult
	QueryName   string
}
