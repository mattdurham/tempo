package vm

// QuerySpec is a blockpack data type.
type QuerySpec struct {
	Filter        FilterSpec
	Aggregate     AggregateSpec
	TimeBucketing TimeBucketSpec
}
