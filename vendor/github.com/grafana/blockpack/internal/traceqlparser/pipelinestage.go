package traceqlparser

// PipelineStage is a blockpack data type.
type PipelineStage struct {
	ThresholdVal interface{}
	Aggregate    AggregateFunc
	By           []string
	Select       []string
	ThresholdOp  BinaryOp
	HasThreshold bool
}
