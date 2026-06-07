package logqlparser

import "time"

// MetricExpr is a blockpack data type.
type MetricExpr struct {
	Selector      *LogSelector
	Function      string
	Unwrap        string
	Pipeline      []PipelineStage
	RangeDuration time.Duration
}
