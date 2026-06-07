package logqlparser

// LogSelector is a blockpack data type.
type LogSelector struct {
	Matchers    []LabelMatcher
	LineFilters []LineFilter
	Pipeline    []PipelineStage
}
