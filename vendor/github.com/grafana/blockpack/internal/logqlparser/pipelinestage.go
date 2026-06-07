package logqlparser

// PipelineStage is a blockpack data type.
type PipelineStage struct {
	LabelFilter *LabelFilter
	OrFilters   []*LabelFilter
	Params      []string
	Type        PipelineStageType
}
