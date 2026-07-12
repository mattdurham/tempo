package executor

// StructuralResult is the output of ExecuteStructural.
type StructuralResult struct {
	Matches    []SpanMatch
	BlocksRead int
}
