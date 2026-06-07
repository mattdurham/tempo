package queryplanner

// Plan is a blockpack data type.
type Plan struct {
	BlockScores    []float64
	Explain        string
	SelectedBlocks []int
	TotalBlocks    int
	PrunedByIndex  int
	PrunedByTime   int
	PrunedByFuse   int
	Limit          int
	Direction      Direction
}
