package queryplanner

// Plan is a blockpack data type.
type Plan struct {
	Explain              string
	SelectedBlocks       []int
	TotalBlocks          int
	PrunedByIndex        int
	PrunedByTime         int
	PrunedByColStats     int // NOTE-449: blocks eliminated by per-block column statistics (issue #368)
	PrunedByIntrinsicTOC int // NOTE-449: blocks eliminated by intrinsic-column TOC intersection
	PrunedByFileBounds   int // NOTE-456: blocks eliminated by file-level range/bloom reject
	Limit                int
	Direction            Direction
}
