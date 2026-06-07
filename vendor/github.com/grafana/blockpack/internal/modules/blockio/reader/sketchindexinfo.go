package reader

// SketchIndexInfo is a blockpack data type.
type SketchIndexInfo struct {
	Blocks             []BlockSketchSummary `json:"blocks"`
	TotalBytes         int                  `json:"total_bytes"`
	HeaderBytes        int                  `json:"header_bytes"`
	SketchedBlockCount int                  `json:"sketched_block_count"`
}
