package queryplanner

// Plan is a blockpack data type.
type Plan struct {
	Explain            string
	SelectedBlocks     []int
	TotalBlocks        int
	PrunedByIndex      int
	PrunedByTime       int
	PrunedByColStats   int // NOTE-449: blocks eliminated by per-block column statistics (issue #368)
	PrunedByFileBounds int // NOTE-456: blocks eliminated by file-level range/bloom reject
	// PrunedByColumnBloom is the number of blocks eliminated by column-presence bloom
	// checks (issue #531): a block whose predicate-required column(s) are all
	// definitively absent per BlockIndexer.MayContainColumn. Distinct from the
	// value-based pruning NOTE(#439) removed — this only proves column ABSENCE, never a
	// value match.
	PrunedByColumnBloom int
	Limit               int
	Direction           Direction
}
