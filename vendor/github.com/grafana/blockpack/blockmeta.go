package blockpack

// BlockMeta is a blockpack data type.
type BlockMeta struct {
	MinStartNanos uint64
	MaxStartNanos uint64
	TotalSpans    int
	TotalTraces   int
	BlockCount    int
	Size          int64
}
