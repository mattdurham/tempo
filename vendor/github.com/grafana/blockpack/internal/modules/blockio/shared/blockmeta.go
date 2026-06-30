package shared

// BlockMeta is a blockpack data type.
type BlockMeta struct {
	Offset     uint64
	Length     uint64
	MinStart   uint64
	MaxStart   uint64
	SpanCount  uint32
	MinTraceID [16]byte
	MaxTraceID [16]byte
	Kind       BlockKind
	// PageNum is the block's start offset divided by 4096 for v2 files (NOTE-V2-001).
	// Zero for v1 files (where alignment is not guaranteed).
	PageNum uint32
}
