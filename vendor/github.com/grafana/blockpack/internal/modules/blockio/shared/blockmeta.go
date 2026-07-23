package shared

// BlockMeta is a blockpack data type.
type BlockMeta struct {
	// ColumnBloom is a fixed ColumnBloomBytes-length bloom filter over the names of every
	// column actually present in this block (issue #531, NOTE-COLUMNBLOOM-1). Nil/empty for
	// blocks written before this field existed, or by any writer that never computed it —
	// callers MUST treat that as "no information, don't prune, must fetch," never as
	// "definitely absent" (see reader.Reader.MayContainColumn). Populated eagerly from the
	// file's own ToCSubTypeColumnBloom section (a SEPARATE optional section from the block
	// index, not inlined into its fixed-layout entries — that keeps this field fully
	// additive/backward-compatible, unlike the old 2026-03-07-removed ColumnNameBloom which
	// was inlined and therefore a breaking wire-format change).
	ColumnBloom []byte
	Offset      uint64
	Length      uint64
	MinStart    uint64
	MaxStart    uint64
	SpanCount   uint32
	// PageNum is the block's start offset divided by 4096 for v2 files (NOTE-V2-001).
	// Zero for v1 files (where alignment is not guaranteed).
	PageNum    uint32
	MinTraceID [16]byte
	MaxTraceID [16]byte
	Kind       BlockKind
}
