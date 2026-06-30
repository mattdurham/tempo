package reader

// FileLayoutReport is a blockpack data type.
type FileLayoutReport struct {
	Sections        []FileLayoutSection `json:"sections"`
	BlockSpanCounts []uint32            `json:"block_span_counts,omitempty"`
	FileSize        int64               `json:"file_size"`
	TotalSpans      int64               `json:"total_spans"`
	BlockCount      int                 `json:"block_count"`
	FileVersion     uint8               `json:"file_version"`
}
