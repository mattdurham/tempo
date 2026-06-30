package reader

// FileLayoutReport is a blockpack data type.
type FileLayoutReport struct {
	Sections        []FileLayoutSection `json:"sections"`
	RangeIndex      []RangeIndexColumn  `json:"range_index,omitempty"`
	SketchIndex     *SketchIndexInfo    `json:"sketch_index,omitempty"`
	BlockSpanCounts []uint32            `json:"block_span_counts,omitempty"`
	FileSize        int64               `json:"file_size"`
	TotalSpans      int64               `json:"total_spans"`
	BlockCount      int                 `json:"block_count"`
	FileVersion     uint8               `json:"file_version"`
}
