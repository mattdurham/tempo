package reader

// FileLayoutSection is a blockpack data type.
type FileLayoutSection struct {
	Section          string `json:"section"`
	ColumnName       string `json:"column_name,omitempty"`
	ColumnType       string `json:"column_type,omitempty"`
	Encoding         string `json:"encoding,omitempty"`
	MinValue         string `json:"min_value,omitempty"`
	MaxValue         string `json:"max_value,omitempty"`
	Offset           int64  `json:"offset"`
	CompressedSize   int64  `json:"compressed_size"`
	UncompressedSize int64  `json:"uncompressed_size,omitempty"`
	BlockIndex       int    `json:"block_index,omitempty"`
	RowCount         int    `json:"row_count,omitempty"`
	IsLogical        bool   `json:"is_logical,omitempty"`
}
