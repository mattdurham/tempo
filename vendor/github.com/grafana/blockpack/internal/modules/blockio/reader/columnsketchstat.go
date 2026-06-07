package reader

// ColumnSketchStat is a blockpack data type.
type ColumnSketchStat struct {
	ColumnName     string `json:"column_name"`
	HLLCardinality uint64 `json:"hll_cardinality"`
	FuseBytes      int    `json:"fuse_bytes,omitempty"`
	TopKCount      int    `json:"top_k_count,omitempty"`
	TopKBytes      int    `json:"top_k_bytes,omitempty"`
}
