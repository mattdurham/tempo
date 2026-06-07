package reader

// BlockSketchSummary is a blockpack data type.
type BlockSketchSummary struct {
	Columns []ColumnSketchStat `json:"columns"`
}
