package shared

// PagedIntrinsicTOC is a blockpack data type.
type PagedIntrinsicTOC struct {
	Pages         []PageMeta
	BlockIdxWidth uint8
	RowIdxWidth   uint8
	Format        uint8
	ColType       ColumnType
}
