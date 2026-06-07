package reader

// FileBloomColumnInfo is a blockpack data type.
type FileBloomColumnInfo struct {
	ColumnName string `json:"column_name"`
	FuseBytes  int    `json:"fuse_bytes"`
}
