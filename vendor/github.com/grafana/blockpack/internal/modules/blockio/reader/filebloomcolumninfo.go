package reader

// FileBloomColumnInfo is a stub type retained for API compatibility after bloom removal (#437).
type FileBloomColumnInfo struct {
	ColumnName string `json:"column_name"`
	FuseBytes  int    `json:"fuse_bytes"`
}
