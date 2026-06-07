package reader

// FileBloomInfo is a blockpack data type.
type FileBloomInfo struct {
	Columns    []FileBloomColumnInfo `json:"columns"`
	TotalBytes int                   `json:"total_bytes"`
}
