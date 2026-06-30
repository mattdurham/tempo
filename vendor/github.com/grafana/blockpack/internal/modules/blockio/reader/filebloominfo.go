package reader

// FileBloomInfo is a stub type retained for API compatibility after bloom removal (#437).
type FileBloomInfo struct {
	Columns    []FileBloomColumnInfo `json:"columns"`
	TotalBytes int                   `json:"total_bytes"`
}
