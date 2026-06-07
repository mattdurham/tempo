package reader

// RangeIndexBucket is a blockpack data type.
type RangeIndexBucket struct {
	Start    string   `json:"start"`
	End      string   `json:"end,omitempty"`
	BlockIDs []uint32 `json:"block_ids"`
}
