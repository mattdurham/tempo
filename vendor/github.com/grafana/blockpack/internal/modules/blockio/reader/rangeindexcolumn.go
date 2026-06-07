package reader

// RangeIndexColumn is a blockpack data type.
type RangeIndexColumn struct {
	ColumnName string             `json:"column_name"`
	ColumnType string             `json:"column_type"`
	BucketMin  string             `json:"bucket_min,omitempty"`
	BucketMax  string             `json:"bucket_max,omitempty"`
	Buckets    []RangeIndexBucket `json:"buckets"`
}
