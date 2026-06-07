package blockpack

// LogQueryOptions is a blockpack data type.
type LogQueryOptions struct {
	StartNano uint64
	EndNano   uint64
	Limit     int
	Forward   bool
}
