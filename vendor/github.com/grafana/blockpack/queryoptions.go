package blockpack

// QueryOptions is a blockpack data type.
type QueryOptions struct {
	SelectColumns []string
	StartNano     uint64
	EndNano       uint64
	Limit         int
	StartBlock    int
	BlockCount    int
	MostRecent    bool
}
