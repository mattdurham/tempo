package compaction

// OutputStorage is a blockpack data type.
type OutputStorage interface {
	Put(path string, data []byte) error
}
