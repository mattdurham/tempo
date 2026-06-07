package blockpack

// WritableStorage is a blockpack data type.
type WritableStorage interface {
	Storage
	Put(path string, data []byte) error
	Delete(path string) error
}
