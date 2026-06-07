package rw

// ReaderProvider is a blockpack data type.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}
