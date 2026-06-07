package blockpack

// CloseableReaderProvider is a blockpack data type.
type CloseableReaderProvider interface {
	ReaderProvider
	Close() error
}
