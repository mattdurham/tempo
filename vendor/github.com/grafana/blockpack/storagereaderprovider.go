package blockpack

// storageReaderProvider adapts a Storage + path to modules_rw.ReaderProvider.
type storageReaderProvider struct {
	storage Storage
	path    string
}
