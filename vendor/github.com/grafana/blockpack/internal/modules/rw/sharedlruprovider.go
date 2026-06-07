package rw

// SharedLRUProvider is a blockpack data type.
type SharedLRUProvider struct {
	underlying ReaderProvider
	cache      *SharedLRUCache
	readerID   string
}
