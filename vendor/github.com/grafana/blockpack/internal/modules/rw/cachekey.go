package rw

type cacheKey struct {
	readerID string
	offset   int64
	length   int
}
