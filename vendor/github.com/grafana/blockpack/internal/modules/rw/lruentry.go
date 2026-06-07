package rw

type lruEntry struct {
	data []byte
	key  cacheKey
	tier int
}
