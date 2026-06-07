package rw

import "sync"

// RangeCachingProvider is a blockpack data type.
type RangeCachingProvider struct {
	underlying ReaderProvider
	cache      []cachedRange
	mu         sync.RWMutex
}
