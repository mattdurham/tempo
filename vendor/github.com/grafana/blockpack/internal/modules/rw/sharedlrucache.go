package rw

import (
	"container/list"
	"sync"
)

// SharedLRUCache is a blockpack data type.
type SharedLRUCache struct {
	lists    [numCacheTiers]*list.List
	index    map[cacheKey]*list.Element
	maxBytes int64
	curBytes int64
	mu       sync.Mutex
}
