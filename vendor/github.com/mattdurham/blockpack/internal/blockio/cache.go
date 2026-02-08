package blockio

// BOT: I would prefer this and the other storage provider interfaces to be in a separate package from the core types and encoding logic. This way we can have multiple implementations (e.g. in-memory, on-disk, cloud storage) without coupling them to the core data structures. We can also avoid exposing internal types and encoding details to users of the storage layer.
//
// ByteCache provides an interface for caching byte slices read from storage.
// Implementations must be thread-safe for concurrent access.
type ByteCache interface {
	// Get retrieves cached data for the given key.
	// Returns the data and true if found, nil and false otherwise.
	Get(key string) ([]byte, bool)

	// Put stores data in the cache with the given key.
	// The cache may evict other entries to make room.
	Put(key string, data []byte)

	// Stats returns current cache statistics.
	Stats() CacheStats
}

// CacheStats contains metrics about cache performance and utilization.
type CacheStats struct {
	Hits        int64 // Number of successful Get operations
	Misses      int64 // Number of failed Get operations
	Evictions   int64 // Number of bytes evicted from cache
	CurrentSize int64 // Current size of cache in bytes
	MaxSize     int64 // Maximum size limit in bytes
}
