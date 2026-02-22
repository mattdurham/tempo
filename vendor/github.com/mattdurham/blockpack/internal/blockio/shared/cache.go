package shared //nolint:revive

// ByteCache provides an interface for caching byte slices read from storage.
// NOTE: This interface is now superseded by the layered provider pattern
// (CachingReaderProvider, DataAwareCachingProvider, MemcacheReaderProvider).
// The new pattern provides better composability and separation of concerns.
// This interface is kept for backward compatibility with existing code.
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
