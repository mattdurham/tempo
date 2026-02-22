package shared

import (
	"fmt"
	"io"
	"sync"
)

// CachingReaderProvider wraps a ReaderProvider with transparent caching.
// It implements the decorator pattern to add caching to any ReaderProvider.
// Thread-safe for concurrent access.
type CachingReaderProvider struct {
	underlying ReaderProvider
	cache      *LRUByteCache
	size       int64
	mu         sync.RWMutex // Protects size field during initialization
}

// CachingProviderConfig configures a CachingReaderProvider.
type CachingProviderConfig struct {
	MaxCacheSize int64 // Maximum cache size in bytes
}

// NewCachingReaderProvider creates a new caching provider wrapping the given provider.
// The cache is created internally based on the provided config.
func NewCachingReaderProvider(underlying ReaderProvider, config CachingProviderConfig) (*CachingReaderProvider, error) {
	if underlying == nil {
		return nil, fmt.Errorf("underlying provider cannot be nil")
	}

	// Get size from underlying provider
	size, err := underlying.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to get provider size: %w", err)
	}

	// Create cache
	cache, err := NewLRUByteCache(config.MaxCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return &CachingReaderProvider{
		underlying: underlying,
		cache:      cache,
		size:       size,
	}, nil
}

// Size returns the size of the underlying data.
// This is cached at construction time to avoid repeated calls to underlying provider.
func (c *CachingReaderProvider) Size() (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size, nil
}

// ReadAt reads bytes from the provider with transparent caching.
// The dataType parameter is currently not used by basic caching but passed through.
// Cache keys are based on offset and length to ensure correct cache hits.
func (c *CachingReaderProvider) ReadAt(p []byte, off int64, dataType DataType) (int, error) {
	// Validate parameters
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Create cache key from offset and length
	// Format: "range:offset:length" ensures unique keys for different ranges
	cacheKey := c.makeCacheKey(off, len(p))

	// Check cache first
	if cached, found := c.cache.Get(cacheKey); found {
		// Cache hit - copy data and return
		n := copy(p, cached)
		// If cached data is shorter than requested, return io.EOF to match io.ReaderAt semantics
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	// Cache miss - read from underlying provider
	n, err := c.underlying.ReadAt(p, off, dataType)
	if err != nil && n == 0 {
		// Only return error if we got no data
		return 0, err
	}

	// Only cache complete reads to avoid breaking io.ReaderAt semantics on repeated reads
	// Partial reads (n < len(p)) should not be cached
	if n > 0 && n == len(p) && err == nil {
		cached := make([]byte, n)
		copy(cached, p[:n])
		c.cache.Put(cacheKey, cached)
	}

	return n, err
}

// makeCacheKey creates a unique cache key for a byte range.
// Format: "offset:length" ensures different ranges have different keys.
func (c *CachingReaderProvider) makeCacheKey(offset int64, length int) string {
	return fmt.Sprintf("%d:%d", offset, length)
}

// Stats returns cache statistics for monitoring and debugging.
func (c *CachingReaderProvider) Stats() CacheStats {
	return c.cache.Stats()
}

// Close closes the cache and frees resources.
// This implements the CloseableReaderProvider pattern.
func (c *CachingReaderProvider) Close() error {
	c.cache.Close()

	// If underlying provider is closeable, close it too
	if closeable, ok := c.underlying.(interface{ Close() error }); ok {
		return closeable.Close()
	}

	return nil
}

// Reset clears all cached data.
// Useful for testing or when you want to force fresh reads.
// This method is NOT thread-safe and should only be called when no other
// goroutines are accessing the provider (e.g., in tests or during shutdown).
func (c *CachingReaderProvider) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	maxSize := c.cache.Stats().MaxSize
	c.cache.Close()

	// Recreate cache with same config
	cache, err := NewLRUByteCache(maxSize)
	if err != nil {
		return fmt.Errorf("failed to recreate cache: %w", err)
	}
	c.cache = cache
	return nil
}
