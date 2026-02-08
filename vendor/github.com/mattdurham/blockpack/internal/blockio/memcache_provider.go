package blockio

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// MemcacheClient defines the interface for a memcache client
// This allows for different implementations (gomemcache, custom, etc.)
type MemcacheClient interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte, expiration int32) error
}

// MemcacheReaderProvider wraps a ReaderProvider with memcache-backed caching
// This is useful for larger cache sizes that don't fit in process memory,
// or for sharing cache across multiple processes/machines
type MemcacheReaderProvider struct {
	underlying ReaderProvider
	client     MemcacheClient
	keyPrefix  string // Prefix for all cache keys
	expiration int32  // Expiration time in seconds (0 or negative = use default 3600s)

	// Statistics
	hits      atomic.Int64
	misses    atomic.Int64
	errors    atomic.Int64
	bytesRead atomic.Int64
}

// MemcacheProviderConfig configures a MemcacheReaderProvider
type MemcacheProviderConfig struct {
	Client MemcacheClient
	// KeyPrefix is REQUIRED and must be unique per file/provider to avoid cache collisions.
	// Use a file-specific identifier like: "blockpack:<filename>:" or "blockpack:<contenthash>:"
	// Cache keys are derived from (prefix, offset, length), so different files with the same
	// offset/length will collide if they share a prefix, causing cross-file data corruption.
	KeyPrefix  string
	Expiration int32 // Expiration in seconds (0 or negative = use default 3600s, positive = specific TTL)
}

// NewMemcacheReaderProvider creates a new memcache-backed caching provider
func NewMemcacheReaderProvider(underlying ReaderProvider, config MemcacheProviderConfig) (*MemcacheReaderProvider, error) {
	if underlying == nil {
		return nil, fmt.Errorf("underlying provider cannot be nil")
	}
	if config.Client == nil {
		return nil, fmt.Errorf("memcache client cannot be nil")
	}

	// Default expiration: 1 hour (for 0 or negative values)
	// Positive values specify exact TTL in seconds
	if config.Expiration <= 0 {
		config.Expiration = 3600
	}

	// KeyPrefix is REQUIRED to avoid cache collisions between different files
	// Each file/provider must have a unique prefix
	if config.KeyPrefix == "" {
		return nil, fmt.Errorf("KeyPrefix is required and must be unique per file (e.g., 'blockpack:<filename>:' or 'blockpack:<hash>:')")
	}

	// Warn if using unsafe default prefix
	if config.KeyPrefix == "blockpack:" {
		return nil, fmt.Errorf("KeyPrefix 'blockpack:' is too generic and will cause cache collisions; use a file-specific prefix like 'blockpack:<filename>:'")
	}

	// Validate KeyPrefix length - memcache keys max at 250 bytes
	// We need room for prefix + hash (32 hex chars), so limit prefix to ~200 bytes
	if len(config.KeyPrefix) > 200 {
		return nil, fmt.Errorf("KeyPrefix too long (%d bytes); memcache keys limited to 250 bytes total", len(config.KeyPrefix))
	}

	return &MemcacheReaderProvider{
		underlying: underlying,
		client:     config.Client,
		keyPrefix:  config.KeyPrefix,
		expiration: config.Expiration,
	}, nil
}

// Size returns the size of the underlying data
func (m *MemcacheReaderProvider) Size() (int64, error) {
	return m.underlying.Size()
}

// ReadAt reads bytes from the provider with memcache-backed caching.
// The dataType parameter is currently not used but could be used for TTL optimization.
func (m *MemcacheReaderProvider) ReadAt(p []byte, off int64, dataType DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Create cache key from offset and length
	cacheKey := m.makeCacheKey(off, len(p))

	// Try to get from memcache
	cached, err := m.client.Get(cacheKey)
	if err == nil && len(cached) > 0 {
		// Cache hit
		m.hits.Add(1)
		m.bytesRead.Add(int64(len(cached)))
		n := copy(p, cached)
		// If cached data is shorter than requested, return io.EOF to match io.ReaderAt semantics
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	// Track cache miss - only count as error if it's not a normal cache miss
	// (Many memcache clients return error for cache miss, which is not a real error)
	if err != nil && !isCacheMissError(err) {
		m.errors.Add(1)
	}
	m.misses.Add(1)

	// Cache miss - read from underlying provider
	n, readErr := m.underlying.ReadAt(p, off, dataType)
	if readErr != nil && n == 0 {
		return 0, readErr
	}

	// Only cache complete reads to avoid breaking io.ReaderAt semantics on repeated reads
	// Partial reads (n < len(p)) should not be cached
	if n > 0 && n == len(p) && readErr == nil {
		toCache := make([]byte, n)
		copy(toCache, p[:n])
		if err := m.client.Set(cacheKey, toCache, m.expiration); err != nil {
			m.errors.Add(1)
			// Log error but don't fail the read
		}
	}

	return n, readErr
}

// makeCacheKey creates a cache key from offset and length
// Uses SHA256 to create a deterministic key that's safe for memcache
func (m *MemcacheReaderProvider) makeCacheKey(offset int64, length int) string {
	// Create a unique key based on prefix, offset, and length
	raw := fmt.Sprintf("%s%d:%d", m.keyPrefix, offset, length)

	// Hash to create a fixed-length key
	hash := sha256.Sum256([]byte(raw))

	// Use hex encoding for memcache-safe keys
	// Memcache keys have a max length of 250 bytes - hash ensures we stay within limits
	return m.keyPrefix + hex.EncodeToString(hash[:16]) // Use first 16 bytes of hash (32 chars hex)
}

// isCacheMissError returns true if the error indicates a cache miss (not a real error)
// This is a simple heuristic - ideally the MemcacheClient interface would have a separate
// method to distinguish between cache misses and real errors
func isCacheMissError(err error) bool {
	// For now, treat any error as a potential cache miss
	// In production, this should check for specific error types from the memcache client
	// e.g., memcache.ErrCacheMiss from github.com/bradfitz/gomemcache
	return err != nil
}

// Stats returns memcache statistics
func (m *MemcacheReaderProvider) Stats() MemcacheStats {
	return MemcacheStats{
		Hits:      m.hits.Load(),
		Misses:    m.misses.Load(),
		Errors:    m.errors.Load(),
		BytesRead: m.bytesRead.Load(),
	}
}

// Close closes the underlying provider if it implements Close
func (m *MemcacheReaderProvider) Close() error {
	if closeable, ok := m.underlying.(interface{ Close() error }); ok {
		return closeable.Close()
	}
	return nil
}

// MemcacheStats contains memcache performance statistics
type MemcacheStats struct {
	Hits      int64 // Number of cache hits
	Misses    int64 // Number of cache misses
	Errors    int64 // Number of memcache errors
	BytesRead int64 // Total bytes read from cache
}

// HitRate returns the cache hit rate as a percentage (0-100)
func (s MemcacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total) * 100
}

// InMemoryMemcacheClient is a simple in-memory implementation of MemcacheClient
// Useful for testing and development without requiring a real memcache server
type InMemoryMemcacheClient struct {
	cache map[string][]byte
	mu    sync.RWMutex
}

// NewInMemoryMemcacheClient creates a new in-memory memcache client
func NewInMemoryMemcacheClient() *InMemoryMemcacheClient {
	return &InMemoryMemcacheClient{
		cache: make(map[string][]byte),
	}
}

// Get retrieves a value from the cache
func (c *InMemoryMemcacheClient) Get(key string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, found := c.cache[key]
	if !found {
		return nil, fmt.Errorf("cache miss")
	}

	// Return a copy to prevent external modification
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Set stores a value in the cache
func (c *InMemoryMemcacheClient) Set(key string, value []byte, expiration int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Store a copy to prevent external modification
	stored := make([]byte, len(value))
	copy(stored, value)
	c.cache[key] = stored

	return nil
}

// Clear removes all entries from the cache
func (c *InMemoryMemcacheClient) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string][]byte)
}

// Size returns the number of entries in the cache
func (c *InMemoryMemcacheClient) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}
