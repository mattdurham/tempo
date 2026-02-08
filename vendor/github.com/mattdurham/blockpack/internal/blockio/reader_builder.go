package blockio

import (
	"fmt"
	"time"
)

// ReaderOption configures a Reader during construction
type ReaderOption func(*readerConfig)

// readerConfig holds configuration for Reader construction
type readerConfig struct {
	// Caching options
	enableCaching bool
	maxCacheSize  int64
	dataAware     bool

	// Memcache options
	enableMemcache bool
	memcacheClient MemcacheClient
	memcachePrefix string
	memcacheExpire int32

	// Tracking options
	enableTracking  bool
	trackingLatency time.Duration
}

// defaultReaderConfig returns sensible defaults
func defaultReaderConfig() readerConfig {
	return readerConfig{
		enableCaching:   false,
		maxCacheSize:    100 * 1024 * 1024, // 100 MB default
		dataAware:       true,              // Enable data-aware caching by default
		enableTracking:  false,
		trackingLatency: 0,
	}
}

// WithCaching enables caching with the specified maximum cache size in bytes
func WithCaching(maxSizeBytes int64) ReaderOption {
	return func(c *readerConfig) {
		c.enableCaching = true
		c.maxCacheSize = maxSizeBytes
	}
}

// WithDataAwareCaching enables data-aware caching (region detection, access pattern tracking)
// This is enabled by default when using WithCaching. Use WithBasicCaching to disable.
func WithDataAwareCaching(maxSizeBytes int64) ReaderOption {
	return func(c *readerConfig) {
		c.enableCaching = true
		c.maxCacheSize = maxSizeBytes
		c.dataAware = true
	}
}

// WithBasicCaching enables basic LRU caching without data awareness
// Use this if you don't need region detection or access pattern tracking
func WithBasicCaching(maxSizeBytes int64) ReaderOption {
	return func(c *readerConfig) {
		c.enableCaching = true
		c.maxCacheSize = maxSizeBytes
		c.dataAware = false
	}
}

// WithTracking enables I/O tracking (bytes read, operations count)
func WithTracking() ReaderOption {
	return func(c *readerConfig) {
		c.enableTracking = true
	}
}

// WithLatencySimulation adds artificial latency to simulate object storage (e.g., S3)
// Typical S3 first-byte latency is 10-20ms
func WithLatencySimulation(latency time.Duration) ReaderOption {
	return func(c *readerConfig) {
		c.enableTracking = true
		c.trackingLatency = latency
	}
}

// WithMemcaching enables memcache-backed caching for distributed/shared cache.
// This is useful for:
// - Larger cache sizes that don't fit in process memory
// - Sharing cache across multiple processes or machines
// - Persistent cache across process restarts
//
// The client parameter should implement the MemcacheClient interface.
// For testing, use NewInMemoryMemcacheClient(). For production, use a real
// memcache client like github.com/bradfitz/gomemcache.
//
// Example:
//
//	memcacheClient := NewInMemoryMemcacheClient()
//	reader, err := NewReaderFromProvider(provider,
//	    WithMemcaching(memcacheClient, "myapp:", 3600))  // 1 hour expiration
func WithMemcaching(client MemcacheClient, keyPrefix string, expiration int32) ReaderOption {
	return func(c *readerConfig) {
		c.enableMemcache = true
		c.memcacheClient = client
		c.memcachePrefix = keyPrefix
		c.memcacheExpire = expiration
	}
}

// NewReaderFromProvider creates a Reader from a ReaderProvider with optional configuration.
// This is the recommended way to create readers.
//
// Example usage:
//
//	// Simple file reading with caching
//	reader, err := NewReaderFromProvider(provider,
//	    WithCaching(100 * 1024 * 1024), // 100 MB cache
//	    WithTracking(),
//	)
//
//	// S3 simulation with latency
//	reader, err := NewReaderFromProvider(provider,
//	    WithDataAwareCaching(500 * 1024 * 1024), // 500 MB cache
//	    WithLatencySimulation(15 * time.Millisecond),
//	)
//
//	// Basic usage without caching
//	reader, err := NewReaderFromProvider(provider)
func NewReaderFromProvider(provider ReaderProvider, opts ...ReaderOption) (*Reader, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}

	// Apply default config
	config := defaultReaderConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&config)
	}

	// Build provider stack based on config
	// Stack order (outer to inner):
	// - Data-aware caching (or basic caching)
	// - Memcache caching
	// - Tracking
	// - Base provider
	var p = provider

	// Layer 1: Tracking (if enabled)
	if config.enableTracking {
		if config.trackingLatency > 0 {
			p = NewTrackingReaderProviderWithLatency(p, config.trackingLatency)
		} else {
			p = NewTrackingReaderProvider(p)
		}
	}

	// Layer 2: Memcache (if enabled)
	if config.enableMemcache {
		memcacheConfig := MemcacheProviderConfig{
			Client:     config.memcacheClient,
			KeyPrefix:  config.memcachePrefix,
			Expiration: config.memcacheExpire,
		}
		memcacheProvider, err := NewMemcacheReaderProvider(p, memcacheConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create memcache provider: %w", err)
		}
		p = memcacheProvider
	}

	// Layer 3: In-process caching (if enabled)
	if config.enableCaching {
		cacheConfig := CachingProviderConfig{
			MaxCacheSize: config.maxCacheSize,
		}

		if config.dataAware {
			// Use data-aware caching
			dataAwareProvider, err := NewDataAwareCachingProvider(p, cacheConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create data-aware caching provider: %w", err)
			}
			p = dataAwareProvider
		} else {
			// Use basic caching
			cachingProvider, err := NewCachingReaderProvider(p, cacheConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create caching provider: %w", err)
			}
			p = cachingProvider
		}
	}

	// Create reader from final provider
	return newReaderFromProvider(p)
}

// NewReaderFromBytes creates a Reader from an in-memory byte slice.
// For better performance with large data, consider using NewReaderFromProvider
// with a custom provider implementation.
func NewReaderFromBytes(data []byte, opts ...ReaderOption) (*Reader, error) {
	return NewReaderFromProvider(&byteSliceProvider{data: data}, opts...)
}

// newReaderFromProvider is the internal constructor that does the actual parsing
// This is called after the provider stack is built
// It wraps the existing NewReaderWithCache logic
func newReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	// Use the existing NewReaderWithCache with empty name and nil cache
	// The provider already has caching built in if configured
	return NewReaderWithCache("", provider, nil)
}
