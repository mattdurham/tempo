package reader

import (
	"fmt"

	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// ReaderOption configures a Reader during construction
type ReaderOption func(*readerConfig) //nolint:revive

// readerConfig holds configuration for Reader construction
type readerConfig struct {
	maxCacheSize int64

	// Caching options
	enableCaching bool
	dataAware     bool

	// Tracking options
	enableTracking bool
}

// defaultReaderConfig returns sensible defaults
func defaultReaderConfig() readerConfig {
	return readerConfig{
		enableCaching:  false,
		maxCacheSize:   100 * 1024 * 1024, // 100 MB default
		dataAware:      true,              // Enable data-aware caching by default
		enableTracking: false,
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
	// - Tracking
	// - Base provider
	p := provider

	// Layer 1: Tracking (if enabled)
	if config.enableTracking {
		var err error
		p, err = shared.NewTrackingReaderProvider(p)
		if err != nil {
			return nil, fmt.Errorf("create tracking provider: %w", err)
		}
	}

	// Layer 2: In-process caching (if enabled)
	if config.enableCaching {
		cacheConfig := shared.CachingProviderConfig{
			MaxCacheSize: config.maxCacheSize,
		}

		if config.dataAware {
			// Use data-aware caching
			dataAwareProvider, err := shared.NewDataAwareCachingProvider(p, cacheConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create data-aware caching provider: %w", err)
			}
			p = dataAwareProvider
		} else {
			// Use basic caching
			cachingProvider, err := shared.NewCachingReaderProvider(p, cacheConfig)
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
