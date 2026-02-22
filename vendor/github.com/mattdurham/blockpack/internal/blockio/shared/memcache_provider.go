package shared

import (
	"fmt"
)

// MemcacheReaderProvider wraps a ReaderProvider with memcache-backed caching
// This is useful for larger cache sizes that don't fit in process memory,
// or for sharing cache across multiple processes/machines
type MemcacheReaderProvider struct {
	underlying ReaderProvider
	client     MemcacheClient
	keyPrefix  string // Prefix for all cache keys
	expiration int32  // Expiration time in seconds (0 or negative = use default 3600s)
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
func NewMemcacheReaderProvider(
	underlying ReaderProvider,
	config MemcacheProviderConfig,
) (*MemcacheReaderProvider, error) {
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
		return nil, fmt.Errorf(
			"KeyPrefix is required and must be unique per file (e.g., 'blockpack:<filename>:' or 'blockpack:<hash>:')",
		)
	}

	// Warn if using unsafe default prefix
	if config.KeyPrefix == "blockpack:" {
		return nil, fmt.Errorf(
			"KeyPrefix 'blockpack:' is too generic and will cause cache collisions; use a file-specific prefix like 'blockpack:<filename>:'",
		)
	}

	// Validate KeyPrefix length - memcache keys max at 250 bytes
	// We need room for prefix + hash (32 hex chars), so limit prefix to ~200 bytes
	if len(config.KeyPrefix) > 200 {
		return nil, fmt.Errorf(
			"KeyPrefix too long (%d bytes); memcache keys limited to 250 bytes total",
			len(config.KeyPrefix),
		)
	}

	return &MemcacheReaderProvider{
		underlying: underlying,
		client:     config.Client,
		keyPrefix:  config.KeyPrefix,
		expiration: config.Expiration,
	}, nil
}

// InMemoryMemcacheClient is a simple in-memory implementation of MemcacheClient
// Useful for testing and development without requiring a real memcache server
type InMemoryMemcacheClient struct {
	cache map[string][]byte
}

// NewInMemoryMemcacheClient creates a new in-memory memcache client
func NewInMemoryMemcacheClient() *InMemoryMemcacheClient {
	return &InMemoryMemcacheClient{
		cache: make(map[string][]byte),
	}
}
