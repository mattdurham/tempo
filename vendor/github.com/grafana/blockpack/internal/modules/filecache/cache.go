package filecache

// Cache is the common interface implemented by all blockpack cache tiers:
// FileCache (disk), memorycache.MemoryCache (in-process), memcache.MemCache (remote),
// and chaincache.ChainedCache (multi-tier).
//
// A nil Cache is NOT safe to use; callers that may receive a nil value should
// check for nil before calling methods, or use NopCache as a safe no-op stand-in.
type Cache interface {
	// Get returns the cached bytes for key, (nil, false, nil) on a miss.
	// The returned slice is an independent copy safe for the caller to modify.
	Get(key string) ([]byte, bool, error)

	// Put stores key→value. Implementations may silently drop entries that
	// exceed their configured size limit.
	Put(key string, value []byte) error

	// GetOrFetch returns the cached value for key; on a miss it calls fetch(),
	// stores the result, and returns it. Implementations are encouraged to
	// deduplicate concurrent fetches for the same key via singleflight.
	GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error)

	// Close releases any resources held by the cache.
	// Implementations must be safe to call on a nil receiver.
	Close() error
}

// nopCache is a no-op Cache that always delegates to the fetch function and
// never stores anything. Used internally when no cache is configured.
type nopCache struct{}

func (nopCache) Get(_ string) ([]byte, bool, error) { return nil, false, nil }
func (nopCache) Put(_ string, _ []byte) error       { return nil }
func (nopCache) GetOrFetch(_ string, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}
func (nopCache) Close() error { return nil }

// NopCache is a Cache that never stores anything and always calls the fetch
// function. Use it as a drop-in when no cache is desired.
var NopCache Cache = nopCache{}
