package reader

import "github.com/grafana/blockpack/internal/modules/filecache"

// Options configures a Reader at construction time.
type Options struct {
	// Cache, if non-nil, enables caching for footer, header, metadata,
	// compact index, and block reads. A nil Cache disables caching.
	// Any filecache.Cache implementation is accepted: FileCache (disk),
	// memorycache.MemoryCache (in-process), memcache.MemCache (remote),
	// or chaincache.ChainedCache (multi-tier).
	Cache filecache.Cache

	// FileID is the unique identifier for this file within the cache namespace.
	// Typically the file path or object storage key (e.g. "/data/blocks/abc.blockpack").
	// Required when Cache is non-nil; ignored when Cache is nil.
	FileID string
}
