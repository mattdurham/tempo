package reader

import "github.com/grafana/blockpack/internal/modules/sectioncache"

// Options configures a Reader at construction time.
type Options struct {
	// Cache, if non-nil, enables caching for footer, header, metadata,
	// compact index, and block reads.
	// If nil, a NopSectionCache is used (all reads are cache misses; writes are discarded).
	// Any sectioncache.SectionCache implementation is accepted: TypedTieredCache,
	// or a FilecacheAdapter wrapping a filecache.Cache implementation.
	Cache sectioncache.SectionCache

	// FileID is the unique identifier for this file within the cache namespace.
	// Typically the file path or object storage key (e.g. "/data/blocks/abc.blockpack").
	// Required when Cache is non-nil; ignored when Cache is nil.
	FileID string
}
