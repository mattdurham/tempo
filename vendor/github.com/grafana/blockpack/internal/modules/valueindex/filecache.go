package valueindex

// filecache.go — in-process value-index file-discovery cache (NOTE-VI-034, issue #462).
//
// DiscoverIndexFiles (issue #458) performs a live S3 LIST on every call. At 100+
// queries/second over ~700 column directories that is far too expensive. The
// filenames already embed wall-clock time ranges (L<level>-<minSec>-<maxSec>-<id>),
// so once a directory listing is cached the per-query time-range filter is O(1)
// per file — no S3 round-trip required.
//
// IndexFileCache caches the *parsed* listing (a []FileMeta with full keys) per
// (colHash, colType) and refreshes it in the background. FilesForTimeRange then
// filters the cached metas in memory and returns the matching keys sorted exactly
// as DiscoverIndexFiles would.
//
// Lifetime: one IndexFileCache per querier process (a singleton, like the
// value-index store). Each querier lists independently; no shared cache.

import (
	"context"
	"path"
	"sync"
	"time"
)

// defaultIndexFileCacheTTL is the refresh interval used when NewIndexFileCache is
// given a non-positive ttl. 30s matches the issue's tolerance: a new L0 file may
// not appear in queries for up to one TTL, which is acceptable because blocks take
// ~90s to flush anyway.
const defaultIndexFileCacheTTL = 30 * time.Second

// colKey identifies a single value-index column directory.
type colKey struct {
	colHash     string
	colTypeName string
}

// colEntry is the cached, parsed listing for one column directory.
type colEntry struct {
	// metas holds every value-index FileMeta in the directory with its FULL key
	// in Filename (so callers can Get/Download directly), sorted by
	// SortFileMetas (Level ASC, WallMinSec ASC, WallMaxSec ASC).
	metas []FileMeta
	// accessed is set on every FilesForTimeRange hit and cleared by the background
	// sweep; it lets the sweep skip cold columns that have not been queried since
	// the previous refresh.
	accessed bool
}

// IndexFileCache maintains a per-(colHash, colType) listing of value-index files,
// refreshed in the background. It wraps DiscoverIndexFiles' Lister and replaces
// the live-per-query S3 LIST with a cached, periodically refreshed listing.
type IndexFileCache struct {
	lister      Lister
	entries     map[colKey]*colEntry
	tenant      string
	indexPrefix string
	ttl         time.Duration
	mu          sync.Mutex
}

// NewIndexFileCache builds an IndexFileCache over lister for a single tenant and
// index prefix. A non-positive ttl uses defaultIndexFileCacheTTL.
func NewIndexFileCache(lister Lister, tenant, indexPrefix string, ttl time.Duration) *IndexFileCache {
	if ttl <= 0 {
		ttl = defaultIndexFileCacheTTL
	}
	return &IndexFileCache{
		lister:      lister,
		tenant:      tenant,
		indexPrefix: indexPrefix,
		ttl:         ttl,
		entries:     make(map[colKey]*colEntry),
	}
}

// FilesForTimeRange returns the cached value-index file keys for the given column
// that overlap [queryMinSec, queryMaxSec], sorted by (Level ASC, WallMinSec ASC,
// WallMaxSec ASC) — identical ordering to DiscoverIndexFiles.
//
// On a cold miss (no cached listing for this column) it performs a synchronous
// LIST + parse and caches the result. On a warm hit it filters the cached metas
// in memory with no S3 round-trip. Marks the column accessed so the background
// sweep keeps it fresh.
//
// Returns (nil, nil) when no files overlap the window.
func (c *IndexFileCache) FilesForTimeRange(
	ctx context.Context,
	colHash, colTypeName string,
	queryMinSec, queryMaxSec uint64,
) ([]string, error) {
	key := colKey{colHash: colHash, colTypeName: colTypeName}

	c.mu.Lock()
	entry := c.entries[key]
	c.mu.Unlock()

	if entry == nil {
		// Cold miss: synchronous list + parse, then cache.
		metas, err := c.listColumn(ctx, colHash, colTypeName)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		// Another goroutine may have populated it concurrently; prefer the existing
		// entry to keep a single source of truth, but still mark it accessed.
		if existing := c.entries[key]; existing != nil {
			existing.accessed = true
			metas = existing.metas
		} else {
			c.entries[key] = &colEntry{
				metas:    metas,
				accessed: true,
			}
		}
		c.mu.Unlock()
		return filterMetas(metas, queryMinSec, queryMaxSec), nil
	}

	// Warm hit: filter the cached metas in memory.
	c.mu.Lock()
	entry.accessed = true
	metas := entry.metas
	c.mu.Unlock()
	return filterMetas(metas, queryMinSec, queryMaxSec), nil
}

// Invalidate evicts the cached listing for one column. The compactor calls this
// after merging files so the next query re-lists and observes the merged layout.
func (c *IndexFileCache) Invalidate(colHash, colTypeName string) {
	c.mu.Lock()
	delete(c.entries, colKey{colHash: colHash, colTypeName: colTypeName})
	c.mu.Unlock()
}

// Background starts the periodic refresh goroutine. It returns immediately; the
// goroutine runs until ctx is canceled. Each tick re-lists every column that has
// been accessed since the previous sweep (cold columns are skipped to avoid
// re-listing directories nobody queries).
func (c *IndexFileCache) Background(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.ttl)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.refreshAccessed(ctx)
			}
		}
	}()
}

// refreshAccessed re-lists every column accessed since the previous sweep and
// clears the accessed flag. A column not queried during the next interval will be
// skipped by the following sweep (it stays cached but goes stale until either a
// query touches it again or it is evicted).
func (c *IndexFileCache) refreshAccessed(ctx context.Context) {
	c.mu.Lock()
	toRefresh := make([]colKey, 0, len(c.entries))
	for key, entry := range c.entries {
		if entry.accessed {
			toRefresh = append(toRefresh, key)
			entry.accessed = false
		}
	}
	c.mu.Unlock()

	for _, key := range toRefresh {
		metas, err := c.listColumn(ctx, key.colHash, key.colTypeName)
		if err != nil {
			// Keep the stale listing on error rather than evicting; a transient
			// LIST failure should not blind queries to known files.
			continue
		}
		c.mu.Lock()
		if entry := c.entries[key]; entry != nil {
			entry.metas = metas
		}
		c.mu.Unlock()
	}
}

// listColumn performs a live LIST of one column directory and returns its parsed
// FileMetas with full keys, sorted by SortFileMetas. Malformed keys are skipped
// (same policy as DiscoverIndexFiles). Returns nil when the directory is empty.
func (c *IndexFileCache) listColumn(ctx context.Context, colHash, colTypeName string) ([]FileMeta, error) {
	prefix := path.Join(c.tenant, c.indexPrefix, colHash, colTypeName) + "/"
	keys, err := c.lister.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	metas := make([]FileMeta, 0, len(keys))
	for _, key := range keys {
		meta, parseErr := ParseFilenameV2(path.Base(key))
		if parseErr != nil {
			continue
		}
		// Preserve the full key so callers can Get/Download directly.
		meta.Filename = key
		metas = append(metas, meta)
	}
	if len(metas) == 0 {
		return nil, nil
	}
	SortFileMetas(metas)
	return metas, nil
}

// filterMetas returns the keys of metas overlapping [queryMinSec, queryMaxSec].
// metas must already be sorted by SortFileMetas; the filter preserves that order.
// Returns nil when nothing overlaps (matching DiscoverIndexFiles' contract).
func filterMetas(metas []FileMeta, queryMinSec, queryMaxSec uint64) []string {
	var out []string
	for i := range metas {
		if metas[i].IsInTimeRange(queryMinSec, queryMaxSec) {
			out = append(out, metas[i].Filename)
		}
	}
	return out
}
