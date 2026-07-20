package valueindexcompactor

import (
	"context"
	"sync"

	"github.com/grafana/blockpack/internal/modules/pgcatalog"
)

// NOTE-VI-017: see internal/modules/valueindexcompactor/NOTES.md.
// Any changes to this file must be reflected there.

// IndexObject is a key+size pair returned by IndexStore.List.
type IndexObject struct {
	Key  string
	Size int64
}

// IndexStore is the object-storage surface the compactor needs: list keys under
// a prefix, read and write whole objects, and delete compacted inputs. It is
// exported so external callers (tempo) can supply their object store through the
// public valueindexcompactor subpackage.
//
// List must return full object keys (not just the leaf names) so the caller can
// Get/Delete them directly. Listing semantics are "all keys whose name begins
// with prefix"; the compactor groups results by directory itself.
//
// ListDirs must return only the immediate child directory prefixes (each ending
// in "/") one level below prefix, non-recursively. It is used by compactTenant
// to walk the col-hash and type levels without loading the full file listing.
type IndexStore interface {
	// List returns the full keys and sizes of all objects whose name begins
	// with prefix.
	List(ctx context.Context, prefix string) ([]IndexObject, error)
	// ListDirs returns the immediate child directory prefixes (ending in "/")
	// one level below prefix, without recursing into them.
	ListDirs(ctx context.Context, prefix string) ([]string, error)
	// Peek reads the first n bytes of the object at key without fetching the
	// whole object. Used to inspect the magic bytes before a full Get.
	Peek(ctx context.Context, key string, n int) ([]byte, error)
	// Get reads the entire object at key.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put writes data to key, creating or overwriting it.
	Put(ctx context.Context, key string, data []byte) error
	// Delete removes the object at key.
	Delete(ctx context.Context, key string) error
}

// CatalogStore is the blockpack_file_catalog capability mergeLevel needs
// (issue #522 Phase 1.1): record a merge output row, and mark the 2+ input
// rows compacted. *pgcatalog.Store satisfies this structurally -- exported
// as a local interface (mirroring IndexStore/SourceExister's own shape)
// rather than importing the concrete type directly at every call site, so
// tests unrelated to catalog wiring never need a real Postgres connection.
//
// SPEC-VI-10 / NOTE-VI-122: when Config.CatalogStore is nil, mergeLevel
// falls back to its pre-#522 delete-input behavior unchanged (mirrors
// SourceExister's own nil-tolerance convention immediately below) -- a
// deliberate, documented choice to keep issue #522's Phase 1.1 rollout
// additive/optional rather than forcing every one of this package's
// existing, catalog-unrelated tests (corruption handling, disk staging,
// memory bounds, concurrency, metrics) to thread through a fake catalog
// store they don't care about. See NOTES.md NOTE-VI-122.
type CatalogStore interface {
	Insert(ctx context.Context, row pgcatalog.Row) error
	MarkCompacted(ctx context.Context, objectKeys []string) error
}

// SourceExister reports whether a source blockpack object still exists in object
// storage. The production implementation is an S3 HEAD; tests substitute a fake.
// It is the low-level probe wrapped by cachingRefChecker for batching/caching.
type SourceExister interface {
	// Exists reports whether the object at sourceRef exists. A retention-deleted
	// blockpack returns false.
	Exists(ctx context.Context, sourceRef string) (bool, error)
}

// cachingRefChecker adapts a SourceExister to valueindex.RefChecker, caching the
// existence of each unique source path for the duration of one compaction job so
// each source is probed at most once. It is safe for concurrent use.
type cachingRefChecker struct {
	exister SourceExister
	cache   map[string]bool
	mu      sync.Mutex
}

// newCachingRefChecker wraps exister with a fresh per-job cache.
func newCachingRefChecker(exister SourceExister) *cachingRefChecker {
	return &cachingRefChecker{exister: exister, cache: make(map[string]bool)}
}

// IsLive reports whether sourceRef still exists, consulting the per-job cache
// first so each unique source is probed at most once.
func (c *cachingRefChecker) IsLive(ctx context.Context, sourceRef string) (bool, error) {
	c.mu.Lock()
	if live, ok := c.cache[sourceRef]; ok {
		c.mu.Unlock()
		return live, nil
	}
	c.mu.Unlock()

	live, err := c.exister.Exists(ctx, sourceRef)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	c.cache[sourceRef] = live
	c.mu.Unlock()
	return live, nil
}
