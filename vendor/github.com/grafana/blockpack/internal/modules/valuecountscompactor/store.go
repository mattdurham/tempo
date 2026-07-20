package valuecountscompactor

import (
	"context"

	"github.com/grafana/blockpack/internal/modules/pgcatalog"
)

// NOTE: see internal/modules/valuecountscompactor/NOTES.md.
// Any changes to this file must be reflected there.

// Object is a key+size pair returned by Store.List.
type Object struct {
	Key  string
	Size int64
}

// Store is the object-storage surface the VCNT compactor needs. Its method set is
// intentionally a strict subset of valueindexcompactor.IndexStore's (no Peek, no
// SourceExister — VCNT files carry no magic header to sniff, and Compact's own
// net-sum-<=-0 rule is the only retention signal). This does not make it automatically
// satisfied by an existing IndexStore implementation: List here returns []Object, a distinct
// named type from valueindexcompactor.IndexObject, so a type satisfying IndexStore does not
// structurally satisfy Store even though the two struct shapes are identical — Go requires
// exact method-signature matches, not just field-compatible types. A production store (e.g.
// tempo's) needs its own Store-shaped method set, or a thin adapter that converts
// []IndexObject to []Object, to satisfy this interface.
type Store interface {
	// List returns the full keys and sizes of all objects whose name begins
	// with prefix.
	List(ctx context.Context, prefix string) ([]Object, error)
	// ListDirs returns the immediate child directory prefixes (ending in "/")
	// one level below prefix, without recursing into them.
	ListDirs(ctx context.Context, prefix string) ([]string, error)
	// Get reads the entire object at key.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put writes data to key, creating or overwriting it.
	Put(ctx context.Context, key string, data []byte) error
	// Delete removes the object at key.
	Delete(ctx context.Context, key string) error
}

// CatalogStore is the blockpack_file_catalog capability mergeLevel needs
// (issue #522 Phase 2.2, mirrors valueindexcompactor.CatalogStore's identical
// shape and nil-tolerance convention): record a merge output row, and mark
// the 2+ input rows compacted. *pgcatalog.Store satisfies this structurally.
//
// SPEC-VC (issue #522 Phase 2.1's own mandatory read-path filter is a
// SEPARATE mechanism from this write-path one -- this interface only covers
// mergeLevel's write side): when Config.CatalogStore is nil, mergeLevel falls
// back to its pre-#522 delete-input behavior unchanged, exactly like
// valueindexcompactor's own NOTE-VI-122 fallback -- a deliberate, documented
// choice so this package's existing, catalog-unrelated tests never need a
// fake catalog store they don't care about.
type CatalogStore interface {
	Insert(ctx context.Context, row pgcatalog.Row) error
	MarkCompacted(ctx context.Context, objectKeys []string) error
}
