package viusage

import "context"

// entryStore is Registry's internal storage abstraction (introduced 2026-07-11 to let
// Registry sit on top of either a whole-blob ObjectStore (existing S3/Local/GCS/Azure
// path) or a row-oriented Postgres backend without changing Registry's own public
// methods at all). blobEntryStore (registry.go) wraps today's ObjectStore + conditional-
// PUT-retry loop, behavior-preserving. A second implementation is constructed from an
// externally-supplied EntryStore (see valueindex_usage.go's re-export) for the Postgres
// case.
type entryStore interface {
	// load returns every entry for tenant. The blob-backed implementation returns them
	// in on-disk order; a Postgres-backed implementation may return them in any order
	// (no caller today depends on Load's ordering — confirmed by reading every external
	// Load call site, all of which discard the etag and either linear-scan or aggregate
	// the result).
	load(ctx context.Context, tenant string) ([]Entry, error)
	// upsertEntry loads-or-creates the row keyed by (tenant, colHash, colType), applies
	// mutate to it, and persists the result — ONE atomic evaluate-and-mutate step,
	// mirroring updateEntryWithRetry's existing create+mutate+persist contract. mutate
	// may be invoked more than once (blob-backed: once per retry attempt; Postgres-
	// backed: exactly once under a row lock) — callers must derive new state from the
	// entry's CURRENT contents each call, never from closure-captured pre-computed
	// values (unchanged from today's contract).
	upsertEntry(
		ctx context.Context, tenant, colHash, colType string,
		createIfMissing func() Entry, mutate func(*Entry) error,
	) (Entry, error)
}

// EntryStore is the EXPORTED counterpart of entryStore, with exported method names —
// what an external (tempo) Postgres implementation actually satisfies. entryStore's
// methods are unexported, and Go enforces unexported method names as package-private —
// an external type cannot structurally implement entryStore even via a type alias,
// since the alias doesn't change the method names. EntryStore plus
// externalEntryStoreAdapter below bridge that gap.
type EntryStore interface {
	Load(ctx context.Context, tenant string) ([]Entry, error)
	UpsertEntry(
		ctx context.Context, tenant, colHash, colType string,
		createIfMissing func() Entry, mutate func(*Entry) error,
	) (Entry, error)
}

// externalEntryStoreAdapter adapts an external EntryStore to the internal, unexported
// entryStore Registry actually holds — mirrors how blobEntryStore adapts ObjectStore.
type externalEntryStoreAdapter struct{ EntryStore }

func (a *externalEntryStoreAdapter) load(ctx context.Context, tenant string) ([]Entry, error) {
	return a.Load(ctx, tenant)
}

func (a *externalEntryStoreAdapter) upsertEntry(
	ctx context.Context, tenant, colHash, colType string,
	createIfMissing func() Entry, mutate func(*Entry) error,
) (Entry, error) {
	return a.UpsertEntry(ctx, tenant, colHash, colType, createIfMissing, mutate)
}

// NewRegistryFromEntryStore constructs a Registry over an externally-supplied
// EntryStore (e.g. tempo's Postgres-backed implementation) instead of an ObjectStore.
// Registry's own public methods (Load/RenewLease/UpdateWatermark/UpdateCatalogCursor)
// are byte-identical regardless of which constructor built it.
func NewRegistryFromEntryStore(store EntryStore, tenant string) *Registry {
	return &Registry{store: &externalEntryStoreAdapter{store}, tenant: tenant}
}
