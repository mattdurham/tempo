package cube

// NOTE: SPEC-CUBE-014 — Registry maintains the tenant-level index of active cubes stored
// at <tenant>/cubes/index.json in object storage. Concurrent writes use conditional PUT via
// the configured ObjectStore implementation — real ETag/If-Match on S3, native
// generation-match on GCS, content-hash+mutex emulation on Local/Azure — with
// exponential-backoff retry (up to 5 attempts). The same deterministic CubeID ensures
// concurrent creators converge on one entry rather than duplicating.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

// indexVersion is the current version of the index.json wire format.
const indexVersion = 1

// ObjectStore is the minimal S3-compatible interface the Registry needs.
// Get returns the raw bytes and the current ETag. A missing object MUST be signaled by
// returning ErrNotFound (wrapped or bare) — NEVER by returning a nil error alongside
// empty (data, etag): that shape is indistinguishable from a genuine transient failure (a
// real implementation's SDK error can itself carry empty data/etag), and Registry.Load
// treats ONLY errors.Is(err, ErrNotFound) as "empty index" (go-presubmit.md CRITICAL
// finding, originally found in viusage's independently-copied version of this same
// pattern — see internal/modules/viusage/NOTES.md NOTE-VIUSAGE-10: inferring not-found
// from value shape previously let a real Get error be silently swallowed as an empty
// index, causing the conditional-PUT retry loop to persist an unconditional PUT that
// destroyed every other tracked cube's state for the tenant). ConditionalPut writes only
// when the stored ETag matches the supplied etag ("" for create-if-not-exists). A
// 412-equivalent conflict is signaled by returning ErrConflict.
type ObjectStore interface {
	Get(ctx context.Context, path string) (data []byte, etag string, err error)
	ConditionalPut(ctx context.Context, path string, data []byte, etag string) error
}

// ErrConflict is returned by ObjectStore.ConditionalPut when the ETag does not match.
var ErrConflict = errors.New("cube: conditional PUT conflict (412)")

// ErrNotFound is returned by ObjectStore.Get when the requested object does not exist.
// This is the ONLY signal Registry.Load treats as "empty index" — any other non-nil error
// (even one with an empty-shaped data/etag return, exactly like a genuine 404) is a real
// failure and propagates as one.
var ErrNotFound = errors.New("cube: object not found")

// cubeIndex is the JSON structure stored in index.json.
type cubeIndex struct {
	Cubes   []RegistryEntry `json:"cubes"`
	Version int             `json:"version"`
}

// Registry loads and persists the per-tenant cube index from object storage.
// It is safe for concurrent read access but all mutations are serialized through
// the conditional-PUT retry loop.
type Registry struct {
	store  entryStore // was: store ObjectStore (2026-07-11 entryStore refactor)
	tenant string
}

// NewRegistry creates a Registry for the given tenant backed by store. Public signature
// unchanged by the 2026-07-11 entryStore refactor — internally wraps store in a
// blobEntryStore (today's S3/Local/GCS/Azure conditional-PUT path, behavior-preserving).
func NewRegistry(store ObjectStore, tenant string) *Registry {
	return &Registry{store: &blobEntryStore{store: store}, tenant: tenant}
}

// NewPgRegistry is a convenience constructor for the common case of wanting a
// Postgres-backed Registry without caring about the EntryStore seam directly
// -- equivalent to NewRegistryFromEntryStore(NewPgEntryStore(pool), tenant).
func NewPgRegistry(pool *pgxpool.Pool, tenant string) *Registry {
	return NewRegistryFromEntryStore(NewPgEntryStore(pool), tenant)
}

// Load fetches and decodes the current index. Returns an empty index when the file does
// not exist yet. Any OTHER error propagates as a real failure. The etag return is always
// "" post-refactor (confirmed via direct grep: every external caller in tempo discards
// it with _; only this package's OWN Add/Remove/UpdateWatermarks retry loops ever used it
// meaningfully, and those now go through blobEntryStore's own internal loadWithEtag,
// never through this public Load) — a Postgres-backed entryStore has no etag concept at
// all, so this keeps Load's public contract meaningful for either backend.
func (r *Registry) Load(ctx context.Context) ([]RegistryEntry, string, error) {
	entries, err := r.store.load(ctx, r.tenant)
	return entries, "", err
}

// IsActive reports whether a cube with the given ID is in the current index.
func (r *Registry) IsActive(ctx context.Context, cubeID string) (bool, error) {
	cubes, _, err := r.Load(ctx)
	if err != nil {
		return false, err
	}
	for _, c := range cubes {
		if c.CubeID == cubeID {
			return true, nil
		}
	}
	return false, nil
}

// Add appends def to the index using a conditional-PUT retry loop.
// If def.CubeID is already present the call is a no-op and returns nil.
// NOTE-CUBE-009: up to 5 retries with 50 ms base, doubling each time (now inside
// blobEntryStore.addEntry — see entry_store.go's 2026-07-11 entryStore refactor).
func (r *Registry) Add(ctx context.Context, def RegistryEntry) error {
	return r.store.addEntry(ctx, r.tenant, def)
}

// Remove deletes the cube with the given ID from the index using conditional-PUT retry
// (now inside blobEntryStore.removeEntry).
func (r *Registry) Remove(ctx context.Context, cubeID string) error {
	return r.store.removeEntry(ctx, r.tenant, cubeID)
}

// UpdateWatermarks records that cubeID's data is completely covered at the given resolution
// level across [minMinute, maxMinute], using the same conditional-PUT retry discipline as
// Add/Remove (E-12a, now inside blobEntryStore.updateWatermarksEntry). Expands the existing
// watermark's range (min of mins, max of maxes) rather than replacing it outright — callers
// are expected to supply monotonically-extending, adjacent windows (the compaction ladder
// processes fully-elapsed boundaries in order, E-12b), so a simple min/max expansion is
// sufficient; gap detection within a level is the router's (E-6b) concern at query time, not
// this write-time bookkeeping.
func (r *Registry) UpdateWatermarks(ctx context.Context, cubeID string, level, minMinute, maxMinute uint32) error {
	return r.store.updateWatermarksEntry(ctx, r.tenant, cubeID, level, minMinute, maxMinute)
}
