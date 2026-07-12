package viusage

// NOTE: SPEC-VIUSAGE-002 — Registry maintains the tenant-level index of tracked
// (column, type) usage/backfill entries stored at <tenant>/viusage/index.json in object
// storage. Concurrent writes use conditional PUT via the configured ObjectStore
// implementation — real ETag/If-Match on S3, native generation-match on GCS,
// content-hash+mutex emulation on Local/Azure — with exponential-backoff retry (up to 5
// attempts), copying cube's registry.go retry PATTERN verbatim (R1) but implemented as an
// independent, package-local type — no import of internal/modules/cube. Registry itself
// delegates all storage through the entryStore interface (entry_store.go, 2026-07-11) so
// it can sit on top of either this blob-backed path or a row-oriented Postgres backend
// without changing any of its own public methods.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// indexVersion is the current version of the index.json wire format.
const indexVersion = 1

// ObjectStore is the minimal S3-compatible interface the Registry needs. Get returns the
// raw bytes and the current ETag. A missing object MUST be signaled by returning
// ErrNotFound (wrapped or bare) — NEVER by returning a nil error alongside empty
// (data, etag): that shape is indistinguishable from a genuine transient failure (a real
// implementation's SDK error can itself carry empty data/etag), and blobEntryStore.load
// treats ONLY errors.Is(err, ErrNotFound) as "empty index" (go-presubmit.md CRITICAL
// finding: inferring not-found from value shape previously let a real Get error be
// silently swallowed as an empty index, causing upsertEntry to persist an unconditional
// PUT that destroyed every other tracked column's state for the tenant). ConditionalPut
// writes only when the stored ETag matches the supplied etag ("" for
// create-if-not-exists). A 412-equivalent conflict is signaled by returning ErrConflict.
// Mirrors cube.ObjectStore's contract exactly (R1: pattern copied, not imported) — EXCEPT
// for this ErrNotFound requirement, which cube's own Get contract does not have (see
// NOTES.md's cross-reference to cube's own latent not-found/real-error shape ambiguity,
// a separate, out-of-scope finding for cube's own registry).
type ObjectStore interface {
	Get(ctx context.Context, path string) (data []byte, etag string, err error)
	ConditionalPut(ctx context.Context, path string, data []byte, etag string) error
}

// ErrConflict is returned by ObjectStore.ConditionalPut when the ETag does not match.
var ErrConflict = errors.New("viusage: conditional PUT conflict (412)")

// ErrNotFound is returned by ObjectStore.Get when the requested object does not exist.
// This is the ONLY signal blobEntryStore.load treats as "empty index" — any other
// non-nil error (even one with an empty-shaped data/etag return, exactly like a genuine
// 404) is a real failure and propagates as one.
var ErrNotFound = errors.New("viusage: object not found")

// usageIndex is the JSON structure stored in index.json.
type usageIndex struct {
	Entries []Entry `json:"entries"`
	Version int     `json:"version"`
}

// Registry loads and persists the per-tenant usage index via its entryStore. It is safe
// for concurrent read access but all mutations are serialized through store's own
// conditional-write discipline (blobEntryStore: conditional-PUT retry loop; an external
// Postgres-backed EntryStore: its own row-lock transaction).
type Registry struct {
	store  entryStore
	tenant string
}

// NewRegistry creates a Registry for the given tenant backed by store, wrapping it in
// the blob-backed entryStore implementation.
func NewRegistry(store ObjectStore, tenant string) *Registry {
	return &Registry{store: &blobEntryStore{store: store}, tenant: tenant}
}

// Load fetches and decodes the current index. Registry.Load's public signature stays
// ([]Entry, string, error) for backwards compatibility, but the etag return value is
// always "" now — entryStore.load has no etag concept (meaningless for a Postgres
// row-per-entry backend), and every external caller in tempo already discards this
// return value with `_` (confirmed via direct grep; only blobEntryStore's OWN internal
// retry loop ever used it meaningfully, and that loop is now entirely internal to
// blobEntryStore.upsertEntry below).
func (r *Registry) Load(ctx context.Context) ([]Entry, string, error) {
	entries, err := r.store.load(ctx, r.tenant)
	return entries, "", err
}

// blobEntryStore adapts an ObjectStore into the entryStore interface Registry holds —
// today's whole-blob JSON index + conditional-PUT-retry loop, behavior-preserving from
// before entryStore existed (2026-07-11 refactor, Part 1.3).
type blobEntryStore struct {
	store ObjectStore
}

// indexPath returns the object storage key for tenant's index.
func (s *blobEntryStore) indexPath(tenant string) string {
	return tenant + "/viusage/index.json"
}

// load fetches and decodes the current index. Returns an empty index when the file does
// not exist yet (ObjectStore.Get returned ErrNotFound). Any OTHER error propagates as a
// real failure, regardless of the accompanying (data, etag) shape — see ObjectStore's own
// doc comment for why shape-based inference is unsafe.
func (s *blobEntryStore) load(ctx context.Context, tenant string) ([]Entry, error) {
	entries, _, err := s.loadWithETag(ctx, tenant)
	return entries, err
}

// loadWithETag is load's ETag-preserving variant, used internally by upsertEntry's
// conditional-PUT retry loop (the etag is meaningless to any entryStore caller outside
// this file, per load's own doc comment).
func (s *blobEntryStore) loadWithETag(ctx context.Context, tenant string) ([]Entry, string, error) {
	data, etag, err := s.store.Get(ctx, s.indexPath(tenant))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("viusage registry: load: %w", err)
	}
	if len(data) == 0 {
		return nil, etag, nil
	}
	var idx usageIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, etag, fmt.Errorf("viusage registry: decode: %w", err)
	}
	return idx.Entries, etag, nil
}

// upsertEntry loads the index, locates the entry keyed by (colHash, colType) — creating
// it via createIfMissing if absent and createIfMissing is non-nil, else returning an
// error — invokes mutate to apply changes, and persists the whole index via the
// conditional-PUT retry loop (5 attempts, 50ms doubling backoff), mirroring cube's
// Add/Remove/UpdateWatermarks pattern (R1: pattern copied, not code shared). mutate may
// be invoked once per retry attempt since a conflicting concurrent write requires
// re-evaluating against freshly-loaded state — mutate must derive new state from entry's
// CURRENT contents each call, not from closure-captured pre-computed values.
func (s *blobEntryStore) upsertEntry(
	ctx context.Context,
	tenant, colHash, colType string,
	createIfMissing func() Entry,
	mutate func(entry *Entry) error,
) (Entry, error) {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		entries, etag, err := s.loadWithETag(ctx, tenant)
		if err != nil {
			return Entry{}, err
		}
		idx := -1
		for i := range entries {
			if entries[i].Tenant == tenant && entries[i].ColumnHash == colHash && entries[i].ColumnType == colType {
				idx = i
				break
			}
		}
		if idx < 0 {
			if createIfMissing == nil {
				return Entry{}, fmt.Errorf("viusage registry: entry %s/%s/%s not found", tenant, colHash, colType)
			}
			entries = append(entries, createIfMissing())
			idx = len(entries) - 1
		}
		if mutateErr := mutate(&entries[idx]); mutateErr != nil {
			return Entry{}, mutateErr
		}
		result := entries[idx]

		data, err := json.Marshal(usageIndex{Version: indexVersion, Entries: entries})
		if err != nil {
			return Entry{}, fmt.Errorf("viusage registry: encode: %w", err)
		}
		if err := s.store.ConditionalPut(ctx, s.indexPath(tenant), data, etag); err != nil {
			if errors.Is(err, ErrConflict) {
				if attempt < maxRetries-1 {
					time.Sleep(backoff)
					backoff *= 2
				}
				continue
			}
			return Entry{}, fmt.Errorf("viusage registry: put: %w", err)
		}
		return result, nil
	}
	return Entry{}, fmt.Errorf(
		"viusage registry: update %s/%s/%s: exceeded %d retries on conflict",
		tenant, colHash, colType, maxRetries,
	)
}

// RenewLease pushes an existing entry's BackfillState.LeaseExpiresAt forward via the same
// conditional-write discipline (4.4 step 2) — required because a real 48h backfill run
// may legitimately outlast one LeaseTTLSeconds period. Errors if the entry does not exist
// (mirrors cube's UpdateWatermarks "cube not found" behavior): renewing a lease implies
// the entry was already created by a prior RecordUseAndMaybeTrigger trigger/acquire call.
func (r *Registry) RenewLease(ctx context.Context, tenant, colHash, colType string, newExpiresAt uint64) error {
	_, err := r.store.upsertEntry(
		ctx, tenant, colHash, colType,
		nil,
		func(e *Entry) error {
			e.Backfill.LeaseExpiresAt = newExpiresAt
			return nil
		},
	)
	return err
}

// UpdateWatermark persists one backfill progress update for (tenant, colHash,
// colType)'s entry via the same conditional-write discipline (plan.md
// Section 4.4/R7/R9): WatermarkSec and the window bounds are written on every
// call (the caller passes the same window on every call within one run, so
// re-writing WindowStartSec/WindowEndSec is a no-op once set — this is also
// the ONLY place these fields are ever set, since the trigger step
// (RecordUseAndMaybeTrigger) never sets them).
//
// done=true additionally sets Done=true AND releases the lease
// (BackfillInProgress=false) in the SAME write — per plan.md
// Section 4.4 step 3, never a separate release step that could leave
// Done=true with the lease still held.
//
// This is the persistence half of R9's finding: cube's own backfill→registry
// wiring never calls an equivalent method from its progressFn (verified
// directly against cube_backfill.go), leaving a real coverage-staleness gap
// for cube. VI's design requires the CALLER (tempo's B2 launcher) to call
// this from every progressFn invocation, not just on Done, closing that gap
// for VI specifically.
//
// Errors if the entry does not exist (mirrors RenewLease's own contract): a
// backfill run's progressFn is only ever invoked after
// RecordUseAndMaybeTrigger's ShouldBackfill=true already created the entry.
func (r *Registry) UpdateWatermark(
	ctx context.Context,
	tenant, colHash, colType string,
	watermarkSec, windowStartSec, windowEndSec uint64,
	done bool,
) error {
	_, err := r.store.upsertEntry(
		ctx, tenant, colHash, colType,
		nil,
		func(e *Entry) error {
			e.Backfill.WatermarkSec = watermarkSec
			e.Backfill.WindowStartSec = windowStartSec
			e.Backfill.WindowEndSec = windowEndSec
			if done {
				e.Backfill.Done = true
				e.Backfill.BackfillInProgress = false
			}
			return nil
		},
	)
	return err
}

// UpdateCatalogCursor advances (tenant, colHash, colType)'s persisted file-catalog
// cursor to rowID via the same conditional-write discipline as RenewLease/
// UpdateWatermark. Monotonic: a rowID lower than or equal to the entry's current
// LastCatalogRowID is a silent no-op (never regresses the cursor — a stale/replayed call
// must not make a later run re-list already-processed catalog rows). Errors if the
// entry does not exist (mirrors RenewLease's own contract).
// SPEC-VIUSAGE-9: monotonic file-catalog cursor -- see entry.go's LastCatalogRowID.
func (r *Registry) UpdateCatalogCursor(ctx context.Context, tenant, colHash, colType string, rowID uint64) error {
	_, err := r.store.upsertEntry(
		ctx, tenant, colHash, colType,
		nil,
		func(e *Entry) error {
			if rowID > e.Backfill.LastCatalogRowID {
				e.Backfill.LastCatalogRowID = rowID
			}
			return nil
		},
	)
	return err
}
