package viusage

// NOTE: SPEC-VIUSAGE-002 — Registry maintains the tenant-level index of tracked
// (column, type) usage/backfill entries stored at <tenant>/viusage/index.json in object
// storage. Concurrent writes use conditional PUT via the configured ObjectStore
// implementation — real ETag/If-Match on S3, native generation-match on GCS,
// content-hash+mutex emulation on Local/Azure — with exponential-backoff retry (up to 5
// attempts), copying cube's registry.go retry PATTERN verbatim (R1) but implemented as an
// independent, package-local type — no import of internal/modules/cube.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// indexVersion is the current version of the index.json wire format.
const indexVersion = 1

// ObjectStore is the minimal S3-compatible interface the Registry needs. Get returns the
// raw bytes and the current ETag. A missing object MUST be signaled by returning
// ErrNotFound (wrapped or bare) — NEVER by returning a nil error alongside empty
// (data, etag): that shape is indistinguishable from a genuine transient failure (a real
// implementation's SDK error can itself carry empty data/etag), and Registry.Load treats
// ONLY errors.Is(err, ErrNotFound) as "empty index" (go-presubmit.md CRITICAL finding:
// inferring not-found from value shape previously let a real Get error be silently
// swallowed as an empty index, causing updateEntryWithRetry to persist an unconditional
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
// This is the ONLY signal Registry.Load treats as "empty index" — any other non-nil error
// (even one with an empty-shaped data/etag return, exactly like a genuine 404) is a real
// failure and propagates as one.
var ErrNotFound = errors.New("viusage: object not found")

// usageIndex is the JSON structure stored in index.json.
type usageIndex struct {
	Entries []Entry `json:"entries"`
	Version int     `json:"version"`
}

// Registry loads and persists the per-tenant usage index from object storage. It is safe
// for concurrent read access but all mutations are serialized through the conditional-PUT
// retry loop.
type Registry struct {
	store  ObjectStore
	tenant string
}

// NewRegistry creates a Registry for the given tenant backed by store.
func NewRegistry(store ObjectStore, tenant string) *Registry {
	return &Registry{store: store, tenant: tenant}
}

// indexPath returns the object storage key for this tenant's index.
func (r *Registry) indexPath() string {
	return r.tenant + "/viusage/index.json"
}

// Load fetches and decodes the current index. Returns an empty index when the file does
// not exist yet (ObjectStore.Get returned ErrNotFound). Any OTHER error propagates as a
// real failure, regardless of the accompanying (data, etag) shape — see ObjectStore's own
// doc comment for why shape-based inference is unsafe.
func (r *Registry) Load(ctx context.Context) ([]Entry, string, error) {
	data, etag, err := r.store.Get(ctx, r.indexPath())
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

// updateEntryWithRetry loads the index, locates the entry keyed by (colHash, colType) —
// creating it via createIfMissing if absent and createIfMissing is non-nil, else
// returning an error — invokes mutate to apply changes, and persists the whole index via
// the conditional-PUT retry loop (5 attempts, 50ms doubling backoff), mirroring cube's
// Add/Remove/UpdateWatermarks pattern (R1: pattern copied, not code shared). mutate may be
// invoked once per retry attempt since a conflicting concurrent write requires
// re-evaluating against freshly-loaded state — mutate must derive new state from entry's
// CURRENT contents each call, not from closure-captured pre-computed values. Shared by
// recordUse/RenewLease here (A2) and A3's RecordUseAndMaybeTrigger, per plan.md 4.2's
// guidance to factor one private retry helper rather than duplicate the loop.
func (r *Registry) updateEntryWithRetry(
	ctx context.Context,
	tenant, colHash, colType string,
	createIfMissing func() Entry,
	mutate func(entry *Entry) error,
) (Entry, error) {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		entries, etag, err := r.Load(ctx)
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
		if err := r.store.ConditionalPut(ctx, r.indexPath(), data, etag); err != nil {
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

// recordUse appends a use timestamp (unix seconds, from now) for (tenant, colName,
// colType), creating the entry on first use, and truncates UseTimestamps to the most
// recent MaxTrackedUses entries (R4/4.1). Internal helper shared by RecordUse (public
// surface, once needed) and A3's RecordUseAndMaybeTrigger, which layers threshold
// evaluation and lease acquisition on top via the same updateEntryWithRetry mutate shape.
func (r *Registry) recordUse(ctx context.Context, tenant, colName, colType string, now time.Time) (Entry, error) {
	colHash := valueindex.ColHash(colName)
	nowSec := uint64(now.Unix()) //nolint:gosec // unix seconds fits uint64 for any realistic timestamp

	return r.updateEntryWithRetry(
		ctx, tenant, colHash, colType,
		func() Entry {
			return Entry{
				Tenant:       tenant,
				ColumnHash:   colHash,
				ColumnName:   colName,
				ColumnType:   colType,
				FirstSeenSec: nowSec,
				CreatedAt:    nowSec,
			}
		},
		func(e *Entry) error {
			e.UseTimestamps = append(e.UseTimestamps, nowSec)
			if len(e.UseTimestamps) > MaxTrackedUses {
				e.UseTimestamps = e.UseTimestamps[len(e.UseTimestamps)-MaxTrackedUses:]
			}
			return nil
		},
	)
}

// RenewLease pushes an existing entry's BackfillState.LeaseExpiresAt forward via the same
// conditional-PUT retry discipline (4.4 step 2) — required because a real 48h backfill run
// may legitimately outlast one LeaseTTLSeconds period. Errors if the entry does not exist
// (mirrors cube's UpdateWatermarks "cube not found" behavior): renewing a lease implies
// the entry was already created by a prior RecordUseAndMaybeTrigger trigger/acquire call.
func (r *Registry) RenewLease(ctx context.Context, tenant, colHash, colType string, newExpiresAt uint64) error {
	_, err := r.updateEntryWithRetry(
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
// colType)'s entry via the same conditional-PUT retry discipline (plan.md
// Section 4.4/R7/R9): WatermarkSec and the window bounds are written on every
// call (the caller passes the same window on every call within one run, so
// re-writing WindowStartSec/WindowEndSec is a no-op once set — this is also
// the ONLY place these fields are ever set, since neither the trigger step
// (RecordUseAndMaybeTrigger) nor the registry's own recordUse sets them).
//
// done=true additionally sets Done=true AND releases the lease
// (BackfillInProgress=false) in the SAME conditional-PUT — per plan.md
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
	_, err := r.updateEntryWithRetry(
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
