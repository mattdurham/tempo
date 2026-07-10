package cube

// NOTE: SPEC-CUBE-014 — Registry maintains the tenant-level index of active cubes stored
// at <tenant>/cubes/index.json in object storage. Concurrent writes use S3 conditional PUT
// (If-Match ETag) with exponential-backoff retry (up to 5 attempts). The same deterministic
// CubeID ensures concurrent creators converge on one entry rather than duplicating.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MaxCubesPerTenant is the default maximum number of active cubes per tenant.
const MaxCubesPerTenant = 1000

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
	store  ObjectStore
	tenant string
	// maxCubes is the per-tenant active-cube limit.
	maxCubes int
}

// NewRegistry creates a Registry for the given tenant backed by store.
func NewRegistry(store ObjectStore, tenant string) *Registry {
	return &Registry{store: store, tenant: tenant, maxCubes: MaxCubesPerTenant}
}

// indexPath returns the S3 key for this tenant's index.
func (r *Registry) indexPath() string {
	return r.tenant + "/cubes/index.json"
}

// Load fetches and decodes the current index. Returns an empty index when the file does
// not exist yet (ObjectStore.Get returned ErrNotFound). Any OTHER error propagates as a
// real failure, regardless of the accompanying (data, etag) shape — see ObjectStore's own
// doc comment for why shape-based inference is unsafe.
func (r *Registry) Load(ctx context.Context) ([]RegistryEntry, string, error) {
	data, etag, err := r.store.Get(ctx, r.indexPath())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("cube registry: load: %w", err)
	}
	if len(data) == 0 {
		return nil, etag, nil
	}
	var idx cubeIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, etag, fmt.Errorf("cube registry: decode: %w", err)
	}
	return idx.Cubes, etag, nil
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
// Returns ErrLimitReached when the per-tenant cube limit would be exceeded.
// NOTE-CUBE-009: up to 5 retries with 50 ms base, doubling each time.
func (r *Registry) Add(ctx context.Context, def RegistryEntry) error {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		cubes, etag, err := r.Load(ctx)
		if err != nil {
			return err
		}
		// Idempotent: already present.
		for _, c := range cubes {
			if c.CubeID == def.CubeID {
				return nil
			}
		}
		// Per-tenant limit.
		if len(cubes) >= r.maxCubes {
			return &ErrLimitReached{Limit: r.maxCubes}
		}
		cubes = append(cubes, def)
		data, err := json.Marshal(cubeIndex{Version: indexVersion, Cubes: cubes})
		if err != nil {
			return fmt.Errorf("cube registry: encode: %w", err)
		}
		if err := r.store.ConditionalPut(ctx, r.indexPath(), data, etag); err != nil {
			if errors.Is(err, ErrConflict) {
				// Another writer won; back off and retry.
				if attempt < maxRetries-1 {
					time.Sleep(backoff)
					backoff *= 2
				}
				continue
			}
			return fmt.Errorf("cube registry: put: %w", err)
		}
		return nil // success
	}
	return fmt.Errorf("cube registry: add %q: exceeded %d retries on conflict", def.CubeID, maxRetries)
}

// Remove deletes the cube with the given ID from the index using conditional-PUT retry.
func (r *Registry) Remove(ctx context.Context, cubeID string) error {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		cubes, etag, err := r.Load(ctx)
		if err != nil {
			return err
		}
		filtered := cubes[:0]
		found := false
		for _, c := range cubes {
			if c.CubeID == cubeID {
				found = true
				continue
			}
			filtered = append(filtered, c)
		}
		if !found {
			return nil // already absent
		}
		data, err := json.Marshal(cubeIndex{Version: indexVersion, Cubes: filtered})
		if err != nil {
			return fmt.Errorf("cube registry: encode: %w", err)
		}
		if err := r.store.ConditionalPut(ctx, r.indexPath(), data, etag); err != nil {
			if errors.Is(err, ErrConflict) {
				if attempt < maxRetries-1 {
					time.Sleep(backoff)
					backoff *= 2
				}
				continue
			}
			return fmt.Errorf("cube registry: put: %w", err)
		}
		return nil
	}
	return fmt.Errorf("cube registry: remove %q: exceeded %d retries on conflict", cubeID, maxRetries)
}

// UpdateWatermarks records that cubeID's data is completely covered at the given resolution
// level across [minMinute, maxMinute], using the same conditional-PUT retry discipline as
// Add/Remove (E-12a). Expands the existing watermark's range (min of mins, max of maxes) rather
// than replacing it outright — callers are expected to supply monotonically-extending, adjacent
// windows (the compaction ladder processes fully-elapsed boundaries in order, E-12b), so a simple
// min/max expansion is sufficient; gap detection within a level is the router's (E-6b) concern at
// query time, not this write-time bookkeeping.
func (r *Registry) UpdateWatermarks(ctx context.Context, cubeID string, level, minMinute, maxMinute uint32) error {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		cubes, etag, err := r.Load(ctx)
		if err != nil {
			return err
		}
		idx := -1
		for i, c := range cubes {
			if c.CubeID == cubeID {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("cube registry: update watermarks: cube %q not found", cubeID)
		}

		newWm := ResolutionWatermark{MinMinute: minMinute, MaxMinute: maxMinute}
		if existing, ok := cubes[idx].Watermarks[level]; ok {
			if existing.MinMinute < newWm.MinMinute {
				newWm.MinMinute = existing.MinMinute
			}
			if existing.MaxMinute > newWm.MaxMinute {
				newWm.MaxMinute = existing.MaxMinute
			}
		}
		if cubes[idx].Watermarks == nil {
			cubes[idx].Watermarks = make(map[uint32]ResolutionWatermark, 1)
		}
		cubes[idx].Watermarks[level] = newWm

		data, err := json.Marshal(cubeIndex{Version: indexVersion, Cubes: cubes})
		if err != nil {
			return fmt.Errorf("cube registry: encode: %w", err)
		}
		if err := r.store.ConditionalPut(ctx, r.indexPath(), data, etag); err != nil {
			if errors.Is(err, ErrConflict) {
				if attempt < maxRetries-1 {
					time.Sleep(backoff)
					backoff *= 2
				}
				continue
			}
			return fmt.Errorf("cube registry: put: %w", err)
		}
		return nil
	}
	return fmt.Errorf("cube registry: update watermarks %q: exceeded %d retries on conflict", cubeID, maxRetries)
}

// ErrLimitReached is returned when the per-tenant cube limit would be exceeded.
type ErrLimitReached struct {
	Limit int
}

func (e *ErrLimitReached) Error() string {
	return fmt.Sprintf("cube registry: per-tenant cube limit (%d) reached", e.Limit)
}
