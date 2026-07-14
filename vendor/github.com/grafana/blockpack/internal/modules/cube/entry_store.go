package cube

// entry_store.go — Registry's internal storage abstraction (introduced 2026-07-11,
// mirrors viusage's identical entry_store.go/registry.go refactor) — lets Registry sit
// on top of either a whole-blob ObjectStore (existing S3/Local/GCS/Azure path) or a
// row-oriented Postgres backend without changing Registry's own public methods at all.
// blobEntryStore (below) wraps today's ObjectStore + conditional-PUT-retry loops,
// behavior-preserving: every method body here is registry.go's PRE-refactor Add/Remove/
// UpdateWatermarks/Load moved verbatim (rename Registry receiver r -> blobEntryStore
// receiver s, r.store -> s.store).
//
// Unlike viusage's entryStore (a single generic upsertEntry(createIfMissing, mutate)
// primitive — appropriate there because every viusage write goes through the identical
// create-or-mutate-one-entry shape), cube's three existing Registry write operations
// (Add/Remove/UpdateWatermarks) have genuinely different list-mutation semantics
// (append-with-limit-check, filter-out, map-mutate-existing) — R1 applies: mirror that
// shape with one entryStore method per operation rather than forcing a generic
// create-or-mutate primitive cube's own usage pattern doesn't need.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// entryStore is Registry's internal storage abstraction.
type entryStore interface {
	// load returns every cube entry for tenant. The blob-backed implementation returns
	// them in on-disk order; a Postgres-backed implementation may return them in any
	// order (no caller today depends on Load's ordering — confirmed by reading every
	// external Load call site, all of which discard the etag and either linear-scan or
	// aggregate the result).
	load(ctx context.Context, tenant string) ([]RegistryEntry, error)
	// addEntry registers entry for tenant, idempotent on entry.CubeID already existing —
	// mirrors Registry.Add's exact current contract. There is no per-tenant cube-count
	// limit (removed, issue #497): active cube count per tenant is unbounded.
	addEntry(ctx context.Context, tenant string, entry RegistryEntry) error
	// removeEntry deletes the entry with cubeID for tenant, a no-op if already absent —
	// mirrors Registry.Remove's exact current contract.
	removeEntry(ctx context.Context, tenant, cubeID string) error
	// updateWatermarksEntry expands cubeID's stored watermark at level to cover
	// [minMinute, maxMinute] (min-of-mins, max-of-maxes) — mirrors
	// Registry.UpdateWatermarks' exact current contract. Errors if cubeID is not found.
	updateWatermarksEntry(ctx context.Context, tenant, cubeID string, level, minMinute, maxMinute uint32) error
}

// blobEntryStore wraps an ObjectStore + the tenant-level cubeIndex JSON blob +
// conditional-PUT-retry loops — literally today's Registry method bodies, moved
// verbatim, behavior-preserving.
type blobEntryStore struct {
	store ObjectStore
}

func (s *blobEntryStore) indexPath(tenant string) string {
	return tenant + "/cubes/index.json"
}

// loadWithEtag is blobEntryStore's own internal primitive: addEntry/removeEntry/
// updateWatermarksEntry's retry loops need the etag for ConditionalPut, but the
// exported entryStore.load method below drops it — mirrors viusage's identical
// etag-is-meaningless-outside-this-file shape.
func (s *blobEntryStore) loadWithEtag(ctx context.Context, tenant string) ([]RegistryEntry, string, error) {
	data, etag, err := s.store.Get(ctx, s.indexPath(tenant))
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

func (s *blobEntryStore) load(ctx context.Context, tenant string) ([]RegistryEntry, error) {
	entries, _, err := s.loadWithEtag(ctx, tenant)
	return entries, err
}

// addEntry is Registry.Add's former body, moved verbatim (NOTE-CUBE-009: up to 5
// retries with 50ms base, doubling each time). No per-tenant cube-count limit is enforced
// (removed, issue #497) — active cube count per tenant is unbounded.
func (s *blobEntryStore) addEntry(ctx context.Context, tenant string, entry RegistryEntry) error {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		cubes, etag, err := s.loadWithEtag(ctx, tenant)
		if err != nil {
			return err
		}
		// Idempotent: already present.
		for _, c := range cubes {
			if c.CubeID == entry.CubeID {
				return nil
			}
		}
		cubes = append(cubes, entry)
		data, err := json.Marshal(cubeIndex{Version: indexVersion, Cubes: cubes})
		if err != nil {
			return fmt.Errorf("cube registry: encode: %w", err)
		}
		if err := s.store.ConditionalPut(ctx, s.indexPath(tenant), data, etag); err != nil {
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
	return fmt.Errorf("cube registry: add %q: exceeded %d retries on conflict", entry.CubeID, maxRetries)
}

// removeEntry is Registry.Remove's former body, moved verbatim.
func (s *blobEntryStore) removeEntry(ctx context.Context, tenant, cubeID string) error {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		cubes, etag, err := s.loadWithEtag(ctx, tenant)
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
		if err := s.store.ConditionalPut(ctx, s.indexPath(tenant), data, etag); err != nil {
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

// updateWatermarksEntry is Registry.UpdateWatermarks' former body, moved verbatim
// (E-12a/E-12b: expands the existing watermark's range, min-of-mins/max-of-maxes).
func (s *blobEntryStore) updateWatermarksEntry(
	ctx context.Context, tenant, cubeID string, level, minMinute, maxMinute uint32,
) error {
	const maxRetries = 5
	backoff := 50 * time.Millisecond

	for attempt := range maxRetries {
		cubes, etag, err := s.loadWithEtag(ctx, tenant)
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
		if err := s.store.ConditionalPut(ctx, s.indexPath(tenant), data, etag); err != nil {
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

// EntryStore is the EXPORTED counterpart of entryStore, with exported method names —
// what an external (tempo) Postgres implementation actually satisfies. Mirrors
// viusage.EntryStore's identical two-interface+adapter shape (Go forbids exporting an
// interface whose method names are unexported, even via a type alias — the alias
// doesn't change the method names) — see entryStore's own doc comment above for why
// cube's shape has 4 methods (one per existing Add/Remove/UpdateWatermarks write
// operation) rather than viusage's single generic UpsertEntry.
type EntryStore interface {
	Load(ctx context.Context, tenant string) ([]RegistryEntry, error)
	AddEntry(ctx context.Context, tenant string, entry RegistryEntry) error
	RemoveEntry(ctx context.Context, tenant, cubeID string) error
	UpdateWatermarksEntry(ctx context.Context, tenant, cubeID string, level, minMinute, maxMinute uint32) error
}

// externalEntryStoreAdapter adapts an external EntryStore to the internal, unexported
// entryStore Registry actually holds — mirrors how blobEntryStore adapts ObjectStore.
type externalEntryStoreAdapter struct{ EntryStore }

func (a *externalEntryStoreAdapter) load(ctx context.Context, tenant string) ([]RegistryEntry, error) {
	return a.Load(ctx, tenant)
}

func (a *externalEntryStoreAdapter) addEntry(
	ctx context.Context,
	tenant string,
	entry RegistryEntry,
) error {
	return a.AddEntry(ctx, tenant, entry)
}

func (a *externalEntryStoreAdapter) removeEntry(ctx context.Context, tenant, cubeID string) error {
	return a.RemoveEntry(ctx, tenant, cubeID)
}

func (a *externalEntryStoreAdapter) updateWatermarksEntry(
	ctx context.Context, tenant, cubeID string, level, minMinute, maxMinute uint32,
) error {
	return a.UpdateWatermarksEntry(ctx, tenant, cubeID, level, minMinute, maxMinute)
}

// NewRegistryFromEntryStore constructs a Registry over an externally-supplied
// EntryStore (e.g. tempo's Postgres-backed implementation) instead of an ObjectStore.
// Registry's own public methods (Load/Add/Remove/UpdateWatermarks) are byte-identical
// regardless of which constructor built it.
func NewRegistryFromEntryStore(store EntryStore, tenant string) *Registry {
	return &Registry{store: &externalEntryStoreAdapter{store}, tenant: tenant}
}
