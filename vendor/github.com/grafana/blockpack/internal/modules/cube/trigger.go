package cube

// NOTE: SPEC-CUBE-015 — CreationTrigger is the first-query cube-creation path.
// On the first metrics query for a (tenant, dims, filters) pattern that passes the
// cardinality gate, it registers the cube in the Registry and immediately hands back
// a pre-built RegistryEntry so the caller can begin forward ingest and async backfill.
// The query response is never blocked; async steps happen in a caller-supplied goroutine.

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/blockpack/internal/modules/valuecounts"
)

// TriggerConfig parameterises the creation trigger.
type TriggerConfig struct {
	// CardinalityLimits overrides the default cardinality limits. Zero value → defaults.
	CardinalityLimits CardinalityLimits
	// MaxCubesPerTenant overrides the per-tenant active-cube limit (default 1000).
	MaxCubesPerTenant int
}

// TriggerResult is returned by CreationTrigger.TryCreate on success.
type TriggerResult struct {
	// Entry is the newly-registered (or pre-existing) cube entry.
	Entry RegistryEntry
	// Created is true when this call registered the cube (false = already existed).
	Created bool
}

// CreationTrigger checks the cardinality gate and registers a cube on the first query.
type CreationTrigger struct {
	registry *Registry
	cfg      TriggerConfig
}

// NewCreationTrigger creates a CreationTrigger backed by the given registry.
func NewCreationTrigger(registry *Registry, cfg TriggerConfig) *CreationTrigger {
	if cfg.MaxCubesPerTenant > 0 {
		registry.maxCubes = cfg.MaxCubesPerTenant
	}
	if cfg.CardinalityLimits.MaxDistinctPerDim == 0 {
		cfg.CardinalityLimits = DefaultCardinalityLimits()
	}
	return &CreationTrigger{registry: registry, cfg: cfg}
}

// TryCreate attempts to register a cube for (tenant, dims, filters) on the first
// query for that pattern. It:
//  1. Checks the per-tenant cube limit.
//  2. Runs the cardinality gate using the supplied VCNT data+dir and time window.
//  3. If both pass, adds the cube to the registry via conditional-PUT (idempotent).
//
// Returns (result, nil) on success — result.Created distinguishes a fresh registration
// from a pre-existing entry found after a 412 conflict.
// Returns (zero, *CardinalityError) when the gate rejects the pattern.
// Returns (zero, *ErrLimitReached) when the per-tenant limit is exhausted.
// Returns (zero, err) on storage errors.
//
// The caller is responsible for any async backfill — TryCreate never blocks the
// query response.
func (t *CreationTrigger) TryCreate(
	ctx context.Context,
	tenant string,
	dims []string,
	filters []ColumnFilter,
	vcntData []byte,
	vcntDir []valuecounts.ChunkDirEntry,
	minTS, maxTS uint64,
) (TriggerResult, error) {
	// Step 1: check the per-tenant active-cube count before the cardinality gate to
	// avoid paying the VCNT I/O cost when the slot is already exhausted.
	cubes, _, err := t.registry.Load(ctx)
	if err != nil {
		return TriggerResult{}, fmt.Errorf("cube trigger: load index: %w", err)
	}
	// Check whether this exact pattern is already registered.
	cubeID := ComputeCubeID(tenant, dims, filters)
	for _, c := range cubes {
		if c.CubeID == cubeID {
			return TriggerResult{Entry: c}, nil // already exists
		}
	}
	// Limit check.
	if len(cubes) >= t.registry.maxCubes {
		return TriggerResult{}, &ErrLimitReached{Limit: t.registry.maxCubes}
	}

	// Step 2: cardinality gate.
	if err := CheckCardinality(vcntData, vcntDir, dims, t.cfg.CardinalityLimits, minTS, maxTS); err != nil {
		return TriggerResult{}, err
	}

	// Step 3: validate the hex CubeID is well-formed (IDFromBytes sanity check).
	if _, idErr := IDFromBytes(cubeID); idErr != nil {
		return TriggerResult{}, fmt.Errorf("cube trigger: compute ID: %w", idErr)
	}

	entry := RegistryEntry{
		CubeID:     cubeID,
		Tenant:     tenant,
		Dimensions: dims,
		Filters:    filters,
		Resolution: 1,                         // L0
		CreatedAt:  uint32(time.Now().Unix()), //nolint:gosec // unix timestamp fits uint32 until 2106
	}

	// Add via conditional-PUT. Registry.Add is idempotent: on a 412 conflict it retries,
	// re-reads the index, and returns nil if another querier already registered the same ID.
	if addErr := t.registry.Add(ctx, entry); addErr != nil {
		// ErrLimitReached or storage errors propagate.
		return TriggerResult{}, fmt.Errorf("cube trigger: register: %w", addErr)
	}

	// Re-read to confirm the exact entry (handles race where another querier won the PUT).
	// If another querier registered first, registry.Add returned nil after re-reading,
	// but our local `entry` has our CreatedAt. Reload to get the canonical stored entry.
	cubes, _, reloadErr := t.registry.Load(ctx)
	if reloadErr != nil {
		// Best-effort reload failed: return our local copy as created.
		return TriggerResult{Entry: entry, Created: true}, nil //nolint:nilerr
	}
	for _, c := range cubes {
		if c.CubeID == cubeID {
			// Determine whether we were the writer by comparing CreatedAt; if the stored
			// CreatedAt matches ours (within 1s) we won the PUT, otherwise another querier did.
			weWrote := c.CreatedAt == entry.CreatedAt
			return TriggerResult{Entry: c, Created: weWrote}, nil
		}
	}
	// Fallback: entry we wrote is in registry (Add succeeded), return it as created.
	return TriggerResult{Entry: entry, Created: true}, nil
}
