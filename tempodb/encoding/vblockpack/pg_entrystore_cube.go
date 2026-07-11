package vblockpack

// pg_entrystore_cube.go — Postgres-backed blockpack.CubeEntryStore implementation
// (mirrors pg_entrystore.go's viusage implementation, 2026-07-11). One row per
// cube_id in cube_entries. Unlike viusage's single generic UpsertEntry, cube's
// three write operations (Add/Remove/UpdateWatermarks) have genuinely different
// list-mutation semantics -- mirrored here with one method per operation, per
// blockpack's own entryStore design (R1: match cube's actual usage pattern
// rather than forcing a generic create-or-mutate primitive it doesn't need).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgCubeEntryStore satisfies blockpack.CubeEntryStore over a *pgxpool.Pool.
type pgCubeEntryStore struct{ pool *pgxpool.Pool }

func newPgCubeEntryStore(pool *pgxpool.Pool) *pgCubeEntryStore {
	return &pgCubeEntryStore{pool: pool}
}

const cubeSelectAllSQL = `
	SELECT cube_id, tenant, dimensions, filters, agg_attrs, resolution, created_at, watermarks
	FROM cube_entries WHERE tenant = $1`

// cubeRowScanner is satisfied by both pgx.Rows and pgx.Row.
type cubeRowScanner interface {
	Scan(dest ...any) error
}

func scanCubeEntry(row cubeRowScanner) (blockpack.CubeRegistryEntry, error) {
	var e blockpack.CubeRegistryEntry
	var dimensions, filters, aggAttrs, watermarks []byte
	err := row.Scan(&e.CubeID, &e.Tenant, &dimensions, &filters, &aggAttrs, &e.Resolution, &e.CreatedAt, &watermarks)
	if err != nil {
		return blockpack.CubeRegistryEntry{}, err
	}
	if err := json.Unmarshal(dimensions, &e.Dimensions); err != nil {
		return blockpack.CubeRegistryEntry{}, fmt.Errorf("decoding dimensions: %w", err)
	}
	if err := json.Unmarshal(filters, &e.Filters); err != nil {
		return blockpack.CubeRegistryEntry{}, fmt.Errorf("decoding filters: %w", err)
	}
	if err := json.Unmarshal(aggAttrs, &e.AggAttrs); err != nil {
		return blockpack.CubeRegistryEntry{}, fmt.Errorf("decoding agg_attrs: %w", err)
	}
	if len(watermarks) > 0 {
		if err := json.Unmarshal(watermarks, &e.Watermarks); err != nil {
			return blockpack.CubeRegistryEntry{}, fmt.Errorf("decoding watermarks: %w", err)
		}
	}
	return e, nil
}

func (s *pgCubeEntryStore) Load(ctx context.Context, tenant string) ([]blockpack.CubeRegistryEntry, error) {
	rows, err := s.pool.Query(ctx, cubeSelectAllSQL, tenant)
	if err != nil {
		return nil, fmt.Errorf("pg cube entrystore: load: %w", err)
	}
	defer rows.Close()

	var out []blockpack.CubeRegistryEntry
	for rows.Next() {
		e, scanErr := scanCubeEntry(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("pg cube entrystore: scan: %w", scanErr)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AddEntry registers entry for tenant, idempotent on entry.CubeID already
// existing and enforcing maxCubes -- mirrors blobEntryStore.addEntry's exact
// contract. Serialized per-tenant via pg_advisory_xact_lock: unlike viusage's
// UpsertEntry (which always has an existing-or-about-to-exist row to SELECT
// ... FOR UPDATE), the FIRST cube for a tenant has no row to lock, so the
// idempotency-check-then-maxCubes-check-then-insert sequence needs an
// explicit advisory lock instead to prevent two concurrent first-cube
// registrations from both passing the maxCubes check.
func (s *pgCubeEntryStore) AddEntry(ctx context.Context, tenant string, entry blockpack.CubeRegistryEntry, maxCubes int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, tenant); err != nil {
		return fmt.Errorf("pg cube entrystore: advisory lock: %w", err)
	}

	var count int
	var alreadyPresent bool
	rows, err := tx.Query(ctx, `SELECT cube_id FROM cube_entries WHERE tenant = $1`, tenant)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: count: %w", err)
	}
	for rows.Next() {
		var cubeID string
		if scanErr := rows.Scan(&cubeID); scanErr != nil {
			rows.Close()
			return fmt.Errorf("pg cube entrystore: count scan: %w", scanErr)
		}
		count++
		if cubeID == entry.CubeID {
			alreadyPresent = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pg cube entrystore: count rows: %w", err)
	}

	if alreadyPresent {
		return nil // idempotent, mirrors blobEntryStore.addEntry
	}
	if count >= maxCubes {
		return &blockpack.CubeErrLimitReached{Limit: maxCubes}
	}

	dimensions, err := json.Marshal(entry.Dimensions)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: encode dimensions: %w", err)
	}
	filters, err := json.Marshal(entry.Filters)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: encode filters: %w", err)
	}
	aggAttrs, err := json.Marshal(entry.AggAttrs)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: encode agg_attrs: %w", err)
	}
	watermarks, err := json.Marshal(entry.Watermarks)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: encode watermarks: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO cube_entries (cube_id, tenant, dimensions, filters, agg_attrs, resolution, created_at, watermarks)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		entry.CubeID, entry.Tenant, dimensions, filters, aggAttrs, entry.Resolution, entry.CreatedAt, watermarks,
	)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg cube entrystore: commit: %w", err)
	}
	return nil
}

// RemoveEntry deletes the entry with cubeID for tenant, a no-op if already
// absent -- mirrors blobEntryStore.removeEntry's exact contract.
func (s *pgCubeEntryStore) RemoveEntry(ctx context.Context, tenant, cubeID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM cube_entries WHERE tenant = $1 AND cube_id = $2`, tenant, cubeID)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: remove: %w", err)
	}
	return nil
}

// UpdateWatermarksEntry expands cubeID's stored watermark at level to cover
// [minMinute, maxMinute] (min-of-mins, max-of-maxes) -- mirrors
// blobEntryStore.updateWatermarksEntry's exact contract. Errors if cubeID is
// not found. SELECT ... FOR UPDATE provides the same read-modify-write
// atomicity viusage's UpsertEntry relies on.
func (s *pgCubeEntryStore) UpdateWatermarksEntry(ctx context.Context, tenant, cubeID string, level, minMinute, maxMinute uint32) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var watermarksRaw []byte
	row := tx.QueryRow(ctx, `SELECT watermarks FROM cube_entries WHERE tenant = $1 AND cube_id = $2 FOR UPDATE`, tenant, cubeID)
	if err := row.Scan(&watermarksRaw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("pg cube entrystore: update watermarks: cube %q not found", cubeID)
		}
		return fmt.Errorf("pg cube entrystore: load for update: %w", err)
	}

	watermarks := map[uint32]blockpack.CubeResolutionWatermark{}
	if len(watermarksRaw) > 0 {
		if err := json.Unmarshal(watermarksRaw, &watermarks); err != nil {
			return fmt.Errorf("pg cube entrystore: decoding watermarks: %w", err)
		}
		// json.Marshal(nil map) produces the JSON literal "null" (not "{}"), and
		// unmarshaling "null" into a map resets it to nil, overwriting the
		// pre-initialized empty map above -- reinitialize so the assignment below
		// never panics on a nil map (confirmed via a real Postgres run: AddEntry's
		// nil-Watermarks-on-creation path round-trips through exactly this null case).
		if watermarks == nil {
			watermarks = map[uint32]blockpack.CubeResolutionWatermark{}
		}
	}

	newWm := blockpack.CubeResolutionWatermark{MinMinute: minMinute, MaxMinute: maxMinute}
	if existing, ok := watermarks[level]; ok {
		if existing.MinMinute < newWm.MinMinute {
			newWm.MinMinute = existing.MinMinute
		}
		if existing.MaxMinute > newWm.MaxMinute {
			newWm.MaxMinute = existing.MaxMinute
		}
	}
	watermarks[level] = newWm

	encoded, err := json.Marshal(watermarks)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: encoding watermarks: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE cube_entries SET watermarks = $3 WHERE tenant = $1 AND cube_id = $2`, tenant, cubeID, encoded); err != nil {
		return fmt.Errorf("pg cube entrystore: update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg cube entrystore: commit: %w", err)
	}
	return nil
}

var _ blockpack.CubeEntryStore = (*pgCubeEntryStore)(nil)
