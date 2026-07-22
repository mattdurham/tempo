package cube

// pg_entry_store.go — native Postgres-backed EntryStore implementation, ported
// verbatim from tempo's tempodb/encoding/vblockpack/pg_entrystore_cube.go
// (2026-07-11), package-adjusted to live directly inside package cube (no more
// blockpack.-qualification needed since this package now defines
// RegistryEntry/ResolutionWatermark/EntryStore natively). One row per cube_id
// in cube_entries. Unlike viusage's single generic UpsertEntry, cube's three
// write operations (Add/Remove/UpdateWatermarks) have genuinely different
// list-mutation semantics -- mirrored here with one method per operation, per
// entry_store.go's own R1 design note.
//
// SPEC-CUBE-031: PgEntryStore must be behaviorally identical to blobEntryStore
// for every Registry public method -- see pg_blob_differential_test.go.

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/pgschema"
)

//go:embed schema.sql
var cubeSchemaSQL string

// ApplySchema applies cube_entries' schema (and its tenant index) against
// pool. Exported, never called automatically by any constructor -- the
// embedding application calls it once at its own startup, mirroring tempo's
// own migrate.Apply precedent.
func ApplySchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgschema.ApplyStatements(ctx, pool, cubeSchemaSQL)
}

// PgEntryStore satisfies EntryStore over a *pgxpool.Pool.
type PgEntryStore struct{ pool *pgxpool.Pool }

// NewPgEntryStore constructs a PgEntryStore over pool, satisfying EntryStore.
func NewPgEntryStore(pool *pgxpool.Pool) *PgEntryStore {
	return &PgEntryStore{pool: pool}
}

const cubeSelectAllSQL = `
	SELECT cube_id, tenant, dimensions, filters, agg_attrs, resolution, created_at, watermarks
	FROM cube_entries WHERE tenant = $1`

// cubeRowScanner is satisfied by both pgx.Rows and pgx.Row.
type cubeRowScanner interface {
	Scan(dest ...any) error
}

func scanCubeEntry(row cubeRowScanner) (RegistryEntry, error) {
	var e RegistryEntry
	var dimensions, filters, aggAttrs, watermarks []byte
	err := row.Scan(&e.CubeID, &e.Tenant, &dimensions, &filters, &aggAttrs, &e.Resolution, &e.CreatedAt, &watermarks)
	if err != nil {
		return RegistryEntry{}, err
	}
	if err := json.Unmarshal(dimensions, &e.Dimensions); err != nil {
		return RegistryEntry{}, fmt.Errorf("decoding dimensions: %w", err)
	}
	if err := json.Unmarshal(filters, &e.Filters); err != nil {
		return RegistryEntry{}, fmt.Errorf("decoding filters: %w", err)
	}
	if err := json.Unmarshal(aggAttrs, &e.AggAttrs); err != nil {
		return RegistryEntry{}, fmt.Errorf("decoding agg_attrs: %w", err)
	}
	if len(watermarks) > 0 {
		if err := json.Unmarshal(watermarks, &e.Watermarks); err != nil {
			return RegistryEntry{}, fmt.Errorf("decoding watermarks: %w", err)
		}
	}
	return e, nil
}

// Load returns every cube entry for tenant.
func (s *PgEntryStore) Load(ctx context.Context, tenant string) ([]RegistryEntry, error) {
	rows, err := s.pool.Query(ctx, cubeSelectAllSQL, tenant)
	if err != nil {
		return nil, fmt.Errorf("pg cube entrystore: load: %w", err)
	}
	defer rows.Close()

	var out []RegistryEntry
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
// existing -- mirrors blobEntryStore.addEntry's exact contract. The cardinality
// gate this used to also enforce was removed repo-wide by blockpack issue #497
// (see blockpack NOTE-CUBE-029); cube count per tenant is now unbounded here
// too. Serialized per-tenant via pg_advisory_xact_lock: unlike viusage's
// UpsertEntry (which always has an existing-or-about-to-exist row to SELECT
// ... FOR UPDATE), the FIRST cube for a tenant has no row to lock, so the
// idempotency-check-then-insert sequence needs an explicit advisory lock
// instead to prevent two concurrent registrations of the same new CubeID from
// both passing the "not present" check before either INSERT commits.
func (s *PgEntryStore) AddEntry(ctx context.Context, tenant string, entry RegistryEntry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, lockErr := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, tenant); lockErr != nil {
		return fmt.Errorf("pg cube entrystore: advisory lock: %w", lockErr)
	}

	var alreadyPresent bool
	row := tx.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM cube_entries WHERE tenant = $1 AND cube_id = $2)`,
		tenant,
		entry.CubeID,
	)
	if scanErr := row.Scan(&alreadyPresent); scanErr != nil {
		return fmt.Errorf("pg cube entrystore: exists check: %w", scanErr)
	}
	if alreadyPresent {
		return nil // idempotent, mirrors blobEntryStore.addEntry
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

	// string(...), not the raw []byte: under simple_protocol query mode (required for pgbouncer
	// transaction pooling), pgx encodes a []byte argument as a bytea hex literal, which Postgres
	// then rejects casting into a jsonb column ("invalid input syntax for type json") -- a plain
	// Go string is sent as a text literal instead, which Postgres CAN implicitly cast to jsonb.
	_, err = tx.Exec(
		ctx, `
		INSERT INTO cube_entries (cube_id, tenant, dimensions, filters, agg_attrs, resolution, created_at, watermarks)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		entry.CubeID, entry.Tenant, string(dimensions), string(filters), string(aggAttrs), entry.Resolution, entry.CreatedAt, string(watermarks),
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
func (s *PgEntryStore) RemoveEntry(ctx context.Context, tenant, cubeID string) error {
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
func (s *PgEntryStore) UpdateWatermarksEntry(
	ctx context.Context,
	tenant, cubeID string,
	level, minMinute, maxMinute uint32,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg cube entrystore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var watermarksRaw []byte
	row := tx.QueryRow(
		ctx,
		`SELECT watermarks FROM cube_entries WHERE tenant = $1 AND cube_id = $2 FOR UPDATE`,
		tenant,
		cubeID,
	)
	if scanErr := row.Scan(&watermarksRaw); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return fmt.Errorf("pg cube entrystore: update watermarks: cube %q not found", cubeID)
		}
		return fmt.Errorf("pg cube entrystore: load for update: %w", scanErr)
	}

	watermarks := map[uint32]ResolutionWatermark{}
	if len(watermarksRaw) > 0 {
		if decodeErr := json.Unmarshal(watermarksRaw, &watermarks); decodeErr != nil {
			return fmt.Errorf("pg cube entrystore: decoding watermarks: %w", decodeErr)
		}
		// json.Marshal(nil map) produces the JSON literal "null" (not "{}"), and
		// unmarshaling "null" into a map resets it to nil, overwriting the
		// pre-initialized empty map above -- reinitialize so the assignment below
		// never panics on a nil map (confirmed via a real Postgres run: AddEntry's
		// nil-Watermarks-on-creation path round-trips through exactly this null case).
		if watermarks == nil {
			watermarks = map[uint32]ResolutionWatermark{}
		}
	}

	newWm := ResolutionWatermark{MinMinute: minMinute, MaxMinute: maxMinute}
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
	// string(encoded), not the raw []byte: see AddEntry's identical comment above.
	if _, err := tx.Exec(ctx, `UPDATE cube_entries SET watermarks = $3 WHERE tenant = $1 AND cube_id = $2`, tenant, cubeID, string(encoded)); err != nil {
		return fmt.Errorf("pg cube entrystore: update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg cube entrystore: commit: %w", err)
	}
	return nil
}

var _ EntryStore = (*PgEntryStore)(nil)
