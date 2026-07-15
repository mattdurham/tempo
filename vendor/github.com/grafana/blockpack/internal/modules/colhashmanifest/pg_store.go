package colhashmanifest

// pg_store.go — native Postgres-backed Store implementation. NOTE: this is a
// deliberate simplification (cross-ref NOTE-COLMANIFEST-1, NOTE-COLMANIFEST-3,
// SPEC-COLMANIFEST-6): a generic key/blob table (Open Decision D1, approved by
// spec-oracle-506), NOT a normalized per-Entry table. No transaction, no
// advisory lock, no SELECT ... FOR UPDATE -- colhashmanifest's own spec
// (SPEC-COLMANIFEST-1/2) explicitly tolerates lost updates under concurrency
// and has no locking/ETag ceremony to preserve; giving this best-effort,
// advisory-only feature cube/viusage's row-lock rigor would be over-
// engineering it. A plain parametrized SELECT / INSERT ... ON CONFLICT DO
// UPDATE is sufficient.

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/pgschema"
)

//go:embed schema.sql
var colhashmanifestSchemaSQL string

// ApplySchema applies column_manifest_blobs' schema against pool. Exported,
// never called automatically by any constructor -- the embedding application
// calls it once at its own startup, mirroring cube/viusage's ApplySchema shape.
func ApplySchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgschema.ApplyStatements(ctx, pool, colhashmanifestSchemaSQL)
}

// PgStore satisfies Store over a *pgxpool.Pool.
type PgStore struct{ pool *pgxpool.Pool }

// NewPgStore constructs a PgStore over pool, satisfying Store.
func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{pool: pool}
}

// Get reads the entire blob stored at key. Returns a real, non-nil error on a
// missing key (never (nil, nil) for a miss) -- Store's contract requires a
// genuine error signal here, even though SPEC-COLMANIFEST-2/Load treats ANY
// Get error identically as "empty manifest" one layer up, so this is
// behaviorally equivalent either way from Load's perspective.
func (s *PgStore) Get(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	row := s.pool.QueryRow(ctx, `SELECT data FROM column_manifest_blobs WHERE key = $1`, key)
	if err := row.Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("pg colhashmanifest store: get %q: not found", key)
		}
		return nil, fmt.Errorf("pg colhashmanifest store: get %q: %w", key, err)
	}
	return data, nil
}

// Put writes data to key, creating or overwriting it (no versioning/ETag).
// updated_at is computed server-side on every insert/update so operators
// querying the table directly can see when a manifest was last written,
// without widening Store's own Get/Put signature.
func (s *PgStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.pool.Exec(
		ctx, `
		INSERT INTO column_manifest_blobs (key, data, updated_at)
		VALUES ($1, $2, extract(epoch from now())::bigint)
		ON CONFLICT (key) DO UPDATE SET data = $2, updated_at = extract(epoch from now())::bigint`,
		key, data,
	)
	if err != nil {
		return fmt.Errorf("pg colhashmanifest store: put %q: %w", key, err)
	}
	return nil
}

var _ Store = (*PgStore)(nil)
