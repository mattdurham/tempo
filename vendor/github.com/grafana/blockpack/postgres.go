package blockpack

// postgres.go — the single Postgres entry point external callers (tempo) use.
// Before this, tempo constructed its own *pgxpool.Pool and threaded it
// directly into half a dozen blockpack functions (ApplyCubeSchema(pool),
// ApplyViUsageSchema(pool), ApplyFileCatalogSchema(pool), NewFileCatalogStore(pool),
// InsertViBackfillJob(pool, ...), etc.) plus its own PgPoolProvider capability
// interface exposing the raw pool back out to other tempo modules
// (block_catalog_notify.go). That means tempo had to know pgxpool exists at
// all, remember every Apply*Schema function and call them in some order, and
// pass a *pgxpool.Pool value across module boundaries as if it were tempo's
// own object.
//
// Postgres collapses all of it into one connect-once handle: tempo passes a
// DSN, gets back an opaque *Postgres, and every other blockpack Postgres
// capability is a method on it. Every schema this package owns
// (file_catalog, viusage, cube, compaction_jobs, column_manifest) is applied
// once at Connect time, so callers never need to remember which Apply*Schema
// functions exist or in what order to call them. Tempo's own, unrelated
// Postgres usage (backend_jobs/jobstore, tenant_redaction_state) is
// completely out of scope here -- this handle only ever touches tables
// blockpack itself owns.
//
// No back-compat: the old free functions (ApplyFileCatalogSchema,
// ApplyViUsageSchema, ApplyCubeSchema, ApplyCompactionJobsSchema,
// ApplyColumnManifestSchema, NewFileCatalogStore, NewPgViUsageRegistry,
// NewPgCubeRegistry, NewPgViUsageEntryStore, NewPgCubeEntryStore,
// NewPgColumnManifestStore, InsertViBackfillJob, InsertCubeBackfillJob) are
// deleted outright, not deprecated-and-kept -- every caller (production and
// test) migrates to the method form.

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/modules/colhashmanifest"
	"github.com/grafana/blockpack/internal/modules/cube"
	"github.com/grafana/blockpack/internal/modules/pgcatalog"
	"github.com/grafana/blockpack/internal/modules/pgqueue"
	"github.com/grafana/blockpack/internal/modules/viusage"
)

var _ colhashmanifest.Store = (*colhashmanifest.PgStore)(nil)

// Postgres is a connected handle to every Postgres-backed capability this
// package owns. The zero value is not usable -- construct via ConnectPostgres.
type Postgres struct {
	pool *pgxpool.Pool
}

// ConnectPostgres connects to dsn and applies every schema this package owns,
// once, in dependency order. Returns an error (closing the pool first) if any
// schema application fails, so a caller never ends up holding a handle whose
// tables might not exist.
func ConnectPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("blockpack: connect postgres: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.ApplySchemas(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

// NewPostgresFromPool wraps an already-connected pool (schemas assumed
// already applied by whoever connected it -- typically another call to
// ConnectPostgres against the same DSN) without touching the connection or
// re-applying any schema. For the rarer caller that already holds a live
// pool for other reasons (e.g. tempo's own backend_jobs queue sharing one
// connection) and just wants this package's methods over it.
func NewPostgresFromPool(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// ApplySchemas applies every schema this package owns against p's pool, in
// dependency order. ConnectPostgres already calls this once; exported
// separately for the NewPostgresFromPool caller that constructed its own
// pool (e.g. for connection tuning ConnectPostgres doesn't expose) and needs
// to apply schemas itself before first use.
func (p *Postgres) ApplySchemas(ctx context.Context) error {
	if err := pgcatalog.ApplyFileCatalogSchema(ctx, p.pool); err != nil {
		return fmt.Errorf("blockpack: apply file_catalog schema: %w", err)
	}
	if err := viusage.ApplySchema(ctx, p.pool); err != nil {
		return fmt.Errorf("blockpack: apply viusage schema: %w", err)
	}
	if err := cube.ApplySchema(ctx, p.pool); err != nil {
		return fmt.Errorf("blockpack: apply cube schema: %w", err)
	}
	if err := pgqueue.ApplyCompactionJobsSchema(ctx, p.pool); err != nil {
		return fmt.Errorf("blockpack: apply compaction_jobs schema: %w", err)
	}
	if err := colhashmanifest.ApplySchema(ctx, p.pool); err != nil {
		return fmt.Errorf("blockpack: apply column_manifest schema: %w", err)
	}
	return nil
}

// Close closes the underlying connection pool. Nil-safe (both on a nil
// *Postgres and a nil pool), mirroring every other Close method in this
// codebase's convention.
func (p *Postgres) Close() {
	if p == nil || p.pool == nil {
		return
	}
	p.pool.Close()
}

// Pool returns the underlying *pgxpool.Pool for the rare caller that
// genuinely needs it directly (e.g. a test seeding a fixture row with raw
// SQL against a table this package owns). Production code should prefer a
// dedicated method below over this escape hatch.
func (p *Postgres) Pool() *pgxpool.Pool {
	return p.pool
}

// FileCatalogStore returns the blockpack_file_catalog store.
func (p *Postgres) FileCatalogStore() *FileCatalogStore {
	return pgcatalog.NewStore(p.pool)
}

// ViUsageRegistry returns a Postgres-backed viusage Registry for tenant.
func (p *Postgres) ViUsageRegistry(tenant string) *Registry {
	return viusage.NewPgRegistry(p.pool, tenant)
}

// ViUsageEntryStore returns the native Postgres-backed viusage EntryStore,
// for the rarer caller that wants the EntryStore seam directly instead of a
// ready-made Registry.
func (p *Postgres) ViUsageEntryStore() EntryStore {
	return viusage.NewPgEntryStore(p.pool)
}

// CubeRegistry returns a Postgres-backed cube CubeRegistry for tenant.
func (p *Postgres) CubeRegistry(tenant string) *CubeRegistry {
	return cube.NewPgRegistry(p.pool, tenant)
}

// CubeEntryStore returns the native Postgres-backed cube CubeEntryStore, for
// the rarer caller that wants the CubeEntryStore seam directly instead of a
// ready-made CubeRegistry.
func (p *Postgres) CubeEntryStore() CubeEntryStore {
	return cube.NewPgEntryStore(p.pool)
}

// ColumnManifestStore returns the Postgres-backed column-hash manifest store.
func (p *Postgres) ColumnManifestStore() colhashmanifest.Store {
	return colhashmanifest.NewPgStore(p.pool)
}

// InsertViBackfillHistory bulk-inserts one pending vi_backfill job per 1-minute window covering
// [now-retention, now) for tenant's column, newest-window-first priority (issue #529). Called
// from tempo's reactive first-trigger query-path hook the instant a never-before-queried column
// is seen, with retention resolved from the tenant's own BlockRetention override -- the caller
// no longer picks an unbounded/arbitrary window; every job is a fixed 1-minute slice, and the
// full retention history is queued immediately instead of trickling in one bounded chunk per
// compaction-planner tick. Idempotent per window: a repeat call for the same column (e.g. a
// crash-recovery re-trigger) silently no-ops on windows already queued, non-terminal.
//
// compaction-planner's own periodic top-up (the "ongoing coverage for new data" half of #529)
// calls the same underlying pgqueue.Store.InsertViBackfillWindows directly for a small trailing
// range on every tick -- it lives entirely inside blockpack and has no need to cross this root
// API boundary.
func (p *Postgres) InsertViBackfillHistory(
	ctx context.Context,
	tenant string,
	col ViBackfillColumn,
	retention time.Duration,
	now time.Time,
) error {
	return pgqueue.New(p.pool).InsertViBackfillWindows(
		ctx, tenant, col, pgqueue.WindowsForRetention(retention, now),
	)
}

// InsertCubeBackfillJob mirrors InsertViBackfillJob for cube_backfill,
// deduped on (tenant, cube ID).
func (p *Postgres) InsertCubeBackfillJob(ctx context.Context, tenant string, d CubeBackfillDetail) error {
	return pgqueue.New(p.pool).InsertCubeBackfill(ctx, tenant, d)
}

// ViBackfillGapRanges returns every NOT-yet-succeeded 1-minute window for (tenant, col), merged
// into the minimal number of contiguous ranges (issue #529) -- the query-time coverage-check
// primitive callers use in place of a single scalar watermark, which cannot correctly represent
// a column whose windows complete out of order across many parallel workers. An empty,
// non-error result means every enqueued window for this column has succeeded.
func (p *Postgres) ViBackfillGapRanges(
	ctx context.Context,
	tenant string,
	col ViBackfillColumn,
) ([]WindowRange, error) {
	return pgqueue.New(p.pool).ViBackfillGapRanges(ctx, tenant, col)
}
