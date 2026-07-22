package pgcatalog

// SPEC-PGCATALOG-1: one shared table discriminated by subsystem ('vi' |
// 'vcnt' | 'cube'), object_key globally unique across all subsystems. See
// SPECS.md.

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/pgschema"
)

//go:embed schema.sql
var fileCatalogSchemaSQL string

// ApplyFileCatalogSchema applies blockpack_file_catalog's schema against
// pool. Exported, never called automatically by any constructor -- the
// embedding application calls it once at its own startup, mirroring
// cube/viusage/colhashmanifest's ApplySchema precedent.
func ApplyFileCatalogSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgschema.ApplyStatements(ctx, pool, fileCatalogSchemaSQL)
}

// Row is one blockpack_file_catalog row: one physical object, at some
// subsystem-defined merge level, for one (tenant, resource_id).
type Row struct {
	CreatedAt   time.Time
	DeletedAt   *time.Time
	CompactedAt *time.Time
	ObjectKey   string
	Subsystem   string
	ResourceID  string
	Tenant      string
	Meta        []byte
	RowID       int64
	MinSec      int64
	MaxSec      int64
	SizeBytes   int64
	Level       int
}

// TraceBlockMeta is the JSON shape stored in a subsystem="trace" Row's Meta column -- the
// tempo backend.BlockMeta fields this table's own typed columns don't already cover
// (Tenant/MinSec/MaxSec/SizeBytes map onto Tenant/StartTime/EndTime/Size_; BlockID is parsed
// from ObjectKey's own path). Field names deliberately mirror backend.BlockMeta's own proto
// field names (issue #522/#525) -- tempo's block-builder notification hook populates this at
// block-creation time (it already has the full BlockMeta in hand, zero extra I/O), and tempo's
// Postgres-sourced block lister unmarshals it back to reconstruct a complete BlockMeta with no
// per-block meta.json fetch ever needed.
type TraceBlockMeta struct {
	Version           string                 `json:"version"`
	DedicatedColumns  []TraceDedicatedColumn `json:"dedicatedColumns,omitempty"`
	TotalObjects      int64                  `json:"totalObjects"`
	TotalRecords      int64                  `json:"totalRecords"`
	IndexPageSize     int                    `json:"indexPageSize"`
	BloomShardCount   int                    `json:"bloomShardCount"`
	FooterSize        int                    `json:"footerSize"`
	ReplicationFactor uint32                 `json:"replicationFactor"`
}

// TraceDedicatedColumn mirrors tempo's backend.DedicatedColumn own JSON shape
// (Scope/Name/Type) -- duplicated here rather than imported, since this package cannot depend
// on tempo's backend package (this repo has no dependency on tempo at all), and distinctly
// named from this repo's OWN, unrelated, simpler DedicatedColumn (internal/modules/blockio/
// writer/dedicatedcolumn.go, Name-only) to avoid confusing the two. Field names/JSON tags
// must stay in sync with tempo's backend.DedicatedColumn by hand.
type TraceDedicatedColumn struct {
	Scope string `json:"scope"`
	Name  string `json:"name"`
	Type  string `json:"type"`
}

// Store satisfies pgcatalog's Postgres-backed catalog operations over a
// *pgxpool.Pool.
type Store struct{ pool *pgxpool.Pool }

// NewStore constructs a Store over pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Insert adds row to the catalog. SPEC-PGCATALOG-2: idempotent on
// row.ObjectKey -- if it already has a row, this is a silent no-op rather
// than a duplicate-key error, so a caller retrying after a crash between
// writing the object and reporting job success can safely re-Insert the same
// row.
func (s *Store) Insert(ctx context.Context, row Row) error {
	_, err := s.pool.Exec(
		ctx, `
		INSERT INTO blockpack_file_catalog
			(subsystem, tenant, resource_id, object_key, level, min_sec, max_sec, size_bytes, meta)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (object_key) DO NOTHING`,
		row.Subsystem, row.Tenant, row.ResourceID, row.ObjectKey, row.Level, row.MinSec, row.MaxSec, row.SizeBytes, row.Meta,
	)
	if err != nil {
		return fmt.Errorf("pgcatalog: insert %q: %w", row.ObjectKey, err)
	}
	return nil
}

// MarkCompacted sets compacted_at = now() on every row in objectKeys that
// isn't already marked, excluding them from ListCandidates from this point
// on. SPEC-PGCATALOG-3: never touches or deletes the underlying storage
// object -- physically deleting the object once the grace window has elapsed
// is the reaper's job (see ListCompactedOlderThan), not this method's.
func (s *Store) MarkCompacted(ctx context.Context, objectKeys []string) error {
	if len(objectKeys) == 0 {
		return nil
	}
	_, err := s.pool.Exec(
		ctx, `
		UPDATE blockpack_file_catalog
		SET compacted_at = now()
		WHERE object_key = ANY($1) AND compacted_at IS NULL`,
		objectKeys,
	)
	if err != nil {
		return fmt.Errorf("pgcatalog: mark compacted: %w", err)
	}
	return nil
}

// ListCandidates returns every live (not compacted, not deleted) row for
// (subsystem, tenant, resourceID), ordered by level then object_key.
// SPEC-PGCATALOG-4: this ordering is load-bearing -- a compaction
// candidate-selection query groups consecutive rows by
// (tenant, resource_id, level) from it (plan.md Section 1.2/2.2/3.2).
func (s *Store) ListCandidates(ctx context.Context, subsystem, tenant, resourceID string) ([]Row, error) {
	rows, err := s.pool.Query(
		ctx, `
		SELECT row_id, subsystem, tenant, resource_id, object_key, level, min_sec, max_sec,
			size_bytes, created_at, compacted_at, deleted_at, meta
		FROM blockpack_file_catalog
		WHERE subsystem = $1 AND tenant = $2 AND resource_id = $3
			AND compacted_at IS NULL AND deleted_at IS NULL
		ORDER BY level, object_key`,
		subsystem, tenant, resourceID,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list candidates: %w", err)
	}
	defer rows.Close()
	return scanFileCatalogRows(rows)
}

// ListLiveKeys returns every live (not compacted, not deleted) row for
// (subsystem, tenant), across ALL resourceIDs -- unlike ListCandidates, which
// is scoped to one resourceID for compaction's own pairwise grouping.
// SPEC-PGCATALOG-8: this is catalog_reconcile's own candidate set for
// detecting rows whose backing object has vanished out-of-band (files fall
// out of retention or otherwise disappear independent of this system's own
// compaction/reap actions) -- see compactionworker's own SPEC-COMPACTIONWORKER-8.
func (s *Store) ListLiveKeys(ctx context.Context, subsystem, tenant string) ([]Row, error) {
	rows, err := s.pool.Query(
		ctx, `
		SELECT row_id, subsystem, tenant, resource_id, object_key, level, min_sec, max_sec,
			size_bytes, created_at, compacted_at, deleted_at, meta
		FROM blockpack_file_catalog
		WHERE subsystem = $1 AND tenant = $2
			AND compacted_at IS NULL AND deleted_at IS NULL`,
		subsystem, tenant,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list live keys: %w", err)
	}
	defer rows.Close()
	return scanFileCatalogRows(rows)
}

// ListLiveKeysInRange returns every live (not compacted, not deleted) row for
// (subsystem, tenant) whose [min_sec, max_sec] overlaps [minSec, maxSec], newest-first
// (ORDER BY min_sec DESC) -- SPEC-PGCATALOG-9: vi_backfill's own BlockFetcher.ListBlocksInRange
// (issue #522, vi_backfill moved fully into blockpack) needs this exact contract:
// newest-to-oldest so BackfillEngine.Run processes newest data first, mirroring tempo's
// now-retired viBlockFetcher/catalogBlockFetcher's identical ordering.
func (s *Store) ListLiveKeysInRange(ctx context.Context, subsystem, tenant string, minSec, maxSec uint64) ([]Row, error) {
	rows, err := s.pool.Query(
		ctx, `
		SELECT row_id, subsystem, tenant, resource_id, object_key, level, min_sec, max_sec,
			size_bytes, created_at, compacted_at, deleted_at, meta
		FROM blockpack_file_catalog
		WHERE subsystem = $1 AND tenant = $2
			AND compacted_at IS NULL AND deleted_at IS NULL
			AND max_sec >= $3 AND min_sec <= $4
		ORDER BY min_sec DESC`,
		subsystem, tenant, minSec, maxSec,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list live keys in range: %w", err)
	}
	defer rows.Close()
	return scanFileCatalogRows(rows)
}

// ListCompactedOlderThan returns every row whose compacted_at is non-NULL and
// strictly before cutoff, and not yet deleted. SPEC-PGCATALOG-5: this is the
// reaper's candidate set for physical deletion once the grace window has
// elapsed (plan.md Section C).
func (s *Store) ListCompactedOlderThan(ctx context.Context, cutoff time.Time) ([]Row, error) {
	rows, err := s.pool.Query(
		ctx, `
		SELECT row_id, subsystem, tenant, resource_id, object_key, level, min_sec, max_sec,
			size_bytes, created_at, compacted_at, deleted_at, meta
		FROM blockpack_file_catalog
		WHERE compacted_at IS NOT NULL AND compacted_at < $1 AND deleted_at IS NULL`,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list compacted older than: %w", err)
	}
	defer rows.Close()
	return scanFileCatalogRows(rows)
}

// ListCompactedKeys returns the subset of keys already marked compacted
// (compacted_at IS NOT NULL) in blockpack_file_catalog. object_key is
// globally unique across every subsystem (SPEC-PGCATALOG-1), so no
// subsystem/tenant filter is needed to disambiguate. SPEC-PGCATALOG-7 (issue
// #522 Phase 2.1): this is the mandatory VCNT read-path filter's query --
// callers exclude every returned key from a query-time fetch/sum BEFORE
// downloading it, closing NOTE-VC-009's double-counting window (a
// compacted-but-undeleted source coexisting with its merged replacement for
// up to the reaper's 30-minute grace window).
func (s *Store) ListCompactedKeys(ctx context.Context, keys []string) (map[string]struct{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(
		ctx, `SELECT object_key FROM blockpack_file_catalog WHERE object_key = ANY($1) AND compacted_at IS NOT NULL`,
		keys,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list compacted keys: %w", err)
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("pgcatalog: scan compacted key: %w", err)
		}
		out[key] = struct{}{}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("pgcatalog: iterate compacted keys: %w", rows.Err())
	}
	return out, nil
}

// ListLiveTenants returns every distinct tenant with at least one row for subsystem that
// isn't hard-deleted (live OR still within its post-compaction grace window) -- issue
// #522/#525: tempo's Postgres-sourced block lister needs this for tenant enumeration, the
// same role backend.Reader.Tenants() plays for the classic S3 bucket-index poller.
func (s *Store) ListLiveTenants(ctx context.Context, subsystem string) ([]string, error) {
	rows, err := s.pool.Query(
		ctx, `SELECT DISTINCT tenant FROM blockpack_file_catalog WHERE subsystem = $1 AND deleted_at IS NULL`,
		subsystem,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list live tenants: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return nil, fmt.Errorf("pgcatalog: scan tenant: %w", err)
		}
		out = append(out, tenant)
	}
	return out, rows.Err()
}

// ListCompactedNotDeleted returns every row for (subsystem, tenant) that has been compacted
// but not yet physically reaped -- issue #522/#525: the Postgres-sourced equivalent of the
// classic poller's "compacted blocklist" (blocks still visible during their post-compaction
// grace window), unscoped by any cutoff time (unlike ListCompactedOlderThan, which is the
// reaper's own candidate query).
func (s *Store) ListCompactedNotDeleted(ctx context.Context, subsystem, tenant string) ([]Row, error) {
	rows, err := s.pool.Query(
		ctx, `
		SELECT row_id, subsystem, tenant, resource_id, object_key, level, min_sec, max_sec,
			size_bytes, created_at, compacted_at, deleted_at, meta
		FROM blockpack_file_catalog
		WHERE subsystem = $1 AND tenant = $2
			AND compacted_at IS NOT NULL AND deleted_at IS NULL`,
		subsystem, tenant,
	)
	if err != nil {
		return nil, fmt.Errorf("pgcatalog: list compacted not deleted: %w", err)
	}
	defer rows.Close()
	return scanFileCatalogRows(rows)
}

// DeleteRow hard-deletes the row identified by rowID. Callers (the reaper's
// backend-worker handler) must only call this AFTER the row's underlying
// object has been physically deleted from storage -- there is no audit value
// in retaining a blockpack_file_catalog row once its object is confirmed
// gone, unlike backend_jobs' terminal rows (plan.md Section C).
func (s *Store) DeleteRow(ctx context.Context, rowID int64) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM blockpack_file_catalog WHERE row_id = $1`, rowID); err != nil {
		return fmt.Errorf("pgcatalog: delete row %d: %w", rowID, err)
	}
	return nil
}

func scanFileCatalogRows(rows pgx.Rows) ([]Row, error) {
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(
			&r.RowID, &r.Subsystem, &r.Tenant, &r.ResourceID, &r.ObjectKey, &r.Level,
			&r.MinSec, &r.MaxSec, &r.SizeBytes, &r.CreatedAt, &r.CompactedAt, &r.DeletedAt, &r.Meta,
		); err != nil {
			return nil, fmt.Errorf("pgcatalog: scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
