package blockpack

// file_catalog.go — public glue for internal/modules/pgcatalog's
// blockpack_file_catalog Store (issue #522). First-ever root re-export for
// pgcatalog: unlike cube/viusage/colhashmanifest (each with a pre-existing
// blob-backed implementation whose interface already needed a Postgres
// constructor at root), pgcatalog is brand-new, Postgres-only infrastructure
// with no interface to satisfy -- Store/Row are re-exported directly as type
// aliases, matching api.go's/timeslice.go's existing `type X = pkg.X` root
// re-export convention for plain data/behavior types with no dual-backend
// need. This IS genuinely new root public API surface — flagged explicitly
// per CLAUDE.md's "no new public API surface without explicit user
// permission": issue #522's own design (job-planner/backend-worker, in the
// tempo repo, must construct and query this catalog directly) is the
// permission granted here, mirroring column_manifest.go's identical
// reasoning for issue #506.

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/modules/pgcatalog"
)

// FileCatalogRow is one blockpack_file_catalog row.
type FileCatalogRow = pgcatalog.Row

// FileCatalogStore is the Postgres-backed blockpack_file_catalog store.
type FileCatalogStore = pgcatalog.Store

// NewFileCatalogStore constructs a FileCatalogStore over pool.
func NewFileCatalogStore(pool *pgxpool.Pool) *FileCatalogStore {
	return pgcatalog.NewStore(pool)
}

// ApplyFileCatalogSchema applies blockpack_file_catalog's schema against
// pool. Exported, never called automatically by any constructor — the
// embedding application calls it once at its own startup.
func ApplyFileCatalogSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgcatalog.ApplyFileCatalogSchema(ctx, pool)
}
