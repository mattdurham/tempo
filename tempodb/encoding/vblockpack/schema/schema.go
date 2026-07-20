// Package schema embeds this directory's own .sql files so they can be applied against a real
// Postgres instance from production code, not just from test infra's os.ReadFile-based
// applySchema helpers (which only work when the test binary's working directory happens to be
// this package's own directory). Sibling to ../migrate, which embeds backend_jobs.sql the same
// way -- kept separate because these files live in a different directory tree, and go:embed
// cannot reach outside its own package's directory.
package schema

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

//go:embed file_catalog.sql
var fileCatalogSchema string

// ApplyFileCatalog applies file_catalog.sql -- including tenant_redaction_state and the
// compaction_level/compacted_at columns added by issue #522 #143 -- against pool. Idempotent
// (every statement uses IF NOT EXISTS/ADD COLUMN IF NOT EXISTS), the same "safe to call on every
// startup" convention migrate.Apply already established for backend_jobs.sql.
func ApplyFileCatalog(ctx context.Context, pool *pgxpool.Pool) error {
	if err := migrate.ApplyStatements(ctx, pool, fileCatalogSchema); err != nil {
		return fmt.Errorf("apply file_catalog schema: %w", err)
	}
	return nil
}
