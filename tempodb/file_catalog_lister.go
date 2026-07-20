package tempodb

// file_catalog_lister.go — issue #522 #159's Postgres-backed blocklist.FileCatalogBlockLister
// implementation: lists a tenant's currently-live/compacted block IDs from file_catalog instead
// of a real backend LIST call. deleted_at IS NULL means "still physically present" (mirrors
// filecatalog.Lister's own "vanished from filesystem" semantic for that column) -- a row with
// deleted_at set is excluded entirely, matching reader.Blocks' own contract of only ever
// returning IDs for objects that still physically exist. compacted_at IS NOT NULL distinguishes
// compacted-but-not-yet-reaped rows (still physically present during the reaper's grace window)
// from live ones.

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/tempodb/blocklist"
)

// fileCatalogBlockLister implements blocklist.FileCatalogBlockLister against a real Postgres
// pool.
type fileCatalogBlockLister struct {
	pool *pgxpool.Pool
}

var _ blocklist.FileCatalogBlockLister = (*fileCatalogBlockLister)(nil)

func newFileCatalogBlockLister(pool *pgxpool.Pool) *fileCatalogBlockLister {
	return &fileCatalogBlockLister{pool: pool}
}

const listFileCatalogBlockIDsSQL = `
	SELECT block_id, compacted_at IS NOT NULL AS is_compacted
	FROM file_catalog
	WHERE tenant = $1 AND deleted_at IS NULL`

func (l *fileCatalogBlockLister) ListBlockIDs(ctx context.Context, tenantID string) (live []uuid.UUID, compacted []uuid.UUID, err error) {
	rows, err := l.pool.Query(ctx, listFileCatalogBlockIDsSQL, tenantID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var blockIDStr string
		var isCompacted bool
		if err := rows.Scan(&blockIDStr, &isCompacted); err != nil {
			return nil, nil, err
		}
		id, err := uuid.Parse(blockIDStr)
		if err != nil {
			// A malformed block_id would be an integrity bug elsewhere (jobstore/lister only
			// ever write real uuid.String() values) -- skip rather than fail the whole poll for
			// one bad row, mirroring pollBlock's own "not necessarily an error, just bail out"
			// posture for individual-block anomalies.
			continue
		}
		if isCompacted {
			compacted = append(compacted, id)
		} else {
			live = append(live, id)
		}
	}
	if rows.Err() != nil {
		return nil, nil, rows.Err()
	}
	return live, compacted, nil
}
