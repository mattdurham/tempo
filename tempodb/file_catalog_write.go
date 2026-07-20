package tempodb

// file_catalog_write.go — issue #522 #159's direct-write-primary path: markCompacted (this
// file's writeCompactionToFileCatalog) and retention.go's ClearBlock call site
// (markFileCatalogDeleted) mirror their existing filesystem-level action into Postgres's
// file_catalog table, at the moment of that action -- the exact same direct-write-primary +
// reconciliation-secondary pattern Section E already establishes for VI/VCNT/cube (this
// initiative's own filecatalog.Lister ticker stays as the reconciliation-secondary self-heal for
// this table, closing any crash-window gap between the direct write and the next poll).
//
// Gated on rw.pgPool != nil (Postgres not configured -- the same nil-means-disabled convention
// used everywhere else in this plan) AND rw.cfg.Block.Version == "vblockpack" -- Block.Version
// is a single deployment-wide config (tempodb.go:518's own encoding.FromVersionForWrites call),
// not a per-tenant runtime choice anywhere in this codebase, so this is a deployment-level gate,
// not a per-tenant one. vparquet/standard deployments are completely unaffected: this file's
// functions are silent no-ops for them.
//
// The filesystem mechanism is NOT replaced by either function -- both run strictly IN ADDITION
// to the existing MarkBlockCompacted/ClearBlock calls, never instead of them. A write failure
// here is logged, never returned/blocking: this table is a visibility/query-optimization layer
// on top of the filesystem's own already-durable state, not the source of truth for whether a
// block is actually compacted/deleted.

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
)

const vblockpackBlockVersion = "vblockpack"

// fileCatalogWriteEnabled reports whether rw should mirror compaction/retention state into
// file_catalog -- both gates (Postgres configured, deployment-wide vblockpack encoding) must
// hold.
func (rw *readerWriter) fileCatalogWriteEnabled() bool {
	return rw.pgPool != nil && rw.cfg.Block.Version == vblockpackBlockVersion
}

const insertFileCatalogRowSQL = `
	INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, size_bytes, compaction_level)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (tenant, block_id) DO UPDATE SET deleted_at = NULL, compaction_level = $7`

const markFileCatalogCompactedSQL = `
	UPDATE file_catalog SET compacted_at = now() WHERE tenant = $1 AND block_id = $2 AND compacted_at IS NULL`

// writeCompactionToFileCatalog mirrors one markCompacted call: inserts a live row (with its
// real CompactionLevel, closing #158's "nothing ever writes a real level" gap for this call
// path -- filecatalog.Lister's own reconciliation upsert gets the identical fix separately,
// since blocks can also become visible to file_catalog via that path alone, e.g. a crash
// between this write and its own commit) for each new output block (block_ref matches
// vblockpack's own blockObjectKey format exactly, see filecatalog.Lister.reconcileTenant's
// identical literal), then sets compacted_at on each old input block's row -- in that order,
// matching Section E's "insert output, then mark inputs compacted" sequencing.
func (rw *readerWriter) writeCompactionToFileCatalog(ctx context.Context, tenantID string, oldBlocks, newBlocks []*backend.BlockMeta) {
	if !rw.fileCatalogWriteEnabled() {
		return
	}
	for _, m := range newBlocks {
		id := m.BlockID.String()
		blockRef := tenantID + "/" + id + "/data.blockpack"
		if _, err := rw.pgPool.Exec(ctx, insertFileCatalogRowSQL,
			tenantID, id, blockRef, m.StartTime.Unix(), m.EndTime.Unix(), int64(m.Size_), int(m.CompactionLevel), //nolint:gosec // block sizes never approach int64 overflow
		); err != nil {
			level.Warn(log.Logger).Log("msg", "failed to insert file_catalog row for compaction output", "tenant", tenantID, "block", id, "err", err)
		}
	}
	for _, m := range oldBlocks {
		id := m.BlockID.String()
		if _, err := rw.pgPool.Exec(ctx, markFileCatalogCompactedSQL, tenantID, id); err != nil {
			level.Warn(log.Logger).Log("msg", "failed to mark file_catalog row compacted", "tenant", tenantID, "block", id, "err", err)
		}
	}
}

const markFileCatalogDeletedSQL = `UPDATE file_catalog SET deleted_at = now() WHERE tenant = $1 AND block_id = $2 AND deleted_at IS NULL`

// markFileCatalogDeleted mirrors one retention.go ClearBlock call: sets deleted_at on the
// physically-deleted block's row, mirroring filecatalog.Lister's own soft-delete semantics for
// this column (never a hard delete -- the row stays for audit/visibility, exactly like every
// other deleted_at usage in this table).
func (rw *readerWriter) markFileCatalogDeleted(ctx context.Context, tenantID string, blockID backend.UUID) {
	if !rw.fileCatalogWriteEnabled() {
		return
	}
	if _, err := rw.pgPool.Exec(ctx, markFileCatalogDeletedSQL, tenantID, blockID.String()); err != nil {
		level.Warn(log.Logger).Log("msg", "failed to mark file_catalog row deleted", "tenant", tenantID, "block", blockID.String(), "err", err)
	}
}
