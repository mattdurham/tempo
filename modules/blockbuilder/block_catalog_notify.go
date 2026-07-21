package blockbuilder

// block_catalog_notify.go — reports a freshly flushed vblockpack block into
// blockpack_file_catalog (trace/span compaction moved fully into blockpack; block-builder's
// remaining job for this concern is exactly what it already does -- cut the initial block from
// the WAL -- plus this one additional notification so compaction-planner can see it as a fresh
// candidate). Best-effort: a failure here is logged but never fails the flush itself, mirroring
// every other "additive, self-healing" catalog write in this project (compaction-planner's own
// catalog_reconcile pass discovers any block this notification misses on its own, next tick, via
// its existing insert-missing self-heal -- this is a latency optimization, not the only path a
// block can ever be discovered through).
//
// Reuses tempodb.PgPoolProvider (issue #522 #157/#159's own established capability) rather than
// requiring new Config/constructor wiring: w's concrete type already exposes PgPool() when
// cfg.Postgres is configured, exactly like modules/frontend/vcnt_fetch.go's identical
// type-assertion at sharder-construction time.

import (
	"context"
	"path"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

// notifyBlockpackFileCatalog reports meta into blockpack_file_catalog (subsystem="trace") if w
// exposes a configured Postgres pool AND meta is vblockpack-encoded -- vparquet/standard blocks
// are never cataloged here, matching this project's "vparquet stays untouched forever" scoping
// (mirrors filecatalog.Lister.reconcileTenant's identical Version filter).
func notifyBlockpackFileCatalog(ctx context.Context, logger log.Logger, w tempodb.Writer, meta *backend.BlockMeta) {
	if meta.Version != vblockpack.VersionString {
		return
	}
	provider, ok := w.(tempodb.PgPoolProvider)
	if !ok {
		return
	}
	pool := provider.PgPool()
	if pool == nil {
		return
	}

	tenantID := meta.TenantID
	blockID := meta.BlockID.String()
	objectKey := path.Join(tenantID, blockID, vblockpack.DataFileName)

	err := blockpack.NewFileCatalogStore(pool).Insert(ctx, blockpack.FileCatalogRow{
		Subsystem:  "trace",
		Tenant:     tenantID,
		ResourceID: "",
		ObjectKey:  objectKey,
		Level:      int(meta.CompactionLevel), //nolint:gosec // G115: compaction level is a small merge-depth counter
		MinSec:     meta.StartTime.Unix(),
		MaxSec:     meta.EndTime.Unix(),
		SizeBytes:  int64(meta.Size_), //nolint:gosec // G115: block size never approaches int64 overflow
	})
	if err != nil {
		level.Warn(logger).Log(
			"msg", "blockbuilder: notify blockpack_file_catalog failed (non-fatal, compaction-planner's own reconcile will self-heal)",
			"tenant", tenantID, "blockID", blockID, "err", err,
		)
	}
}
