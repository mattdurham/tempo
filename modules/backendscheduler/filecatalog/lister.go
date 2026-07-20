package filecatalog

// lister.go — periodic reconciliation of backend-scheduler's already-live,
// already-maintained per-tenant blocklist snapshot (s.store.BlockMetas) into
// the Postgres file_catalog table (2026-07-11). NOT a new S3 List() call --
// reuses state backend-scheduler already polls for on BlocklistPoll's
// existing cadence; this loop just reconciles that snapshot against Postgres
// on its OWN, faster tick (default 5m, see Config.CatalogListInterval),
// independent of the (default 5m) BlocklistPoll interval it happens to
// currently match.
//
// Soft-delete reconciliation: any file_catalog row (for a tenant this pass
// covers) whose block_id is NOT present in the current BlockMetas snapshot
// gets deleted_at set. Since BlockMetas already excludes blocks a worker has
// reported as compacted/retained (applyJobsToBlocklist), this closes the
// "backfill's catalog query hands back a since-deleted block" race within one
// lister-tick's latency -- bounded by ListInterval, with
// CompactedBlockRetention's default 1h (tempodb/config.go's own
// CompactorConfig default) as the safety margin between "worker reports a
// block cleared" and "the object is actually gone from storage." An operator
// narrowing CompactedBlockRetention below roughly 2-3x ListInterval should be
// flagged as a real, load-bearing operational invariant this design relies
// on.
//
// Deliberately does NOT hook tempodb/retention.go's MarkBlockCompacted/
// ClearBlock directly: hooking retention.go would require tempodb's own
// retention path (used by EVERY deployment, not just Postgres-opted-in ones)
// to gain a Postgres dependency, violating the "opt-in additional backend,
// zero risk to existing paths" principle this whole file_catalog feature is
// built on. This periodic-reconciliation approach keeps 100% of the
// Postgres/catalog logic inside this package and backendscheduler.go's own
// already-conditional new ticker branch.

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
)

const upsertLiveBlocksSQL = `
	INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, size_bytes, compaction_level, deleted_at)
	SELECT $1, t.block_id, t.block_ref, t.start_sec, t.end_sec, t.size_bytes, t.compaction_level, NULL
	FROM unnest($2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::int[])
		AS t(block_id, block_ref, start_sec, end_sec, size_bytes, compaction_level)
	ON CONFLICT (tenant, block_id) DO UPDATE SET deleted_at = NULL, compaction_level = EXCLUDED.compaction_level`

const softDeleteVanishedBlocksSQL = `
	UPDATE file_catalog
	SET deleted_at = now()
	WHERE tenant = $1 AND deleted_at IS NULL AND block_id != ALL($2::text[])`

// Lister periodically reconciles backend-scheduler's in-memory blocklist
// (blockMetas) into the Postgres file_catalog table. blockMetas and tenants
// are injected functions (not a direct storage.Store dependency) so this
// package is unit-testable against fakes without a real backend --
// BackendScheduler passes s.store.BlockMetas/s.store.Tenants directly.
type Lister struct {
	pool       *pgxpool.Pool
	blockMetas func(tenant string) []*backend.BlockMeta
	tenants    func() []string
}

// NewLister constructs a Lister. pool must be non-nil (callers gate
// construction on cfg.Postgres != nil, mirroring every other nil-means-
// disabled convention in this plan).
func NewLister(pool *pgxpool.Pool, blockMetas func(tenant string) []*backend.BlockMeta, tenants func() []string) *Lister {
	return &Lister{pool: pool, blockMetas: blockMetas, tenants: tenants}
}

// Close closes l's underlying Postgres pool. Nil-safe (mirrors this plan's
// other nil-tolerant Postgres conventions) -- the caller (BackendScheduler.
// stopping) must call this on shutdown so the pool never leaks past process
// restart, mirroring tempodb.go's readerWriter.Shutdown() closing its own
// pgPool identically.
func (l *Lister) Close() {
	if l == nil || l.pool == nil {
		return
	}
	l.pool.Close()
}

// RunOnce performs one reconciliation pass over every tenant currently known
// to l.tenants(). For each tenant: upserts every currently-live block
// (clearing deleted_at if it was previously soft-deleted and has
// reappeared), then soft-deletes any previously-live row whose block_id is
// no longer in the current snapshot.
func (l *Lister) RunOnce(ctx context.Context) error {
	for _, tenant := range l.tenants() {
		if err := l.reconcileTenant(ctx, tenant); err != nil {
			return fmt.Errorf("filecatalog: reconcile tenant %s: %w", tenant, err)
		}
	}
	return nil
}

// vblockpackVersion is the exact backend.BlockMeta.Version string vblockpack-encoded blocks
// carry -- mirrors tempodb/blockselector/compaction_block_selector.go's own
// `meta.Version == "vblockpack"` check, the established convention for this comparison
// elsewhere in the codebase.
const vblockpackVersion = "vblockpack"

func (l *Lister) reconcileTenant(ctx context.Context, tenant string) error {
	allMetas := l.blockMetas(tenant)

	// Filter to vblockpack-encoded blocks only (issue #522 #158 fix): file_catalog's
	// block_ref column is hardcoded below to the vblockpack ".../data.blockpack" object-key
	// shape -- reconciling a vparquet/standard block here would fabricate a wrong block_ref
	// for it (that block's real object key never matches). This also gives file_catalog a
	// reliable "vblockpack-encoded tenants only" scoping property BY CONSTRUCTION, with no
	// new schema column needed: #158's job-planner candidate query (plan_trace_compaction.go)
	// depends on file_catalog never containing a non-vblockpack row at all, per plan.md Phase
	// 4c's "vparquet/standard tenants' compaction is completely untouched, forever" scoping.
	metas := make([]*backend.BlockMeta, 0, len(allMetas))
	var nonVblockpackCount int
	for _, m := range allMetas {
		if m.Version == vblockpackVersion {
			metas = append(metas, m)
			continue
		}
		nonVblockpackCount++
	}
	// Cheap safety-net sanity check (issue #522 #159): the poller flip's deployment-level gate
	// assumes no tenant ever mixes vblockpack and non-vblockpack blocks -- confirmed true for
	// this deployment, but a future violation of that assumption would otherwise silently make
	// these blocks invisible once the poller stops calling the real backend LIST for this
	// tenant. Zero extra I/O: allMetas is already fetched above for the filter itself.
	if nonVblockpackCount > 0 {
		metricUnexpectedNonVblockpackBlocks.WithLabelValues(tenant).Add(float64(nonVblockpackCount))
		level.Warn(log.Logger).Log(
			"msg", "filecatalog: tenant has non-vblockpack-encoded blocks that will never be reconciled into file_catalog -- "+
				"if this tenant's poller has flipped to file_catalog-primary, these blocks may be invisible to it",
			"tenant", tenant, "count", nonVblockpackCount,
		)
	}

	blockIDs := make([]string, len(metas))
	blockRefs := make([]string, len(metas))
	startSecs := make([]int64, len(metas))
	endSecs := make([]int64, len(metas))
	sizeBytes := make([]int64, len(metas))
	compactionLevels := make([]int32, len(metas))
	for i, m := range metas {
		id := m.BlockID.String()
		blockIDs[i] = id
		// "<tenant>/<block-id>/data.blockpack" -- matches vblockpack's own
		// blockObjectKey format exactly (verified against
		// tempodb/encoding/vblockpack/blockevents.go), duplicated as a literal
		// here rather than imported: modules/backendscheduler is a lower-level
		// module than tempodb/encoding/vblockpack, and this package's own test
		// infra already accepts a small, deliberate duplication across that
		// module boundary rather than introducing a cross-layer dependency.
		// Safe now that metas is filtered to vblockpack-encoded blocks only.
		blockRefs[i] = tenant + "/" + id + "/data.blockpack"
		startSecs[i] = m.StartTime.Unix()
		endSecs[i] = m.EndTime.Unix()
		sizeBytes[i] = int64(m.Size_) //nolint:gosec // block sizes never approach int64 overflow
		// compaction_level (issue #522 #159): the real merge-depth, closing #158's "nothing
		// ever writes a real level" gap for the reconciliation path specifically (compaction's
		// own direct write, tempodb/file_catalog_write.go, closes it for the compaction-time
		// path; this covers every OTHER way a block becomes visible here, e.g. this Lister's
		// own periodic catch-up reconciliation for a block compaction never directly wrote).
		compactionLevels[i] = int32(m.CompactionLevel) //nolint:gosec // compaction levels never approach int32 overflow
	}

	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if len(metas) > 0 {
		if _, err := tx.Exec(ctx, upsertLiveBlocksSQL, tenant, blockIDs, blockRefs, startSecs, endSecs, sizeBytes, compactionLevels); err != nil {
			return fmt.Errorf("upsert live blocks: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, softDeleteVanishedBlocksSQL, tenant, blockIDs); err != nil {
		return fmt.Errorf("soft-delete vanished blocks: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Run ticks RunOnce every interval until ctx is done, mirroring
// tempodb/retention.go's retentionLoop shape exactly for consistency with
// this codebase's existing convention.
func (l *Lister) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-ticker.C:
			if err := l.RunOnce(ctx); err != nil {
				level.Warn(log.Logger).Log("msg", "filecatalog: lister run failed", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
