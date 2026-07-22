package tempodb

// pg_poller.go — issue #522/#525: Postgres is the sole source of truth for which vblockpack
// blocks exist in S3. This replaces the classic S3 bucket-index poller's data source for
// vblockpack blocks: instead of a raw bucket LIST or a written tenant-index blob (both can go
// stale relative to blockpack's own compaction-worker deleting compacted blocks via
// catalog_reap, causing exactly the "the specified key does not exist" query failures this
// issue was opened for), live/compacted block lists are sourced directly from
// blockpack_file_catalog.
//
// Produces the exact same (blocklist.PerTenant, blocklist.PerTenantCompacted, error) shape
// the classic Poller.Do returns, so rw.blocklist.ApplyPollResults and every downstream
// consumer (search/metrics/tag sharding via BlockMetas(), trace-by-id Find, retention,
// redaction) need zero changes -- they already read from the SAME rw.blocklist this poller
// populates, via the SAME poll cycle (rw.pollBlocklist), just with rw.pg != nil selecting this
// source instead of rw.blocklistPoller.Do.

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/google/uuid"

	blockpack "github.com/grafana/blockpack"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/blocklist"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

// pollBlocklistFromPostgres is the Postgres-sourced equivalent of blocklist.Poller.Do, scoped
// to subsystem="trace" (vblockpack blocks only). A tenant with zero trace rows in Postgres
// simply gets no entry, never an error -- this deployment's tenants are 100% vblockpack in
// practice (team-lead ruling: no tenant ever mixes encodings), but nothing here assumes that.
func (rw *readerWriter) pollBlocklistFromPostgres(ctx context.Context) (blocklist.PerTenant, blocklist.PerTenantCompacted, error) {
	store := rw.pg.FileCatalogStore()

	tenants, err := store.ListLiveTenants(ctx, "trace")
	if err != nil {
		return nil, nil, fmt.Errorf("pg_poller: list live tenants: %w", err)
	}

	live := blocklist.PerTenant{}
	compacted := blocklist.PerTenantCompacted{}

	for _, tenantID := range tenants {
		liveRows, listErr := store.ListLiveKeys(ctx, "trace", tenantID)
		if listErr != nil {
			level.Error(rw.logger).Log("msg", "pg_poller: list live keys failed", "tenant", tenantID, "err", listErr)
			continue
		}
		if liveMetas := rw.traceBlockMetasFromRows(ctx, tenantID, liveRows); len(liveMetas) > 0 {
			live[tenantID] = liveMetas
		}

		compactedRows, listErr := store.ListCompactedNotDeleted(ctx, "trace", tenantID)
		if listErr != nil {
			level.Error(rw.logger).Log("msg", "pg_poller: list compacted-not-deleted failed", "tenant", tenantID, "err", listErr)
			continue
		}
		compactedMetas := make([]*backend.CompactedBlockMeta, 0, len(compactedRows))
		for _, row := range compactedRows {
			meta, convErr := rw.traceBlockMetaFromRow(ctx, row)
			if convErr != nil {
				level.Warn(rw.logger).Log(
					"msg", "pg_poller: skipping compacted block, could not build BlockMeta",
					"tenant", tenantID, "objectKey", row.ObjectKey, "err", convErr,
				)
				continue
			}
			compactedTime := row.CreatedAt
			if row.CompactedAt != nil {
				compactedTime = *row.CompactedAt
			}
			compactedMetas = append(compactedMetas, &backend.CompactedBlockMeta{
				BlockMeta:     *meta,
				CompactedTime: compactedTime,
			})
		}
		if len(compactedMetas) > 0 {
			compacted[tenantID] = compactedMetas
		}
	}

	return live, compacted, nil
}

// markBlockCompactedForRetention is retention's aging-out action: when rw.pg == nil, it's the
// classic backend.Compactor.MarkBlockCompacted (renames meta.json -> meta.compacted.json). When
// rw.pg != nil, Postgres is the sole source of truth for vblockpack block existence (issue
// #522/#525) -- nothing reads meta.json for these blocks, so the classic rename would be a
// no-op nobody observes. Instead this sets compacted_at directly on the block's
// blockpack_file_catalog row; from there, blockpack's own compaction-planner/catalog_reap
// pipeline (ListCompactedOlderThan, subsystem-agnostic) takes over physical deletion on its own
// schedule -- see the ClearBlock call site in retention.go, which defers to that pipeline
// instead of racing a second deleter against it.
func (rw *readerWriter) markBlockCompactedForRetention(ctx context.Context, tenantID string, blockID backend.UUID) error {
	if rw.pg != nil {
		return rw.pg.FileCatalogStore().MarkCompacted(ctx, []string{traceObjectKey(tenantID, blockID)})
	}
	return rw.c.MarkBlockCompacted(uuid.UUID(blockID), tenantID)
}

func (rw *readerWriter) traceBlockMetasFromRows(ctx context.Context, tenantID string, rows []blockpack.FileCatalogRow) []*backend.BlockMeta {
	metas := make([]*backend.BlockMeta, 0, len(rows))
	for _, row := range rows {
		meta, err := rw.traceBlockMetaFromRow(ctx, row)
		if err != nil {
			level.Warn(rw.logger).Log(
				"msg", "pg_poller: skipping block, could not build BlockMeta",
				"tenant", tenantID, "objectKey", row.ObjectKey, "err", err,
			)
			continue
		}
		metas = append(metas, meta)
	}
	return metas
}

// traceBlockMetaFromRow builds a full backend.BlockMeta from row. When row.Meta is populated
// (every block written since blockpack_file_catalog gained its Meta column, grafana/blockpack
// #525), this needs zero object-storage I/O. row.Meta is nil only for a legacy row written
// before that column existed -- fetches meta.json once as a fallback, mirroring the classic
// poller's own pollBlock behavior; self-healing, since that block is eventually superseded by
// a newer compacted output whose row DOES have Meta.
func (rw *readerWriter) traceBlockMetaFromRow(ctx context.Context, row blockpack.FileCatalogRow) (*backend.BlockMeta, error) {
	blockID, err := blockIDFromTraceObjectKey(row.ObjectKey)
	if err != nil {
		return nil, err
	}

	if row.Meta == nil {
		meta, err := rw.r.BlockMeta(ctx, uuid.UUID(blockID), row.Tenant)
		if err != nil {
			return nil, fmt.Errorf("fetch meta.json for legacy row with no Meta column: %w", err)
		}
		return meta, nil
	}

	var tm blockpack.TraceBlockMeta
	if err := json.Unmarshal(row.Meta, &tm); err != nil {
		return nil, fmt.Errorf("unmarshal Meta: %w", err)
	}

	dedicated := make(backend.DedicatedColumns, 0, len(tm.DedicatedColumns))
	for _, dc := range tm.DedicatedColumns {
		dedicated = append(dedicated, backend.DedicatedColumn{
			Scope: backend.DedicatedColumnScope(dc.Scope),
			Name:  dc.Name,
			Type:  backend.DedicatedColumnType(dc.Type),
		})
	}

	return &backend.BlockMeta{
		BlockID:           blockID,
		TenantID:          row.Tenant,
		Version:           tm.Version,
		StartTime:         time.Unix(row.MinSec, 0),
		EndTime:           time.Unix(row.MaxSec, 0),
		TotalObjects:      tm.TotalObjects,
		Size_:             uint64(row.SizeBytes), //nolint:gosec // G115: block size never approaches int64->uint64 overflow
		CompactionLevel:   uint32(row.Level),     //nolint:gosec // G115: compaction level is a small merge-depth counter
		IndexPageSize:     uint32(tm.IndexPageSize),
		TotalRecords:      uint32(tm.TotalRecords),
		BloomShardCount:   uint32(tm.BloomShardCount),
		FooterSize:        uint32(tm.FooterSize),
		DedicatedColumns:  dedicated,
		ReplicationFactor: tm.ReplicationFactor,
	}, nil
}

// traceObjectKey is the forward counterpart of blockIDFromTraceObjectKey: builds the
// subsystem="trace" object key for (tenantID, blockID), matching block_catalog_notify.go's own
// path.Join("<tenant>", "<blockID>", vblockpack.DataFileName) shape.
func traceObjectKey(tenantID string, blockID backend.UUID) string {
	return tenantID + "/" + uuid.UUID(blockID).String() + "/" + vblockpack.DataFileName
}

// blockIDFromTraceObjectKey parses the block ID out of a subsystem="trace" object key, whose
// path shape is always "<tenant>/<blockID>/data.blockpack" (block_catalog_notify.go's own
// path.Join call, the only writer of these rows).
func blockIDFromTraceObjectKey(objectKey string) (backend.UUID, error) {
	parts := strings.Split(path.Clean(objectKey), "/")
	if len(parts) != 3 {
		return backend.UUID{}, fmt.Errorf("pg_poller: object key %q does not match <tenant>/<blockID>/data.blockpack", objectKey)
	}
	blockID, err := backend.ParseUUID(parts[1])
	if err != nil {
		return backend.UUID{}, fmt.Errorf("pg_poller: parse block ID from object key %q: %w", objectKey, err)
	}
	return blockID, nil
}
