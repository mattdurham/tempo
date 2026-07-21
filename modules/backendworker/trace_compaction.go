package backendworker

// trace_compaction.go — issue #522 #158's processTraceCompactionJobPostgres handler: claims a
// trace_compaction job (job-planner's plan_trace_compaction.go already selected 2 same-window,
// vblockpack-encoded blocks via file_catalog) and executes it through the exact same,
// already-tested merge logic the legacy gRPC CompactionProvider path uses -- w.compact, a thin
// wrapper over store.CompactWithConfig (see processCompactionJob's identical pattern). No new
// merge logic anywhere, only new Postgres dispatch plumbing, mirroring every other Postgres job
// type's shape.
//
// Deliberately does NOT touch file_catalog itself (compaction_level/compacted_at writes are
// #159's job, not this handler's) -- store.CompactWithConfig already updates the in-memory
// blocklist and the physical backend's own meta.json/meta.compacted.json exactly as it always
// has; file_catalog's own visibility into that state update is filecatalog.Lister's existing
// reconciliation ticker until #159 ships the direct-write-primary path.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-kit/log/level"

	"github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

func (w *BackendWorker) processTraceCompactionJobPostgres(ctx context.Context, job *jobstore.Job) error {
	if job.Tenant == "" {
		return fmt.Errorf("trace_compaction job missing tenant")
	}
	var detail jobstore.TraceCompactionDetail
	if err := json.Unmarshal(job.Detail, &detail); err != nil {
		return fmt.Errorf("trace_compaction: unmarshal detail: %w", err)
	}
	if len(detail.InputBlockIDs) != 2 {
		return fmt.Errorf("trace_compaction: expected exactly 2 input block IDs, got %d", len(detail.InputBlockIDs))
	}

	// Collect the real BlockMeta for each input ID from the already-loaded in-memory
	// blocklist -- the exact same lookup processCompactionJob's own gRPC-dispatched handler
	// already performs, since file_catalog's own row shape (tenant, block_id, block_ref,
	// start_sec, end_sec, size_bytes) has nowhere near enough fields to reconstruct a full
	// backend.BlockMeta (Version, Encoding, DedicatedColumns, TotalObjects, ...) -- it is a
	// discovery/candidate index only, never the execution-time source of truth.
	blockMetas := w.store.BlockMetas(job.Tenant)
	var sourceMetas []*backend.BlockMeta
	for _, meta := range blockMetas {
		for _, id := range detail.InputBlockIDs {
			if meta.BlockID.String() == id {
				sourceMetas = append(sourceMetas, meta)
			}
		}
	}
	if len(sourceMetas) < 2 {
		// A planned input already vanished from the live blocklist by the time this job
		// actually ran (already compacted by a race, or cleared by retention/reconciliation)
		// -- not a failure. Blocks disappear independent of this handler's own actions,
		// exactly like blockpack_file_catalog's VI/VCNT/cube inputs do (issue #522,
		// SPEC-COMPACTIONWORKER-7/8 in blockpack) -- file_catalog's own
		// modules/backendscheduler/filecatalog/lister.go already notices and soft-deletes
		// the vanished block's row independently on its own reconciliation tick, so this
		// handler doesn't need to touch file_catalog itself. Any block that IS still live
		// is left completely untouched for job-planner to pair with a different sibling in
		// a future planning pass.
		level.Warn(log.Logger).Log(
			"msg", "trace_compaction job has fewer than 2 live input blocks, skipping",
			"job_id", job.ID, "tenant", job.Tenant,
			"requested", len(detail.InputBlockIDs), "live", len(sourceMetas),
		)
		return nil
	}

	newCompacted, err := w.compact(ctx, sourceMetas, job.Tenant)
	if err != nil {
		return fmt.Errorf("trace_compaction: compacting blocks: %w", err)
	}

	level.Debug(log.Logger).Log(
		"msg", "trace_compaction job completed", "job_id", job.ID, "tenant", job.Tenant,
		"inputs", len(sourceMetas), "outputs", len(newCompacted),
	)
	return nil
}
