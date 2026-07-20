package backendscheduler

// redaction_state.go — issue #522 #152/Phase 4c's redaction-pending mirror: direct writes into
// Postgres's tenant_redaction_state table (schema added by #143), populated at the exact moment
// a redaction batch starts (SubmitRedaction) and finishes (cleanupBatchIfDone) -- the SAME
// direct-write-primary pattern Section E already establishes for VI/VCNT/cube compaction and
// jobstore.go's own raw-SQL style. work.Interface's in-memory TenantPending (backed by
// batchStore.hasActive) stays the fast-path check the existing gRPC dispatch already uses; this
// table is the new, additional Postgres-visible mirror of the same state, consulted by
// job-planner's trace-compaction candidate query (plan_trace_compaction.go, #158) instead --
// job-planner has no access to backend-scheduler's own in-memory work.Interface across the
// process boundary.
//
// s.pgPool is nil when cfg.Postgres is nil (the same nil-means-disabled convention used
// everywhere else in this file) -- both functions below are silent no-ops in that case, mirrors
// catalogLister's own "Postgres not configured, feature simply doesn't run" posture. A write
// failure here is logged, never returned as an error: it must never block the actual redaction
// batch submission/cleanup (which is already durably tracked in-memory/on local disk regardless
// of this mirror's success) -- this table is a visibility aid for job-planner, not the source of
// truth for redaction state itself.

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/grafana/tempo/pkg/util/log"
)

const upsertRedactionPendingSQL = `
	INSERT INTO tenant_redaction_state (tenant, pending, batch_id, started_at, updated_at)
	VALUES ($1, TRUE, $2, now(), now())
	ON CONFLICT (tenant) DO UPDATE SET pending = TRUE, batch_id = $2, started_at = now(), updated_at = now()`

const clearRedactionPendingSQL = `
	INSERT INTO tenant_redaction_state (tenant, pending, batch_id, started_at, updated_at)
	VALUES ($1, FALSE, NULL, NULL, now())
	ON CONFLICT (tenant) DO UPDATE SET pending = FALSE, batch_id = NULL, started_at = NULL, updated_at = now()`

// markTenantRedactionPending mirrors a just-started redaction batch into tenant_redaction_state,
// called once AddBatch/AddPendingJobs have both already succeeded in SubmitRedaction (so this
// mirror never needs its own Postgres-side rollback on the existing in-memory rollback path).
func (s *BackendScheduler) markTenantRedactionPending(ctx context.Context, tenantID, batchID string) {
	if s.pgPool == nil {
		return
	}
	if _, err := s.pgPool.Exec(ctx, upsertRedactionPendingSQL, tenantID, batchID); err != nil {
		level.Warn(log.Logger).Log(
			"msg", "failed to mirror redaction-pending state into Postgres", "tenant", tenantID, "batch_id", batchID, "err", err,
		)
	}
}

// clearTenantRedactionPending mirrors a just-completed redaction batch's cleanup into
// tenant_redaction_state, called once RemoveBatch has already succeeded in cleanupBatchIfDone.
func (s *BackendScheduler) clearTenantRedactionPending(ctx context.Context, tenantID string) {
	if s.pgPool == nil {
		return
	}
	if _, err := s.pgPool.Exec(ctx, clearRedactionPendingSQL, tenantID); err != nil {
		level.Warn(log.Logger).Log(
			"msg", "failed to clear redaction-pending state in Postgres", "tenant", tenantID, "err", err,
		)
	}
}
