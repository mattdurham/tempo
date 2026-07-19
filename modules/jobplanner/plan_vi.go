package jobplanner

// plan_vi.go — issue #518: polls viusage_entries for columns that have
// already been triggered at least once (decision (c): job-planner never
// scans for never-queried columns) and have not yet reached full historical
// coverage, chain-enqueuing the next bounded-window vi_backfill job for each.
//
// backfill_in_progress=FALSE in the WHERE clause is a cheap candidate-set
// reduction only, NOT the correctness guard against double-enqueuing a column
// with a genuinely in-flight job -- InsertViBackfill's dedup_key partial
// unique index on backend_jobs is what actually prevents that (a stale
// backfill_in_progress=true row with no real in-flight job must not
// permanently block chaining). "No more history left" is determined entirely
// by the VI engine's own done flag; job-planner does no separate
// retention-boundary computation for VI.

import (
	"context"
	"fmt"

	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// viBackfillInserter is the minimal seam planViRow needs -- lets
// TestPlanViRow_* exercise the query-row-to-Insert-call mapping against a
// fake, without a real Postgres connection. *jobstore.Store satisfies this in
// production.
type viBackfillInserter interface {
	InsertViBackfill(ctx context.Context, tenant string, d jobstore.ViBackfillDetail) error
}

// viUsageRow is one candidate row from viUsagePlanQuery.
type viUsageRow struct {
	Tenant     string
	ColHash    string
	ColType    string
	ColumnName string
}

// viUsagePlanQuery finds columns already triggered at least once with more
// history left to backfill (schema: vendor/github.com/grafana/blockpack/
// internal/modules/viusage/schema.sql).
//
// backfill_in_progress = FALSE OR lease_expires_at < now(): a genuinely
// in-flight job's lease is in the future, so it stays excluded; a
// crashed/killed worker that never got to call UpdateWatermark again leaves
// backfill_in_progress=true with a lease that eventually expires -- without
// the OR clause, that column would be permanently invisible to job-planner
// (nothing else ever clears the flag except a real UpdateWatermark call),
// defeating the whole point of a periodic re-checker. Mirrors
// viusage/trigger.go's own staleness check (BackfillInProgress &&
// LeaseExpiresAt > nowSec) at the reactive-trigger call site.
const viUsagePlanQuery = `
	SELECT tenant, col_hash, col_type, column_name
	FROM viusage_entries
	WHERE triggered = TRUE
	  AND done = FALSE
	  AND (backfill_in_progress = FALSE OR lease_expires_at < extract(epoch FROM now())::bigint)`

// planViRow enqueues the next bounded-window vi_backfill job for row.
func planViRow(ctx context.Context, inserter viBackfillInserter, cfg common.JobPlannerConfig, row viUsageRow) error {
	return inserter.InsertViBackfill(ctx, row.Tenant, jobstore.ViBackfillDetail{
		ColumnHash:    row.ColHash,
		ColumnName:    row.ColumnName,
		ColumnType:    row.ColType,
		WindowSeconds: cfg.ViWindowSeconds,
	})
}

// planVi runs viUsagePlanQuery against the real database and chain-enqueues a
// job per candidate row. A single row's scan/insert failure is recorded but
// does not stop the remaining rows in this tick from being planned.
func (s *Service) planVi(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, viUsagePlanQuery)
	if err != nil {
		return fmt.Errorf("jobplanner: query viusage_entries: %w", err)
	}
	defer rows.Close()

	var firstErr error
	for rows.Next() {
		var row viUsageRow
		if scanErr := rows.Scan(&row.Tenant, &row.ColHash, &row.ColType, &row.ColumnName); scanErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("jobplanner: scan viusage_entries row: %w", scanErr)
			}
			continue
		}
		if insErr := planViRow(ctx, s.jobStore, s.cfg, row); insErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("jobplanner: insert vi_backfill for tenant %q column %q: %w", row.Tenant, row.ColumnName, insErr)
			}
			continue
		}
		metricColumnsPlanned.Inc()
	}
	if rows.Err() != nil {
		return fmt.Errorf("jobplanner: iterate viusage_entries: %w", rows.Err())
	}
	return firstErr
}
