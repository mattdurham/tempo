package jobplanner

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestGroupTraceCompactionPairs_GroupsByTenantAndWindowOnly proves the pure grouping function
// pairs same-tenant, same-window candidates regardless of compaction_level (brainstormer-522's
// confirmed guidance -- level isn't part of the grouping key), and that a group with only 1 row
// produces no pair.
func TestGroupTraceCompactionPairs_GroupsByTenantAndWindowOnly(t *testing.T) {
	windowSecs := traceCompactionWindowSeconds
	candidates := []traceCompactionCandidateRow{
		{tenant: "tenant-a", blockID: "b1", startSec: 100, sizeBytes: 1024},
		{tenant: "tenant-a", blockID: "b2", startSec: 200, sizeBytes: 1024},
		// Different tenant, same window -- must not be paired with tenant-a's rows.
		{tenant: "tenant-b", blockID: "b3", startSec: 100, sizeBytes: 1024},
		// Different window (>= windowSecs later) -- must not be paired with the first group.
		{tenant: "tenant-a", blockID: "b4", startSec: windowSecs + 100, sizeBytes: 1024},
	}

	pairs := groupTraceCompactionPairs(candidates)
	require.Len(t, pairs, 1, "only tenant-a's first window has >= 2 candidates")
	require.Equal(t, "tenant-a", pairs[0].tenant)
	require.ElementsMatch(t, []string{"b1", "b2"}, []string{pairs[0].blockIDA, pairs[0].blockIDB})
}

// TestGroupTraceCompactionPairs_MoreThanTwoInGroup_OnlyFirstTwoPaired proves a group with 3+
// candidates still emits exactly one pair, for the first 2 (by the query's own ORDER BY) --
// pairwise-only (#151), the leftover candidate waits for the next tick.
func TestGroupTraceCompactionPairs_MoreThanTwoInGroup_OnlyFirstTwoPaired(t *testing.T) {
	candidates := []traceCompactionCandidateRow{
		{tenant: "tenant-a", blockID: "b1", startSec: 100, sizeBytes: 1024},
		{tenant: "tenant-a", blockID: "b2", startSec: 110, sizeBytes: 1024},
		{tenant: "tenant-a", blockID: "b3", startSec: 120, sizeBytes: 1024},
	}

	pairs := groupTraceCompactionPairs(candidates)
	require.Len(t, pairs, 1)
	require.Equal(t, "b1", pairs[0].blockIDA)
	require.Equal(t, "b2", pairs[0].blockIDB)
}

// TestGroupTraceCompactionPairs_EmptyInput_NoPairs is the trivial base case.
func TestGroupTraceCompactionPairs_EmptyInput_NoPairs(t *testing.T) {
	require.Empty(t, groupTraceCompactionPairs(nil))
}

// TestPlanTraceCompaction_SeedsTwoRowsInSameWindow_OneJobCreated is the real-Postgres proof that
// planTraceCompaction's own SQL is correct against the real file_catalog schema, mirroring
// jobplanner_e2e_test.go's rationale for planVi/planCube's own real-SQL tests.
func TestPlanTraceCompaction_SeedsTwoRowsInSameWindow_OneJobCreated(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	tenant := "trace-compaction-tenant"
	blockA, blockB := uuid.New().String(), uuid.New().String()
	_, err := pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, size_bytes)
		VALUES ($1, $2, $2, 100, 100, 1024), ($1, $3, $3, 110, 110, 1024)`,
		tenant, blockA, blockB,
	)
	require.NoError(t, err)

	s := New(pool, common.JobPlannerConfig{})
	require.NoError(t, s.planTraceCompaction(ctx))

	var count int
	require.NoError(
		t, pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = $1 AND tenant = $2`, "trace_compaction", tenant).Scan(&count),
	)
	require.Equal(t, 1, count, "exactly one trace_compaction job must be enqueued for the 2-row window")
}

// TestPlanTraceCompaction_ExcludesRowsAtOrAboveGlobalSizeCutoff mirrors VI's own equivalent
// test: a row at or above the cutoff is invisible to planTraceCompaction even with a real
// sibling row otherwise eligible.
func TestPlanTraceCompaction_ExcludesRowsAtOrAboveGlobalSizeCutoff(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	tenant := "trace-compaction-tenant-2"
	smallID, hugeID := uuid.New().String(), uuid.New().String()
	_, err := pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, size_bytes)
		VALUES ($1, $2, $2, 100, 100, 1024), ($1, $3, $3, 110, 110, $4)`,
		tenant, smallID, hugeID, traceCompactionGlobalSizeCutoffBytes,
	)
	require.NoError(t, err)

	s := New(pool, common.JobPlannerConfig{})
	require.NoError(t, s.planTraceCompaction(ctx))

	var count int
	require.NoError(
		t, pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = $1 AND tenant = $2`, "trace_compaction", tenant).Scan(&count),
	)
	require.Equal(t, 0, count, "a lone under-cutoff row with no other eligible sibling must not form a job")
}

// TestPlanTraceCompaction_ExcludesTenantsWithPendingRedaction is #158's required TDD gate: a
// tenant with an active tenant_redaction_state.pending=TRUE row must never have its blocks
// selected as compaction candidates, even with 2 otherwise fully-eligible rows -- closes the
// redaction/compaction race (issue #522 #152/#158).
func TestPlanTraceCompaction_ExcludesTenantsWithPendingRedaction(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	tenant := "redaction-pending-tenant"
	blockA, blockB := uuid.New().String(), uuid.New().String()
	_, err := pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, size_bytes)
		VALUES ($1, $2, $2, 100, 100, 1024), ($1, $3, $3, 110, 110, 1024)`,
		tenant, blockA, blockB,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO tenant_redaction_state (tenant, pending, batch_id, started_at)
		VALUES ($1, TRUE, 'batch-1', now())`,
		tenant,
	)
	require.NoError(t, err)

	s := New(pool, common.JobPlannerConfig{})
	require.NoError(t, s.planTraceCompaction(ctx))

	var count int
	require.NoError(
		t, pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = $1 AND tenant = $2`, "trace_compaction", tenant).Scan(&count),
	)
	require.Equal(t, 0, count, "a tenant with pending=TRUE must never get a trace_compaction job, even with otherwise-eligible blocks")
}
