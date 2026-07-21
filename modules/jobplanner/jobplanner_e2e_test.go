package jobplanner

// jobplanner_e2e_test.go — real-Postgres proof that planVi/planCube's own SQL
// (viUsagePlanQuery/cubeEntriesPlanQuery) is correct against the real
// viusage_entries/cube_entries schemas -- the fake-based tests in
// plan_vi_test.go/plan_cube_test.go only prove the row-to-Insert-call mapping,
// not that the WHERE clauses actually select/reject the right rows. The
// heavier "chained jobs make genuine backward progress across a real worker
// claim+execute cycle" proof (issue #518's actual regression target) is
// task #135's scope, not this file's.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// TestJobPlanner_PollOnce_ChainsViBackfillWhenNotDone simulates the realistic
// "idle, more work to do" state job-planner is meant to rescue: triggered,
// not done, and its backfill lease has genuinely expired (a crashed/killed
// worker that never got to call UpdateWatermark again -- the ONLY way
// backfill_in_progress can go stale without ever being cleared). Moves
// lease_expires_at into the past directly via SQL, mirroring this project's
// established pattern for testing lease/retry expiry without a real
// wall-clock wait (e.g. modules/backendworker's next_retry_at trick).
func TestJobPlanner_PollOnce_ChainsViBackfillWhenNotDone(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "e2e-vi-chain-tenant"

	registry := blockpack.NewPostgresFromPool(pool).ViUsageRegistry(tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill, "first-ever use must trigger immediately")

	_, err = pool.Exec(ctx,
		`UPDATE viusage_entries SET lease_expires_at = extract(epoch FROM now())::bigint - 60 WHERE tenant = $1`, tenant)
	require.NoError(t, err)

	s := New(pool, common.JobPlannerConfig{ViWindowSeconds: 21600})
	require.NoError(t, s.planVi(ctx))

	var (
		status  string
		detail  jobstore.ViBackfillDetail
		rawJSON []byte
	)
	row := pool.QueryRow(ctx, `SELECT status, detail FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status, &rawJSON))
	require.Equal(t, string(jobstore.StatusPending), status)
	require.NoError(t, json.Unmarshal(rawJSON, &detail))
	require.EqualValues(t, 21600, detail.WindowSeconds)
}

func TestJobPlanner_PollOnce_SkipsWhenDone(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "e2e-vi-done-tenant"

	registry := blockpack.NewPostgresFromPool(pool).ViUsageRegistry(tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill)

	require.NoError(t, registry.UpdateWatermark(
		ctx, tenant, triggerResult.Entry.ColumnHash, triggerResult.Entry.ColumnType, 0, 0, 0, true,
	))

	s := New(pool, common.JobPlannerConfig{ViWindowSeconds: 21600})
	require.NoError(t, s.planVi(ctx))

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count, "a done column must never be chained")
}

func TestJobPlanner_PollOnce_SkipsWhenNonTerminalJobAlreadyExists(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "e2e-vi-existing-tenant"

	registry := blockpack.NewPostgresFromPool(pool).ViUsageRegistry(tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill)

	store := jobstore.New(pool)
	require.NoError(t, store.InsertViBackfill(ctx, tenant, jobstore.ViBackfillDetail{
		ColumnHash: triggerResult.Entry.ColumnHash, ColumnName: triggerResult.Entry.ColumnName,
		ColumnType: triggerResult.Entry.ColumnType, WindowSeconds: 999,
	}))

	s := New(pool, common.JobPlannerConfig{ViWindowSeconds: 21600})
	require.NoError(t, s.planVi(ctx), "the dedup_key conflict must be a silent no-op, not an error")

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 1, count, "a genuinely in-flight job must never be double-enqueued")

	var windowSeconds uint64
	row = pool.QueryRow(ctx, `SELECT (detail->>'window_seconds')::bigint FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&windowSeconds))
	require.EqualValues(t, 999, windowSeconds, "the pre-existing row's own detail must be untouched by the no-op insert attempt")
}

func TestJobPlanner_PollOnce_ChainsCubeBackfillFromExistingWatermark(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "e2e-cube-chain-tenant"
	cubeID := "cube-with-l0-watermark"

	cubeRegistry := blockpack.NewPostgresFromPool(pool).CubeRegistry(tenant)
	require.NoError(t, cubeRegistry.Add(ctx, blockpack.CubeRegistryEntry{
		CubeID: cubeID, Tenant: tenant, Dimensions: []string{"resource.service.name"},
	}))
	require.NoError(t, cubeRegistry.UpdateWatermarks(ctx, cubeID, blockpack.CubeRollupL0, 100, 200))

	s := New(pool, common.JobPlannerConfig{CubeWindowMinutes: 1440})
	require.NoError(t, s.planCube(ctx))

	var (
		status string
		detail jobstore.CubeBackfillDetail
		raw    []byte
	)
	row := pool.QueryRow(ctx, `SELECT status, detail FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status, &raw))
	require.Equal(t, string(jobstore.StatusPending), status)
	require.NoError(t, json.Unmarshal(raw, &detail))
	require.Equal(t, cubeID, detail.CubeID)
	require.EqualValues(t, 1440, detail.WindowMinutes)
}

func TestJobPlanner_PollOnce_SkipsCubeWithNoL0WatermarkYet(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "e2e-cube-nowm-tenant"
	cubeID := "cube-without-l0-watermark"

	cubeRegistry := blockpack.NewPostgresFromPool(pool).CubeRegistry(tenant)
	require.NoError(t, cubeRegistry.Add(ctx, blockpack.CubeRegistryEntry{
		CubeID: cubeID, Tenant: tenant, Dimensions: []string{"resource.service.name"},
	}))

	s := New(pool, common.JobPlannerConfig{CubeWindowMinutes: 1440})
	require.NoError(t, s.planCube(ctx))

	var count int
	row := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count, "a cube with no confirmed L0 watermark yet (still on its first pass) must never be chained")
}
