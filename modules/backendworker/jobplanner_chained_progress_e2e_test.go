package backendworker

// jobplanner_chained_progress_e2e_test.go -- issue #518 Part 4's single most
// important test: proves job-planner's REAL poll-decide-enqueue loop, chained
// across multiple real backend-worker claim+execute cycles, makes genuine
// BACKWARD progress (each chained window strictly older than the last), not
// just "a job ran." Lives in package backendworker (not modules/jobplanner)
// specifically to reuse this package's existing real-Postgres/real-fake-S3
// worker test infrastructure (setupDependencies, newFakeS3Config,
// newTestPostgresPoolAndDSN) rather than duplicating it -- job-planner's own
// planVi/planCube are unexported, so this drives them via
// jobplanner.Service.PollOnce, the one exported seam added for exactly this
// cross-package integration-testing purpose. No import cycle: jobplanner
// never imports backendworker.
//
// Both tests target an intentionally EMPTY fake S3 bucket: idle
// minutes/blocks are cheap regardless of window size (mirrors
// TestE2E_CubeBackfill_BoundedRetention_ReachesFullCompletion's own
// "idle minutes are cheap against a fake S3 server" reasoning) -- what these
// tests assert is WHERE each chained job's resulting watermark lands, not
// whether real data got extracted.

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/jobplanner"
	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// TestJobPlanner_ChainedViJobsMakeGenuineBackwardProgress is issue #518 Part
// 4's VI mirror of the cube chained-progress test: drives 3 real
// poll -> insert -> claim -> execute cycles (job-planner's real PollOnce,
// backend-worker's real processJobs) against an empty fake S3 bucket, and
// asserts each cycle's resulting watermark is exactly WindowSeconds older
// than the previous cycle's -- genuine, exact backward progress, not merely
// "a job ran three times." Regression-pins #132's Correction 2 (AnchorSec)
// end to end through job-planner's own real enqueue decision, not just a
// hand-inserted single job (already covered by
// TestE2E_ViBackfill_ChainedJobAnchorsToPersistedWatermarkNotNow).
func TestJobPlanner_ChainedViJobsMakeGenuineBackwardProgress(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-jobplanner-vi-chain-bucket")

	tenant := "e2e-jobplanner-vi-chain-tenant"
	registry := blockpack.NewPgViUsageRegistry(pool, tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill, "first-ever use must trigger immediately (R4)")

	_, err = pool.Exec(ctx,
		`UPDATE viusage_entries SET lease_expires_at = extract(epoch FROM now())::bigint - 60 WHERE tenant = $1`, tenant)
	require.NoError(t, err)

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}
	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)

	const windowSeconds = 3600
	planner := jobplanner.New(pool, common.JobPlannerConfig{ViWindowSeconds: windowSeconds})

	watermarks := make([]uint64, 0, 3)
	for i := 0; i < 3; i++ {
		require.NoError(t, planner.PollOnce(ctx), "iteration %d: PollOnce", i)
		require.NoError(t, w.processJobs(ctx), "iteration %d: processJobs", i)

		entries, _, loadErr := registry.Load(ctx)
		require.NoError(t, loadErr)
		require.Len(t, entries, 1)
		require.False(t, entries[0].Backfill.Done,
			"iteration %d: a bounded window with nonzero resolved minSec must never claim Done (#519)", i)
		watermarks = append(watermarks, entries[0].Backfill.WatermarkSec)

		_, err = pool.Exec(ctx,
			`UPDATE viusage_entries SET lease_expires_at = extract(epoch FROM now())::bigint - 60 WHERE tenant = $1`, tenant)
		require.NoError(t, err)
	}

	require.Len(t, watermarks, 3)
	for i := 1; i < len(watermarks); i++ {
		require.EqualValues(t, watermarks[i-1]-windowSeconds, watermarks[i],
			"chained watermark %d must be exactly windowSeconds older than watermark %d", i, i-1)
	}
}

// TestJobPlanner_ChainedCubeJobsMakeGenuineBackwardProgress is issue #518
// Part 4's single most important test: mirrors the VI test above for cube,
// driving 3 real poll -> insert -> claim -> execute cycles against an empty
// fake S3 bucket, asserting Watermarks[CubeRollupL0].MinMinute strictly
// decreases by exactly CubeWindowMinutes each cycle. Regression-pins #133's
// Correction 1 (currentMinuteAnchor) end to end through job-planner's own
// real enqueue decision.
func TestJobPlanner_ChainedCubeJobsMakeGenuineBackwardProgress(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-jobplanner-cube-chain-bucket")

	tenant := "e2e-jobplanner-cube-chain-tenant"
	cubeID := "e2e-jobplanner-cube-chain-1"
	cubeRegistry := blockpack.NewPgCubeRegistry(pool, tenant)
	require.NoError(t, cubeRegistry.Add(ctx, blockpack.CubeRegistryEntry{
		CubeID: cubeID, Tenant: tenant, Dimensions: []string{"resource.service.name"},
		AggAttrs: []string{blockpack.CubeDurationColumn}, Resolution: 1,
	}))

	// Seed an existing L0 watermark far in the past, decoupled from wall-clock
	// now, so job-planner sees this as "already backfilled at least once, more
	// history may remain" (decision (c) -- a cube with NO watermark yet is
	// intentionally excluded, since that's the reactive first-trigger's job).
	const initialMinMinute = 10_000_000
	require.NoError(t, cubeRegistry.UpdateWatermarks(ctx, cubeID, blockpack.CubeRollupL0, initialMinMinute, initialMinMinute+1000))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}
	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)

	const windowMinutes = 60
	planner := jobplanner.New(pool, common.JobPlannerConfig{CubeWindowMinutes: windowMinutes})

	minMinutes := make([]uint32, 0, 3)
	for i := 0; i < 3; i++ {
		require.NoError(t, planner.PollOnce(ctx), "iteration %d: PollOnce", i)

		boundedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = w.processJobs(boundedCtx)
		cancel()
		require.NoError(t, err, "iteration %d: processJobs", i)

		var status string
		row := pool.QueryRow(ctx,
			`SELECT status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1 ORDER BY created_at DESC LIMIT 1`, tenant)
		require.NoError(t, row.Scan(&status))
		require.Equal(t, string(jobstore.StatusSucceeded), status, "iteration %d", i)

		entries, _, loadErr := cubeRegistry.Load(ctx)
		require.NoError(t, loadErr)
		require.Len(t, entries, 1)
		wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
		require.True(t, ok, "iteration %d: expected a real L0 watermark", i)
		minMinutes = append(minMinutes, wm.MinMinute)
	}

	require.Len(t, minMinutes, 3)
	for i := 1; i < len(minMinutes); i++ {
		require.EqualValues(t, minMinutes[i-1]-windowMinutes, minMinutes[i],
			"chained MinMinute %d must be exactly windowMinutes older than MinMinute %d", i, i-1)
	}
}
