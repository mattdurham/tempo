package vblockpack

// cube_backfill_watermark_test.go — #508 Phase 14 migration: the per-callback watermark
// persistence and circuit-breaker logic this file used to unit-test directly against
// runCubeBackfillCore + an injectable fake CubeValueIndexSource moved into blockpack
// (cube_backfill_runner.go) along with that entire dependency-injection seam.
// blockpack's own cube_backfill_runner_test.go now covers the EXACT SAME scenarios this file
// used to (TestRunCubeBackfill_WritesFileAndAdvancesWatermark for per-callback persistence,
// TestRunCubeBackfill_CircuitBreakerAbortsAfterConsecutiveFailures for the consecutive-failure
// abort, TestRunCubeBackfill_CtxCancelReturnsNonNilError for ctx cancellation) using fakes,
// since blockpack still owns that seam.
//
// What remains testable from tempo's side: RunCubeBackfill (this package) is now a thin
// wrapper with NO fake-injection seam of its own (it builds a real *minio.Client from
// *s3backend.Config and delegates to blockpack.RunCubeBackfill directly) -- a genuinely
// completed run is not practically testable here in reasonable time (WindowMinutes is
// hardcoded to math.MaxUint32 with no override, and Backfiller.Run's own doc comment/#181
// Phase 4's investigation both independently confirmed an unbounded from-scratch run never
// returns quickly regardless of real or fake S3 -- this was already true before #508, not a
// regression). What IS fast and deterministic to test here is the metric-increment LOGIC this
// wrapper itself now owns (moved out of the old per-callback progressFn, since blockpack has no
// metrics dependency): an already-cancelled ctx makes Backfiller.Run's very first loop
// iteration return ctx.Err() immediately, before any S3/VI I/O, letting this test observe
// RunCubeBackfill's real error-handling branch without waiting on an unbounded window.

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
)

// TestRunCubeBackfill_NilS3Config_NoOpNoMetrics proves the pre-existing "s3cfg == nil is a
// silent no-op" contract survived the #508 move: no metric increments, no error.
func TestRunCubeBackfill_NilS3Config_NoOpNoMetrics(t *testing.T) {
	entry := blockpack.CubeRegistryEntry{
		CubeID: "nil-s3cfg-cube", Tenant: "tenant-a",
		Dimensions: []string{"service.name"}, AggAttrs: []string{blockpack.CubeDurationColumn}, Resolution: 1,
	}
	startedBefore := testutil.ToFloat64(metricCubeBackfillStarted)
	completedBefore := testutil.ToFloat64(metricCubeBackfillCompleted)
	failedBefore := testutil.ToFloat64(metricCubeBackfillFailed)

	err := RunCubeBackfill(context.Background(), entry, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, startedBefore, testutil.ToFloat64(metricCubeBackfillStarted), "nil s3cfg must be a no-op before metricCubeBackfillStarted")
	assert.Equal(t, completedBefore, testutil.ToFloat64(metricCubeBackfillCompleted))
	assert.Equal(t, failedBefore, testutil.ToFloat64(metricCubeBackfillFailed))
}

// TestRunCubeBackfill_CtxAlreadyCancelled_ReturnsErrorWithoutIncrementingFailedMetric proves the
// thin wrapper's own metric-increment logic: a ctx cancellation is the caller's own decision,
// not a genuine backfill failure, so metricCubeBackfillFailed must NOT increment for it -- this
// distinction now lives entirely in THIS wrapper (moved out of the old per-callback progressFn,
// since blockpack owns no metrics dependency). metricCubeBackfillStarted DOES still increment
// (set before the call, mirroring the pre-#508 launchBackfill/RunCubeBackfill's own posture of
// counting every attempt, successful or not).
func TestRunCubeBackfill_CtxAlreadyCancelled_ReturnsErrorWithoutIncrementingFailedMetric(t *testing.T) {
	entry := blockpack.CubeRegistryEntry{
		CubeID: "ctx-cancelled-cube", Tenant: "tenant-a",
		Dimensions: []string{"service.name"}, AggAttrs: []string{blockpack.CubeDurationColumn}, Resolution: 1,
	}
	pool := newTestPostgresPool(t)
	registry := blockpack.NewPgCubeRegistry(pool, entry.Tenant)
	require.NoError(t, registry.Add(context.Background(), entry))

	s3cfg := newFakeS3Config(t, "e2e-ctx-cancel-bucket")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before RunCubeBackfill is even called

	startedBefore := testutil.ToFloat64(metricCubeBackfillStarted)
	completedBefore := testutil.ToFloat64(metricCubeBackfillCompleted)
	failedBefore := testutil.ToFloat64(metricCubeBackfillFailed)

	err := RunCubeBackfill(ctx, entry, s3cfg, pool)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "err = %v, want context.Canceled", err)

	assert.Equal(t, startedBefore+1, testutil.ToFloat64(metricCubeBackfillStarted), "every attempt increments Started, successful or not")
	assert.Equal(t, completedBefore, testutil.ToFloat64(metricCubeBackfillCompleted), "a cancelled ctx must never increment Completed")
	assert.Equal(t, failedBefore, testutil.ToFloat64(metricCubeBackfillFailed), "a ctx cancellation is the caller's own decision, not a genuine backfill failure")
}
