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
// completed run is not practically testable here in reasonable time even after #512 gave
// Backfiller.Run real parallelism, since that still means real S3 I/O for however many minutes
// the resolved retention window covers. What IS fast and deterministic to test here is the
// metric-increment LOGIC this wrapper itself now owns (moved out of the old per-callback
// progressFn, since blockpack has no metrics dependency): an already-cancelled ctx makes
// Backfiller.Run's very first loop iteration return ctx.Err() immediately, before any S3/VI I/O,
// letting this test observe RunCubeBackfill's real error-handling branch without waiting on the
// window. cubeBackfillWindowMinutes (2026-07-17, WindowMinutes bounded by tenant retention
// instead of hardcoded math.MaxUint32) is extracted as a small pure function specifically so
// this one piece of new logic has a real, direct unit test too.

import (
	"context"
	"errors"
	"math"
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

	err := RunCubeBackfill(context.Background(), entry, nil, nil, 0)
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

	err := RunCubeBackfill(ctx, entry, s3cfg, pool, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "err = %v, want context.Canceled", err)

	assert.Equal(t, startedBefore+1, testutil.ToFloat64(metricCubeBackfillStarted), "every attempt increments Started, successful or not")
	assert.Equal(t, completedBefore, testutil.ToFloat64(metricCubeBackfillCompleted), "a cancelled ctx must never increment Completed")
	assert.Equal(t, failedBefore, testutil.ToFloat64(metricCubeBackfillFailed), "a ctx cancellation is the caller's own decision, not a genuine backfill failure")
}

// TestCubeBackfillWindowMinutes pins the 2026-07-17 fix (follow-up to #512): WindowMinutes is
// now bounded by the tenant's actual resolved retention instead of an unconditional
// math.MaxUint32. retentionMinutes == 0 means "retention disabled/unbounded for this tenant" (the
// same convention tempodb.go's retainTenant uses for CompactorOverrides.BlockRetentionForTenant's
// zero value) and must preserve the original unbounded-window behavior exactly.
func TestCubeBackfillWindowMinutes(t *testing.T) {
	tests := []struct {
		name             string
		retentionMinutes uint32
		want             uint32
	}{
		{"zero retention means unbounded", 0, math.MaxUint32},
		{"real retention passes through unchanged", 43200, 43200},
		{"small real retention passes through unchanged", 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cubeBackfillWindowMinutes(tc.retentionMinutes)
			assert.Equal(t, tc.want, got)
		})
	}
}
