package vblockpack

// cube_backfill_watermark_test.go — task #127 regression test: cube's own
// launchBackfill/RunCubeBackfill progressFn callbacks were found (#496
// planning, Section 1) to ONLY log on completion, never persist a watermark
// to the cube registry -- the registry's watermark only ever advanced later,
// incidentally, via a subsequent compaction pass calling the SAME
// UpdateWatermarks method. runCubeBackfillCore's dependency-injected split
// (cube_backfill.go) mirrors vi_backfill.go's runViBackfillCore split and
// makes this testable against fakes without a live S3/minio server --
// cube_backfill.go previously had NO test coverage at all, exactly the R9
// gap this closes.
//
// 2026-07-15 migration (issue #504): cube's registry is Postgres-only now (no
// blob/index.json fallback) -- runCubeBackfillCore's registry-persist side takes a
// *pgxpool.Pool directly (blockpack.NewPgCubeRegistry internally), not an injectable
// blockpack.CubeObjectStore fake. This file's tests now seed/assert against a real
// ephemeral Postgres instance (newTestPostgresPool, shared with pg_entrystore_test.go)
// instead of the old in-memory fakeCubeRegistryObjectStore. Two consequences:
//
//  1. Postgres has no ConditionalPut/etag-conflict concept, so
//     TestRunCubeBackfillCore_PersistFailureAbortsRun's "always-conflict" fake store is
//     gone -- the natural Postgres-equivalent persist failure is "cube not found"
//     (never added to the registry), the same failure mode
//     cube_backfill_s3config_test.go's TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure
//     already exercises end-to-end.
//  2. There is no per-call counter available on the real Postgres client the way the fake
//     store's putCalls field provided, so "persistence happens on EVERY progress callback,
//     not just at Done" can no longer be proven by counting calls.
//     TestRunCubeBackfillCore_PersistsProgressBeforeAbort proves the same contract more
//     directly instead: a successful minute followed by enough consecutive failures to trip
//     the circuit breaker (aborting the run with an error, before Done) must still have
//     durably persisted the earlier success's watermark. If persistence only happened once
//     at the very end (the pre-fix bug this whole file guards against), an aborted run would
//     persist nothing at all, exactly like
//     TestRunCubeBackfillCore_ConsecutiveStructuralFailuresAbortEarly's own "no minute ever
//     succeeded" case below -- the one earlier success surviving the abort is the proof.

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
)

// fakeEmptyCubeValueIndexSource returns no data for every LookupColumn call.
// processMinute (internal/modules/cube/backfill.go) treats a dim1 miss as
// "no data for this minute -- write nothing (sparse cube)" and returns nil,
// so every minute in the run still reaches progressFn as a genuine success,
// which is exactly the property this test needs: a per-minute
// UpdateWatermarks call regardless of whether that minute had real content.
type fakeEmptyCubeValueIndexSource struct{}

func (fakeEmptyCubeValueIndexSource) LookupColumn(
	_ context.Context, _, _ string, _, _ uint64,
) ([]blockpack.VIQueryResult, error) {
	return nil, nil
}

// seedCubeEntryPg registers entry directly via blockpack.NewPgCubeRegistry(pool, ...).Add
// so runCubeBackfillCore's UpdateWatermarks calls succeed (it errors on a not-found cube,
// mirroring viusage.Registry.UpdateWatermark's identical not-found contract).
func seedCubeEntryPg(t *testing.T, pool *pgxpool.Pool, entry blockpack.CubeRegistryEntry) {
	t.Helper()
	registry := blockpack.NewPgCubeRegistry(pool, entry.Tenant)
	require.NoError(t, registry.Add(context.Background(), entry))
}

// TestRunCubeBackfillCore_CallsUpdateWatermarksOnEachProgress is THE critical
// R9 regression test: the run's final persisted registry state must reflect the
// full backfilled window, not just whatever a later, unrelated compaction pass
// happened to write.
func TestRunCubeBackfillCore_CallsUpdateWatermarksOnEachProgress(t *testing.T) {
	pool := newTestPostgresPool(t)
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "abc123",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	seedCubeEntryPg(t, pool, entry)

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: 3,
	}

	completedBefore := testutil.ToFloat64(metricCubeBackfillCompleted)

	err := runCubeBackfillCore(context.Background(), entry, fakeEmptyCubeValueIndexSource{}, pool, cfg, 0)
	require.NoError(t, err)

	assert.Equal(t, completedBefore+1, testutil.ToFloat64(metricCubeBackfillCompleted),
		"reaching prog.Watermark.Done must increment metricCubeBackfillCompleted exactly once")

	registry := blockpack.NewPgCubeRegistry(pool, entry.Tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok, "the run must persist an L0 watermark entry")
	assert.Equal(t, wm.MaxMinute-wm.MinMinute+1, uint32(3),
		"the persisted watermark range must cover all 3 backfilled minutes, not just the last one")
}

// TestRunCubeBackfillCore_PersistsProgressBeforeAbort is the per-callback-persistence proof
// for the Postgres-only world (see file doc comment): one successful minute, immediately
// followed by cubeBackfillMaxConsecutiveFailures (5) consecutive failures that trip the
// circuit breaker and abort the run with an error -- the earlier success's watermark must
// still be durably visible afterward. If persistence only happened once at Done (the R9 bug
// this file exists to catch), an aborted run would persist nothing, exactly like
// TestRunCubeBackfillCore_ConsecutiveStructuralFailuresAbortEarly's all-failing case below.
func TestRunCubeBackfillCore_PersistsProgressBeforeAbort(t *testing.T) {
	pool := newTestPostgresPool(t)
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "one-success-then-abort-cube",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	seedCubeEntryPg(t, pool, entry)

	const (
		currentMinute = 1000
		windowMinutes = 10 // would process minutes [990, 999] if never aborted; processed newest-first
	)
	// Minute 999 (the first one processed, newest-first) succeeds; the next 5 consecutive
	// (994-998) fail, tripping the breaker before minutes 990-993 are ever reached.
	src := &failingMinuteCubeValueIndexSource{
		failMinutes: map[uint32]bool{994: true, 995: true, 996: true, 997: true, 998: true},
		failErr:     errors.New("definition must include duration in AggAttrs"),
	}

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: windowMinutes,
	}

	err := runCubeBackfillCore(context.Background(), entry, src, pool, cfg, currentMinute)
	require.Error(t, err, "5 consecutive failures must abort the run")
	assert.Contains(t, err.Error(), "aborted after 5 consecutive per-minute failures")

	registry := blockpack.NewPgCubeRegistry(pool, entry.Tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok,
		"minute 999's success before the abort must have been durably persisted, proving persistence happens per-callback, not only at Done")
	assert.Equal(t, uint32(999), wm.MinMinute)
	assert.Equal(t, uint32(999), wm.MaxMinute)
}

// TestRunCubeBackfillCore_PersistFailureAbortsRun verifies a watermark-persist
// failure aborts the run rather than silently continuing to backfill more
// minutes the registry cannot yet account for -- mirrors
// TestRunViBackfillCore_PersistFailureAbortsRun's identical contract. The
// Postgres-equivalent persist failure (issue #504) is "cube not found": entry is
// deliberately never added to the registry, so the very first UpdateWatermarksEntry
// call fails exactly the way a crashed/inconsistent trigger would produce (mirrors
// cube_backfill_s3config_test.go's TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure).
func TestRunCubeBackfillCore_PersistFailureAbortsRun(t *testing.T) {
	pool := newTestPostgresPool(t)
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "never-added-cube",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	// Deliberately NOT seeded into the registry.

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: 3,
	}
	err := runCubeBackfillCore(context.Background(), entry, fakeEmptyCubeValueIndexSource{}, pool, cfg, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// failingMinuteCubeValueIndexSource is a deterministic LookupColumn fake, keyed by minute
// (derived from minSec/60, exactly the value processMinute always passes for a single-minute
// call), that returns failErr for every minute in failMinutes and otherwise behaves exactly
// like fakeEmptyCubeValueIndexSource (no data, sparse-minute success). This is the smallest
// extension of the existing fakeEmptyCubeValueIndexSource injection seam that lets a test
// deterministically control WHICH minutes structurally fail processMinute's dim1 lookup
// (2026-07-14, circuit-breaker regression tests below) -- a live/fake S3 server has no
// natural way to key a failing GET by minute without a much larger, VI-filename-parsing-aware
// test double, and runCubeBackfillCore's own dependency-injected split exists precisely so
// this kind of scenario is unit-testable against fakes (see its doc comment above).
type failingMinuteCubeValueIndexSource struct {
	failMinutes map[uint32]bool
	failErr     error
}

func (s *failingMinuteCubeValueIndexSource) LookupColumn(
	_ context.Context, _, _ string, minSec, _ uint64,
) ([]blockpack.VIQueryResult, error) {
	if s.failMinutes[uint32(minSec/60)] { //nolint:gosec // test-only, minSec always fits uint32*60
		return nil, s.failErr
	}
	return nil, nil
}

// TestRunCubeBackfillCore_ScatteredTransientFailuresTolerated proves the circuit breaker
// (cubeBackfillMaxConsecutiveFailures) does NOT trip on isolated, non-consecutive per-minute
// failures scattered through an otherwise-healthy run: the run must still complete (Done=true,
// no error), with the watermark covering the full window -- exactly the "a few isolated bad
// minutes scattered through an otherwise-healthy run should NOT trip the breaker" contract.
func TestRunCubeBackfillCore_ScatteredTransientFailuresTolerated(t *testing.T) {
	pool := newTestPostgresPool(t)
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "scattered-cube",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	seedCubeEntryPg(t, pool, entry)

	const (
		currentMinute = 1000
		windowMinutes = 20 // processes minutes [980, 999]
	)
	// 3 scattered, non-adjacent failing minutes -- well under
	// cubeBackfillMaxConsecutiveFailures (5) at any point since each is isolated.
	src := &failingMinuteCubeValueIndexSource{
		failMinutes: map[uint32]bool{995: true, 990: true, 985: true},
		failErr:     errors.New("transient lookup failure"),
	}

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: windowMinutes,
	}

	err := runCubeBackfillCore(context.Background(), entry, src, pool, cfg, currentMinute)
	require.NoError(t, err, "isolated, scattered per-minute failures must not abort the run")

	registry := blockpack.NewPgCubeRegistry(pool, entry.Tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok)
	assert.Equal(t, uint32(980), wm.MinMinute, "the oldest minute (980, itself a success) must still be covered")
	assert.Equal(t, uint32(999), wm.MaxMinute, "the newest minute (999, itself a success) must still be covered")
}

// TestRunCubeBackfillCore_ConsecutiveStructuralFailuresAbortEarly proves the circuit breaker
// DOES trip on an uninterrupted run of cubeBackfillMaxConsecutiveFailures per-minute failures
// (the shape a structural failure like a missing-AggAttrs registry entry produces on literally
// every remaining minute): the run must abort with a real error naming the consecutive-failure
// count, WITHOUT ever persisting a watermark, rather than continuing to burn the rest of the
// window on work that can never succeed.
func TestRunCubeBackfillCore_ConsecutiveStructuralFailuresAbortEarly(t *testing.T) {
	pool := newTestPostgresPool(t)
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "consecutive-fail-cube",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	seedCubeEntryPg(t, pool, entry)

	const (
		currentMinute = 1000
		windowMinutes = 20 // would process minutes [980, 999] if never aborted
	)
	// The 5 newest minutes (995-999) all fail consecutively -- exactly
	// cubeBackfillMaxConsecutiveFailures, with no interleaved success to reset the counter.
	src := &failingMinuteCubeValueIndexSource{
		failMinutes: map[uint32]bool{995: true, 996: true, 997: true, 998: true, 999: true},
		failErr:     errors.New("definition must include duration in AggAttrs"),
	}

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: windowMinutes,
	}

	err := runCubeBackfillCore(context.Background(), entry, src, pool, cfg, currentMinute)
	require.Error(t, err, "an uninterrupted run of consecutive structural failures must abort the run")
	assert.Contains(t, err.Error(), "aborted after 5 consecutive per-minute failures")

	registry := blockpack.NewPgCubeRegistry(pool, entry.Tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	_, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	assert.False(t, ok, "no minute ever succeeded before the abort, so no watermark should have been persisted")
}
