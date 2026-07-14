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

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
)

// fakeCubeRegistryObjectStore is an in-memory blockpack.CubeObjectStore for
// tests, mirroring fakeViObjectStore's (vi_backfill_test.go) exact shape and
// putCalls counter so the two backfill engines' watermark-persistence
// regression tests read the same way.
type fakeCubeRegistryObjectStore struct {
	mu       sync.Mutex
	data     []byte
	etag     string
	putCalls int
}

func (s *fakeCubeRegistryObjectStore) Get(_ context.Context, _ string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil, "", blockpack.CubeErrNotFound
	}
	cp := append([]byte(nil), s.data...)
	return cp, s.etag, nil
}

func (s *fakeCubeRegistryObjectStore) ConditionalPut(_ context.Context, _ string, data []byte, etag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.etag != etag {
		return blockpack.CubeErrConflict
	}
	s.data = append([]byte(nil), data...)
	s.etag = etag + "x"
	s.putCalls++
	return nil
}

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

// seedCubeEntry registers entry directly via CubeRegistry.Add so
// runCubeBackfillCore's UpdateWatermarks calls succeed (it errors on a
// not-found cube, mirroring viusage.Registry.UpdateWatermark's identical
// not-found contract).
func seedCubeEntry(t *testing.T, store blockpack.CubeObjectStore, entry blockpack.CubeRegistryEntry) {
	t.Helper()
	registry := blockpack.NewCubeRegistry(store, entry.Tenant)
	require.NoError(t, registry.Add(context.Background(), entry))
}

// TestRunCubeBackfillCore_CallsUpdateWatermarksOnEachProgress is THE critical
// R9 regression test mirroring
// TestRunViBackfillCore_CallsUpdateWatermarkOnEachProgress's exact assertion
// style: assert CubeRegistry.UpdateWatermarks (via the fake store's
// ConditionalPut) is actually called once per successfully-processed minute,
// not just once at the end -- and that the final persisted registry state
// reflects the full backfilled window, not just whatever a later,
// unrelated compaction pass happened to write.
func TestRunCubeBackfillCore_CallsUpdateWatermarksOnEachProgress(t *testing.T) {
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "abc123",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	store := &fakeCubeRegistryObjectStore{}
	seedCubeEntry(t, store, entry)
	preSeedPutCalls := store.putCalls

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: 3,
	}

	completedBefore := testutil.ToFloat64(metricCubeBackfillCompleted)

	err := runCubeBackfillCore(context.Background(), entry, fakeEmptyCubeValueIndexSource{}, store, cfg, 0)
	require.NoError(t, err)

	// One UpdateWatermarks (ConditionalPut) call per backfilled minute (3), on
	// top of the seed Add -- i.e. strictly more than one, proving persistence
	// happens on EVERY progress callback, not just the last.
	assert.Equal(t, preSeedPutCalls+3, store.putCalls,
		"expected 1 ConditionalPut per backfilled minute (3), proving per-callback persistence")

	assert.Equal(t, completedBefore+1, testutil.ToFloat64(metricCubeBackfillCompleted),
		"reaching prog.Watermark.Done must increment metricCubeBackfillCompleted exactly once")

	registry := blockpack.NewCubeRegistry(store, entry.Tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok, "the run must persist an L0 watermark entry")
	assert.Equal(t, wm.MaxMinute-wm.MinMinute+1, uint32(3),
		"the persisted watermark range must cover all 3 backfilled minutes, not just the last one")
}

// TestRunCubeBackfillCore_PersistFailureAbortsRun verifies a watermark-persist
// failure aborts the run rather than silently continuing to backfill more
// minutes the registry cannot yet account for -- mirrors
// TestRunViBackfillCore_PersistFailureAbortsRun's identical contract.
func TestRunCubeBackfillCore_PersistFailureAbortsRun(t *testing.T) {
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "abc123",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	store := &fakeCubeRegistryObjectStore{}
	seedCubeEntry(t, store, entry)

	failing := &alwaysConflictCubeStore{inner: store}

	cfg := blockpack.CubeBackfillConfig{
		Store:         &fakeCubeObjectPutter{},
		Workers:       1,
		WindowMinutes: 3,
	}
	err := runCubeBackfillCore(context.Background(), entry, fakeEmptyCubeValueIndexSource{}, failing, cfg, 0)
	require.Error(t, err)
}

type alwaysConflictCubeStore struct {
	inner *fakeCubeRegistryObjectStore
}

func (s *alwaysConflictCubeStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	return s.inner.Get(ctx, path)
}

func (s *alwaysConflictCubeStore) ConditionalPut(context.Context, string, []byte, string) error {
	return blockpack.CubeErrConflict
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
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "scattered-cube",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	store := &fakeCubeRegistryObjectStore{}
	seedCubeEntry(t, store, entry)
	preSeedPutCalls := store.putCalls

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

	err := runCubeBackfillCore(context.Background(), entry, src, store, cfg, currentMinute)
	require.NoError(t, err, "isolated, scattered per-minute failures must not abort the run")

	// One ConditionalPut per successfully-processed minute (20 total - 3 failures = 17).
	assert.Equal(t, preSeedPutCalls+17, store.putCalls)

	registry := blockpack.NewCubeRegistry(store, entry.Tenant)
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
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "consecutive-fail-cube",
		Tenant:     "tenant-a",
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	store := &fakeCubeRegistryObjectStore{}
	seedCubeEntry(t, store, entry)

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

	err := runCubeBackfillCore(context.Background(), entry, src, store, cfg, currentMinute)
	require.Error(t, err, "an uninterrupted run of consecutive structural failures must abort the run")
	assert.Contains(t, err.Error(), "aborted after 5 consecutive per-minute failures")

	registry := blockpack.NewCubeRegistry(store, entry.Tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	_, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	assert.False(t, ok, "no minute ever succeeded before the abort, so no watermark should have been persisted")
}
