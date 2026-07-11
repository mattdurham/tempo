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

	err := runCubeBackfillCore(context.Background(), entry, fakeEmptyCubeValueIndexSource{}, store, cfg)
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
	err := runCubeBackfillCore(context.Background(), entry, fakeEmptyCubeValueIndexSource{}, failing, cfg)
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
