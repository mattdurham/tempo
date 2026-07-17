package vblockpack

// cubequerypath_warming_test.go — F-10 (issue #481 part 3, R1's self-healing story):
// real-write-path/real-conversion-path tests for tryQueryFromCube's ErrCubeWarming
// distinction and QueryRange's error-path wiring. Exercises tryQueryFromCube's
// cube-not-found branch through the REAL production method, without a live S3/minio server.
//
// 2026-07-15 migration (issue #504): cube's registry is Postgres-only now (no
// blob/index.json fallback) -- newEmptyTestCubeQueryPath used to back cqp with a fake
// always-"not found" blockpack.CubeObjectStore (emptyCubeObjectStore, via
// cubequerypath.go's now-removed objectStore() DI seam) to simulate a genuinely empty
// registry. A freshly-created ephemeral Postgres pool (newTestPostgresPool) with no
// cube_entries rows achieves the exact same "not found" semantics now.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// withCubeQueryPath installs cqp as the process-level cube query path for the duration of the
// test, restoring the previous value (nil in production tests, since ConfigureCubeQueryPath is
// never called in this test binary) on cleanup.
func withCubeQueryPath(t *testing.T, cqp *cubeQueryPath) {
	t.Helper()
	processCubeQueryPathMu.Lock()
	prev := processCubeQueryPath
	processCubeQueryPath = cqp
	processCubeQueryPathMu.Unlock()
	t.Cleanup(func() {
		processCubeQueryPathMu.Lock()
		processCubeQueryPath = prev
		processCubeQueryPathMu.Unlock()
	})
}

// newEmptyTestCubeQueryPath returns a *cubeQueryPath backed by a genuinely empty (freshly
// migrated, no cube_entries rows) real Postgres pool — every query with group-by dims and a
// cube-representable filter reaches tryQueryFromCube's "!result.Found" branch (a genuinely
// empty registry), firing blockpack.CubeQueryPath's internal creation trigger and returning
// ErrCubeWarming.
//
// #508: files/lister/vi are all nil -- safe here because blockpack.CubeQueryPath's own
// not-found/warming path never touches them (only the found/fan-out path does, which these
// warming-focused tests never reach), and OnCreateAttempt is left nil (the zero value), so the
// background creation-trigger goroutine never touches S3/minio either -- unlike the pre-#508
// launchBackfill call this test file used to have to suppress via a cooldown-map hack, there is
// no longer any nil-client panic risk to guard against.
func newEmptyTestCubeQueryPath(t *testing.T) *cubeQueryPath {
	t.Helper()
	pool := newTestPostgresPool(t)
	qp := blockpack.NewCubeQueryPath(nil, nil, nil, pool, blockpack.CubeQueryPathConfig{})
	return &cubeQueryPath{pgPool: pool, qp: qp}
}

// TestTryQueryFromCube_NoGroupByDims_NowReachesQueryRange is #508's Zero-Dimension Cube Support
// regression pin: prior to #508, a query with no group-by dims declined at tryQueryFromCube's
// FIRST check, before any registry access at all (nil, false, nil), never ErrCubeWarming. #508
// deliberately REMOVED that early return so an ungrouped query's empty dims flow into
// CubeQueryPathRequest.Dims and on into blockpack's creation trigger/router unchanged (both are
// dims-length-agnostic, SPEC-CUBE-033) -- an ungrouped query against a genuinely empty registry
// now ALSO fires cube creation and returns ErrCubeWarming, exactly like a grouped query.
func TestTryQueryFromCube_NoGroupByDims_NowReachesQueryRange(t *testing.T) {
	cqp := newEmptyTestCubeQueryPath(t)

	resp, ok, err := cqp.tryQueryFromCube(context.Background(), "test-tenant", &tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()", // no `by (...)` — no group-by dims
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	})
	require.False(t, ok)
	require.Nil(t, resp)
	require.Error(t, err, "#508: an ungrouped query against an empty registry now reaches QueryRange and triggers creation, no longer a silent (nil,false,nil) decline")
	require.True(t, errors.Is(err, ErrCubeWarming), "err = %v, want ErrCubeWarming", err)
}

// TestTryQueryFromCube_EmptyRegistry_ReturnsCubeWarming is F-10's companion positive case: a
// query WITH group-by dims and a cube-representable filter, against a genuinely empty registry
// (no cube exists yet for this shape), returns ErrCubeWarming — proving the distinction the test
// above pins actually fires for the warming case, not just that it doesn't misfire for the
// non-applicable case.
func TestTryQueryFromCube_EmptyRegistry_ReturnsCubeWarming(t *testing.T) {
	cqp := newEmptyTestCubeQueryPath(t)
	const query = `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`

	resp, ok, err := cqp.tryQueryFromCube(context.Background(), "test-tenant", &tempopb.QueryRangeRequest{
		Query: query,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	})
	require.False(t, ok)
	require.Nil(t, resp)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrCubeWarming), "err = %v, want ErrCubeWarming", err)
}

// TestQueryRange_CubeNotYetBackfilled_ProductionDefault_ReturnsWarmingTypedError_TriggersCreate
// is the MUST per F-10/R1's self-healing story, through the REAL production QueryRange entry
// point: a group-by query whose filter has genuine VI coverage (real block, real write path,
// mirroring TestQueryRange_IndexOnlyReturnsTypedErrorOnGroupByInnerDecline's fixture) but whose
// aggregate SHAPE is unsupported by VI's count/rate-only engine — under the PRODUCTION DEFAULT
// (IndexOnly=false, unlike that sibling test), the cube path is tried FIRST; against a
// genuinely empty registry it fires cube creation and declines with ErrCubeWarming, which
// QueryRange must surface distinguishably from the permanent "shape not answerable" reason,
// since a repeat query after backfill should succeed.
func TestQueryRange_CubeNotYetBackfilled_ProductionDefault_ReturnsWarmingTypedError_TriggersCreate(t *testing.T) {
	cqp := newEmptyTestCubeQueryPath(t)
	const query = `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`
	withCubeQueryPath(t, cqp)

	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: query,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{}) // production default: IndexOnly=false
	require.Error(t, err, "an unsupported aggregate shape must still hard-error under the production default, never scan")
	require.True(t, errors.Is(err, ErrCubeWarming),
		"err = %v, want ErrCubeWarming (cube creation was triggered on this exact query, self-healing per R1)", err)
	require.False(t, errors.Is(err, blockpack.ErrMetricsShapeNotAnswerable),
		"must NOT surface the permanent 'shape not answerable' reason once cube creation has been triggered — that would incorrectly discourage retrying")
}

// TestQueryRange_ZeroDimCubeNotYetBackfilled_ReturnsWarmingTypedError_TriggersCreate is #511's
// companion regression pin to the ErrMetricsShapeNotAnswerable case above: a genuinely
// zero-dimension, match-all query (`{} | rate()`, no `by (...)`, no leaf predicate at all)
// declines from VI with blockpack.ErrMetricsNoCoverage (unfiltered_metrics_vi_misattribution_
// test.go's #198 fixture — "no column list to enumerate", not an unsupported aggregate shape),
// never ErrMetricsShapeNotAnswerable.
//
// Before #511 removed tryQueryFromCube's `len(dims) == 0` early return, a zero-dim query could
// never reach blockpack's cube creation trigger at all, so cubeWarming was always false here —
// backend_block.go's cube-warming override only ever needed to check ErrMetricsShapeNotAnswerable
// (the grouped-query case). #511 made cubeWarming reachable for this shape too, exposing a real
// gap: the override's condition still only matched ErrMetricsShapeNotAnswerable, so a zero-dim
// cube's "creation just triggered, retry shortly" signal was silently dropped in favor of the
// permanent-sounding ErrMetricsNoCoverage — discovered via a live retest against the deployed
// cluster, not by inspection. Fixed by adding ErrMetricsNoCoverage to the override condition.
func TestQueryRange_ZeroDimCubeNotYetBackfilled_ReturnsWarmingTypedError_TriggersCreate(t *testing.T) {
	cqp := newEmptyTestCubeQueryPath(t)
	withCubeQueryPath(t, cqp)

	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq("{} | rate()")
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{})
	require.Error(t, err, "a zero-dim query against an empty cube registry must still hard-error, never scan")
	require.True(t, errors.Is(err, ErrCubeWarming),
		"err = %v, want ErrCubeWarming (cube creation was triggered on this exact zero-dim query, self-healing per R1/#511)", err)
	require.False(t, errors.Is(err, blockpack.ErrMetricsNoCoverage),
		"must NOT surface the permanent 'no coverage' reason once zero-dim cube creation has been triggered — that would incorrectly discourage retrying")
}
