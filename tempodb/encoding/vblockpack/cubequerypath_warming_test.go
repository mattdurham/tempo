package vblockpack

// cubequerypath_warming_test.go — F-10 (issue #481 part 3, R1's self-healing story):
// real-write-path/real-conversion-path tests for tryQueryFromCube's ErrCubeWarming
// distinction and QueryRange's error-path wiring. Uses cubeQueryPath's store injection seam
// (cubequerypath.go's objectStore()) to exercise tryQueryFromCube's cube-not-found branch
// through the REAL production method, without a live S3/minio server.

import (
	"context"
	"errors"
	"strings"
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

// emptyCubeObjectStore is a minimal blockpack.CubeObjectStore reporting "not found" for every
// path — a genuinely empty cube registry, mirroring minioObjectStore.Get's own NoSuchKey
// convention (nil data, empty etag, nil error).
type emptyCubeObjectStore struct{}

func (emptyCubeObjectStore) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", nil
}

func (emptyCubeObjectStore) ConditionalPut(context.Context, string, []byte, string) error {
	return nil
}

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

// newEmptyTestCubeQueryPath returns a *cubeQueryPath backed by emptyCubeObjectStore — every
// query with group-by dims and a cube-representable filter reaches tryQueryFromCube's
// "!result.Found" branch (a genuinely empty registry), firing maybeCreateCube and returning
// ErrCubeWarming.
func newEmptyTestCubeQueryPath() *cubeQueryPath {
	return &cubeQueryPath{
		store:      emptyCubeObjectStore{},
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
	}
}

// preventBackgroundCreateAttempt pre-populates cqp.createSeen with a FRESH cooldown entry for
// (tenant, query)'s exact (dims, filters) key — computed the SAME way maybeCreateCube itself
// does — so the background goroutine tryQueryFromCube fires (`go cqp.maybeCreateCube(...)`)
// hits its own cooldown check and returns IMMEDIATELY, before ever reaching fetchVCNTSection/
// TryCreate/launchBackfill. Those deeper steps need a real S3/minio client (cqp.client, which
// this test's cqp intentionally leaves nil via the store-injection seam) and are exercised by
// cube_scheduler_test.go/cubemanager_test.go elsewhere — NOT this file's concern. Without this,
// the fire-and-forget goroutine outlives the test function and can panic on a nil *minio.Client
// asynchronously, crashing an unrelated LATER test in the same process (verified: this is
// exactly what happened before this helper was added).
func preventBackgroundCreateAttempt(cqp *cubeQueryPath, tenant, query string) {
	dims := extractGroupByDims(query)
	filters, _ := extractFilters(query)
	key := tenant + "|" + strings.Join(dims, ",") + "|" + filterDedupKey(filters)
	cqp.createSeen[key] = time.Now()
}

// TestTryQueryFromCube_NoGroupByDims_DistinctFromCubeWarming is a MUST per F-10: a query with
// no group-by dims declines at tryQueryFromCube's FIRST check, before any registry/object-store
// access at all — this must return (nil, false, nil), never ErrCubeWarming, since "not
// cube-applicable at all" and "cube not yet backfilled" are different, non-conflatable reasons
// (only the latter is self-healing / worth a "retry shortly" message).
func TestTryQueryFromCube_NoGroupByDims_DistinctFromCubeWarming(t *testing.T) {
	// client/bucket/store are all zero-value/nil — if this test reaches any object-store code,
	// it panics, proving the no-group-by-dims decline really does short-circuit before any I/O.
	cqp := &cubeQueryPath{tenants: make(map[string]*tenantCubeState), createSeen: make(map[string]time.Time)}

	resp, ok, err := cqp.tryQueryFromCube(context.Background(), "test-tenant", &tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()", // no `by (...)` — no group-by dims
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	})
	require.False(t, ok)
	require.Nil(t, resp)
	require.NoError(t, err, "no-group-by-dims must decline silently (nil,false,nil), never ErrCubeWarming")
	require.False(t, errors.Is(err, ErrCubeWarming))
}

// TestTryQueryFromCube_EmptyRegistry_ReturnsCubeWarming is F-10's companion positive case: a
// query WITH group-by dims and a cube-representable filter, against a genuinely empty registry
// (no cube exists yet for this shape), returns ErrCubeWarming — proving the distinction the test
// above pins actually fires for the warming case, not just that it doesn't misfire for the
// non-applicable case.
func TestTryQueryFromCube_EmptyRegistry_ReturnsCubeWarming(t *testing.T) {
	cqp := newEmptyTestCubeQueryPath()
	const query = `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`
	preventBackgroundCreateAttempt(cqp, "test-tenant", query)

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
	cqp := newEmptyTestCubeQueryPath()
	const query = `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`
	preventBackgroundCreateAttempt(cqp, "test-tenant", query)
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
