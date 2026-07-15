package vblockpack

// cubequerypath_jobstore_test.go — #181 Phase 2, §6.2/§9 "2.1": trigger-point wiring
// tests for maybeCreateCube's Created branch inserting a durable backend_jobs row
// alongside (not instead of) launchBackfill's existing behavior.
//
// cqp.client is deliberately left nil (never installed as the process singleton via
// withCubeQueryPath) so launchBackfill's own getCubeQueryPath()==nil no-op guard fires
// instead of touching a real S3/minio client -- mirrors cubequerypath_warming_test.go's
// own documented reasoning for avoiding a nil-client panic in the async backfill
// goroutine (preventBackgroundCreateAttempt's doc comment).

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// TestMaybeCreateCube_Created_InsertsPendingJob proves §6.2's "insert alongside, don't
// replace" design: a real Postgres pending cube_backfill row is created for a
// newly-registered cube.
func TestMaybeCreateCube_Created_InsertsPendingJob(t *testing.T) {
	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	cqp := &cubeQueryPath{
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
		jobStore:   jobstore.New(pool),
		// pgPool backs maybeCreateCube's cube registry (issue #504: Postgres-only now, no
		// blob/index.json fallback) -- a genuinely empty (freshly migrated, no cube_entries
		// rows) real Postgres instance is this test's "no cube exists yet" fixture, in place
		// of the old emptyCubeObjectStore blob fake.
		pgPool: pool,
	}

	tenant := "tenant-cube-a"
	dims := []string{"resource.service.name"}
	now := time.Now()
	minTS, maxTS := uint64(now.Add(-time.Hour).Unix()), uint64(now.Unix())

	cqp.maybeCreateCube(context.Background(), tenant, dims, nil, "", blockpack.CubeAggAttrTypeFloat64, false, minTS, maxTS)

	var (
		jobType, gotTenant, status string
		count                      int
	)
	row := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 1, count, "expected exactly one cube_backfill row for %s", tenant)

	row = pool.QueryRow(context.Background(),
		`SELECT job_type, tenant, status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&jobType, &gotTenant, &status))
	assert.Equal(t, "cube_backfill", jobType)
	assert.Equal(t, tenant, gotTenant)
	assert.Equal(t, "pending", status)
}

// TestMaybeCreateCube_Created_NilPgPool_SkipsInsertNoError is the regression guard for
// every non-Postgres deployment: cqp.jobStore == nil must not panic/error, and the
// existing launchBackfill-only behavior is fully preserved (launchBackfill itself is a
// safe no-op here since this cqp is never installed as the process singleton).
func TestMaybeCreateCube_Created_NilPgPool_SkipsInsertNoError(t *testing.T) {
	cqp := &cubeQueryPath{
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
		// pgPool deliberately left nil -- this is the "no Postgres configured" regression
		// guard itself (see test name/doc comment).
	}

	tenant := "tenant-cube-b"
	dims := []string{"resource.service.name"}
	now := time.Now()
	minTS, maxTS := uint64(now.Add(-time.Hour).Unix()), uint64(now.Unix())

	assert.NotPanics(t, func() {
		cqp.maybeCreateCube(context.Background(), tenant, dims, nil, "", blockpack.CubeAggAttrTypeFloat64, false, minTS, maxTS)
	})
}
