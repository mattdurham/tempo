package vblockpack

// backend_jobs_e2e_test.go — #181 Phase 6.1/6.2 (trigger half): real end-to-end proof
// that the REAL production trigger functions (realUsageRecorder.RecordUse, reached via
// ConfigureViUsage's installed singleton -- the exact call vi_usage_hook_jobstore_test.go's
// own Phase 2 tests use; cubeQueryPath.maybeCreateCube, reached via the SAME
// withCubeQueryPath singleton-injection helper cubequerypath_warming_test.go already
// established) insert a durable backend_jobs row when Postgres is configured, against a
// REAL Postgres (testcontainers, mirroring every prior phase's convention) and a REAL
// S3-compatible endpoint (fake_s3_e2e_test.go's hand-rolled server -- necessary because
// RunCubeBackfill/LoadCubeEntry (cube_backfill.go) construct their own *minio.Client
// directly from *s3backend.Config with no dependency-injection seam, so proving the
// trigger's DOWNSTREAM in-process goroutine also runs for real requires a real S3-shaped
// HTTP endpoint, not a hand-built fake ObjectStore).
//
// The WORKER-claims-and-dispatches half of #181 Phase 6 (6.1/6.2 steps 2-3, 6.3, 6.4)
// lives in modules/backendworker/backend_jobs_e2e_test.go, NOT here: BackendWorker.
// processJobs/dispatchPostgresJob are unexported methods of package backendworker, which
// this package cannot import (backendworker already imports vblockpack; the reverse
// would be a cycle) -- exactly the same package-boundary constraint that made Phase 4's
// own backendworker_postgres_jobstore_test.go seed rows directly via
// jobstore.Store.InsertViBackfill/InsertCubeBackfill rather than reaching into this
// package's private trigger functions. This file proves the trigger half genuinely
// reaches the real jobstore.Insert* calls through PRODUCTION code, not a hand-built Job;
// the backendworker-package file proves the worker half claims and executes a REAL row
// through PRODUCTION code, not a hand-built Job either. Together they cover the full
// pipeline; no single Go test function can span both without violating the import-cycle
// boundary the plan's own §4 reasoned about when it placed jobstore as a sibling package.

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// TestE2E_ViRecordUse_RealTrigger_InsertsPendingJobAndBackfillCompletes is #181 Phase 6.1
// step 1 (real-wiring half): a real ConfigureViUsage + RecordUse call, against a real
// Postgres and a real S3-compatible endpoint, must both (a) durably insert a pending
// vi_backfill row and (b) let the existing in-process launchViBackfill goroutine run to
// completion for real -- proving the trigger's real side effect is not just the row's
// existence, but a genuine backfill against real (if empty) block storage. An empty
// tenant (zero real trace blocks in the fake S3 bucket) is a legitimate, real "nothing to
// backfill" outcome: viBlockFetcher.ListBlocksInRange lists zero blocks via a real HTTP
// round trip, so BackfillEngine.Run's very first (and only) progress callback reports
// Done=true immediately -- this is a genuine completion, not a shortcut.
func TestE2E_ViRecordUse_RealTrigger_InsertsPendingJobAndBackfillCompletes(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	s3cfg := newFakeS3Config(t, "e2e-vi-bucket")

	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}
	require.NoError(t, ConfigureViUsage(s3cfg, nil, nil, usageCfg, triggerCfg, pool))

	rec := getViUsageRecorder()
	require.NotNil(t, rec)

	tenant := "e2e-vi-tenant"
	before := testutil.ToFloat64(metricViBackfillCompleted)
	result, err := rec.RecordUse(context.Background(), tenant, "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill, "first recorded use must trigger per LeaseTTLSeconds' threshold")

	// Real side effect 1: a durable pending row landed in backend_jobs via the REAL
	// jobStore.InsertViBackfill call inside onShouldBackfill.
	var (
		jobType, gotTenant, status string
		count                      int
	)
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
		return row.Scan(&count) == nil && count == 1
	}, 5*time.Second, 10*time.Millisecond, "expected exactly one vi_backfill row for %s", tenant)
	row := pool.QueryRow(context.Background(),
		`SELECT job_type, tenant, status FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&jobType, &gotTenant, &status))
	assert.Equal(t, "vi_backfill", jobType)
	assert.Equal(t, tenant, gotTenant)
	assert.Equal(t, "pending", status)

	// Real side effect 2: the SAME trigger's in-process goroutine (launchViBackfill)
	// really ran and really persisted a Done=true watermark to the real registry --
	// not a hand-built Entry, and not merely a metric increment divorced from actual
	// registry state. Because pgPool is configured here, rec.registryFor(tenant)
	// (vi_usage_hook.go's registryFor) uses the POSTGRES-backed registry
	// (NewRegistryFromEntryStore(newPgViUsageEntryStore(pgPool), tenant)), not the
	// S3-backed one -- confirmed live (a direct fake-S3 object dump during this test's
	// development showed only the VI Putter's own output file, never a
	// tenant/viusage/index.json, proving the registry write really goes to Postgres
	// when pgPool != nil, exactly per the 2026-07-11 registryFor fix this test
	// exercises for real). Block listing also transparently switches to the
	// Postgres-backed file_catalog fetcher (NewViBackfillDepsCatalogOverride) in this
	// configuration -- an empty catalog (no rows for this tenant) is a real, valid
	// "zero blocks" outcome, exactly like the empty-S3-bucket case, and Done=true
	// still requires the real registry write to have round-tripped through Postgres.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metricViBackfillCompleted) > before
	}, 10*time.Second, 20*time.Millisecond, "the real in-process backfill goroutine must complete")

	registry := blockpack.NewRegistryFromEntryStore(newPgViUsageEntryStore(pool), tenant)
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	assert.True(t, entries[0].Backfill.Done,
		"the real backfill run must have persisted Done=true to the real Postgres-backed registry")
}

// TestE2E_MaybeCreateCube_RealTrigger_InsertsPendingJob is #181 Phase 6.2 step 1
// (real-wiring half): a real maybeCreateCube call -- reached the SAME way
// cubequerypath_jobstore_test.go's own Phase 2 tests reached it, but this time with a
// REAL minio client (not nil) installed as the process singleton via withCubeQueryPath
// (cubequerypath_warming_test.go's existing helper) so launchBackfill's own internal
// getCubeQueryPath() lookup finds the SAME real client -- against a real Postgres and a
// real S3-compatible endpoint, must durably insert a pending cube_backfill row.
//
// This test deliberately does NOT wait on launchBackfill's async goroutine
// (runCubeBackfillCore against WindowMinutes=math.MaxUint32): RunCubeBackfill's own doc
// comment and #181 Phase 4's backendworker_postgres_jobstore_test.go both independently
// confirmed a from-scratch cube backfill with an unbounded window never returns in any
// reasonable test time, real S3 or not -- launchBackfill also hardcodes
// context.Background() for that goroutine (not a caller-supplied, cancelable ctx), so it
// cannot be bounded from outside either. That goroutine keeps running in the background
// for the remainder of this test binary's process life (self-limiting: it exits when the
// process does), exactly mirroring real production behavior -- not suppressed here, since
// suppressing it (e.g. by leaving cqp.client nil, Phase 2's own choice) would be LESS
// realistic than this test's real-client wiring, not more. #181 Phase 6.2's own "real side
// effect" proof for the WORKER's claim-and-execute half (a bounded, ctx-limited variant of
// the same call) lives in modules/backendworker/backend_jobs_e2e_test.go instead, where a
// context deadline can legitimately bound the equivalent call.
func TestE2E_MaybeCreateCube_RealTrigger_InsertsPendingJob(t *testing.T) {
	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	s3cfg := newFakeS3Config(t, "e2e-cube-bucket")
	client, err := minio.New(s3cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3cfg.AccessKey, "test-secret-key", ""),
		Secure: false,
		Region: s3cfg.Region,
	})
	require.NoError(t, err)

	cqp := &cubeQueryPath{
		client:     client,
		bucket:     s3cfg.Bucket,
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
		jobStore:   jobstore.New(pool),
		// pgPool backs maybeCreateCube's cube registry (issue #504: Postgres-only now, no
		// blob/index.json fallback) -- without this, maybeCreateCube's nil-pgPool guard
		// would skip cube creation entirely, and this test's whole point (a real durable
		// pending job actually getting inserted) would never fire.
		pgPool: pool,
	}
	withCubeQueryPath(t, cqp)

	tenant := "e2e-cube-tenant"
	dims := []string{"resource.service.name"}
	now := time.Now()
	minTS, maxTS := uint64(now.Add(-time.Hour).Unix()), uint64(now.Unix())

	cqp.maybeCreateCube(context.Background(), tenant, dims, nil, "", blockpack.CubeAggAttrTypeFloat64, false, minTS, maxTS)

	var (
		jobType, gotTenant, status string
		count                      int
	)
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
		return row.Scan(&count) == nil && count == 1
	}, 5*time.Second, 10*time.Millisecond, "expected exactly one cube_backfill row for %s", tenant)
	row := pool.QueryRow(context.Background(),
		`SELECT job_type, tenant, status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&jobType, &gotTenant, &status))
	assert.Equal(t, "cube_backfill", jobType)
	assert.Equal(t, tenant, gotTenant)
	assert.Equal(t, "pending", status)
}
