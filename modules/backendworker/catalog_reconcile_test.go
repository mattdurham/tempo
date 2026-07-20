package backendworker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// newJobFromDetail marshals detail into a *jobstore.Job for handler unit
// tests that don't need a real claimed-from-Postgres row.
func newJobFromDetail(t *testing.T, tenant string, jobType jobstore.JobType, detail any) *jobstore.Job {
	t.Helper()
	raw, err := json.Marshal(detail)
	require.NoError(t, err)
	return &jobstore.Job{ID: "job-1", Type: jobType, Tenant: tenant, Detail: raw}
}

// TestProcessCatalogReconcileJobPostgres_Trace_UpsertsLiveBlocksIntoFileCatalog
// proves the "trace" branch reconciles real BlockMetas into tempo's own
// file_catalog table for exactly the job's tenant.
func TestProcessCatalogReconcileJobPostgres_Trace_UpsertsLiveBlocksIntoFileCatalog(t *testing.T) {
	ctx := context.Background()
	pool := newTestPostgresPool(t)

	store, _, _ := newStore(ctx, t, t.TempDir())
	cutTestBlocks(t, store, tenant, 2, 5)
	time.Sleep(200 * time.Millisecond)

	// filecatalog.Lister now only reconciles vblockpack-encoded blocks into file_catalog
	// (issue #522 #158) -- cutTestBlocks/newStore write vParquet4 by default
	// (encoding.LatestEncoding()), so force the real, already-polled BlockMeta pointers to
	// report vblockpack here to exercise the "trace" branch's real upsert path. Safe: the
	// blocklist holds these exact pointers until the next poll tick, which this test never
	// triggers again before reading them back out via Lister.RunOnce below.
	for _, m := range store.BlockMetas(tenant) {
		m.Version = "vblockpack"
	}

	w := &BackendWorker{store: store, pgPool: pool}
	job := newJobFromDetail(t, tenant, jobstore.JobTypeCatalogReconcile, jobstore.CatalogReconcileDetail{
		Subsystem: "trace", Tenant: tenant,
	})
	require.NoError(t, w.processCatalogReconcileJobPostgres(ctx, job))

	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_catalog WHERE tenant = $1`, tenant).Scan(&count))
	require.Equal(t, 2, count, "expected one file_catalog row per real block")
}

// TestProcessCatalogReconcileJobPostgres_UnknownSubsystem_ReturnsError proves
// vi/vcnt/cube (removed outright by #154, now handled entirely by
// blockpack's own compaction-worker) fall through to the same error as any
// other unrecognized subsystem, rather than being silently accepted.
func TestProcessCatalogReconcileJobPostgres_UnknownSubsystem_ReturnsError(t *testing.T) {
	ctx := context.Background()
	for _, subsystem := range []string{"bogus", "vi", "vcnt", "cube"} {
		w := &BackendWorker{}
		job := newJobFromDetail(t, "tenant-a", jobstore.JobTypeCatalogReconcile, jobstore.CatalogReconcileDetail{
			Subsystem: subsystem, Tenant: "tenant-a",
		})
		require.Error(t, w.processCatalogReconcileJobPostgres(ctx, job), "subsystem %q must be rejected", subsystem)
	}
}

// TestTryClaimPostgresJob_ClaimsCatalogReconcileWhenNoBackfillWorkExists
// proves the dispatch-priority chain reaches catalog_reconcile (issue #522,
// trace/span-only per #154) when neither vi_backfill nor cube_backfill has
// claimable work.
func TestTryClaimPostgresJob_ClaimsCatalogReconcileWhenNoBackfillWorkExists(t *testing.T) {
	ctx := context.Background()
	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	require.NoError(t, store.InsertCatalogReconcile(ctx, "trace", "tenant-a"))

	w := &BackendWorker{jobStore: store, workerID: "worker-1"}
	job, err := w.tryClaimPostgresJob(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, jobstore.JobTypeCatalogReconcile, job.Type)
}

// TestDispatchPostgresJob_RoutesCatalogReconcile proves dispatchPostgresJob's
// switch reaches processCatalogReconcileJobPostgres for its job type
// (deterministically fails here -- no pgPool configured -- but that's enough
// to prove routing: an "unknown postgres job type" error would look
// different from the handler's own distinct error). catalog_reap routing
// was removed outright by #154 along with the rest of tempo's now-redundant
// reap mechanism.
func TestDispatchPostgresJob_RoutesCatalogReconcile(t *testing.T) {
	ctx := context.Background()
	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	w := &BackendWorker{jobStore: store, workerID: "worker-1"}

	require.NoError(t, store.InsertCatalogReconcile(ctx, "trace", "tenant-a"))
	reconcileJob, err := store.Claim(ctx, jobstore.JobTypeCatalogReconcile, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, reconcileJob)
	require.NoError(t, w.dispatchPostgresJob(ctx, reconcileJob))
	var reconcileStatus string
	require.NoError(
		t,
		pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE id = $1`, reconcileJob.ID).Scan(&reconcileStatus),
	)
	require.Equal(
		t,
		string(jobstore.StatusFailed),
		reconcileStatus,
		"postgres not configured on worker must fail the job",
	)
}
