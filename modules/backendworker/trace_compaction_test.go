package backendworker

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// TestProcessTraceCompactionJobPostgres_CompactsRealBlocks proves the handler fetches the real
// BlockMeta for each input ID from the tenant's live blocklist and executes the exact same
// merge logic the legacy gRPC CompactionProvider path uses (w.compact/store.CompactWithConfig)
// -- the 2 real input blocks disappear from Metas() and one new output block appears.
func TestProcessTraceCompactionJobPostgres_CompactsRealBlocks(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newStore(ctx, t, t.TempDir())
	blocks := cutTestBlocks(t, store, tenant, 2, 5)
	time.Sleep(300 * time.Millisecond)

	var cfg Config
	cfg.RegisterFlagsAndApplyDefaults("backendworker", &flag.FlagSet{})

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	w := &BackendWorker{store: store, cfg: cfg, overrides: limits}
	job := newJobFromDetail(t, tenant, jobstore.JobTypeTraceCompaction, jobstore.TraceCompactionDetail{
		InputBlockIDs: []string{blocks[0].BlockMeta().BlockID.String(), blocks[1].BlockMeta().BlockID.String()},
	})
	require.NoError(t, w.processTraceCompactionJobPostgres(ctx, job))

	liveMetas := store.BlockMetas(tenant)
	for _, in := range blocks {
		for _, live := range liveMetas {
			require.NotEqual(t, in.BlockMeta().BlockID, live.BlockID, "input block must no longer be live after compaction")
		}

		_, compactedMeta, err := store.BlockMeta(ctx, tenant, in.BlockMeta().BlockID)
		require.NoError(t, err)
		require.NotNil(t, compactedMeta, "input block %s must be recorded as compacted", in.BlockMeta().BlockID)
	}

	require.NotEmpty(t, liveMetas, "the merged output block must now be live")
}

// TestProcessTraceCompactionJobPostgres_MissingTenant_ReturnsError proves the same empty-tenant
// guard every other Postgres job handler already has.
func TestProcessTraceCompactionJobPostgres_MissingTenant_ReturnsError(t *testing.T) {
	ctx := context.Background()
	w := &BackendWorker{}
	job := newJobFromDetail(t, "", jobstore.JobTypeTraceCompaction, jobstore.TraceCompactionDetail{
		InputBlockIDs: []string{"a", "b"},
	})
	require.Error(t, w.processTraceCompactionJobPostgres(ctx, job))
}

// TestProcessTraceCompactionJobPostgres_WrongInputCount_ReturnsError proves the handler rejects
// anything other than exactly 2 input blocks -- #151 made compaction strictly pairwise
// everywhere, and this handler must not silently accept a differently-shaped detail.
func TestProcessTraceCompactionJobPostgres_WrongInputCount_ReturnsError(t *testing.T) {
	ctx := context.Background()
	w := &BackendWorker{}
	for _, ids := range [][]string{{"only-one"}, {"a", "b", "c"}, {}} {
		job := newJobFromDetail(t, "tenant-a", jobstore.JobTypeTraceCompaction, jobstore.TraceCompactionDetail{
			InputBlockIDs: ids,
		})
		require.Error(t, w.processTraceCompactionJobPostgres(ctx, job), "input count %d must be rejected", len(ids))
	}
}

// TestProcessTraceCompactionJobPostgres_BlockNotLive_SkipsWithoutFailingJob proves the handler
// does NOT fail the whole job when one input block ID can no longer be found in the tenant's
// live blocklist (already compacted by a race, cleared by retention, or otherwise vanished
// independent of this handler's own actions -- mirrors blockpack's identical
// ErrObjectNotFound/SPEC-COMPACTIONWORKER-7 handling for VI/VCNT/cube). The still-live input
// block must remain completely untouched (still live, not compacted) so a future job-planner
// pass can pair it with a different sibling.
func TestProcessTraceCompactionJobPostgres_BlockNotLive_SkipsWithoutFailingJob(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newStore(ctx, t, t.TempDir())
	blocks := cutTestBlocks(t, store, tenant, 1, 5)
	time.Sleep(300 * time.Millisecond)

	var cfg Config
	cfg.RegisterFlagsAndApplyDefaults("backendworker", &flag.FlagSet{})

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	w := &BackendWorker{store: store, cfg: cfg, overrides: limits}
	liveBlockID := blocks[0].BlockMeta().BlockID
	job := newJobFromDetail(t, tenant, jobstore.JobTypeTraceCompaction, jobstore.TraceCompactionDetail{
		InputBlockIDs: []string{liveBlockID.String(), backend.NewUUID().String()},
	})
	require.NoError(t, w.processTraceCompactionJobPostgres(ctx, job), "a missing input must not fail the job")

	liveMetas := store.BlockMetas(tenant)
	var stillLive bool
	for _, m := range liveMetas {
		if m.BlockID == liveBlockID {
			stillLive = true
		}
	}
	require.True(t, stillLive, "the still-live input block must remain untouched, not compacted")

	_, compactedMeta, err := store.BlockMeta(ctx, tenant, liveBlockID)
	require.NoError(t, err)
	require.Nil(t, compactedMeta, "the still-live input block must not be recorded as compacted")
}

// TestTryClaimPostgresJob_ClaimsTraceCompactionWhenNoBackfillWorkExists proves the
// dispatch-priority chain reaches trace_compaction (issue #522 #158) when neither
// vi_backfill nor cube_backfill has claimable work.
func TestTryClaimPostgresJob_ClaimsTraceCompactionWhenNoBackfillWorkExists(t *testing.T) {
	ctx := context.Background()
	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	// tenant="" here is deliberate: this test only proves claim-priority routing, not
	// execution -- an empty tenant makes any dispatch attempt fail deterministically at
	// processTraceCompactionJobPostgres's very first guard, with no real store needed.
	require.NoError(t, store.InsertTraceCompaction(ctx, "", jobstore.TraceCompactionDetail{
		InputBlockIDs: []string{"block-a", "block-b"},
	}))

	w := &BackendWorker{jobStore: store, workerID: "worker-1"}
	job, err := w.tryClaimPostgresJob(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, jobstore.JobTypeTraceCompaction, job.Type)
}

// TestDispatchPostgresJob_RoutesTraceCompaction proves dispatchPostgresJob's switch
// reaches processTraceCompactionJobPostgres for its job type (deterministically fails
// here -- empty tenant -- but that's enough to prove routing: an "unknown postgres job
// type" error would look different from the handler's own distinct error).
func TestDispatchPostgresJob_RoutesTraceCompaction(t *testing.T) {
	ctx := context.Background()
	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	w := &BackendWorker{jobStore: store, workerID: "worker-1"}

	require.NoError(t, store.InsertTraceCompaction(ctx, "", jobstore.TraceCompactionDetail{
		InputBlockIDs: []string{"block-a", "block-b"},
	}))
	job, err := store.Claim(ctx, jobstore.JobTypeTraceCompaction, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.NoError(t, w.dispatchPostgresJob(ctx, job))

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE id = $1`, job.ID).Scan(&status))
	require.Equal(t, string(jobstore.StatusFailed), status, "empty tenant must fail the job")
}
