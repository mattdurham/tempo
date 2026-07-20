package backendscheduler

// redaction_state_test.go — TDD coverage for issue #522 #152's redaction-pending mirror: proves
// SubmitRedaction/cleanupBatchIfDone's direct writes into tenant_redaction_state actually land
// against a real Postgres instance, end to end through the real gRPC-shaped call paths (not
// just unit tests of markTenantRedactionPending/clearTenantRedactionPending in isolation).

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
)

// TestBackendScheduler_New_AppliesFileCatalogMigration mirrors
// TestBackendScheduler_New_AppliesBackendJobsMigration's exact shape (backend_jobs_migration_test.go)
// for the new file_catalog migration (issue #522 #152): tenant_redaction_state (added by #143)
// must actually exist after construction, closing the pre-existing gap where file_catalog.sql
// was never applied by any production code path.
func TestBackendScheduler_New_AppliesFileCatalogMigration(t *testing.T) {
	dsn := newTestPostgresDSN(t)

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	cfg.LocalWorkPath = t.TempDir()
	cfg.Postgres = &postgres.Config{DSN: dsn}

	ctx, cancel := context.WithCancel(context.Background())
	store, rr, ww := newStore(ctx, t, t.TempDir())
	defer func() {
		cancel()
		store.Shutdown()
	}()

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	_, err = New(cfg, nil, store, limits, rr, ww)
	require.NoError(t, err)

	verifyPool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer verifyPool.Close()

	var tableCount int
	err = verifyPool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('file_catalog', 'tenant_redaction_state')
	`).Scan(&tableCount)
	require.NoError(t, err)
	require.Equal(t, 2, tableCount, "expected both file_catalog and tenant_redaction_state to exist after BackendScheduler construction with cfg.Postgres configured")
}

// TestSubmitRedaction_MirrorsPendingIntoTenantRedactionState proves SubmitRedaction's direct
// write actually lands: after a real SubmitRedaction call, tenant_redaction_state must show
// pending=TRUE with the real batch_id, for the real tenant.
func TestSubmitRedaction_MirrorsPendingIntoTenantRedactionState(t *testing.T) {
	dsn := newTestPostgresDSN(t)

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	cfg.LocalWorkPath = t.TempDir()
	cfg.Postgres = &postgres.Config{DSN: dsn}

	ctx, cancel := context.WithCancel(context.Background())
	tmpDir := t.TempDir()
	store, rr, ww := newStore(ctx, t, tmpDir)
	defer func() {
		cancel()
		store.Shutdown()
	}()

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	testTenant := "tenant-redact-pg"
	writeTenantBlocks(ctx, t, backend.NewWriter(ww), testTenant, 2)
	time.Sleep(300 * time.Millisecond)

	s, err := New(cfg, nil, store, limits, rr, ww)
	require.NoError(t, err)

	resp, err := s.SubmitRedaction(user.InjectOrgID(ctx, testTenant), &tempopb.SubmitRedactionRequest{
		TraceIds: [][]byte{[]byte("some-trace-id")},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	verifyPool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer verifyPool.Close()

	var pending bool
	var batchID string
	err = verifyPool.QueryRow(
		ctx, `SELECT pending, batch_id FROM tenant_redaction_state WHERE tenant = $1`, testTenant,
	).Scan(&pending, &batchID)
	require.NoError(t, err)
	require.True(t, pending, "tenant_redaction_state.pending must be TRUE immediately after SubmitRedaction")
	require.Equal(t, resp.BatchId, batchID, "batch_id must match the real batch SubmitRedaction created")
}

// TestCleanupBatchIfDone_ClearsPendingInTenantRedactionState proves cleanupBatchIfDone's direct
// write actually lands: once a redaction batch's jobs are all complete and cleanupBatchIfDone
// removes the in-memory batch, tenant_redaction_state must show pending=FALSE for that tenant.
func TestCleanupBatchIfDone_ClearsPendingInTenantRedactionState(t *testing.T) {
	dsn := newTestPostgresDSN(t)

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	cfg.LocalWorkPath = t.TempDir()
	cfg.Postgres = &postgres.Config{DSN: dsn}

	ctx, cancel := context.WithCancel(context.Background())
	tmpDir := t.TempDir()
	store, rr, ww := newStore(ctx, t, tmpDir)
	defer func() {
		cancel()
		store.Shutdown()
	}()

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	testTenant := "tenant-redact-cleanup-pg"
	writeTenantBlocks(ctx, t, backend.NewWriter(ww), testTenant, 1)
	time.Sleep(300 * time.Millisecond)

	s, err := New(cfg, nil, store, limits, rr, ww)
	require.NoError(t, err)

	resp, err := s.SubmitRedaction(user.InjectOrgID(ctx, testTenant), &tempopb.SubmitRedactionRequest{
		TraceIds: [][]byte{[]byte("some-trace-id")},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	verifyPool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer verifyPool.Close()

	var pending bool
	require.NoError(
		t, verifyPool.QueryRow(ctx, `SELECT pending FROM tenant_redaction_state WHERE tenant = $1`, testTenant).Scan(&pending),
	)
	require.True(t, pending, "sanity check: pending must be TRUE before cleanup")

	// Drain and complete every pending redaction job for the tenant, mirroring the
	// real worker sequence (NextPendingJob pops it off the pending queue,
	// RegisterJob/AddJob promotes it to active, StartJob/CompleteJob finishes it) --
	// so cleanupBatchIfDone's HasJobsForTenant guard passes.
	for {
		j := s.work.NextPendingJob(tempopb.JobType_JOB_TYPE_REDACTION)
		if j == nil {
			break
		}
		s.work.RegisterJob(j)
		require.NoError(t, s.work.AddJob(j))
		s.work.StartJob(j.ID)
		s.work.CompleteJob(j.ID)
	}

	s.cleanupBatchIfDone(ctx, testTenant)

	require.NoError(
		t, verifyPool.QueryRow(ctx, `SELECT pending FROM tenant_redaction_state WHERE tenant = $1`, testTenant).Scan(&pending),
	)
	require.False(t, pending, "tenant_redaction_state.pending must be FALSE after cleanupBatchIfDone")
}
