package vblockpack

// vi_usage_hook_jobstore_test.go — #181 Phase 2, §6.1/§9 "2.1": trigger-point wiring
// tests for ConfigureViUsage's onShouldBackfill closure inserting a durable
// backend_jobs row alongside (not instead of) launchViBackfill's existing
// in-process goroutine.

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// TestConfigureViUsage_OnShouldBackfill_InsertsPendingJobWhenPgPoolConfigured proves both
// halves of §6.1's "insert alongside, don't replace" design: a real Postgres pending
// vi_backfill row exists AND the existing in-process goroutine (launchViBackfill ->
// RunViBackfill) still ran.
func TestConfigureViUsage_OnShouldBackfill_InsertsPendingJobWhenPgPoolConfigured(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	rawR, rawW := newLocalRawBackend(t)
	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}

	err := ConfigureViUsage(nil, rawR, rawW, usageCfg, triggerCfg, blockpack.NewPostgresFromPool(pool))
	require.NoError(t, err)

	before := testutil.ToFloat64(metricViBackfillStarted)

	rec := getViUsageRecorder()
	require.NotNil(t, rec)
	result, err := rec.RecordUse(context.Background(), "tenant-a", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill, "first recorded use must trigger per LeaseTTLSeconds' threshold")

	// Half 1: the durable row exists in backend_jobs.
	var (
		jobType, tenant, status string
		count                   int
	)
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-a'`)
		return row.Scan(&count) == nil && count == 1
	}, 5*time.Second, 10*time.Millisecond, "expected exactly one vi_backfill row for tenant-a")

	row := pool.QueryRow(context.Background(),
		`SELECT job_type, tenant, status FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-a'`)
	require.NoError(t, row.Scan(&jobType, &tenant, &status))
	assert.Equal(t, "vi_backfill", jobType)
	assert.Equal(t, "tenant-a", tenant)
	assert.Equal(t, "pending", status)

	// Half 2: the existing in-process goroutine still ran -- RunViBackfill increments
	// metricViBackfillStarted exactly when launchViBackfill's goroutine actually invokes it.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metricViBackfillStarted) > before
	}, 5*time.Second, 10*time.Millisecond, "the in-process backfill goroutine must still run alongside the durable insert")
}

// TestConfigureViUsage_OnShouldBackfill_CrossTypeSameNameColumns_EachGetsOwnRow is the
// trigger-point mutation-test guard for InsertViBackfill's dedup-key construction
// (§9 Phase 2's "Mutation-test guard"): two columns sharing the same tenant and column
// name but DIFFERENT column types must never collide on the same dedup_key -- each must
// get its own backend_jobs row. If ColumnType were dropped from the dedup key, the
// second insert would be wrongly absorbed by the first's non-terminal row.
func TestConfigureViUsage_OnShouldBackfill_CrossTypeSameNameColumns_EachGetsOwnRow(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	rawR, rawW := newLocalRawBackend(t)
	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}

	err := ConfigureViUsage(nil, rawR, rawW, usageCfg, triggerCfg, blockpack.NewPostgresFromPool(pool))
	require.NoError(t, err)

	rec := getViUsageRecorder()
	require.NotNil(t, rec)

	_, err = rec.RecordUse(context.Background(), "tenant-collision", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	_, err = rec.RecordUse(context.Background(), "tenant-collision", "span.custom.attr", "int", time.Now())
	require.NoError(t, err)

	var count int
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-collision'`)
		return row.Scan(&count) == nil && count == 2
	}, 5*time.Second, 10*time.Millisecond,
		"same-name, different-typed columns must each get their own backend_jobs row, got count=%d", count)
}

// TestConfigureViUsage_OnShouldBackfill_NilPgPool_SkipsInsertNoError is the regression guard
// for every non-Postgres deployment: pgPool == nil must not panic/error, and the existing
// goroutine-only behavior must be fully preserved.
func TestConfigureViUsage_OnShouldBackfill_NilPgPool_SkipsInsertNoError(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	rawR, rawW := newLocalRawBackend(t)
	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}

	err := ConfigureViUsage(nil, rawR, rawW, usageCfg, triggerCfg, nil)
	require.NoError(t, err)

	before := testutil.ToFloat64(metricViBackfillStarted)

	rec := getViUsageRecorder()
	require.NotNil(t, rec)
	assert.NotPanics(t, func() {
		result, recErr := rec.RecordUse(context.Background(), "tenant-b", "span.other.attr", "string", time.Now())
		require.NoError(t, recErr)
		require.True(t, result.ShouldBackfill)
	})

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metricViBackfillStarted) > before
	}, 5*time.Second, 10*time.Millisecond, "the in-process backfill goroutine must run unchanged when pgPool is nil")
}
