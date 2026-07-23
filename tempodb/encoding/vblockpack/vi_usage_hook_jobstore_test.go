package vblockpack

// vi_usage_hook_jobstore_test.go — trigger-point wiring tests for ConfigureViUsage's
// onShouldBackfill closure inserting a durable job alongside (not instead of)
// launchViBackfill's existing in-process goroutine. Issue #522 (2026-07-21): retargeted
// from tempo's own now-retired backend_jobs/jobstore to blockpack's own compaction_jobs
// queue (pg.InsertViBackfillJob) -- newTestPostgresPool already applies every schema
// blockpack owns (including compaction_jobs), so no separate migration call is needed
// here anymore.
//
// seedLiveTraceBlock (issue #532, 2026-07-23): InsertViBackfillHistory now enumerates REAL
// blocks from blockpack_file_catalog instead of computing wall-clock windows analytically --
// a tenant with zero catalog rows correctly gets zero vi_backfill jobs regardless of how many
// times a column is used. Every test below that expects a durable insert must seed at least
// one live (uncompacted, undeleted) trace-subsystem catalog row first.

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedLiveTraceBlock inserts one live blockpack_file_catalog row for tenant so
// InsertViBackfillHistory's real block enumeration has something to find -- see this file's own
// package doc comment for why this is required after issue #532.
func seedLiveTraceBlock(t *testing.T, pool *pgxpool.Pool, tenant string, nowSec int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO blockpack_file_catalog (subsystem, tenant, resource_id, object_key, min_sec, max_sec)
		VALUES ('trace', $1, '', $2, $3, $4)`,
		tenant, tenant+"/00000000-0000-0000-0000-000000000001/data.blockpack", nowSec-60, nowSec)
	require.NoError(t, err)
}

// TestConfigureViUsage_OnShouldBackfill_InsertsPendingJobWhenPgPoolConfigured proves both
// halves of the "insert alongside, don't replace" design: a real Postgres pending
// vi_backfill row exists in blockpack's own compaction_jobs table AND the existing
// in-process goroutine (launchViBackfill -> RunViBackfill) still ran.
func TestConfigureViUsage_OnShouldBackfill_InsertsPendingJobWhenPgPoolConfigured(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	pool := newTestPostgresPool(t)

	rawR, rawW := newLocalRawBackend(t)
	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}

	seedLiveTraceBlock(t, pool, "tenant-a", time.Now().Unix())

	err := ConfigureViUsage(nil, rawR, rawW, usageCfg, triggerCfg, blockpack.NewPostgresFromPool(pool), 5*time.Minute)
	require.NoError(t, err)

	before := testutil.ToFloat64(metricViBackfillStarted)

	rec := getViUsageRecorder()
	require.NotNil(t, rec)
	result, err := rec.RecordUse(context.Background(), "tenant-a", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill, "first recorded use must trigger per LeaseTTLSeconds' threshold")

	// Half 1: the durable row exists in blockpack's own compaction_jobs table -- one per real
	// block in the file catalog overlapping the retention window (issue #532), not a synthetic
	// wall-clock window -- exactly one here since seedLiveTraceBlock seeded exactly one block.
	var (
		jobType, tenant, status string
		count                   int
	)
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM compaction_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-a'`)
		return row.Scan(&count) == nil && count == 1
	}, 5*time.Second, 10*time.Millisecond, "expected exactly one vi_backfill row for tenant-a's one seeded block")

	row := pool.QueryRow(context.Background(),
		`SELECT job_type, tenant, status FROM compaction_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-a' LIMIT 1`)
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
// trigger-point mutation-test guard for InsertViBackfill's dedup-key construction: two
// columns sharing the same tenant and column name but DIFFERENT column types must never
// collide on the same dedup_key -- each must get its own compaction_jobs row. If
// ColumnType were dropped from the dedup key, the second insert would be wrongly absorbed
// by the first's non-terminal row.
func TestConfigureViUsage_OnShouldBackfill_CrossTypeSameNameColumns_EachGetsOwnRow(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	pool := newTestPostgresPool(t)

	rawR, rawW := newLocalRawBackend(t)
	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}

	seedLiveTraceBlock(t, pool, "tenant-collision", time.Now().Unix())

	err := ConfigureViUsage(nil, rawR, rawW, usageCfg, triggerCfg, blockpack.NewPostgresFromPool(pool), 5*time.Minute)
	require.NoError(t, err)

	rec := getViUsageRecorder()
	require.NotNil(t, rec)

	_, err = rec.RecordUse(context.Background(), "tenant-collision", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	_, err = rec.RecordUse(context.Background(), "tenant-collision", "span.custom.attr", "int", time.Now())
	require.NoError(t, err)

	// Each column type gets its own set of block-shaped rows (issue #532) -- the dedup-key guard
	// this test proves is that BOTH types' rows exist independently (distinct column_type count
	// of 2), never collapsed onto one type's rows via a colliding dedup_key. Exactly one row per
	// type here since seedLiveTraceBlock seeded exactly one block.
	var distinctTypes int
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(DISTINCT column_type) FROM compaction_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-collision'`)
		return row.Scan(&distinctTypes) == nil && distinctTypes == 2
	}, 5*time.Second, 10*time.Millisecond,
		"same-name, different-typed columns must each get their own independent set of compaction_jobs rows, got distinct types=%d", distinctTypes)

	var stringCount, intCount int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM compaction_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-collision' AND column_type = 'string'`,
	).Scan(&stringCount))
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM compaction_jobs WHERE job_type = 'vi_backfill' AND tenant = 'tenant-collision' AND column_type = 'int'`,
	).Scan(&intCount))
	assert.Equal(t, 1, stringCount, "expected exactly one vi_backfill row for the string column's one seeded block")
	assert.Equal(t, 1, intCount, "expected exactly one vi_backfill row for the int column's one seeded block")
}

// TestConfigureViUsage_OnShouldBackfill_NilPgPool_SkipsInsertNoError is the regression guard
// for every non-Postgres deployment: pg == nil must not panic/error, and the existing
// goroutine-only behavior must be fully preserved.
func TestConfigureViUsage_OnShouldBackfill_NilPgPool_SkipsInsertNoError(t *testing.T) {
	prevRec := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prevRec) })

	rawR, rawW := newLocalRawBackend(t)
	usageCfg := blockpack.Config{DedicatedColumnsEnabled: true}
	triggerCfg := blockpack.TriggerConfig{LeaseTTLSeconds: 1800}

	err := ConfigureViUsage(nil, rawR, rawW, usageCfg, triggerCfg, nil, 5*time.Minute)
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
