package vblockpack

// cube_backfill_s3config_test.go — two 2026-07-14 fixes to RunCubeBackfill/LoadCubeEntry,
// PLUS a 2026-07-15 migration (issue #504: cube registry is Postgres-only now, no
// blob/index.json fallback -- RunCubeBackfill's registry-persist side now goes through a
// real Postgres pool, not the S3-backed CubeRegistry these tests originally seeded/asserted
// against):
//
//  1. RunCubeBackfill previously had a void return, so a real failure (e.g. a
//     watermark-persist call against a cube ID never added to the registry) was silently
//     swallowed -- TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure proves it now
//     surfaces as a real error. The Postgres-dispatch-path proof (Store.Fail actually
//     getting called, with a retry scheduled) lives in
//     modules/backendworker/backend_jobs_e2e_test.go's
//     TestE2E_CubeBackfill_FailureThenReclaimSucceeds, since that requires
//     processCubeBackfillJobPostgres, an unexported method of a different package.
//  2. RunCubeBackfill/LoadCubeEntry built their own minio.Client via
//     credentials.NewEnvAWS() directly, bypassing s3backend.Config's own
//     AccessKey/SecretKey fields entirely -- TestE2E_RunCubeBackfill_UsesConfigCredentialsNotEnv
//     proves the fixed newCubeBackfillMinioClient authenticates using config-supplied
//     credentials alone, with no AWS_* environment variables set. The registry-persist side
//     of this test is now Postgres-backed (see #504 note above); only the VI-source-read
//     side still needs the fake S3/minio server, which is why this test stays in this file
//     rather than moving to a pure-Postgres one.

import (
	"context"
	"math"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure proves RunCubeBackfill's new error
// return actually carries a real failure: entry.CubeID has never been added to the real
// Postgres-backed registry (issue #504), so runCubeBackfillCore's very first progressFn
// callback (there is no VI data either, so processMinute itself succeeds trivially -- the
// registry-persist call is what genuinely fails) hits UpdateWatermarksEntry's real
// "cube ... not found" error, exactly the failure mode a crashed/inconsistent trigger would
// produce.
func TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure(t *testing.T) {
	s3cfg := newFakeS3Config(t, "e2e-cube-backfill-error-bucket")
	pgPool := newTestPostgresPool(t)
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-never-added",
		Tenant:     "e2e-cube-backfill-error-tenant",
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := RunCubeBackfill(ctx, entry, s3cfg, pgPool, math.MaxUint32, 0)
	require.Error(t, err, "RunCubeBackfill must surface a real registry-persist failure, not swallow it")
	assert.Contains(t, err.Error(), "not found")
}

// TestE2E_RunCubeBackfill_UsesConfigCredentialsNotEnv proves RunCubeBackfill/LoadCubeEntry
// authenticate using s3backend.Config's own AccessKey/SecretKey when set, not only the
// ambient AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables: the fake S3 server
// here rejects any request not SigV4-signed with a specific access key, and no such
// environment variables are set anywhere in this test. A real, authenticated watermark
// update landing in the (Postgres-backed, issue #504) registry is the proof -- if
// RunCubeBackfill fell back to credentials.NewEnvAWS() (pre-fix behavior) for its VI-source
// read side, every request would sign anonymously (no Authorization header, since no AWS_*
// env vars exist in this process) and the fake server would reject it with 403 before any
// per-minute progress (and therefore any watermark) could ever be persisted.
func TestE2E_RunCubeBackfill_UsesConfigCredentialsNotEnv(t *testing.T) {
	const (
		bucket    = "e2e-cube-config-creds-bucket"
		accessKey = "config-only-access-key"
		secretKey = "config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)
	pgPool := newTestPostgresPool(t)

	tenant := "e2e-cube-config-creds-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-config-creds-cube",
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}

	registry := blockpack.NewPostgresFromPool(pgPool).CubeRegistry(tenant)
	require.NoError(t, registry.Add(context.Background(), entry))

	// RunCubeBackfill's own window is unbounded (math.MaxUint32 minutes), so this never
	// reaches Done=true in test time -- bound the ctx and only assert real partial
	// progress, mirroring TestE2E_CubeBackfill_WorkerClaimsAndExecutesWithoutGRPC
	// (modules/backendworker/backend_jobs_e2e_test.go)'s identical accommodation.
	boundedCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = RunCubeBackfill(boundedCtx, entry, s3cfg, pgPool, math.MaxUint32, 0)

	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok,
		"RunCubeBackfill must have authenticated with s3cfg.AccessKey/SecretKey (no AWS_* env vars set) and persisted at least one real watermark update")
	assert.Greater(t, wm.MaxMinute, uint32(0))
}
