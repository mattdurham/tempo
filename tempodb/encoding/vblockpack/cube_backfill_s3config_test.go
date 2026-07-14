package vblockpack

// cube_backfill_s3config_test.go — two 2026-07-14 fixes to RunCubeBackfill/LoadCubeEntry:
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
//     credentials alone, with no AWS_* environment variables set.

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure proves RunCubeBackfill's new error
// return actually carries a real failure: entry.CubeID has never been added to the real
// S3-backed registry, so runCubeBackfillCore's very first progressFn callback (there is no
// VI data either, so processMinute itself succeeds trivially -- the registry-persist call
// is what genuinely fails) hits UpdateWatermarks' real "cube ... not found" error, exactly
// the failure mode a crashed/inconsistent trigger would produce.
func TestE2E_RunCubeBackfill_ReturnsErrorOnRegistryFailure(t *testing.T) {
	s3cfg := newFakeS3Config(t, "e2e-cube-backfill-error-bucket")
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-never-added",
		Tenant:     "e2e-cube-backfill-error-tenant",
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := RunCubeBackfill(ctx, entry, s3cfg)
	require.Error(t, err, "RunCubeBackfill must surface a real registry-persist failure, not swallow it")
	assert.Contains(t, err.Error(), "not found")
}

// TestE2E_RunCubeBackfill_UsesConfigCredentialsNotEnv proves RunCubeBackfill/LoadCubeEntry
// authenticate using s3backend.Config's own AccessKey/SecretKey when set, not only the
// ambient AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables: the fake S3 server
// here rejects any request not SigV4-signed with a specific access key, and no such
// environment variables are set anywhere in this test. A real, authenticated watermark
// update landing in the registry is the proof -- if RunCubeBackfill fell back to
// credentials.NewEnvAWS() (pre-fix behavior), every request would sign anonymously (no
// Authorization header, since no AWS_* env vars exist in this process) and the fake server
// would reject it with 403 before any watermark could ever be persisted.
func TestE2E_RunCubeBackfill_UsesConfigCredentialsNotEnv(t *testing.T) {
	const (
		bucket    = "e2e-cube-config-creds-bucket"
		accessKey = "config-only-access-key"
		secretKey = "config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)

	tenant := "e2e-cube-config-creds-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-config-creds-cube",
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}

	// Seed the registry entry via an independently-constructed client using the SAME
	// config-only credentials (not RunCubeBackfill's own client), proving the fake
	// server's access-key check itself is live and not accidentally bypassed.
	seedClient, err := minio.New(s3cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
		Region: s3cfg.Region,
	})
	require.NoError(t, err)
	registry := blockpack.NewCubeRegistry(&minioObjectStore{client: seedClient, bucket: bucket}, tenant)
	require.NoError(t, registry.Add(context.Background(), entry))

	// RunCubeBackfill's own window is unbounded (math.MaxUint32 minutes), so this never
	// reaches Done=true in test time -- bound the ctx and only assert real partial
	// progress, mirroring TestE2E_CubeBackfill_WorkerClaimsAndExecutesWithoutGRPC
	// (modules/backendworker/backend_jobs_e2e_test.go)'s identical accommodation.
	boundedCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = RunCubeBackfill(boundedCtx, entry, s3cfg)

	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok,
		"RunCubeBackfill must have authenticated with s3cfg.AccessKey/SecretKey (no AWS_* env vars set) and persisted at least one real watermark update")
	assert.Greater(t, wm.MaxMinute, uint32(0))
}
