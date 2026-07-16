package vblockpack

// minio_client_config_creds_test.go — consistency fix (2026-07-14, follow-on to
// cube_backfill_s3config_test.go): cube_backfill.go's RunCubeBackfill/LoadCubeEntry built
// their own *minio.Client via credentials.NewEnvAWS() only, bypassing s3backend.Config's own
// AccessKey/SecretKey/SessionToken fields entirely. The SAME bug (independently, at each call
// site) existed in every other minio.Client construction in this package:
//
//   - valueindex.go's ConfigureValueIndex (via the former newMinioForValueIndex)
//   - vi_backfill.go's NewViBackfillDepsS3 (via the former newViBackfillMinioClient)
//   - cubemanager.go's ConfigureCubeManager (inline construction)
//   - cubequerypath.go's ConfigureCubeQueryPath (inline construction)
//   - vi_usage_hook.go's newViUsageObjectStoreForBackend (via the former
//     newViBackfillMinioClient -- a 5th call site sharing vi_backfill.go's helper, found
//     during this same fix rather than in the original bug report)
//
// All five (plus cube_backfill.go's own two) are now consolidated onto ONE shared helper,
// newMinioClientFromS3Config (valueindex.go) -- every construction site in this package lives
// in the same package with the same *s3backend.Config type, so per-location copies would have
// been pure duplication with no structural reason (different package, different Config type,
// import cycle) to keep them separate.
//
// Each test below proves ITS call site's real, production entry point authenticates using
// s3backend.Config's AccessKey/SecretKey against a fake S3 server that REJECTS any request not
// SigV4-signed with that exact key, with NO AWS_* environment variables set anywhere in this
// process -- mirroring cube_backfill_s3config_test.go's own
// TestE2E_RunCubeBackfill_UsesConfigCredentialsNotEnv proof. Falling back to
// credentials.NewEnvAWS() (the pre-fix behavior) would sign every request anonymously and the
// fake server would reject it with 403 before any of these assertions could pass.

import (
	"context"
	"sync"
	"testing"

	blockpack "github.com/grafana/blockpack"
	minio "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigureValueIndex_S3Path_UsesConfigCredentialsNotEnv proves ConfigureValueIndex's S3
// branch (valueindex.go) authenticates with s3cfg's own credentials, not the AWS environment.
func TestConfigureValueIndex_S3Path_UsesConfigCredentialsNotEnv(t *testing.T) {
	const (
		bucket    = "e2e-vi-config-creds-bucket"
		accessKey = "vi-config-only-access-key"
		secretKey = "vi-config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)

	valueIndexSinkMu.Lock()
	prevSink, prevPrefix, prevVcnt := valueIndexSink, valueIndexPrefix, vcntSink
	valueIndexSink, valueIndexPrefix, vcntSink = nil, "", nil
	valueIndexSinkMu.Unlock()
	valueIndexConfigOnce = sync.Once{}
	t.Cleanup(func() {
		valueIndexSinkMu.Lock()
		valueIndexSink, valueIndexPrefix, vcntSink = prevSink, prevPrefix, prevVcnt
		valueIndexSinkMu.Unlock()
	})

	ConfigureValueIndex(true, s3cfg, nil, "indexes")

	sink, prefix := getValueIndexSink()
	require.NotNil(t, sink, "ConfigureValueIndex must install a sink when s3cfg is set")
	assert.Equal(t, "indexes", prefix)

	err := sink.Put("e2e-vi-config-creds-tenant/indexes/probe.blockpack", []byte("probe"))
	require.NoError(t, err,
		"ConfigureValueIndex's sink must have authenticated with s3cfg.AccessKey/SecretKey "+
			"(no AWS_* env vars set); falling back to credentials.NewEnvAWS() would sign "+
			"anonymously and the fake server would reject the PUT with 403")
}

// TestNewViBackfillDepsS3_UsesConfigCredentialsNotEnv proves NewViBackfillDepsS3 (vi_backfill.go),
// the dependency bundle RunViBackfill/launchViBackfill use, authenticates its Putter/ObjStore
// with s3cfg's own credentials.
func TestNewViBackfillDepsS3_UsesConfigCredentialsNotEnv(t *testing.T) {
	const (
		bucket    = "e2e-vibackfill-config-creds-bucket"
		accessKey = "vibackfill-config-only-access-key"
		secretKey = "vibackfill-config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)

	deps, err := NewViBackfillDepsS3(s3cfg)
	require.NoError(t, err)
	require.NotNil(t, deps.Putter)
	require.NotNil(t, deps.ObjStore)

	putErr := deps.Putter.Put("e2e-vibackfill-config-creds-tenant/indexes/probe.blockpack", []byte("probe"))
	require.NoError(t, putErr,
		"deps.Putter must have authenticated with s3cfg.AccessKey/SecretKey (no AWS_* env vars "+
			"set); falling back to credentials.NewEnvAWS() would sign anonymously and the fake "+
			"server would reject the PUT with 403")

	// A real 404 (translated to blockpack.ErrNotFound) proves the GET was accepted/authenticated;
	// an env-fallback client would instead get a 403 AccessDenied here, a DIFFERENT error.
	_, _, getErr := deps.ObjStore.Get(context.Background(), "e2e-vibackfill-config-creds-tenant/viusage/index.json")
	assert.ErrorIs(t, getErr, blockpack.ErrNotFound)
}

// TestConfigureCubeManager_S3Path_UsesConfigCredentialsNotEnv proves ConfigureCubeManager's S3
// branch (cubemanager.go) authenticates with s3cfg's own credentials.
func TestConfigureCubeManager_S3Path_UsesConfigCredentialsNotEnv(t *testing.T) {
	withSavedCubeManager(t)

	const (
		bucket    = "e2e-cubemgr-config-creds-bucket"
		accessKey = "cubemgr-config-only-access-key"
		secretKey = "cubemgr-config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)

	ConfigureCubeManager(true, s3cfg, nil, nil, "e2e-cubemgr-config-creds-tenant", nil)

	cm := getCubeManager()
	require.NotNil(t, cm, "S3 path must configure a cube manager")

	putErr := cm.store.Put("e2e-cubemgr-config-creds-tenant/cubes/probe-cube/probe.cube", []byte("probe"))
	require.NoError(t, putErr,
		"cubeManager.store must have authenticated with s3cfg.AccessKey/SecretKey (no AWS_* env "+
			"vars set); falling back to credentials.NewEnvAWS() would sign anonymously and the "+
			"fake server would reject the PUT with 403")

	_, _, getErr := cm.objStore.Get(context.Background(), "e2e-cubemgr-config-creds-tenant/cubes/index.json")
	assert.ErrorIs(t, getErr, blockpack.CubeErrNotFound)
}

// TestConfigureCubeQueryPath_UsesConfigCredentialsNotEnv proves ConfigureCubeQueryPath
// (cubequerypath.go) authenticates with s3cfg's own credentials.
func TestConfigureCubeQueryPath_UsesConfigCredentialsNotEnv(t *testing.T) {
	processCubeQueryPathMu.Lock()
	prev := processCubeQueryPath
	processCubeQueryPath = nil
	processCubeQueryPathMu.Unlock()
	cubeQueryPathOnce = sync.Once{}
	t.Cleanup(func() {
		processCubeQueryPathMu.Lock()
		processCubeQueryPath = prev
		processCubeQueryPathMu.Unlock()
	})

	const (
		bucket    = "e2e-cqp-config-creds-bucket"
		accessKey = "cqp-config-only-access-key"
		secretKey = "cqp-config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)

	ConfigureCubeQueryPath(true, s3cfg, nil)

	cqp := getCubeQueryPath()
	require.NotNil(t, cqp, "ConfigureCubeQueryPath must install the process-level query path")

	// #508: cqp.listObjects (a thin wrapper around cqp.client.ListObjects) moved into
	// blockpack's own Lister/CubeFileStore adapters, no longer reachable from tempo test code.
	// cqp.client itself is still a real field on cubeQueryPath (kept for exactly this kind of
	// construction-site proof) -- listing through it directly, mirroring minioVIStore.List's own
	// channel-drain pattern, proves the SAME thing the old cqp.listObjects proxy call did: a real
	// bucket-listing GET through cqp.client, rejected with 403 by the fake server for anything
	// not signed with accessKey, so success here proves real authentication with s3cfg's own
	// credentials (no AWS_* env vars set anywhere in this test).
	var listErr error
	for obj := range cqp.client.ListObjects(context.Background(), cqp.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			listErr = obj.Err
			break
		}
	}
	require.NoError(t, listErr,
		"cqp.client must have authenticated with s3cfg.AccessKey/SecretKey; falling back to "+
			"credentials.NewEnvAWS() would sign anonymously and the fake server would reject the "+
			"request with 403")
}

// TestNewViUsageObjectStoreForBackend_S3Path_UsesConfigCredentialsNotEnv proves
// newViUsageObjectStoreForBackend's S3 branch (vi_usage_hook.go, shared by ConfigureViUsage)
// authenticates with s3cfg's own credentials. Found as a 5th affected call site during this fix
// (it shared vi_backfill.go's now-removed newViBackfillMinioClient), beyond the 4 originally
// reported.
func TestNewViUsageObjectStoreForBackend_S3Path_UsesConfigCredentialsNotEnv(t *testing.T) {
	const (
		bucket    = "e2e-viusage-config-creds-bucket"
		accessKey = "viusage-config-only-access-key"
		secretKey = "viusage-config-only-secret-key"
	)
	s3cfg := newFakeS3ConfigConfigCredsOnly(t, bucket, accessKey, secretKey)

	store, err := newViUsageObjectStoreForBackend(s3cfg, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, store)

	ctx := context.Background()
	const path = "e2e-viusage-config-creds-tenant/viusage/index.json"
	putErr := store.ConditionalPut(ctx, path, []byte(`{"probe":true}`), "")
	require.NoError(t, putErr,
		"viUsageObjectStore must have authenticated with s3cfg.AccessKey/SecretKey (no AWS_* env "+
			"vars set); falling back to credentials.NewEnvAWS() would sign anonymously and the "+
			"fake server would reject the PUT with 403")

	data, _, getErr := store.Get(ctx, path)
	require.NoError(t, getErr)
	assert.JSONEq(t, `{"probe":true}`, string(data))
}
