package vblockpack

// minio_notfound_test.go -- regression test for a real, live-confirmed bug: minio-go's
// GetObject is lazy (verified directly against vendor/github.com/minio/minio-go/v7/
// api-get-object.go -- the real HTTP GET only fires on the first Read/ReadAt, GetObject
// itself only validates bucket/object name syntax), so a nonexistent key's real 404
// error surfaces from io.ReadAll(obj), not from the GetObject call. viUsageObjectStore.Get
// and minioObjectStore.Get each had their NoSuchKey/404 classification applied ONLY to
// GetObject's own error, silently leaking the raw, untranslated minio error through
// io.ReadAll instead of the expected blockpack.ErrNotFound/CubeErrNotFound sentinel --
// live-confirmed on tempo-dev-test-03 (debug trace showed
// `err="viusage registry: load: The specified key does not exist."` on every tenant's
// first-ever registry access), meaning the registry file was NEVER created and no column
// ever crossed the trigger threshold for the entire lifetime of a tenant that had not
// already had a registry created some other way.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
)

// newNoSuchKeyMinioClient returns a real *minio.Client pointed at a test server that
// responds to every request with a real S3-shaped 404 NoSuchKey error -- exercising the
// SAME lazy-GetObject-then-fail-on-Read path a genuinely nonexistent object hits in
// production, not a hand-rolled fake that would bypass the exact bug this test guards.
func newNoSuchKeyMinioClient(t *testing.T) *minio.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>x</Key><RequestId>x</RequestId><HostId>x</HostId></Error>`))
	}))
	t.Cleanup(server.Close)

	client, err := minio.New(server.URL[len("http://"):], &minio.Options{
		Creds:  credentials.NewStaticV4("test", "test", ""),
		Secure: false,
		Region: "us-east-1",
	})
	require.NoError(t, err)
	return client
}

func TestViUsageObjectStore_Get_NonexistentKey_ReturnsErrNotFound(t *testing.T) {
	store := &viUsageObjectStore{client: newNoSuchKeyMinioClient(t), bucket: "test-bucket"}

	_, _, err := store.Get(context.Background(), "tenant/viusage/index.json")

	require.Error(t, err)
	assert.ErrorIs(t, err, blockpack.ErrNotFound,
		"a nonexistent registry key must return blockpack.ErrNotFound (the real 404 surfaces "+
			"from io.ReadAll, not from GetObject itself -- minio-go's GetObject is lazy) so "+
			"Registry.Load can distinguish a genuine first-time miss from a real failure; "+
			"returning the raw, untranslated minio error instead silently prevents the "+
			"registry from ever being created")
}

func TestMinioObjectStore_Get_NonexistentKey_ReturnsCubeErrNotFound(t *testing.T) {
	store := &minioObjectStore{client: newNoSuchKeyMinioClient(t), bucket: "test-bucket"}

	_, _, err := store.Get(context.Background(), "tenant/cubes/index.json")

	require.Error(t, err)
	assert.ErrorIs(t, err, blockpack.CubeErrNotFound,
		"a nonexistent cube registry key must return blockpack.CubeErrNotFound for the same "+
			"reason as viUsageObjectStore.Get -- see that test's doc comment")
}
