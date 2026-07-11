package vblockpack

import (
	"bytes"
	"context"
	"errors"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawFileStore_List_ReturnsFullKeysRecursively(t *testing.T) {
	ctx := context.Background()
	rawR, rawW, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)

	write := func(keypath backend.KeyPath, name string, data []byte) {
		require.NoError(t, rawW.Write(ctx, name, keypath, bytes.NewReader(data), int64(len(data)), nil))
	}

	write(backend.KeyPath{"tenant-a", "vi", "colhash1", "string"}, "L0-1-2-a.blockpack", []byte("aaa"))
	write(backend.KeyPath{"tenant-a", "vi", "colhash1", "string"}, "L0-3-4-b.blockpack", []byte("bbbb"))
	write(backend.KeyPath{"tenant-a", "vi", "colhash2", "int"}, "L0-5-6-c.blockpack", []byte("cc"))

	store := newRawFileStore(rawR)

	keys, err := store.List(ctx, "tenant-a/vi/colhash1/string/")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"tenant-a/vi/colhash1/string/L0-1-2-a.blockpack",
		"tenant-a/vi/colhash1/string/L0-3-4-b.blockpack",
	}, keys)
}

// TestRawFileStore_List_NonexistentPrefixReturnsEmptyNotError is the regression test for a
// real bug found by the backend-agnostic VI/cube integration test
// (modules/frontend/vi_backfill_local_integration_test.go): List(ctx, prefix) for a prefix
// whose directory was never created (e.g. a column that has never been indexed yet -- the
// exact "no coverage" case RecordUsageIfNoIndexCoverage exists to detect) must return an
// EMPTY list with a nil error, mirroring S3's minioVIStore.List semantics (S3 has no real
// directory concept, so listing a nonexistent prefix naturally yields zero objects, never an
// error). local.Backend.Find's underlying fs.WalkDir/os.DirFS call returns a real
// "no such file or directory" error for a nonexistent path (verified directly, not assumed),
// which List previously propagated unchanged -- RecordUsageIfNoIndexCoverage's
// `if err != nil || len(files) > 0 { continue }` then incorrectly treated that error the
// SAME as "genuine coverage found", silently skipping usage recording for every
// never-indexed column on Local (and any other fs.WalkDir-backed RawReader), a correctness
// gap invisible to every prior test in this file (all of which List an EXISTING prefix).
func TestRawFileStore_List_NonexistentPrefixReturnsEmptyNotError(t *testing.T) {
	ctx := context.Background()
	rawR, _, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)

	store := newRawFileStore(rawR)

	keys, err := store.List(ctx, "tenant-never-indexed/vi/some-colhash/string/")
	require.NoError(t, err, "a nonexistent prefix must not surface as an error -- it means zero files, not a failure")
	assert.Empty(t, keys)
}

func TestRawFileStore_GetSizeReadAt_RoundTrip(t *testing.T) {
	ctx := context.Background()
	rawR, rawW, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)

	data := []byte("hello value index contents")
	keypath := backend.KeyPath{"tenant-b", "vi", "colhash", "string"}
	name := "L0-1-2-x.blockpack"
	require.NoError(t, rawW.Write(ctx, name, keypath, bytes.NewReader(data), int64(len(data)), nil))

	store := newRawFileStore(rawR)
	fullKey := "tenant-b/vi/colhash/string/L0-1-2-x.blockpack"

	got, err := store.Get(ctx, fullKey)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	size, err := store.Size(fullKey)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), size)

	buf := make([]byte, 5)
	n, err := store.ReadAt(fullKey, buf, 6)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, data[6:11], buf)
}

func TestRawFileStore_MapsNotFoundToValueIndexFileNotFound(t *testing.T) {
	ctx := context.Background()
	rawR, _, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)

	store := newRawFileStore(rawR)
	missingKey := "tenant-c/vi/colhash/string/does-not-exist.blockpack"

	_, err = store.Get(ctx, missingKey)
	assert.True(t, errors.Is(err, blockpack.ErrValueIndexFileNotFound))

	_, err = store.Size(missingKey)
	assert.True(t, errors.Is(err, blockpack.ErrValueIndexFileNotFound))

	buf := make([]byte, 4)
	_, err = store.ReadAt(missingKey, buf, 0)
	assert.True(t, errors.Is(err, blockpack.ErrValueIndexFileNotFound))
}
