package vblockpack

// rawobjectputter_test.go — Track C (backend-agnostic VI/cube usage-recording +
// backfill machinery, plan.md §8 item 7) unit tests for rawobjectputter.go's
// unconditional Put wrapper over backend.RawWriter.

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/require"
)

func TestRawObjectPutter_Put_WritesFullKeySplitIntoNameAndKeyPath(t *testing.T) {
	ctx := context.Background()
	rawR, rawW := newLocalRawBackend(t)
	putter := newRawObjectPutter(rawW)

	key := "tenant1/indexes/colhash1/string/L0-1-2-a.blockpack"
	data := []byte("value index file contents")

	require.NoError(t, putter.Put(key, data))

	name, keypath := splitKeyForRaw(key)
	rc, size, err := rawR.Read(ctx, name, keypath, nil)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	require.Equal(t, int64(len(data)), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// erroringRawWriter satisfies backend.RawWriter with only Write overridden, returning err
// unconditionally — the other methods are never called by rawObjectPutter.Put and are left
// as a nil embedded interface (a panic if ever invoked would itself indicate a test bug).
type erroringRawWriter struct {
	backend.RawWriter
	err error
}

func (w *erroringRawWriter) Write(context.Context, string, backend.KeyPath, io.Reader, int64, *backend.CacheInfo) error {
	return w.err
}

func TestRawObjectPutter_Put_PropagatesWriteError(t *testing.T) {
	wantErr := errors.New("boom")
	putter := newRawObjectPutter(&erroringRawWriter{err: wantErr})

	err := putter.Put("tenant1/indexes/colhash1/string/L0-1-2-a.blockpack", []byte("data"))
	require.ErrorIs(t, err, wantErr)
}
