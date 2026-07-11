package vblockpack

// value_index_query_raw_test.go — unit tests for ConfigureValueIndexQueryRaw
// (plan.md §9 item 7), the non-S3 sibling of ConfigureValueIndexQuery.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withSavedViQueryReader snapshots viQueryReaderPtr and restores it on cleanup, so tests
// that call ConfigureValueIndexQueryRaw directly don't leak process-level singleton state
// into other tests in this package.
func withSavedViQueryReader(t *testing.T) {
	t.Helper()
	viQueryReaderMu.Lock()
	prev := viQueryReaderPtr
	viQueryReaderMu.Unlock()
	t.Cleanup(func() {
		viQueryReaderMu.Lock()
		viQueryReaderPtr = prev
		viQueryReaderMu.Unlock()
	})
}

func TestConfigureValueIndexQueryRaw_InstallsReaderBackedByRawFileStore(t *testing.T) {
	withSavedViQueryReader(t)

	ctx := context.Background()
	rawR, rawW := newLocalRawBackend(t)

	key := "tenant1/indexes/colhash/string/L0-1-2-a.blockpack"
	data := []byte("value index contents")
	name, keypath := splitKeyForRaw(key)
	require.NoError(t, rawW.Write(ctx, name, keypath, bytes.NewReader(data), int64(len(data)), nil))

	ConfigureValueIndexQueryRaw(rawR, "indexes", time.Minute, 0)

	viQueryReaderMu.RLock()
	reader := viQueryReaderPtr
	viQueryReaderMu.RUnlock()
	require.NotNil(t, reader)
	assert.Equal(t, "indexes", reader.indexPrefix)

	got, err := reader.store.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestConfigureValueIndexQueryRaw_DefaultsEmptyIndexPrefix(t *testing.T) {
	withSavedViQueryReader(t)

	rawR, _ := newLocalRawBackend(t)
	ConfigureValueIndexQueryRaw(rawR, "", time.Minute, 0)

	viQueryReaderMu.RLock()
	defer viQueryReaderMu.RUnlock()
	require.NotNil(t, viQueryReaderPtr)
	assert.Equal(t, "indexes", viQueryReaderPtr.indexPrefix)
}

func TestConfigureValueIndexQueryRaw_NilRawRDisablesReader(t *testing.T) {
	withSavedViQueryReader(t)

	viQueryReaderMu.Lock()
	viQueryReaderPtr = &viQueryReader{} // sentinel non-nil, to prove nil rawR clears it
	viQueryReaderMu.Unlock()

	ConfigureValueIndexQueryRaw(nil, "indexes", time.Minute, 0)

	viQueryReaderMu.RLock()
	defer viQueryReaderMu.RUnlock()
	assert.Nil(t, viQueryReaderPtr)
}
