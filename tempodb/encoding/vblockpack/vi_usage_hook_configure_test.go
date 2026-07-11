package vblockpack

// vi_usage_hook_configure_test.go — unit tests for newViUsageObjectStoreForBackend and
// ConfigureViUsage's own backend-selection logic (plan.md §9 item 4, §17 edge case: a
// config/wiring bug that supplies neither S3 nor a generic raw backend must return a clear
// error, not silently construct a store that would nil-panic on first real use).

import (
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewViUsageObjectStoreForBackend_NoBackendConfigured_ReturnsError(t *testing.T) {
	store, err := newViUsageObjectStoreForBackend(nil, nil, nil)
	assert.Error(t, err, "neither S3 nor a generic raw backend supplied must be a clear error, not a nil-panic waiting to happen")
	assert.Nil(t, store)
}

func TestNewViUsageObjectStoreForBackend_GenericBackendUsesRawObjectStore(t *testing.T) {
	rawR, rawW := newLocalRawBackend(t)
	store, err := newViUsageObjectStoreForBackend(nil, rawR, rawW)
	require.NoError(t, err)
	require.NotNil(t, store)
	_, ok := store.(*rawObjectStore)
	assert.True(t, ok, "expected *rawObjectStore for a local.Backend (no GCS capability), got %T", store)
}

func TestConfigureViUsage_NoBackendConfigured_ReturnsError(t *testing.T) {
	err := ConfigureViUsage(nil, nil, nil, blockpack.Config{DedicatedColumnsEnabled: true}, blockpack.TriggerConfig{})
	assert.Error(t, err)
}

func TestConfigureViUsage_DedicatedColumnsDisabled_NoopEvenWithNoBackend(t *testing.T) {
	prev := getViUsageRecorder()
	t.Cleanup(func() { ConfigureViUsageRecorder(prev) })

	err := ConfigureViUsage(nil, nil, nil, blockpack.Config{DedicatedColumnsEnabled: false}, blockpack.TriggerConfig{})
	require.NoError(t, err, "disabled config must not attempt backend construction at all")
	assert.Nil(t, getViUsageRecorder())
}
