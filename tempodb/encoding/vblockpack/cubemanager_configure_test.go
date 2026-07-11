package vblockpack

// cubemanager_configure_test.go — unit tests for ConfigureCubeManager's generic (non-S3)
// backend path (plan.md §9 item 6, §15 S3-unchanged verification).

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withSavedCubeManager snapshots processCubeManager and resets cubeManagerOnce so
// ConfigureCubeManager can be re-exercised per test, restoring the prior process-level state
// on cleanup.
func withSavedCubeManager(t *testing.T) {
	t.Helper()
	processCubeManagerMu.Lock()
	prev := processCubeManager
	processCubeManager = nil
	processCubeManagerMu.Unlock()
	cubeManagerOnce = sync.Once{}
	t.Cleanup(func() {
		processCubeManagerMu.Lock()
		processCubeManager = prev
		processCubeManagerMu.Unlock()
	})
}

func TestConfigureCubeManager_DisabledIsNoop(t *testing.T) {
	withSavedCubeManager(t)

	ConfigureCubeManager(false, nil, nil, nil, "tenant-x", nil)
	assert.Nil(t, getCubeManager(), "disabled config must leave the manager unset")
}

func TestConfigureCubeManager_NoBackendConfiguredIsNoop(t *testing.T) {
	withSavedCubeManager(t)

	// enabled=true but neither S3 nor a generic rawR/rawW backend supplied.
	ConfigureCubeManager(true, nil, nil, nil, "tenant-x", nil)
	assert.Nil(t, getCubeManager(), "no backend configured must leave the manager unset")
}

func TestConfigureCubeManager_GenericBackendUsesRawStores(t *testing.T) {
	withSavedCubeManager(t)

	rawR, rawW := newLocalRawBackend(t)
	ConfigureCubeManager(true, nil, rawR, rawW, "tenant-x", nil)

	cm := getCubeManager()
	require.NotNil(t, cm, "generic backend path must configure a cube manager")
	assert.Equal(t, "tenant-x", cm.tenant)

	_, ok := cm.store.(*rawObjectPutter)
	assert.True(t, ok, "expected the generic path to install a *rawObjectPutter store, got %T", cm.store)

	_, ok = cm.objStore.(*rawCubeObjectStore)
	assert.True(t, ok, "expected a *rawCubeObjectStore objStore for a local.Backend (no GCS capability), got %T", cm.objStore)
}
