package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	common "github.com/grafana/tempo/tempodb/encoding/common"
)

// fakeManifestStore is a minimal in-memory manifestStore for tests -- it satisfies the
// exact Get/Put shape colhashmanifest.Store requires (and which toVICConsumerCfg /
// toVCNTCompactorCfg's ms parameter accepts), mirroring blockpack's own
// internal/modules/{valueindexconsumer,valuecountscompactor} manifest_hook_test.go fakes.
type fakeManifestStore struct {
	objects map[string][]byte
}

func (f *fakeManifestStore) Get(_ context.Context, key string) ([]byte, error) {
	return f.objects[key], nil
}

func (f *fakeManifestStore) Put(_ context.Context, key string, data []byte) error {
	f.objects[key] = data
	return nil
}

// TestToVICConsumerCfg_ManifestStore confirms toVICConsumerCfg (issue #507) actually wires
// the supplied manifestStore into vicconsumer.Config.ManifestStore, and that it is left nil
// when the caller passes nil (the "manifest recording disabled" case initValueIndexConsumer
// falls into when neither Postgres nor an object store is configured for it).
func TestToVICConsumerCfg_ManifestStore(t *testing.T) {
	cfg := common.ValueIndexConsumerConfig{Enabled: true}

	t.Run("non-nil manifest store is wired through", func(t *testing.T) {
		ms := &fakeManifestStore{objects: map[string][]byte{}}
		out := toVICConsumerCfg(cfg, ms)
		assert.NotNil(t, out.ManifestStore)
		assert.Same(t, ms, out.ManifestStore)
	})

	t.Run("nil manifest store stays nil", func(t *testing.T) {
		out := toVICConsumerCfg(cfg, nil)
		assert.Nil(t, out.ManifestStore)
	})
}

// TestToVCNTCompactorCfg_ManifestStore is the VCNT-compactor counterpart of
// TestToVICConsumerCfg_ManifestStore (issue #507).
func TestToVCNTCompactorCfg_ManifestStore(t *testing.T) {
	cfg := common.ValueCountCompactorConfig{Enabled: true}

	t.Run("non-nil manifest store is wired through", func(t *testing.T) {
		ms := &fakeManifestStore{objects: map[string][]byte{}}
		out := toVCNTCompactorCfg(cfg, ms)
		assert.NotNil(t, out.ManifestStore)
		assert.Same(t, ms, out.ManifestStore)
	})

	t.Run("nil manifest store stays nil", func(t *testing.T) {
		out := toVCNTCompactorCfg(cfg, nil)
		assert.Nil(t, out.ManifestStore)
	})
}

// TestTempoVCCStore_SatisfiesManifestStore locks in the structural-typing decision behind
// issue #507's blob-backed fallback: tempoVCCStore (the existing S3-backed VI/VCNT compactor
// object store) is used directly as the ManifestStore when no Postgres pool is configured, with
// no wrapper type and no blockpack change. If tempoVCCStore's Get/Put signatures ever drift
// from colhashmanifest.Store's shape, this assignment stops compiling.
func TestTempoVCCStore_SatisfiesManifestStore(t *testing.T) {
	var _ manifestStore = &tempoVCCStore{}
}
