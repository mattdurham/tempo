package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	common "github.com/grafana/tempo/tempodb/encoding/common"
)

// fakeManifestStore is a minimal in-memory manifestStore for tests -- it satisfies the
// exact Get/Put shape colhashmanifest.Store requires (and which toVICConsumerCfg's ms
// parameter accepts), mirroring blockpack's own
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
// when the caller passes nil (initValueIndexConsumer itself never does this in production
// since 2026-07-15 -- Postgres is a hard requirement -- but the pass-through is still worth
// locking in directly).
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
