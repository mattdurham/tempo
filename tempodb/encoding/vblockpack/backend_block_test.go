package vblockpack

import (
	"testing"

	"github.com/grafana/blockpack"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestOpenMemCache_TierLabelAvoidsRegistrationCollision guards against the #515
// Phase 1 regression: prometheus.WrapRegistererWith(Labels{"tier": ...}) adds
// "tier" as a *constant* label, which collides with MemCache's own "tier"
// *variable* label at registration time. The vendored memcacheRegisterOrReuse
// helper only special-cases AlreadyRegisteredError, so the collision error is
// silently swallowed and the metric family never actually registers with the
// Registerer, even though the in-process counter keeps working. Using
// MemCacheConfig.TierLabel instead keeps "tier" a plain variable label with no
// collision. This test reproduces both shapes against real registries and
// confirms only the TierLabel shape's metrics survive Gather().
func TestOpenMemCache_TierLabelAvoidsRegistrationCollision(t *testing.T) {
	oldReg := prometheus.NewRegistry()
	oldMC, err := blockpack.OpenMemCache(blockpack.MemCacheConfig{
		Servers:    []string{"127.0.0.1:1"},
		Enabled:    true,
		Registerer: prometheus.WrapRegistererWith(prometheus.Labels{"tier": "metadata"}, oldReg),
	})
	if err != nil || oldMC == nil {
		t.Fatalf("OpenMemCache (old WrapRegistererWith shape) = %v, %v; want non-nil, nil", oldMC, err)
	}
	_, _, _ = oldMC.Get("k") // connection refused -> increments the (never-registered) errs counter

	oldFamilies, err := oldReg.Gather()
	if err != nil {
		t.Fatalf("Gather (old shape): %v", err)
	}
	if hasMetricFamily(oldFamilies, "blockpack_cache_errors_total") {
		t.Fatalf("blockpack_cache_errors_total unexpectedly present after Gather() with the old " +
			"WrapRegistererWith shape — the registration collision this test guards against may no " +
			"longer reproduce; re-check whether this test is still exercising the right mechanism")
	}

	newReg := prometheus.NewRegistry()
	newMC, err := blockpack.OpenMemCache(blockpack.MemCacheConfig{
		Servers:    []string{"127.0.0.1:1"},
		Enabled:    true,
		Registerer: newReg,
		TierLabel:  "metadata",
	})
	if err != nil || newMC == nil {
		t.Fatalf("OpenMemCache (new TierLabel shape) = %v, %v; want non-nil, nil", newMC, err)
	}
	_, _, _ = newMC.Get("k")

	newFamilies, err := newReg.Gather()
	if err != nil {
		t.Fatalf("Gather (new shape): %v", err)
	}
	if !hasMetricFamily(newFamilies, "blockpack_cache_errors_total") {
		t.Fatalf("blockpack_cache_errors_total missing after Gather() with the TierLabel shape — " +
			"the #515 Phase 1 fix should register this metric family successfully")
	}
}

func hasMetricFamily(mfs []*dto.MetricFamily, name string) bool {
	for _, mf := range mfs {
		if mf.GetName() == name {
			return true
		}
	}
	return false
}

// TestMetaAndDataMemCacheConfigs_UsesTierLabelNotWrapRegistererWith directly
// regression-guards getCache()'s actual call site (backend_block.go's
// metaAndDataMemCacheConfigs), not just the general TierLabel-vs-
// WrapRegistererWith mechanism in isolation. getCache() itself is memoized
// behind a package-level sync.Once, making it impractical to exercise
// end-to-end in a unit test — this pure helper was extracted specifically so
// the exact config values getCache() builds can be asserted directly. A
// regression back to Registerer: prometheus.WrapRegistererWith(Labels{"tier":
// ...}, ...) at this call site — the #515 Phase 1 bug — would fail this test.
func TestMetaAndDataMemCacheConfigs_UsesTierLabelNotWrapRegistererWith(t *testing.T) {
	cfg := blockpackCacheConfig{
		metadataMemServers: []string{"metadata-instance:11211"},
		memServers:         []string{"data-instance:11211"},
	}

	metaCfg, dataCfg := metaAndDataMemCacheConfigs(cfg)

	if metaCfg.TierLabel != "metadata" {
		t.Errorf("metaCfg.TierLabel = %q, want %q", metaCfg.TierLabel, "metadata")
	}
	if dataCfg.TierLabel != "data" {
		t.Errorf("dataCfg.TierLabel = %q, want %q", dataCfg.TierLabel, "data")
	}
	if metaCfg.Registerer != prometheus.DefaultRegisterer {
		t.Errorf("metaCfg.Registerer = %v, want prometheus.DefaultRegisterer — a wrapped registerer "+
			"here (e.g. WrapRegistererWith) would reintroduce the #515 Phase 1 tier-label collision", metaCfg.Registerer)
	}
	if dataCfg.Registerer != prometheus.DefaultRegisterer {
		t.Errorf("dataCfg.Registerer = %v, want prometheus.DefaultRegisterer — a wrapped registerer "+
			"here (e.g. WrapRegistererWith) would reintroduce the #515 Phase 1 tier-label collision", dataCfg.Registerer)
	}
	if len(metaCfg.Servers) != 1 || metaCfg.Servers[0] != "metadata-instance:11211" {
		t.Errorf("metaCfg.Servers = %v, want [metadata-instance:11211]", metaCfg.Servers)
	}
	if len(dataCfg.Servers) != 1 || dataCfg.Servers[0] != "data-instance:11211" {
		t.Errorf("dataCfg.Servers = %v, want [data-instance:11211]", dataCfg.Servers)
	}
}
