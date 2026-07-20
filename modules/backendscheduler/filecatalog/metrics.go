package filecatalog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metricUnexpectedNonVblockpackBlocks is issue #522 #159's cheap safety-net sanity check: the
// poller flip's deployment-level gate (cfg.Block.Version == "vblockpack") assumes a tenant never
// has a genuine mix of vblockpack and non-vblockpack blocks -- confirmed true for this
// deployment, but if that assumption is ever violated in the future (a new onboarding path, a
// config change mid-migration), leftover non-vblockpack blocks would silently become invisible
// to the poller once it stops calling the real backend LIST for a tenant. This counter (and the
// accompanying log line in reconcileTenant) makes a violation loud and observable rather than a
// silent data-visibility regression, at zero extra I/O cost: reconcileTenant already has every
// tenant's full, already-fetched BlockMetas in hand for its own vblockpack-only filter.
var metricUnexpectedNonVblockpackBlocks = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "tempodb",
	Name:      "filecatalog_unexpected_nonvblockpack_blocks_total",
	Help: "Total number of non-vblockpack-encoded blocks observed for a tenant while reconciling " +
		"file_catalog (issue #522 #159). Expected to always be zero -- this deployment's poller " +
		"flip assumes no tenant ever mixes encodings; a nonzero value means that assumption has " +
		"been violated and those blocks are at risk of becoming invisible to the poller.",
}, []string{"tenant"})
