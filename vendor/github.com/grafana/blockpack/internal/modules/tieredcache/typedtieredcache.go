package tieredcache

import (
	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/prometheus/client_golang/prometheus"
)

// TypedTieredCache is a blockpack data type.
type TypedTieredCache struct {
	footer          filecache.Cache
	toc             filecache.Cache
	bloom           filecache.Cache
	metadata        filecache.Cache
	traceIdx        filecache.Cache
	block           filecache.Cache
	intrinsic       filecache.Cache
	sectionRequests *prometheus.CounterVec
	sectionCounters [numSections][3]prometheus.Counter
	sectionObs      [numSections][3]prometheus.Observer
}
