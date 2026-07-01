package tieredcache

import (
	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/prometheus/client_golang/prometheus"
)

// TypedConfig is a blockpack data type.
type TypedConfig struct {
	Footer     filecache.Cache
	TOC        filecache.Cache
	Bloom      filecache.Cache
	Metadata   filecache.Cache
	TraceIdx   filecache.Cache
	Block      filecache.Cache
	Registerer prometheus.Registerer
}
