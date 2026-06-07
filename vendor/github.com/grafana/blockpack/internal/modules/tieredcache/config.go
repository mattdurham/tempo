package tieredcache

import (
	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/prometheus/client_golang/prometheus"
)

// Config is a blockpack data type.
type Config struct {
	Metadata   filecache.Cache
	Data       filecache.Cache
	Registerer prometheus.Registerer
}
