package blockpack

import "github.com/prometheus/client_golang/prometheus"

// FileCacheConfig is a blockpack data type.
type FileCacheConfig struct {
	Registerer prometheus.Registerer
	Path       string
	MaxBytes   int64
	Enabled    bool
}
