package blockpack

import "github.com/prometheus/client_golang/prometheus"

// MemoryCacheConfig is a blockpack data type.
type MemoryCacheConfig struct {
	Registerer prometheus.Registerer
	MaxBytes   int64
}
