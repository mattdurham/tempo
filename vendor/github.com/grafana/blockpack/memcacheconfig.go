package blockpack

import "github.com/prometheus/client_golang/prometheus"

// MemCacheConfig is a blockpack data type.
type MemCacheConfig struct {
	Registerer prometheus.Registerer
	TierLabel  string
	Servers    []string
	Expiration int32
	Enabled    bool
}
