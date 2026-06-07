package memcache

import "github.com/prometheus/client_golang/prometheus"

// Config is a blockpack data type.
type Config struct {
	Registerer prometheus.Registerer
	TierLabel  string
	Servers    []string
	Expiration int32
	Enabled    bool
}
