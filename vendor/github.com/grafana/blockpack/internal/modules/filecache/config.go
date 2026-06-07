package filecache

import "github.com/prometheus/client_golang/prometheus"

// Config is a blockpack data type.
type Config struct {
	Registerer prometheus.Registerer
	Path       string
	MaxBytes   int64
	Enabled    bool
}
