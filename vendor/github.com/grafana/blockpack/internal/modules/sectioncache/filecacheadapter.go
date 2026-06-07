package sectioncache

import "github.com/grafana/blockpack/internal/modules/filecache"

// FilecacheAdapter is a blockpack data type.
type FilecacheAdapter struct {
	cache filecache.Cache
}
