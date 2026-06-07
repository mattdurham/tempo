package chaincache

import (
	"github.com/grafana/blockpack/internal/modules/filecache"
	"golang.org/x/sync/singleflight"
)

// ChainedCache is a blockpack data type.
type ChainedCache struct {
	group singleflight.Group
	tiers []filecache.Cache
}
