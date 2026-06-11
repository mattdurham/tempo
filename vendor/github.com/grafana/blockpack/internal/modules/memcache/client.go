package memcache

import (
	"context"

	gomemcache "github.com/grafana/gomemcache/memcache"
)

type client interface {
	Get(key string, opts ...gomemcache.Option) (*gomemcache.Item, error)
	// GetMulti fetches many keys in one request. gomemcache groups the keys
	// per server and pipelines them over a single connection per server, so a
	// batch of N keys destined for one server costs ONE connection acquisition
	// instead of N (NOTE-179).
	GetMulti(ctx context.Context, keys []string, opts ...gomemcache.Option) (map[string]*gomemcache.Item, error)
	Set(item *gomemcache.Item) error
	Close()
}
