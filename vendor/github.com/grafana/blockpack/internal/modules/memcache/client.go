package memcache

import gomemcache "github.com/grafana/gomemcache/memcache"

type client interface {
	Get(key string, opts ...gomemcache.Option) (*gomemcache.Item, error)
	Set(item *gomemcache.Item) error
	Close()
}
