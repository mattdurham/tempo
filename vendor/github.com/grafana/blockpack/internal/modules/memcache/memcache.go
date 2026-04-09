// Package memcache provides a remote memcache-backed implementation of
// filecache.Cache. It is intended as the outermost (slowest but largest)
// tier in a multi-level cache chain.
//
// Keys are hashed with SHA-256 before being sent to memcache so that
// arbitrary-length blockpack cache keys (e.g. long S3 paths) are always
// valid memcache keys (≤ 250 chars, ASCII printable).
package memcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	gomemcache "github.com/grafana/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// client is the subset of gomemcache.Client we use, allowing the
// implementation to be tested without a real memcache server.
type client interface {
	Get(key string, opts ...gomemcache.Option) (*gomemcache.Item, error)
	Set(item *gomemcache.Item) error
	Close()
}

// Config configures a MemCache.
type Config struct {
	// Servers is the list of memcache server addresses (host:port).
	Servers []string

	// Expiration is the TTL in seconds for stored items.
	// 0 means no expiration.
	Expiration int32

	// Enabled controls whether the cache is active.
	// If false, Open returns (nil, nil) and all operations become no-ops.
	Enabled bool

	// Registerer is an optional Prometheus registerer.
	// When non-nil, cache metrics are registered and incremented.
	Registerer prometheus.Registerer
}

// MemCache is a remote memcache-backed cache that implements filecache.Cache.
// It is safe for concurrent use.
//
// A nil *MemCache is safe to use: all operations become pass-throughs.
type MemCache struct {
	group      singleflight.Group
	c          client
	expiration int32
	requests   *prometheus.CounterVec
	bytes      *prometheus.CounterVec
	errs       *prometheus.CounterVec
}

// Open creates a MemCache connecting to cfg.Servers.
// Returns (nil, nil) when cfg.Enabled is false.
func Open(cfg Config) (*MemCache, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("memcache: at least one server address required")
	}
	m := &MemCache{
		c:          gomemcache.New(cfg.Servers...),
		expiration: cfg.Expiration,
	}
	if cfg.Registerer != nil {
		m.requests = memcacheRegisterOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_cache_requests_total",
			Help: "Total number of cache requests by tier and result.",
		}, []string{"tier", "result"}))
		m.bytes = memcacheRegisterOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_cache_bytes_total",
			Help: "Total bytes read from cache by tier.",
		}, []string{"tier"}))
		m.errs = memcacheRegisterOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_cache_errors_total",
			Help: "Total number of cache errors by tier.",
		}, []string{"tier"}))
	}
	return m, nil
}

// memcacheRegisterOrReuse registers a CounterVec with the given Registerer.
// If the metric is already registered (AlreadyRegisteredError), it returns the
// previously registered metric instead of panicking.
func memcacheRegisterOrReuse(reg prometheus.Registerer, cv *prometheus.CounterVec) *prometheus.CounterVec {
	err := reg.Register(cv)
	if err == nil {
		return cv
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
			return existing
		}
	}
	return cv
}

// memcacheKey converts an arbitrary blockpack cache key into a valid
// memcache key (SHA-256 hex, always 64 chars, no spaces or control chars).
func memcacheKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// Get returns the cached bytes for key, or (nil, false, nil) on a miss.
// Connection and other transient errors are treated as cache misses.
// The returned slice is an independent copy safe for the caller to modify.
func (m *MemCache) Get(key string) ([]byte, bool, error) {
	if m == nil {
		return nil, false, nil
	}
	item, err := m.c.Get(memcacheKey(key))
	if errors.Is(err, gomemcache.ErrCacheMiss) {
		if m.requests != nil {
			m.requests.WithLabelValues("remote", "miss").Inc()
		}
		return nil, false, nil
	}
	if err != nil {
		// Treat transient errors (connection loss, server unavailable) as misses.
		// Memcache is a best-effort cache; the caller falls back to the underlying reader.
		if m.errs != nil {
			m.errs.WithLabelValues("remote").Inc()
		}
		return nil, false, nil //nolint:nilerr
	}
	out := make([]byte, len(item.Value))
	copy(out, item.Value)
	if m.requests != nil {
		m.requests.WithLabelValues("remote", "hit").Inc()
	}
	if m.bytes != nil {
		m.bytes.WithLabelValues("remote").Add(float64(len(out)))
	}
	return out, true, nil
}

// Put stores key→value in memcache. Non-fatal errors (e.g. connection loss)
// are silently ignored so that an unavailable memcache server never breaks
// the read path.
func (m *MemCache) Put(key string, value []byte) error {
	if m == nil {
		return nil
	}
	_ = m.c.Set(&gomemcache.Item{
		Key:        memcacheKey(key),
		Value:      value,
		Expiration: m.expiration,
	})
	return nil
}

// GetOrFetch returns the cached value for key; on a miss it calls fetch(),
// stores the result, and returns it. Concurrent calls for the same uncached
// key share a single fetch invocation via singleflight.
func (m *MemCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if m == nil {
		return fetch()
	}

	if val, ok, err := m.Get(key); err != nil {
		return nil, err
	} else if ok {
		return val, nil
	}

	result, err, _ := m.group.Do(key, func() (any, error) {
		if val, ok, getErr := m.Get(key); getErr != nil {
			return nil, getErr
		} else if ok {
			return val, nil
		}

		fetched, fetchErr := fetch()
		if fetchErr != nil {
			return nil, fetchErr
		}

		if putErr := m.Put(key, fetched); putErr != nil {
			return nil, putErr
		}
		return fetched, nil
	})
	if err != nil {
		return nil, err
	}

	src, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("memcache: unexpected singleflight result type %T", result)
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}

// Close closes the underlying memcache connections.
// Safe to call on a nil *MemCache.
func (m *MemCache) Close() error {
	if m == nil {
		return nil
	}
	m.c.Close()
	return nil
}
