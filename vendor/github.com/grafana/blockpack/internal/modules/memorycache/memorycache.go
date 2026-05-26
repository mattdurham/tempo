// Package memorycache provides an in-memory, byte-bounded LRU cache that
// implements filecache.Cache. It is intended as the fastest tier in a
// multi-level cache chain (memorycache → filecache → memcache).
//
// Eviction is LRU: when total cached bytes would exceed MaxBytes the
// least-recently-used entry is removed first.
// Concurrent fetch deduplication uses singleflight.
package memorycache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// Config configures a MemoryCache.
type Config struct {
	// Registerer is an optional Prometheus registerer.
	// When non-nil, cache metrics are registered and incremented.
	Registerer prometheus.Registerer
	// MaxBytes is the maximum total bytes the cache may hold.
	// Required and must be positive.
	MaxBytes int64
}

// entry is one record in the LRU list.
type entry struct {
	key  string
	data []byte
}

// MemoryCache is an in-memory, byte-bounded LRU cache safe for concurrent use.
// It implements filecache.Cache.
//
// A nil *MemoryCache is safe to use: all operations become pass-throughs.
type MemoryCache struct {
	group     singleflight.Group
	index     map[string]*list.Element
	lru       *list.List // back = MRU, front = LRU
	requests  *prometheus.CounterVec
	bytes     *prometheus.CounterVec
	evictions *prometheus.CounterVec
	// Pre-resolved histogram observers for 0-alloc hot path.
	// Each is nil when Registerer is not configured.
	durGetHit  prometheus.Observer
	durGetMiss prometheus.Observer
	durPutOk   prometheus.Observer
	maxBytes   int64
	curBytes   int64
	mu         sync.Mutex
}

// New creates a MemoryCache with the given byte capacity.
func New(cfg Config) (*MemoryCache, error) {
	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("memorycache: MaxBytes must be positive, got %d", cfg.MaxBytes)
	}
	c := &MemoryCache{
		maxBytes: cfg.MaxBytes,
		index:    make(map[string]*list.Element),
		lru:      list.New(),
	}
	if cfg.Registerer != nil {
		c.requests = registerOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_cache_requests_total",
			Help: "Total number of cache requests by tier and result.",
		}, []string{"tier", "result"}))
		c.bytes = registerOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_cache_bytes_total",
			Help: "Total bytes read from cache by tier.",
		}, []string{"tier"}))
		c.evictions = registerOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_cache_evictions_total",
			Help: "Total number of cache evictions by tier.",
		}, []string{"tier"}))
		h := registerOrReuseHistogram(cfg.Registerer, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:                            "blockpack_cache_operation_duration_seconds",
				Help:                            "Duration of cache Get and Put operations by tier, operation, and result.",
				NativeHistogramBucketFactor:     1.1,
				NativeHistogramMaxBucketNumber:  100,
				NativeHistogramMinResetDuration: 15 * time.Minute,
			},
			[]string{"tier", "operation", "result"},
		))
		// Pre-resolve label combinations for 0-alloc hot path.
		c.durGetHit = h.WithLabelValues("memory", "get", "hit")
		c.durGetMiss = h.WithLabelValues("memory", "get", "miss")
		c.durPutOk = h.WithLabelValues("memory", "put", "ok")
	}
	return c, nil
}

// registerOrReuse registers a CounterVec with the given Registerer.
// If the metric is already registered (AlreadyRegisteredError), it returns the
// previously registered metric instead of panicking.
func registerOrReuse(reg prometheus.Registerer, cv *prometheus.CounterVec) *prometheus.CounterVec {
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
	// If registration fails for another reason, return the unregistered vec
	// (it will not be connected to the registry but won't panic).
	return cv
}

// registerOrReuseHistogram registers a HistogramVec with the given Registerer.
// If the metric is already registered (AlreadyRegisteredError), it returns the
// previously registered collector instead of panicking.
func registerOrReuseHistogram(
	reg prometheus.Registerer,
	hv *prometheus.HistogramVec,
) *prometheus.HistogramVec {
	err := reg.Register(hv)
	if err == nil {
		return hv
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
			return existing
		}
	}
	return hv
}

// Get returns the cached bytes for key, or (nil, false, nil) on a miss.
// The returned slice is read-only; callers must not modify it.
func (c *MemoryCache) Get(key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}

	var start time.Time
	if c.durGetHit != nil {
		start = time.Now()
	}

	c.mu.Lock()
	elem, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		if c.requests != nil {
			c.requests.WithLabelValues("memory", "miss").Inc()
		}
		if c.durGetMiss != nil {
			c.durGetMiss.Observe(time.Since(start).Seconds())
		}
		return nil, false, nil
	}
	c.lru.MoveToBack(elem)
	src := elem.Value.(*entry).data
	c.mu.Unlock()
	if c.requests != nil {
		c.requests.WithLabelValues("memory", "hit").Inc()
	}
	if c.bytes != nil {
		c.bytes.WithLabelValues("memory").Add(float64(len(src)))
	}
	if c.durGetHit != nil {
		c.durGetHit.Observe(time.Since(start).Seconds())
	}
	return src, true, nil
}

// Put stores key→value in the cache.
// The caller must not modify value after calling Put.
// No-op if the key already exists or if len(value) > MaxBytes.
func (c *MemoryCache) Put(key string, value []byte) error {
	if c == nil {
		return nil
	}
	var start time.Time
	if c.durPutOk != nil {
		start = time.Now()
	}
	needed := int64(len(value))
	if needed > c.maxBytes {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[key]; exists {
		return nil
	}

	c.evictLocked(needed)

	if c.curBytes+needed > c.maxBytes {
		return nil // still over budget after eviction (shouldn't happen)
	}

	e := &entry{key: key, data: value}
	elem := c.lru.PushBack(e)
	c.index[key] = elem
	c.curBytes += needed
	if c.durPutOk != nil {
		c.durPutOk.Observe(time.Since(start).Seconds())
	}
	return nil
}

// evictLocked removes LRU entries until curBytes+needed ≤ maxBytes.
// Must be called with c.mu held.
func (c *MemoryCache) evictLocked(needed int64) {
	for c.curBytes+needed > c.maxBytes && c.lru.Len() > 0 {
		elem := c.lru.Front()
		e := elem.Value.(*entry)
		c.lru.Remove(elem)
		delete(c.index, e.key)
		c.curBytes -= int64(len(e.data))
		if c.evictions != nil {
			c.evictions.WithLabelValues("memory").Inc()
		}
	}
}

// GetOrFetch returns the cached value for key; on a miss it calls fetch(),
// stores the result, and returns it. Concurrent calls for the same uncached
// key share a single fetch invocation.
func (c *MemoryCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if c == nil {
		return fetch()
	}

	if val, ok, err := c.Get(key); err != nil {
		return nil, err
	} else if ok {
		return val, nil
	}

	result, err, _ := c.group.Do(key, func() (any, error) {
		if val, ok, getErr := c.Get(key); getErr != nil {
			return nil, getErr
		} else if ok {
			return val, nil
		}

		fetched, fetchErr := fetch()
		if fetchErr != nil {
			return nil, fetchErr
		}

		if putErr := c.Put(key, fetched); putErr != nil {
			return nil, putErr
		}
		return fetched, nil
	})
	if err != nil {
		return nil, err
	}

	src, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("memorycache: unexpected singleflight result type %T", result)
	}
	return src, nil
}

// Close is a no-op for an in-memory cache.
// Safe to call on a nil *MemoryCache.
func (c *MemoryCache) Close() error { return nil }
