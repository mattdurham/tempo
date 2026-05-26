// Package tieredcache provides a filecache.Cache that routes cache keys to one
// of two sub-caches based on key content:
//   - Keys matching the raw block data pattern (*/block/<decimal-integer>) → dataCache
//   - All other keys (footer, header, metadata, sections, trace index) → metadataCache
//
// This enables independent cache tiers for large infrequently-reused block data
// (pod-local memcache) and small frequently-reused metadata (shared remote memcache).
package tieredcache

import (
	"errors"
	"strings"

	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/prometheus/client_golang/prometheus"
)

// Config holds the configuration for a TieredCache.
type Config struct {
	Metadata   filecache.Cache
	Data       filecache.Cache
	Registerer prometheus.Registerer // optional; nil = no metrics
}

// TieredCache routes filecache.Cache operations to one of two sub-caches:
//   - block data keys (*/block/<digits>) → data
//   - all other keys                    → metadata
//
// TieredCache is safe for concurrent use. Thread safety is delegated to the
// sub-caches; each sub-cache handles its own locking.
//
// A nil *TieredCache is safe to call Close on.
type TieredCache struct {
	metadata filecache.Cache
	data     filecache.Cache
	requests *prometheus.CounterVec // labels: tier, operation, result
	bytes    *prometheus.CounterVec // labels: tier, operation
}

// NewTieredCacheWithConfig creates a TieredCache from the given configuration.
// When cfg.Registerer is non-nil, Prometheus counters are registered and incremented
// on every cache operation. Nil Metadata or Data values are normalized to
// filecache.NopCache so callers never need to guard against nil sub-caches.
func NewTieredCacheWithConfig(cfg Config) *TieredCache {
	if cfg.Metadata == nil {
		cfg.Metadata = filecache.NopCache
	}
	if cfg.Data == nil {
		cfg.Data = filecache.NopCache
	}
	t := &TieredCache{
		metadata: cfg.Metadata,
		data:     cfg.Data,
	}
	if cfg.Registerer != nil {
		t.requests = tieredRegisterOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_tiered_cache_requests_total",
			Help: "Total number of tiered cache requests by tier, operation, and result.",
		}, []string{"tier", "operation", "result"}))
		t.bytes = tieredRegisterOrReuse(cfg.Registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "blockpack_tiered_cache_bytes_total",
			Help: "Total bytes transferred through the tiered cache by tier and operation.",
		}, []string{"tier", "operation"}))
	}
	return t
}

// tieredRegisterOrReuse registers cv with reg. On AlreadyRegisteredError it
// returns the previously registered collector, preserving idempotency across
// multiple NewTieredCacheWithConfig calls sharing the same Registerer.
func tieredRegisterOrReuse(reg prometheus.Registerer, cv *prometheus.CounterVec) *prometheus.CounterVec {
	if err := reg.Register(cv); err == nil {
		return cv
	} else if are := (prometheus.AlreadyRegisteredError{}); errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
			return existing
		}
	}
	return cv
}

// New creates a TieredCache routing metadata keys to metadataCache and
// raw block data keys to dataCache. Nil arguments are normalized to
// filecache.NopCache.
func New(metadataCache, dataCache filecache.Cache) *TieredCache {
	return NewTieredCacheWithConfig(Config{
		Metadata: metadataCache,
		Data:     dataCache,
	})
}

// isBlockDataKey reports whether key is a raw block data key, i.e. it ends with
// a "/block/" segment followed immediately by one or more decimal digits and
// nothing else.
//
// Algorithm:
//  1. Find the last occurrence of "/block/" in key (strings.LastIndex).
//  2. Everything after that segment must be a non-empty run of ASCII digits.
//
// Using LastIndex (not Contains/Index) ensures that a fileID which itself
// contains "/block/" as an internal path component is not misclassified.
// The byte-scan approach is zero-alloc and equivalent to a regexp for this
// simple pattern, with no lock contention under concurrency.
func isBlockDataKey(key string) bool {
	const seg = "/block/"
	idx := strings.LastIndex(key, seg)
	if idx < 0 {
		return false
	}
	tail := key[idx+len(seg):]
	if len(tail) == 0 {
		return false
	}
	for i := range len(tail) {
		if tail[i] < '0' || tail[i] > '9' {
			return false
		}
	}
	return true
}

// pick selects the sub-cache appropriate for key.
func (t *TieredCache) pick(key string) filecache.Cache {
	if isBlockDataKey(key) {
		return t.data
	}
	return t.metadata
}

// tierLabel returns "data" for block data keys and "metadata" for all others.
func tierLabel(key string) string {
	if isBlockDataKey(key) {
		return "data"
	}
	return "metadata"
}

// Get returns the cached bytes for key from the appropriate sub-cache.
// Returns (nil, false, nil) on a miss.
func (t *TieredCache) Get(key string) ([]byte, bool, error) {
	val, ok, err := t.pick(key).Get(key)
	if t.requests != nil {
		tier := tierLabel(key)
		var result string
		switch {
		case err != nil:
			result = "error"
		case ok:
			result = "hit"
		default:
			result = "miss"
		}
		t.requests.WithLabelValues(tier, "get", result).Inc()
		if ok && t.bytes != nil {
			t.bytes.WithLabelValues(tier, "get").Add(float64(len(val)))
		}
	}
	return val, ok, err
}

// Put stores key→value in the appropriate sub-cache.
func (t *TieredCache) Put(key string, value []byte) error {
	err := t.pick(key).Put(key, value)
	if t.requests != nil {
		tier := tierLabel(key)
		if err != nil {
			t.requests.WithLabelValues(tier, "put", "error").Inc()
		} else {
			t.requests.WithLabelValues(tier, "put", "ok").Inc()
			if t.bytes != nil {
				t.bytes.WithLabelValues(tier, "put").Add(float64(len(value)))
			}
		}
	}
	return err
}

// GetOrFetch returns the cached value for key from the appropriate sub-cache;
// on a miss it calls fetch(), stores the result, and returns it.
func (t *TieredCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	cache := t.pick(key)
	if t.requests == nil {
		return cache.GetOrFetch(key, fetch)
	}

	tier := tierLabel(key)
	fetchCalled := false
	val, err := cache.GetOrFetch(key, func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	if err != nil {
		return nil, err
	}
	if fetchCalled {
		t.requests.WithLabelValues(tier, "get", "miss").Inc()
		t.bytes.WithLabelValues(tier, "put").Add(float64(len(val)))
	} else {
		t.requests.WithLabelValues(tier, "get", "hit").Inc()
		t.bytes.WithLabelValues(tier, "get").Add(float64(len(val)))
	}
	return val, nil
}

// Close closes both sub-caches and returns all errors joined.
// Safe to call on a nil *TieredCache (returns nil).
func (t *TieredCache) Close() error {
	if t == nil {
		return nil
	}
	return errors.Join(t.metadata.Close(), t.data.Close())
}
