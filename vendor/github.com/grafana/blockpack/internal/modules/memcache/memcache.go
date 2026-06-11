// Package memcache provides a remote memcache-backed implementation of
// filecache.Cache. It is intended as the outermost (slowest but largest)
// tier in a multi-level cache chain.
//
// Keys are hashed with SHA-256 before being sent to memcache so that
// arbitrary-length blockpack cache keys (e.g. long S3 paths) are always
// valid memcache keys (≤ 250 chars, ASCII printable).
package memcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	gomemcache "github.com/grafana/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// client is the subset of gomemcache.Client we use, allowing the
// implementation to be tested without a real memcache server.

// Config configures a MemCache.

// Registerer is an optional Prometheus registerer.
// When non-nil, cache metrics are registered and incremented.

// TierLabel overrides the "tier" Prometheus label value (default "remote").

// Servers is the list of memcache server addresses (host:port).

// Expiration is the TTL in seconds for stored items.
// 0 means no expiration.

// Enabled controls whether the cache is active.
// If false, Open returns (nil, nil) and all operations become no-ops.

// MemCache is a remote memcache-backed cache that implements filecache.Cache.
// It is safe for concurrent use.
//
// A nil *MemCache is safe to use: all operations become pass-throughs.
type MemCache struct {
	group    singleflight.Group
	c        client
	requests *prometheus.CounterVec
	bytes    *prometheus.CounterVec
	errs     *prometheus.CounterVec
	// Pre-resolved histogram observers for 0-alloc hot path.
	// Each is nil when Registerer is not configured.
	durGetHit  prometheus.Observer
	durGetMiss prometheus.Observer
	durPutOk   prometheus.Observer
	tierLabel  string
	expiration int32
}

// expandServers resolves each host:port entry to all IPs returned by DNS,
// returning individual IP:port entries. This ensures that Kubernetes headless
// services with multiple pod IPs are all added to the consistent hash ring
// at startup — gomemcache.New resolves each entry via net.ResolveTCPAddr
// which returns only one IP per hostname.
// On DNS failure for a server the original entry is kept as a fallback.
func expandServers(servers []string) []string {
	expanded := make([]string, 0, len(servers))
	for _, server := range servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			expanded = append(expanded, server)
			continue
		}
		addrs, err := net.LookupHost(host)
		if err != nil || len(addrs) == 0 {
			expanded = append(expanded, server)
			continue
		}
		for _, addr := range addrs {
			expanded = append(expanded, net.JoinHostPort(addr, port))
		}
	}
	return expanded
}

// memcacheMaxIdleConns is the per-server idle-connection pool size for the
// remote memcache client.
//
// NOTE-162: gomemcache defaults MaxIdleConns to 2. A single heavy metrics
// query (e.g. M1/M4 {} | rate()) fans out into hundreds of concurrent block
// page / TOC / intrinsic cache lookups, each of which calls Client.Get. With
// only 2 idle connections per server the pool is instantly exhausted, so every
// additional concurrent Get dials a brand-new TCP connection and discards it
// after use. A querier CPU profile (2026-06-10) confirmed this is the dominant
// cost: ~66% of querier CPU was in memcache (*Client).dial -> net.Dialer ->
// kernel __inet_hash_connect / __inet_check_established / tcp_twsk_unique
// (ephemeral-port + TIME_WAIT-reuse churn) plus TLS re-handshake crypto, while
// the executor scan/decode path was <2%. Sizing the idle pool above peak
// parallel requests lets connections be reused instead of re-dialed.
const memcacheMaxIdleConns = 512

// newPooledClient builds a gomemcache client with an idle-connection pool deep
// enough to survive a heavy metrics query's concurrent cache fan-out, instead
// of the default 2 (NOTE-162). MinIdleConnsHeadroomPercentage is set negative
// so idle connections are never proactively closed between queries — the
// background reaper closing the pool down to 2 between bursts is exactly what
// forces the re-dial storm on the next query.
func newPooledClient(servers []string) *gomemcache.Client {
	ss := new(gomemcache.ServerList)
	_ = ss.SetServers(servers...)
	c := gomemcache.NewFromSelector(ss)
	c.MaxIdleConns = memcacheMaxIdleConns
	c.MinIdleConnsHeadroomPercentage = -1
	return c
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
	tl := cfg.TierLabel
	if tl == "" {
		tl = "remote"
	}
	m := &MemCache{
		c:          newPooledClient(expandServers(cfg.Servers)),
		expiration: cfg.Expiration,
		tierLabel:  tl,
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
		h := memcacheRegisterOrReuseHistogram(cfg.Registerer, prometheus.NewHistogramVec(
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
		tl := cfg.TierLabel
		if tl == "" {
			tl = "remote"
		}
		m.tierLabel = tl
		m.durGetHit = h.WithLabelValues(tl, "get", "hit")
		m.durGetMiss = h.WithLabelValues(tl, "get", "miss")
		m.durPutOk = h.WithLabelValues(tl, "put", "ok")
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

// memcacheRegisterOrReuseHistogram registers a HistogramVec with the given Registerer.
// If the metric is already registered (AlreadyRegisteredError), it returns the
// previously registered collector instead of panicking.
func memcacheRegisterOrReuseHistogram(
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
	var start time.Time
	if m.durGetHit != nil {
		start = time.Now()
	}
	item, err := m.c.Get(memcacheKey(key))
	if errors.Is(err, gomemcache.ErrCacheMiss) {
		if m.requests != nil {
			m.requests.WithLabelValues(m.tierLabel, "miss").Inc()
		}
		if m.durGetMiss != nil {
			m.durGetMiss.Observe(time.Since(start).Seconds())
		}
		return nil, false, nil
	}
	if err != nil {
		// Treat transient errors (connection loss, server unavailable) as misses.
		// Memcache is a best-effort cache; the caller falls back to the underlying reader.
		if m.errs != nil {
			m.errs.WithLabelValues(m.tierLabel).Inc()
		}
		return nil, false, nil //nolint:nilerr
	}
	out := make([]byte, len(item.Value))
	copy(out, item.Value)
	if m.requests != nil {
		m.requests.WithLabelValues(m.tierLabel, "hit").Inc()
	}
	if m.bytes != nil {
		m.bytes.WithLabelValues(m.tierLabel).Add(float64(len(out)))
	}
	if m.durGetHit != nil {
		m.durGetHit.Observe(time.Since(start).Seconds())
	}
	return out, true, nil
}

// GetMulti fetches many keys in a single batched request and returns a map of
// the original (unhashed) keys that hit to their independently-copied values.
// Missing keys are simply absent from the result. Transient errors are treated
// as a total miss (empty map, nil error) so an unavailable memcache server never
// breaks the read path, exactly like Get.
//
// NOTE-179: gomemcache.GetMulti groups the (hashed) keys per server and
// pipelines them over a SINGLE connection per server. A heavy query that touches
// many small block columns previously issued one Get — and thus one connection
// acquisition — per column; a querier CPU profile attributed ~28% of CPU to
// memcache (*Client).dial because the concurrent per-column fan-out exhausted the
// idle pool and forced fresh dials. Collapsing those N round-trips into one
// batched request removes the dial storm and keeps the single-RTT latency.
func (m *MemCache) GetMulti(keys []string) (map[string][]byte, error) {
	if m == nil || len(keys) == 0 {
		return nil, nil
	}
	var start time.Time
	if m.durGetHit != nil {
		start = time.Now()
	}
	// Map hashed memcache keys back to their original keys for the result.
	hashed := make([]string, len(keys))
	origByHashed := make(map[string]string, len(keys))
	for i, k := range keys {
		h := memcacheKey(k)
		hashed[i] = h
		origByHashed[h] = k
	}
	items, err := m.c.GetMulti(context.Background(), hashed)
	if err != nil {
		// Treat transient errors as a total miss; the caller re-fetches the
		// missing keys from the underlying provider, exactly like Get.
		if m.errs != nil {
			m.errs.WithLabelValues(m.tierLabel).Inc()
		}
		return nil, nil //nolint:nilerr
	}
	out := make(map[string][]byte, len(items))
	var hitBytes int
	for h, item := range items {
		orig, ok := origByHashed[h]
		if !ok {
			continue
		}
		cp := make([]byte, len(item.Value))
		copy(cp, item.Value)
		out[orig] = cp
		hitBytes += len(cp)
	}
	if m.requests != nil {
		hits := len(out)
		if hits > 0 {
			m.requests.WithLabelValues(m.tierLabel, "hit").Add(float64(hits))
		}
		if misses := len(keys) - hits; misses > 0 {
			m.requests.WithLabelValues(m.tierLabel, "miss").Add(float64(misses))
		}
	}
	if m.bytes != nil && hitBytes > 0 {
		m.bytes.WithLabelValues(m.tierLabel).Add(float64(hitBytes))
	}
	if m.durGetHit != nil {
		m.durGetHit.Observe(time.Since(start).Seconds())
	}
	return out, nil
}

// Put stores key→value in memcache. Non-fatal errors (e.g. connection loss)
// are silently ignored so that an unavailable memcache server never breaks
// the read path.
func (m *MemCache) Put(key string, value []byte) error {
	if m == nil {
		return nil
	}
	var start time.Time
	if m.durPutOk != nil {
		start = time.Now()
	}
	_ = m.c.Set(&gomemcache.Item{
		Key:        memcacheKey(key),
		Value:      value,
		Expiration: m.expiration,
	})
	if m.durPutOk != nil {
		m.durPutOk.Observe(time.Since(start).Seconds())
	}
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
