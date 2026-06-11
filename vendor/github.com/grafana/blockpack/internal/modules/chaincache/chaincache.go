// Package chaincache provides a multi-tier filecache.Cache that delegates
// through an ordered list of caches. The intended chain is:
//
//	memorycache.MemoryCache → filecache.FileCache → memcache.MemCache
//
// On a cache hit at tier N, the value is written back to all tiers 0..N-1
// so that subsequent reads are served from the fastest available tier.
// On a miss across all tiers, the fetch function is called and the result
// is stored in every tier.
//
// Concurrent fetches for the same missing key are deduplicated via singleflight
// at the chain level, so only one fetch is issued regardless of how many
// goroutines request the same key simultaneously.
package chaincache

import (
	"errors"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/filecache"
)

// ChainedCache is a multi-tier filecache.Cache. It is safe for concurrent use.

// New creates a ChainedCache from the given tiers ordered fastest-first.
// The recommended order is: memorycache → filecache → memcache.
// Nil tiers are silently dropped, so disabled caches (e.g. OpenMemCache with
// Enabled:false returns nil) can be passed directly without wrapping.
// Passing zero non-nil tiers returns a cache equivalent to filecache.NopCache.
func New(tiers ...filecache.Cache) *ChainedCache {
	active := tiers[:0]
	for _, t := range tiers {
		if t != nil {
			active = append(active, t)
		}
	}
	return &ChainedCache{tiers: active}
}

// Get searches each tier in order and returns the first hit.
// On a hit at tier N, the value is written back to tiers 0..N-1.
func (c *ChainedCache) Get(key string) ([]byte, bool, error) {
	for i, tier := range c.tiers {
		val, ok, err := tier.Get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			c.writeBack(key, val, i)
			return val, true, nil
		}
	}
	return nil, false, nil
}

// batchGetter is an optional interface a tier may implement to fetch many keys
// in one round-trip (NOTE-179). The memcache tier implements it via GetMulti,
// which pipelines all keys over a single connection per server.
type batchGetter interface {
	GetMulti(keys []string) (map[string][]byte, error)
}

// GetMulti fetches many keys, returning a map of hit keys to values. It first
// probes the faster tiers per key (cheap in-process / local lookups), then
// batches every key still missing into ONE GetMulti against the first tier that
// supports batch fetching (the memcache tier). Hits found in a slower tier are
// written back to the faster tiers that missed. Keys absent from the result
// missed every tier and must be fetched from the provider by the caller.
//
// NOTE-179: the per-column block read path previously issued one GetOrFetch —
// and thus one memcache connection acquisition — per column, fanned out
// concurrently, which exhausted the idle pool and forced a dial per column
// (~28% of querier CPU in (*Client).dial). Batching the memcache misses into a
// single pipelined request collapses that dial storm.
func (c *ChainedCache) GetMulti(keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte, len(keys))
	// Copy so the in-place filtering below never mutates the caller's slice.
	remaining := make([]string, len(keys))
	copy(remaining, keys)

	for i, tier := range c.tiers {
		if len(remaining) == 0 {
			break
		}
		if bg, ok := tier.(batchGetter); ok {
			// Batch-capable tier: one round-trip for all remaining keys.
			hits, err := bg.GetMulti(remaining)
			if err != nil {
				return nil, err
			}
			next := remaining[:0]
			for _, k := range remaining {
				if v, found := hits[k]; found {
					out[k] = v
					c.writeBack(k, v, i)
				} else {
					next = append(next, k)
				}
			}
			remaining = next
			continue
		}
		// Non-batch tier: probe each remaining key individually. These tiers are
		// in-process or local-disk, so per-key lookups carry no connection cost.
		next := remaining[:0]
		for _, k := range remaining {
			v, found, err := tier.Get(k)
			if err != nil {
				return nil, err
			}
			if found {
				out[k] = v
				c.writeBack(k, v, i)
			} else {
				next = append(next, k)
			}
		}
		remaining = next
	}
	return out, nil
}

// Put stores key→value in every tier.
func (c *ChainedCache) Put(key string, value []byte) error {
	var errs []error
	for _, tier := range c.tiers {
		if err := tier.Put(key, value); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// GetOrFetch returns the cached value for key; on a miss it calls fetch(),
// stores the result in all tiers, and returns it.
// Concurrent callers for the same missing key share a single fetch invocation.
func (c *ChainedCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
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

		// Store in all tiers; ignore individual tier errors.
		for _, tier := range c.tiers {
			_ = tier.Put(key, fetched)
		}
		return fetched, nil
	})
	if err != nil {
		return nil, err
	}

	src, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("chaincache: unexpected singleflight result type %T", result)
	}
	return src, nil
}

// Close closes every tier and returns any errors joined together.
// Safe to call on a nil *ChainedCache.
func (c *ChainedCache) Close() error {
	if c == nil {
		return nil
	}
	var errs []error
	for _, tier := range c.tiers {
		if err := tier.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// writeBack stores val in tiers 0..hitTier-1 (the faster tiers that missed).
func (c *ChainedCache) writeBack(key string, val []byte, hitTier int) {
	for i := range hitTier {
		_ = c.tiers[i].Put(key, val)
	}
}
