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
	"golang.org/x/sync/singleflight"
)

// ChainedCache is a multi-tier filecache.Cache. It is safe for concurrent use.
type ChainedCache struct {
	group singleflight.Group
	tiers []filecache.Cache
}

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
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
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
