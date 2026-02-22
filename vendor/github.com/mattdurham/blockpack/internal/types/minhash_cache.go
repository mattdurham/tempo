package ondisk

import "strings"

// MinHashCache memoizes MinHash signatures by token set.
// It is not concurrency-safe; callers should provide external synchronization if needed.
type MinHashCache struct {
	entries    map[string]MinHashSignature
	maxEntries int
	hits       int
	misses     int
}

// MinHashCacheStats reports cache hit/miss counts and current entry count.
type MinHashCacheStats struct {
	Hits    int
	Misses  int
	Entries int
}

// NewMinHashCache creates a MinHash cache with a maximum entry count.
// A maxEntries of 0 disables caching.
func NewMinHashCache(maxEntries int) *MinHashCache {
	return &MinHashCache{
		maxEntries: maxEntries,
		entries:    make(map[string]MinHashSignature, maxEntries),
	}
}

// Compute returns the MinHash signature for tokens, using the cache when enabled.
func (c *MinHashCache) Compute(tokens []string) MinHashSignature {
	if c == nil || c.maxEntries <= 0 {
		return ComputeMinHash(tokens)
	}

	key := minhashTokensKey(tokens)
	if sig, ok := c.entries[key]; ok {
		c.hits++
		return sig
	}

	c.misses++
	sig := ComputeMinHash(tokens)

	if len(c.entries) >= c.maxEntries {
		// Evict a subset of entries instead of clearing the entire cache to avoid
		// pathological behavior where the cache repeatedly fills and fully clears.
		evictCount := len(c.entries) / 2
		if evictCount > 0 {
			i := 0
			for k := range c.entries {
				delete(c.entries, k)
				i++
				if i >= evictCount {
					break
				}
			}
		}
	}
	c.entries[key] = sig
	return sig
}

// Stats returns cache hit/miss counts and current entry count.
func (c *MinHashCache) Stats() MinHashCacheStats {
	if c == nil || c.maxEntries <= 0 {
		return MinHashCacheStats{}
	}
	return MinHashCacheStats{
		Hits:    c.hits,
		Misses:  c.misses,
		Entries: len(c.entries),
	}
}

func minhashTokensKey(tokens []string) string {
	switch len(tokens) {
	case 0:
		return ""
	case 1:
		return tokens[0]
	}

	totalLen := 0
	for _, token := range tokens {
		totalLen += len(token)
	}
	totalLen += len(tokens) - 1

	var b strings.Builder
	b.Grow(totalLen)
	for i, token := range tokens {
		if i > 0 {
			b.WriteByte(0x1f)
		}
		b.WriteString(token)
	}
	return b.String()
}
