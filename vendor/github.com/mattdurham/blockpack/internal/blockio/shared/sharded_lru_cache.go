package shared

import (
	"fmt"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// ShardedLRUByteCache implements ByteCache using multiple sharded LRU caches
// to reduce lock contention in concurrent workloads.
//
// Performance characteristics compared to LRUByteCache:
//   - Single-threaded: similar performance (~120ns/op)
//   - Parallel workloads: 1.35x faster
//   - Read-heavy workloads: 4x faster (208ns → 53ns)
//   - Write-heavy workloads: 3.7x faster (235ns → 63ns)
//
// Recommended for all multi-core, concurrent use cases. Use 16-32 shards
// for best performance on typical multi-core systems.
type ShardedLRUByteCache struct {
	shards      []*LRUByteCache
	shardCount  uint32
	shardMask   uint32
	maxSize     int64
	evictions   atomic.Int64
	currentSize atomic.Int64
}

// NewShardedLRUByteCache creates a new sharded LRU byte cache with the given
// maximum size in bytes and number of shards (typically 16-32 for best performance).
func NewShardedLRUByteCache(maxBytes int64, shardCount int) (*ShardedLRUByteCache, error) {
	if shardCount < 1 {
		shardCount = 16 // Default to 16 shards
	}

	// Round up to next power of 2 for efficient modulo via bitmasking
	shardCount = nextPowerOf2(shardCount)

	c := &ShardedLRUByteCache{
		shards:     make([]*LRUByteCache, shardCount),
		shardCount: uint32(shardCount),     //nolint:gosec
		shardMask:  uint32(shardCount - 1), //nolint:gosec
		maxSize:    maxBytes,
	}

	// Create shards with evenly divided capacity, distributing remainder
	shardMaxBytes := maxBytes / int64(shardCount)
	remainder := maxBytes % int64(shardCount)
	for i := 0; i < shardCount; i++ {
		// First 'remainder' shards get an extra byte to use all capacity
		shardBytes := shardMaxBytes
		if int64(i) < remainder {
			shardBytes++
		}
		shard, err := newLRUByteCacheShard(shardBytes, c)
		if err != nil {
			return nil, fmt.Errorf("create LRU cache shard %d: %w", i, err)
		}
		c.shards[i] = shard
	}

	return c, nil
}

// newLRUByteCacheShard creates a shard that reports stats to parent
func newLRUByteCacheShard(maxBytes int64, parent *ShardedLRUByteCache) (*LRUByteCache, error) {
	c := &LRUByteCache{
		maxSize: maxBytes,
	}

	// Create LRU with eviction callback that reports to parent
	// Ensure minimum of 100 entries per shard to avoid excessive churn
	entriesPerShard := 10000 / int(parent.shardCount)
	if entriesPerShard < 100 {
		entriesPerShard = 100
	}
	cache, err := lru.NewWithEvict(entriesPerShard, func(key string, value []byte) {
		size := int64(len(value))
		c.currentSize.Add(-size)
		parent.evictions.Add(size)
		parent.currentSize.Add(-size)
	})
	if err != nil {
		return nil, fmt.Errorf("create shard LRU cache: %w", err)
	}

	c.cache = cache
	return c, nil
}

// nextPowerOf2 returns the next power of 2 >= n
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
