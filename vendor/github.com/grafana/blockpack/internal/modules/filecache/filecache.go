// Package filecache provides a disk-backed, size-bounded byte cache backed by bbolt.
// It deduplicates concurrent fetches for the same key using singleflight, making it
// safe to share across many concurrent goroutines opening the same blockpack files.
//
// Key format by convention: "<fileID>/<section>" where section is one of
// "footer", "header", "metadata", "compact", or "block/<blockIdx>".
package filecache

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/singleflight"
)

var (
	bucketData  = []byte("data")
	bucketOrder = []byte("order")
)

// Config configures the file cache.
type Config struct {
	// Path is the file path of the bbolt database file.
	Path string

	// MaxBytes is the maximum total logical payload bytes tracked by the cache
	// (sum of len(key)+len(value) for all entries). It does not include bbolt
	// page or bucket overhead, so actual on-disk file size will be somewhat larger.
	// When exceeded, the oldest entries (FIFO) are evicted until under the limit.
	MaxBytes int64

	// Enabled controls whether the cache is active.
	// If false, Open returns (nil, nil) and all cache operations are no-ops.
	Enabled bool
}

// FileCache is a disk-backed, size-bounded byte cache.
// It is safe for concurrent use across any number of goroutines.
//
// Eviction policy: FIFO by insertion order. When a new entry would push total
// stored bytes above MaxBytes, the oldest entries are removed first.
//
// Concurrent fetch deduplication: when multiple goroutines request the same uncached
// key simultaneously, only one fetch() is executed and the result is shared.
type FileCache struct {
	db       *bolt.DB
	group    singleflight.Group
	mu       sync.Mutex // protects curBytes
	curBytes int64
	maxBytes int64
}

// Open opens (or creates) a FileCache at cfg.Path.
// Returns (nil, nil) if cfg.Enabled is false — the nil cache is safe to use;
// all methods on a nil *FileCache are no-ops that fall through to fetch().
func Open(cfg Config) (*FileCache, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("filecache: MaxBytes must be positive, got %d", cfg.MaxBytes)
	}

	if cfg.Path == "" {
		return nil, fmt.Errorf("filecache: Path must not be empty")
	}

	db, err := bolt.Open(cfg.Path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("filecache: open %s: %w", cfg.Path, err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, cerr := tx.CreateBucketIfNotExists(bucketData); cerr != nil {
			return cerr
		}

		_, cerr := tx.CreateBucketIfNotExists(bucketOrder)

		return cerr
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("filecache: init buckets: %w", err)
	}

	var curBytes int64

	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketData).ForEach(func(k, v []byte) error {
			curBytes += int64(len(k) + len(v))
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("filecache: compute initial size: %w", err)
	}

	return &FileCache{
		db:       db,
		curBytes: curBytes,
		maxBytes: cfg.MaxBytes,
	}, nil
}

// Close closes the underlying database.
// It is safe to call Close on a nil *FileCache.
func (c *FileCache) Close() error {
	if c == nil {
		return nil
	}

	return c.db.Close()
}

// Get retrieves a cached value by key.
// Returns (nil, false, nil) on a miss.
// The returned slice is an independent copy safe for the caller to use.
func (c *FileCache) Get(key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}

	var val []byte

	if err := c.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketData).Get([]byte(key))
		if v == nil {
			return nil
		}

		val = make([]byte, len(v))
		copy(val, v)

		return nil
	}); err != nil {
		return nil, false, fmt.Errorf("filecache: get %q: %w", key, err)
	}

	return val, val != nil, nil
}

// Put stores key→value in the cache, evicting the oldest entries (FIFO) if needed
// to stay within MaxBytes. No-op if the key already exists or if the single entry
// exceeds MaxBytes.
func (c *FileCache) Put(key string, value []byte) error {
	if c == nil {
		return nil
	}

	needed := int64(len(key) + len(value))
	if needed > c.maxBytes {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// delta is computed inside the closure but applied only after a successful
	// commit — this avoids a desync if bbolt's transaction commit fails after
	// the closure has already executed (e.g., on disk-full or fsync failure).
	var delta int64

	err := c.db.Update(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketData)
		order := tx.Bucket(bucketOrder)

		// No-op if already cached.
		if data.Get([]byte(key)) != nil {
			return nil
		}

		// Collect FIFO eviction candidates before modifying.
		var evictSeqs [][]byte
		var evictKeys [][]byte
		evicted := int64(0)

		if c.curBytes+needed > c.maxBytes {
			cur := order.Cursor()

			for sk, dk := cur.First(); sk != nil; sk, dk = cur.Next() {
				if c.curBytes-evicted+needed <= c.maxBytes {
					break
				}

				dv := data.Get(dk)
				evicted += int64(len(dk) + len(dv))
				evictSeqs = append(evictSeqs, sliceCopy(sk))
				evictKeys = append(evictKeys, sliceCopy(dk))
			}
		}

		// Apply evictions.
		for i, sk := range evictSeqs {
			if err := order.Delete(sk); err != nil {
				return fmt.Errorf("evict order: %w", err)
			}

			if err := data.Delete(evictKeys[i]); err != nil {
				return fmt.Errorf("evict data: %w", err)
			}
		}

		// Insert the new entry.
		seq, err := order.NextSequence()
		if err != nil {
			return fmt.Errorf("next sequence: %w", err)
		}

		seqKey := encodeSeq(seq)

		if err := order.Put(seqKey, []byte(key)); err != nil {
			return fmt.Errorf("order put: %w", err)
		}

		if err := data.Put([]byte(key), value); err != nil {
			return fmt.Errorf("data put: %w", err)
		}

		delta = needed - evicted

		return nil
	})

	if err == nil {
		c.curBytes += delta
	}

	return err
}

// GetOrFetch returns the cached value for key, calling fetch() if not present.
// Concurrent calls for the same key share a single fetch invocation — only one
// goroutine executes fetch() and all waiters receive the same result.
//
// On a nil *FileCache, fetch() is called directly with no caching.
// Each caller receives an independent copy of the result bytes.
func (c *FileCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if c == nil {
		return fetch()
	}

	// Fast path: cache hit before entering singleflight.
	if val, ok, err := c.Get(key); err != nil {
		return nil, err
	} else if ok {
		return val, nil
	}

	// Slow path: deduplicated fetch via singleflight.
	// The callback returns raw (uncopied) bytes so that the single copy below
	// covers all paths — avoiding a double-copy when the inner Get() hits the
	// cache for a concurrent waiter that arrives after the first fetch completes.
	result, err, _ := c.group.Do(key, func() (any, error) {
		// Double-check under singleflight lock in case a concurrent Put landed.
		if val, ok, getErr := c.Get(key); getErr != nil {
			return nil, getErr
		} else if ok {
			// Return a raw reference; the outer copy will create an independent slice.
			return val, nil
		}

		fetched, fetchErr := fetch()
		if fetchErr != nil {
			return nil, fetchErr
		}

		// Store in cache; non-fatal on failure.
		if putErr := c.Put(key, fetched); putErr != nil {
			slog.Debug("filecache: Put failed, skipping cache", "err", putErr)
		}

		return fetched, nil
	})
	if err != nil {
		return nil, err
	}

	// Copy once: singleflight shares the same backing array among all concurrent
	// waiters, so every caller must receive its own independent slice.
	// SPEC-ROOT-001: two-value type assertion to prevent panic on unexpected singleflight result type
	src, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("filecache: unexpected singleflight result type %T", result)
	}
	out := make([]byte, len(src))
	copy(out, src)

	return out, nil
}

// encodeSeq encodes a uint64 sequence number as an 8-byte big-endian slice.
// Big-endian ensures lexicographic order equals numeric order for bbolt cursor scans.
func encodeSeq(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)

	return b
}

// sliceCopy returns an independent copy of b.
func sliceCopy(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)

	return out
}
