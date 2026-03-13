# rw — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants
for the `rw` package. It complements NOTES.md (design rationale) and
TESTS.md (test plan).

---

## 1. Responsibility Boundary

rw **provides the storage abstraction layer for blockpack**. It owns the byte-moving
types: the storage backend interface, read-type hints, and provider compositions
(tracking, caching, default, shared LRU). It does not parse or encode blockpack file formats.

| Concern | Owner |
|---------|-------|
| Storage backend interface (ReaderProvider) | **rw** |
| Read-type hints (DataType) | **rw** |
| I/O counting and byte tracking (TrackingReaderProvider) | **rw** |
| Sub-range memory caching (RangeCachingProvider) | **rw** |
| Standard provider composition (DefaultProvider) | **rw** |
| Shared cross-reader LRU cache (SharedLRUCache / SharedLRUProvider) | **rw** |
| Format parsing and encoding | caller packages (blockio/reader, blockio/writer) |
| Span field access | blockio/shared (SpanFieldsProvider) |

---

## 2. ReaderProvider — Storage Backend Interface

```go
type ReaderProvider interface {
    Size() (int64, error)
    ReadAt(p []byte, off int64, dataType DataType) (int, error)
}
```

### 2.1 Size

Returns the total size of the storage in bytes.

**Invariant:** Must be deterministic for a given storage object (does not change during use).

### 2.2 ReadAt

pread-style offset-based random read.

**Preconditions:**
- `off >= 0`
- `off + len(p) <= Size()`

**Invariant:** Safe for concurrent use from multiple goroutines.

**Invariant:** `dataType` is a hint only — implementations may ignore it entirely.
Caching layers use it for priority-based eviction; correctness must not depend on its value.

---

## 3. DataType — Read-Type Hint

```go
type DataType uint8
```

A typed constant (not a string) passed to `ReaderProvider.ReadAt` identifying the
semantic category of bytes being read. Caching layers use it to determine eviction
priority. Implementations that do not cache may ignore it.

**Breaking change from prior versions:** `DataType` was previously `string`; it is
now `uint8` with iota-assigned values. String conversion is no longer valid. See
NOTES.md §7 for rationale.

### 3.1 Constants

| Constant | Iota | Semantic | Eviction tier |
|----------|------|----------|---------------|
| `DataTypeUnknown` | 0 | Unknown / unset; treated as lowest priority | 3 (easiest) |
| `DataTypeFooter` | 1 | File footer section | 0 (hardest) |
| `DataTypeHeader` | 2 | File header section | 0 (hardest) |
| `DataTypeMetadata` | 3 | Block metadata section | 1 |
| `DataTypeTraceBloomFilter` | 4 | Compact trace-ID bloom filter + lookup index | 1 |
| `DataTypeTimestampIndex` | 5 | Per-file timestamp index | 2 |
| `DataTypeBlock` | 6 | Span block payload | 3 (easiest) |

**Priority order (hardest to evict → easiest):**
`Footer ≈ Header > Metadata ≈ TraceBloomFilter > TimestampIndex > Block`

Back-ref: `internal/modules/rw/provider.go:DataType`

---

## 4. TrackingReaderProvider — I/O Metrics Wrapper

```go
type TrackingReaderProvider struct { ... }
```

Wraps a `ReaderProvider` and counts the number of `ReadAt` calls and total bytes read.

### 4.1 NewTrackingReaderProvider

```go
func NewTrackingReaderProvider(underlying ReaderProvider) *TrackingReaderProvider
```

Creates a tracker wrapping `underlying`. Initial counters are zero.

### 4.2 ReadAt

Increments `IOOps` by 1 and `BytesRead` by the number of bytes successfully read,
then delegates to `underlying`.

**Invariant:** Counter increments happen atomically — safe for concurrent use.

### 4.3 IOOps / BytesRead

```go
func (t *TrackingReaderProvider) IOOps() int64
func (t *TrackingReaderProvider) BytesRead() int64
```

Return current counter values. Thread-safe.

### 4.4 Reset

```go
func (t *TrackingReaderProvider) Reset()
```

Atomically zeroes both counters.

---

## 5. RangeCachingProvider — Sub-Range Memory Cache

```go
type RangeCachingProvider struct { ... }
```

Wraps a `ReaderProvider`. Caches the bytes of each successful read. On subsequent
reads, if the requested `[off, off+len(p))` range is fully contained within a
previously cached range, bytes are served from memory without calling the underlying
provider.

### 5.1 NewRangeCachingProvider

```go
func NewRangeCachingProvider(underlying ReaderProvider) *RangeCachingProvider
```

Creates a caching provider with an empty cache.

### 5.2 ReadAt — Cache Semantics

1. Acquire read lock; scan cache for a range that fully contains `[off, end)`.
2. On hit: copy from cache, release lock, return. No underlying I/O.
3. On miss: release lock; issue real read to underlying.
4. On success: acquire write lock; append new cached range; release lock.
5. If underlying returns `n < len(p)` with `err == nil`, return `(n, io.ErrUnexpectedEOF)`.
   The partial read is not cached.

**Invariant:** Cache hit iff `cr.offset <= off && off+len(p) <= cr.offset+len(cr.data)`.

**Invariant:** Short reads (`n < len(p)` with no error) are surfaced as `io.ErrUnexpectedEOF` and are never cached.

**Invariant:** All cached ranges are independent — no merging or eviction occurs.

**Invariant:** Safe for concurrent use. RWMutex protects the cache slice.

---

## 6. DefaultProvider — Standard Composition

```go
type DefaultProvider struct { ... }
```

The canonical provider composition for blockpack readers:

```
DefaultProvider
  └── RangeCachingProvider (outer — serves cache hits without underlying I/O)
        └── TrackingReaderProvider (inner — counts only real I/O operations)
              └── user storage (innermost)
```

**Composition invariant:** Cache wraps tracker, not the reverse. This ensures
`IOOps()` and `BytesRead()` reflect only real storage I/O, not cache hits.
See NOTES.md §1 for rationale.

### 6.1 NewDefaultProvider

```go
func NewDefaultProvider(underlying ReaderProvider) *DefaultProvider
```

Creates the standard cache → tracker → storage stack.

### 6.2 NewDefaultProviderWithLatency

```go
func NewDefaultProviderWithLatency(underlying ReaderProvider, latency time.Duration) *DefaultProvider
```

Inserts a `latencyProvider` between the tracker and storage. Used only in tests to
simulate object-storage first-byte latency. `latencyProvider` is unexported; callers
access it only through this constructor.

### 6.3 IOOps / BytesRead

Delegate to the inner tracker. Report only real storage I/O, not cache hits.

### 6.4 Reset

```go
func (d *DefaultProvider) Reset()
```

Delegates to the inner `TrackingReaderProvider.Reset()`. Zeroes both `IOOps` and
`BytesRead` counters. Useful for isolating I/O metrics between query phases (e.g.,
metadata reads vs. block reads) without creating a new provider instance.

---

## 7. SharedLRUCache — Cross-Reader Shared Cache

```go
type SharedLRUCache struct { ... }
```

A byte-bounded, priority-tiered LRU cache designed to be shared across multiple
`SharedLRUProvider` instances (one per file). The total memory footprint never
exceeds `maxBytes`. When capacity is exceeded, entries from the lowest-priority
tier are evicted first; within a tier, the least-recently-used entry is removed first.

### 7.1 NewSharedLRUCache

```go
func NewSharedLRUCache(maxBytes int64) *SharedLRUCache
```

Creates a cache with the given byte capacity. Safe for concurrent use immediately.

### 7.2 Get

```go
func (c *SharedLRUCache) Get(readerID string, off int64, length int) ([]byte, bool)
```

Looks up a cached range keyed by `(readerID, off, length)`. On hit, promotes the
entry to MRU position within its tier and returns a **copy** of the cached bytes.
Returns `(nil, false)` on miss.

**Invariant:** Returned bytes are a copy — callers may mutate them without affecting the cache.

### 7.3 Put

```go
func (c *SharedLRUCache) Put(readerID string, off int64, data []byte, dt DataType)
```

Stores `data` in the cache keyed by `(readerID, off, len(data))` with priority
derived from `dt`. No-ops if the key already exists or if `len(data) > maxBytes`.
Before inserting, evicts entries from lower-priority tiers until the new entry fits.

**Invariant:** `curBytes` never exceeds `maxBytes` after any `Put`.

**Invariant:** Eviction order: tier 3 (Block) → tier 2 (TimestampIndex) → tier 1
(Metadata/TraceBloomFilter) → tier 0 (Footer/Header). Within a tier, LRU is evicted first.

Back-ref: `internal/modules/rw/lru.go:SharedLRUCache`

---

## 8. SharedLRUProvider — Per-Reader Cache Front-End

```go
type SharedLRUProvider struct { ... }
```

Wraps a `ReaderProvider` and routes all reads through a `*SharedLRUCache`. Multiple
`SharedLRUProvider` instances sharing the same `*SharedLRUCache` share the cache
namespace. Each provider identifies itself via a `readerID` string (e.g. file path).

### 8.1 NewSharedLRUProvider

```go
func NewSharedLRUProvider(underlying ReaderProvider, readerID string, cache *SharedLRUCache) *SharedLRUProvider
```

`readerID` must be unique within the cache namespace (e.g. object storage key).

### 8.2 ReadAt

1. Check cache: `cache.Get(readerID, off, len(p))`.
2. On hit: copy cached bytes into `p`, return.
3. On miss: call `underlying.ReadAt`; on success store result via `cache.Put`; return.
4. If underlying returns `n < len(p)` with `err == nil`, return `(n, io.ErrUnexpectedEOF)`.
   The partial read is not cached.

**Invariant:** Cache hits never issue underlying I/O.

Back-ref: `internal/modules/rw/lru.go:SharedLRUProvider`

---

## 9. Thread Safety

All exported types in this package are safe for concurrent use.

---

