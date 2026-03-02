# rw — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants
for the `rw` package. It complements NOTES.md (design rationale) and
TESTS.md (test plan).

---

## 1. Responsibility Boundary

rw **provides the storage abstraction layer for blockpack**. It owns the byte-moving
types: the storage backend interface, read-type hints, and provider compositions
(tracking, caching, default). It does not parse or encode blockpack file formats.

| Concern | Owner |
|---------|-------|
| Storage backend interface (ReaderProvider) | **rw** |
| Read-type hints (DataType) | **rw** |
| I/O counting and byte tracking (TrackingReaderProvider) | **rw** |
| Sub-range memory caching (RangeCachingProvider) | **rw** |
| Standard provider composition (DefaultProvider) | **rw** |
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
Caching layers use it for future segmentation; correctness must not depend on its value.

---

## 3. DataType — Read-Type Hint

```go
type DataType string
```

A hint passed to `ReaderProvider.ReadAt` to inform caching layers of the semantic
category of the bytes being read. Implementations are free to ignore the hint.

### 3.1 Constants

| Constant | Value | Semantic |
|----------|-------|----------|
| `DataTypeFooter` | `"footer"` | File footer section |
| `DataTypeHeader` | `"header"` | File header section |
| `DataTypeMetadata` | `"metadata"` | Metadata section |
| `DataTypeBlock` | `"block"` | Block data section |
| `DataTypeIndex` | `"index"` | Index section |
| `DataTypeCompact` | `"compact"` | Compact trace index section |

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

## 7. Thread Safety

All exported types in this package are safe for concurrent use.
