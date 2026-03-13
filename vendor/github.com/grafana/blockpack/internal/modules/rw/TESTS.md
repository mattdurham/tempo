# rw — Test Plan

This document maps each test function in `provider_test.go` to its scenario,
setup, and assertions. It complements SPECS.md (contracts) and NOTES.md (rationale).

---

## RW-T-01: TrackingReaderProvider

### TestTrackingReaderProviderInitialCountersAreZero
**Scenario:** Freshly created tracker has zero counters.
**Setup:** `memProvider{data: "hello world"}`, `NewTrackingReaderProvider(mem)`.
**Assertions:** `IOOps() == 0`, `BytesRead() == 0`.

### TestTrackingReaderProviderIncrementsOnRead
**Scenario:** A single `ReadAt` call increments both counters.
**Setup:** `memProvider{data: "hello world"}`, read 5 bytes at offset 0.
**Assertions:** `n == 5`, `IOOps() == 1`, `BytesRead() == 5`.

### TestTrackingReaderProviderMultipleReads
**Scenario:** Multiple `ReadAt` calls accumulate correctly.
**Setup:** `memProvider` of 100 bytes; 5 reads of 10 bytes each.
**Assertions:** `IOOps() == 5`, `BytesRead() == 50`.

### TestTrackingReaderProviderReset
**Scenario:** `Reset()` zeroes both counters after reads.
**Setup:** `memProvider` of 100 bytes; one read of 10 bytes; then `Reset()`.
**Assertions:** `IOOps() == 0`, `BytesRead() == 0`.

### TestTrackingReaderProviderSizeDelegates
**Scenario:** `Size()` delegates to the underlying provider.
**Setup:** `memProvider{data: make([]byte, 42)}`.
**Assertions:** `Size() == 42, err == nil`.

---

## RW-T-02: RangeCachingProvider

### TestRangeCachingProviderCacheMiss
**Scenario:** First read is a cache miss — underlying is called.
**Setup:** `memProvider{data: "abcdefghij"}`, read 5 bytes at offset 0.
**Assertions:** `n == 5`, `buf == "abcde"`, `mem.ReadCount() == 1`.

### TestRangeCachingProviderCacheHit
**Scenario:** Second read (sub-range) is a cache hit — underlying not called again.
**Setup:** `memProvider{data: "abcdefghij"}`;
  - First read: 10 bytes at offset 0 (cache miss, primes [0,10)).
  - Second read: 3 bytes at offset 2 (cache hit, sub-range of [0,10)).
**Assertions:** Second read returns `"cde"`, `mem.ReadCount() == 1` (no second I/O).

### TestRangeCachingProviderSubRangeHit
**Scenario:** Sub-range of a larger cached region is served correctly.
**Setup:** `memProvider` of 100 bytes (values 0..99);
  - Prime cache: 50 bytes at offset 0.
  - Sub-read: 10 bytes at offset 10.
**Assertions:** bytes 10..19 returned correctly, `mem.ReadCount() == 1`.

### TestRangeCachingProviderSizeDelegates
**Scenario:** `Size()` delegates to the underlying provider.
**Setup:** `memProvider{data: make([]byte, 77)}`.
**Assertions:** `Size() == 77, err == nil`.

---

## RW-T-03: DefaultProvider

### TestDefaultProviderRoundTrip
**Scenario:** Basic read through the full cache+tracker stack.
**Setup:** `memProvider` with 22 bytes; `NewDefaultProvider(mem)`; read all bytes.
**Assertions:** Correct bytes returned, no error.

### TestDefaultProviderTracksIOOps
**Scenario:** Cache deduplicates I/O — only 1 real op for 3 identical reads.
**Setup:** `memProvider` of 100 bytes; 3 reads of 10 bytes at offset 0.
**Assertions:** `IOOps() == 1` (second and third reads are cache hits).

### TestDefaultProviderBytesRead
**Scenario:** `BytesRead()` reflects actual bytes from storage.
**Setup:** `memProvider` of 50 bytes; read all 50.
**Assertions:** `BytesRead() == 50`.

### TestDefaultProviderSizeDelegates
**Scenario:** `Size()` propagates through all layers.
**Setup:** `memProvider` of 99 bytes.
**Assertions:** `Size() == 99, err == nil`.

### TestDefaultProviderWithLatencyInjectsDelay
**Scenario:** `NewDefaultProviderWithLatency` injects per-read latency.
**Setup:** `memProvider` of 10 bytes; `latency = 5ms`; one read.
**Assertions:** Elapsed time >= 5ms, no error.

### TestDefaultProviderReset
**Scenario:** `Reset()` zeroes both I/O counters on DefaultProvider.
**Setup:** `memProvider` of 50 bytes; `NewDefaultProvider(mem)`; read all 50 bytes;
  then call `Reset()`.
**Assertions:** `IOOps() == 0`, `BytesRead() == 0` after reset.

---

## RW-T-04: Concurrent Safety

### TestRangeCachingProviderConcurrentReads
**Scenario:** Multiple goroutines call `ReadAt` concurrently on a shared
  `RangeCachingProvider`; the Go race detector must report no races.
**Setup:** `memProvider` of 64 bytes (values 0..63); `NewRangeCachingProvider(mem)`;
  20 goroutines each reading a 10-byte sub-range at one of four overlapping offsets.
**Assertions:** All reads return correct byte values with no error; no data races
  detected under `-race`.

---

## RW-T-05: Partial-Read Guard

### TestRangeCachingProviderPartialReadReturnsError
**Scenario:** Underlying provider returns `n < len(p)` with `err == nil` (short read,
no error — violates the full-read contract).
**Setup:** `shortReadProvider{data: "abcdefghij"}` (10 bytes); `NewRangeCachingProvider(sp)`;
  read 10 bytes at offset 0. `shortReadProvider.ReadAt` returns only half the requested
  bytes with `err == nil`.
**Assertions:** `err` is `io.ErrUnexpectedEOF`; `n < 10`.

---

## RW-T-06: SharedLRUCache
*File: `lru_test.go`*

### TestSharedLRUCache_MissReturnsNotFound
**Scenario:** Cache is empty; Get returns (nil, false).
**Setup:** `NewSharedLRUCache(1024)`; `Get("file1", 0, 10)`.
**Assertions:** `ok == false`.

### TestSharedLRUCache_PutAndGet
**Scenario:** Put then Get returns the same data.
**Setup:** Put 11 bytes at (file1, 0, DataTypeBlock); Get same key.
**Assertions:** `ok == true`; returned bytes equal original.

### TestSharedLRUCache_GetReturnsCopy
**Scenario:** Mutating the returned slice does not corrupt the cache.
**Setup:** Put data; Get it; mutate result; Get again.
**Assertions:** Second Get returns unmutated original data.

### TestSharedLRUCache_DifferentReaderIDsAreIsolated
**Scenario:** Two files at the same offset are independent cache entries.
**Setup:** Put `"aaa"` for file1 and `"bbb"` for file2 at offset 0.
**Assertions:** file1 returns `"aaa"`, file2 returns `"bbb"`.

### TestSharedLRUCache_EntryLargerThanCapacityIsDropped
**Scenario:** An entry larger than maxBytes is silently dropped.
**Setup:** `NewSharedLRUCache(5)`; Put 10 bytes.
**Assertions:** Get returns `(nil, false)`.

### TestSharedLRUCache_EvictsBlocksBeforeHighPriorityData
**Scenario:** When cache is full, block entries are evicted before footer entries.
**Setup:** Cache capacity = 2×10 bytes; Put footer(10B) + block(10B); add second block.
**Assertions:** Footer and new block present; original block evicted.

### TestSharedLRUCache_PriorityTierOrder
**Scenario:** Full four-tier eviction order: Block first, then TimestampIndex, then higher.
**Setup:** Cache capacity = 4×10 bytes; Put one entry per tier (Footer, TraceBloomFilter,
  TimestampIndex, Block); add one more block entry to force eviction.
**Assertions:** Footer, bloom, timestamp index all survive; original block evicted;
  new block present.

### TestSharedLRUCache_ConcurrentAccess
**Scenario:** Concurrent Put and Get calls from 20 goroutines do not race.
**Setup:** `NewSharedLRUCache(64KB)`; 20 goroutines each Put+Get a unique key.
**Assertions:** No data races under `-race`.

---

## RW-T-07: SharedLRUProvider
*File: `lru_test.go`*

### TestSharedLRUProvider_CacheMissReadsUnderlying
**Scenario:** First read is a cache miss; underlying is called exactly once.
**Setup:** `memProvider` of 10 bytes; `NewSharedLRUProvider(mem, "file1", cache)`.
**Assertions:** Correct 5 bytes returned; `mem.ReadCount() == 1`.

### TestSharedLRUProvider_CacheHitSkipsUnderlying
**Scenario:** Repeated identical read is served from cache; no second underlying I/O.
**Setup:** Two identical `ReadAt(buf, 0, DataTypeBlock)` calls through same provider.
**Assertions:** Both return correct data; `mem.ReadCount() == 1`.

### TestSharedLRUProvider_SharedCacheAcrossProviders
**Scenario:** Two providers share the same cache; each reader has an isolated namespace.
**Setup:** Two `SharedLRUProvider` instances with different `readerID`s over the same cache;
  read through p1 (primes file1 cache); read through p2 (different readerID, must miss).
**Assertions:** `mem1.ReadCount() == 1`; `mem2.ReadCount() == 1`; re-read through p1
  hits cache (`mem1.ReadCount()` stays at 1).

### TestSharedLRUProvider_SizeDelegates
**Scenario:** `Size()` delegates to the underlying provider.
**Setup:** `memProvider` of 42 bytes.
**Assertions:** `Size() == 42, err == nil`.

### TestSharedLRUProvider_ShortReadBecomesErrUnexpectedEOF
**Scenario:** Underlying returns `n < len(p)` with `err == nil`; result must not be cached.
**Setup:** `shortReadProvider` (10 bytes, returns half on each call); read 10 bytes at offset 0.
**Assertions:** First read returns `io.ErrUnexpectedEOF` with `n < 10`. Second read also
errors (partial result was not cached — underlying is called again).

---

## ~~RW-T-08 through RW-T-10~~ — Removed

`DiskCache`, `DiskLRUProvider`, and `ObjectCache` were removed in 2026-03-05.
Their test files (`disk_cache_test.go`, `object_cache_test.go`) were deleted.
See NOTES.md NOTE-009 for the removal rationale.
