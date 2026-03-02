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
