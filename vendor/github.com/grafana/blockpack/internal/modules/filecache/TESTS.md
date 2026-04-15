# filecache — Test Plan

This document defines the required tests for the `internal/modules/filecache` package.
Tests exercise both the NopCache and FileCache implementations of the Cache interface.

---

## FILE-TEST-001: TestDisabledCacheIsNil

**Scenario:** Opening a cache with Enabled=false returns nil without error.

**Setup:** Open(Config{Enabled: false}).

**Assertions:** Returned cache is nil; error is nil.

---

## FILE-TEST-002: TestNilCacheFallsThrough

**Scenario:** A nil Cache is treated as a NopCache — Get always misses, Put is a no-op.

**Setup:** var c Cache = nil.

**Assertions:** Get returns nil; Put returns nil; GetOrFetch calls fetch and returns its value.

---

## FILE-TEST-003: TestGetMissReturnsNil

**Scenario:** Get for an absent key returns nil without error.

**Setup:** Open a real FileCache; do not Put the key.

**Assertions:** Get("missing") returns nil, nil.

---

## FILE-TEST-004: TestPutAndGet

**Scenario:** A value written with Put is retrievable with Get.

**Setup:** Open a FileCache; Put("k", []byte("hello")).

**Assertions:** Get("k") returns []byte("hello").

---

## FILE-TEST-005: TestPutIsIdempotent

**Scenario:** Putting the same key twice does not corrupt the stored value.

**Setup:** Put("k", v1) then Put("k", v2).

**Assertions:** Get("k") returns v2.

---

## FILE-TEST-006: TestFIFOEviction

**Scenario:** When the total stored bytes exceed MaxBytes, the oldest entries are evicted.

**Setup:** MaxBytes=100; Put entries totalling > 100 bytes.

**Assertions:** Total cache size never exceeds MaxBytes; earliest-written keys are evicted first.

---

## FILE-TEST-007: TestOversizedEntrySkipped

**Scenario:** A single entry larger than MaxBytes is silently skipped (not stored).

**Setup:** MaxBytes=50; Put("big", make([]byte, 100)).

**Assertions:** Get("big") returns nil; cache size unchanged.

---

## FILE-TEST-008: TestGetOrFetchCachesResult

**Scenario:** GetOrFetch stores the fetched value so subsequent Gets return it without calling fetch again.

**Setup:** Open FileCache; key absent. Fetch returns []byte("v").

**Assertions:** Second Get("k") returns "v" without invoking fetch.

---

## FILE-TEST-009: TestGetOrFetchSingleflight

**Scenario:** Concurrent GetOrFetch calls for the same key invoke fetch exactly once.

**Setup:** Start N goroutines calling GetOrFetch for the same key simultaneously.

**Assertions:** Fetch called once; all goroutines receive the same value.

---

## FILE-TEST-010: TestGetOrFetchIndependentCopies

**Scenario:** GetOrFetch returns independent copies — mutating the result does not affect cached data.

**Setup:** GetOrFetch returns v; mutate returned slice.

**Assertions:** Subsequent Get returns the original unmodified value.

---

## FILE-TEST-011: TestPersistsAcrossReopen

**Scenario:** Data written to a FileCache survives close and reopen.

**Setup:** Put("k", v); close cache; reopen same directory.

**Assertions:** Get("k") returns v after reopen.

---

## FILE-TEST-012: TestGetOrFetchFetchErrorNotCached

**Scenario:** If the fetch function returns an error, the result is not cached.

**Setup:** GetOrFetch with fetch that returns nil, error.

**Assertions:** Error is propagated; subsequent Get("k") still returns nil.

---

## FILE-TEST-013: TestCloseNilIsNoop

**Scenario:** Closing a nil cache is a safe no-op.

**Setup:** var c *FileCache = nil.

**Assertions:** c.Close() returns nil without panicking.

---

## FILE-TEST-014: TestOpenInvalidConfig

**Scenario:** Open with an invalid configuration (e.g., zero MaxBytes with Enabled=true) returns an error.

**Setup:** Open(Config{Enabled: true, MaxBytes: 0}).

**Assertions:** Returned error is non-nil.

---

## FILE-TEST-015: TestMultipleKeyTypes

**Scenario:** Keys of varying lengths and content hash to distinct cache entries.

**Setup:** Put N distinct keys with distinct values.

**Assertions:** Get on each key returns the corresponding value without cross-contamination.

---

## FILE-TEST-016: TestConcurrentPuts

**Scenario:** Concurrent Put calls from multiple goroutines do not corrupt cache state.

**Setup:** N goroutines each putting a distinct key concurrently.

**Assertions:** After all goroutines finish, each key is retrievable; no race detected.

---

## FILE-TEST-017: TestCacheDir

**Scenario:** The cache directory is created if it does not exist.

**Setup:** Open with a non-existent CacheDir.

**Assertions:** Open succeeds; Put/Get work correctly.

---

## FILE-TEST-018: TestFileLayoutUsesSha256Subdirs

**Scenario:** Cached files are stored in SHA-256-based subdirectories to prevent inode exhaustion.

**Setup:** Put several keys; inspect on-disk layout.

**Assertions:** Files are distributed across subdirectory prefixes matching the SHA-256 hash.

---

## FILE-TEST-019: TestSurvivesReopen

**Scenario:** A FileCache recovered from an existing directory reads existing entries correctly.

**Setup:** Put values; close; reopen; verify Gets.

**Assertions:** All pre-close values are accessible post-reopen.

---

## FILE-TEST-020: TestBulkEvictionTo90Percent

**Scenario:** When eviction is triggered, entries are removed until total size is below 90% of MaxBytes.

**Setup:** Fill cache to > MaxBytes; trigger eviction.

**Assertions:** Post-eviction size <= 0.9 * MaxBytes.

---

## FILE-TEST-021: TestConcurrentPutsAndGetsNoRace

**Scenario:** Mixed concurrent Get and Put operations produce no data races.

**Setup:** N goroutines doing Put; M goroutines doing Get; run with -race.

**Assertions:** Race detector reports no violations.

---

## FILE-TEST-022: TestCorruptFileSkippedOnLoad

**Scenario:** A corrupt on-disk entry (bad header or truncated data) is skipped and treated as a miss.

**Setup:** Write a malformed file into the cache directory; open cache.

**Assertions:** Get for the corrupt key returns nil; no error propagated to caller.

---

## FILE-TEST-023: TestConcurrentSameKeyPutNoCorruption

**Scenario:** Concurrent Puts for the same key from multiple goroutines do not corrupt the stored value.

**Setup:** N goroutines Put("k", v) concurrently.

**Assertions:** Post-Put Get("k") returns a valid value; no panic or partial write.
