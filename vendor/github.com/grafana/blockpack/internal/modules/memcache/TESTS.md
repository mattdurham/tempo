# memcache — Test Plan

This document defines the required tests for the `internal/modules/memcache` package.
Tests exercise the Memcache-backed Cache implementation, including key hashing, transient
error handling, nil-receiver safety, and singleflight deduplication.

---

## MEM-TEST-001: TestGetMiss

**Scenario:** Get for a key absent in memcache returns nil without error.

**Setup:** Configured MemCache; key not stored.

**Assertions:** Get("missing") returns nil, nil.

---

## MEM-TEST-002: TestPutGet

**Scenario:** A value written with Put is retrievable with Get.

**Setup:** Put("k", []byte("hello")); Get("k").

**Assertions:** Get returns []byte("hello").

---

## MEM-TEST-003: TestGetReturnsCopy

**Scenario:** Get returns an independent copy — mutating the result does not corrupt cached data.

**Setup:** Put("k", v); Get; mutate returned slice.

**Assertions:** Subsequent Get("k") returns the original unmodified value.

---

## MEM-TEST-004: TestGetOrFetch_Hit

**Scenario:** GetOrFetch returns the cached value and does not invoke fetch.

**Setup:** Put("k", v); call GetOrFetch("k", fetch).

**Assertions:** Returns v; fetch is never called.

---

## MEM-TEST-005: TestGetOrFetch_Miss

**Scenario:** GetOrFetch calls fetch on a cache miss and stores the result.

**Setup:** Key absent; fetch returns []byte("fetched").

**Assertions:** Returns "fetched"; subsequent Get("k") returns "fetched".

---

## MEM-TEST-006: TestTransientErrorTreatedAsMiss

**Scenario:** A connection error from memcache is treated as a miss, not a hard error.

**Setup:** MemCache configured with unreachable server; Get("k").

**Assertions:** Get returns nil, nil (transient error swallowed).

---

## MEM-TEST-007: TestNilReceiver

**Scenario:** A nil *MemCache is a safe NopCache — all operations are no-ops.

**Setup:** var c *MemCache = nil.

**Assertions:** Get returns nil; Put returns nil; GetOrFetch calls fetch; Close returns nil.

---

## MEM-TEST-008: TestOpen_Disabled

**Scenario:** Open with Enabled=false returns nil without error.

**Setup:** Open(Config{Enabled: false}).

**Assertions:** Returned cache is nil; error is nil.

---

## MEM-TEST-009: TestOpen_MissingServers

**Scenario:** Open with Enabled=true but empty Servers list returns an error.

**Setup:** Open(Config{Enabled: true, Servers: []string{}}).

**Assertions:** Returned error is non-nil.

---

## MEM-TEST-010: TestMemcacheKey_AlwaysValid

**Scenario:** Arbitrary cache keys (including those with spaces, unicode, or long strings) are
hashed to valid memcache keys (printable ASCII, <= 250 bytes, no whitespace).

**Setup:** Generate keys with spaces, control characters, very long strings, unicode codepoints.

**Assertions:** hashKey(k) produces a string that is accepted by the memcache protocol validator.
