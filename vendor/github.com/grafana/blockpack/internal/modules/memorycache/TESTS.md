# memorycache — Test Plan

This document defines the required tests for the `internal/modules/memorycache` package.
Tests exercise LRU eviction, byte-budget enforcement, nil-receiver safety, and concurrent
singleflight deduplication.

---

## MEMO-TEST-001: TestNew_InvalidMaxBytes

**Scenario:** Constructing a MemoryCache with MaxBytes <= 0 returns an error.

**Setup:** New(Config{MaxBytes: 0}) and New(Config{MaxBytes: -1}).

**Assertions:** Both return a non-nil error; returned cache is nil.

---

## MEMO-TEST-002: TestGetMiss

**Scenario:** Get for an absent key returns nil without error.

**Setup:** Empty MemoryCache; Get("missing").

**Assertions:** Returns nil, nil.

---

## MEMO-TEST-003: TestPutGet

**Scenario:** A value written with Put is retrievable with Get.

**Setup:** Put("k", []byte("hello")); Get("k").

**Assertions:** Get returns []byte("hello").

---

## MEMO-TEST-004: TestGetReturnsCopy

**Scenario:** Get returns an independent copy — mutating the result does not corrupt cached data.

**Setup:** Put("k", v); Get; mutate returned slice.

**Assertions:** Subsequent Get("k") returns the original unmodified value.

---

## MEMO-TEST-005: TestEviction

**Scenario:** When total stored bytes exceed MaxBytes, the least-recently-used entries are evicted.

**Setup:** MaxBytes=100; Put entries totalling > 100 bytes.

**Assertions:** Total cache byte size <= MaxBytes; oldest-accessed keys are removed first.

---

## MEMO-TEST-006: TestLRUPromotion

**Scenario:** Accessing a key promotes it to most-recently-used, protecting it from eviction.

**Setup:** Put A, B, C; Get A; Put D (triggers eviction). B should be evicted before A.

**Assertions:** Get("a") returns value; Get("b") returns nil.

---

## MEMO-TEST-007: TestGetOrFetch_Hit

**Scenario:** GetOrFetch returns the cached value and does not invoke fetch.

**Setup:** Put("k", v); call GetOrFetch("k", fetch).

**Assertions:** Returns v; fetch is never called.

---

## MEMO-TEST-008: TestGetOrFetch_Miss

**Scenario:** GetOrFetch calls fetch on a cache miss and stores the result.

**Setup:** Key absent; fetch returns []byte("fetched").

**Assertions:** Returns "fetched"; subsequent Get("k") returns "fetched".

---

## MEMO-TEST-009: TestGetOrFetch_Concurrent

**Scenario:** Concurrent GetOrFetch calls for the same key invoke fetch exactly once.

**Setup:** N goroutines call GetOrFetch for the same absent key simultaneously.

**Assertions:** Fetch called once; all goroutines receive the same value; race detector passes.

---

## MEMO-TEST-010: TestNilReceiver

**Scenario:** A nil *MemoryCache is a safe NopCache — all operations are no-ops.

**Setup:** var c *MemoryCache = nil.

**Assertions:** Get returns nil; Put returns nil; GetOrFetch calls fetch; Close returns nil.

---

## MEMO-TEST-011: TestOversizedEntryDropped

**Scenario:** A single entry larger than MaxBytes is silently dropped — not stored, no error.

**Setup:** MaxBytes=50; Put("big", make([]byte, 100)).

**Assertions:** Put returns nil; Get("big") returns nil.
