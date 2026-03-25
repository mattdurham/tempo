# objectcache Module — Test Plan

## TEST-OC-001: TestCache_GetMissReturnsNil
*Added: 2026-03-23*
*Covers: SPEC-OC-001*

**Setup:** Declare a zero-value `Cache[testValue]`; call `Get("missing")` on a key that was never stored.

**Assertions:**
- Return value is nil.

---

## TEST-OC-002: TestCache_PutThenGet
*Added: 2026-03-23*
*Covers: SPEC-OC-001, SPEC-OC-002*

**Setup:** Declare a zero-value `Cache[testValue]`; Put a value under `"key1"`; Get it back.

**Assertions:**
- Return value is non-nil.
- `got.n == 42`.

---

## TEST-OC-003: TestCache_GCEviction
*Added: 2026-03-23*
*Covers: SPEC-OC-003*

**Setup:** Put a value via the `//go:noinline putValue` helper so the compiler cannot keep the strong reference live past the call site; force two GC cycles via `runtime.GC()`.

**Assertions:**
- `Get("evictable")` returns nil after GC (weak pointer was reclaimed).

---

## TEST-OC-004: TestCache_StaleKeyCleanedOnGet
*Added: 2026-03-23*
*Covers: SPEC-OC-004*

**Setup:** Put a value, drop the strong reference, force two GC cycles; call `Get("stale")` to trigger lazy deletion; then Put a new value under the same key and Get it.

**Assertions:**
- First `Get` returns nil (GC evicted).
- Second `Put` + `Get` cycle returns the new value (`got.n == 8`), confirming the stale key was cleaned and the slot is reusable.

---

## TEST-OC-005: TestCache_Clear
*Added: 2026-03-23*
*Covers: SPEC-OC-005*

**Setup:** Put two distinct values (`"a"`, `"b"`); call `Clear()`.

**Assertions:**
- `Get("a")` returns nil.
- `Get("b")` returns nil.

---

## TEST-OC-006: TestCache_ConcurrentPutGet
*Added: 2026-03-23*
*Updated: 2026-03-23 — exercises concurrent Put AND Get, not just concurrent Get*
*Covers: SPEC-OC-006*

**Setup:** Spawn 10 goroutines; each goroutine performs 1000 iterations of `Put("shared", v)` followed by `Get("shared")` on a shared `Cache[testValue]`. Run with `-race`.

**Assertions:**
- No data race detected.
- No panic from concurrent access.
- Individual `Get` may return nil if GC ran between Put and Get; this is acceptable per SPEC-OC-001/SPEC-OC-003.

---

## TEST-OC-007: TestCache_OverwriteSameKey
*Added: 2026-03-23*
*Covers: SPEC-OC-002*

**Setup:** Put `v1` under `"k"`, then Put `v2` under `"k"` (overwrite); Get the key.

**Assertions:**
- Return value is non-nil.
- `got.n == 2` (second Put wins).

---

## TEST-OC-008: TestCache_PutNilReturnsError
*Added: 2026-03-23*
*Covers: SPEC-OC-002*

**Setup:** Declare a zero-value `Cache[testValue]`; call `Put("key", nil)`.

**Assertions:**
- The call returns a non-nil error.
- `Get("key")` returns nil (no entry was stored).
