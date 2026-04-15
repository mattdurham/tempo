# chaincache — Test Plan

This document defines the required tests for the `internal/modules/chaincache` package.
All tests use in-memory cache tiers to exercise the full multi-tier delegation logic.

---

## CHAIN-TEST-001: TestGet_HitInFirstTier

**Scenario:** A key present in tier 0 is returned without consulting tier 1.

**Setup:** Two-tier chain (tier0, tier1). Prime tier0 with key "k".

**Assertions:** Get("k") returns the value; tier1.Get is never called.

---

## CHAIN-TEST-002: TestGet_HitInSecondTier_WritesBackToFirst

**Scenario:** A key absent in tier 0 but present in tier 1 is returned and written back to tier 0.

**Setup:** Two-tier chain. Key present only in tier1.

**Assertions:** Get("k") returns the value; tier0.Put is called with the same value.

---

## CHAIN-TEST-003: TestGet_MissAllTiers

**Scenario:** A key absent in all tiers returns nil without error.

**Setup:** Two-tier chain; key not present in either tier.

**Assertions:** Get("k") returns nil, nil.

---

## CHAIN-TEST-004: TestGetOrFetch_FetchesOnMiss

**Scenario:** When all tiers miss, the fetch function is called and its result is stored in all tiers.

**Setup:** Two-tier chain; key absent. Fetch returns []byte("value").

**Assertions:** GetOrFetch("k", fetch) returns "value"; both tiers contain "k" afterward.

---

## CHAIN-TEST-005: TestGetOrFetch_NoFetchOnHit

**Scenario:** When tier 0 has the key, the fetch function is never called.

**Setup:** Two-tier chain; key present in tier0.

**Assertions:** GetOrFetch("k", fetch) returns cached value; fetch is not invoked.

---

## CHAIN-TEST-006: TestPut_WritesToAllTiers

**Scenario:** Put propagates the value to every tier in the chain.

**Setup:** Two-tier chain.

**Assertions:** After Put("k", v), both tier0.Get("k") and tier1.Get("k") return v.

---

## CHAIN-TEST-007: TestClose_ClosesAllTiers

**Scenario:** Close calls Close on every tier and returns joined errors.

**Setup:** Two-tier chain where tier1.Close returns an error.

**Assertions:** Chain.Close() returns a non-nil error containing tier1's error.

---

## CHAIN-TEST-008: TestGet_PropagatesError

**Scenario:** A non-miss error from a tier is propagated to the caller.

**Setup:** tier0.Get returns a non-nil, non-miss error.

**Assertions:** Chain.Get("k") returns that error.

---

## CHAIN-TEST-009: TestEmptyChain

**Scenario:** A chain constructed with zero (or all-nil) tiers behaves as a NopCache.

**Setup:** New([]Cache{}) or New([]Cache{nil}).

**Assertions:** Get returns nil; Put returns nil; Close returns nil.

---

## CHAIN-TEST-010: TestNilTiersFilteredInNew

**Scenario:** Nil tiers are silently dropped during construction.

**Setup:** New([]Cache{nil, realTier, nil}).

**Assertions:** Chain operates as a single-tier chain over realTier; no panics.

---

## CHAIN-TEST-011: TestNilReceiverClose

**Scenario:** Calling Close on a nil *ChainCache is a safe no-op.

**Setup:** var c *ChainCache = nil.

**Assertions:** c.Close() returns nil without panicking.
