# blockio/shared — Test Specifications

This document defines the required tests for the `internal/modules/blockio/shared` package.

---

## Bloom Filter Tests

### SHARED-01: TestBloomEmpty

**Scenario:** An empty bloom filter returns false for all queries.

**Assertions:** `TestBloom(emptyBloom, "anything") == false`,
`TestBloom(emptyBloom, "service.name") == false`,
`TestBloom(emptyBloom, "") == false`.

Back-ref: `shared_test.go:TestBloomEmpty`

---

### SHARED-02: TestBloomAddAndTest

**Scenario:** A name added to the filter tests as present; untouched names test as absent.

**Assertions:** After `AddToBloom(bloom, "http.method")`:
- `TestBloom(bloom, "http.method") == true`
- `TestBloom(bloom, "http.status") == false`
- `TestBloom(bloom, "service.name") == false`

Back-ref: `shared_test.go:TestBloomAddAndTest`

---

### SHARED-03: TestBloomNoFalseNegatives

**Scenario:** 50 randomly generated names are added to a bloom filter; all must test true.

**Assertions:** For every added name, `TestBloom(bloom, name) == true`.

Back-ref: `shared_test.go:TestBloomNoFalseNegatives`

---

### SHARED-04: TestBloomHashDistribution

**Scenario:** Both hash functions produce bit positions in [0, 255] for several typical names.

**Assertions:** `BloomHash1(name) <= 255`, `BloomHash2(name) <= 255` for all test names.

Back-ref: `shared_test.go:TestBloomHashDistribution`

---

## Presence RLE Tests

### SHARED-05: TestPresenceRLEAllPresent (RLE-01)

**Scenario:** All 10 bits set → encode → decode → all 10 bits set.

Back-ref: `shared_test.go:TestPresenceRLEAllPresent`

---

### SHARED-06: TestPresenceRLEAllAbsent (RLE-02)

**Scenario:** No bits set → encode → decode → no bits set.

Back-ref: `shared_test.go:TestPresenceRLEAllAbsent`

---

### SHARED-07: TestPresenceRLEAlternating (RLE-03)

**Scenario:** Alternating bits (20 bits) → encode → decode → matches original.

Back-ref: `shared_test.go:TestPresenceRLEAlternating`

---

### SHARED-08: TestPresenceRLERandom

**Scenario:** 1000 random bits → encode → decode → matches original.

Back-ref: `shared_test.go:TestPresenceRLERandom`

---

### SHARED-09: TestPresenceRLECountPresent

**Scenario:** `CountPresent` returns correct count for known bitsets.

**Assertions:** `CountPresent({0b01010101}, 8) == 4`, `CountPresent({0xFF}, 8) == 8`,
`CountPresent({0xFF}, 5) == 5`.

Back-ref: `shared_test.go:TestPresenceRLECountPresent`

---

### SHARED-10: TestPresenceRLEIsPresent

**Scenario:** `IsPresent` correctly reads individual bits, including out-of-bounds.

**Assertions:** Specific bits at known positions; `IsPresent(bitset, 100) == false` (OOB).

Back-ref: `shared_test.go:TestPresenceRLEIsPresent`

---

### SHARED-11: TestPresenceRLEEmpty

**Scenario:** `nBits = 0` → encode → decode → empty slice, no error.

Back-ref: `shared_test.go:TestPresenceRLEEmpty`

---

### SHARED-12: TestPresenceRLEVersionCheck

**Scenario:** Corrupt version byte in encoded data → `DecodePresenceRLE` returns error.

Back-ref: `shared_test.go:TestPresenceRLEVersionCheck`

---

## Index RLE Tests

### SHARED-13: TestIndexRLEAllSame

**Scenario:** All-same value slice → encode → decode → matches original.

Back-ref: `shared_test.go:TestIndexRLEAllSame`

---

### SHARED-14: TestIndexRLERandom

**Scenario:** 200 random uint32 values → encode → decode → matches original.

Back-ref: `shared_test.go:TestIndexRLERandom`

---

### SHARED-15: TestIndexRLEEmpty

**Scenario:** Empty slice → encode → decode → empty slice, no error.

Back-ref: `shared_test.go:TestIndexRLEEmpty`

---

### SHARED-16: TestIndexRLEWrongCount

**Scenario:** Request more elements than encoded → error returned.

Back-ref: `shared_test.go:TestIndexRLEWrongCount`

---

## CoalesceConfig Tests

### SHARED-17: TestAggressiveCoalesceConfig

**Scenario:** `AggressiveCoalesceConfig` has `MaxGapBytes == 4MB` and `MaxWasteRatio == 1.0`.

Back-ref: `shared_test.go:TestAggressiveCoalesceConfig`

---

## Coverage Requirements

- All bloom filter functions (`AddToBloom`, `TestBloom`, `BloomHash1`, `BloomHash2`, `SetBit`,
  `IsBitSet`) must be exercised.
- Both RLE codecs must cover round-trip, empty, and error paths.
- `IsPresent` out-of-bounds must be covered (returns false, no panic).
- `AggressiveCoalesceConfig` values must be asserted to catch accidental changes.
