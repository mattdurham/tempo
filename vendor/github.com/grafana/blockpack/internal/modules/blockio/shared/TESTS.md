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

## Flat-Column Bounds Check Tests (BUG-1 / BUG-13)

### SHARED-18: TestScanFlatColumnRefs_OversizedRowCount
**Scenario:** Blob with rowCount*8 > len(raw) returns nil without panic.
**Setup:** Hand-crafted snappy blob with rowCount=0x0FFFFFFF, minimal trailing bytes.
**Assertions:** `ScanFlatColumnRefs(blob, ...)` returns nil; no panic.
Back-ref: `intrinsic_codec_bounds_test.go:TestScanFlatColumnRefs_OversizedRowCount`

### SHARED-19: TestScanFlatColumnTopKRefs_OversizedRowCount
**Scenario:** Same oversized rowCount triggers early return in TopKRefs variant.
**Assertions:** `ScanFlatColumnTopKRefs(blob, 10, false)` returns nil; no panic.
Back-ref: `intrinsic_codec_bounds_test.go:TestScanFlatColumnTopKRefs_OversizedRowCount`

### SHARED-20: TestScanFlatColumnRefsFiltered_OversizedRowCount
**Scenario:** Same oversized rowCount triggers early return in Filtered variant with a non-nil filter callback.
**Setup:** Hand-crafted snappy blob with rowCount=0x0FFFFFFF, minimal trailing bytes; non-nil filter callback that always returns true.
**Assertions:** `ScanFlatColumnRefsFiltered(blob, false, 10, func(_ BlockRef) bool { return true })` returns nil; no panic.
Back-ref: `intrinsic_codec_bounds_test.go:TestScanFlatColumnRefsFiltered_OversizedRowCount`

### SHARED-21: TestDecodeVariableWidthRef_BlockWZero
**Scenario:** `decodeVariableWidthRef` with blockW=0 returns an error.
**Assertions:** error is non-nil; no garbage BlockRef produced.
Back-ref: `intrinsic_codec_bounds_test.go:TestDecodeVariableWidthRef_BlockWZero`

### SHARED-22: TestDecodeVariableWidthRef_BlockWThree / TestDecodeVariableWidthRef_RowWZero
**Scenario:** Invalid width values (3, 0) each return an error from `decodeVariableWidthRef`.
**Assertions:** error is non-nil for blockW=3; error is non-nil for rowW=0.
Back-ref: `intrinsic_codec_bounds_test.go:TestDecodeVariableWidthRef_BlockWThree`, `TestDecodeVariableWidthRef_RowWZero`

### SHARED-23: TestScanFlat*_InvalidBlockW
**Scenario:** Blob encoding blockW=0 causes each flat-scan function to return nil.
**Assertions:** All three scan functions return nil for blockW=0; no garbage refs.
Back-ref: `intrinsic_codec_bounds_test.go:TestScanFlatColumnRefs_InvalidBlockW`,
  `TestScanFlatColumnTopKRefs_InvalidBlockW`, `TestScanFlatColumnRefsFiltered_InvalidBlockW`

---

## Intrinsic Buffer Pool Tests (NOTE-011)

### SHARED-24: TestAcquireIntrinsicBuf_NonNil
**Scenario:** AcquireIntrinsicBuf returns a non-nil *[]byte with a non-nil backing slice.
**Assertions:** `bp != nil`, `*bp != nil`.
Back-ref: `intrinsic_pool_test.go:TestAcquireIntrinsicBuf_NonNil`

---

### SHARED-25: TestReleaseIntrinsicBuf_ResetsLen
**Scenario:** A buffer with 3 appended bytes is released; the next acquire returns len==0.
**Assertions:** After `ReleaseIntrinsicBuf(bp)`, `AcquireIntrinsicBuf()` returns `len==0`.
Back-ref: `intrinsic_pool_test.go:TestReleaseIntrinsicBuf_ResetsLen`

---

### SHARED-26: TestReleaseIntrinsicBuf_CapGuard
**Scenario:** An oversized buffer (5MB > 4MB cap guard) is released; the pool discards it
and returns a fresh 64KB buffer on the next acquire.
**Assertions:** Reacquired buffer `len==0`; `cap <= 4MB`.
Back-ref: `intrinsic_pool_test.go:TestReleaseIntrinsicBuf_CapGuard`

---

### SHARED-27: TestIntrinsicBufPool_DecodeCorrectness
**Scenario:** A snappy-compressed blob is decoded using a pooled buffer; decoded bytes match
the original input.
**Assertions:** `snappy.Decode(*bp, compressed)` produces bytes equal to original.
Back-ref: `intrinsic_pool_test.go:TestIntrinsicBufPool_DecodeCorrectness`

---

### SHARED-28: TestDecodeFlatPage_BytesAreCopied
**Scenario:** `DecodeFlatPage` returns BytesValues that are independent copies of the raw
input buffer. Mutating the raw input after decode must not affect the returned BytesValues.
**Assertions:** `col.BytesValues[0]` equals original value after mutating `payload[2]`.
Back-ref: `intrinsic_pool_test.go:TestDecodeFlatPage_BytesAreCopied`

---

## Coverage Requirements

- All bloom filter functions (`AddToBloom`, `TestBloom`, `BloomHash1`, `BloomHash2`, `SetBit`,
  `IsBitSet`) must be exercised.
- Both RLE codecs must cover round-trip, empty, and error paths.
- `IsPresent` out-of-bounds must be covered (returns false, no panic).
- `AggressiveCoalesceConfig` values must be asserted to catch accidental changes.
