# Sketch Package — Test Plan

## HyperLogLog Tests

### SK-T-01: TestHLL_EmptyCardinality
Verify that a freshly constructed HLL returns Cardinality() == 0.
Satisfies SPEC-SK-02.

### SK-T-02: TestHLL_AddOne
Verify that after adding one element, Cardinality() > 0.
Satisfies SPEC-SK-01 (no panic) and basic Add functionality.

### SK-T-03: TestHLL_AddMany
Add 1000 distinct values. Verify Cardinality() is in range [700, 1300] (within 30% of true count).
Satisfies SPEC-SK-01 and validates estimation quality.

### SK-T-04: TestHLL_MarshalRoundTrip
Marshal and Unmarshal an HLL with values. Verify:
1. Marshal returns exactly 16 bytes (SPEC-SK-03)
2. Unmarshal succeeds
3. Cardinality() after unmarshal equals original (SPEC-SK-05)

### SK-T-05: TestHLL_UnmarshalBadLength
Verify Unmarshal returns non-nil error for:
1. Empty slice
2. Slice of 15 bytes
3. Slice of 17 bytes
Satisfies SPEC-SK-04.

## Count-Min Sketch Tests

### SK-T-06: TestCMS_EmptyEstimate
Verify Estimate() returns 0 for any key on a freshly constructed CMS.
Satisfies SPEC-SK-07.

### SK-T-07: TestCMS_AddEstimate
Add a value 5 times. Verify Estimate() >= 5 (over-estimate guarantee).
Satisfies SPEC-SK-08.

### SK-T-08: TestCMS_MarshalRoundTrip
Marshal and Unmarshal a CMS with values. Verify:
1. Marshal returns exactly 512 bytes (SPEC-SK-09)
2. Unmarshal succeeds
3. Estimate(v) after unmarshal equals original for added value (SPEC-SK-11)

### SK-T-09: TestCMS_UnmarshalBadLength
Verify Unmarshal returns non-nil error for:
1. Empty slice
2. Slice of 511 bytes
3. Slice of 513 bytes
Satisfies SPEC-SK-10.

### SK-T-10: TestCMS_NeverFalseNegative
Add 100 distinct values with count=1. Verify that for all 100 values, Estimate() >= 1.
Satisfies SPEC-SK-08 (over-estimate guarantee implies no false negatives).

## BinaryFuse8 Tests

### SK-T-11: TestFuse8_EmptyFilter
Verify NewBinaryFuse8(nil) succeeds and Contains() returns false.
Satisfies SPEC-SK-15.

### SK-T-12: TestFuse8_ContainsAllKeys
Build a filter with 100 keys. Verify Contains(k) == true for all 100 keys.
Satisfies SPEC-SK-12 and SPEC-SK-13.

### SK-T-13: TestFuse8_NoFalseNegatives
Build a filter with 1000 keys. Verify Contains(k) == true for all 1000 keys.
Satisfies SPEC-SK-12 (no false negatives guarantee).

### SK-T-14: TestFuse8_MarshalRoundTrip
Build a filter, marshal, unmarshal into new filter. Verify all original keys still return
Contains == true. Satisfies SPEC-SK-14.

### SK-T-15: TestFuse8_UnmarshalBadData
Verify UnmarshalBinary returns non-nil error for data shorter than 28 bytes
(minimum: 8 seed + 4 segLen + 4 segLenMask + 4 segCount + 4 segCountLen + 4 fpLen).

### SK-T-16: TestFuse8_DuplicateKeys
Build a filter with duplicate keys (e.g., [1,1,1,1,1]). Verify:
1. Construction succeeds (no error)
2. Contains(1) == true
Satisfies SPEC-SK-15 (deduplication before construction).

## TopK Tests

### SK-T-17: TestTopK_Empty
Verify that a freshly constructed TopK returns nil from Entries().

### SK-T-18: TestTopK_AddAndEntries
Add values with varying frequencies. Verify Entries() returns them sorted by count descending.
Satisfies SPEC-SK-17.

### SK-T-19: TestTopK_ExceedsK
Add more than TopKSize (20) distinct values. Verify Entries() returns at most TopKSize entries.
Satisfies SPEC-SK-17.

### SK-T-20: TestTopK_TruncatesLongKeys
Add a key longer than TopKMaxKeyLen (100) bytes. Verify the stored key is truncated to exactly
TopKMaxKeyLen bytes. Satisfies SPEC-SK-18.

### SK-T-21: TestTopK_MarshalRoundTrip
Marshal and unmarshal a TopK with multiple entries. Verify all entries (key, count) are preserved
exactly. Satisfies SPEC-SK-17 and SPEC-SK-18.

### SK-T-22: TestTopK_EmptyMarshalRoundTrip
Marshal an empty TopK. Verify it produces exactly 1 byte (count=0). Unmarshal and verify no entries.
