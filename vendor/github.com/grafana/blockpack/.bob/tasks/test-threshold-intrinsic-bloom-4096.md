# Test: Intrinsic bloom size clamping (4096 bytes max)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Bloom filter size is clamped at 4096 bytes with formula checking `if b > 4096 { return 4096 }`. Tests exist for very small and very large itemCounts, but no test exercises the exact boundary where itemCount computes to exactly 4095, 4096, or 4097 bytes.

## Risk

Saturation at 4096 bytes could mask off-by-one errors in the growth formula, or the boundary check could be incorrect (e.g., `>=` vs `>`).

## Location

File: `internal/modules/blockio/shared/intrinsic_bloom.go:26`
Function: `IntrinsicBloomSize`

## Test Design

Calculate itemCount values that yield 4095, 4096, and 4097 bytes respectively using the formula. Write intrinsic columns with those itemCounts and verify bloom buffer sizes match expectations. Check clamping occurs at the right boundary.

## Notes

Reverse-engineer the bloom size formula to find exact itemCount boundaries. Test multiple values around 4096 to ensure off-by-one errors are caught.
