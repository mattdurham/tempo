# Test: Dict ref width boundary (256 blocks/rows)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Dict refs use 1 byte (0-255) or 2 bytes (256+) depending on max block/row indices. Lines 222-228 in intrinsic_accum.go have checks `if maxBlock > 255` and `if maxRow > 255`, but no explicit unit tests verify these width calculations at the boundary (e.g., blockIdx=255 vs 256).

## Risk

Off-by-one errors: blockIdx=255 vs 256 could serialize/deserialize incorrectly, causing data corruption or incorrect field values.

## Location

File: `internal/modules/blockio/writer/intrinsic_accum.go:222-228`
Function: `dictRefWidths`

## Test Design

Build dict columns with exactly 255, 256, and 257 unique blocks and rows respectively. Serialize and roundtrip through the writer/reader. Verify the ref width (1 or 2 bytes) matches expectations and values deserialize correctly.

## Notes

Manually construct intrinsic dictionaries with controlled block/row counts. Inspect serialized bytes to verify width encoding. Test both dimensions (blockIdx and rowIdx) independently.
