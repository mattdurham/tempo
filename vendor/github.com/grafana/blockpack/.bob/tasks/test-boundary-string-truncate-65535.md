# Test: Slice truncation edge case

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

String values are silently truncated at 65535 chars with check `if len(strVal) > 65535`. No test creates a string of exactly 65535, 65536 characters to verify truncation.

## Risk

String column values silently corrupted above 65K chars without error or warning.

## Location

File: `internal/modules/blockio/writer/intrinsic_accum.go:388`
Function: Dict column accumulation

## Test Design

Write dict entry with 65535-char string (should fit), 65536-char string (should truncate), and 100000-char string (should truncate). Roundtrip and verify values are correct or truncated as expected.

## Notes

Test both at and above boundary. Verify no data loss indication (e.g., no error thrown).
