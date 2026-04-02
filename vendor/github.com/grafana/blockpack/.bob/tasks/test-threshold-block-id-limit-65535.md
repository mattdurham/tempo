# Test: Block ID limit (65535 blocks)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Block IDs are uint16, capped at 65534 (IDs are 0-based). Writer.go line 542 checks `if blockID >= 65535`, but no test reaches blockID=65534, 65535, or 65536 to verify the boundary is enforced correctly.

## Risk

Might accept blockID=65535 when it should reject, or reject 65534 incorrectly, causing data loss or corruption.

## Location

File: `internal/modules/blockio/writer/writer.go:542`
Function: `Write` or block splitting logic

## Test Design

Write exactly 65535 spans distributed to trigger block 65534 (the last valid ID); verify succeeds. Write 65536 spans; verify error is returned with clear message. Check that the error message is informative.

## Notes

Must generate enough spans to trigger creation of many blocks. May need to set MaxSpansPerBlock to a small value to reach the block limit efficiently.
