# Test: Footer parse version mismatch

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Footer version is read but wrong version bytes are never tested. No test for footer version 0, 1 (too old), or 5+ (too new).

## Risk

Reader might panic or silently misinterpret footer layout when reading unexpected version.

## Location

File: `internal/modules/blockio/reader/parser.go`
Function: `readFooter` or NewReader

## Test Design

Write a blockpack file, then manually change the footer version byte to 99. Attempt to open the file. Verify proper error message is returned (not panic).

## Notes

Locate footer version byte in binary file. Test multiple invalid versions (0, 1, 5, 99). Verify error message is clear and identifies the version issue.
