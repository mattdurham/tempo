# Test: CRLF line endings in log body

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

No test covers logfmt with CRLF (`key=value\r\n`), JSON with CRLF inside values, or mixed LF/CRLF in same body.

## Risk

Logfmt/JSON parser fails or includes \r in parsed values, causing field corruption.

## Location

File: `internal/logqlparser/pipeline.go`
Function: JSON/logfmt parsing

## Test Design

Create log entry with body containing CRLF. Parse with logfmt/JSON stage. Verify \r is not included in output values and parsing succeeds.

## Notes

Test multiple formats: logfmt with CRLF, JSON with CRLF in string values, mixed line endings. Verify field extraction is correct.
