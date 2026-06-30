# Test: Nested braces in log body

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

No test for JSON with deeply nested braces, escaped braces in string values, or unmatched braces in raw log line.

## Risk

Parser confuses brace nesting or fails to handle escaped chars.

## Location

File: `internal/logqlparser/pipeline.go`
Function: JSON stage parsing

## Test Design

Parse log with deeply nested JSON (`{"data": {"nested": {"deep": "value"}}}`). Verify all levels extracted. Parse log with escaped braces in values (`{"msg": "use {braces}"}`). Verify preserved.

## Notes

Test multiple nesting depths and escaped character scenarios.
