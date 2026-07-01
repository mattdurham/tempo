# Test: Unicode in field names and values

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

No test covers Unicode in logfmt field names (`日本語=value`), JSON keys (`{"中文": "value"}`), emoji in values, or right-to-left text. Unicode handling varies by parser.

## Risk

Unicode handling varies by parser; potential data loss or incorrect field extraction.

## Location

File: `internal/logqlparser/pipeline.go`, `internal/traceqlparser/parser.go`
Function: JSON/logfmt parsing

## Test Design

Parse log bodies with various Unicode: field names in logfmt, JSON keys in Chinese, emoji in values, RTL text. Verify fields extracted correctly and values preserve encoding.

## Notes

Test multiple Unicode scenarios: Japanese field names, Chinese JSON keys, emoji, Arabic RTL text. Verify encoding is preserved through parsing and storage.
