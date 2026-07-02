# Test: collectIntrinsicTopKScan with TimeRange filtering

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** test-gap-analysis

## Gap

The scan path in `collectIntrinsicTopKScan` (triggered when >= 8000 refs) does not apply time-range filtering to selected refs. The KLL path (< 8000 refs) correctly applies this filter at lines 729-738. No test exercises intrinsic-topk-scan path + non-zero TimeRange, so the bug was not caught.

## Risk

Rows outside the requested time window are returned when using the scan path. Wrong query results — time filtering is silently ignored for large result sets.

## Location

File: `internal/modules/executor/stream.go:764-806`
Function: `collectIntrinsicTopKScan`

## Test Design

Force the scan path via `SortScanThreshold = 1` (so even small ref counts trigger the scan path). Build test data with multiple spans having distinct timestamps [100, 200, 300, 400, 500]. Run Collect with:
- `Limit=10`
- `TimestampColumn="span:start"`
- `TimeRange={MinNano: 250, MaxNano: 450}`

Verify:
1. ExecutionPath is "intrinsic-topk-scan"
2. Exactly 2 rows returned (timestamps 300 and 400 within [250, 450])
3. All returned rows have timestamps within the range

Without the fix, rows with timestamps 100, 200, and 500 would incorrectly be included.

## Notes

Do NOT call `t.Parallel()` since test modifies global `SortScanThreshold`. Use cleanup func to restore value. See test-gap-analysis.md for detailed implementation guidance.
