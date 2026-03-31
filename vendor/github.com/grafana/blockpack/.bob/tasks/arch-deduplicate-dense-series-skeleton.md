# Cleanup: Deduplicate dense time-series assembly skeleton in metrics_log.go and metrics_trace.go

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**source:** discover-arch

## Location

`internal/modules/executor/metrics_log.go:279-341` (`logBuildDenseRows`)
`internal/modules/executor/metrics_trace.go:313-389` (`traceBuildDenseSeries`)

## Issue

Both functions share an identical structural skeleton of ~45 lines:

```go
// Step 1: guard
if len(buckets) == 0 { return nil }

// Step 2: compute numBuckets (identical formula)
numBuckets := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
if numBuckets <= 0 { return nil }

// Step 3: collect unique attrGroupKeySet (identical loop + strings.Cut)
attrGroupKeySet := make(map[string]struct{})
for compositeKey := range buckets {
    _, after, _ := strings.Cut(compositeKey, "\x00")
    attrGroupKeySet[after] = struct{}{}
}

// Step 4: sort (identical)
sort.Strings(attrGroupKeys)

// Step 5: iterate attrGroupKey × bucketIdx building compositeKey (identical key format)
compositeKey := strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey
```

The differences are only in the output assembly (log produces `[]LogMetricsRow` with `[]string GroupKey`; trace produces `[]TraceTimeSeries` with `[]TraceMetricLabel` and an extra `sort.Slice` on labels).

## What to Do

Extract a shared helper that returns the sorted list of distinct attrGroupKeys and the computed numBuckets:

```go
// collectGroupKeys returns the sorted unique attr-group-key strings from buckets
// and the number of time buckets. Returns (nil, 0) if the result set is empty.
// compositeKey format: "bucketIdxStr\x00attrGroupKey[\x00extra]"
func collectGroupKeys(buckets map[string]*aggBucketState, tb vm.TimeBucketSpec) ([]string, int64) {
    if len(buckets) == 0 {
        return nil, 0
    }
    numBuckets := int64(0)
    if tb.Enabled && tb.StepSizeNanos > 0 {
        numBuckets = (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
    }
    if numBuckets <= 0 {
        return nil, 0
    }
    keySet := make(map[string]struct{})
    for compositeKey := range buckets {
        _, after, ok := strings.Cut(compositeKey, "\x00")
        if !ok {
            continue
        }
        keySet[after] = struct{}{}
    }
    if len(keySet) == 0 {
        return nil, 0
    }
    keys := make([]string, 0, len(keySet))
    for k := range keySet {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    return keys, numBuckets
}
```

Then simplify both `logBuildDenseRows` and `traceBuildDenseSeries` to call `collectGroupKeys` and skip straight to their output-type-specific assembly.

Note: the HISTOGRAM path in `traceAccumulateRow` uses a 3-segment key (`bucketIdx\x00attrGroupKey\x00boundary`). The `strings.Cut` on `\x00` still produces the correct `after` segment (it cuts at the FIRST `\x00`), so the existing logic naturally handles HISTOGRAM composite keys. Verify this invariant is preserved after refactor.

## Acceptance Criteria

- `collectGroupKeys` helper exists and is called by both `logBuildDenseRows` and `traceBuildDenseSeries`.
- ~35 lines removed total.
- All existing metrics tests pass.
- `make precommit` passes.
- No new functionality introduced.
