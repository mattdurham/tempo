# Fix error-handling: scanDictPagedBlob discards scanDictPageRaw failure signal

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** bug-fix
**severity:** LOW
**source:** bug-finder

## Location

`internal/modules/blockio/shared/intrinsic_codec.go:755` — `scanDictPagedBlob`

## Bug

`scanDictPageRaw` returns `([]BlockRef, bool)`. The bool is false when the page data is corrupt or truncated. In `scanDictPagedBlob` at line 755, the bool is discarded:

```go
result, _ = scanDictPageRaw(pageRaw, blockW, rowW, toc.ColType, matchFn, maxRefs, result)
```

When `scanDictPageRaw` returns false (page decode failure), the function continues to the next page and returns whatever partial refs have been accumulated. This produces an incomplete ref list rather than returning `nil` to signal failure — callers of `ScanDictColumnRefsWithBloom` / `ScanDictColumnRefs` distinguish `nil` (failure/not applicable) from `[]BlockRef{}` (empty result). Returning partial results on corruption is a silent false-negative: matching rows are omitted without any error.

## Trigger

Corrupt or truncated intrinsic column page data. Under normal operation this should not occur. Relevant on disk corruption, truncated writes, or a future writer bug.

## Impact

Silent false negatives: matching spans or log entries are omitted from query results for blocks whose page data is corrupt. No crash, no error returned to caller.

## Fix

Check the bool return and propagate the failure:

```go
var ok bool
result, ok = scanDictPageRaw(pageRaw, blockW, rowW, toc.ColType, matchFn, maxRefs, result)
if !ok {
    return nil // corrupt page — return nil to signal failure to caller
}
```

## Acceptance Criteria

- When `scanDictPageRaw` returns false, `scanDictPagedBlob` returns nil immediately rather than returning partial results.
- All existing tests pass.
- No new functionality introduced.
