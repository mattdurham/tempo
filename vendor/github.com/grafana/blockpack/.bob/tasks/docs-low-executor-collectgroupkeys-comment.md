# Fix executor/metrics_log.go: collectGroupKeys misleading composite key comment

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** LOW
**Source:** discover-docs (round 2)

## Problem

`internal/modules/executor/metrics_log.go` line 280 doc comment says:

```go
// compositeKey format: "bucketIdxStr\x00attrGroupKey[\x00extra]" — cuts at first \x00.
```

The `[\x00extra]` notation implies an optional third fixed segment. There is none. The
actual format is:

```go
attrGroupKey := strings.Join(attrVals, "\x00")        // e.g. "prod\x00us-east"
compositeKey := bucketIdxStr + "\x00" + attrGroupKey  // e.g. "5\x00prod\x00us-east"
```

For multi-group-by queries, `attrGroupKey` itself contains embedded `\x00` separators.
The comment misleadingly suggests a three-part structure when the real structure is
`bucketIdx \x00 (all group-by values joined by \x00)`.

## Fix

Update the comment on line 280 of `internal/modules/executor/metrics_log.go` to:

```go
// collectGroupKeys returns the sorted unique attr-group-key strings from buckets
// and the number of time buckets. Returns (nil, 0) if the result set is empty.
// compositeKey format: "bucketIdxStr\x00groupVal1[\x00groupVal2...]" — cuts at first \x00 to get attrGroupKey.
```

## Acceptance criteria
- The comment accurately describes the key structure without the misleading `[\x00extra]` notation.
