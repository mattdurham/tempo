# Cleanup: Extract filterRefsByShardRange helper — shard filter duplicated 4 times

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**severity:** MEDIUM
**source:** discover-arch

## Location

`internal/modules/executor/stream.go`
- Copy 1: lines ~537-550 (`collectIntrinsicPlain`)
- Copy 2: lines ~821-840 (`collectMixedPlain`)
- Copy 3: lines ~925-938 (`collectMixedTopK`)
- Copy 4: inline variant at line ~694 (`collectIntrinsicTopKKLL`, early-continue style)

## What to Extract

The following pattern appears verbatim 3 times (with minor variation for copy 4):

```go
if opts.BlockCount > 0 {
    endBlock := opts.StartBlock + opts.BlockCount
    filtered := refs[:0]
    for _, ref := range refs {
        bi := int(ref.BlockIdx)
        if bi >= opts.StartBlock && bi < endBlock {
            filtered = append(filtered, ref)
        }
    }
    refs = filtered
    if len(refs) == 0 {
        return nil, nil
    }
}
```

Extract as a package-private helper:

```go
// filterRefsByShardRange filters refs to those within [opts.StartBlock, opts.StartBlock+opts.BlockCount).
// Returns the filtered slice. Returns nil when BlockCount is 0 (no sharding active).
// Returns an empty slice when all refs are outside the range.
func filterRefsByShardRange(refs []modules_shared.BlockRef, opts CollectOptions) []modules_shared.BlockRef {
    if opts.BlockCount == 0 {
        return refs
    }
    endBlock := opts.StartBlock + opts.BlockCount
    filtered := refs[:0]
    for _, ref := range refs {
        bi := int(ref.BlockIdx)
        if bi >= opts.StartBlock && bi < endBlock {
            filtered = append(filtered, ref)
        }
    }
    return filtered
}
```

Replace each copy site with:

```go
refs = filterRefsByShardRange(refs, opts)
if len(refs) == 0 {
    return nil, nil
}
```

For copy 4 (`collectIntrinsicTopKKLL`), the current form is:
```go
if opts.BlockCount > 0 && (bi < opts.StartBlock || bi >= opts.StartBlock+opts.BlockCount) {
    continue
}
```
This is an early-continue style rather than a filter. It can remain as-is since it operates on individual refs during a different loop (not a standalone filter pass), OR it can be restructured to use the helper after grouping into `blockRefs`. Either approach is acceptable; preserving the early-continue is less disruptive.

The `int`-slice variant in `Collect` at line ~181 (filtering `plan.SelectedBlocks`) operates on `[]int` not `[]BlockRef` and should remain inline.

## Why

The 2-3 Rule: 4 consumers for an extracted function is above threshold. Any change to the shard filter logic (e.g. supporting stride-based sharding, adding logging) currently requires touching 4 files. The comment "Sort refs and apply shard filter (same as collectIntrinsicPlain)" in `collectMixedPlain` already acknowledges the duplication.

## Acceptance Criteria

- `filterRefsByShardRange` helper exists
- The 3 verbatim copies in `collectIntrinsicPlain`, `collectMixedPlain`, `collectMixedTopK` are replaced
- All tests pass (`make test`)
- No new functionality introduced
