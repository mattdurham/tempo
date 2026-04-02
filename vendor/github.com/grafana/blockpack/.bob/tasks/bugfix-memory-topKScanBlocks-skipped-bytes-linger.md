# Fix resource-leak: topKScanBlocks retains skipped block bytes after group fetch

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** bug-fix
**severity:** MEDIUM
**source:** bug-finder

## Location

`internal/modules/executor/stream_topk.go:159-184` — `topKScanBlocks`

## Bug

When `topKSkipBlock` returns true for a block (line 161), the code calls `delete(fetched, blockIdx)` intending to "release bytes if already fetched in this group". This is correct when the group has already been fetched. However, if the group has NOT yet been fetched (the skipped block appears BEFORE any non-skipped block from the same group in `plan.SelectedBlocks`), the delete is a no-op.

Later, when a non-skipped block from the same group is encountered, `maps.Copy(fetched, groupRaw)` copies ALL blocks from the group into `fetched` — including the previously skipped block. Since the skipped block is never processed, its bytes are never `delete`d from `fetched` and persist for the remainder of the function.

For comparison, `iterateLogRows` in `stream_log_topk.go` correctly handles this by maintaining a `skippedBlocks` set and deleting those entries from `fetched` immediately after `maps.Copy`.

With 8 MB coalesced groups and selective topK queries that skip many blocks, this can hold multiple MB of decoded block bytes unnecessarily until the function returns.

## Trigger

`Collect()` call with `TimestampColumn != ""` and `Limit > 0` (topK path via `topKScanBlocks`), where multiple blocks share a coalesced group and a block is skipped by `topKSkipBlock` before its group is fetched.

## Impact

Elevated peak memory usage during topK scans. Memory is released when the function returns (no permanent leak). Increases GC pressure in high-throughput environments.

## Fix

Mirror the `skippedBlocks` tracking from `iterateLogRows`:

```go
skippedBlocks := make(map[int]bool)

for _, blockIdx := range plan.SelectedBlocks {
    meta := r.BlockMeta(blockIdx)
    if topKSkipBlock(buf, opts.Limit, backward, meta) {
        gi2, ok2 := blockToGroup[blockIdx]
        if ok2 && fetchedGroupsSeen[gi2] {
            delete(fetched, blockIdx)
        } else {
            skippedBlocks[blockIdx] = true
        }
        continue
    }

    gi, ok := blockToGroup[blockIdx]
    if !ok {
        continue
    }
    if !fetchedGroupsSeen[gi] {
        groupRaw, fetchErr := r.ReadGroup(groups[gi])
        if fetchErr != nil {
            return fetchCount, fmt.Errorf("ReadGroup: %w", fetchErr)
        }
        maps.Copy(fetched, groupRaw)
        for _, bi := range groups[gi].BlockIDs {
            if skippedBlocks[bi] {
                delete(fetched, bi)
            }
        }
        fetchCount += len(groups[gi].BlockIDs)
        fetchedGroupsSeen[gi] = true
    }
    // ... rest unchanged
```

## Acceptance Criteria

- Bytes for blocks skipped by `topKSkipBlock` are removed from `fetched` promptly (either before or immediately after the group is fetched).
- All existing tests pass.
- No new functionality introduced.
