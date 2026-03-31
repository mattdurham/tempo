# Code Review: Blockpack Intrinsic Optimization -- Efficiency Analysis

**Date:** 2026-03-29
**Branch:** blockpack-integration
**Reviewer:** Claude Opus 4.6 (code-reviewer agent)
**Scope:** Efficiency review of intrinsic ref-filter, stream collect, and predicate scanning

---

## Summary

Focused efficiency review of the intrinsic optimization hot paths: `DecodePagedColumnBlobFiltered`,
`EnsureRefIndex`/`LookupRefFast`, `collectIntrinsicPlain`/`TopK`, `lookupIntrinsicFields`,
`scanDecodedDictRefs`, `scanDecodedFlatRangeRefs`, and `BlockRefsFromIntrinsicTOC` overFetch logic.

**Findings:** 9 issues (1 high, 2 medium-high, 1 medium, 2 low-medium, 3 very low)

**Top priority:** Findings 4 and 5 -- full-column tsMap construction on hot paths allocates
~130MB of short-lived memory per file per query when only K (typically 20) lookups are needed.
The fix is straightforward using existing `EnsureRefIndex` + `LookupRefFast` infrastructure.

---

## Findings

### 1. DEAD PARAMETER: refFilter in DecodePagedColumnBlobFiltered

**WHAT:** The `refFilter map[uint32]struct{}` parameter is accepted, never used, and explicitly
discarded on line 114 with `_ = refFilter`. The caller (`GetIntrinsicColumnForRefs` at
intrinsic_reader.go:229) still builds the refFilter map, allocating memory for no purpose.

**WHERE:** `internal/modules/blockio/shared/intrinsic_ref_filter.go:30` (parameter),
`internal/modules/blockio/reader/intrinsic_reader.go:229-232` (caller allocates map)

**WHY:** After NOTE-007 removed page-skipping, refFilter serves no purpose. The caller
allocates a `map[uint32]struct{}` sized to `len(refs)` on every call (~48KB for 1000 refs)
that is immediately discarded.

**SEVERITY:** Low. `GetIntrinsicColumnForRefs` has no callers in the executor.

**SUGGESTION:** Remove the parameter from `DecodePagedColumnBlobFiltered` and stop building
the map in `GetIntrinsicColumnForRefs`. This is internal code with no external API contract.

---

### 2. DEAD CODE: LookupRef O(N) linear scan method

**WHAT:** `LookupRef` (lines 208-243) performs O(N) linear scan. It has zero production callers
-- all executor paths use `LookupRefFast` (O(log N) via `EnsureRefIndex`).

**WHERE:** `internal/modules/blockio/shared/intrinsic_ref_filter.go:208-243`

**WHY:** Superseded by `LookupRefFast`. Risks accidental use by future contributors.

**SEVERITY:** Low.

**SUGGESTION:** Add `// Deprecated:` annotation pointing to `LookupRefFast`, or remove.

---

### 3. UNBOUNDED OVERFETCH: overFetch = limit * 10000

**WHAT:** In `BlockRefsFromIntrinsicTOC` (predicates.go:1915), multi-condition AND queries set
`overFetch = limit * 10000`. With limit=20 this is 200K; with limit=100 this is 1M. Each
condition returns up to `overFetch` BlockRefs (4 bytes each), then `intersectBlockRefSets`
sorts each set at O(N log N).

**WHERE:** `internal/modules/executor/predicates.go:1915`

**WHY:** No upper bound. For 3-node AND with limit=100: 3 sets x 1M refs x 4 bytes = 12MB
allocation + 3 sorts of 1M elements. The factor 10,000 assumes joint selectivity >= 0.5%
but provides no cap.

**SEVERITY:** Medium. Production Tempo uses limit=20 (200K refs = ~800KB per condition), but
custom limits or many-condition queries could cause multi-MB allocations.

**SUGGESTION:** Cap with a hard maximum: `overFetch = min(limit*10000, 500_000)`. This bounds
worst-case to ~2MB per condition while still providing 0.004% joint selectivity for limit=20.

---

### 4. HOT PATH ALLOCATION: collectIntrinsicTopKScan builds full tsMap for time-range filter

**WHAT:** When the scan path selects refs and a time range is active (lines 995-1019), it
decodes the full timestamp column via `GetIntrinsicColumn` (cached, free on warm queries)
then builds `tsMap := make(map[uint32]uint64, len(tsCol.BlockRefs))`. For 3.3M spans this
allocates ~130MB (40 bytes/entry: 4-byte key + 8-byte value + ~28 bytes map overhead).
This map is used only to look up `selected` refs, which is bounded by `limit` (typically 20).

**WHERE:** `internal/modules/executor/stream.go:998-1001`

**WHY:** Building a 3.3M-entry map to look up 20 keys is ~6500x over-provisioned. The map
is short-lived (function-scoped), creating ~130MB of GC pressure per file per query.

**SEVERITY:** High. With 50 concurrent queries x 10 files = 65GB of GC pressure.

**SUGGESTION:** Use `EnsureRefIndex` + `LookupRefFast` on the cached timestamp column:

```go
// Replace lines 996-1018 with:
if len(selected) > 0 && (opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0) {
    tsCol, tsErr := r.GetIntrinsicColumn(opts.TimestampColumn)
    if tsErr == nil && tsCol != nil {
        tsCol.EnsureRefIndex()
        filtered := selected[:0]
        for _, ref := range selected {
            key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
            val, ok := tsCol.LookupRefFast(key)
            if !ok {
                continue
            }
            ts := val.(uint64)
            if opts.TimeRange.MinNano > 0 && ts < opts.TimeRange.MinNano {
                continue
            }
            if opts.TimeRange.MaxNano > 0 && ts > opts.TimeRange.MaxNano {
                continue
            }
            filtered = append(filtered, ref)
        }
        selected = filtered
    }
}
```

This gives O(K log N) for K=20 lookups with zero additional allocation (refIndex is cached
on the column via sync.Once).

---

### 5. HOT PATH ALLOCATION: collectIntrinsicTopKKLL builds full tsMap for small ref sets

**WHAT:** `collectIntrinsicTopKKLL` (stream.go:865-868) builds `tsMap` from the full timestamp
column (3.3M entries, ~130MB) even though `refs` is bounded by `SortScanThreshold` (8000).
Only 8000 lookups are performed against the 3.3M-entry map.

**WHERE:** `internal/modules/executor/stream.go:865-868`

**WHY:** Same root cause as Finding 4. The full column is cached (objectcache), but the map is
rebuilt on every query invocation. O(N) construction + ~130MB allocation for O(M) lookups where
M << N.

**SEVERITY:** Medium-High. Same GC pressure as Finding 4, triggered on the KLL path.

**SUGGESTION:** Use `EnsureRefIndex` + `LookupRefFast` instead of building tsMap. The KLL path
needs timestamps for sorting, so build a local `pairs` slice directly:

```go
tsCol.EnsureRefIndex()
for _, ref := range refs {
    key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
    val, ok := tsCol.LookupRefFast(key)
    if !ok {
        continue
    }
    pairs = append(pairs, refTS{ref: ref, ts: val.(uint64)})
}
```

This replaces O(N) map construction with O(M log N) lookups (M=8000, N=3.3M), eliminating
the 130MB allocation entirely.

---

### 6. MISSED CACHE: scanDecodedDictRefs uses raw regexp.Compile

**WHAT:** `scanDecodedDictRefs` (predicates.go:2039) calls `regexp.Compile(leaf.Pattern)`
directly despite `cachedRegexCompile` existing at predicates.go:67.

**WHERE:** `internal/modules/executor/predicates.go:2039`
Also `internal/modules/executor/predicates.go:669` (another direct `regexp.Compile` call).

**WHY:** Called per-file during `BlockRefsFromIntrinsicTOC`. For a regex query across 100
files, the same pattern is compiled 100 times (~1-10us each = 0.1-1ms total).

**SEVERITY:** Low-Medium. Easy fix.

**SUGGESTION:** Replace `regexp.Compile(leaf.Pattern)` with `cachedRegexCompile(leaf.Pattern)`
at both locations.

---

### 7. REDUNDANT ALLOCATION: scanDecodedDictRefs allocates both wantStr and wantInt

**WHAT:** Lines 2061-2062 unconditionally allocate both `wantStr` and `wantInt` maps.
Only one is ever consulted based on `isInt`.

**WHERE:** `internal/modules/executor/predicates.go:2061-2062`

**WHY:** One map per call is wasted (~200 bytes for typical 1-3 element predicates).

**SEVERITY:** Very Low.

**SUGGESTION:** Conditionally allocate: `if isInt { wantInt = make(...) } else { wantStr = make(...) }`.

---

### 8. UNBOUNDED CACHE: intrinsicRegexCache sync.Map grows forever

**WHAT:** `intrinsicRegexCache` is a package-level `sync.Map` with no eviction or size limit.

**WHERE:** `internal/modules/executor/predicates.go:62`

**WHY:** In a long-running process, distinct regex patterns accumulate monotonically. Each
compiled `*regexp.Regexp` holds AST and buffers. Practically bounded by user behavior.

**SEVERITY:** Very Low. No action needed unless profiling shows growth.

**SUGGESTION:** Monitor; replace with bounded LRU if needed.

---

### 9. SUBOPTIMAL INIT: DecodePagedColumnBlobFiltered zero-length slice allocation

**WHAT:** Lines 58-60 initialize flat column slices as `make([]T, 0)` instead of pre-sizing
from TOC metadata. The TOC `RowCount` per page is available before the decode loop.

**WHERE:** `internal/modules/blockio/shared/intrinsic_ref_filter.go:58-60`

**WHY:** Forces append to grow from zero, causing log2(N) reallocations. Could pre-compute
total rows from `toc.Pages[i].RowCount` and allocate once.

**SEVERITY:** Very Low. Amortized doubling cost is small.

**SUGGESTION:** Sum `RowCount` across pages and pre-allocate with that capacity.

---

## Priority Summary

| # | Finding | Severity | Estimated Impact |
|---|---------|----------|-----------------|
| 4 | Full tsMap in scan path time-filter | **High** | ~130MB alloc/file/query |
| 5 | Full tsMap in KLL path | **Medium-High** | ~130MB alloc/file/query |
| 3 | overFetch unbounded for high limits | Medium | Latent OOM for custom limits |
| 6 | Missing cachedRegexCompile | Low-Medium | ~0.1-1ms wasted CPU per query |
| 1 | refFilter dead parameter | Low | ~48KB wasted per call |
| 2 | LookupRef dead code | Low | Maintenance risk |
| 7 | Dual map allocation | Very Low | ~200 bytes |
| 8 | Unbounded regex cache | Very Low | Theoretical |
| 9 | Zero-length slice init | Very Low | Minor reallocation |

**Recommended fix order:** 4, 5 (same pattern, one fix), then 3 (add cap), then 6 (one-line change).
