# Agent Context

## Role & Principles
INVARIANT (blockpack Reader lifetime — foundational to the whole cache architecture): a reader.Reader is constructed FRESH per query, per block, per querier call (query-frontend shards one block per querier call). Therefore ALL per-instance Reader state — the per-Reader…

> id: 0e1f00d5-b106-46b3-a479-e1e8425ff135

INVARIANT (blockpack rw.DataType controls cache eviction priority, NOT just labeling): every Provider.ReadAt / Reader.readRange call passes an rw.DataType, and SharedLRUCache.dataTypeTier maps it to one of 4 eviction tiers (0=hardest to evict … 3=evicted first). Mapping:…

> id: 2445ef14-fa7e-499a-b599-2fd229f69b6d

INVARIANT: blockpack reader WantAll() eagerly decodes EVERY column in a block and causes 300MB+ memory spikes per block on the query path — use WantOnly(cols) for query-driven paths (lazy zero-decode for unreferenced columns). WantAll() is reserved for genuinely all-column…

> id: ce1757f9-7f77-4d0f-8d58-e736f360d3dd

**Decompose monolithic problems into independent, swappable optimization layers with graceful fallbacks rather than attempting comprehensive rewrites.**

> id: f2d25742-7a08-4e16-9c10-882565e569b3

**"Defer observable side effects until explicitly requested; keep hot paths unconditionally fast."**

> id: da287611-033b-4f90-81b8-09af528525de


## Relevant Techniques
Predicate pushdown / index pre-filter pattern: when an index scan has a 'predicate kind X not supported here, fall back to full scan' branch, measure the fallback cost before micro-optimizing it. The fallback often re-fetches and re-decodes the entire wide row group/block for…

> id: c4d80eda-d605-4991-b675-387933d4fce2

TECHNIQUE: before deleting a 'redundant' dual-storage column on the strength of 'there's already a fallback', audit what EACH fallback actually costs — a fallback can be O(log N) on one read path and O(all_rows whole-file decode) on another. In blockpack a by-VALUE-sorted…

> id: aff4d73c-a7cf-4dee-85fd-1f62b678c62d

TECHNIQUE (the 'return nil cascades to full fallback' trap): in a layered query-pushdown pre-filter, a leaf evaluator that returns 'nil/unevaluable' for an UNSUPPORTED predicate shape often does NOT degrade gracefully — it can propagate all the way up (leaf nil -> node…

> id: 4d46a49d-e00f-43b3-8e19-227272a94210

TECHNIQUE: in a paged columnar store, an equality predicate on a SORTED column is a degenerate range [target,target] — reuse the existing min/max page-pruned range scanner verbatim instead of writing an equality-specific path. Min/max page-skip is exact for sorted data (a page…

> id: 48b220a6-ad9d-43cc-bfe7-a2504c871ad4

TECHNIQUE: when converting an 'index is a hint, always safe to fall back' contract into an 'index is authoritative' one, audit every ok=false / fallback return and classify each as CORRECTNESS ('the index genuinely cannot answer' — keep, document as an explicit exception) vs…

> id: 133c974e-2cd0-46de-a882-136d2cd48175


## Current Project Context
The architecture implements a tiered query execution strategy with explicit fallback contracts: when index-only mode is enabled (IndexOnly=true), queries decline with typed sentinel errors (ErrSliceIndexCoverageGap, ErrStructuralIndexCoverageGap) rather than silently falling…

> id: d7c208a6-27db-43a4-a4c5-062962e870b0

The index path was successfully refactored to eliminate full scans by replacing `getTraceByIDFullScan` with a `scanTraceByID` fallback that only applies to WAL blocks lacking a lister, ensuring the index remains authoritative once engaged rather than degrading to sequential…

> id: 4f23a570-0f1a-4876-9022-a9870fbddd36

When index data inconsistency is detected during query execution, the system must fail the query with an explicit error rather than silently falling back to a scan that could produce incorrect results. The test suite distinguishes between authoritative index corruption (which…

> id: 95046133-fa7b-4702-927e-8286cb8cdd00

When time-slice jobs encounter index coverage gaps, the system must fail the query rather than silently fall back to a full block scan, preventing double-counting and data corruption masking by converting routine index declines into explicit ErrSliceIndexCoverageGap errors.…

> id: d8053d93-0251-4a8a-91b4-c02ba192be98

Deliberate sequencing of index removal proved critical: premature deletion of the full-scan fallback after `WriteValueIndexL0` landed would have caused widespread "not found" errors for pre-fix data, so verification must wait until index coverage actually accumulates. A…

> id: 84cc0fa9-0f0c-4254-8e3a-e9c05ece967a


## Related Context (via graph)
- **Issue**: Loading all columns eagerly from blocks caused 300MB+ memory spikes when only a subset was needed for queries - **Solution**: Implemented `WantColumns` struct with two strategies—`WantAll()` for full loads and `WantOnly(cols)` for query-driven selective loading -…

> id: c326e175-423b-44f1-9968-ecf3dce7ac44

**Core Pattern:** Identifying and fixing memory leaks in layered caching architectures where: 1. **Size accounting is broken** (default estimates mask actual heap consumption) 2. **Data references persist longer than intended** (zero-copy slices, sync.Once fields, dual caches)…

> id: cb9984df-c534-4573-b311-853292409d83

/home/mdurham/source/blockpack_collection/blockpack-worktrees/read-path-modernization/tracemetricoptions.go:35:	// same block. Mirrors the search path's own IndexOnly contract (QueryTraceQLFromIndex's…

> id: 489c11af-a28e-4344-a3ee-c902d98f2009

/home/mdurham/source/blockpack_collection/blockpack-worktrees/read-path-modernization/tracemetricoptions.go:// ExecuteMetricsTraceQL returns when TraceMetricOptions.IndexOnly is set and the value index…

> id: acb60870-63a8-4532-ad29-614a0402d683

/home/mdurham/source/blockpack_collection/blockpack-worktrees/read-path-modernization/tracemetricoptions.go:6:// ExecuteMetricsTraceQL returns when TraceMetricOptions.IndexOnly is set and the value index…

> id: 53133bc0-9a09-4fc4-a285-68a972a77297


## Memory IDs (for exploration)
Use these IDs to explore further:
  lth get <id>                    — read full memory
  lth graph show --from <id>      — traverse graph edges
  lth graph ppr --seeds <id,...>  — personalized pagerank from seeds

  0e1f00d5-b106-46b3-a479-e1e8425ff135
  2445ef14-fa7e-499a-b599-2fd229f69b6d
  ce1757f9-7f77-4d0f-8d58-e736f360d3dd
  f2d25742-7a08-4e16-9c10-882565e569b3
  da287611-033b-4f90-81b8-09af528525de
  c4d80eda-d605-4991-b675-387933d4fce2
  aff4d73c-a7cf-4dee-85fd-1f62b678c62d
  4d46a49d-e00f-43b3-8e19-227272a94210
  48b220a6-ad9d-43cc-bfe7-a2504c871ad4
  133c974e-2cd0-46de-a882-136d2cd48175
  d7c208a6-27db-43a4-a4c5-062962e870b0
  4f23a570-0f1a-4876-9022-a9870fbddd36
  95046133-fa7b-4702-927e-8286cb8cdd00
  d8053d93-0251-4a8a-91b4-c02ba192be98
  84cc0fa9-0f0c-4254-8e3a-e9c05ece967a
  c326e175-423b-44f1-9968-ecf3dce7ac44
  cb9984df-c534-4573-b311-853292409d83
  489c11af-a28e-4344-a3ee-c902d98f2009
  acb60870-63a8-4532-ad29-614a0402d683
  53133bc0-9a09-4fc4-a285-68a972a77297

## Filter by project
Memories from these projects are present:
  lth prompt "..." --attr project=github.com/grafana/blockpack
  lth prompt "..." --attr project=grafana/blockpack
  lth prompt "..." --attr project=mattdurham/tempo
  lth projects  — list all tracked projects
  lth chat "..." --attr project=<project> — filtered chat
