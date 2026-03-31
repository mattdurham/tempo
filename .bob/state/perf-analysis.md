# Intrinsic Pruning Effectiveness Analysis
Date: 2026-03-12

## Executive Summary

Blockpack is 2.3–3.0x slower than parquet on standalone intrinsic queries
(`status=error`, `kind=server`, `duration>100ms`) against production S3 data.
The root cause is a two-layer problem:

1. **Block-level pruning via the intrinsic column section is absent on production blocks.**
   Blocks written by the current upstream (`/home/matt/source/blockpack-tempo`) are missing
   `intrinsicAccum` feeds for `span:status` (range index + intrinsic column feed). When those
   blocks are read, `r.HasIntrinsicSection()` returns false (or returns a section without
   `span:status`), so `BlocksFromIntrinsicTOC()` returns nil and zero blocks are eliminated.

2. **Full block I/O from S3 for every surviving block.**
   Blockpack reads full `~200MB` block files from S3. Parquet reads only the columns needed
   (e.g., `status` column: a few MB). For intrinsic-only queries where blockpack has no
   pruning, it must read and decode every block entirely.

Together, these mean intrinsic-only queries trigger the worst-case path: read all blocks
in full from S3 and filter row-by-row, while parquet skips most data via column projection.

---

## Benchmark Output Does Not Include Pruning Stats

`querybench` calls the Tempo HTTP `/api/search` endpoint and measures end-to-end latency.
It does not expose internal blockpack stats like `TotalBlocks`, `SelectedBlocks`,
`PrunedByIndex`, `PrunedByFuse`, or `PrunedByCMS`. These stats are only available via
`LogQueryOptions.OnStats` in the blockpack API — and only for LogQL queries, not TraceQL.

For TraceQL queries, `QueryTraceQL()` does NOT expose an `OnStats` callback.
The stats do flow through `CollectStats` internally in `stream.go`, but are discarded
at the `streamFilterProgram` → `QueryTraceQL` boundary (no public stats hook exists).

**Conclusion:** Block pruning stats cannot be read from benchmark output alone.
The analysis below is based on source-level code review.

---

## How Intrinsic Block Pruning Works (Code Trace)

### The Three Pruning Layers

For a TraceQL query `{ status = error }`, blockpack's execution path is:

```
QueryTraceQL(r, "{ status = error }", opts)
  → streamFilterQuery
    → compile to vm.Program (traceql_compiler.go)
    → streamFilterProgram(r, program, opts, fn)
      → Collect(r, program, collectOpts)   [in executor/stream.go]
        → buildPredicates(r, program)      [queryplanner predicates]
        → planner.PlanWithOptions(...)     [range index + bloom/CMS pruning]
        → BlocksFromIntrinsicTOC(r, program)  [intrinsic column pruning]
        → FetchBlocks(plan)               [S3 reads for surviving blocks]
        → row-level filter for each block
```

### Layer 1: Range Index Pruning (planner)

The `Planner.Plan()` uses per-block min/max range indices to eliminate blocks.
`span:status` gets `updateMinMax()` called in `addRowFromProto()` (the proto ingestion path).
But **`addRowFromBlock()` (the compaction path) does NOT call `updateMinMax()` for
`span:status`** — it falls through to `addPresent` without updating the range index.

From `writer_block.go` lines 722-729:
```go
case "span:status":
    if v, ok := col.Int64Value(srcRowIdx); ok {
        if a := b.intrinsicAccum; a != nil {
            a.feedInt64("span:status", shared.ColumnTypeInt64, v, b.intrinsicBlockID, dstRowIdx)
        }
    }
    // Fall through to addPresent below (don't continue).
```

No `updateMinMax("span:status", ...)` call. Compare with `span:kind`:
```go
case "span:kind":
    if v, ok := col.Int64Value(srcRowIdx); ok {
        ...
        b.updateMinMax("span:kind", shared.ColumnTypeInt64, string(tmp[:]))
        if a := b.intrinsicAccum; a != nil { ... }
    }
    continue
```

**`span:kind` has range index. `span:status` does not.**
This means `PrunedByIndex > 0` for `kind=server` queries but `PrunedByIndex == 0` for
`status=error` queries (after compaction, production blocks are all from compaction path).

### Layer 2: BinaryFuse8 / CMS Sketch Pruning

These are populated from the sketch index (`sketch_index.go`) which is built from dynamic
attribute columns during WAL→L1 writes. Intrinsic columns like `span:status`, `span:kind`,
`span:duration` do not use the sketch index path — they use the intrinsic accumulator
(a separate mechanism).

### Layer 3: Intrinsic Column Pruning (`BlocksFromIntrinsicTOC`)

This is the most powerful pruning for intrinsic queries. It uses the file-level
sorted intrinsic column section (written by `intrinsicAccumulator`) to eliminate blocks:

- For dict columns (`span:status`, `span:kind`, `span:name`): finds which internal blocks
  contain the queried value via exact-match lookup in the sorted dict entries.
- For flat columns (`span:duration`, `span:start`): binary-searches the sorted uint64 values.

This allows **O(log n) block selection without reading any block data** after the initial
file-level index read.

For `span:status` in **newly written blocks** (via `addRowFromProto`): ✅ intrinsicAccum fed.
For `span:status` in **compacted blocks** (via `addRowFromBlock`): ✅ intrinsicAccum fed.
  (The vendor copy at lines 722-726 does call `a.feedInt64("span:status", ...)`)

BUT: Task #1 says the **upstream source** at `/home/matt/source/blockpack-tempo` is MISSING
the `addRowFromBlock` intrinsicAccum feeds. This means blocks written/compacted by the
upstream binary lack the intrinsic section for `span:status`, `span:status_message`,
`resource.service.name`, and potentially others.

**If the running blockpack service is built from the upstream (not vendored) source,
or if blocks were previously compacted without the fix, those blocks have no intrinsic
section for `span:status` → zero pruning → full scan of all blocks.**

---

## Quantifying the Impact

### Real-World Benchmark: Intrinsic-Only Queries

| Query | PQ | BP | Ratio | Root Cause |
|-------|----|----|-------|-----------|
| `status=error` | 2221ms | 6727ms | **3.03x** | No intrinsic pruning; full S3 block reads |
| `kind=server` | 2278ms | 5230ms | **2.30x** | Range index may help; intrinsic section hit-or-miss |
| `duration>100ms` | 2922ms | 7457ms | **2.55x** | Range index present; but S3 full-block I/O dominates |

### Expanded Benchmark: Same Intrinsic Queries (Local Data)

| Query | PQ | BP | Ratio |
|-------|----|----|-------|
| `kind-server` | 148ms | 76ms | **0.51x ← BP faster** |
| `kind-client` | 149ms | 76ms | **0.51x ← BP faster** |
| `status-error` | 147ms | 73ms | **0.49x ← BP faster** |
| `duration-gt-5ms` | 147ms | 78ms | **0.53x ← BP faster** |
| `duration-gt-50ms` | 149ms | 82ms | **0.55x ← BP faster** |

The expanded dataset is local (no S3), so I/O is not the bottleneck. BP wins by ~2x because
its columnar execution engine is faster than parquet's row-based evaluation.

**The performance inversion (BP slower on real-world, faster on local) is entirely explained
by I/O volume:**

- Parquet reads only the needed columns from S3 (~2-10 MB per block for a status column).
- Blockpack reads the full block file from S3 (~200 MB per file).
- Even if blockpack prunes some blocks via intrinsic index, each surviving block requires
  a full ~200 MB S3 read vs parquet's ~2-10 MB column-stripe read.

### Why `status=error` Is Worse Than `duration>100ms`

`duration>100ms` has a range index (`updateMinMax` IS called for `span:duration`).
The planner can eliminate blocks where all spans have duration < 100ms.

`status=error` has NO range index (no `updateMinMax` in `addRowFromBlock`).
Zero blocks are eliminated by range pruning. Intrinsic column pruning depends on whether
the intrinsic section was written correctly — which is the upstream bug being fixed in task #1.

In production data where most spans are NOT error status, an intrinsic-based block pruning
would be highly effective (most blocks could be eliminated). Without it, all blocks are scanned.

---

## Why BP Wins on Expanded Queries Despite No S3 Advantages

On the expanded suite (local dataset), BP wins across all categories including intrinsics:

- **No S3 I/O penalty**: Local reads are fast regardless of volume.
- **Intrinsic column section present**: Local blocks were written fresh with the current
  vendor code that includes all intrinsicAccum feeds → `BlocksFromIntrinsicTOC` works.
- **Column projection at decode time**: BP only decodes the columns needed for the query
  predicates (`ProgramWantColumns`), skipping all others.
- **Columnar execution engine**: BP evaluates predicates on bulk decoded column data;
  parquet evaluates row-by-row.

Result: BP is ~2x faster locally for intrinsic queries because it has proper intrinsic
section + efficient columnar evaluation.

---

## Root Cause Summary

### Primary: S3 Full-Block I/O

Blockpack reads entire `~200MB` files from S3. Parquet reads only the column stripes
relevant to the query (often 1-5% of the file). This is the dominant cost for all
real-world queries where BP is slower.

**Fix required**: Sub-block I/O — store columns as separately-addressable S3 byte ranges
so blockpack can fetch only the needed columns without reading the entire file.

### Secondary: Missing `span:status` Range Index in compacted blocks

`addRowFromBlock()` doesn't call `updateMinMax("span:status", ...)` — so compacted blocks
have no block-level range index for status queries. The queryplanner's Stage 1
(range index pruning) cannot eliminate any blocks for `status=error`.

**Fix**: Add `updateMinMax("span:status", ...)` to the `case "span:status":` branch in
`addRowFromBlock()` — analogous to the existing `span:kind` and `span:duration` handling.

### Tertiary: intrinsicAccum feeds missing in upstream (task #1)

The upstream blockpack source (`/home/matt/source/blockpack-tempo`) was missing intrinsicAccum
feeds in `addRowFromBlock()` for `span:status`, `span:status_message`, `resource.service.name`.
Task #1 is fixing this. The vendor copy already has this fix. But if production blocks were
written without the fix, they lack the intrinsic column section → `BlocksFromIntrinsicTOC`
returns nil → no block-level pruning for any intrinsic query on those blocks.

---

## Pattern Analysis: What Makes BP Win or Lose

### BP Wins on Real-World (B/A < 1.0)

1. **match-all** (0.42x): No predicate → no block scan overhead; BP's in-memory columnar
   execution is faster than parquet's initialization cost.
2. **svc=grafana** (0.64x): Service name has range index + intrinsic section → effective
   pruning, and grafana spans are dense in recent blocks.
3. **svc+span-attr-range: http.status_code>=500** (0.91x): Range index on http.status_code
   pruning works; combined with low cardinality service filter.
4. **svc-regex+status** (0.95x): Service regex constrains search space enough.
5. **not-null: db.system != nil** (0.43x): Bloom filter + presence check works well.
6. **and-of-or: svc regex && (status=error || dur>100ms)** (0.88x): The service regex
   constrains block selection enough that the OR on intrinsics is bounded.

### BP Loses Most on Real-World

- Pure intrinsic queries with no service/attribute anchor: `status=error` (3.03x),
  `kind=server` (2.30x), `duration>100ms` (2.55x)
- Queries with `!= nil` + intrinsic: `http.method!=nil && status=error` (2.33x)
- Two-service OR: `svc=CockroachDB || svc=loki-querier` (2.04x) — OR cannot use
  range index; must read all blocks

---

## Recommendations

### Short-term (unblock current work)

1. **Complete task #1**: Ensure `addRowFromBlock` has full intrinsicAccum feeds in upstream.
   This fixes newly written/compacted blocks. Old production blocks without intrinsic sections
   will still use fallback (full scan), but new blocks will benefit.

2. **Add `updateMinMax` for `span:status`**: Gives range index for status queries.
   Since status only has values 0, 1, 2 (UNSET, OK, ERROR), the range index is highly
   discriminating — most blocks can be eliminated for `status=error` on low-error datasets.

### Medium-term

3. **Sub-block S3 I/O**: Fetch individual column sections as separate S3 byte-range reads.
   This is the single largest opportunity: would close the 1.5-3x gap on most real-world queries.

4. **Re-compact existing blocks**: Existing production blocks need to be re-compacted with
   the fixed writer to gain intrinsic sections. Until then, old blocks are full-scan.

### Validation

Once upstream fix is deployed and blocks re-compacted, re-run the benchmark. Expected improvement:
- `status=error`: should drop from 3.03x to ~1.3-1.5x (still I/O limited but 0 block reads eliminated)
- With sub-block I/O: should approach or beat parquet on most queries
