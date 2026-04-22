# executor — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/executor` package.
All benchmarks pre-build the reader outside the timed loop.

---

## Metric Targets (Never Regress Below)

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| Execute latency (100 spans, 1 block, match-all) | < 500 µs | 500 µs–2 ms | > 2 ms |
| Execute latency (1000 spans, 10 blocks, match-all) | < 5 ms | 5–20 ms | > 20 ms |
| Matches/sec (match-all, in-memory) | > 500K | 100K–500K | < 100K |

---

## BENCH-EX-01: BenchmarkExecute_MatchAll
*Status: not yet implemented*

Measures end-to-end Execute cost for `{}` against an in-memory file.

**Setup (outside timed loop):**
- Write N spans with `MaxBlockSpans=100`.
- Open reader with in-memory provider.
- Compile `{}` program once.

**Variants:**

| Sub-benchmark | Spans | Blocks |
|---------------|-------|--------|
| `_100spans`   | 100   | 1      |
| `_1000spans`  | 1000  | 10     |

**Required custom metrics:**
```go
b.ReportMetric(float64(len(result.Matches)), "matches")
b.ReportMetric(float64(b.N*len(result.Matches))/elapsed.Seconds(), "matches/sec")
```

---

## BENCH-EX-02: BenchmarkExecute_ServiceFilter
*Status: not yet implemented*

Measures Execute cost for a service-name equality filter that selects ~10% of spans.

**Setup (outside timed loop):**
- Write 1000 spans across 10 service names (100 each), `MaxBlockSpans=100`.
- Compile `{ resource.service.name = "svc-0" }` program.

**Required custom metrics:**
```go
b.ReportMetric(float64(result.BlocksScanned), "blocks_scanned")
b.ReportMetric(float64(result.BytesRead)/float64(b.N), "bytes_read/op")
```

---

## BENCH-EX-04: BenchmarkCollectStream_AllocCount
*Added: 2026-04-08*

Measures rowSet `growslice` allocations in the `collectStream` path, exercised by a full-scan
`executor.Collect` call against a 200-span single-block file.

**Setup (outside timed loop):**
- Write 200 spans with `MaxBlockSpans=201` (single block).
- Compile match-all program `{}`.
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench=BenchmarkCollectStream_AllocCount -benchmem -count=3 ./internal/modules/executor/
```

**Baseline (post-prealloc):** ~304 allocs/op — regression threshold.
This serves as the upper bound for allocations on the full-scan path with `newRowSetWithCap`
preallocated to `SpanCount()`.

Back-ref: `internal/modules/executor/prealloc_bench_test.go:BenchmarkCollectStream_AllocCount`

---

## BENCH-EX-05: BenchmarkTraceAccumulateRow_AllocCount
*Added: 2026-04-08*

Measures per-call allocations in the `traceAccumulateRow` hot path by running
`ExecuteTraceMetrics` with a 1-element GroupBy over 200 matching spans in a single block.

**Setup (outside timed loop):**
- Write 200 spans with `MaxBlockSpans=201` (single block), cycling `resource.env` across 3 values.
- Compile `{ } | count_over_time() by (resource.env)` with 1s buckets.
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench=BenchmarkTraceAccumulateRow_AllocCount -benchmem -count=3 ./internal/modules/executor/
```

**Baseline (post-compositeKey pool + Join elimination):** ~325 allocs/op — regression threshold.

History:
- Pre-NOTE-054: ~452 allocs/op
- After NOTE-054 (attrVals hoist, 2026-04-14): ~252 allocs/op
- Pre-#228 / pre-#230 (intervening streamHistogramGroupBy + other additions): ~523 allocs/op
- After NOTE-071 (compositeKey pool) + NOTE-073 (strings.Join elimination): ~325 allocs/op

Why 325 and not lower: this benchmark uses 200 spans all in bucket 0 with 3 cycling
resource.env values, so there are 3 map-misses per `b.Loop()` iteration (not 1). The
remaining ~322 allocs are per-call fixed overhead (block parsing, rowSet, series
building). The BENCH-EX-09 HISTOGRAM variant (same setup) is the definitive map-hit
zero-alloc regression guard — it isolates the per-span path.

Back-ref: `internal/modules/executor/prealloc_bench_test.go:BenchmarkTraceAccumulateRow_AllocCount`,
          `internal/modules/executor/metrics_trace.go:traceAccumulateRow`

---

## BENCH-EX-09: BenchmarkTraceAccumulateRow_HISTOGRAM_AllocCount
*Added: 2026-04-16. Renumbered from BENCH-EX-08 during PR combination — #229 already took BENCH-EX-08 for BenchmarkTraceMetrics_FilteredIntrinsic_AllocCount.*

Measures per-call allocations in the HISTOGRAM path of `traceAccumulateRow`. Uses 200
spans with `span:duration`, cycling `resource.env` across 3 values, 1s step buckets,
HISTOGRAM function. All spans have identical duration (500µs), mapping to the same
histogram bucket, so 3 map-misses per iteration (one per env value) and ~197 map-hits.

**Target:** 0 allocs/op on map-hit. 1 alloc/op on map-miss (stored key).

**Run command:**
```
go test -bench=BenchmarkTraceAccumulateRow_HISTOGRAM -benchmem -count=3 ./internal/modules/executor/
```

**Baseline (post-compositeKey pool):** ~325 allocs/op. Before fix: ~523 allocs/op (+198 from compositeKey allocs).

**Back-ref:** `internal/modules/executor/prealloc_bench_test.go:BenchmarkTraceAccumulateRow_HISTOGRAM_AllocCount`

---

## BENCH-EX-06: BenchmarkLogAccumulateRow_AllocCount
*Added: 2026-04-08*

Measures per-call allocations in the `logAccumulateRow` hot path by running
`ExecuteLogMetrics` with a 1-element GroupBy over 200 matching log records in a single block.

**Setup (outside timed loop):**
- Write 200 log records with `MaxBlockSpans=201` (single block), all with `service.name=svc`.
- Query `{service.name = "svc"}` with `count_over_time` and `GroupBy=["service.name"]`, 1s buckets.
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench=BenchmarkLogAccumulateRow_AllocCount -benchmem -count=3 ./internal/modules/executor/
```

**Baseline (post-prealloc):** ~310–318 allocs/op — regression threshold.
Before the prealloc change: +200 allocs/op from `attrVals := make([]string, len(groupBy))` per row.
After: attrVals allocated once per block; `clear(attrVals)` at function entry eliminates per-row alloc.

Back-ref: `internal/modules/executor/prealloc_bench_test.go:BenchmarkLogAccumulateRow_AllocCount`,
          `internal/modules/executor/metrics_log.go:logAccumulateRow`

---

## BENCH-EX-07: BenchmarkCollectIntrinsicTopKKLL_AllocCount
*Added: 2026-04-08*

Measures `blockOrder`/`blockRefs` growslice allocations in `collectIntrinsicTopKKLL` when
refs span multiple distinct blocks. Before the fix, `blockOrder` was preallocated to cap=8,
causing repeated growslice when refs > 8.

**Setup (outside timed loop):**
- Write 200 spans with `MaxBlockSpans=10` (≈20 blocks).
- Compile match-all program; route through intrinsic KLL path with `Limit=100, TimestampColumn="span:start"`.
- Run with `b.ReportAllocs()`.

**What the benchmark measures:** With ≈20 distinct blocks from 200 spans, it verifies that
`blockOrder` is preallocated to `min(len(refs), 64)`, avoiding the former cap=8 growslice
that triggered when refs exceeded 8 distinct blocks.

**Run command:**
```
go test -bench=BenchmarkCollectIntrinsicTopKKLL_AllocCount -benchmem -count=3 ./internal/modules/executor/
```

**Baseline (post-prealloc):** ~1023 allocs/op — regression threshold.
The `min(len(refs), 64)` cap eliminates growslice overhead for the common case of ≤64 distinct blocks.

Back-ref: `internal/modules/executor/prealloc_bench_test.go:BenchmarkCollectIntrinsicTopKKLL_AllocCount`,
          `internal/modules/executor/stream.go:collectIntrinsicTopKKLL`

---

## BENCH-EX-03: BenchmarkExecute_Limit
*Status: not yet implemented*

Measures the early-exit benefit of `Options.Limit` for large result sets.

**Setup (outside timed loop):**
- Write 10000 spans, `MaxBlockSpans=100`.
- Compile `{}` program.

**Variants:**

| Sub-benchmark | Limit |
|---------------|-------|
| `_limit1`     | 1     |
| `_limit10`    | 10    |
| `_nolimit`    | 0     |

**Required custom metrics:**
```go
b.ReportMetric(float64(result.BlocksScanned), "blocks_scanned")
b.ReportMetric(float64(len(result.Matches)), "matches")
```

---

## BENCH-EX-08: BenchmarkTraceMetrics_FilteredIntrinsic_AllocCount
*Added: 2026-04-16*

Measures per-iteration allocations in `executeTraceMetricsIntrinsic` on the filtered
intrinsic path (predicate filter present, count/rate, no aggregate column), exercising the
`mergeJoinFilteredRefsWithVals` replacement for `filteredKeys map[uint32]struct{}`.

**Setup (outside timed loop):**
- Write 300 spans (200 with `status=error`, 100 with `status=ok`) with `MaxBlockSpans=301`
  (single block).
- Compile `{ status = error } | rate()` with 6 × 60s buckets. Exercises the merge-join
  path (filteredRefs non-empty) and the `streamCountRateNoGroupBy` fast path (count/rate,
  no group-by, no aggregate column).
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench=BenchmarkTraceMetrics_FilteredIntrinsic_AllocCount -benchmem -count=3 ./internal/modules/executor/
```

**Baseline (post-merge-join):** ~76 allocs/op (~15452 B/op) — regression threshold.
All remaining allocations are O(1) per file: filteredRefs clone, refIdx index slice,
outRefs/outVals output slices, buckets map, reader internals. Zero `map[uint32]struct{}`
allocations per iteration (down from O(N) spans before this change).

Before this change (filteredKeys map): ~17.49% CPU from map operations + ~10.66% GC
pressure (Pyroscope profile 2026-04-16).

Back-ref: `internal/modules/executor/metrics_trace_intrinsic_test.go:BenchmarkTraceMetrics_FilteredIntrinsic_AllocCount`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals` (NOTE-070)

---

## BENCH-EX-10: BenchmarkIntrinsicCountRateGroupBy_AllocCount
*Added: 2026-04-17*

Measures per-call allocations for `{ } | rate() by (resource.service.name)` on the
dict-ID count/rate group-by fast path (`streamCountRateGroupByID`). Exercises
`buildGroupIDMap` + `streamCountRateGroupByID` over 300 spans in a single block with
3 cycling service names (3 distinct groups, ~100 spans per group).

**Setup (outside timed loop):**
- Write 300 spans with `MaxBlockSpans=301` (single block), cycling service.name across
  `["svc-a", "svc-b", "svc-c"]`.
- Compile `{ } | rate() by (resource.service.name)` with 1s buckets.
- Open reader with in-memory provider.
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench='BenchmarkIntrinsicCountRateGroupBy_AllocCount' \
    -benchmem -count=5 ./internal/modules/executor/
```

**Baseline (post-dict-ID fast path):** ~46 allocs/op (~34918 B/op) — regression threshold.
End-to-end allocs include fixed per-call overhead (reader block parsing, result construction,
context). Zero per-span string allocations in the `streamCountRateGroupByID` hot loop.

**History:**
- Before dict-ID fast path: string-keyed path hashed ~300 group strings per call.
- After (NOTE-074): `map[groupIDKey]` with `[8]uint32` key; string resolution deferred to
  series-emit (O(unique groups), not O(spans)).
- After (NOTE-082): this benchmark exercises the N=1 fast path (`streamCountRateGroupByIDSingle`)
  since group-by is single-dimension. Allocs/op unchanged (46) — fast path reduces map key
  size from 32 to 4 bytes, improving throughput without changing allocation count.

Back-ref: `internal/modules/executor/intrinsic_group_id_bench_test.go:BenchmarkIntrinsicCountRateGroupBy_AllocCount`

---

## BENCH-EX-11: BenchmarkIntrinsicHistogramGroupBy_AllocCount
*Added: 2026-04-17*

Measures per-call allocations for `{ } | histogram_over_time(span:duration) by (resource.service.name)`
on the dict-ID histogram group-by fast path (`streamHistogramGroupByID`). Exercises
`buildGroupIDMap` + `streamHistogramGroupByID` over 300 spans in a single block with
3 cycling service names. All spans have duration=500000ns mapping to the same histogram
bucket — 3 unique composite keys (1 per service); 3 map-misses + ~297 map-hits per call.

**Setup (outside timed loop):**
- Write 300 spans with `MaxBlockSpans=301` (single block), cycling service.name across
  `["svc-a", "svc-b", "svc-c"]`, all with span duration=500000ns.
- Compile `{ } | histogram_over_time(span:duration) by (resource.service.name)` with 1s buckets.
- Open reader with in-memory provider.
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench='BenchmarkIntrinsicHistogramGroupBy_AllocCount' \
    -benchmem -count=5 ./internal/modules/executor/
```

**Baseline (post-dict-ID fast path):** ~55 allocs/op (~40394 B/op) — regression threshold.
End-to-end allocs include fixed per-call overhead. Zero `strconv.FormatFloat` calls in the
`streamHistogramGroupByID` hot loop — boundary stored as `float64` in `histGroupIDKey`.

**History:**
- Before dict-ID fast path: `FormatFloat` + string concat per span for boundary key;
  string-keyed group map lookup.
- After (NOTE-074): `map[histGroupIDKey]` with `struct { dims [8]uint32; boundary float64;
  bucketIdx int64 }` key; float64 comparison safe because boundary is always power-of-2 or
  0 (SPEC-ETM-13.4).
- After (NOTE-082): this benchmark exercises the N=1 fast path (`streamHistogramGroupByIDSingle`)
  since group-by is single-dimension. Allocs/op unchanged (55) — fast path reduces map key
  size from 48 to 24 bytes, improving throughput without changing allocation count.

Back-ref: `internal/modules/executor/intrinsic_group_id_bench_test.go:BenchmarkIntrinsicHistogramGroupBy_AllocCount`

## BENCH-EX-12: BenchmarkStreamScanEqualAny_ByType
*Added: 2026-04-17*

Per-type sub-benchmarks comparing fast-path vs forced-generic performance for each
dict-encoded column type. Setup: 10K spans, D=8 dict entries cycling, nValues=5 query values.

### Baseline Results (2026-04-17, 13th Gen Intel Core i5-13500)

| Sub-benchmark | fast (ns/op) | generic (ns/op) | Speedup |
|---|---|---|---|
| String_string/fast | 6,963 | 99,040 | **14.2x** |
| String_int64/fast | 6,879 | 132,701 | **19.3x** |
| Int64_int64/fast | 6,453 | 77,688 | **12.0x** |
| Int64_float64/fast | 6,352 | 85,410 | **13.4x** |
| Float64_float64/fast | 6,296 | 87,477 | **13.9x** |
| Float64_int64/fast | 6,274 | 87,356 | **13.9x** |
| Bool_bool/fast | 22,794 | 21,230 | **~1x** (see note) |

**Bool note:** Bool dict has D=2 entries max. The fast path builds a dictMatch mask of size 2
and then scans N rows via BoolIdx — the overhead of EnsureDecoded and slice allocation
nearly cancels the benefit of skipping per-row BoolValue() calls. For Bool, the fast path
is a correctness extension (handles multiple bool values without O(N×nValues) blowup) rather
than a performance win. The ≥5× gate applies to numeric types; Bool is exempt.

Required performance gate: ≥5× speedup on each numeric type (String, Int64, Float64) vs generic baseline.
All numeric types exceed 12× — gate passed.

Run command:
```
go test -bench='BenchmarkStreamScanEqualAny_ByType' -benchmem -count=5 \
    ./internal/modules/executor/
```

Back-ref: `internal/modules/executor/scan_equal_any_bench_test.go:BenchmarkStreamScanEqualAny_ByType`

## BENCH-EX-13: BenchmarkStructuralParentResolve_AllocCount
*Added: 2026-04-17*

Measures per-call allocations in `resolveStructuralParentIndices` over a 100-trace ×
60-span workload (6000 spans total). Captures the string-alloc reduction from NOTE-079.

**Setup:**
- Build `map[[16]byte][]structuralSpanRec` with 100 traces, 60 spans each.
- Each trace has a linear parent-child chain: span[i].parentID = span[i-1].spanID.
- Clone is done with `b.StopTimer()` / `b.StartTimer()` so only `resolveStructuralParentIndices`
  allocations are counted. The function nils parentID in-place, requiring a fresh clone each iteration.

**Run command:**
```
go test -bench=BenchmarkStructuralParentResolve_AllocCount -benchmem -count=3 \
    ./internal/modules/executor/
```

**Baseline (pre-fix, map[string]int):** ~12100 allocs/op (~1 alloc per span insert
× 6000 spans + 100 map makes; lookup is optimized by the compiler but insert always allocates).

**Baseline (post-fix, map[[8]byte]int):** ~300 allocs/op (100 map makes + runtime overhead,
zero string allocs) — regression threshold is ≤ 400 allocs/op.

**Back-ref:** `internal/modules/executor/structural_bench_test.go:BenchmarkStructuralParentResolve_AllocCount` (BENCH-EX-13)

---

## BENCH-EX-14: BenchmarkStructuralTypedRowLookup
*Added: 2026-04-21*

Measures per-row allocation reduction from replacing `[]map[string]any` with a typed
`[]intrinsicRowFields` slice in the structural hot path (NOTE-081). Compares two
sub-benchmarks: `Typed` (lookupIntrinsicFieldsTyped) and `Map` (lookupIntrinsicFields)
over 100 refs pointing at block 0 of a 5000-span file.

**Setup (outside timed loop):**
- Write 5000 spans with `MaxBlockSpans=100` (≈50 blocks), 5 cycling service names, 3 span kinds.
- Open reader with in-memory provider.
- Build refs for 100 rows in block 0 with wantCols = {trace:id, span:id, span:parent_id}.
- Run with `b.ReportAllocs()`.

**Run command:**
```
go test -bench=BenchmarkStructuralTypedRowLookup -benchmem -count=5 ./internal/modules/executor/
```

**Baseline (post-typed-struct fix, NOTE-081):**
- Typed: ~302 allocs/op — regression threshold.
- Map: ~401 allocs/op (one map per row, before fix).
- Speedup: 1.6–2× faster ns/op on the Typed path vs Map.

Back-ref: `internal/modules/executor/intrinsic_bench_test.go:BenchmarkStructuralTypedRowLookup`

---

## BENCH-EX-15: BenchmarkCollect_IntrinsicMaterialization
*Added: 2026-04-21*

End-to-end benchmark guarding against the regression from PR #172 where intrinsic columns
were removed from block payloads. Three sub-benchmarks exercise the intrinsic pre-filter
and result materialization paths.

**Setup (outside timed loop):**
- Write 5000 spans with `MaxBlockSpans=100`, 5 cycling service names, 3 span kinds.
- Open reader with in-memory provider.
- Compile query per sub-benchmark (see below).
- Run with `b.ReportAllocs()`.

**Sub-benchmarks:**

| Sub-benchmark | Query | Matching spans |
|---|---|---|
| `PureIntrinsic` | `{ resource.service.name = "auth-service" }` | ~1000 |
| `MixedQuery` | `{ resource.service.name = "auth-service" && span:kind = server }` | ~333 |
| `ResultFields` | same as PureIntrinsic with AllColumns=true | ~1000 |

**Run command:**
```
go test -bench=BenchmarkCollect_IntrinsicMaterialization -benchmem -count=3 ./internal/modules/executor/
```

**What this measures:** With dual storage (block columns + intrinsic section), field access is O(1) per row. Without dual storage (PR #172 regression), each GetField triggered an O(N) scan through intrinsic column entries. The `ResultFields` sub-benchmark exercises the full materialization path with AllColumns=true.

Back-ref: `internal/modules/executor/intrinsic_bench_test.go:BenchmarkCollect_IntrinsicMaterialization`

---

## BENCH-EX-16: BenchmarkIntrinsicCountRateGroupBy_AllocCount (N=1 path)
*Added: 2026-04-22*

NOTE-082 annotation: BENCH-EX-10 now implicitly exercises the N=1 fast path
(`streamCountRateGroupByIDSingle`) because the query groups by a single dimension
(`resource.service.name`). This entry documents the expected N=1 path alloc baseline
explicitly.

**Expected:** ≤46 allocs/op — same as BENCH-EX-10 baseline. The N=1 fast path reduces map
key size (32→4 bytes) but does not change allocation count; throughput improvement is measured
by ns/op, not allocs/op.

**Run command:**
```
go test -bench='BenchmarkIntrinsicCountRateGroupBy_AllocCount' \
    -benchmem -count=5 ./internal/modules/executor/
```

**Spec:** NOTE-082 — N=1 group-by fast path.

Back-ref: `internal/modules/executor/intrinsic_group_id_bench_test.go:BenchmarkIntrinsicCountRateGroupBy_AllocCount`
