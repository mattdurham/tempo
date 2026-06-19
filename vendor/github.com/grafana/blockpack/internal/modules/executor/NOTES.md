# executor — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/executor` package.

---

## NOTE-352: exponent-keyed boundary indexing on the dense histogram paths

The dense direct histogram accumulators — `accumulateHistogramDirect` (N=1 no-predicate),
`accumulateHistogramDirectN0` (N=0 no-predicate), and `streamByRefSliceHistogram` (the
keymap fallback) — each built a `boundaryCache map[float64]int64` and a closure that, per
dict-entry / per-row, computed `intrinsicHistogramBoundary(v)` and hashed the resulting
float64 boundary through the map to assign its first-encounter index. A CPU profile of the
dense path (BenchmarkIntrinsicHistogramGroupBy_AllocCount) attributed ~50% of accumulate CPU
to `runtime.f64hash` + `mapaccess2` + `mapassign` inside this closure, plus another ~26% to
the same float-keyed map inside `countIntrinsicHistogramBoundaries` (called once per
accumulate to SIZE the flat grid).

NOTE-182 had already solved exactly this for the COMPACT path (`scanAggColHistogramCompact`)
with `boundaryIndexer`: every positive value maps to the boundary `2**(exp-1)` where `exp ==
frexpExpPos(scaledValue)`, so the binary exponent is a perfect dense integer key — a slice
indexed by `(exp - expBase)` replaces the float hash. This change extends that indexer to the
three dense paths that were still on the legacy map: each now constructs a `boundaryIndexer`
and passes `bi.index` as the `getBoundaryIdx` callback, and emits with `bi.boundaries`. The
indexer's first-encounter ordering and the `actualStride-1` discard cap are byte-identical to
the former closure (the N0 path passes `actualStride = histFlatStride+1` so the discard
sentinel `>= histFlatStride` still trips `scanHistogramN0`'s guard exactly as before).

`countIntrinsicHistogramBoundaries` is likewise rewritten to count distinct *exponents*
(dense `[]bool` indexed by `exp - expLo`, with a separate flag for the v<=0 boundary-0
bucket) rather than distinct boundary floats. Distinct boundaries are in 1:1 correspondence
with distinct exponents (boundary `= 2**(exp-1)`), so the returned count is identical to the
former map cardinality for every input — locked by
TestCountIntrinsicHistogramBoundaries_MatchesMapReference (vs a legacy map oracle over dict
int64/string, flat, delta-ascending, all-zero, and cap-exceeded columns) and the
boundary↔exponent bijection test. The DeltaUint64 branch no longer needs its
compare-against-prev shortcut — the `seenExp` dedup naturally skips the long runs of identical
exponents in a sorted column.

Result (BenchmarkIntrinsicHistogramGroupBy_AllocCount, 50000x): ns/op ~50633 → ~23404
(~-54%), allocs 39 → 35 (the two maps + the boundaries-slice churn removed). The profile's
`f64hash`/`mapassign`/`f64equal` nodes vanish; the boundary work is now `frexpExpPos` (~4.6%)
+ `boundaryIndexer.index` (~7%). The compact/parallel histogram paths were already on the
indexer (NOTE-182/210) and are unchanged.

---

## NOTE-350: histogram dense-series direct emit (skip the buckets map round-trip)

The intrinsic histogram path (M8: `histogram_over_time(duration) by (...)`) accumulated into a
flat `groupCountsFlat[gIdx*stride1 + bIdx*stride2 + timeIdx]` grid, then `streamByRefSliceHistogramFlatEmit`
serialized every non-zero CELL into a `map[string]*aggBucketState` keyed by the composite
`"timeIdx\x00gk\x00boundaryStr"` (one `strconv.AppendInt` + byte appends + map insert + a
per-cell `&aggBucketState{}` heap alloc — of which histogram only ever reads `.count`). The
consumer `traceHistogramSeries` then re-parsed every key (`IndexByte` + `ParseInt` +
`LastIndexByte`) to scatter the counts back into the SAME `(group, boundary, time)` dense grid.
A full serialize/deserialize round-trip plus O(non-zero-cells) heap allocs, purely to move
integer counts.

This is the direct analog of the count/rate `seriesSink` fast path (NOTE-247): on the intrinsic
path `buckets` is never shared across blocks (`executeTraceMetricsIntrinsic` accumulates per file
and consumes immediately), so each `(gIdx, bIdx)` slab of the flat grid IS one final series — its
labels are the group-by dims (`dict[gIdx]`, `"\x00"`-joined for N=1) plus `__bucket=boundaryStr`,
and `Values` is the `[base : base+stride2]` row. `intrinsicDirectSeriesSinks` hands the histogram
accumulation paths a `histSink *[]TraceTimeSeries`; `emitHistogramFlat` routes to
`streamByRefSliceHistogramFlatEmitDirect` (scatter straight into dense series) when it is non-nil,
falling back to the legacy `buckets` map emit otherwise. The result site sorts the sink with
`finalizeCountRateSeries` (matching `traceHistogramSeries`' final SortFunc), so the output series
are byte-identical to the `buckets → traceHistogramSeries` path (guarded by
`TestStreamByRefSliceHistogram_3D_DirectEmitEquivalence`).

**Scope gate:** the sink is only created for N=0 and N=1 group-by histograms, which all funnel
through `emitHistogramFlat`. The N>1 group-by paths (`streamHistogramGroupByID`/
`streamHistogramGroupBy`) write straight into the string-keyed `buckets` map and never reach the
flat emit, so `intrinsicDirectSeriesSinks` leaves `histSink` nil for them and they keep the
`traceHistogramSeries` consumer unchanged.

---

## NOTE-349: pool the per-block []intrinsicRowFields scatter scratch

`lookupIntrinsicFieldsTypedForBlock` (structural search path, e.g. `{...} >> {...}`),
`lookupIntrinsicFieldsTyped` (the ref-filter path in `filterRowSetByIntrinsicNodes`), and
`identityFieldsFromBlockColsTyped` (legacy-file branch) each allocated a fresh
`[]intrinsicRowFields` of `SpanCount` entries **once per block per query**. `intrinsicRowFields`
is ~120 bytes (three string headers + two byte arrays + scalars), so a block with thousands of
spans is a multi-hundred-KB short-lived allocation that the GC must scan (the strings are
pointers). The slice is fully consumed by the caller's immediately-following row loop and then
discarded — a textbook `sync.Pool` target.

`getIntrinsicRowFields(n)`/`putIntrinsicRowFields` (intrinsicrowfields.go) draw a backing array
from a `sync.Pool` (growing when `cap < n`) and return a length-`n` prefix. **Correctness
hinges on zeroing**: the typed scatter functions (NOTE-345) only write entries that have a
`RefIndexEntry`, so an unpopulated row keeps whatever was in the recycled backing array. Every
reader of an `intrinsicRowFields` first checks the `present` bitmap (e.g.
`if row.present&intrinsicPresentTraceID == 0 { continue }`), so the get path `clear()`s the
returned prefix — guaranteeing every recycled row starts at `present==0` with no stale
string/byte payload observable. All three producers now draw from the pool so callers
(`stream_structural.go` row loop, `filterRowSetByIntrinsicNodes` filter loop) release
unconditionally via `defer putIntrinsicRowFields(...)` after the loop fully consumes the slice.

Lifetime safety: the structural row loop copies the value-typed identity fields
(`[8]byte`/`[16]byte`) into `structuralSpanRec` and `computeNodeMatchForRow` reads `*row`
synchronously and returns a `uint8` — nothing retains a pointer into the slice past the loop,
and the defer fires after `releaseBlockColumnProvider`. Error paths release before returning nil.

## NOTE-348: share the POPCNT rank index between time-bucket scatter and group-by scan

The compact N=1 count/rate group-by path (`streamCountRateN1Compact`, the M4/M6 `rate() by (…)`
hot path) built the SAME `pkBitset` + POPCNT `rankPrefix` index TWICE from the identical `pkSet`:

  1. `fillPKSetAndTimeBuckets` → `scatterTimeBucketsByRank` built it to scatter each ref's time
     bucket into its rank slot, then freed both pooled slices.
  2. `streamCountRateN1CompactCore` → `scanGroupByColCompact` rebuilt the byte-identical index
     (NOTE-135/140) from the same `pkSet` to scan the group-by column by rank.

Both are pure functions of `(pkSet, maxPK)`. At `maxPK ≈ 16M` each build is two ~1 MB pool
allocations plus an `O(maxPK/64)` bitset clear and an `O(maxPK/64)` popcount-prefix pass — paid
twice per block on the dominant 24h `rate() by (resource.service.name)` query.

The fix factors the index into a `pkRankIndex{minPK, maxPK, bitset, rankPrefix}` value built once
by `buildPKRankIndex`. `fillPKSetAndTimeBuckets` now returns the index it built for the scatter;
`streamCountRateN1Compact` holds it (deferring `release()`) and threads it through
`streamCountRateN1CompactCore` into `scanGroupByColCompact`, which reuses it instead of
rebuilding. When the index is `nil` (the general-agg path, the predicate-filtered
`…FromRefs` paths, and the parallel-groupby test) `scanGroupByColCompact` builds a local index and
frees it — byte-identical to the pre-NOTE-348 behavior. The reused index is provably identical to
the rebuilt one because both derive from the same `pkSet`/`maxPK`; `minPK` is now computed inside
`buildPKRankIndex` (was a separate min-scan in the scan), so the bounds skip is unchanged.

Correctness: the rank a ref receives is its position in the bitset, order-independent (NOTE-223),
so reusing vs. rebuilding the index yields the same `dictIdxByPos`. Lifetime: the shared index
outlives both the scatter and the scan and is released exactly once by the owning caller.

---

## NOTE-249: early-break the count/rate entryGIdx build — first pass becomes O(dict entries)

`accumulateIntrinsicBucketsDirect` builds a per-dict-entry first pass that, for every
`groupByCol.DictEntries` entry, walks ALL of that entry's `BlockRefs` to (a) assign the entry a
group index (`entryGIdx[i]`) the first time it sees a ref ≤ `maxPK`, and (b) populate the dense
`dictByPK[pk]` packKey→group array. `dictByPK` is **only** allocated for the histogram/agg paths
(NOTE-091: count/rate uses `entryGIdx` exclusively and leaves `dictByPK == nil`).

For the count/rate path the loop therefore did redundant work: after the first ref ≤ `maxPK`
assigns `entryGIdx[i]`, every subsequent ref in the entry only re-tested `pk <= maxPK` and hit the
`if dictByPK != nil` guard — which is false — so it accumulated nothing. The downstream
`accumulateCountRateDirect` re-walks every entry's `BlockRefs` itself (its per-span bucket lookup
is keyed off `entryGIdx`, not `dictByPK`), so the first pass never needed to visit more than the
first matching ref per entry on the count/rate path. `break`ing as soon as the entry is assigned
(when `dictByPK == nil`) turns this first pass from O(total spans in the group-by column) into
O(num dict entries) for count/rate.

The biggest win is the no-predicate `{} | rate() by (...)` shape (M4), where every entry's refs
are all ≤ `maxPK` so the entry is assigned on its first ref — the inner loop collapses to one
iteration per entry instead of one per span. The histogram and agg paths (`dictByPK != nil`) must
still visit every ref to fill the dense `dictByPK`, so they do NOT break and scan in full —
behavior and output are byte-identical to before. No benchmark-specific constants; a general
algorithmic reduction. Validated by the existing N=1 count/rate equivalence tests
(`TestAccumulateCountRateDirect_*` / dispatch end-to-end) which still pass with `-race`. Back-ref:
`internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect`.

## NOTE-223: drop the radix sort in streamCountRateN1Compact — rank is order-independent
*Added: 2026-06-12*

**Decision:** Remove the `radixSortByPackKey` pass from `streamCountRateN1Compact` (the N=1
count/rate group-by path: M4 `{} | rate() by (...)`, M6, and the unfiltered N=1 case). The
function previously packed each in-range ref into a `pkOrder` uint64 (`packKey<<32 | relIdx`),
radix-sorted the whole N-element array by packKey, then walked the sorted array to populate
`sortedPKs[i]` and `timeBucketByPos[i]` (with `i == sorted position`). The sorted array was then
handed to `streamCountRateN1CompactCore` → `scanGroupByColCompact`.

**Rationale:** `scanGroupByColCompact` (and the histogram analog) never depend on the *order* of
their packKey input. They consume it as (a) a `minPK`/`maxPK` range, (b) a `pkBitset` membership
set, and (c) a POPCNT `rankPrefix` index. The position assigned to a ref is its **rank** in the
bitset — `rankPrefix[word] + popcount(bitset[word] & ((1<<bit)-1))` — which equals its sorted
position because in-range span packKeys are distinct (one entry per span). So the sort existed
purely to make `sorted-array-index == rank`; the rank can instead be computed directly from a
bitset built over the *unsorted* refs. The N-element radix pass (`radixSortByPackKey`, profiled at
~7% querier self-time on the rate-by-group path) is replaced by: one O(N) pass to fill the packKey
set + min/max, an O(maxPK/64) bitset+rankPrefix build, and one O(N) scatter that writes each ref's
time bucket to its rank slot. `radixSortByPackKey` remains in use by the general-agg N=1 path
(`streamAggN1Compact`), the histogram compact path, and the merge-join reorder, so it is not dead.

**Generalization required for correctness:** `scanGroupByColCompact` (and only it; the histogram
path keeps its sorted callers) previously read `sortedPKs[0]`/`sortedPKs[len-1]` for `minPK`/`maxPK`.
It now derives min/max with an O(N) scan so it is correct for an *unsorted* input. This is a strict
generalization — for the merge-join callers that still pass a sorted slice, a scan yields the same
min/max, so their behavior is byte-identical. The scatter in `streamCountRateN1Compact` writes every
`timeBucketByPos` slot exactly once (rank is a bijection onto `[0, n)` over distinct packKeys),
matching the prior sorted walk's full coverage; out-of-window rows leave the pooled `int32` slot at
the cleared 0 sentinel, identical to before.

**Queries affected:** M4/M6 and any unfiltered N=1 `rate()`/`count_over_time` by a single group.
Back-refs: `streamCountRateN1Compact`, `scanGroupByColCompact` in
`internal/modules/executor/metrics_trace_intrinsic.go`.

---

## NOTE-225: drop the radix sort in streamHistogramN1Compact — extend NOTE-223/224 to histograms
*Added: 2026-06-12*

**Decision:** Apply the NOTE-223/224 sort-elimination to `streamHistogramN1Compact`, the
compact-memory N=1 group-by path for `histogram_over_time(...) by (single group)` queries (the M8
benchmark). It previously packed each in-range ref into a `pkOrder` uint64 (~57 MB at n≈7.2 M),
`radixSortByPackKey`'d it, then walked the sorted array to fill `sortedPKs[i]`/`timeBucketByPos[i]`
(`i == sorted position`), before handing `sortedPKs` to `scanGroupByColCompact` and
`scanAggColHistogramCompact`.

**Rationale:** identical to NOTE-223/224. Both downstream scans consume the packKeys only as a
`minPK`/`maxPK` range, a `pkBitset` membership set, and a POPCNT `rankPrefix` index; the position
they assign a ref (`histRefPassPos`) is its **rank** in the bitset, which equals its sorted
position because in-range span packKeys are distinct. The sort existed purely so
`sorted-array-index == rank`. Replaced with the same one-pass set+maxPK fill, bitset+rankPrefix
build, and rank-scatter of `timeBucketByPos` used by `streamCountRateN1Compact` — factored into the
new `scatterTimeBucketsByRank` helper shared by `fillPKSetAndTimeBuckets` (column,lo,hi form) and
the new `fillPKSetAndTimeBucketsFromRefs` (the histogram path's separate inRangeRefs/inRangeVals
slices). `radixSortByPackKey` is still used by the merge-join reorder, so it stays live.

**Generalization required for correctness:** `scanAggColHistogramCompact` previously read
`sortedPKs[0]`/`sortedPKs[len-1]` for `minPK`/`maxPK`. It now derives them with an O(N) scan, so it
is correct for an *unsorted* input. Strict generalization: the predicate-filtered caller
`streamHistogramN1CompactFromRefs` still passes a sorted slice (from `mergeJoinFilteredRefsWithVals`),
and a scan over a sorted slice yields the same min/max, so its behavior is byte-identical. The
rank-scatter writes every `timeBucketByPos` slot exactly once (rank is a bijection onto `[0, n)` over
distinct packKeys); out-of-window rows leave the pooled `int32` slot at the cleared 0 sentinel, and
the downstream absent-row pass and emit are unchanged.

**Queries affected:** N=1 histogram group-by queries
(`{...} | histogram_over_time(duration) by (single group)`, e.g. M8), wherever
`accumulateIntrinsicBuckets` takes the compact histogram path on large files.
Back-refs: `streamHistogramN1Compact`, `scanAggColHistogramCompact`, `scatterTimeBucketsByRank`,
`fillPKSetAndTimeBucketsFromRefs` in `internal/modules/executor/metrics_trace_intrinsic.go`.

---

## NOTE-224: drop the radix sort in streamAggN1Compact — extend NOTE-223 to general aggregates
*Added: 2026-06-12*

**Decision:** Apply the NOTE-223 sort-elimination to `streamAggN1Compact`, the compact-memory N=1
group-by path for general aggregates (min/max/sum/avg/etc., the non-count/rate, non-histogram
case). It previously packed each in-range ref into a `pkOrder` uint64, `radixSortByPackKey`'d it,
then walked the sorted array to fill `sortedPKs[i]`/`timeBucketByPos[i]` (`i == sorted position`),
before handing `sortedPKs` to `scanGroupByColCompact` and `scanAggColCompact`.

**Rationale:** identical to NOTE-223. Both downstream scans consume the packKeys only as a
`minPK`/`maxPK` range, a `pkBitset` membership set, and a POPCNT `rankPrefix` index; the position
they assign a ref is its **rank** in the bitset, which equals its sorted position because in-range
span packKeys are distinct. The sort existed purely so `sorted-array-index == rank`. Replaced with
the same one-pass set+min/max fill, bitset+rankPrefix build, and rank-scatter of `timeBucketByPos`
used by `streamCountRateN1Compact`. `radixSortByPackKey` is still used by the histogram compact
path and the merge-join reorder, so it stays live.

**Generalization required for correctness:** `scanAggColCompact` (the only function NOTE-223 did
not touch — NOTE-141's out-of-scope item) previously read `sortedPKs[0]`/`sortedPKs[len-1]` for
`minPK`/`maxPK`. It now derives them with an O(N) scan, so it is correct for an *unsorted* input.
Strict generalization: the predicate-filtered caller `streamAggN1CompactFromRefs` still passes a
sorted slice, and a scan over a sorted slice yields the same min/max, so its behavior is
byte-identical. The scatter in `streamAggN1Compact` writes every `timeBucketByPos` slot exactly
once (rank is a bijection onto `[0, n)` over distinct packKeys); out-of-window rows leave the
pooled `int32` slot at the cleared 0 sentinel, identical to before.

**Queries affected:** N=1 general-aggregate `... by (single group)` queries (min/max/sum/avg over a
field, e.g. `{} | max(duration) by (...)`), wherever `accumulateIntrinsicBucketsDirect` falls back
to the compact path on large files (`maxPK > maxDirectArrayEntries`).
Back-refs: `streamAggN1Compact`, `scanAggColCompact` in
`internal/modules/executor/metrics_trace_intrinsic.go`.

---

## NOTE-210: prewarmSortedAscending — data-free histogram boundary pre-warm for value-sorted columns
*Added: 2026-06-11*

**Decision:** Replace the full O(numRows) serial pre-warm walk (`prewarmBoundaries`) with a
data-free O(numBoundaries) range enumeration (`boundaryIndexer.prewarmSortedAscending`) for
DeltaUint64 (value-sorted-ascending) columns on the parallel histogram path
(`scanAggColHistogramCompact`). The parallel scan needs a serial pre-warm so that `bi.lookup`
(the race-free worker read path) finds every reachable boundary's exponent slot already assigned
and so `bi.boundaries` is in ascending first-encounter order (NOTE-143/182). That pre-warm
previously re-ran `histRefPassPos` for every row — duplicating the per-row pk-range/bitset/time
filter the parallel workers then repeat — purely to discover which boundaries appear.

**Rationale:** A 2026-06-11 querier CPU profile (gcx, process_cpu, 30m) attributed ~7.5% of
querier CPU to the histogram scan, of which `prewarmBoundaries` was 1.74% self-time plus the
duplicated `histRefPassPos` cost. DeltaUint64 `Uint64Values` are sorted ascending and
`intrinsicHistogramBoundary` is monotone non-decreasing, so the set of boundaries the scan can
reach is exactly the boundaries spanned by `[minPositive, max]` — enumerable by stepping the
binary exponent with no row scan and no per-row filter. The smallest positive value is found by
`sort.Search` (O(log n)) over the sorted slice.

**Correctness:** `bi.lookup` returns the discard sentinel for any exponent slot left unassigned,
so the pre-warm MUST record a SUPERSET of the boundaries the scan reaches — guaranteed by
bounding the enumeration with the actual `[minPositive, max]` value range (not an arbitrary low).
Pre-recording a few interior boundaries no passing row reaches only adds zero-count
`groupCountsFlat` cells, which `streamByRefSliceHistogramFlatEmit` skips, so emitted series are
byte-identical. The enumeration iterates the exponent directly (`math.Frexp` of the scaled
endpoints, then `math.Ldexp(1, exp-1)` per exponent) rather than doubling a float, so the slot
assigned matches `index`/`lookup`'s exponent decode with no float round-trip drift; the
`b*1e9` round-trip for span:duration recovers the exact exponent real ns values decode to
(verified 0 mismatches across the full ns range). `record()`'s `maxStride-1` discard cap is
honored identically. `TestHistParallelIndexedEquivalence_DeltaUint64` (unfiltered + filtered ×
span:duration + a plain numeric field) asserts the indexed fast path is emit-identical to the
serial scan via a boundary-VALUE-keyed canonical projection (dense indices may differ; emitted
cells must not). `go test -race ./executor` green, `make precommit` fully green.

**Queries affected:** `histogram_over_time` over a value-sorted intrinsic column (e.g. M8 and
duration histograms) where the parallel path fires (numItems ≥ histParallelMinItems, NumCPU > 1).

---

## NOTE-182: boundaryIndexer — exponent-indexed boundary lookup, no per-row float64 hashing
*Added: 2026-06-11*

**Decision:** Replace the per-row `getBoundaryIdx` closure — a `map[float64]int64` keyed
by the histogram boundary value — used by every histogram aggregation scan
(`scanAggColHistogramCompact`/`Shard`) with a `boundaryIndexer` that keys on the boundary's
*binary exponent* via a dense `[]int64` slice instead of hashing the float. The boundary is
always either `0` (for `v <= 0`) or an exact power of two `2**(exp-1)` where `exp` comes from
the single `math.Frexp(v)` already implied by NOTE-181's `pow2Floor`. The exponent is therefore
a perfect dense integer key: `bi.index(v)` decodes the exponent and does one bounds-checked
array read/write instead of a float64 hash + map probe. A read-only `bi.lookup(v)` variant
serves the parallel workers after a serial pre-warm (NOTE-143) populates the dense table, so
the workers no longer rebuild or probe the legacy `frozen map[float64]int64` either.

**Rationale:** A 2026-06-11 querier CPU profile (gcx, process_cpu, 30m) attributed ~14% of
total querier CPU to the histogram aggregation scan: `scanAggColHistogramShard` 5.63%,
`buildFrozenBoundaryIdx` 3.78%, `histRefPassPos` 2.29%, `scanAggColHistogramCompact` 1.16%.
The dominant remaining per-row cost inside `scanAggColHistogramShard` (after NOTE-181 removed
the transcendentals) was the float64 map lookup performed for *every passing row* — float
hashing plus a probe. Replacing it with an exponent-indexed array read removes the hash and
the map entirely from the hot loop, a general algorithmic improvement on the boundary domain
(powers of two), not a workload-specific shortcut.

**Correctness:** `bi.index` mirrors the former closure exactly — the original value is tested
`<= 0` FIRST (negatives and zero map to the boundary-0 cell regardless of field, since
`intrinsicHistogramBoundary` returns 0 before its dead `math.Abs`), span:duration values are
then scaled by `1e9`, the `actualStride-1` overflow/discard sentinel and the first-encounter
append order of `boundaries[]` (load-bearing for the emit step, NOTE-143) are preserved
byte-for-byte. `bi.lookup` never mutates indexer state, so it is race-free across the parallel
workers; the serial pre-warm visits exactly the rows the workers reach, so every read hits an
assigned exponent slot. The dense table spans the full float64 exponent range
`[-1075, 1025)`, so any finite value (incl. denormals, ±Inf→exp 0, NaN→exp 0) indexes in
bounds. Verified bit-exact against a verbatim copy of the pre-NOTE-182 closure
(`TestBoundaryIndexerParity`) over thousands of values per (field, stride): zeros, negatives,
exact powers of two, the full ns-duration range, denormals, and repeats — for both `index` and
`lookup`, including matching `boundaries[]` value and order. `go test -race ./executor` green.

**Queries affected:** All `histogram_over_time` queries routed through the intrinsic compact
histogram scan (M8 and group-by histograms).

---

## NOTE-181: pow2Floor — branch-free power-of-2 floor for histogram boundaries
*Added: 2026-06-10*

**Decision:** Replace the `math.Pow(2, math.Floor(math.Log2(x)))` expression — used at
both histogram-boundary call sites, `intrinsicHistogramBoundary` (intrinsic fast path,
~25 reachable sites) and `traceHistogramBucket` (legacy block-scan path) — with a shared
`pow2Floor(x)` helper that computes the same value branch-free and without any
transcendental call. `pow2Floor` uses `math.Frexp` to decompose `x = frac * 2**exp` with
`frac ∈ [0.5, 1)` and returns `math.Ldexp(1, exp-1)`. Because `log2(frac) ∈ [-1, 0)`,
`floor(log2(x)) == exp-1` exactly — including the exact-power-of-2 case, where `frac == 0.5`
and `exp` is one larger so `exp-1` still yields the correct exponent.

**Rationale:** A 2026-06-10 querier CPU profile (gcx, 30m window) attributed the
transcendental calls reached only through these two helpers to ~10% of total querier CPU:
`math.pow` 5.99% + `math.archLog` 4.15% + `math.log2` 1.29%, against the ~1.6% that
`math.frexp` (0.89%) + `math.ldexp` (0.70%) reintroduce — a net ~8.9pp reduction on the
CONFIRMED-CPU-BOUND queriers. The boundary is computed once per distinct dictionary value
in the memoized pre-scans (`buildFrozenBoundaryIdx`, `countIntrinsicHistogramBoundaries`)
but per-row in the legacy block-scan path, so the absolute call volume is large on every
`histogram_over_time` query (M8 and group-by histograms).

This supersedes the reverted NOTE-161 (same identity, same helper intent); that change was
reverted only because it was unmeasurable under a then-overloaded cluster, not because it
was wrong. Re-landed against a quieter cluster.

**Correctness:** Verified bit-exact to the old `math.Pow(2, math.Floor(math.Log2(v)))` form
over 20M values: exact powers of 2 (and ±1 ULP around them), the full ns-duration `/1e9`
range, uniformly random magnitudes across 20 decades, and denormals — 0 mismatches. The
`v <= 0` and `vSec <= 0` guards in the two callers are unchanged, so `pow2Floor` is only
ever invoked with strictly-positive finite inputs.

**Queries affected:** All `histogram_over_time` queries — intrinsic fast path (M8, group-by
histograms) and the legacy block-scan path.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:pow2Floor`,
`internal/modules/executor/metrics_trace.go:traceHistogramBucket`

---

## 1. Responsibility Boundary

_Added: 2026-02-10_

The executor owns span-level evaluation. Block selection (which blocks to read) is
delegated to `queryplanner`; raw I/O (coalescing, wire parsing) is delegated to
`blockio/reader`. The executor only decides which spans within a selected block match the
query.

---

## 2. Collect Takes \*reader.Reader, Not BlockIndexer

_Added: 2026-02-10 (updated 2026-03-11: renamed Execute → Collect per NOTE-035)_

**Decision:** `Collect` takes `*reader.Reader` directly rather than `queryplanner.BlockIndexer`.

**Rationale:** After block selection, the executor needs `ParseBlockFromBytes` and
`BlockMeta` — methods that are on `*reader.Reader` but not part of `BlockIndexer`.
Extending `BlockIndexer` with parsing methods would conflate two distinct concerns:
index lookup (what the planner needs) and block decoding (what the executor needs).
Using `*reader.Reader` directly is explicit and avoids unnecessary interface widening.

`*reader.Reader` still satisfies `BlockIndexer` via Go structural typing, so the planner
works without a type assertion.

---

## 3. ColumnDataProvider for Modules Block

_Added: 2026-02-10_

**Decision:** The executor implements `vm.ColumnDataProvider` directly against
`*reader.Block` (modules format) in `column_provider.go`, rather than converting to the
old `blockpackio.Block` type via the adapter in `internal/modules/blockio/adapter.go`.

**Rationale:** The adapter conversion copies every column dictionary, which allocates
significantly more memory than necessary. The modules `Block` type exposes its columns
directly via `GetColumn`, making a direct implementation straightforward and allocation-free.

---

## 4. RowSet Is Sorted by Construction

_Added: 2026-02-10_

**Decision:** `rowSet` maintains rows in ascending order. Stream scans iterate rows
0..n-1 sequentially, so `Add` is called in ascending order. `ToSlice` returns the
backing slice directly without re-sorting.

**Rationale:** Sorted row indices are required for:

- `Contains` (binary search, O(log n))
- `Union/Intersect/Complement` (merge-style algorithms, O(n+m))
- Deterministic output ordering

---

## 5. SpanMatch Contains Block and Row Indices

_Added: 2026-02-10_

**Decision:** `SpanMatch` includes `BlockIdx` and `RowIdx` in addition to `TraceID` and
`SpanID`.

**Rationale:** The caller (e.g. a Tempo query handler) may want to fetch additional column
values for matched spans without re-scanning the block. Having `BlockIdx` and `RowIdx`
allows targeted column access via `ParseBlockFromBytes` + `GetColumn(name).StringValue(rowIdx)`.

---

## 6. Predicate Extraction and Dedicated Index Pruning

_Added: 2026-02-25_

**Decision:** The executor calls `BuildPredicates(r, program)` (in `predicates.go`),
which extracts column names and encoded values from `program.Predicates` and builds
`[]queryplanner.Predicate` for bloom-filter and range-index block pruning.

**Design (superseded 2026-03-06 — see NOTE-030):**
The original design used `HasOROperations` flag and flat maps (`DedicatedColumns`,
`DedicatedRanges`, `DedicatedColumnsRegex`, `UnscopedColumnNames`) to determine
per-column predicates. The current design uses a `RangeNode` tree that is recursively
translated to `queryplanner.Predicate` composites, enabling per-child OR pruning
rather than per-query. See NOTE-030 for the complete redesign rationale.

This gives two-stage block-level pruning. See queryplanner/NOTES.md §8 for
range-pruning design.

**History:** An earlier version of this note documented a period when `predicates.go` was
removed and the executor passed `nil` predicates to `planner.Plan`. That design was
superseded and predicate extraction was reinstated with the full two-stage pruning
described above.

---

## 7. Integration Coverage via blockio/executor_test.go

_Added: 2026-02-25_

**As of the modules executor migration:** the primary integration tests for this
executor live in `internal/modules/blockio/executor_test.go` (EX-01 through EX-07),
not only in `internal/modules/executor/executor_test.go`.

`internal/modules/blockio/executor_test.go` exercises the full round-trip:
writer (`modules_blockio.NewWriterWithConfig`) → reader
(`modules_reader.NewReaderFromProvider`) → this executor (`executor.Collect`).

**Coverage provided by the blockio integration tests:**

- `bloomPredicates` with real data (AND and OR query paths, EX-01, EX-03, EX-04, EX-06)
- `Collect` empty-file short-circuit (EX-05)
- Multi-block scanning with `BlocksScanned >= 2` assertion (EX-04)
- Zero-match result (EX-02)

`internal/modules/executor/executor_test.go` additionally covers:

- `Options.Limit` early-exit (EX-08)
- `SpanMatch.TraceID` / `SpanMatch.SpanID` field population (EX-09)
- `statsOut.TotalBlocks` populated via CollectStats (EX-10)

---

## 8. Signal-Aware Column Selection in spanMatchFromBlock

_Added: 2026-03-02_

**Decision:** `spanMatchFromBlock` receives the file's `SignalType` and uses different
column names depending on whether the file holds trace or log data.

**Trace files (`SignalTypeTrace`):** `trace:id` and `span:id`.
**Log files (`SignalTypeLog`):** `log:trace_id` and `log:span_id`.

**Rationale:** Log files store trace and span correlation IDs under `log:trace_id` and
`log:span_id` (the OpenTelemetry log record fields), not under the `trace:id` / `span:id`
columns used by trace files. Without signal-aware lookup, `TraceID` and `SpanID` would
always be zero/nil for log queries. The signal type is read once via `r.SignalType()` and
passed to `spanMatchFromBlock` to avoid repeated calls.

---

## 9. log. Column Scope in Unscoped Attribute Expansion

_Added: 2026-03-02_

**Decision:** Unscoped attribute expansion (e.g. `.level`) now expands to three column
names: `resource.{attr}`, `span.{attr}`, and `log.{attr}`.

**Rationale:** Log files store user attributes under the `log.{key}` prefix. Without the
`log.` expansion, unscoped queries like `{ .level = "error" }` would never match log records
even when the attribute is present. The bloom-OR predicate across all three column names is
safe: a block is only pruned when none of the three columns is present.

---

## NOTE-010: BuildPredicates Consumes DedicatedRanges; encodeValue Handles Plain Column Types

_Added: 2026-03-02 — superseded by NOTE-030 (2026-03-06)_

**Superseded:** The `DedicatedRanges` map no longer exists. Range predicates are now
represented as `RangeNode{Min, Max}` in the `Nodes` tree and translated by `translateNode`.
The `encodeValue` function and its handling of plain vs `Range*` column types is unchanged.

**Original decision (historical):** `BuildPredicates` included a loop over
`program.Predicates.DedicatedRanges` that encoded range bounds as point queries for
range-index pruning. `encodeValue` accepted both the `Range*` column type variants and
their non-range counterparts (`ColumnTypeUint64`, `ColumnTypeInt64`, `ColumnTypeFloat64`).

**Rationale:** Log intrinsics like `log:timestamp` are stored with `ColumnTypeUint64` in the
range index (not `ColumnTypeRangeUint64`). The writer's `updateLogMinMax` uses
`ColumnTypeUint64` because the raw column type is non-range. Without handling plain column
types, `encodeValue` would return `("", false)` for `log:timestamp` range predicates,
leaving the `Values` slice empty and silently skipping range-index pruning.

**Encoding rules (unchanged):** 8-byte little-endian for numeric types; raw string for
string types. The `Range*` and plain variants produce identical wire encoding.

---

## NOTE-011: Regex Prefix Optimization for Range-Index Pruning

_Added: 2026-03-03 — updated 2026-03-06 (NOTE-030)_

**Decision:** `translateRegexNode` (formerly a loop over `DedicatedColumnsRegex` in
`BuildPredicates`) analyzes regex patterns using `vm.AnalyzeRegex` and produces
range-index predicates when the pattern has an extractable literal prefix.

**Design:**

1. `vm.AnalyzeRegex(pattern)` parses the regex syntax tree (via `regexp/syntax`) and
   extracts literal prefixes from optimizable patterns: `foo.*`, `^error`, `error|warn`.
2. For optimizable patterns, `BuildPredicates` encodes each prefix as a `RangeString`
   value in `Predicate.Values`. The planner's `pruneByIndex` uses `BlocksForRange` to
   eliminate blocks whose string range buckets don't overlap the prefix.
3. The column type defaults to `ColumnTypeRangeString` when no range index exists (the
   planner's `pruneByIndex` gracefully skips when `RangeColumnType` returns false).
4. The full regex is still evaluated per-span by the VM — range-index pruning only
   eliminates blocks, never individual spans.

**Case-sensitive single-prefix patterns use interval matching for range-index pruning.**
Go's regex parser factors out common prefixes from alternations at parse time (before
`Simplify()`). For example, `cluster-0|cluster-1` becomes
`Concat(Literal("cluster-"), CharClass([01]))` with the common prefix `"cluster-"`.
A point lookup via `BlocksForRange("col", "cluster-")` returns nil because `'-'` (0x2D)
is less than `'0'` (0x30) — `"cluster-"` falls below all actual bucket lower boundaries
(which start at values like `"cluster-0"`).

Fix: for any case-sensitive regex with a **single extracted prefix**, `BuildPredicates`
now uses interval matching `[prefix, prefix+"\xff"]` instead of a point lookup.
`BlocksForRangeInterval("col", "cluster-", "cluster-\xff")` correctly finds all buckets
with lower boundaries in that range (e.g., `"cluster-0"` through `"cluster-4"`).

The `\xff` suffix also ensures blocks in buckets where the lower boundary extends
beyond the bare prefix (e.g., `"foo-service"` for prefix `"foo"`) are included.

**Multi-prefix patterns** (e.g., `error|warn|info`, which parses as
`Alternate([OpLiteral("error"), OpLiteral("warn"), OpLiteral("info")])` without
common-prefix factoring) continue to use union point lookups per prefix, which
correctly handle full-string values.

**Case-insensitive patterns (`(?i)...`) use interval matching for range-index pruning.**
The range index stores original-case values and uses lexicographic comparison.
`"DEBUG"` and `"debug"` occupy different bucket positions, so a single point lookup
would miss blocks containing other case variants (false negatives).

Instead, `buildCaseInsensitiveRegexPredicate` generates an interval query:

- Min key: `strings.ToUpper(prefix)` (e.g., `"DEBUG"`)
- Max key: `strings.ToLower(prefix) + "\xff"` (e.g., `"debug\xff"`)

The `\xff` suffix ensures the interval captures buckets whose lower boundary extends
beyond the exact prefix (e.g., a bucket with lower `"debug-service"` is > `"debug"` but
< `"debug\xff"`). Without the suffix, such buckets would be pruned even though the regex
`(?i)debug.*` matches values in those buckets.

The predicate has `IntervalMatch: true`. The planner calls `BlocksForRangeInterval`
which finds all buckets whose lower boundary falls within [min, max]. This works
because uppercase ASCII/UTF-8 bytes sort before lowercase — the interval covers
every case variant (e.g., `"Debug"`, `"dEBUG"`, `"DeBuG"` all fall within
`["DEBUG", "debug\xff"]`).

**Case-insensitive alternations** (`(?i)(error|warn)`) fall back to bloom-only predicates.
Each prefix needs a separate interval, and different prefixes may span non-overlapping
lexicographic ranges — a single interval cannot safely cover all branches.

**Rationale:** Log workloads heavily use regex patterns like `(?i)debug`, `error.*`,
`^GET /api`. Block pruning is the primary performance lever in this system (I/O ops
reduction). Converting prefix patterns to range-index lookups enables block pruning
without any writer-side changes.

Back-ref: `internal/modules/executor/predicates.go:BuildPredicates`,
`internal/vm/regex_optimize.go:AnalyzeRegex`

---

## NOTE-012: Stream vs Execute — Lazy Callback vs Eager Batch

_Added: 2026-03-03_ | _Historical — both `Stream` and `Execute` have been removed; see addendum below._

**Decision:** `Stream` was a separate method from `Execute` rather than a flag on `Options`.

**Rationale:**

- `Execute` returns `*Result` (batch `[]SpanMatch`). Callers like `tempoapi` expect a
  fully-materialized slice — adding callback plumbing to `Execute` would complicate its
  contract without benefit for those callers.
- `Stream` uses lazy `CoalescedGroups`/`ReadGroup` I/O instead of eager `FetchBlocks`.
  This is the correct strategy for streaming with a Limit: I/O is proportional to results
  returned, not to total selected blocks.
- Separating the two methods keeps each one simple and makes their different I/O strategies
  explicit at the call site.

**Signal unification:** `Stream` accepts `StreamOptions.TimestampColumn` to distinguish
trace mode (no per-row filter) from log mode (`"log:timestamp"` filter). The executor no
longer needs two divergent code paths — the difference is parameterized rather than
duplicated.

**Callers after this task:** `api.go:streamFilterProgram` calls `executor.Collect`;
`api.go:streamLogProgram` calls `executor.CollectLogs`. The `Stream` method has been
removed — see NOTE-035. No inline block-scan loops remain in api.go.

**Addendum (2026-03-03):** The "out of scope" logql/engine.go:StreamLogs work was completed
as part of the logql→executor unification. StreamLogs and ExecuteLogMetrics now live in
executor/stream_log.go and executor/metrics_log.go, both using the same lazy coalesced-group
I/O pattern. The internal/modules/logql package has been deleted. See NOTE-013 for the
new executor→logqlparser import edge.

---

## Migration: Content migrated from internal/modules/logql/NOTES.md

_Migrated: 2026-03-03 — package internal/modules/logql deleted; execution logic moved to executor_

## NOTE-001: Why a New Module Instead of Extending logqlparser

_Added: 2026-03-03_

**Decision:** The LogQL execution engine lived in `internal/modules/logql/`, not in
`internal/logqlparser/`. It has now been merged into executor.

**Rationale:** `internal/logqlparser/` is a parser — it converts LogQL strings into AST
nodes and compiles them to `vm.Program` values. Adding execution logic (pipeline stage
evaluation, metric aggregation, block scanning) would conflate two distinct concerns and
break the separation that exists between `internal/traceqlparser/` (parse only) and
`internal/modules/executor/` (execute only).

The architecture now mirrors the established TraceQL architecture:

- TraceQL: `traceqlparser` (parse + compile) + `executor` (execute)
- LogQL: `logqlparser` (parse + compile + Pipeline types) + `executor` (execute)

---

## NOTE-002: Per-Row Pipeline Execution, Not Column-Level

_Added: 2026-03-03_

**Decision:** Pipeline stages operate per matched row with signature
`func(ts, line, labels) (line, labels, bool)`, not over entire columns.

**Rationale:** LogQL pipeline stages are inherently row-oriented: `| json` parses each
log body independently; `| label_filter` tests per-row label values; `| line_format`
templates use per-row label state. There is no vectorized equivalent that would safely
apply to blockpack's columnar layout.

Column-level operations (e.g., selecting which column to read for `log:body`) remain in
the block scan path. The per-row pipeline runs only after the block scan returns matching
row indices — keeping the hot path in the columnar scan and the post-filter in the pipeline.

---

## NOTE-003: Reuse vm.QuerySpec for Metrics, Not a New Bucketing System

_Added: 2026-03-03_

**Decision:** Metric aggregation in `metrics_log.go` reuses `vm.QuerySpec` for time
bucketing. It does NOT call `executor.ExecuteMetrics`.

**Rationale:** `vm.QuerySpec` (time bucketing: step, start, end) is generic — it was
designed for TraceQL metrics but is not TraceQL-specific. Reusing it avoids creating a
parallel bucketing system.

The engine does NOT delegate to `ExecuteMetrics` because that function is tightly coupled
to TraceQL span match fields and does not provide a hook for per-row pipeline execution.
LogQL metrics needs to interpose the pipeline between block scan and bucket accumulation.

---

## NOTE-004: Label Map Mutability and Caller Responsibility

_Added: 2026-03-03_

**Decision:** `Pipeline.Process` may mutate the `labels` map in place. The caller is
responsible for copying the map if the original state is needed after the call.

**Rationale:** Allocating a new map per row for every pipeline call would generate
significant GC pressure at high log volumes. Mutation in place is safe because:

1. Pipeline stages chain sequentially (no concurrent access to the same map).
2. The engine constructs a fresh labels map per row from block columns.
3. Callers that need the pre-pipeline state (unusual) can copy before calling Process.

---

## NOTE-005: Template Pre-Compilation for line_format

_Added: 2026-03-03_

**Decision:** `text/template` parsing for `| line_format "tmpl"` happens once at query
compile time (`logqlparser/compile.go`), not per-row.

**Rationale:** `text/template.Parse` is expensive (it involves regex compilation and
AST building). Parsing the same template string for every log row is unnecessary and
would dominate pipeline overhead. The compiled `*template.Template` is embedded in the
`PipelineStageFunc` closure and reused for every row.

---

## NOTE-006: UnwrapValueKey as Special Label

_Added: 2026-03-03_

**Decision:** `UnwrapStage` stores the extracted numeric value in the labels map under
the key `"__unwrap_value__"` (constant `logqlparser.UnwrapValueKey`).

**Rationale:** Passing a separate numeric value alongside the labels map would require
changing the `PipelineStageFunc` signature, breaking all existing stage implementations.
Using a reserved label key is a pragmatic convention that preserves the uniform stage
interface.

---

## NOTE-007: Silent Failure Policy for Parse Errors in Stages

_Added: 2026-03-03_

**Decision:** `JSONStage` and `LogfmtStage` do NOT drop rows when the log body is not
valid JSON/logfmt. They keep the row with unmodified labels.

**Rationale:** This matches Loki's behavior. In a mixed-format log stream, some records
may not conform to the expected format. Silently keeping them allows downstream label
filters to handle the case explicitly.

---

## NOTE-008: Engine Does Not Modify the Reader

_Added: 2026-03-03_

**Decision:** `StreamLogs` and `ExecuteLogMetrics` take `*reader.Reader` as a parameter
but never call writer-side methods. They are pure read paths.

---

## NOTE-009: logqlparser Produces Everything Needed to Execute

_Added: 2026-03-03_

**Decision:** `logqlparser.CompilePipeline` produces `*logqlparser.Pipeline` alongside
`*vm.Program`. The executor receives both and does not re-parse the query.

---

## NOTE-064: Free Functions Instead of Engine Struct

_Added: 2026-03-03_

**Decision:** `StreamLogs` and `ExecuteLogMetrics` are package-level free functions, not
methods on an `Engine` struct.

**Rationale:** The execution logic carries no state. Free functions are simpler to call,
test, and reason about.

---

## NOTE-065: Label Column Scoping — resource.\* and log.\* Prefixes Stripped

_Added: 2026-03-03_

**Decision:** `logReadLabels` strips the `resource.` and `log.` column name prefixes when
building the per-row labels map. The label `resource.service.name` becomes `service.name`.

**Rationale:** In LogQL, stream labels do NOT carry the `resource.` prefix. This matches
Loki semantics where resource-level attributes are exposed as first-class labels.

---

## NOTE-014: Reuse NewColumnProvider, Not a Reimplemented Interface

_Added: 2026-03-03 (migrated from internal/modules/logql/NOTES.md NOTE-012)_

**Decision:** `ExecuteLogMetrics` and `StreamLogs` call `NewColumnProvider(block)` (now
in the same package) to satisfy `vm.ColumnDataProvider`, rather than implementing the
interface again.

---

## NOTE-013: executor now imports logqlparser — new dependency edge

_Added: 2026-03-03_

**Decision:** `stream_log.go` and `metrics_log.go` import `logqlparser` for the
`*logqlparser.Pipeline` type.

**Rationale:** Pipeline types were moved from `internal/modules/logql` into
`internal/logqlparser` as part of unifying the logql package (all execution moves to
executor, all parsing/compilation stays in logqlparser). executor → logqlparser introduces
no import cycle: logqlparser imports only vm, standard library packages, and
github.com/go-logfmt/logfmt — none of which import executor.

This mirrors the established TraceQL architecture: traceqlparser (parse + compile) +
executor (execute). For LogQL: logqlparser (parse + compile + Pipeline types) + executor
(execute).

---

## NOTE-015: blockHasBodyParsed — parser-skip optimization for log queries

_Added: 2026-03-04_

**Decision:** `blockHasBodyParsed(block)` scans `block.Columns()` looking for any
`log.*` key with `Type == ColumnTypeRangeString`. When the block has body-auto-parsed
columns, `StreamLogs` / `StreamLogsTopK` acquire a `blockLabelSet` for the block and, in
the per-row scan loop, invoke `pipeline.ProcessSkipParsers` instead of `pipeline.Process`,
skipping the logfmt/JSON parse stage.

**Rationale:** The blockpack writer stores body-auto-parsed fields as `log.{key}`
`ColumnTypeRangeString` sparse columns (see logqlparser NOTE-9). `blockLabelSet` exposes
these fields lazily via column reads before the pipeline runs. Running `| logfmt` or
`| json` after the label set already contains those fields is redundant — these stages
call `logfmt.NewDecoder(bytes.NewBufferString(line))` per row, allocating ~15 objects
per row. On T7-T9 queries (env=prod + component filter), this produced ~18M allocs
for a 100 MB dataset.

**Column type discriminator:** Body-auto-parsed columns use `ColumnTypeRangeString`;
explicit OTLP LogRecord attributes (e.g. `log.level`, `log.detected_level`) use
`ColumnTypeString`. The discriminator is the column type byte stored in the block
metadata. `blockHasBodyParsed` uses this to determine which code path to take.

**Bug fixed:** `stringColumnBuilder.colType()` previously hardcoded `ColumnTypeString`
regardless of the type passed to `newColumnBuilder`. This caused body-parsed columns to
be written with type=0 (ColumnTypeString) instead of type=11 (ColumnTypeRangeString),
making `blockHasBodyParsed` always return false. Fixed by storing `typ` in the struct.
See blockio writer SPECS §8 for the column type encoding.

Back-ref: `internal/modules/executor/stream_log.go:blockHasBodyParsed`
Back-ref: `internal/modules/blockio/writer/column_types.go:stringColumnBuilder`

---

## NOTE-016: Three-Layer Regex Scan Optimization

_Added: 2026-03-04_

**Decision:** `ScanRegexFast` / `ScanRegexNotMatchFast` implement three compounding
optimizations over the original `StreamScanRegex` path:

1. **Pre-compiled regex** — the `*regexp.Regexp` is compiled once at query-compile time
   (in `logqlparser.Compile` or `traceql_compiler.compileColumnPredicateComparison`) and
   passed directly to the scan. `regexp.Compile` is never called inside the hot per-block
   scan loop.

2. **Literal prefix pre-filter** — `vm.RegexPrefixes(pattern)` extracts literal substrings
   that _must_ appear in any matching string. `containsAnySubstring` checks these with
   `strings.Contains` before invoking the DFA/NFA engine. For patterns like `"error|warn"`,
   this eliminates the regex call entirely for the majority of rows that match neither
   prefix. Case-insensitive patterns (`(?i)`) are excluded from the pre-filter because
   `strings.Contains` is case-sensitive and would produce false negatives.

3. **Flat batch string extraction** — `col.StringValues()` builds a `[]string` of length
   `SpanCount` in a single pass over `StringIdx/StringDict`. The scan loop then indexes
   into this flat slice, eliminating the two-level dictionary dereference
   (`StringIdx[i] → StringDict[di]`) on every iteration.

4. **CI literal bypass (fold-contains)** — pure case-insensitive literal patterns like
   `(?i)error`, `(?i)error|warn` are detected at query-compile time via
   `vm.AnalyzeRegex(...).IsLiteralContains && .CaseInsensitive`. When true, the caller
   passes `nil` for `re` and pre-lowercased prefixes. The scan path then uses
   `strings.ToLower(v)` + `containsAnySubstring` instead of the regex engine entirely.
   This eliminates Go's NFA backtracking path (`regexp.(*Regexp).tryBacktrack`), which
   dominates CPU for `(?i)` patterns and accounted for ~60% of CPU in T3/Q20 profiling.
   Anchored patterns (`^`, `$`) are excluded — `IsLiteralContains` is false when any anchor
   is present, ensuring correctness.

**Correctness:** The prefix pre-filter is safe — it is a superset filter. Any row skipped
by `containsAnySubstring` cannot be matched by the regex (a regex can only match a string
that contains its required literal substrings). No false negatives are possible.

**CI bypass correctness:** `re == nil` is used only when `IsLiteralContains = true`, meaning
`strings.Contains(v, prefix)` is exactly equivalent to `re.MatchString(v)`. The anchored
pattern exclusion (`hasAnchor = false` required) ensures no false positives.

**Not-match path:** For the regex path, `ScanRegexNotMatchFast` uses optimizations 1 and 3
only. For the CI bypass path, it uses optimization 4 (fold-contains negated).

Back-ref: `internal/modules/executor/column_provider.go:ScanRegexFast`,
`internal/modules/executor/column_provider.go:ScanRegexNotMatchFast`,
`internal/modules/executor/column_provider.go:streamScanRegexFast`,
`internal/modules/blockio/reader/block.go:StringValues`,
`internal/vm/regex_optimize.go:RegexAnalysis.IsLiteralContains`

---

## NOTE-SL-017: blockLabelSet and sync.Pool for Zero-Alloc Hot Path

_Added: 2026-03-04_

**Decision:** `StreamLogs`, `StreamLogsTopK`, and `ExecuteLogMetrics` use a `sync.Pool`
of `*blockLabelSet` instead of building a `map[string]string` per row.

**Rationale:** On the pre-parsed block path (where `blockHasBodyParsed` returns true),
label values already live in block column dictionaries. `blockLabelSet.Get(key)` calls
`col.StringValue(rowIdx)` directly — one array index lookup, zero allocations. The overlay
map is allocated only when `Set` or `Delete` is called. For rows dropped by label
filters before any mutation, the per-row allocation cost is exactly zero (one pool
Get/Put, which is amortised across goroutines via the sync.Pool shard mechanism).

`buildBlockColMapsWithLogCache` is called once per block (not per row) to build the label-name →
original-column-name index. The `colNames` slice and `colMap` map are shared across all
rows in the block. The `*blockLabelSet` itself is reset per-row via `resetForRow`, which
clears the overlay and deleted maps in-place to reuse their backing arrays.

For `StreamLogsTopK`, entries stored in the heap call `Materialize()` immediately after
pipeline processing and before pool release. This ensures the heap holds stable
`map[string]string`-backed `LabelSet` values regardless of pool reuse.

**colName conflict resolution:** `buildBlockColMapsWithLogCache` uses a two-pass approach: `resource.*`
columns are registered first (explicit OTLP attributes take priority), then `log.*`
columns fill in gaps. Within each scope, `ColumnTypeString` wins over `ColumnTypeRangeString`
for the same stripped label name (explicit attribute beats auto-parsed body field).

Back-ref: `internal/modules/executor/block_label_set.go:blockLabelSet`,
`internal/modules/executor/block_label_set.go:buildBlockColMapsWithLogCache`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-018: Two-Pass Column Decode for Non-Matching Block Elimination

_Added: 2026-03-04_

**Decision:** All executor code paths use a two-pass block parse strategy:

1. **First pass** — `ParseBlockFromBytes` with `ProgramWantColumns(program)`, which decodes only the columns referenced by the query predicate (leaf columns from `Predicates.Nodes` tree, plus `Predicates.Columns` for negations/pushdown/log:body).
2. **Predicate evaluation** — `program.ColumnPredicate` runs against the minimal block.
3. **Early exit** — if `rowSet.Size() == 0`, skip this block entirely. No second parse.
4. **Second pass** (only for blocks with matches) — re-parse `bwb.RawBytes` with `nil` wantColumns to decode all columns for output delivery.

**Rationale:** Object-storage blocks commonly have 50–200 columns. A selective query
(e.g. `{span.http.method = "GET"}`) touches 1–3 columns for the predicate but would
previously decompress all columns in every candidate block. For queries where most
candidate blocks have no matches (the common case), this eliminates the bulk of
decompression work. For blocks with matches, the cost is a second parse, but those
are a small fraction of candidate blocks in typical trace/log queries.

**Exception — `ExecuteStructural`:** This path includes identity columns
(`trace:id`, `span:id`, etc.) in `ProgramWantColumns(program, ...)` so output can be
built from the first-pass block directly. No second parse is ever issued.

**`ProgramWantColumns` must include ALL predicate column sources (updated 2026-03-06):**

- Leaf `Column` values from the `Nodes` RangeNode tree (via `collectNodeColumns`)
- `Columns []string` — negations, `log:body`, pushdown label-filter columns, and any
  column that needs row-level decode but produces no pruning node
  Omitting any source causes `ParseBlockFromBytes` to skip the column, resulting in
  false-empty predicate results (regression). See NOTE-030.

**When `wantColumns` is nil** (no predicates / match-all program), the first pass
decodes all columns and no second pass is issued.

Back-ref: `internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`,
`internal/modules/executor/stream_topk.go:topKScanBlocks`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`

---

## NOTE-019: GetColumn O(1) via colCols Slice in blockLabelSet

_Added: 2026-03-05_

**Decision:** `buildBlockColMapsWithLogCache` resolves `*Column` pointers once per block into a
parallel `colCols []*Column` slice alongside `colNames`. All four `block.GetColumn(colNames[idx])`
calls in `blockLabelSet.Get`, `Has`, `Keys`, and `Materialize` are replaced with direct
`colCols[idx]` slice indexing. The `log:timestamp` and `log:body` column pointers are lifted
out of the per-row loop in `logTopKScan`, `logCollectAll`, and `StreamLogs`.

**Rationale:** `Block.GetColumn(name)` iterates the full `map[ColumnKey]*Column` to find a
column by name (O(M) where M = total columns in block, typically 50-200). For a block with
100 columns and 1000 rows passing the pipeline, each label access costs up to 100 map
iterations. The column set is fixed for a block's lifetime; resolving `*Column` pointers
once per block converts all per-row accesses to O(1) slice indexing.

**Invariant:** `colCols[i]` and `colNames[i]` always refer to the same column. Both slices
are built in lockstep in `buildBlockColMapsWithLogCache` — every append to `colNames` is immediately
followed by an append to `colCols`. `colCols` is nil'd in `releaseBlockLabelSet` before
pool return to prevent stale pointer retention.

**NOTE-038:** `buildBlockColMaps` was removed (2026-03-16) — it was a strict subset of
`buildBlockColMapsWithLogCache`. Non-log callers now call `buildBlockColMapsWithLogCache`
and discard the `logStrNames`/`logStrCols` returns with `_, _`. This eliminates ~55 lines
of duplicated column-selection logic.

Back-ref: `internal/modules/executor/block_label_set.go:buildBlockColMapsWithLogCache`,
`internal/modules/executor/block_label_set.go:blockLabelSet`

---

## NOTE-020: internStrings Reset Per Block to Bound Map Size

_Added: 2026-03-05_

**Decision:** `Reader.ResetInternStrings()` is called before the first-pass
`ParseBlockFromBytes` in each log scan block loop (`logTopKScan`, `logCollectAll`,
`StreamLogs`). It is NOT called before the second-pass parse.

**Rationale:** `r.internStrings` was originally scoped to the `*Reader` lifetime, meaning
it accumulated every unique string from every block across the scan. At 1GB scale this
grows to millions of entries, causing slow map lookups due to long probe chains. String
interning is only beneficial within a single block parse (deduplication of dictionary
entries); cross-block deduplication provides no benefit for streaming log queries.

Resetting before the first pass (via the for-range delete idiom, which preserves the
backing hash table) bounds the map to the unique strings in one block (hundreds, not
millions). Not resetting before the second pass is intentional: the second pass parses the
same raw bytes (same dictionary), so all strings from the first pass are already interned;
second-pass intern lookups are near-free cache hits.

**Thread safety:** `r.internStrings` is single-goroutine use only (documented invariant
on the field). The reset is safe because reset and parse are sequential on the same goroutine.

Back-ref: `internal/modules/blockio/reader/reader.go:ResetInternStrings`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-021: Time Pre-Filter Before Second-Pass Decode

_Added: 2026-03-05_

**Decision:** `logTopKScan` and `logCollectAll` now apply the `opts.TimeRange` filter
using the first-pass block BEFORE triggering the second-pass full decode.
`"log:timestamp"` is explicitly injected into the `wantColumns` copy at the start of
`StreamLogsTopK` to guarantee the timestamp column is always present in the first-pass block.

**Rationale:** The second-pass decode (NOTE-018) was firing for every block with any
`ColumnPredicate` match, even when every row was subsequently dropped by the time range
filter. For topK and time-range queries, many candidate blocks have no rows within the
requested time window. Before this fix, a full 50-200 column decode fired for each such
block (measured at 1.72s of 3.64s total for T7/Q55 with a 1GB dataset).

Pre-filtering with a cheap uint64 comparison (`ts < MinNano || ts > MaxNano`) on the
already-decoded timestamp column eliminates the full second-pass decode for blocks where
all predicate-matching rows fall outside the time window.

**wantColumns injection:** `"log:timestamp"` is added to a copy of `wantColumns` once per
`StreamLogsTopK` call (not per block). When `wantColumns` is nil (match-all program), no
copy is needed — the first-pass block already decodes all columns including the timestamp.

**Row loop simplification:** The per-row time filter that previously ran inside the main
row loop (after the second pass) is replaced by the pre-filter. The main row loop iterates
`keptByTime []int` (time-passing row indices) instead of `rowSet.ToSlice()`.

**`StreamLogs` scope:** Fix 3 does not apply to `StreamLogs` because `StreamLogs` has no
`opts.TimeRange` parameter; the pre-filter concept requires an external time range to
screen against.

Back-ref: `internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-022: AddColumnsToBlock Replaces Second-Pass ParseBlockFromBytes

_Added: 2026-03-05_

**Decision:** The second-pass `r.ParseBlockFromBytes(bwb.RawBytes, nil, meta)` in
`logTopKScan`, `logCollectAll`, and `StreamLogs` has been replaced with
`r.AddColumnsToBlock(bwb, nil)`.

**Rationale:** `parseBlockColumnsReuse` (the core of `ParseBlockFromBytes`) iterates
raw bytes from scratch on every call. When `wantColumns = nil` (second pass), it
decompresses ALL columns — including those already decoded in the first pass. This
was re-doing decompression work for predicate columns that were already present in
`bwb.Block.columns`, accounting for ~1.1-1.2s out of ~1.6-2.1s total for
`logTopKScan` in T7/Q55 and T12/Q86 profiling.

`AddColumnsToBlock(bwb, nil)` re-reads the raw block bytes to parse column metadata,
then for each column checks whether it already exists in `bwb.Block.columns`. Columns
present from the first pass are skipped (zero decompression cost). Only newly-encountered
columns are decompressed and added. The result is a fully-populated block with zero
redundant decompression work.

**Nil guard in AddColumnsToBlock:** A nil guard was added to the `addColumns` parameter
check. Previously, calling `AddColumnsToBlock(bwb, nil)` was a no-op (nil map read in
Go returns false, so the name filter always fired and no columns were added). The nil
guard changes semantics: `addColumns == nil` now means "add all missing columns".
This is the correct semantics for the second-pass use case.

**In-place mutation:** `AddColumnsToBlock` mutates `bwb.Block.columns` directly.
The `bwb` pointer is not reassigned at the call site. All code below the call that
accesses `bwb.Block` sees the fully-populated block.

**internStrings interaction (NOTE-020):** NOTE-020 states we do not reset
`r.internStrings` before the second pass because the second pass re-parses the same
dictionary (all first-pass strings are already interned, giving near-free cache hits).
With `AddColumnsToBlock`, only new columns are decoded. Their dictionary strings may
not be interned yet, but are looked up in the same bounded intern map (reset before the
first pass). New strings are added on miss. The intern map remains bounded to one
block's unique strings. NOTE-020's invariant is preserved.

Back-ref: `internal/modules/blockio/reader/reader.go:AddColumnsToBlock`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-023: opts.TimeRange Forwarded to planner.Plan in StreamLogsTopK

_Added: 2026-03-05_

**Decision:** `StreamLogsTopK` previously passed `queryplanner.TimeRange{}` to
`planner.Plan`, disabling block-level time pruning entirely. It now passes
`queryplanner.TimeRange{MinNano: opts.TimeRange.MinNano, MaxNano: opts.TimeRange.MaxNano}`.

**Rationale:** For queries with an explicit time window, the planner can eliminate
blocks whose `[MinStart, MaxStart]` range lies entirely outside `[MinNano, MaxNano]`
before any I/O is issued. When `opts.TimeRange` is zero, `queryplanner.TimeRange{}`
is passed as before — no behavioral change.

**Relationship to NOTE-018:** NOTE-018 documented that `ObjectCache` is not at the
executor layer; it does not address time pruning. The original comment at the call
site referred to a concern about combining block-level time pruning with pipeline
row filtering causing false negatives. That concern is addressed by the two-level
filtering design: block pruning eliminates whole blocks only when they are entirely
outside the window (the planner's standard interval overlap test). Per-row time
filtering (NOTE-021) then handles sub-block granularity. These two levels are
complementary and do not cause false negatives.

**When opts.TimeRange is zero:** Zero is the identity value for `queryplanner.TimeRange`;
the planner treats zero MinNano/MaxNano as "no time constraint" and skips time pruning.
This ensures a zero `opts.TimeRange` produces identical behavior to the previous
hardcoded `queryplanner.TimeRange{}`.

Back-ref: `internal/modules/executor/stream_log_topk.go:StreamLogsTopK`

---

## NOTE-024: Literal Alternation Detection Fixes Overly Wide Regex Interval

_Added: 2026-03-05_

**Problem:** Go's `regexp/syntax` parser factors common prefixes from alternations before
`AnalyzeRegex` receives the parsed tree. For `"cluster-0|cluster-1"`, the parser produces
`Concat(Literal("cluster-"), CharClass([01]))`, so `AnalyzeRegex` returns a single prefix
`"cluster-"`. `BuildPredicates` emits interval `["cluster-", "cluster-\xff"]`, matching
ALL cluster-X blocks — zero pruning for non-matching clusters like cluster-2, cluster-3.

**Fix:** `extractLiteralAlternatives(pattern string) []string` checks whether the raw
pattern string is a pure OR of complete literal strings (splitting on `|`, verifying each
part has no regex metacharacters: `.*+?[]{}()^$\`). Called in the `len(analysis.Prefixes) == 1`
branch before emitting the interval. When it returns 2+ literals, those literals are used
as point lookups (same as the existing multi-prefix path for patterns like `"error|warn|info"`
which parse without common-prefix factoring). When it returns nil or a single literal, the
existing interval path is used unchanged.

**Why `len(lits) > 1`:** A single-literal pattern like `"debug"` splits to `["debug"]`.
Keeping it on the interval path is correct: a point lookup for `"debug"` would miss blocks
in range-index buckets whose lower boundary extends beyond the bare prefix (e.g.,
`"debug-service"` for prefix `"debug"`). The interval `["debug", "debug\xff"]` covers all such buckets.

**Correctness:** Point lookups are correct for full-string literals because the range index
stores the actual string value. A lookup for `"cluster-0"` finds all blocks containing at
least one span with that exact attribute value.

**Relationship to existing paths:**

- `"prod|staging"` (no common prefix) → already uses the multi-prefix path
  (`len(analysis.Prefixes) == 2`) — unaffected by this change.
- `"cluster-0|cluster-1"` (common prefix) → previously: wrong interval; now: point lookups.
- `"debug.*"` (single prefix with trailing wildcard) → still uses interval — unchanged.
- `"(?i)cluster-0|cluster-1"` → case-insensitive branch fires first — unchanged.

Back-ref: `internal/modules/executor/predicates.go:BuildPredicates`,
`internal/modules/executor/predicates.go:extractLiteralAlternatives`

---

## NOTE-025: AddColumnsToBlock Removed — Lazy Column Decode Handles All Columns

_Added: 2026-03-05_

**Decision:** Removed `r.AddColumnsToBlock(bwb, nil)` calls from `StreamLogs`,
`StreamLogsTopK`, `logCollectAll`, and the second `ParseBlockFromBytes(raw, nil, meta)` call
from `MetricsLogsQuery`. These were the "second pass" that decoded all remaining columns
after the predicate filter.

**Rationale:** `ParseBlockFromBytes` now performs lazy registration for all columns not in
`wantColumns`. Columns not needed for predicate evaluation are registered with presence-only
decode (no zstd decompression). Their full decode is deferred to the first value access
(`StringValue`, `Uint64Value`, etc.), which calls `Column.decodeNow()` internally.
This eliminates the 90+ eager zstd decompressions per block for non-predicate columns,
reducing per-block decode cost from O(all columns) to O(predicate columns + accessed columns).

**Estimated savings:** For T9/Q66 (1997 blocks, ~90 non-predicate columns per block):

- Old: 1997 × 90 × zstd_decompress ≈ 0.87s
- New: 1997 × 90 × presence_only + 1997 × ~15 × zstd_decompress ≈ 0.04s + 0.15s = 0.19s

**Safety:** The `rawEncoding` slice points into `bwb.RawBytes`, which is valid for the
lifetime of `bwb`. All lazy decodes complete within the block's row loop iteration,
before `bwb` goes out of scope. Single-goroutine scan path — no locking needed.

Back-ref: `internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/metrics_log.go:MetricsLogsQuery`,
`internal/modules/blockio/reader/block.go:Column.decodeNow`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## NOTE-026: Dictionary-Level Float Parse for Numeric String Column Scans

_Added: 2026-03-05_

**Decision:** `scanStringDictFloat` pre-parses the string dictionary of a `ColumnTypeRangeString`
or `ColumnTypeString` column once per block, then uses boolean array lookups per row for numeric
threshold comparisons. It is invoked by `StreamScanGreaterThan`, `StreamScanGreaterThanOrEqual`,
`StreamScanLessThan`, and `StreamScanLessThanOrEqual` when the column is string-typed and the
comparison value is `float64`.

**Context:** T12 benchmark queries (`| latency_ms > 4500`) use numeric comparisons against
string-typed log attribute columns (e.g. `log.latency_ms` stored as `ColumnTypeRangeString` with
values "0"–"4999" as string OTLP attributes, not numeric). The previous hot path in `rowCompare`
called `strconv.ParseFloat(col.StringValue(rowIdx), 64)` for every row — approximately
`blockSpanCount × blocksScanned` ParseFloat calls per query (e.g. ~2M calls for a 1002-block scan
with 2000 rows/block).

**Optimization:** String columns use dictionary encoding (`StringDict []string` + `StringIdx []uint32`).
The dictionary typically has O(hundreds) unique values even when `SpanCount` is in the thousands.
`scanStringDictFloat`:

1. Iterates `col.StringDict` once (O(dictSize), typically ~700 entries).
2. Calls `strconv.ParseFloat` per dictionary entry — only those that survive are marked `matches[i] = true`.
3. Scans rows using `col.StringIdx[i]` → `matches[di]` — O(1) bool lookup per row, zero ParseFloat calls.

**Measured impact** (8-iteration benchmark, 1002-block scan):

- Q87 (`env+gt4000`, 507 blocks): −22% CPU (308→240 cpuMs)
- Q88 (`cluster+gt4000`, 482 blocks): −29% CPU (323→228 cpuMs)
- Q86 (`env+gt4500`, 1002 blocks): −12% CPU (486→428 cpuMs)

**Range pruning note:** For uniformly distributed string-numeric values (e.g. random 0–4999 across all
blocks), the range index assigns every block to every bucket (every block's min≈0, max≈4999). No range
index pruning is possible for this workload. The dictionary-level float parse is the primary lever.

Back-ref: `internal/modules/executor/column_provider.go:scanStringDictFloat`,
`internal/modules/executor/column_provider.go:StreamScanGreaterThan`,
`internal/modules/executor/column_provider.go:StreamScanGreaterThanOrEqual`,
`internal/modules/executor/column_provider.go:StreamScanLessThan`,
`internal/modules/executor/column_provider.go:StreamScanLessThanOrEqual`

---

## NOTE-027: encodeValue TypeString Cross-Encoding for Numeric Range Index

_Added: 2026-03-05_

**Decision:** `encodeValue` now handles `vm.TypeString` for `ColumnTypeRangeInt64` and
`ColumnTypeRangeFloat64`. When the string parses as the appropriate numeric type, the
function encodes it as 8-byte LE and returns true. Otherwise it returns false (falls back
to bloom-only pruning).

**Rationale:** When the writer promotes a string column to a numeric range index
(NOTE-040 in blockio NOTES.md), `r.RangeColumnType("log.latency_ms")` returns
`ColumnTypeRangeInt64`. A query `| latency_ms > 4500` where the VM produces
`{TypeInt, 4500}` already works via the existing TypeInt path. However, a query
`| latency_ms > "4500"` (string comparison) produces `{TypeString, "4500"}`. Without
this change, `encodeValue` returned `("", false)` → bloom-only pruning. With this
change, the string "4500" is parsed as int64 and encoded correctly.

**Safety:** Returns false (no pruning) when ParseInt/ParseFloat fails. No false negatives.
Only improves pruning; never removes correct blocks.

**Back-ref:** `internal/modules/executor/predicates.go:encodeValue`

---

## NOTE-029: Multi-Prefix Regex Path Must Use Full Literals, Not Go-Factored Partial Prefixes

_Added: 2026-03-05_

**Problem (T6/Q45 and T6/Q50 false-negative pruning):**

In `BuildPredicates`, when `len(analysis.Prefixes) > 1`, the code previously encoded
the Go-extracted prefixes as point lookups. This is safe for patterns like `"prod|staging|dev"`
where Go does NOT factor a common prefix (3 independent literal branches). But for patterns
like `"us-east-1|us-west-2|eu-west-1"`, Go's regex parser factors the common prefix:

```
us-east-1|us-west-2|eu-west-1
→ Alternate(Concat(Literal("us-"), Alternate("east-1","west-2")), Literal("eu-west-1"))
→ analysis.Prefixes = ["us-", "eu-west-1"]
```

The partial prefix `"us-"` is NOT a valid range-index key — the index stores full values like
`"us-east-1"` and `"us-west-2"`. Using `"us-"` as a point lookup returns 0 blocks →
all blocks with `region=us-east-1` or `region=us-west-2` get pruned → **false negatives / data loss**.

**Fix:** In the multi-prefix branch, call `extractLiteralAlternatives(pattern)` on the raw
pattern string first. If all alternatives are pure literals (no metacharacters), use those
full literals as point lookups. If not (or if the call returns nil), fall back to bloom-only
to prevent false negatives.

**Why this is safe:** `extractLiteralAlternatives` checks the original pattern string before
Go's regex parser can factor it, so it recovers the original full literals regardless of
what Go's internal representation looks like.

**Scope of impact:** This bug affected any LogQL stream selector OR TraceQL predicate using
a regex alternation pattern with 2+ distinct prefix groups — typically patterns like
`region=~"us-east-1|us-west-2|eu-west-1"` where some alternatives share a prefix.
Patterns with no common prefix factoring (`"prod|staging|dev"`) were unaffected.

**Relationship to NOTE-024:** NOTE-024 fixed the single-prefix case (Go factors ALL alternatives
into a common prefix — enters the `len == 1` branch). NOTE-029 fixes the multi-prefix case
(some alternatives share a prefix, others don't — enters the `len > 1` branch).

**Back-ref:** `internal/modules/executor/predicates.go:BuildPredicates` (multi-prefix branch)

---

## NOTE-028: SearchMetaColumns — Scoped Second-Pass Decode for Trace Search

_Added: 2026-03-05_

**Decision:** `StreamOptions.AllColumns` controls the second-pass decode in `Stream()`.
By default (`false`), the second pass decodes only `searchMetaColumns() ∪ wantColumns`
(predicate columns) instead of all columns. Callers that require full column access set
`AllColumns = true`. `streamFilterProgram` in `api.go` leaves `AllColumns` at its default.

**Rationale:** Mirrors Tempo's two-pass design (pkg/traceql/engine.go `ExecuteSearch`):
first pass fetches only filter predicate columns; second pass fetches a fixed set of
search-meta intrinsics plus any predicate columns already in scope. Tempo's parquet format
stores `RootSpanName`, `RootServiceName`, `TraceDuration`, and `TraceStartTime` as
pre-computed trace-level columns. Blockpack stores everything per-span, so the equivalent
set is 8 columns: `trace:id`, `span:id`, `span:start`, `span:end`, `span:duration`,
`span:name`, `span:parent_id` (root detection), `resource.service.name`.

`FindTraceByID` (`GetTraceByID` in api.go) uses `GetBlockWithBytes` directly and always
reads all columns — unaffected by this change. Only `QueryTraceQL` filter queries benefit.

**Back-ref:** `internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/predicates.go:searchMetaColumns`

**Addendum (2026-03-25):** `searchMetaCols` was shrunk as part of the intrinsic-section
migration (NOTE-050). Trace-signal identity columns (trace:id, span:id, span:start, span:end,
span:duration, span:name, span:parent_id, resource.service.name, span:status,
span:status_message, span:kind) were removed from `searchMetaCols` because they are now
stored exclusively in the intrinsic TOC section and injected directly into `secondPassCols`
via the `traceIntrinsicColumns` loop (all 11 columns). These columns are absent from block
payloads; identity values are fetched via `lookupIntrinsicFields`.

---

## NOTE-030: RangeNode Tree Replaces Flat QueryPredicates Maps

_Added: 2026-03-06_

**Decision:** `vm.QueryPredicates` was redesigned from a collection of flat maps
(`DedicatedColumns`, `DedicatedRanges`, `UnscopedColumnNames`, `DedicatedColumnsRegex`,
`AttributesAccessed`, `HasOROperations`) to a unified tree:

```go
type RangeNode struct {
    Min, Max *Value   // range interval bounds
    Column   string   // leaf: fully-scoped column name
    Pattern  string   // leaf: regex pattern
    Values   []Value  // leaf: equality values (OR'd)
    Children []RangeNode
    IsOR     bool     // composite: OR vs AND semantics
}

type QueryPredicates struct {
    Nodes   []RangeNode // AND-combined top-level pruning tree
    Columns []string    // columns for row-level decode only (negations, log:body)
}
```

**Why the flat maps were removed:**

1. `HasOROperations` was a coarse flag that disabled range-index pruning for the entire
   query when any OR was present — even when the OR was between scoped columns that could
   each be individually indexed. The tree allows OR composites to be built per-attribute
   rather than per-query.

2. `UnscopedColumnNames` was a separate bloom-only list for unscoped attributes. This
   prevented range-index pruning for unscoped equality predicates (`.service.name = "auth"`).
   With the tree, unscoped attributes expand to `{IsOR:true, Children:[resource.X, span.X, log.X]}`
   at compile time, enabling per-scope range-index lookup.

3. `DedicatedColumns` / `DedicatedRanges` / `DedicatedColumnsRegex` were separate maps
   that `BuildPredicates` iterated independently. The tree unifies all predicate types under
   a single recursive `translateNode` function, eliminating the impedance mismatch between
   compile-time structure and runtime translation.

4. `AttributesAccessed` was used by `ProgramWantColumns` to include columns in the
   first-pass decode. `Columns []string` serves the same purpose more directly: only columns
   that need decode-but-not-pruning (negations, `log:body`, pushdown label filters) appear
   in `Columns`; columns that drive pruning nodes are collected from `Nodes` via
   `collectNodeColumns`.

**Backward-incompatible changes:** all callers of `BuildPredicates`, `ProgramWantColumns`,
and the logql/traceql compilers were updated in the same commit. No old field names remain
in production code.

**Negation invariant:** `!=` and `!~` predicates produce no `Nodes`. A bloom filter
says "column possibly present" — removing a block because `service.name != "prod"` would
risk removing blocks where some spans DO satisfy the predicate. Negations only go to
`Columns` for the row-level decode.

**Back-ref:** `internal/modules/executor/predicates.go:BuildPredicates`,
`internal/modules/executor/predicates.go:translateNode`,
`internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/vm/bytecode.go:RangeNode`,
`internal/vm/traceql_compiler.go:extractTraceQLPredicates`

---

## NOTE-031: Early-Skip Materialization in logTopKScan When Heap Is Full

_Added: 2026-03-06_

**Problem:** In `logTopKScan`, for every row that passes the pipeline filter, we previously
called `labels.Materialize()` (creates `map[string]string`), `collectLogStringAttrs()`
(creates another `map[string]string`), and `&LogEntry{}` (heap alloc) before trying to insert
into the top-K heap. For `limit=1000` queries with 200K+ matching rows, ~99.5% of these
allocations were immediately wasted — the row was inserted but immediately evicted, or never
even challenged the heap root.

**Fix:** After pipeline processing (which must still run for keep/drop decisions), check
whether the heap is already full and whether this row's timestamp can possibly displace the
current worst entry:

```go
if buf.Len() >= opts.Limit {
    worst := buf.entries[0].ts
    if (backward && ts <= worst) || (!backward && ts >= worst) {
        releaseBlockLabelSet(bls)
        continue
    }
}
```

For backward queries (newest-first), `ts <= worst` means this row is older than the oldest
entry currently in the heap, so it cannot improve the result. For forward queries (oldest-first),
`ts >= worst` means this row is newer than the newest entry in the heap. In either case, skip
materialization.

**Correctness:** The pipeline runs before the check, so any `| keep` / `| drop` / `| label_format`
mutations have already been applied to `line` and `labels`. The check only gates map allocation,
not pipeline evaluation. The heap invariant is maintained: we only skip rows that genuinely
cannot enter the top-K.

**Impact:** Eliminates ~99% of `Materialize()` and `collectLogStringAttrs()` calls in the
top-K path for queries where matching rows >> limit (e.g., T12/Q86 with 200K+ matching rows
and limit=1000). Expected ~30-40% CPU reduction for logfmt/numeric queries on large datasets.

**Back-ref:** `internal/modules/executor/stream_log_topk.go:logTopKScan`

---

## NOTE-032: aggBucketState Shared Between Log and Trace Metrics

_Added: 2026-03-08_

**Decision:** `aggBucketState` (defined in `metrics_log.go`) is reused by
`metrics_trace.go` rather than duplicating the struct.

**Rationale:** Both log and trace metric execution accumulate the same per-bucket state:
`sum`, `count`, `min`, `max`, and a `values []float64` slice for quantile. Keeping a
single struct definition in `metrics_log.go` avoids drift between the two
implementations. Any change to `aggBucketState` affects both callers, which is
intentional — they share the same aggregation semantics.

**Consequence:** `metrics_trace.go` imports nothing extra; it simply references
`aggBucketState` from the same package. The struct is unexported, which is correct —
it is an internal accumulator, not part of the public API.

**Back-ref:** `internal/modules/executor/metrics_log.go:aggBucketState`,
`internal/modules/executor/metrics_trace.go:traceUpdateBucket`

---

## NOTE-033: HISTOGRAM, QUANTILE, and STDDEV in ExecuteTraceMetrics

_Added: 2026-03-08_

**Decision:** Three previously unimplemented aggregate functions now work in
`ExecuteTraceMetrics`. Each uses the existing block scan + `traceUpdateBucket` +
`traceRowValue` pipeline with targeted extensions.

**HISTOGRAM — composite key with 3rd segment:**
`traceAccumulateRow` detects `FuncNameHISTOGRAM` and embeds the log2 bucket boundary
as a 3rd `"\x00"`-delimited segment in the composite key:
`"bucketIdx\x00attrGroupKey\x00bucketBoundary"`
For `span:duration` (stored as nanoseconds), the boundary is computed in seconds:
`pow(2, floor(log2(nanos/1e9)))`. For other fields: `pow(2, floor(log2(|v|)))`.
A dedicated `traceHistogramSeries` function reads back the 3rd segment to build
`__bucket` labels and counts. `ExecuteTraceMetrics` dispatches to it when HISTOGRAM.
Non-HISTOGRAM functions continue to use the 2-segment key format unchanged.

**Why separate series builder:** The 2-segment key produces one series per `attrGroupKey`.
The 3-segment key produces one series per `(attrGroupKey, bucketBoundary)`. These
are structurally different — `traceBuildDenseSeries` cannot handle 3-segment keys
without special-casing that would obfuscate the non-histogram path.

**QUANTILE — values slice + logComputeQuantile reuse:**
`traceUpdateBucket` adds a `FuncNameQUANTILE` case that appends each span's field
value to `bucket.values` (same as `logFuncQuantileOverTime` in `metrics_log.go`).
`traceRowValue` calls `logComputeQuantile(bucket.values, quantile)` — both functions
are in the same package, so no import is needed. The `quantile float64` parameter is
threaded through `traceRowValue`'s signature (mirroring `logRowValues`'s pattern).

**STDDEV — Welford online algorithm with `aggBucketState.mean` and `.m2`:**
Two new fields (`mean`, `m2`) are added to `aggBucketState` in `metrics_log.go`.
They are zero-initialized for log-metric buckets (no behavioral change for any log path).
`traceUpdateBucket` applies Welford's recurrence:
delta = v - mean; count++; mean += delta/count; m2 += delta\*(v-mean)
`traceRowValue` emits `sqrt(m2/(count-1))` for sample stddev, or NaN when `count < 2`.

**Back-ref:** `internal/modules/executor/metrics_trace.go:traceUpdateBucket`,
`internal/modules/executor/metrics_trace.go:traceRowValue`,
`internal/modules/executor/metrics_trace.go:traceHistogramSeries`,
`internal/modules/executor/metrics_trace.go:traceHistogramBucket`,
`internal/modules/executor/metrics_log.go:aggBucketState`

---

## NOTE-034: ExecuteStructural — Three-Phase Structural Query Algorithm

_Added: 2026-03-08_
_(Phase 2 implementation superseded by NOTE-078, 2026-04-17: byID map changed from map[string]int to map[[8]byte]int to eliminate string heap allocations.)_

`ExecuteStructural` ports the three-phase structural query algorithm from `api.go`
(`streamStructuralQuery`) into the modules executor package.

**Phase 1 (Collect):** All blocks are scanned via `queryplanner.Plan(nil, TimeRange{})` —
no pruning is applied. For each span, we record spanID, parentID, leftMatch, rightMatch keyed
by `[16]byte` trace ID. Using a fixed-size array key avoids the `fmt.Sprintf("%x", ...)` hex
allocation per span that the api.go version performs.

**Phase 2 (Resolve):** Parent indices are resolved with a `string(spanID)→index` map. Using
`string([]byte)` as a map key avoids allocating a persistent string; Go optimises this pattern.

**Phase 3 (Evaluate):** The structural operator is applied per trace. Matching right-side span
indices are deduplicated and emitted as `SpanMatch` entries.

**No lazy coalesced groups / early-stop:** Structural queries still rely on
`queryplanner.FetchBlocks` / `Reader.ReadBlocks` for aggressive I/O coalescing, but they do
not use the lazy coalesced-group / bloom-filter early-stop machinery. All candidate blocks
from `Plan(nil, TimeRange{})` are fetched up front (`FetchBlocks` is used directly, same as
`Execute` in `executor.go`) and then fully scanned.

**allMatchSet:** When a left or right filter is nil (empty `{}`), a local `allMatchSet` rowSet
is returned instead of calling `ColumnPredicate`. This avoids compiling a trivial program.

Back-ref: `internal/modules/executor/stream_structural.go`

---

## NOTE-035: Execute → Collect Migration and Sub-File Sharding

_Added: 2026-03-11_

**Decision:** The original `Execute` method (returning `*Result` with `[]SpanMatch`) has been
replaced by `Collect` (returning `[]MatchedRow`). The `Stream` method has been merged into
`Collect` — both now use the same lazy coalesced-group I/O path.

**Rationale:**

- `MatchedRow` carries a reference to the parsed `*Block`, allowing callers to extract
  arbitrary columns after collection (e.g., `SpanMatchFromRow`, `IterateFields`). The old
  `SpanMatch` required the executor to eagerly extract TraceID/SpanID during scan.
- Lazy coalesced-group I/O (previously Stream-only) is now the single I/O strategy for all
  filter queries. `FetchBlocks` (eager bulk fetch) is retained only for `ExecuteStructural`.

**Nil program semantics change:** The old `Execute` panicked on nil `program` when blocks
contained spans. `Collect` returns an error instead. This is safer for callers and consistent
with `StreamLogs`, and `StreamLogsTopK` which all return errors for nil program.

**Sub-file sharding (added 2026-03-11):** `CollectOptions.StartBlock` and `BlockCount` allow
the frontend sharder to partition a single blockpack file across multiple parallel jobs.
Sharding is applied post-planner (after pruning) so all statistics remain file-wide; only
the block iteration window is narrowed. `Collect` supports sharding.

Back-ref: `internal/modules/executor/stream.go:Collect`

---

## NOTE-036: planBlocks Unification — Intrinsic TOC Pruning in All Query Paths

_Added: 2026-03-14_

**Decision:** All five query paths (Collect, ExecuteTraceMetrics, ExecuteLogMetrics,
StreamLogs, CollectLogs) now use a shared `planBlocks` helper that runs:

1. `BuildPredicates` — converts vm.Program predicates into planner predicates
2. `PlanWithOptions` — range-index/fuse pruning and time range filtering
3. `fileLevelReject` — O(1) file-level fast reject using KLL bucketMin/bucketMax boundaries
4. `BlocksFromIntrinsicTOC` intersection — intrinsic-column fast reject

**Rationale:** Before this change, intrinsic TOC pruning was only active in `Collect`.
The four other paths called `planner.Plan` or `planner.PlanWithOptions` directly without
the `BlocksFromIntrinsicTOC` intersection step. Queries involving intrinsic columns (e.g.
trace:id, span:id) on the metrics or log paths could therefore scan blocks that would have
been pruned on the trace search path — wasting I/O.

**Why safe:** `BlocksFromIntrinsicTOC` returns nil when no pruning is possible (no intrinsic
section, no intrinsic predicates, or all blocks survive). The intersection is only applied
when it actually shrinks the selected set, so correctness is preserved and there is no
performance regression for queries with no intrinsic predicates.

Back-ref: `internal/modules/executor/plan_blocks.go:planBlocks`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`,
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/stream_log_topk.go:CollectLogs`

---

## NOTE-037: LogAttrs — Flat Slice Struct Instead of map[string]string

_Added: 2026-03-15_

**Decision:** `LogEntry.LogAttrs` field changed from `map[string]string` to a new
`LogAttrs` struct with parallel `Names []string` and `Values []string` slices.
`collectLogStringAttrs` uses slice append instead of map insertion.

**Rationale:** Per-row map allocation was a hot path. Most rows have zero or one
log.\* attribute (log.level, log.detected_level). A flat slice struct avoids the
map header allocation (~8 bytes overhead) and internal hash-table bucket
allocations entirely. For the common zero-attribute case, both slices remain nil.

**Impact on callers:**

- `logEntryFields.GetField`: linear scan over Names slice (typically 0–2 entries;
  faster than map lookup for such small N due to cache locality).
- `logEntryFields.IterateFields`: iterate parallel slice indices.
- `converter.logAttrsToLabelAdapters`: iterate `.Names`/`.Values` directly.
- Tests: no assertions on LogAttrs type; no changes needed.

Back-ref: `internal/modules/executor/stream_log.go:LogAttrs`,
`internal/modules/executor/stream_log_topk.go:collectLogStringAttrs`,
`api.go:logEntryFields`,
`benchmark/lokibench/converter.go:logAttrsToLabelAdapters`

---

## NOTE-059: cmp3 Generic Helper Reduces rowCompare Cyclomatic Complexity

_Added: 2026-03-16_

**Decision:** Introduced `cmp3[T cmp.Ordered](a, b T) (int, bool)` — a one-line wrapper
around `cmp.Compare` — in `column_provider.go`. All repeated three-way `switch { case v < t:
return -1, true; case v > t: return 1, true; default: return 0, true }` blocks in
`rowCompare` are replaced with `return cmp3(v, t)`.

**Rationale:** `rowCompare` had cyclomatic complexity 40 (CRITICAL tier) with the same
comparison pattern repeated 8+ times across all numeric and string types. Using the
stdlib `cmp.Compare` (available since Go 1.21) reduces line count from ~120 to ~50 and
cyclomatic complexity from 40 to ~20 with no behavior change.

**Float64 parse path:** The `ColumnTypeString` + `float64` branch (numeric threshold against
string column, used by `| latency_ms > 100` pipeline filters) retains its `strconv.ParseFloat`
call — `cmp3(parsed, f)` replaces only the comparison switch, not the parse step.

Back-ref: `internal/modules/executor/column_provider.go:rowCompare`,
`internal/modules/executor/column_provider.go:cmp3`

---

## NOTE-060: scanIntrinsicLeafRefs leaf.Values Loop Merged From Two Passes to One

_Added: 2026-03-16_

**Decision:** In `scanIntrinsicLeafRefs` (dict-format + exact-values branch), two sequential
`for _, v := range leaf.Values` loops were merged into one. Previously:

1. First loop: built `wantStr map[string]struct{}` and `wantInt map[int64]struct{}`
2. Second loop: built `bloomKeys [][]byte`

Both loops iterated the same `leaf.Values` slice with an identical `switch v.Type { case
TypeString/TypeInt/TypeDuration }` structure, with no ordering dependency between the
match-set population and bloom-key construction.

**Rationale:** The merge is a strict reduction — both outputs are constructed in a single
pass. The identical switch structure makes the merge mechanical. `bloomKeys` is appended
inline with the map insertions. No behavioral change.

**Impact:** Reduces `scanIntrinsicLeafRefs` by ~20 lines and cyclomatic complexity from
~35 to ~28.

Back-ref: `internal/modules/executor/predicates.go:scanIntrinsicLeafRefs`

---

## NOTE-061: logTopKEntry.ts Field Removed — Use entry.TimestampNanos Directly

_Added: 2026-03-16_

**Decision:** `logTopKEntry.ts uint64` has been removed. All heap comparisons
(`logTopKHeap.Less`, `logTopKCanSkipBlock`, `logTopKInsert`, early-skip guard, sort closures
in `logDeliverAll`) now read `entry.TimestampNanos` directly.

**Rationale:** `ts` was always set to the same value as `entry.TimestampNanos` at every
construction site. The redundant field added 8 bytes per entry × limit (e.g., 1000 entries =
8KB overhead), plus one extra assignment per row. There was no case where `ts != entry.TimestampNanos`.
Removing the field eliminates the class of bug where one is updated but the other is not.

Back-ref: `internal/modules/executor/stream_log_topk.go:logTopKHeap.Less`,
`internal/modules/executor/stream_log_topk.go:logTopKInsert`,
`internal/modules/executor/stream_log_topk.go:logTopKCanSkipBlock`

---

## NOTE-062: iterateLogRows Extracts Shared Block-Iteration Boilerplate

_Added: 2026-03-16_

**Decision:** The ~130 lines of shared block-iteration boilerplate that was duplicated between
`logTopKScan` and `logCollectAll` has been extracted into `iterateLogRows`. Both callers
are now ~15-line wrappers that pass their differing logic as callbacks:

- `canSkipBlock func(meta shared.BlockMeta) bool` — `logTopKScan` passes heap-based block pruning;
  `logCollectAll` passes nil (never skip).
- `fn func(ts uint64, entry LogEntry) bool` — `logTopKScan` passes the NOTE-031 early-skip guard
  - heap insertion; `logCollectAll` passes a slice append.

**Rationale:** Both functions shared: `CoalescedGroups` + `blockToGroup`, `ReadGroup` loop,
`ResetInternStrings` + `ParseBlockFromBytes`, `ColumnPredicate` evaluation, NOTE-021 time
pre-filter, column cache setup, and the per-row pipeline loop. Any fix to the shared boilerplate
(e.g., NOTE-021, NOTE-001, NOTE-SL-017) previously had to be applied twice. The extraction
eliminates this duplication class.

**Complexity impact:** Combined cyclomatic complexity drops from (36+30)=66 to one ~30
(`iterateLogRows`) + two trivial callers.

Back-ref: `internal/modules/executor/stream_log_topk.go:iterateLogRows`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-038: Unified Intrinsic Pre-Filter — Partial-AND for Mixed Queries

_Added: 2026-03-16_

**Decision:** `collectFromIntrinsicRefs` is rewritten as a unified 4-case dispatcher
replacing the separate `collectFromIntrinsicRefs` (plain) and `collectTopKFromIntrinsicRefs`
(topK) functions. The `Collect` entry gate changes from `ProgramIsIntrinsicOnly` to
`hasSomeIntrinsicPredicates`, extending the intrinsic pre-filter to mixed queries.

**Problem with the old design:**
Mixed queries like `{ resource.service.name = "svc" && span.http.method = "GET" }` bypassed
the intrinsic pre-filter entirely because `ProgramIsIntrinsicOnly` returned false. The pre-filter
was an all-or-nothing gate: if any non-intrinsic column appeared, the full block scan ran.

**New design — partial-AND semantics:**
`evalNodeBlockRefsPartialAND` is a new variant of `evalNodeBlockRefs` where AND nodes skip
unevaluable (non-intrinsic) children rather than failing. The result is a superset of the
true matching rows. `blockRefsFromIntrinsicPartial` uses this variant to build candidate refs
for mixed queries.

**Why partial-AND is safe for AND nodes:**
The pre-filter is a superset: it may return rows that do NOT satisfy non-intrinsic conditions.
After fetching the candidate blocks, `program.ColumnPredicate` re-evaluates the full predicate,
eliminating false positives. This is the same principle used by bloom-filter + range-index block
pruning in `planBlocks`: the planner produces a superset, the VM corrects it.

**Why OR nodes remain fail-fast:**
An unevaluable OR child would produce an unbounded result — potentially the entire file's row
set. There is no useful partial OR pre-filter: you cannot narrow candidates when any alternative
is unconstrained. Keeping OR fail-fast means `blockRefsFromIntrinsicPartial` falls through to
the full block scan when an evaluable-but-unevaluable-OR is encountered, which is conservative
but correct.

**4-case dispatch:**

| ProgramIsIntrinsicOnly | TimestampColumn | Case | Block reads                                              |
| ---------------------- | --------------- | ---- | -------------------------------------------------------- |
| true                   | empty           | A    | minimal (candidate blocks)                               |
| true                   | set             | B    | zero (IntrinsicFields rows)                              |
| false                  | empty           | C    | minimal (candidate blocks + ColumnPredicate)             |
| false                  | set             | D    | minimal (candidate blocks + ColumnPredicate + topK heap) |

**Global top-K correctness for Case D:**
The partial-AND pre-filter is a superset — it never excludes a row that satisfies all intrinsic
predicates. Therefore all true matching rows (those satisfying both intrinsic and non-intrinsic
conditions) are present among the candidate blocks. ColumnPredicate eliminates non-intrinsic
false positives. topKScanRows then finds the globally correct top-K timestamp order within the
true matching rows. SPEC-STREAM-7 is preserved.

**Dead code removed:**
`buildPredicateMatchSet`, `evalNodeMatchKeys`, `collectTopKFromIntrinsicRefs`,
`unionSortedKeys`, `intersectSortedKeys` — all deleted. `collectTopKFromIntrinsicRefs`
was a parallel implementation of `evalNodeBlockRefs` in packed-key representation; that
duplication is eliminated by inlining the packed-key construction into `collectIntrinsicTopK`.

**Relationship to NOTE-039 (in code comments referencing the old design):**
NOTE-039 stated that both OR and AND must fail fast on unevaluable children because refs are
returned directly without VM re-evaluation. This remains true for `evalNodeBlockRefs` (used
on the pure-intrinsic path). The new `evalNodeBlockRefsPartialAND` explicitly relaxes the AND
constraint for the mixed-query path, where VM re-evaluation is mandatory.

Back-ref: `internal/modules/executor/predicates.go:hasSomeIntrinsicPredicates`,
`internal/modules/executor/predicates.go:evalNodeBlockRefsPartialAND`,
`internal/modules/executor/predicates.go:blockRefsFromIntrinsicPartial`,
`internal/modules/executor/stream.go:collectFromIntrinsicRefs`

---

## NOTE-039: EX-INT-06 Comment Updated — Case C, Not True Fallback

_Added: 2026-03-16_

The comment in `TestIntrinsicFastPath_FallbackToBlockScan` previously said "fast path is
not applicable" for the query `{ resource.service.name =~ "loki-.*" && span.http.method = "GET" }`.
After the gate change from `ProgramIsIntrinsicOnly` to `hasSomeIntrinsicPredicates` (NOTE-038),
this query now takes Case C (mixed + no sort): the intrinsic pre-filter narrows candidates
by service name, then VM re-evaluation eliminates all rows because no spans have http.method
set. The comment was updated to accurately describe Case C behavior.

---

## NOTE-040: EX-INT-13 — True Non-Intrinsic-Only Fallback Path Test

_Added: 2026-03-16_

Added `TestCollect_NonIntrinsicOnly_FallsBackToBlockScan` (EX-INT-13) to exercise the true
fallback path: a query with zero intrinsic leaves (e.g., `{ span.http.method = "GET" }`)
causes `hasSomeIntrinsicPredicates` to return false, and the executor falls through to the
full block scan. The test confirms no error and no panic, and returns 0 results because none
of the test spans have `http.method` set.

---

## NOTE-041: SPECS.md §4.2 MatchedRow — IntrinsicFields Field Added

_Added: 2026-03-16_

The `IntrinsicFields modules_shared.SpanFieldsProvider` field added to `MatchedRow` in the
unified pre-filter implementation (NOTE-038) was absent from the §4.2 struct definition in
SPECS.md. The definition now matches the actual struct in `stream.go`. `SPEC-STREAM-9`
already described the semantics (IntrinsicFields rows for Case A/B zero-block-read paths);
this change ensures the struct definition itself is also complete.

---

## NOTE-042: collectIntrinsicTopK Sort Path — Map Lookup Replaces O(N) Scan of Timestamp Blob

_Added: 2026-03-16_

**Decision:** In `collectIntrinsicTopK`'s sort path (M < sortScanThreshold), the O(N) scan
of the timestamp blob via `ScanFlatColumnRefsFiltered` is replaced for small M by a
map-then-sort approach: build a `packed-key → timestamp` map from the full decoded
`IntrinsicColumn`, look up each matching ref's timestamp in O(1), sort the M pairs by
timestamp, and take top K.

**Problem with the previous stash approach:**
The stash tried to use `planBlocks` block ordering (KLL sketch, newest-first for MostRecent)
to iterate matching refs. Block ordering only provides inter-block ordering — it says nothing
about per-row timestamp ordering within a single block. When all matching refs live in a
single block (common in tests and for rare-service queries), block ordering provided zero
information, violating the descending-timestamp assertion in EX-INT-10.

**Key observation — flat column layout:**
The flat timestamp column (`span:start`, `log:timestamp`) stores `BlockRefs` in ascending
timestamp order, NOT in ascending packed-key order. Binary search by packed key is therefore
incorrect. The correct lookup is: build a `map[uint32]uint64` (packed-key → timestamp) from
the decoded column's parallel `BlockRefs` and `Uint64Values` arrays, then look up each ref.

**Performance characteristics:**

- Map build: O(N) — same cost as the scan path but paid once per query.
- Per-ref lookup: O(1) hash map (vs O(N/M) amortized for scan).
- Sort: O(M log M) for M matching refs.
- Effective for M << N (rare-service queries); scan path remains for M >= sortScanThreshold.
- For Q35 (M=15, N=130K): map lookup + sort ≈ 130K insertions + 15 log(15) comparisons.
  The dominant cost is the O(N) map build, but this avoids decompressing the blob again
  since `GetIntrinsicColumn` returns the already-decoded column from cache.

**Test fix:**
`TestCollect_PureIntrinsicWithSort_ZeroBlockRead` (EX-INT-10) was failing because the stash
used block-level ordering which does not provide per-row timestamp order within a single
block. Map-then-sort correctly retrieves per-row timestamps regardless of block/row layout.

Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopK`

---

## NOTE-043: CollectStats ExecutionPath Telemetry and Fast-Path OnStats Wiring

_Added: 2026-03-17_

**Decision:** `CollectStats` gains four new fields to identify which of the eight
execution paths ran for a given `Collect` call:

- `ExecutionPath string`: one of "intrinsic-plain" (Case A), "intrinsic-topk-sort"
  (Case B map path), "intrinsic-topk-scan" (Case B scan path), "mixed-plain" (Case C),
  "mixed-topk" (Case D), "block-plain" (block-scan no sort), "block-topk" (block-scan
  with topK heap), "intrinsic-need-block-scan" (fast path tried but fell through).
- `IntrinsicRefCount int`: number of refs from `BlockRefsFromIntrinsicTOC` (M for Case B).
- `IntrinsicScanCount int`: entries visited by `ScanFlatColumnRefsFiltered` in Case B
  scan path. Zero for the map path and all other paths.
- `MixedCandidateBlocks int`: number of unique candidate blocks from
  `blockRefsFromIntrinsicPartial` for Cases C and D. Zero for all other paths.

**Problem solved:** The `OnStats` defer in `Collect` (SPEC-STREAM-6) fires only for the
block-scan fallback. Fast-path returns from `collectFromIntrinsicRefs` exited before the
defer was registered, giving callers no stats for the most common query case (intrinsic
fast path). This was an observable gap: setting `OnStats` on a service-name filter query
produced no callback at all.

**Fix:** A `*CollectStats` pointer is passed to `collectFromIntrinsicRefs` and all four
sub-functions. Each path populates its fields. `Collect` calls `opts.OnStats(fastStats)`
synchronously on successful fast-path return (`err != errNeedBlockScan`). For
`errNeedBlockScan`, the partial stats are discarded and the block-scan defer provides the
final stats as before.

**`SortScanThreshold` exported:** `sortScanThreshold` (unexported const) renamed to
`SortScanThreshold` (exported var) to allow test overrides that force the scan path for
small M. Production code does not modify this variable.

**No double callback:** Fast-path and block-scan callbacks are mutually exclusive.
`opts.OnStats(fastStats)` is called only when `err != errNeedBlockScan`, which means
`return rows, err` follows immediately — the block-scan defer is registered AFTER this
return and therefore never fires for the fast-path case.

Back-ref: `internal/modules/executor/stream.go:CollectStats`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/stream.go:collectFromIntrinsicRefs`,
`internal/modules/executor/stream.go:SortScanThreshold`

---

## NOTE-044: collectIntrinsicTopK KLL Path — Block-Level MaxStart Ordering

_Added: 2026-03-17_

**Decision:** In `collectIntrinsicTopK`'s small-M path (M < SortScanThreshold), replace the
flat map-then-sort approach ("intrinsic-topk-sort") with a block-aware KLL path
("intrinsic-topk-kll") that groups matching refs by BlockIdx and orders blocks by
`BlockMeta.MaxStart` DESC before collecting per-row timestamps.

**What changed:**

- M refs are grouped into `blockRefs[BlockIdx]` and unique block indices collected into
  `blockOrder []int`.
- `blockOrder` is sorted by `r.BlockMeta(bi).MaxStart` DESC (largest MaxStart first — newest
  block first). `MaxStart` is the KLL-sketch upper bound on `span:start` within a block.
- For each block (newest first), each ref's timestamp is looked up in the packed-key →
  timestamp map (same O(N) map build as before).
- (ref,ts) pairs are collected in block-descending order, then sorted globally by timestamp
  DESC (or ASC for forward) and top-K selected.
- `ExecutionPath` is set to `"intrinsic-topk-kll"` to distinguish from the old sort path.

**Why block ordering by MaxStart:**
Block-level ordering by MaxStart biases collection toward newer blocks first. For the common
case where matching refs are clustered in recent blocks, this provides a natural pre-sort
benefit before the final O(M log M) sort. Future extensions can use early termination here
(once the heap is full and the next block's MaxStart is below the heap min, all remaining
blocks are pruned) without correctness risk — the current implementation always sorts all M
pairs to ensure globally correct top-K.

**Correctness:**
The final sort over all M (ref,ts) pairs preserves the global ordering invariant. Block-level
MaxStart ordering is an optimization hint, not a correctness dependency.

**ExecutionPath telemetry:**
The old path "intrinsic-topk-sort" is retired. The new path "intrinsic-topk-kll" replaces it
for M < SortScanThreshold. The scan path "intrinsic-topk-scan" (M >= SortScanThreshold) is
unchanged. `SPEC-STREAM-9` back-ref and SPECS.md §4.4 and §6.1 updated accordingly.

Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopK`,
`internal/modules/executor/stream_perf_test.go:TestCollect_IntrinsicTopK_KLLPath`

**Correction to NOTE-043 bullet list:** NOTE-043's `ExecutionPath` constant list
(`intrinsic-topk-sort`) is retired by this note; the correct value is `intrinsic-topk-kll`
for all non-scan small-M paths. The NOTE-043 bullet `"intrinsic-topk-sort" (Case B map path)`
should be read as `"intrinsic-topk-kll" (Case B KLL path, M < SortScanThreshold)`.

---

## NOTE-045: File-Level Bloom Reject — Fuse8 and Compact Bloom for Equality Pruning

_Added: 2026-03-20; updated: 2026-04-02 (CMS removed, bloom path documented)_

`fileLevelBloomReject` in `plan_blocks.go` uses file-level bloom filters to reject a file
entirely when a queried equality value is definitely absent from every block.

**Two filter types:**

- `resource.service.name` and other string columns: `FileBloom` (Fuse8 per column, FBLM section).
  `bloomRejectString` checks each value against the per-column Fuse8 filter in the `FileBloom`.
- `trace:id`: compact per-file bloom via `r.MayContainTraceID`. `bloomRejectTraceID` checks
  each 16-byte trace ID value.

**Call chain:**

```
fileLevelBloomReject(r, nodes)
  └─ bloomRejectByEquality(r, fb, node)          // recursive over AND/OR tree
       ├─ bloomRejectTraceID(r, values)           // for node.Column == "trace:id"
       └─ bloomRejectString(fb, col, values)      // for string columns via FileBloom
```

**AND/OR semantics:**

- AND node: reject if ANY child rejects (conservative).
- OR node: reject only if ALL children reject (conservative).
- Leaf: only equality predicates (Values non-empty, no Min/Max/Pattern).

**Limitations:**

- `FileBloom` only covers columns for which a Fuse8 filter was written (typically
  `resource.service.name` and high-cardinality string columns). Other columns pass through.
- Both filter types have false positives (Fuse8 ~0.39% FPR, compact bloom also FPR > 0).
  False positives mean we don't prune the file — conservative and safe.
- This is an optimization only; correctness is maintained by the block-level scan that follows.

**Note on prior CMS path:** The original NOTE-045 described a `fileLevelCMSReject` function
that used a merged `FileSketchSummary` (Count-Min Sketch). That function and `cmsRejectByNodes`
were removed when CMS was eliminated from the sketch system (2026-04-02; see queryplanner/NOTES.md
NOTE-018). File-level pruning is now exclusively bloom-based.

Back-ref: `internal/modules/executor/plan_blocks.go:fileLevelBloomReject`,
`internal/modules/executor/plan_blocks.go:bloomRejectByEquality`,
`internal/modules/executor/plan_blocks.go:bloomRejectString`,
`internal/modules/executor/plan_blocks.go:bloomRejectTraceID`

## NOTE-046: Zero-Block-Read Fast Path for ExecuteTraceMetrics

**Date:** 2026-03-20

`ExecuteTraceMetrics` previously called `r.ReadBlocks()` unconditionally — reading full block
bytes even for queries like `{ } | count_over_time()` where only `span:start` is needed.

**Fast path condition:** all `wantColumns` (span:start + aggregate field + group-by + filter columns)
must be in `traceIntrinsicColumns`. When true, the query is answered entirely from the intrinsic
section — which is read once at `NewReaderFromProvider` time and cached — with zero full block reads.

**Eligible queries include:** `count_over_time()`, `rate()`, `histogram_over_time(span.duration)`,
any aggregate grouped by `resource.service.name` or `span:status`, `span:kind`, etc.

**Not eligible:** any query referencing non-intrinsic columns (e.g. `sum(span.latency_ms)`,
`count by (span.http.status_code)`) — these fall through to the existing block-scan path.

**Implementation:** `executeTraceMetricsIntrinsic` in `metrics_trace_intrinsic.go`.

- No predicates (`{ }`) with group-by or aggregate field: builds `keyToBucket` (packKey → bucketIdx) by iterating `span:start` flat column. For count/rate with no group-by, span:start is streamed inline without allocating `keyToBucket`.
- Intrinsic predicates: `BlockRefsFromIntrinsicTOC(r, program, 0)` filters refs before building `keyToBucket`.
- count/rate, no group-by: streams `span:start` inline without `keyToBucket` (zero intermediate maps).
- Group-by: `buildGroupKeyMap` iterates dict/flat column entries directly to build one composite key map.
- Aggregate field + group-by: `buildAggValsMap` (one map) combined with `groupKeyMap` in final pass.
- Aggregate field, no group-by: `streamAggColumnNoGroupBy` iterates the aggregate column directly.

**Back-refs:** `metrics_trace_intrinsic.go`, `metrics_trace.go:executeTraceMetricsIntrinsic` call site.

---

## NOTE-047: Unified Field Population in collectIntrinsicPlain

**Date:** 2026-03-23
_Updated: 2026-03-29_

Case A (pure intrinsic + no sort) originally dispatched on `hasRangePredicate(program)` to
choose between two sub-paths:

- **Range path:** `lookupIntrinsicFields` populated `MatchedRow.IntrinsicFields` directly from
  cached intrinsic blobs (zero additional I/Os). `MatchedRow.Block` was nil.
- **Equality path:** `forEachBlockInGroups` fetched only the matching blocks; field population
  was deferred until block decode. `MatchedRow.IntrinsicFields` was nil.

**Current state (2026-03-29):** The two-sub-path dispatch has been unified.
`collectIntrinsicPlain` now always uses `lookupIntrinsicFields` for field population regardless
of predicate type. The `hasRangePredicate` helper and the equality-specific block-fetch path
have been removed. All matched rows have `MatchedRow.IntrinsicFields` populated at collection
time; `MatchedRow.Block` is nil for the intrinsic-only case.

**Rationale:** The equality block-fetch path added I/O complexity without a measurable latency
advantage in production workloads. `lookupIntrinsicFields` reads from already-cached intrinsic
blobs for both range and equality predicates, keeping the code path uniform and eliminating the
`hasRangePredicate` branch.

**Back-refs:** `stream.go:collectIntrinsicPlain`,
`stream.go:collectFromIntrinsicRefs` (call site).

---

## NOTE-048: Parallel Phase 1 Fetch in forEachBlockInGroups

**Date:** 2026-03-24

`forEachBlockInGroups` is split into two phases:

**Phase 1 (parallel I/O):** All coalesced groups are fetched concurrently via goroutines.
`r.ReadGroup` only calls `r.provider.ReadAt` which is stateless and safe for concurrent use.
Parallelism overlaps S3/disk latency across groups — at 36ms per request, reading 3 sequential
groups takes 108ms; in parallel it takes 36ms.

**Phase 2 (sequential decode):** `Reader.ParseBlockFromBytes` is not safe for concurrent use
on the same `*Reader` instance; Phase 2 remains single-goroutine.

**Early-stop trade-off:** All groups are pre-fetched in Phase 1 before Phase 2 begins. If
`fn` returns early (e.g. limit reached), the remaining group data is discarded — the S3 reads
have already been issued. In practice `forEachBlockInGroups` is only called for Case A
(pure intrinsic, equality predicates) where refs cluster in 1–3 blocks, so at most 1–2
groups are ever pre-fetched. The over-fetch cost is negligible.

**Buffer lifecycle:** Each group's `map[int][]byte` is released entry-by-entry via
`delete(fetched[i].data, blockIdx)` immediately after `ParseBlockFromBytes` returns,
letting the GC reclaim block bytes before the next group is processed. The local `bwb`
variable holds `bwb.RawBytes` alive through the `fn` call (NOTE-001 lazy decode safety).

**Back-refs:** `stream.go:forEachBlockInGroups`

---

## NOTE-049: scanBlocks Intern Map Pool and Clone Elimination

_Added: 2026-03-25_

**Problem:** `scanBlocks` called `r.ParseBlockFromBytes` twice per matching block (first pass
for predicate evaluation + second pass to decode result columns). Each call allocated a fresh
`make(map[string]string)` intern map inside `ParseBlockFromBytes`. With hundreds of blocks
per query, this produced hundreds of map allocations and corresponding GC pressure.

Additionally, `rows := slices.Clone(rowSet.ToSlice())` allocated a redundant copy of the
matched-row index slice. The clone comment stated it was needed because "ToSlice returns the
backing slice and must not be modified", but `rowSet` is never accessed again after line 382
in `scanBlocks` — no `Contains` calls occur on the `scanBlocks` code path after `ToSlice()`.

**Fix 1 — Intern map pool:** `scanBlocks` now acquires a pooled `map[string]string` from
`modules_reader.AcquireInternMap()` at the start of each block iteration and passes it to
`r.ParseBlockFromBytesWithIntern()` for both parse passes. The map is released via
`modules_reader.ReleaseInternMap()` after `streamSortedRows` completes — this is the correct
release point because lazy columns registered during the first parse may call `decodeNow()`
during row emission, and they reference the intern map directly. Releasing before
`streamSortedRows` would corrupt lazy string decodes.

**Fix 2 — Clone elimination:** Changed `rows := slices.Clone(rowSet.ToSlice())` to
`rows := rowSet.ToSlice()`. The clone was conservative but unnecessary on this code path.
`streamSortedRows` sorts `rows` in-place, but since `rowSet` is not used after `ToSlice()`,
sorting the backing slice directly is safe.

**Lifetime contract for pooled intern maps:**

1. Acquire: at block-iteration start in `scanBlocks`.
2. Keep alive: through both `ParseBlockFromBytesWithIntern` calls AND all of `streamSortedRows`
   (where lazy `decodeNow()` calls may occur).
3. Release: immediately after `streamSortedRows` returns (or on any early-exit error path).

**Back-refs:** `stream.go:scanBlocks`,
`internal/modules/blockio/reader/column.go:AcquireInternMap`,
`internal/modules/blockio/reader/reader.go:ParseBlockFromBytesWithIntern`,
`internal/modules/blockio/reader/NOTES.md:NOTE-006`

## NOTE-050: Intrinsic Columns — Stored Exclusively in Intrinsic TOC Section

_Added: 2026-03-25_

_Addendum (2026-03-25): Original entry claimed dual-storage (block columns AND intrinsic
section). That was incorrect. Intrinsic columns are written ONLY to the intrinsic TOC
section; `addPresent` calls for these columns were removed. This addendum corrects the record._

**Decision:** Intrinsic columns (trace:id, span:id, span:parent_id, span:name, span:kind,
span:start, span:duration, span:status, span:status_message, resource.service.name) are
stored exclusively in the intrinsic TOC section. They are NOT written to block column
payloads. `ParseBlockFromBytes` returns nil columns for these names; this is handled by
`nilIntrinsicScan` which produces FullScan results for AND intersection.

**Rationale:**

- The intrinsic section enables fast pre-filtering (bloom, min/max) and O(1) identity
  lookup via `lookupIntrinsicFields` without full block decodes.
- Removing dual-storage eliminates redundant data in block payloads.

**Consequences for executor:**

- `searchMetaCols` no longer lists trace-signal intrinsic column names because those
  columns are served via the intrinsic section path (`lookupIntrinsicFields`) rather
  than through the `wantColumns` second-pass decode. Log-signal identity columns remain
  in `searchMetaCols` because log blocks use different identity column names.
- `secondPassCols` injects all `traceIntrinsicColumns` (trace:id, span:id, span:start,
  etc.) so that `lookupIntrinsicFields` populates `IntrinsicFields` correctly. The
  names are passed to `ParseBlockFromBytes` as `wantColumns`; they return nil columns
  since the data is absent from block payloads. Identity values come from `lookupIntrinsicFields`.
- `SpanMatchFromRow` accepts a `*Reader` parameter and calls `lookupIntrinsicFields`
  to populate identity fields from the intrinsic section for the block-scan path.
- `RangeNode.MinInclusive`/`MaxInclusive` fields distinguish `>` vs `>=` and `<` vs `<=`
  for the flat-column intrinsic scan, ensuring correctness at exact boundaries.

**Back-ref:** `internal/modules/executor/predicates.go:searchMetaCols`,
`internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/executor.go:SpanMatchFromRow`,
`internal/vm/bytecode.go:RangeNode`,
`internal/modules/blockio/writer/writer_block.go:newBlockBuilder`

## NOTE-051: Mixed-OR Predicates — False-Negative Limitation for OR(intrinsic, non-intrinsic)

_Added: 2026-03-25_

**Decision:** The block-scan post-filter (`filterRowSetByIntrinsicNodes` /
`rowSatisfiesIntrinsicNodesOR`) uses a conservative skip for non-intrinsic leaves in an
OR context: when a leaf references a non-intrinsic column it returns `false` (no match)
rather than `true` (pass-through). This can produce false negatives for OR queries such as
`{span:name="foo" || span.http.url="bar"}` where a row matches only the non-intrinsic branch.

**Rationale:** Returning `true` for non-intrinsic leaves in OR would cause false positives
because `nilIntrinsicScan` returns `FullScan` for nil intrinsic columns (columns absent from
block payloads). In an AND context `FullScan` is an identity element (safe); in an OR context
it would admit every row regardless of the non-intrinsic predicate value, producing incorrect
results. Fixing this correctly requires a two-pass approach that evaluates non-intrinsic
columns before composing the OR — a more invasive architectural change that is deferred.

**Consequences:**

- Queries combining an intrinsic predicate with a non-intrinsic predicate under OR may drop
  rows that satisfy only the non-intrinsic branch during block-level scanning.
- Impact is low: OR between intrinsic columns and user-attribute columns is uncommon in
  practice. Queries with `Limit > 0` are unaffected because they use the intrinsic fast path
  which does not go through `filterRowSetByIntrinsicNodes`.
- Full-table scans (no limit, no intrinsic index pruning) are the only affected path.

**Back-ref:** `internal/modules/executor/predicates.go:rowSatisfiesIntrinsicNodesOR`

**Addendum (2026-04-17, NOTE-076):** The `OR(non-intrinsic-only)` sub-case — where ALL
OR children are non-intrinsic — is now fixed. `rowSatisfiesIntrinsicNodesOR` returns `true`
(pass-through) for such nodes. The documented false-negative limitation in NOTE-051 applies
only to `OR(intrinsic, non-intrinsic)` where at least one child IS intrinsic but another
child is non-intrinsic. That case is not changed by NOTE-076.

## NOTE-052: Dual Storage Coexistence — Block Columns and Intrinsic Section

_Added: 2026-03-26_

**Decision:** After the rollback of PR #172 (see writer NOTE-002), dual storage is in effect:
intrinsic columns are present in BOTH block column payloads AND the intrinsic TOC section.
Both fast paths and block-scan paths remain valid and coexist.

**Two access patterns, two storage layers:**

1. **Block column payloads** (via `ParseBlockFromBytes` + `GetColumn`) — used by the block-scan
   path for predicate evaluation and result materialization. O(1) row access by (blockIdx, rowIdx)
   once the block is in memory.

2. **Intrinsic TOC section** (via `BlockRefsFromIntrinsicTOC`, `ScanFlatColumnRefsFiltered`,
   `lookupIntrinsicFields`) — used by the intrinsic fast paths (Cases A–D from
   `collectFromIntrinsicRefs`) for zero-block-read query execution, TOC-level bloom pruning,
   and metrics aggregation over intrinsic columns.

**Executor workarounds remain but are now conservative no-ops for block-scan:** The
`nilIntrinsicScan` (SPEC-STREAM-10.1), `userAttrProgram` (SPEC-STREAM-10.2), and
`filterRowSetByIntrinsicNodes` (SPEC-STREAM-10.3) mechanisms introduced for the
exclusive-intrinsic model still compile and run. With dual storage, `Block.GetColumn` returns
a non-nil column for all intrinsic names, so `nilIntrinsicScan` is never triggered during
block-scan. These mechanisms add no overhead in the dual-storage regime and provide
defence-in-depth should a future format version again omit intrinsic columns from block payloads.

**Back-ref:** `internal/modules/executor/stream.go:collectFromIntrinsicRefs`,
`internal/modules/executor/column_provider.go:nilIntrinsicScan`,
`internal/modules/executor/predicates.go:userAttrProgram`,
`internal/modules/blockio/writer/NOTES.md:NOTE-002`

---

## NOTE-053: Replace CollectStats/OnStats Callback with QueryStats Return Value

_Added: 2026-03-30_

**Decision:** Remove the `OnStats func(CollectStats)` callback from `CollectOptions` and
the `CollectStats` struct entirely. Replace them with a `QueryStats` second return value
from `Collect` and `CollectLogs`. Public API type aliases `QueryStats` and `StepStats`
are exported from `api.go`.

**Rationale:** The callback pattern created ergonomic friction — callers had to declare
a `var statsOut CollectStats` before the call and pass a closure. It was also easy to miss
the callback entirely (no compiler enforcement). Returning stats as a value follows the
standard Go convention for enriched return types (`net/http` `Response`, `sql` `Result`)
and makes the stats impossible to miss in call sites.

**Design — QueryStats:**

- `ExecutionPath string` — one of the 8 path constants (unchanged from CollectStats).
- `TotalDuration time.Duration` — wall-clock duration for the full call.
- `Steps []StepStats` — one entry per phase that ran. Steps are absent if the phase did
  not execute (e.g., no `"plan"` step on intrinsic fast paths).

**Design — StepStats:**

- `Name string` — phase name: `"plan"`, `"intrinsic"`, `"mixed-prefilter"`, `"block-scan"`.
- `Duration time.Duration` — wall-clock for this phase.
- `BytesRead int64` — raw bytes read from storage during this phase.
- `IOOps int` — number of ReadGroup calls (coalesced I/O operations).
- `Metadata map[string]any` — phase-specific numeric fields (total_blocks, fetched_blocks,
  ref_count, scan_count, candidate_blocks, explain, etc.). Nil map is safe to read.

**slog.Warn on errNeedBlockScan fallback:** When the intrinsic fast path falls through to
the full block scan, `slog.Warn("intrinsic fast path fell through to full block scan", ...)`
is emitted. This replaces the previously silent fallback and helps operators diagnose
unexpected path switches in production.

**Consequence:** All callers of `Collect` and `CollectLogs` must update to the 3-value
return. Callers that do not need stats use `_` for the second return. The `LogQueryStats`
type (formerly in `api.go`) is removed; `QueryStats` covers both trace and log queries.

Back-ref: `internal/modules/executor/query_stats.go:QueryStats`,
`internal/modules/executor/query_stats.go:StepStats`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/stream_log_topk.go:CollectLogs`,
`api.go:QueryStats`, `api.go:StepStats`

---

## NOTE-054: attrVals Scratch-Slice Reuse with clear()

_Added: 2026-04-08_

**Decision:** `attrVals []string` is allocated once per block (outer loop in
`ExecuteTraceMetrics` and `ExecuteLogMetrics`) and passed as a scratch parameter to
`traceAccumulateRow` / `logAccumulateRow`. Each function clears the slice with `clear(attrVals)`
as its very first statement, before any early-return guards.

**Safety argument:** `strings.Join(attrVals, "\x00")` builds a new string by copying the
slice contents. The resulting `attrGroupKey` / `compositeKey` string is independent of the
backing array. Reusing `attrVals` on the next call cannot alias or corrupt previously built
keys. The `clear` at function entry (not at the group-key build site) eliminates the latent
hazard where an early return would leave stale values that a future refactor might read before
the next `clear`.

**Allocation benefit:** Eliminates one `make([]string, N)` allocation per matched row in
metrics queries — from O(matched-rows) allocs to O(blocks) allocs for the scratch slice.
For a 200-row single-block query with 1-element GroupBy, this removes 200 allocs/op.

**Related changes in this PR:**

- `blockOrder` / `blockRefs` in `collectIntrinsicTopKKLL` capped at `min(len(refs), 64)`
  instead of `len(refs)`. Distinct block count per query is typically far smaller than the
  total ref count; 64 is a safe upper bound for common block fan-out.
- `results` in the `block-plain` path: when `opts.Limit == 0` (unlimited), the slice is
  left nil rather than preallocated to `len(plan.SelectedBlocks)` (block count is not a
  useful bound for span-level results).

Back-ref: `internal/modules/executor/metrics_trace.go:traceAccumulateRow`,
`internal/modules/executor/metrics_log.go:logAccumulateRow`,
`internal/modules/executor/stream.go:collectIntrinsicTopKKLL`,
`internal/modules/executor/stream.go:Collect` (block-plain path)

---

## NOTE-055: streamHistogramGroupBy — Dict Amortization for Histogram Group-By Path

_Added: 2026-04-14_

**Decision:** The `buildAggValsMap` + for-loop two-step in `accumulateIntrinsicBuckets`
for `agg.Function == vm.FuncNameHISTOGRAM` is replaced by `streamHistogramGroupBy`,
which streams the aggregate column directly and emits histogram buckets in a single pass.

**Rationale:** `buildAggValsMap` allocates a `map[uint32]float64` of size `len(keyToBucket)`
(one float64 per in-range span). The subsequent for-loop then calls
`intrinsicHistogramBoundary` and `strconv.FormatFloat` for each span individually.

For the dict column format (typical for `span:duration` which has ~30 distinct duration
buckets across 10K+ spans), both operations can be computed once per dict entry and
reused for every ref under that entry. `streamHistogramGroupBy` exploits this by hoisting
`boundary` and `boundaryStr` outside the inner ref loop.

For the flat column format there is no amortization win (each ref has a unique value), but
the `map[uint32]float64` allocation is still eliminated — replaced by
`map[uint32]struct{}` (seen-map, 8x smaller per entry) which is only needed to handle the
absent-row fallback.

**Invariant preserved:** Spans absent from the aggregate column fall into the boundary-0
bucket, matching `streamAggColumnNoGroupBy` and the block-scan path.

**Composite key format unchanged:**
`strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKey + "\x00" + boundaryStr`

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamHistogramGroupBy`,
`internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBuckets`

---

## NOTE-056: buildGroupKeyMap Single-Group-By Fast Path — colVals Elimination (2026-04-14)

_Added: 2026-04-14_

**Decision:** Added a fast path in `buildGroupKeyMap` for `len(groupBy) == 1` that writes
values directly into the output map, eliminating the `colVals` intermediate map allocation.

**Rationale:** The general multi-group-by path allocates
`colVals := make(map[uint32]string, len(keyToBucket))` for each group-by column.
For the overwhelmingly common case of a single group-by column (e.g.
`by (resource.service.name)`), this allocation is unnecessary — values can be written
directly into `out`. The fast path removes one map allocation from the critical path.

For `len(groupBy) == 1`: one fewer `map[uint32]string` allocation per query.
For `len(groupBy) > 1`: unchanged; the multi-column path continues to use per-column
`colVals` maps.

**Invariant preserved:** Rows absent from the group-by column receive an empty string in
`out`, matching the Tempo convention (Tempo `strings.Join(attrVals, "\x00")` produces
empty string for absent column positions).

**Absent-pk fill:** The fast path explicitly fills absent pks with `""` after the column
scan, matching the multi-column path's behavior of iterating `keyToBucket` for every row.

---

## NOTE-057: ExecuteTraceMetrics — Lazy CoalescedGroups I/O (M-20) (2026-04-15)

_Added: 2026-04-15_

**Decision:** Replaced the eager `r.ReadBlocks(plan.SelectedBlocks)` call in
`ExecuteTraceMetrics` with a lazy `r.CoalescedGroups` / `r.ReadGroup` loop, mirroring
the pattern already used by `Stream` and `CollectLogs`.

**Rationale:** The previous implementation materialised all selected block bytes into a
single `map[int][]byte` before processing any of them. For metrics queries over large time
ranges, `SelectedBlocks` can be hundreds or thousands of blocks — peak memory was
`N_blocks × avg_block_size`, which could reach GBs. With the `blockGroupPipeline`
pattern, the semaphore-gated dispatcher limits in-flight dispatch to at most
`defaultPipelineWorkers` (W=8) groups ahead of the consumer. Peak memory is bounded by
W groups (at most W-1 in the pending reorder map + 1 being processed), not all selected
blocks. See NOTE-058 for the full rationale on W=8.

Unlike the `Stream` path there is no early-exit for metrics (all blocks must be scanned for
correct aggregate results), so the lazy pattern here is purely a memory reduction, not a
latency optimization. Correctness is unchanged: metrics accumulation is order-independent.

**Why raw bytes are deleted from groupRaw before ParseBlockFromBytes:** The local `raw`
variable holds the slice header and keeps the underlying bytes alive through the parse and
row-iteration loop. Deleting the entry from `groupRaw` removes the map's reference so that
the bytes become eligible for GC as soon as the enclosing block-index iteration exits —
without waiting for the full group loop to complete.

Back-ref: `internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`
SPEC-ETM-12

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildGroupKeyMap`

---

## NOTE-058: blockGroupPipeline — Unified Bounded Sliding-Window I/O

_Added: 2026-04-15_

**Decision:** Replace the five divergent block-read loops (`scanBlocks`, `forEachBlockInGroups`,
`ExecuteTraceMetrics` sequential loop, `ExecuteLogMetrics` eager `ReadBlocks` call,
`topKScanBlocks`) with a single `blockGroupPipeline(ctx, r, groups, workerCount, processGroup)`
function. (`topKScanBlocks` was migrated to `blockGroupPipeline` in a follow-up change; see
SPEC-STREAM-11 update.)

**Rationale:**

1. `scanBlocks` was lazy-sequential (one group at a time); `forEachBlockInGroups` was
   eager-parallel (all groups upfront); `ExecuteLogMetrics` used `ReadBlocks` (all upfront
   without group awareness). Three incompatible patterns for the same underlying operation.
2. The bounded channel (capacity=W) makes peak memory O(W × group_size ≈ W × 8MB)
   regardless of total selected blocks. `ExecuteLogMetrics` previously held all selected
   blocks in memory simultaneously.
3. Concurrent I/O at W=defaultPipelineWorkers (8). Sequential parse is required
   (Reader.ParseBlockFromBytes is not goroutine-safe — NOTE-048).
4. An internal ordered reorder buffer ensures `processGroup` is called in group-index
   order, preserving the block-then-row match ordering invariant (SPEC §4.2).
5. `errLimitReached` as the early-exit sentinel is preserved from the previous design.
   The pipeline translates it to a nil return, not an error.
6. `ExecuteStructural` is exempt: NOTE-034 requires all blocks pre-fetched for the
   three-phase parent-resolve algorithm. `queryplanner.FetchBlocks` / `ReadBlocks`
   are retained for that path.
7. `ParseBlockFromBytes` (no intern pool) is kept for `forEachBlockInGroups` callers
   to minimize behavioral change. `scanBlocks`-derived callers continue to use
   `ParseBlockFromBytesWithIntern` for allocation savings (NOTE-006).

**Window size W — I/O-bound latency hiding:**
This is an I/O-bound pipeline, not a CPU-bound one. The goroutines issuing `ReadGroup`
calls are blocked in `ReadAt` (network syscall); they consume no CPU. W=8 was chosen
for these reasons:

- **Wrong frame**: `runtime.NumCPU()` is for CPU-bound work. NumCPU=4 under-parallelises
  on small VMs; NumCPU=64 would hold 512MB in-flight on large nodes.
- **Latency-hiding math**: T_io≈75ms (median S3), T_parse<5ms → optimal saturation
  W≈15. W=8 absorbs S3 tail latency (225-375ms) across 8 concurrent requests.
- **Memory ceiling**: W=8 → up to W=8 groups × 8MB = 64MB peak in-memory. The
  semaphore-gated dispatcher limits dispatch to W groups ahead of nextExpected; the
  pending reorder map holds at most W-1 out-of-order groups, plus 1 currently being
  processed. Fixed and predictable. Does not consult GOMEMLIMIT (objectcache already
  owns that budget via NOTE-OC-004).
- **Empirical**: W=8 matches prior `NumCPU` default on typical pod sizes (8-16 vCPUs)
  without coupling correctness to host topology.

`blockGroupPipeline` takes `workerCount int` as a parameter for testability (tests use
W=1 or W=2). All five production block-scan paths (`scanBlocks`, `forEachBlockInGroups`,
`ExecuteTraceMetrics`, `ExecuteLogMetrics`, `topKScanBlocks`) pass
`defaultPipelineWorkers = 8` to `blockGroupPipeline` (SPEC-STREAM-11).

Back-ref: `internal/modules/executor/block_group_pipeline.go:blockGroupPipeline`
Back-ref: `internal/modules/executor/block_group_pipeline.go:defaultPipelineWorkers`
Back-ref: `internal/modules/executor/stream_topk.go:topKScanBlocks`

Note (CRIT-BGP-1): The semaphore token is released only AFTER `processGroup` returns,
not before. This ensures the dispatcher cannot enqueue a new group while the current
group's raw bytes are still held in memory by `processGroup`, keeping peak in-memory
groups strictly at ≤ W (not W+1).

**Addendum — sequential log-TopK paths not covered by this pipeline:**
`logTopKScan` and `logCollectAll` route through `iterateLogRows`
(`stream_log_topk.go`), which uses a sequential per-group `r.ReadGroup` call.
They are NOT covered by this bounded pipeline and hold at most one coalesced
group in memory at a time. These paths are also exempt from SPEC-STREAM-11 along
with `ExecuteStructural`.

---

## NOTE-066: preFn gate in forEachBlockInGroups

_Added: 2026-04-15_

**Decision:** A nullable `preFn func(pb parsedBlock, candidates []int) bool` parameter was
added to `forEachBlockInGroups`. When non-nil, it is called after the first-pass column
decode; returning false skips both the second-pass decode and `fn` invocation for that block.

**Rationale:** Previously the second-pass decode always ran for every block that reached
`forEachBlockInGroups`, even if `ColumnPredicate` on the first-pass columns would have
returned an empty row set. For mixed-predicate paths (collectMixedPlain, collectMixedTopK)
the predicate evaluation is now moved into preFn so that second-pass column decoding (which
includes output-only columns such as `searchMetaColumns`) is skipped entirely for non-matching
blocks. Intrinsic-only paths pass nil because they have no first-pass column predicate to
evaluate.

**How to apply:** Any future caller that evaluates a column predicate before emitting rows
should pass a preFn rather than deferring the evaluation to fn. preFn and fn share a
captured rowSet variable; processGroup is sequential (SPEC-STREAM-11) so no synchronisation
is needed.

Back-ref: `internal/modules/executor/stream.go:forEachBlockInGroups`

---

## NOTE-067: metricsColumnsAreIntrinsic — Zero-I/O Fast-Path Existence Check (2026-04-16)

_Added: 2026-04-16_

**Problem:** `metricsColumnsAreIntrinsic` called `r.IntrinsicColumnMeta(col)` for each
wanted column. For V14 files, `IntrinsicColumnMeta` triggers a lazy blob read when
`meta.Format == 0` (i.e. on the first call per column). Combined with NOTE-016 removing
the eager pre-reads from `parseSectionsLazyV14`, this meant every metrics query that
hit the fast-path check would issue one GCS read per needed column per file — just to
verify the column exists, before deciding whether to use the intrinsic fast path.

**Decision:** Replace `r.IntrinsicColumnMeta(col)` with `r.HasIntrinsicColumn(col)` in
`metricsColumnsAreIntrinsic`. `HasIntrinsicColumn` is a pure map lookup added to Reader
(NOTE-016) that never issues any I/O. The existence check is all `metricsColumnsAreIntrinsic`
needs — `Format/Type/Count` are not consulted here.

**Rationale:** The cold-cache cost for a metrics query across 107 Tempo blocks was:

- Before: 107 × N_columns GCS reads in `parseSectionsLazyV14` at open, plus N×107 in
  `metricsColumnsAreIntrinsic` if eager reads were removed.
- After: 0 GCS reads at open, 0 GCS reads in the existence check. Only the actual column
  data needed by `executeTraceMetricsIntrinsic` (e.g. `span:start`, group-by column) is
  fetched, lazily and only once per column per file.

**How to apply:** Any future fast-path existence check that does not need Format/Type/Count
should use `HasIntrinsicColumn` rather than `IntrinsicColumnMeta`.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:metricsColumnsAreIntrinsic`

---

## NOTE-068: count/rate no-group-by fast path — flat []int64 instead of map[string]\*aggBucketState (2026-04-16)

_Added: 2026-04-16_

The original hot loop for `count/rate` with no group-by allocated a new string per span
(`strconv.FormatInt(idx, 10) + "\x00"`) and did a `map[string]*aggBucketState` lookup on every
iteration. With ~1.4M spans/file × 107 files, M1 (`{} | rate()`) executed ~150M string
allocations and hash lookups per query.

Pyroscope CPU profile (2026-04-16, 2-hour window) showed:

- `maps.ctrlGroupMatchH2`: 23s self (14%) — Swiss-table map probing from these lookups
- `runtime.gcBgMarkWorker` + `gcDrain`: ~46s (28%) — GC pressure from string allocations

**Fix:** Pre-allocate `counts []int64` of size `numSteps` (e.g., 360 for 6h/60s step).
In the hot loop, write `counts[idx]++` — zero allocations, zero hash lookups.
After the loop, populate `buckets` from non-zero `counts` entries (at most `numSteps` iterations).
The key format (`strconv.FormatInt(idx, 10) + "\x00"`) is identical to before, so
`traceBuildDenseSeries` and `collectGroupKeys` require no changes.

**Why not apply to group-by paths:** The composite key includes the group-by value, so a
simple slice index cannot replace the map without a separate dimension per group-by value.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:executeTraceMetricsIntrinsic` lines 133–165

---

## NOTE-069: count/rate group-by fast path — map[string][]int64 instead of per-span composite string (2026-04-16)

_Added: 2026-04-16_

The group-by count/rate loop in `accumulateIntrinsicBuckets` built a composite string key
(`strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKeyMap[pk]`) for every span in
`keyToBucket`, allocating a new string each iteration. For M5 (`{} | rate() by (kind)`)
with ~150M spans, this was ~150M string allocations and hash lookups — the same pattern
fixed in NOTE-068 for the no-group-by path.

**Fix:** `streamCountRateGroupBy` uses `map[string][]int64` keyed by the group value
(e.g., "server", "client") with a `[]int64` count slice of size `numSteps` as the value.
Per-span work: one existing-string map lookup (`groupKeyMap[pk]`) + one array write.
After the loop, populate `buckets` from non-zero (group, step) pairs only — at most
`numGroups × numSteps` entries (e.g., 6 kinds × 360 steps = 2,160 for M5, vs 150M before).

---

## NOTE-070: Metrics Filter ColumnPredicate Bug and Fix

_Added: 2026-04-16_

**Issue:** `CompileTraceQLMetrics` set `program.Predicates` (block-level pruning)
but never compiled a real `ColumnPredicate` from the filter expression. The program
always used `FullScan()` for the `ColumnPredicate`, so the block-scan path in
`ExecuteTraceMetrics` accumulated all rows regardless of the filter.

**Fix:** When a filter is present, instantiate a `traceqlCompiler` and call
`compileColumnPredicate(filter.Expr)`, exactly as `CompileTraceQLFilter` does. When no
filter is present (`{}` queries), keep `compileMatchAllProgram()`.

**Why not post-filter in `ExecuteTraceMetrics`?** The `filterRowSetByIntrinsicNodes`
post-filter used in the search path only handles intrinsic columns. Non-intrinsic range
predicates like `span.http.response.status_code >= 400` require column scan, which is
already implemented in `compileColumnPredicate`. Re-implementing that at the executor
level would duplicate the VM machinery.

**Invariant:** `program.ColumnPredicate` must be a real predicate when a filter
expression is present. The only valid `FullScan()` column predicate is for match-all
(`{}`) queries.

---

## NOTE-071: compositeKey Scratch-Buffer Pool — Zero-Allocation Map Lookup (2026-04-16)

**Problem:** `traceAccumulateRow`, `traceBuildDenseSeries`, and the histogram series builder each
constructed a composite map key using `strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey`,
allocating one heap string per span per metrics query. Pyroscope object-count profiling showed
2.2M `strconv.FormatInt` heap allocations per query, making GC the dominant CPU consumer
(74% on querier).

**Fix:** Added `compositeKeyScratchPool` (`sync.Pool` of `*[]byte`, 128-byte initial capacity).
Key is built with `strconv.AppendInt` / `append` / `strconv.AppendFloat` into the pooled buffer.
`string([]byte)` used as a map-index expression does not allocate — the Go compiler recognizes
this pattern and avoids copying when the string doesn't escape the index expression. A real
`string` allocation is made only inside the `!exists` branch where the key must be stored as a
map key.

**Why `*[]byte` not `[]byte`:** `sync.Pool` stores `any`; storing `*[]byte` avoids the interface
boxing allocation on `Put` and lets `AppendInt`/`AppendFloat` grow the slice without losing the
new backing array.

Back-ref: `internal/modules/executor/metrics_trace.go:traceAccumulateRow`,
`traceBuildDenseSeries`, `traceHistogramSeries`

---

## NOTE-072: mergeJoinFilteredRefsWithVals — Merge-Join Replaces filteredKeys Map (2026-04-16)

_Added: 2026-04-16_

**Problem:** `executeTraceMetricsIntrinsic` (and `streamCountRateNoGroupBy`) used a
`filteredKeys map[uint32]struct{}` to record which packed keys passed the predicate filter.
With ~1.4M spans/file × 107 files, this allocated one `map[uint32]struct{}` per file plus
one map-insert and one map-lookup per span. Pyroscope CPU profiling (2026-04-16) attributed
17.49% CPU to map operations and 10.66% to GC pressure from these allocations.

**Fix:** `mergeJoinFilteredRefsWithVals` sorts both ref slices by packKey and walks them
with two pointers in O(M log M + N log N + N + M). The sort is paid once per file call;
per-span work is zero allocations and zero hash lookups.

**Critical sort invariant:** `inRangeRefs` is a sub-slice of `tsCol.BlockRefs`, which is
**timestamp-sorted** (`types.go:228`, NOTE-042) — NOT packKey-sorted. A binary search
narrows `tsCol.BlockRefs` to the in-range window; that sub-slice preserves timestamp order.
Using it directly as one side of a packKey merge-join is incorrect and would produce missing
or spurious results.

**Implementation:**

- `filteredRefs` is cloned via `slices.Clone` before sorting. `BlockRefsFromIntrinsicTOC`
  may return a sub-slice of a cached structure; sorting in-place would silently corrupt it.
- `inRangeRefs` is NOT sorted in-place. A `[]refIdx` sorted index is built over the
  original positions so that `inRangeVals` alignment is preserved (both slices are
  parallel; reordering `inRangeRefs` without reordering `inRangeVals` would misalign values).
- The two-pointer walk advances over the `refIdx` index and the sorted `filteredRefs` clone.
  Matched entries use `ri.pos` to read the original `inRangeRefs[ri.pos]` and `inRangeVals[ri.pos]`.

**Output slices:** `outRefs` and `outVals` are allocated once per file (size ≤ min(N,M))
and passed through to `accumulateIntrinsicBuckets` or iterated directly in
`streamCountRateNoGroupBy`. They are O(1) per file, not O(N spans).

**Call sites:**

- `executeTraceMetricsIntrinsic` (`metrics_trace_intrinsic.go`): calls
  `mergeJoinFilteredRefsWithVals` after binary-search narrows `inRangeRefs`/`inRangeVals`,
  BEFORE dispatching to `streamCountRateNoGroupBy` (count/rate, no group-by) or the
  `keyToBucket` group-by / aggregate-field path. Both downstream paths receive the
  already-filtered `inRangeRefs`/`inRangeVals` directly.
- `streamCountRateNoGroupBy`: `filteredKeys map[uint32]struct{}` parameter removed and the
  `filteredKeys != nil` check deleted from the loop body. The function now receives
  pre-filtered `inRangeRefs`/`inRangeVals` from `executeTraceMetricsIntrinsic`.
- `keyToBucket` loop (group-by / agg-field path in `executeTraceMetricsIntrinsic`):
  `filteredKeys != nil` check removed; iterates pre-filtered `inRangeRefs` directly.

**Benchmark result:** BENCH-EX-08 post-fix baseline: 76 allocs/op (~15452 B/op). All
remaining allocations are O(1) per file (filteredRefs clone, refIdx index slice,
outRefs/outVals output slices, buckets map, reader internals).

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateNoGroupBy`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:executeTraceMetricsIntrinsic`

---

## NOTE-073: strings.Join eliminated in traceAccumulateRow compositeKey path

_Added: 2026-04-16_

**Decision:** Extend the pool pattern from NOTE-071 (compositeKey pool from #228) by also
eliminating the intermediate `strings.Join(attrVals, "\x00")` in `traceAccumulateRow`.
attrVals are appended directly into the pooled scratch buffer with `\x00` separators.

**Why:** After NOTE-071, the compositeKey buffer was pooled but `strings.Join` still
allocated one heap string per span for the intermediate attrGroupKey. For M8 with 150M
spans/query this was still ~150M allocations driving residual ~85% GC CPU on filtered
histogram queries.

**Byte format invariant:** The byte sequence is identical to NOTE-071:

- `'\x00'` separator after bucketIdx UNCONDITIONALLY (matches `FormatInt + "\x00" + ""`
  for empty groupBy)
- `'\x00'` between attrVals only when `i > 0` (matches `strings.Join` semantics)
- HISTOGRAM appends `'\x00' + AppendFloat` as before
  Gate-tested by EX-CK-01 through EX-CK-07.

**Benchmark result:** BENCH-EX-05 post-fix: ~325 allocs/op (down from pre-this-PR ~523).
BENCH-EX-09 (HISTOGRAM variant) similar.

**Back-ref:** `internal/modules/executor/metrics_trace.go:traceAccumulateRow`

---

## NOTE-076: Q7 Bugfix — rowSatisfiesIntrinsicNodesOR Pass-Through for All-Non-Intrinsic OR

_Added: 2026-04-17_

**Bug:** `rowSatisfiesIntrinsicNodesOR` returned `false` when all OR children referenced
non-intrinsic columns, incorrectly eliminating rows that the VM's `ColumnPredicate` had
already validated. This caused queries of the shape `{intrinsicCol=X && (spanAttr=A || spanAttr=B)}`
to return 0 results even when matching spans existed.

**Fix:** Track `hadConstrainedChild` in the OR loop. When no constrained child (intrinsic leaf
or composite) was found, return `true` (pass-through) instead of `false`. Non-intrinsic leaf
predicates in the OR are the VM ColumnPredicate's responsibility; this function should only
veto rows that fail an intrinsic constraint. Composite children (AND/OR groups) are always
marked as constrained so that nested intrinsic failures are not bypassed.

**Not affected:** OR nodes with at least one intrinsic leaf (NOTE-051 case) are unchanged
— `hadConstrainedChild=true` and the function returns `false` when all intrinsic children fail.

**Triggering condition:** Both full block-scan path (`scanBlocks`, `Limit==0`) and the intrinsic
fast path (`Limit>0`) call `filterRowSetByIntrinsicNodes` and were both affected by the bug.

**Back-ref:** `internal/modules/executor/predicates.go:rowSatisfiesIntrinsicNodesOR`
**Test:** `TestANDMultiValueOR_IntrinsicAND_SpanAttrOR` (EX-INT-14 through EX-INT-17, EX-INT-21)

## NOTE-074: Dict-ID-Keyed Group Map — Intrinsic Fast Path (2026-04-17)

**Decision:** Replace `map[uint32]string groupKeyMap` + `map[string][]int64 groupCounts`
in the intrinsic fast path with `map[uint32]groupIDKey` + `map[groupIDKey][]int64`, where
`groupIDKey = [8]uint32`.

**Why:** r81 Pyroscope showed `ctrlGroupMatchH2` at 24.67% (string hash cost in
`groupCounts[gk]` lookups) and `buildGroupKeyMap` at 19.24% (string value allocations
in the group key map). For M8 warm (150M spans, primarily intrinsic), this is ~1.68s and
~1.31s respectively. Switching to uint32 array keys reduces hash cost by ~50-60% and
eliminates the O(blocks × unique_groups) string allocations.

**Key decisions:**

1. `[8]uint32` cap (32 bytes, one cache line). Queries with >8 group-by dims fall back to
   string-keyed path — pathological in production (zero real queries use >4).
2. dict index 0 = absent/empty sentinel in every dimension. Column dicts are prefixed with
   "" so absent pks naturally map to index 0 without special-casing.
3. String resolution (ID → label string) is deferred to series-emit time, which is
   O(unique groups), not O(spans). The final series label values are byte-identical to
   the string-keyed path output.
4. `histGroupIDKey struct { dims [8]uint32; boundary float64; bucketIdx int64 }` is the
   histogram key — avoids both string hash for group dims AND `strconv.FormatFloat` per
   span for the boundary value. Boundary is always a power-of-2 or 0 (never NaN), making
   float64 map key safe.
5. Flat intrinsic columns in group-by: build on-the-fly string→uint32 dict (sequential IDs
   from first-seen string values). This path is cold in production (nobody groups by
   `span:start`).
6. Block-scan path (`traceAccumulateRow`) is unchanged — it is not the M8 bottleneck.

**PR:** Layer 3 dict-ID group map
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildGroupIDMap`,
`internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateGroupByID`,
`internal/modules/executor/metrics_trace_intrinsic.go:streamHistogramGroupByID`,
`internal/modules/executor/metrics_trace_intrinsic.go:streamAggGroupByID`

## NOTE-077: Per-Type Dict-Mask Fast Path for StreamScanEqualAny

_Added: 2026-04-17_

**Decision:** Extend the `StreamScanEqualAny` dict-mask fast path to dict-encoded column
types Int64, Uint64, Float64, and the String+int64 coercion shape. Bool is excluded (see below).

**Approach (Style A):** Per-type concrete helper functions dispatched via a type-switch in
`dispatchDictFastPath`. Each helper calls `col.EnsureDecoded()`, pre-builds a `[]bool dictMatch`
over the dict entries in O(D×nValues), then scans rows via `scanDictMaskRows` in O(N).
No func-pointer dispatch in hot loops. Matches existing `scanStringDictFloat` pattern (NOTE-026).

**Coercion invariants (byte-identical to rowEqual/rowCompare):**

- String+float64: rowEqual veto (line 84-89) → fast path returns 0 immediately.
- String+int64: uses `strconv.ParseInt(s, 10, 64)` identical to rowCompareString line 162-168.
- Int64+float64: `float64(dictEntry)` promotion, identical to rowCompare line 109.
- Uint64+int64: `uint64(queryInt64)` cast (//nolint:gosec), identical to rowCompare line 117.
- Float64+int64: `float64(queryInt64)` promotion, identical to rowCompare line 142.
- Mixed value kinds: `classifyValueKind` returns kindMixed → falls through to generic; no
  silent-zero (the PR #234 bug class is avoided by detecting uniformity before dispatching).

**Bool exclusion:** Bool columns are excluded from the fast path. D=2 max dict (false/true)
makes EnsureDecoded + make([]bool,2) + mask-build overhead exceed savings vs the direct
`rowCompareBool` path in the generic loop. Measured: 23µs (fast) vs 21µs (generic) — 1.08x
SLOWER. Bool always falls through to generic. `kindAllBool` dispatches to no fast path.

**EnsureDecoded requirement:** All helpers call `col.EnsureDecoded()` before accessing
XxxDict/XxxIdx directly. Direct slice access bypasses per-row lazy decode; EnsureDecoded
calls decodeNow() + expandDenseIdx() (same reasoning as scanStringDictFloat, NOTE-026).

**Linear scan over map for numeric types:** For Int64/Uint64/Float64, linear scan over
targets (1 ns primitive compare) beats map hash+probe (~30 ns) for nValues ≤ 30, which
covers all practical query shapes. Map is kept for String+string (existing path).

**Back-ref:** `internal/modules/executor/column_provider.go:dispatchDictFastPath`,
`internal/modules/executor/column_provider.go:scanStringDictEqualAny`,
`internal/modules/executor/column_provider.go:scanInt64DictEqualAny`,
`internal/modules/executor/column_provider.go:scanUint64DictEqualAny`,
`internal/modules/executor/column_provider.go:scanFloat64DictEqualAny`

---

## NOTE-075: StreamScanEqualAny — Mixed-Type Coercion Guard (2026-04-17)

_Added: 2026-04-17_

**Bug:** The original `StreamScanEqualAny` dict fast path for string columns built a
`map[string]struct{} wantSet` by iterating all values and filtering to those that type-asserted
to `string`. Non-string values (e.g. `int64`, `float64`) were silently dropped from `wantSet`.
When the query contained any non-string value — such as `span.http.response.status_code` in `{200, 201}`
where the column is string-encoded but the query values are `int64` — the fast path returned 0 matches
without error, while the generic `rowEqual` path (correctly) found matches via `rowCompareString →
strconv.ParseInt`. Queries that used a mix of int64/float64 values against string-typed columns
always silently returned zero results.

**Fix:** Classify the value slice with `allStrings bool` before entering the fast path. If any value
is not a `string`, set `allStrings = false` and skip the dict fast path entirely, falling through to
the generic `scanWith` + `rowEqual` loop. This makes the path selection conservative: the fast path
only fires when every value is a string, guaranteeing the set-membership comparison is type-safe.
The single-value `ScanEqual` path already handled int→string coercion correctly via `rowCompare`;
the multi-value path now does too.

**Why not coerce in the fast path:** Coercing int64 values into the string `wantSet` via ParseInt
would require encoding the parsed integer back as a string for set membership. That is fragile
(leading zeros, base variants, overflow). The correct path for coerced equality is per-dict-entry
comparison via `strconv.ParseInt` — which is what NOTE-077 later implements as a first-class fast
path for the String+int64 shape. NOTE-075 was the conservative guard; NOTE-077 is the full solution.

**Back-ref:** `internal/modules/executor/column_provider.go:StreamScanEqualAny` (allStrings guard)
**Commit:** 029d041 (coercion fix, this branch)

## NOTE-078: StreamScanNotEqual — Absent User-Attribute Column Returns 0, Not FullScan

_Added: 2026-04-17_

**Decision:** When `StreamScanNotEqual` is called for a user-attribute column that is entirely
absent from the block (`lookupColumn` returns nil), return `0, nil` immediately (no matches).
Do not loop over all rows. For intrinsic columns, delegate to `nilIntrinsicScan` first
(SPEC-STREAM-10.1), which may return FullScan for old-format blocks.

**Rationale (bug fix):** The original implementation had no early-exit for `col == nil`. It
looped over all N rows, and since `col == nil` made `present` always false, every row was
included in the result — equivalent to FullScan. This produced false-positive matches for
queries like `{span.db.system != "" && span.rpc.service != ""}`: both predicates returned
FullScan for absent columns, and their AND intersection was also FullScan, matching every
span regardless of whether it had those attributes.

**Correct semantics:** An absent user-attribute column means the attribute is NULL for every
row in that block. NULL is not equal to any value, but NULL is also not "not equal to" any
value — SQL NULL semantics. `attr != "foo"` is false when `attr` is NULL (absent). Therefore,
absent column → 0 matches for any `!=` predicate.

**Per-row null handling (extended fix):** The fix was extended to also exclude per-row absent
rows. When a column IS present (`col != nil`) but a specific row has no value
(`!col.IsPresent(i)`), that row is now also excluded from `!=` results. This is consistent
with the SQL NULL semantics above: a span without the attribute is NULL for that attribute,
and NULL != X evaluates to NULL/false, not true. Parquet applies the same semantics.

**Pattern:** This fix makes `StreamScanNotEqual` consistent with all other comparison
`StreamScan*` functions (Equal, EqualAny, LessThan, LessThanOrEqual, GreaterThan,
GreaterThanOrEqual) for the `col == nil` case. Note that `StreamScanIsNull` and
`StreamScanNotNull` intentionally handle `col == nil` differently (they test for presence,
not value) and are unaffected.

**Back-ref:** `internal/modules/executor/column_provider.go:StreamScanNotEqual`

---

## NOTE-079: resolveStructuralParentIndices — `map[[8]byte]int` key to eliminate string allocs

_Added: 2026-04-17_

**Decision:** Changed `byID` from `map[string]int` to `map[[8]byte]int` in
`resolveStructuralParentIndices`. Changed `seen map[int]struct{}` in `evalStructuralMatches`
to `slices.Sort` + dedup-in-range.

**Supersedes NOTE-034 Phase 2 rationale:** NOTE-034 claimed that `string([]byte)` map key
"avoids allocating a persistent string" via Go compiler optimisation. This is true for MAP
LOOKUPS but NOT for MAP INSERTS. The insert `byID[string(sp.spanID)] = i` always allocates
a heap string. Profiler (Pyroscope, 2026-04-17) confirmed 1.80M alloc_objects (18.8% of
total) attributable to this function.

**Why `[8]byte` map key works:**

- OTel spec mandates span IDs are exactly 8 bytes (trace.pb.go proto comment).
- Blockpack writer enforces this at write time: `writer.go:1002` rejects any span with
  non-8-byte spanID. Legacy files that predate this check are guarded by `len == 8` guards
  in the new code — non-8-byte IDs are treated as "no parent found" (parentIdx = -1),
  which is the same fallback as before.
- `[8]byte` is a value type: the map key is stack-allocated and copied into the bucket
  array. No heap allocation for key storage on insert OR lookup.
- This pattern is already established in this file: `map[[16]byte][]structuralSpanRec` at
  `collectAllStructuralSpans` line 127, and `[8]byte` span ID variables in
  `stream_structural_test.go:34-37`.

**Why not pool the map:** NOTE-034 deliberately avoided pooling to prevent clearing
complexity. The map is now cheaper (value-key), making the make() per trace the only
remaining alloc — acceptable given it is one allocation per trace (not per span).

**Why sort+dedup for seen-map:** `rightIndices` is a freshly allocated local `[]int` from
`applyStructuralOp`. Sorting it in-place (using already-imported `slices` package) costs
O(N log N) where N is the number of right-matches per trace — typically small (< 20).
This eliminates one `make(map[int]struct{})` per trace with any structural matches, at the
cost of reordering right-match output. Output order is not contractually specified (no
ORDER BY on structural queries).

**Measured improvement (BENCH-EX-13):** `resolveStructuralParentIndices` itself: ~12200
→ ~300 allocs/op (100 traces × 60 spans). The 300 remaining allocs are one map make per
trace (unavoidable) plus runtime overhead. String-key allocs are eliminated.

**Back-ref:** `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices`,
`internal/modules/executor/stream_structural.go:evalStructuralMatches`

---

## NOTE-080: N-Node Structural Query Chains — Bitmask and Chain Evaluator

_Added: 2026-04-21_

**Decision:** Extended the structural query engine to support N-node chains (e.g., `A >> B >> C`).
Changes are backward-compatible: 2-node queries route through the unchanged per-operator
eval functions; N>2 chains route through `evalOpChain`.

**Design:**

- `StructuralQuery.Right` changed from `*FilterExpression` to `Expr`, allowing `*StructuralQuery`
  as the right operand, enabling right-associative parsing of N-node chains.
- `traceqlparser.FlattenChain(q)` recursively walks the right chain and returns
  `[]*FilterExpression` (length N) and `[]StructuralOp` (length N-1).
- `structuralSpanRec.nodeMatch uint8` replaces the old 2-field approach.
  Bit i is set if the span matches program[i]. Supports up to 8 nodes.
- `compileStructuralPrograms([]*FilterExpression)` compiles one `*vm.Program` per chain node.
- `collectBlockStructuralSpanRecs` evaluates all N programs per block and tags each span
  with the resulting `nodeMatch` bitmask via `computeNodeMatchForRow`.
- `evalOpChain` + `evalOpChainStep` implement pairwise chain evaluation using an
  intermediate `map[int]struct{}` match set. Each step: given the matched set for node i,
  compute the matched set for node i+1 using `ops[i]`.
- 2-node case: `nodeMatch & 0x01` = left (node 0), `nodeMatch & 0x02` = right (node 1) —
  all existing per-op functions remain unchanged.

**Rationale:** Parquet handles `A >> B >> C` natively; blockpack must match that behavior.
The bitmask avoids heap allocations for N<=8 nodes (common case). The intermediate set
approach for N>2 avoids a mega-evaluator and keeps each operator's semantics isolated.

**Limitation:** N>8 nodes are rejected at runtime with an error (enforced in `ExecuteStructural`);
no practical query uses 8+ levels. Negation operators in multi-node chains are also rejected
at runtime rather than silently returning empty results.

Back-ref: `internal/modules/executor/stream_structural.go:evalOpChain`,
`internal/modules/executor/stream_structural.go:evalOpChainStep`,
`internal/traceqlparser/parser.go:FlattenChain`

---

## NOTE-081: Typed struct replaces map[string]any in structural hot path (2026-04-21)

**Problem:** `collectBlockStructuralSpanRecs` called `lookupIntrinsicFields` which allocated one
`map[string]any` per span row. Pyroscope showed ~25GB allocated in production (20.97GB from the
closure filling maps, 4.03GB from `make(map[string]any)`).

**Decision:** Replace `[]map[string]any` row representation in the structural path with a single
typed struct slice `[]intrinsicRowFields`. One `make([]intrinsicRowFields, N)` allocation covers
all N rows instead of N `make(map[string]any)` allocations.

**Design:**

- `intrinsicRowFields` struct stores all 11 intrinsic columns with typed fields
- `present` bitmask (uint16) tracks which fields are populated (replaces map key absence)
- `lookupIntrinsicFieldsTyped` returns `[]intrinsicRowFields` (one allocation for N rows)
- `identityFieldsFromBlockColsTyped` handles the legacy path (no intrinsic section)
- `rowSatisfiesIntrinsicNodesTyped` / `rowSatisfiesIntrinsicNodesORTyped` evaluate predicates against
  the typed struct using type-group getter helpers (`intrinsicLeafGetBytesTyped` etc.) to keep
  cyclomatic complexity within the 30-limit
- The old map-based `lookupIntrinsicFields` and `rowSatisfiesIntrinsicNodes` are retained unchanged
  for the non-structural path (`filterRowSetByIntrinsicNodes`); only the structural path switches
- `computeNodeMatchForRow` updated to accept `*intrinsicRowFields` and call
  `rowSatisfiesIntrinsicNodesTyped` (NOTE-080 introduced this function; NOTE-081 typed it)

**Absent-field behavior:** When a present bit is clear, the typed predicate evaluator returns
`len(n.Values)==0 && n.Min==nil && n.Max==nil && n.Pattern==""` — the same "is null" logic as the
old map's `!ok` case. For bytes columns specifically, the getter returns `(nil, true)` and
`matchIntrinsicBytesField` handles nil as absent.

**Back-ref:** `internal/modules/executor/intrinsic_row.go:lookupIntrinsicFieldsTyped`
**Back-ref:** `internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`
**Back-ref:** `internal/modules/executor/stream_structural.go:computeNodeMatchForRow`
**Back-ref:** `internal/modules/executor/predicates.go:rowSatisfiesIntrinsicNodesTyped`

---

## NOTE-082: N=1 group-by fast path — uint32 map key instead of [8]uint32 (2026-04-22)

**Decision:** When `len(dicts)==1` (single group-by dimension), `streamCountRateGroupByID`,
`streamAggGroupByID`, and `streamHistogramGroupByID` dispatch to helper functions
(`streamCountRateGroupByIDSingle`, `streamAggGroupByIDSingle`, `streamHistogramGroupByIDSingle`) that use
`uint32`/compact struct keys instead of `groupIDKey = [8]uint32` / `aggGroupIDKey` / `histGroupIDKey`.

**Rationale:** For N=1, only `gid[0]` is ever non-zero. Using the full `[8]uint32` key wastes
28 bytes of memcmp per map probe. Go's `mapaccess2_fast32` (for uint32 keys) is ~3x faster than
the generic path taken for 32-byte array keys. The primary production queries (span:kind,
span:status, resource.service.name) are all N=1. N>1 retains the existing [8]uint32 path.

**Key type reductions:**

- `streamCountRateGroupByIDSingle`: `uint32` (4 bytes) vs `groupIDKey = [8]uint32` (32 bytes)
- `streamAggGroupByIDSingle`: `aggSingleGroupIDKey{bucketIdx int64, dictIdx uint32}` (16 bytes) vs `aggGroupIDKey` (40 bytes)
- `streamHistogramGroupByIDSingle`: `histSingleGroupIDKey{dictIdx uint32, boundary float64, bucketIdx int64}` (24 bytes) vs `histGroupIDKey` (48 bytes)
- nil-column path: `histSingleAbsentKey{bucketIdx int64, dictIdx uint32}` (16 bytes) vs `histGroupIDKey` (48 bytes)

**Output contract:** For N=1, the group key is `dict[dictIdx]` with a bounds-checked empty
string fallback (if `int(dictIdx) >= len(dict)`, the key resolves to `""`), identical to what
`resolveGroupIDKey({dictIdx,0,...}, [][]string{dict})` would produce. The `*Single` helpers
resolve directly via `dict[dictIdx]`; `resolveGroupIDKey` is not called on the N=1 path.

**Scope:** Only the three stream functions in `metrics_trace_intrinsic.go`. `buildGroupIDMap`
and `resolveGroupIDKey` are unchanged.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateGroupByID`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateGroupByIDSingle`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamAggGroupByID`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamAggGroupByIDSingle`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamHistogramGroupByID`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamHistogramGroupByIDSingle`

## NOTE-083: span:kind and span:status enum resolution at metrics emit time (2026-04-21)

**Decision:** `scanIntrinsicColDictIDs` and `scanIntrinsicColVals` now accept a `colName string`
parameter. When a dict entry has an empty `.Value` (regardless of `.Int64Val`), they call
`intrinsicInt64ColToString(colName, v)` to convert the stored int64 to a human-readable OTel
name instead of falling back to `strconv.FormatInt(v, 10)`.

**Rationale:** `span:kind` and `span:status` are stored as int64 enum values in blockpack.
Emitting "2" instead of "server" in metrics group-by output is incorrect from the user
perspective. The fix is applied at emit time, requiring zero block-format or writer changes.
Out-of-range values (e.g., kind=99 from a corrupt block) fall back to `strconv.FormatInt`,
preserving the existing behavior for unknown values.

**Mappings match** `spanKindToInt64` and `statusCodeToInt64` in `internal/vm/traceql_compiler.go`
(the canonical OTel enum tables used by the query compiler).

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:intrinsicInt64ColToString`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:scanIntrinsicColDictIDs`
**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:scanIntrinsicColVals`
**Back-ref:** `benchmark/trace_metrics_bench_test.go:traceMetricsQueries` (rate_by_span_kind, rate_by_span_status entries)

---

## NOTE-084: perf-metrics-array-groupby Regression Post-Mortem (2026-04-21)

**Decision:** The perf-metrics-array-groupby branch (r98) was reverted. It replaced the
`keyToBucket` range loop with a `col.DictEntries` outer loop, which caused 20–35% regression.

**Rationale:** Iterating `col.DictEntries` as the outer loop converted ~150M sequential
`keyToBucket` range-iterations into ~150M random `keyToBucket[pk]` lookups (lookups by
arbitrary pk values in dict-entry order). The cache-miss cost on 150M entries exceeded the
savings from eliminating the groupCounts map probe. The keyToBucket range loop must remain
the outer loop for any accumulator that touches the full span set.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateGroupByIDSingle`

---

## NOTE-085: Slice accumulator in streamCountRateGroupByIDSingle (2026-04-21)

**Decision:** Replace `groupCounts map[uint32][]int64` with `[][]int64` pre-allocated to
`len(dict)` in `streamCountRateGroupByIDSingle`. Index into it directly via `dictIdx`.

**Rationale:** `dictIdx` values from `groupIDMap` are dense integers in `[0, len(dict)-1]`
(guaranteed by `scanIntrinsicColDictIDs` which assigns dictIdx = len(dict) before append).
Direct slice indexing requires zero hash computation, zero collision probing, and zero
bounds-check divergence compared to map access. For M5 queries (150M in-range spans,
span:kind N≤6), this eliminates ~150M `mapaccess1_fast32` calls per file in the hot loop.

The `keyToBucket` range loop is kept as the outer loop (unchanged from NOTE-082) — this
avoids the random-access regression identified in NOTE-084.

Pre-allocation cost: `len(dict) × numSteps × 8 bytes`. For span:kind (N=6, numSteps=360):
~17 KB. For service.name (N=500, numSteps=360): ~1.4 MB. Acceptable for a single goroutine.

The absent-sentinel (dictIdx=0 → dict[0]="") is handled correctly: `groupCounts[0]` is
pre-allocated and accumulates absent spans, resolving to `""` at emit time.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateGroupByIDSingle`

---

## NOTE-086: 3D slice accumulator in streamByRefSliceHistogram (2026-04-21)

**Decision:** Replace `map[histSingleGroupIDKey]int64` with a pre-allocated 3D slice
`groupCounts[groupIdx][boundaryIdx][timeIdx]` in `streamByRefSliceHistogram`. Add a
pre-scan phase that builds `boundaryIdxForRef[]` by memoizing `intrinsicHistogramBoundary`
via `boundaryCache map[float64]int16`.

**Rationale:**
The old hot loop called `intrinsicHistogramBoundary` (which includes `math.Pow` + `math.Log2`)
for every span — up to 150M times per file. It also probed a 24-byte struct key into
`map[histSingleGroupIDKey]int64` per span.

The new approach:

- **Pre-scan phase:** Iterates inRangeRefs once, calling `intrinsicHistogramBoundary` at most
  `len(unique_boundaries)` times (≤30 for span:duration per block, memoized via
  `boundaryCache`). Builds `boundaryIdxForRef[i]` (int16, 1-based, 0=absent).
- **Hot loop:** Three array reads + one `int64` increment. Zero float math, zero hash maps.
  `groupCounts[gIdx][bIdx][bucketIdx]++` where all indices are pre-resolved.

3D array size for typical queries: 100 groups × 30 boundaries × 36 steps × 8 bytes = 864 KB.
Fits in L3 cache. Pre-allocation cost is negligible vs the 150M span iterations.

**Consequence:** `aggValsForRef`/`aggPresent` are now fetched by the caller
(`accumulateIntrinsicBuckets`) for HISTOGRAM along with all other non-count/rate agg
functions, eliminating the special-case `agg.Function != vm.FuncNameHISTOGRAM` branch.
`streamByRefSlice` no longer needs `r *modules_reader.Reader` and its signature was trimmed.

Two helpers extracted to keep cyclomatic complexity within limits:
`streamByRefSliceHistogramAlloc` (3D slice allocation) and `streamByRefSliceHistogramEmit`
(iterate groupCounts → composite string keys → buckets).

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamByRefSliceHistogram`

_Addendum (2026-04-22):_ NOTE-087 reversed the consequence — `streamByRefSlice` gained `r`, `dictByPK`,
and `maxPK` back so the histogram branch can scan the column directly without
`buildAggValsForRef`. See NOTE-087 below.

---

## NOTE-087: Direct aggregate column scan in streamByRefSliceHistogram (2026-04-22)

_Added: 2026-04-22_

**Decision:** Eliminate `buildAggValsForRef` from the HISTOGRAM branch of the N=1 group-by
path. Instead, `streamByRefSliceHistogram` scans the aggregate intrinsic column directly,
using dense array lookups (`bucketByPK`, `dictByPK`) for O(1) group and time-bucket resolution
per ref. A `histSpanEntry` intermediate slice defers 3D accumulator allocation until all unique
boundaries are known.

**Rationale:**
`buildAggValsForRef` (NOTE-086 caller) allocated:

- `valByPK [maxPK+1]float64` — ~7.7 MB for a 15-block file (maxPK ≈ 983 040)
- `hasByPK [maxPK+1]bool` — ~1.9 MB
- `aggValsForRef []float64`, `aggPresent []bool` — parallel to inRangeRefs (~150 M entries)

Then it read those arrays back into the pre-scan loop inside `streamByRefSliceHistogram`,
adding ~300 M extra array operations (write + read per in-range ref). These are pure overhead
that the r99 baseline avoided by scanning columns directly.

The new approach:

- **Step 1:** Build `bucketByPK[maxPK+1]int64` from `inRangeRefs`/`inRangeVals` — one write
  per in-range ref (same cost as the old per-ref loop in `buildAggValsForRef`).
- **Step 2:** Scan the aggregate column once. For dict format, `getBoundaryIdx` is called once
  per dict entry (≤30 for span:duration), not once per span. For flat format, it is called per
  ref but uses the memoized cache (~30 entries, L1-resident). `seenByPK[maxPK+1]bool` tracks
  absent spans.
- **Step 3:** Absent-row pass over `inRangeRefs` using `seenByPK` — identical semantics to the
  old path.
- **Step 4:** Allocate 3D accumulator now that `numBoundaries` is final. Hot loop iterates the
  `histSpanEntry` intermediate slice — 3 array reads + 1 increment, zero float math.

**Consequence:**

- `buildAggValsForRef` is no longer called for HISTOGRAM (still called for SUM/AVG/MIN/MAX/etc.).
- `streamByRefSlice` signature gains `r *modules_reader.Reader`, `dictByPK []uint32`, `maxPK uint32`
  (passed through to the HISTOGRAM branch only; ignored by COUNT/RATE and other agg branches).
- `histSpanEntry` lifted to package scope so `streamByRefSliceHistogramScan` (extracted helper)
  can share the type without a type alias.
- Memory savings per file: ~9.6 MB (valByPK + hasByPK) + ~1.2 MB (aggValsForRef + aggPresent).
- Operation savings per file: ~300 M array writes/reads eliminated.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamByRefSliceHistogram`,
`streamByRefSliceHistogramScanDict`

_Addendum (2026-04-21):_ NOTE-088 reversed the consequence — `histSpanEntry` intermediate slice
and `streamByRefSliceHistogramScan` are deleted. The flat pre-allocated accumulator accumulates
directly during the column scan with no second pass. See NOTE-088 below.

---

## NOTE-088: Flat pre-allocated accumulator in streamByRefSliceHistogram (2026-04-21)

_Added: 2026-04-21_

**Decision:** Replace the `histSpanEntry` intermediate slice in `streamByRefSliceHistogram` with
a flat pre-allocated `groupCountsFlat []int64` accumulator sized using a fixed boundary cap
(`histFlatStride = 64`). Accumulate directly during the column scan — single pass, no second loop.

**Rationale:**
The NOTE-087 approach eliminated `buildAggValsForRef` (saving ~10.8 MB) but introduced a new
intermediate allocation: `entries []histSpanEntry` sized to `len(inRangeRefs)`. At 150 M in-range
spans, `histSpanEntry` (10 bytes each) = 1.5 GB. The subsequent second-pass accumulation loop
then wrote every entry into the 3D accumulator, totalling ~3 GB of memory traffic per file.

By pre-allocating `groupCountsFlat` with a fixed boundary cap (`histFlatStride = 64`) before
the column scan, we can accumulate inline during the scan:

- No intermediate `histSpanEntry` slice — eliminates 1.5 GB allocation at 150 M spans.
- No second accumulation pass — eliminates the second 1.5 GB read.
- Flat layout `[gIdx * 64 * numSteps + bIdx * numSteps + timeIdx]` is cache-friendly and
  requires only one `make` call vs. N×M `make` calls for the 3D slice-of-slices.
- `histFlatStride = 64` covers all realistic log2 histogram boundaries for `span:duration`
  (max ~46 distinct values) with headroom.

**Consequence:**

- `histSpanEntry` struct deleted — no longer needed.
- `streamByRefSliceHistogramScan` (appended to entries) deleted and replaced by
  `streamByRefSliceHistogramScanDict` (accumulates directly into flat array).
- `streamByRefSliceHistogramAlloc` (3D slice allocator) deleted.
- `streamByRefSliceHistogramEmit` (3D slice emitter) deleted and replaced by
  `streamByRefSliceHistogramFlatEmit` (flat array emitter with same semantics).
- `getBoundaryIdx` return type changed from `int16` to `int64` (matches flat index arithmetic).
- All existing tests pass unchanged — `streamByRefSliceHistogram` signature is identical.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:streamByRefSliceHistogram`,
`streamByRefSliceHistogramScanDict`, `streamByRefSliceHistogramFlatEmit`

---

## NOTE-089: Direct path extended to HISTOGRAM and general agg functions (2026-04-21)

_Added: 2026-04-21_

**Decision:** Remove the `&& isCountRate` gate at `executeTraceMetricsIntrinsic` and extend
`accumulateIntrinsicBucketsDirect` to dispatch to two new functions: `accumulateHistogramDirect`
(HISTOGRAM) and `accumulateAggDirect` (SUM/AVG/MIN/MAX/STDDEV/QUANTILE).

**Rationale:**
The direct path (NOTE-085) built `bucketByPK`/`dictByPK` arrays but then returned `(false, nil)`
for non-count/rate functions, forcing a fallback to `accumulateIntrinsicBucketsViaKeyMap` which
materializes `inRangeRefs`/`inRangeVals` (~3 GB) and calls `streamByRefSliceAgg` (150 M hash
map probes per file for M9/M11/M15/M17 queries via `idBuckets map[aggKey]*aggBucketState`).

`accumulateHistogramDirect`: receives the pre-built `bucketByPK` (already available in
`accumulateIntrinsicBucketsDirect`) and passes it directly to `streamByRefSliceHistogramScanDict`,
eliminating the redundant O(inRangeRefs) rebuild that `streamByRefSliceHistogram` performed.
Absent-row pass walks `bucketByPK` directly (like `accumulateCountRateDirect`) — no `inRangeRefs`.

`accumulateAggDirect`: allocates `[][]*aggBucketState` sized `[numGroups][numSteps]` (lazy nil
pointers per cell). Scans the agg column directly using `dictByPK`/`bucketByPK` for O(1) group
and time-bucket lookups — no hash map, no `buildAggValsForRef` (7.7 MB `valByPK` eliminated),
no `inRangeRefs` scan. Absent-row pass walks `bucketByPK` to create count=0 bucket entries,
matching `streamByRefSliceAgg`'s NaN-emit behavior. Column scan extracted to
`accumulateAggDirectScanCol` to bound cyclomatic complexity.

**Consequence:**

- `&& isCountRate` gate removed; `accumulateIntrinsicBucketsDirect` now handles all N=1
  no-predicate queries with dict-format group-by.
- `streamByRefSliceAgg` and `buildAggValsForRef` remain as fallback for N>1 or flat group-by.
- 150 M hash ops per file eliminated for M9/M11/M15/M17 agg queries; 7.7 MB `valByPK` removed.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect`,
`accumulateHistogramDirect`, `accumulateAggDirect`, `accumulateAggDirectScanCol`,
`accumulateHistogramDirectN0`

_Addendum (2026-04-22):_ Extended NOTE-089 to include the N=0 (no group-by) histogram direct path.
`accumulateHistogramDirectN0` eliminates `inRangeRefs` materialization and the `keyToBucket` hash
map for `{} | histogram_over_time(duration)` style queries. Dispatched from
`executeTraceMetricsIntrinsic` before the N=1 case when `len(agg.GroupBy) == 0` and
`agg.Function == FuncNameHISTOGRAM` with no predicates. Originally used an all-zeros `dictByPK`
slice (gIdx always 0); replaced by `scanHistogramN0` which eliminates the 256 KB `dictByPK`
allocation and per-span lookup entirely, closing the remaining ~1.5× gap vs parquet.

---

## NOTE-090: Canonical metrics accumulation pattern — single-pass dense arrays (2026-04-23)

Summarises the design constraints that emerged across NOTE-085 through NOTE-089. Any future metrics
accumulator in this package must follow all five rules:

**Rule 1 — Never materialize `inRangeRefs`.**
Build `bucketByPK[maxPK+1]int64` directly from `tsCol.BlockRefs[lo:hi]`. Materialising a
`[]BlockRef` slice first and then iterating it a second time to accumulate was the original design
and the root cause of multi-GB allocations at query time (1.2 GB for `inRangeRefs` alone at 150 M
spans). The time-column ref slice is the only authoritative source of in-range span identity.

**Rule 2 — Single pass per value column.**
For each dict entry (or flat row), look up `bucketByPK[pk]` and `dictByPK[pk]` and write directly
to the accumulator. Never collect refs into an intermediate slice and process them in a separate
loop. Intermediate slices (`histSpanEntry`, `aggValsForRef`) were removed in NOTE-087/088 for
exactly this reason.

**Rule 3 — Dense arrays, not hash maps.**
`bucketByPK[pk]` and `dictByPK[pk]` give O(1) cache-friendly array reads. The old `keyToBucket`
and `groupKeyMap` hash maps caused 54% of metrics query CPU to be spent in `maps.ctrlGroup.matchH2`
(hash probing). Dense arrays eliminate hash collision entirely and allow the CPU prefetcher to
predict access patterns.

**Rule 4 — Allocate proportional to the problem, not the maximum.**

- Group arrays: size to `numGroups` (number of distinct group-by values), not `maxPK`.
- `dictByPK`: only allocate when `numGroups > 1`. For N=0 (no group-by), gIdx is always 0 —
  allocating and reading a zero-filled `dictByPK` wastes up to 256 KB per block and adds a
  memory load per span. Use a specialised scanner (`scanHistogramN0`) instead.
- `seenByPK`: size to `maxPK+1`; always needed for the absent-row pass.

**Rule 5 — Absent-row pass walks `bucketByPK`, not refs.**
After scanning the value column, walk `bucketByPK` and emit count=0 (or min/max sentinel) buckets
for every pk where `bk != 0 && !seenByPK[pk]`. This handles spans that are in the time range but
absent from the value column (e.g. a span with no `span:duration`). Walking `bucketByPK` is O(maxPK)
not O(inRangeCount); for sparse blocks this is cheaper and avoids ref materialisation.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateCountRateDirect`,
`accumulateHistogramDirect`, `accumulateAggDirect`, `accumulateHistogramDirectN0`,
`scanHistogramN0`

---

## NOTE-091: Structural Query Block Selection via planBlocks (2026-04-22, updated 2026-05-06, 2026-05-07)

**Context:** `ExecuteStructural` previously called `planner.Plan(nil, tr)` — no predicates,
so no bloom, range, or intrinsic-TOC pruning fired. Only the TS index applied. For
`{span.http.status_code="500"} >> {}` on a large file this forced scanning every block.

**Decision (2026-04-22/2026-05-06):** Apply file-level bloom/range rejection per program.

**Decision (2026-05-07): planBlocks per structural node.** Each structural node is a regular
filter program with an added relationship constraint (isChild, isParent, isAdjacent, etc.).
Block selection now calls `planBlocks(r, prog, tr, opts)` per program — the same bloom,
range-index, intrinsic-TOC, and TS-index pruning as plain filter queries — and unions the
selected block sets across all programs.

Any block absent from every program's selected set has no spans matching any structural
node. Because structural matches require spans from matching nodes, such a block cannot
contribute to any match.

**Limitation (multi-block traces):** Intermediate ancestor spans that do not match any
program predicate but are needed to link a descendant to an ancestor may live in a pruned
block, causing false negatives for that specific trace. This is acceptable for typical
workloads where individual traces fit within a single internal block.

**Negation-LHS programs** (`shouldRejectFileForProgram=false`) use `planBlocks(nil, tr, opts)`
so time-range pruning still applies but no predicate pruning is attempted.

**Why improvements to planBlocks propagate automatically:** Structural queries share the
same block-selection code path as plain filter queries. Any new index type or pruning
strategy added to `planBlocks` benefits structural queries without duplication.

**File-level rejection safety:** Bloom filters have ~0.4% FPR. Accepted: same semantics as
tempodb's existing bloom-based file selection.

**Edge case:** When all programs are nil (e.g. `{} >> {}`), `planBlocks(nil, tr, opts)`
is called for each program, selecting all blocks in the time range — identical to the
pre-NOTE-091 behavior.

Back-ref: `internal/modules/executor/stream_structural.go:collectAllStructuralSpans`

---

## NOTE-092: Pipeline Query Streaming Count — Two-Pass to Avoid O(N_spans) Allocation (2026-04-22)

**Context:** `streamPipelineQuery` accumulated all matching spans into `allSpans []SpanMatch`
before grouping and aggregating. For `{} | count() > 5` on a large file with 500K+ spans,
this caused 60s timeouts due to massive allocation and GC pressure.

**Decision:** For `count`/`count_over_time` aggregates with a threshold, use a two-pass approach:

- Pass 1: stream spans, accumulate per-trace integer counters only (no span content).
- Filter qualifying trace IDs by threshold.
- Pass 2: stream spans again, emit only spans from qualifying traces.

Memory: O(N_distinct_traces) instead of O(N_spans). I/O: 2× (two full scans), which is
acceptable because the alternative is OOM or timeout.

For other aggregates (avg, min, max, sum) where span field values are needed, the general
path with `allSpans` accumulation and `maxPipelineSpans` guard is retained.

**Edge case — no qualifying traces:** When `qualified` map is empty after pass 1, `fn(nil, false)`
is called immediately and pass 2 is skipped entirely.

**Edge case — limit:** Pass 2 tracks a `limit` counter per the caller's `opts.Limit`; once
exhausted, streaming stops early via the callback returning false.

Back-ref: `query_traceql.go:streamPipelineQuery`, `query_traceql.go:streamPipelineQueryCount`

## NOTE-093: Thread blockIdx/rowIdx through structuralSpanRec to enable Fields population

_Added: 2026-04-28_

**Decision:** Added `blockIdx int` and `rowIdx int` to `structuralSpanRec`. The rec-building
loop (`collectBlockStructuralSpanRecs`) populates them from the `blockIdx` function parameter
and the `rowIdx` loop variable. `evalStructuralMatches` copies them into the emitted
`SpanMatch.BlockIdx` / `SpanMatch.RowIdx`. The conversion layer in `api.go` uses these to
call `modules_blockio.NewSpanFieldsAdapterWithReader(nil, r, m.BlockIdx, m.RowIdx)` — passing
`nil` for the block because structural queries do not retain decoded `*reader.Block` objects
after phase 1.

**Why nil block:** Structural phase 1 eagerly fetches all blocks via `FetchBlocks` (NOTE-034,
SPEC-STREAM-11 exemption). Decoded blocks are local variables inside the phase-1 block loop
and are discarded after `collectBlockStructuralSpanRecs` returns. Retaining a `*reader.Block`
pointer in every `structuralSpanRec` for N spans would keep all decoded block column data live
through phases 2 and 3 — a significant memory cost for no benefit. The `SpanFieldsAdapter`
with a nil block falls through to the intrinsic section path via `loadIntrinsicCache`, which
uses `reader.GetIntrinsicColumn` to resolve field values. The nil-block guard added to
`span_fields.go` enables this.

**Memory cost:** 16 bytes per `structuralSpanRec` (two `int` fields). For 100K spans: ~1.6 MB
additional heap during phase 1. Acceptable.

**Back-ref:** `internal/modules/executor/stream_structural.go:structuralSpanRec`,
`internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`,
`internal/modules/executor/stream_structural.go:evalStructuralMatches`,
`api.go` (structural branch),
`internal/modules/blockio/span_fields.go:GetField`,
`internal/modules/blockio/span_fields.go:IterateFields`

---

## NOTE-094: populateTypedColumn — Typed Accessors (LookupRefFast) + [8]byte spanID/parentID (2026-04-28)

_Added: 2026-04-28_

**Decision:** Two optimizations applied together:

**(1) `populateTypedColumn` rewritten to use `LookupRefFast` + `storeTypedField`** — the hot loop
now calls `col.LookupRefFast(packed)` (returning `any`) and dispatches type-specifically in
`storeTypedField`. The typed accessor methods (`LookupRefFastUint64` etc., NOTE-015) were removed
from `intrinsic_ref_index.go`; `LookupRefFast` returns the concrete type directly via a format switch,
eliminating the interface boxing that produced 171M/120s alloc_objects (59.6% of total, Pyroscope 2026-04-28).
Updated `tsCol.LookupRef(packed) + val.(uint64)` callers in `stream.go` to use `LookupRefFast` + type assert.

**(2) [8]byte fixed arrays for spanID/parentID** — changed `intrinsicRowFields.spanID`,
`.parentID` and `structuralSpanRec.spanID`, `.parentID` from `[]byte` to `[8]byte`.
`storeTypedField` previously called `append([]byte(nil), b...)` for each matched span for
spanID and parentID — one heap allocation each. For structural queries with 1000 spans
per block × identity columns × many blocks, this accounted for ~26M allocs.

**[8]byte rationale:** `[8]byte` is a value type stored directly in the `[]intrinsicRowFields`
slice — no separate heap allocation. `copy8()` helper writes 8 bytes into the already-allocated
slice element; no clone needed.

**Why [8]byte is safe:** `BytesValues` entries are independent copies (guaranteed by
blockio/shared NOTE-012 — `DecodeFlatPage` uses make+copy before appending; and by NOTE-013 —
`decodeXORBytesPage` similarly uses make+copy). The source slice does not alias pool memory.

**Zero-value sentinel:** `[8]byte{}` (all zeros) replaces `nil` as the absent-ID sentinel.
The `present` bitmask in `intrinsicRowFields` remains the canonical absent-indicator.
For `structuralSpanRec`, `structuralSpanIDPresent` / `structuralParentIDPresent` bits are used.

**present-bit guard in evalStructuralMatches:** Added a guard `if spans[ri].present&structuralSpanIDPresent == 0 { continue }`
to avoid emitting zero-byte SpanIDs for absent spans.

**API boundary:** `evalStructuralMatches` converts `spans[ri].spanID [8]byte` back to `[]byte`
via `append([]byte(nil), spans[ri].spanID[:]...)` when building `SpanMatch.SpanID`. This
allocation is per-match (O(matched spans)), not per-scanned span.

**Betteralign:** After the type change, `[8]byte` (8 bytes) is smaller than the former `[]byte`
(24-byte slice header). Struct field ordering was updated accordingly.

Back-ref: `internal/modules/executor/intrinsic_row.go:populateTypedColumn`
Back-ref: `internal/modules/executor/intrinsic_row.go:storeTypedField`
Back-ref: `internal/modules/executor/intrinsic_row.go:intrinsicRowFields`
Back-ref: `internal/modules/executor/stream_structural.go:structuralSpanRec`
Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopKKLL`
Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopKScan`
Back-ref: `internal/modules/executor/predicates.go:intrinsicLeafGetBytesTyped`

---

## NOTE-095: All-Program File-Level Rejection for Structural Queries (2026-04-28)

_Added: 2026-04-28_

**Decision:** Extend `collectAllStructuralSpans` to apply `fileLevelReject` and
`fileLevelBloomReject` for ALL chain programs, not just the first non-nil (LHS) program.
If any program for which rejection is safe rejects the file, return nil immediately.

**Safety:** File-level rejection is safe when the program corresponds to the RHS of any
operator, or when it is the LHS of a non-negation operator. For the LHS program of a
negation operator (!>>, !>, !~), file-level rejection on the LHS is NOT safe: if the LHS
filter matches nothing in the file, all RHS spans trivially qualify for the negation
(e.g. `{ ghost } !>> { svc }` → every svc span has no ghost ancestor → all svc spans match).
Rejecting the file based on LHS absence would incorrectly return no results.

**Helper:** `shouldRejectFileForProgram(ops, progIdx)` encodes this rule. `isNegationOp`
identifies negation operators. Both are tagged NOTE-095 in code.

**Performance:** Eliminates entire files where the RHS predicate (e.g. `db.system = "redis"`)
guarantees no match — useful for OR structural queries where the RHS has high selectivity.

Back-ref: `internal/modules/executor/stream_structural.go:collectAllStructuralSpans`
Back-ref: `internal/modules/executor/stream_structural.go:shouldRejectFileForProgram`
Back-ref: `internal/modules/executor/stream_structural.go:isNegationOp`

---

## NOTE-096: Trace Pre-Qualification Before Phase 3 (2026-04-28)

_Added: 2026-04-28_

**Decision:** In `evalStructuralMatches`, before calling `applyStructuralOps` for each trace,
check that the trace has at least one span matching each required nodeMatch bit. If any
required bit is absent, skip the trace entirely — no structural match is possible.

**Implementation:** `traceCanMatch(spans, ops)` builds a `present` bitmask by OR-ing all
span nodeMatch values. For non-negation operators, all node bits [0..N-1] must be present.
For single-negation-op queries (!>>, !>, !~), only bit 1 (RHS) must be present — absent LHS
means all RHS spans qualify (the negation holds vacuously), so requiring LHS bit present
would incorrectly prune valid results.

**Cost:** O(spans_per_trace) — one pass over the already-collected span slice. This replaces
O(right_matches × depth) ancestor-chain walk cost for traces that cannot match.

**Performance:** For OR structural queries where the LHS filter is selective (few traces have
any LHS-matching span), this eliminates the majority of Phase 3 work.

Back-ref: `internal/modules/executor/stream_structural.go:evalStructuralMatches`
Back-ref: `internal/modules/executor/stream_structural.go:traceCanMatch`

---

## NOTE-097: Intrinsic TOC File-Level Rejection for Structural Queries (2026-04-28)

_Added: 2026-04-28_

**Decision:** Extend the existing bloom/range rejection loop in `collectAllStructuralSpans`
to also call `BlocksFromIntrinsicTOC(r, prog)` for each eligible program. If the result
is a non-nil empty slice (zero blocks survive), return `nil, nil` to skip the file.

**Key semantics of `BlocksFromIntrinsicTOC` return values:**

- `nil` → no intrinsic section, no intrinsic predicates, or all blocks survive: no file rejection.
- `[]int{}` (non-nil empty) → zero blocks survive → entire file rejected.
- `[]int{i, j, ...}` (non-empty) → block-level subset: accumulated into the union across programs
  and used for block-level union pruning after planning (see NOTE-091 updated 2026-05-06).

**Safety — negation operators:** `shouldRejectFileForProgram(ops, i)` already gates the call.
For the LHS of a negation op (!>>, !>, !~), absent LHS means all RHS spans qualify vacuously —
we must not skip the file based on LHS absence. The gate function returns false for this case,
preventing false file rejection. No new safety logic is needed.

**Safety — OR predicates:** `BlocksFromIntrinsicTOC` calls `collectIntrinsicLeaves` which
handles same-column OR nodes via `mergeORIntrinsicLeaves`. If both branches reference the same
intrinsic column with equality predicates, they are merged into a single leaf with unioned values.
Mixed-column OR nodes are conservatively skipped (function returns nil). No false negatives.

**Placement:** TOC check runs after bloom/range checks (cheaper checks first; TOC I/O is skipped
for already-rejected files) and before `planner.Plan(nil, tr)` (block selection skipped for
TOC-rejected files, saving the call entirely).

**Performance:** Primary benefit is for range predicates on intrinsic columns (span:duration,
span:start) where bloom filters (Fix 1) provide no pruning. Secondary benefit for equality
predicates: TOC provides definitive block-by-block presence data vs. bloom FPR (~0.4%).
Benchmark (BENCH-EX-18): TOC-rejected files execute ~36x faster than non-rejected files.

Back-ref: `internal/modules/executor/stream_structural.go:collectAllStructuralSpans`

---

## NOTE-098: wantCols Threading into SpanFieldsAdapter for Selective Intrinsic Decode

_Added: 2026-04-30_

**Decision:** `NewSpanFieldsAdapterWithReader` now accepts `wantCols map[string]struct{}` and
`isDualStorage bool` parameters. Call sites in `query_traceql.go`, `api.go`, and `reader.go`
pass appropriate values.

**Rationale:** Before this change, `loadIntrinsicCache` unconditionally decoded all intrinsic
columns for every matched span on the backward-compat path (old files lacking dual storage).
`IterateFields` ran an O(N_intrinsics) detection loop per call. Both costs are eliminated or
bounded by this change.

**Call-site decisions:**

- `query_traceql.go:streamFilterProgram` — passes `isDualStorage=computeIsDualStorage(row.Block, r)`
  and `wantCols=ComputeSecondPassCols(program, opts.SelectColumns)`, computed once before the row loop.
- `query_logql.go:streamLogProgram` — passes `isDualStorage=computeIsDualStorage(row.Block, r)`
  and `wantCols=ComputeSecondPassCols(program, nil)`. `LogQueryOptions` has no SelectColumns.
- `api.go:QueryTraceQL` (structural path) — passes `isDualStorage=false,
wantCols=ComputeSecondPassCols(nil, opts.SelectColumns)`. Nil program is correct at result time.
  Block is nil for structural matches (NOTE-093); false is the correct safe default for isDualStorage.
  When SelectColumns is empty, result is nil (load-all). When non-empty, restricts to selected
  columns ∪ searchMetaCols ∪ traceIntrinsicColumns.
- `reader.go:GetTraceByID` — passes `isDualStorage=computeIsDualStorage(bwb.Block, r), wantCols=nil`.
  GetTraceByID requires all fields; nil wantCols is correct.

**SPEC-ROOT-017** codifies this as a codebase-wide invariant.

Back-ref: `internal/modules/blockio/span_fields.go:NewSpanFieldsAdapterWithReader`,
`internal/modules/blockio/span_fields.go:loadIntrinsicCache`,
`internal/modules/blockio/span_fields.go:IterateFields`,
`internal/modules/executor/predicates.go:ComputeSecondPassCols`,
`query_traceql.go:streamFilterProgram`,
`query_logql.go:streamLogProgram`,
`api.go:QueryTraceQL`,
`reader.go:GetTraceByID`

---

## NOTE-099: Sorted-Slice BlockRef Union/Intersect — No Map Allocations

_Added: 2026-05-02_

**Problem:** Pyroscope CPU profiles on the dev-03 querier showed `unionBlockRefs` and
`intersectBlockRefSets` (called from `collectFromIntrinsicRefs` via `evalNodeBlockRefs` and
`evalNodeBlockRefsPartialAND`) consuming 20%+ of total CPU:

- `maps.ctrlGroupMatchH2`: 11.85% self
- `maps.ctrlGroup.matchH2`: 8.3% self
- `mapassign_fast32`: 2.33% self

All traced to `map[refKey]struct{}` allocations inside these two functions. The hot pattern
is same-column OR queries: `(service.name="grafana" || service.name="faro")` where
ref sets for OR branches are always **disjoint** (a span has exactly one value per column).

**Solution:** Replace map-based union/intersect with sorted-slice merge operations:

1. **Pack refs**: `uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)` — fits in one word, gives
   BlockIdx-major ordering.
2. **Sort on entry**: each ref slice is sorted in-place (via `sortRefs`) before merge if not
   already sorted (checked via O(N) `isSortedRefs` scan). Refs from `scanIntrinsicLeafRefs`
   are not guaranteed to be sorted by packed value (flat columns are sorted by column value;
   dict column refs are appended in block-processing order).
3. **Union**: `unionSortedRefs` — O(M+N) two-pointer merge with dedup. Handles both
   disjoint (same-column OR) and overlapping (cross-column OR) cases correctly.
4. **Intersect**: `intersectSortedRefSets` — sorts sets by length, then pairwise two-pointer
   intersection walk. O(total refs) with no allocations beyond the result slice.

**Implementation:** `internal/modules/executor/sorted_refs.go` — all merge primitives.
`predicates.go:unionBlockRefs` and `predicates.go:intersectBlockRefSets` now delegate to
the sorted-slice implementations after sorting their inputs.

**Invariant:** all slices passed to `unionSortedRefs` / `intersectSortedRefSets` must be
sorted in ascending packed-ref order. The wrapper functions in `predicates.go` ensure this.

Back-ref: `internal/modules/executor/sorted_refs.go`,
`internal/modules/executor/predicates.go:unionBlockRefs`,
`internal/modules/executor/predicates.go:intersectBlockRefSets`

---

## NOTE-100: Block-Boundary Scatter for Structural Hot Path

_Added: 2026-05-04_

**Problem:** Pyroscope CPU profiles on the dev-03 querier showed `LookupRefFast` (37%),
`BinarySearchFunc` (14%), and `storeTypedField` (12%) = 63% of structural query CPU
concentrated in `collectBlockStructuralSpanRecs` → `lookupIntrinsicFieldsTyped`.

The bottleneck: for each block with N spans and K intrinsic columns, `lookupIntrinsicFieldsTyped`
performed O(K × N × log(B×N)) binary searches (one per column per span) and allocated a
`[]BlockRef` of length N on every block call. Also, `LookupRefFast` → `storeTypedField`
boxes each value into `any`, incurring interface allocations.

**Solution:** Add `lookupIntrinsicFieldsTypedForBlock` + `populateTypedColumnForBlock` to
the executor, backed by `BlockRefRange` in `shared`.

Key observations that make this safe:
1. In the structural path, `allRefs` is always blockIdx=constant, rowIdx=0..N-1 — exactly the
   entries for one block in the refIndex.
2. Because `Packed = blockIdx<<16 | rowIdx` and refIndex is sorted by Packed, all entries for
   a single blockIdx are **contiguous** in refIndex.
3. One binary search finds the start; a linear walk finds the end: O(log(B×N) + N_in_block).
4. Scatter the results directly into `[]intrinsicRowFields` using typed field writes — no boxing.

**Complexity:** O(K × (log(B×N) + N)) vs O(K × N × log(B×N)) per block.
For N=1000 spans and B×N=50000 entries: ~16 searches vs ~16000 searches per column.

**Secondary gains:**
- Eliminates `make([]BlockRef, N)` allocation per block call.
- Outer column-switch in `populateTypedColumnForBlock` eliminates per-row interface boxing
  from `LookupRefFast` / `storeTypedField` (12% CPU).
- Benchmark (BENCH-EX-20): ~10× ns/op improvement, ~540× fewer allocations per call.

**Implementation:**
- `shared/intrinsic_ref_index.go:BlockRefRange` — O(log N) block range finder. (NOTE-016)
- `executor/intrinsic_row_block.go:populateTypedColumnForBlock` — typed scatter per column.
- `executor/intrinsic_row_block.go:lookupIntrinsicFieldsTypedForBlock` — drop-in replacement.
- `executor/stream_structural.go:collectBlockStructuralSpanRecs` — uses new function.

Back-ref: `internal/modules/executor/intrinsic_row_block.go`,
`internal/modules/blockio/shared/intrinsic_ref_index.go:BlockRefRange`,
`internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`

## NOTE-101: rowIndexScratchPool — Per-Block Scratch Slice Pool for Log Hot Path
*Added: 2026-05-15*
**Decision:** Add `rowIndexScratchPool sync.Pool` of `*[]int` in stream_log_topk.go.
`filterRowsByTimeRange` and `collectMixedTopK` (stream.go) use `acquireRowIndexScratch` /
`releaseRowIndexScratch` to avoid per-block `make([]int, 0, N)` allocations.
**Rationale:** pprof showed `filterRowsByTimeRange`'s `kept := make([]int, 0, len(rows))`
and `collectMixedTopK`'s `qualifying := make([]int, 0, len(candidateRows))` are per-block
allocations in every log query with a time range. The pool eliminates them.
**Cap guard:** Pool items with `cap > 65536` are replaced on return with
`make([]int, 0, 256)` to bound pool memory (~512 KiB max per pooled item, 65536 int64 elements × 8 bytes). Matches the
pattern established by compositeKeyScratchPool (NOTE-071).
**Lifetime invariant:** Scratch is released after the per-block closure returns and
before the next block starts. It must not escape to callers.
Back-ref: `internal/modules/executor/stream_log_topk.go:acquireRowIndexScratch`,
`internal/modules/executor/stream.go:collectMixedTopK`

## NOTE-102: blockColumnProviderPool — Pool for Per-Block Column Provider Struct
*Added: 2026-05-15*
**Decision:** Add `blockColumnProviderPool sync.Pool` of `*blockColumnProvider` in
column_provider.go. `acquireBlockColumnProvider(block)` / `releaseBlockColumnProvider(p)`
replace `newBlockColumnProvider(block)` at all call sites in the executor package.
**Rationale:** pprof showed `newBlockColumnProvider` at 1.01% of allocs — one per block
in all scan paths. The struct is trivial (single *Block pointer); the pool eliminates this.
**GC safety invariant:** `p.block` is set to nil before returning to pool. This prevents
the pool from retaining stale references to decoded block column data beyond the block's
processing lifetime.
**Lifetime invariant:** The provider is not used after the per-block closure returns.
All ColumnPredicate calls complete before releaseBlockColumnProvider is called.
**Migrated call sites:**
- `internal/modules/executor/stream.go:scanBlocks`
- `internal/modules/executor/stream.go:collectMixedPlain`
- `internal/modules/executor/stream.go:collectMixedTopK`
- `internal/modules/executor/stream_topk.go:topKScanBlocks`
- `internal/modules/executor/stream_log_topk.go:logCollectAll`
- `internal/modules/executor/metrics_trace.go`
- `internal/modules/executor/metrics_log.go`
- `internal/modules/executor/stream_structural.go`
Back-ref: `internal/modules/executor/column_provider.go:acquireBlockColumnProvider`

## NOTE-103: WantColumns cache on vm.Program — compile-time column set
*Added: 2026-05-23*
**Decision:** Add `WantColumns map[string]struct{}` field to `vm.Program`. Populated once at compile time by `program.ComputeWantColumns()` (called at the end of `CompileTraceQLFilter`, `CompileTraceQLFilterWithOptions`, the metrics compiler, `logqlparser.Compile`, and `logqlparser.CompileAll`). `ProgramWantColumns` returns this cached map directly when `len(extra) == 0`. When `len(extra) > 0`, `ProgramWantColumns` copies the cached set and merges extra — O(cached-set-size + extra) — rather than re-walking the predicate tree. `ProgramWantColumns` is called from 6 distinct locations across the executor package (computeColumnFilters, ExecuteLogMetrics, stream_log.go, stream_log_topk.go, metrics_trace.go, predicates.go:ComputeSecondPassCols).
**Rationale:** `ProgramWantColumns` was called once per file and once per second-pass decode, walking the `RangeNode` tree and building a new `map[string]struct{}` each time. For 10 000 files per query, this was 10 000 map allocations + tree walks. Since `Program` is immutable after compilation, the column set never changes — caching it eliminates all per-file allocs on the hot path.
**Immutability invariant:** `WantColumns` is written once at compile time and never modified after that. Callers must not mutate the returned map. When extra columns are needed (`len(extra) > 0`), `ProgramWantColumns` returns a fresh copy.
**Legacy programs:** Programs constructed manually (e.g. in tests, or via the `compileMatchAllProgram` path) have `WantColumns == nil`. `ProgramWantColumns` still falls through to the tree-walk path, so behavior is unchanged for those callers.
**LogQL programs:** `logqlparser.Compile` and `logqlparser.CompileAll` also call `ComputeWantColumns()` so LogQL programs benefit from the same compile-time cache.
Back-ref: `internal/vm/program.go:ComputeWantColumns`, `internal/modules/executor/predicates.go:ProgramWantColumns`

## NOTE-104: filterRowSetByIntrinsicNodes wired to typed lookup — eliminates N map allocs per block
*Added: 2026-05-23*
**Decision:** Replace `lookupIntrinsicFields` / `rowSatisfiesIntrinsicNodes` with `lookupIntrinsicFieldsTyped` / `rowSatisfiesIntrinsicNodesTyped` in `filterRowSetByIntrinsicNodes`. Also replace `make([]BlockRef, len(rows))` with a stack-allocated `[64]modules_shared.BlockRef` backing array for the common case of ≤64 candidate rows.
**Rationale:** `lookupIntrinsicFields` returned `[]map[string]any` — N heap-allocated maps for N candidate rows. `lookupIntrinsicFieldsTyped` (added in NOTE-081 for the structural hot path) returns `[]intrinsicRowFields`, a slice of value structs with no interior pointers. For a trace query with 100 candidate rows per block and 1000 blocks, this eliminates 100 000 map allocations. The stack pre-alloc for `refs` eliminates one `[]BlockRef` heap alloc per block for the typical small-row-set case (≤64 rows).
**Stack pre-alloc invariant:** `refsArr` is stack-allocated and lives for the duration of `filterRowSetByIntrinsicNodes`. `lookupIntrinsicFieldsTyped` only reads `refs` during the call and does not retain the slice.
**Consequence:** `filterRowSetByIntrinsicNodes` now uses the same typed path as the structural hot path. The typed variants were already validated against the structural path; no new test coverage is required.
Back-ref: `internal/modules/executor/stream.go:filterRowSetByIntrinsicNodes`, `internal/modules/executor/intrinsic_row.go:lookupIntrinsicFieldsTyped`, `internal/modules/executor/predicates.go:rowSatisfiesIntrinsicNodesTyped`

## NOTE-105: flat []int replaces map[int]int for blockToGroup in scan functions
*Added: 2026-05-23*
**Decision:** Replace `blockToGroup map[int]int` with `blockToGroupSlice []int` (indexed by block ID, sentinel -1 for absent) in `scanBlocks`, `topKScanBlocks`, and `iterateLogRows`.
**Rationale:** Block IDs are bounded by `r.BlockCount()`, making a flat slice a valid O(1) replacement for the map. A map with capacity hint still allocates a hash table bucket array and carries per-entry overhead; a flat int slice does not. For typical files with <10 000 blocks the slice is ≤80 KiB. Eliminates one `map[int]int` alloc per file per query across all three scan paths.
**Bounds invariant:** All block IDs written and read are guarded by `bi < blockCount` / `blockIdx < blockCount`. The planner guarantees block IDs are in `[0, blockCount)` by construction; the check is a safety net.
**Consequence:** Memory for `blockToGroupSlice` is proportional to `r.BlockCount()`, not to `len(selectedBlocks)`. For very sparse selections this uses slightly more memory than the original map, but the allocation savings dominate in practice.
Back-ref: `internal/modules/executor/stream.go:scanBlocks`, `internal/modules/executor/stream_topk.go:topKScanBlocks`, `internal/modules/executor/stream_log_topk.go:iterateLogRows`

## NOTE-106: unsafe.String stack-buffer for logAccumulateRow composite key lookup
*Added: 2026-05-23*
**Decision:** In `logAccumulateRow`, build the composite bucket key (`bucketIdx + "\x00" + attrVals joined by "\x00"`) into a 512-byte stack-local `[512]byte` array and use `unsafe.String(&buf[0], n)` for the map lookup. On insert (cache miss), `string(buf[:n])` copies to the heap. Falls back to the original string-concatenation path when `n > 400` bytes. The bucket index integer is written via `strconv.AppendInt(buf[:0], bucketIdx, 10)` — appending directly into the stack buffer with no intermediate heap allocation.
**Rationale:** The original path called `strconv.FormatInt` (heap), `strings.Join` (heap), and `+` concatenation (heap) — 2–3 string allocations per row even when the bucket already exists. For a log metrics query over a high-cardinality file with 10 000 matching rows, this eliminated ~20 000–30 000 string allocations in the lookup-hit common case. Using `strconv.AppendInt(buf[:0], ...)` instead of `strconv.AppendInt(nil, ...)` + copy eliminates the one remaining intermediate heap allocation.
**unsafe.String lifetime contract:** `lookupKey` is valid only for the duration of `logAccumulateRow`. It is used solely in the `buckets[lookupKey]` map lookup before the function returns. It is never stored in a map, returned to callers, or passed to a goroutine. The `buf` array is stack-allocated and outlives `lookupKey`. This satisfies the `unsafe.String` requirement that the pointer remain valid for the string's lifetime.
**Fallback threshold (n > 400):** Keys longer than 400 bytes indicate very long label values. Rather than truncating silently (which would produce wrong bucket keys), the code falls back to the original heap path. 400 bytes is conservative — typical keys are <100 bytes.
Back-ref: `internal/modules/executor/metrics_log.go:logAccumulateRow`

## NOTE-107: scratch []int on blockColumnProvider — borrowed rowSet backing for Scan* methods
*Added: 2026-05-23*
**Decision:** Add `scratch []int` and `scratchInUse bool` fields to `blockColumnProvider`. `collectStreamInto` (a new method replacing the former `collectStream` free function in all 14 `Scan*` methods) reuses `p.scratch` for the first Scan* call within a `ColumnPredicate` evaluation, avoiding one `*rowSet` struct alloc and one `[]int` backing-slice alloc. Subsequent Scan* calls within the same `ColumnPredicate` evaluation (compound AND/OR predicates) detect `scratchInUse=true` and fall back to heap allocation to avoid clobbering the first result. `releaseBlockColumnProvider` resets both `scratch` (length to 0, capacity retained) and `scratchInUse`.
**Rationale:** pprof showed `newRowSetWithCap` contributing allocs in every Scan* path. For single-predicate queries (the common case), the scratch eliminates one `[]int` alloc per block. For compound predicates only the first scan reuses scratch; the rest fall back to heap — still a net improvement. The `blockColumnProviderPool` is bounded by `defaultPipelineWorkers` (a fixed constant, not block count or file count), so scratch growth is O(workers), not O(blocks). Pool `New` pre-allocates `make([]int, 0, 256)` to avoid first-use alloc.
**Lifetime invariant:** The rowSet returned by the first `collectStreamInto` call borrows `p.scratch` as its backing array. `releaseBlockColumnProvider(p)` must be called AFTER all reads of the rowSet are complete (`rowSet.Size()`, `rowSet.ToSlice()`, `rowSet.Contains()`). Releasing early and re-acquiring the same pooled provider (from any goroutine) would reset `p.scratch[:0]` and overwrite the backing array the rowSet still references. All call sites follow the pattern: acquire → ColumnPredicate → consume rowSet fully → release.
**Call-site audit (all callers confirmed safe):**
- `stream.go:scanBlocks` — release after `rowSet.ToSlice()` at the final consumer point. SAFE.
- `stream.go:collectMixedPlain` — provider shared between preFn and fn; released at end of fn after `mixedPlainRowSet.Contains()`. When preFn returns false (Size()==0), fn is skipped and provider is released in preFn. SAFE.
- `stream.go:collectMixedTopK` — same preFn/fn pattern as collectMixedPlain. SAFE.
- `stream_topk.go:topKScanBlocks` — release after `rowSet.ToSlice()` at lines 242/244. SAFE.
- `stream_log_topk.go:iterateLogRows` — release after `processLogRows` returns. `rowSet.ToSlice()` may be backed by `cp.scratch`; releasing earlier would let a concurrent pool user clobber the backing array (which is also aliased by `keptByTime` when the time-range filter is a no-op). SAFE.
- `metrics_log.go` — release after `rowSet.ToSlice()` loop completes. SAFE.
- `metrics_trace.go` — release after `rowSet.ToSlice()` loop completes. SAFE.
- `stream_structural.go:processBlock` — release after the row loop that calls `computeNodeMatchForRow` (which reads sets[i].Contains()). SAFE.
Back-ref: `internal/modules/executor/column_provider.go:collectStreamInto`

## NOTE-108: streamCountRateN1Compact — compact fallback for N=1 count/rate to avoid 4 GB allocations
*Added: 2026-06-07*
**Decision:** When `accumulateIntrinsicBucketsDirect` fails for N=1 count/rate queries (maxPK > 16 M, i.e. large production files), route to a new `streamCountRateN1Compact` function instead of `accumulateIntrinsicBucketsViaKeyMap`.
**Rationale:** `accumulateIntrinsicBucketsViaKeyMap` builds a `map[uint32]int64` with one entry per in-range span (150 M entries ≈ 3 GB for a 24h dataset), then `buildDictIdxForRefs` allocates a dense `[]uint32` of size `maxPK+1` (up to ~944 MB). For a 13 GiB GOMEMLIMIT querier processing multiple files concurrently, these allocations trigger GC storms and OOM pod restarts. `streamCountRateN1Compact` replaces both with sorted packKey arrays (sortedPKs + timeBucketByPos + dictIdxByPos ≈ 116 MB), reducing peak memory ~35×.
**Approach:** Sort the n in-range refs by packKey once (O(n log n)), then scan the group-by column with `scanGroupByColCompact` (binary search, O(total_refs × log n)) to build `dictIdxByPos`. Accumulate via a sequential pass over all n positions into a flat `[]int64` accumulator. Cache-friendly: `dictIdxByPos` and `timeBucketByPos` are accessed sequentially; `groupCountsFlat[gIdx*numSteps + bk-1]` accesses a fixed numSteps-element subarray for each run of same-group positions.
**Queries affected:** M4 `{} | rate() by (resource.service.name)`, M5 `{} | rate() by (span.kind)`, and any no-predicate N=1 count/rate query on files where maxPK > 16 M.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateN1Compact`

## NOTE-110: streamCountRateN1CompactFromRefs — predicate-filtered N=1 count/rate compact path
*Added: 2026-06-07*
**Decision:** For predicate-filtered N=1 count/rate queries (filteredRefs != nil, len(GroupBy)==1), route to a new `streamCountRateN1CompactFromRefs` instead of `accumulateIntrinsicBucketsViaKeyMap`.
**Rationale:** The `accumulateIntrinsicBucketsViaKeyMap` path allocates a `keyToBucket map[uint32]int64` (~24 bytes/entry × 75 M filtered refs ≈ 1.8 GB), then `buildDictIdxForRefs` allocates a dense `dictByPK []uint32` of size maxPK+1 (~64 MB) and `dictIdxForRef []uint32` (n × 4 bytes = ~300 MB). Total ~2 GB per goroutine per file, causing GC pressure. The compact path replaces all of these with `sortedPKs` (n × 4 = 300 MB) and `timeBucketByPos` (n × 8 = 600 MB) + binary search via `scanGroupByColCompact`.
**Key insight:** `inRangeRefs` produced by `mergeJoinFilteredRefsWithVals` is already sorted by packKey (the merge iterates `idx` in ascending packKey order). `streamCountRateN1CompactFromRefs` exploits this by building `sortedPKs` in a single O(n) pass — no `pkOrder` allocation or O(n log n) sort, unlike `streamCountRateN1Compact` for the unfiltered path.
**Approach:** Extract a shared `streamCountRateN1CompactCore(sortedPKs, timeBucketByPos, ...)` called by both the filtered and unfiltered compact paths. The filtered path builds sortedPKs from pre-sorted inRangeRefs; the unfiltered path sorts tsCol refs first.
**Measured improvement:** M6 `{span.kind=server} | rate() by (span.http.request.method)`: 58724ms → 34348ms (-41%) cold cache. Also helps M9-style queries (predicate N=1 rate by service.name).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateN1CompactFromRefs,streamCountRateN1CompactCore`

## NOTE-111: mergeJoinFilteredRefsWithVals — packed uint64 idx, uint32 filteredPKs
*Added: 2026-06-08*
**Decision:** Replace `[]refIdx{uint32, int}` (12 bytes/entry) with packed `[]uint64` (pk in high 32 bits, original index in low 32 bits — 8 bytes/entry) in `mergeJoinFilteredRefsWithVals`. Also replace `slices.Clone(filteredRefs)+SortFunc` with `[]uint32 filteredPKs + slices.Sort`.
**Rationale:** idx array is 86 MB for 7.5M refs × 12 bytes. Packing to uint64 saves 33% (57 MB). Matches the pkOrder uint64 packing optimization used in `streamCountRateN1Compact` (r121). Also: `slices.Sort` on `[]uint64` avoids the closure allocation overhead of `SortFunc`; `filteredPKs []uint32` avoids recomputing `packKey()` during each comparison step.
**Queries affected:** All predicate-filtered metrics queries (M2, M6, M9, M10).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`

## NOTE-112: streamAggN1CompactFromRefs — predicate-filtered N=1 agg compact path
*Added: 2026-06-08*
**Decision:** For predicate-filtered N=1 general agg queries (filteredRefs != nil, GroupBy len 1, not count/rate or histogram), route to `streamAggN1CompactFromRefs` instead of `accumulateIntrinsicBucketsViaKeyMap`.
**Rationale:** `accumulateIntrinsicBucketsViaKeyMap` for 3.75M filtered refs allocates keyToBucket (~90MB) + dictByPK (~64MB) + dictIdxForRef (~15MB) + aggVals (~30MB) + aggPresent (~4MB) ≈ 203MB. The compact path allocates sortedPKs (~15MB) + timeBucketByPos (~30MB) + dictIdxByPos (~15MB) + aggValByPos (~30MB) + aggPresentByPos (~4MB) ≈ 94MB — 54% reduction.
**Key insight:** inRangeRefs from mergeJoinFilteredRefsWithVals is pre-sorted by packKey, so sortedPKs is built O(n) without the pkOrder sort used by streamAggN1Compact.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamAggN1CompactFromRefs`

## NOTE-113: streamCountRateN0HashFilter — hash-filter for N=0 predicate count/rate
*Added: 2026-06-08*
**Decision:** For N=0 count/rate with a selective predicate (F ≤ N/4), bypass `mergeJoinFilteredRefsWithVals` entirely and hash-filter `tsCol[lo:hi]` directly.
**Rationale:** `streamCountRateNoGroupBy` does not need refs in sorted order, so `mergeJoinFilteredRefsWithVals` (O(N log N + F log F) sort) is wasted work. The hash-filter approach: (1) build `map[uint32]struct{}` from filteredRefs (F entries, O(F)); (2) scan tsCol[lo:hi] sequentially checking hash set (O(N)). At 25% selectivity (F ≤ N/4), the hash map (F × ~20 bytes) fits in L3 cache, making hash lookups fast (~10 ns) vs sorting N = O(N log N).
**Threshold F ≤ N/4:** For M2 (service.name = "grafana", F ≈ 500K, N = 7.5M, 6.7% selectivity), hash map ≈ 10 MB < L3 cache (30 MB). For M6 (span.kind = server, F ≈ 3.75M = 50%), hash map ≈ 75 MB >> L3 — mergeJoin is preferred.
**Measured improvement:** M2 `{service.name = "grafana"} | rate()`: 19871ms → 10269ms (-48%).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateN0HashFilter`

## NOTE-114: streamHistogramN1CompactFromRefs — predicate-filtered N=1 histogram compact path
*Added: 2026-06-08*
**Decision:** For predicate-filtered N=1 histogram queries (filteredRefs != nil, GroupBy len 1, histogram), route to `streamHistogramN1CompactFromRefs` instead of `accumulateIntrinsicBucketsViaKeyMap` → `streamHistogramN1Compact`.
**Rationale:** The old path allocated `keyToBucket map[uint32]int64` (~90MB for 3.75M filtered refs) in `accumulateIntrinsicBucketsViaKeyMap`, then `streamHistogramN1Compact` allocated `pkOrder []uint64` (~30MB, freed early) + the binary-search compact arrays. Total peak: ~248MB per goroutine. The new path skips both intermediates: since `inRangeRefs` from `mergeJoinFilteredRefsWithVals` is already packKey-sorted, `sortedPKs` and `timeBucketByPos` are built in O(n) without pkOrder allocation or sort. Also eliminates the O(n log n) sort that `streamHistogramN1Compact` performs unnecessarily for pre-sorted filtered refs.
**Memory profile** (3.75M filtered refs, 280 groups, 1440 steps, ~20 boundaries):
- keyMap path: keyToBucket(~90MB) + pkOrder(~30MB) + sortedPKs(~15MB) + timeBucketByPos(~30MB) + dictIdxByPos(~15MB) + seenByPos(~4MB) + groupCountsFlat(~64MB) ≈ 248MB
- compact path (this): sortedPKs(~15MB) + timeBucketByPos(~30MB) + dictIdxByPos(~15MB) + seenByPos(~4MB) + groupCountsFlat(~64MB) ≈ 128MB (48% reduction)
**Queries affected:** M8 `{span.kind = server} | histogram_over_time(duration) by (resource.service.name)`.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamHistogramN1CompactFromRefs`

## NOTE-119: accumulateCountRateDirect — flat groupCountsFlat replaces [][]int64
*Added: 2026-06-08*
**Decision:** Replace `groupCounts := make([][]int64, numGroups)` (numGroups+1 allocations) with `groupCountsFlat := make([]int64, numGroups*numSteps)` (single allocation) in `accumulateCountRateDirect`.
**Rationale:** The 2D slice pattern (`groupCounts[gIdx][bk-1]++`) requires loading the inner slice header (pointer + len + cap) from `groupCounts[gIdx]` before accessing the element. For numGroups=276 groups, this is 276 inner slice allocations plus one outer slice. The flat array layout (index = `gIdx*numSteps + bk - 1`) replaces the pointer load with one multiply, which is faster (~3 cycles) than a potential cache miss. The contiguous layout also enables better compiler vectorization of the emit loop. Memory is identical (276×1440×8 = 3.2MB either way). The base `base := gIdx * numSteps` hoist eliminates the repeated multiply inside the inner loop.
**Invariant:** `groupCountsFlat[gIdx*numSteps + bk - 1]` stores the same count as `groupCounts[gIdx][bk-1]` (bk is 1-based from bucketByPK). Absent-row pass and emit loop access in 0-based form (`groupCountsFlat[gIdx*numSteps + bk]` for bk=0..numSteps-1), consistent with stepCounts indexing.
**Impact:** Eliminates numGroups=276 small allocations per file (×24 goroutines × ~N files = thousands of objects removed from GC). Theoretical hot-loop savings: removes 1 memory load (inner slice pointer) per in-range span, replaced by 1 multiply. For 150M spans per file at ~3-cycle multiply vs ~4-cycle L1 load: marginal, but correct and cleaner.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateCountRateDirect`

## NOTE-118: timeBucketIndex — one division replaces two (div + mod)
*Added: 2026-06-08*
**Decision:** Replace `offset/stepNanos + (offset%stepNanos==0 ? bkt-- : 0)` with `(offset-1)/stepNanos`.
**Rationale:** The original formula calls `DIVQ` twice (once for quotient, once for modulo) or once if the compiler fuses them but still evaluates two operands. The simplified formula is algebraically equivalent for all `offset > 0` (which all callers guarantee via binary-search `lo/hi`). `(k*s + r - 1)/s = k-1` when `r=0` (boundary case); `= k` when `0 < r < s` (interior case). Single `DIVQ` saves one division per call. With 150M calls per file for M1/M3 (`streamCountRateNoGroupBy`) and ~15ns per division at 3.5 GHz: savings up to 2.25s per goroutine.
**Invariant:** All callers use binary-search to ensure `ts > startTime` (i.e., `offset > 0`). If this invariant were violated and `offset = 0`, the simplified formula returns `-1/stepNanos = 0` (Go truncates toward zero), while the original returns `-1`. Both versions are wrong in this case, but the invariant prevents it.
Back-ref: `internal/modules/executor/metrics_trace.go:timeBucketIndex`

## NOTE-117: maxDirectAggEntries = 4M — compact path for agg when dictByPK > L3 cache
*Added: 2026-06-08*
**Decision:** In `accumulateIntrinsicBucketsDirect`, add a threshold `maxDirectAggEntries = 4_000_000` for non-histogram agg functions. When `maxPK > 4M`, return `false` to trigger `streamAggN1Compact` instead of proceeding with `dictByPK` allocation.
**Rationale:** `dictByPK` costs `(maxPK+1)×4` bytes: 16MB at 4M entries, 64MB at 16M entries. When `maxPK > 4M`, dictByPK exceeds typical L3 cache (24MB), causing DRAM-level cache misses on every `dictByPK[pk]` read in the `accumulateAggDirectScanCol` hot loop (150M lookups per file). The compact path (`streamAggN1Compact`) uses `sortedPKs` sized to n×4 bytes (n = in-range refs, not maxPK); for a file with 1M in-range refs, `sortedPKs` = 4MB (L2-resident). Binary search in L2 (4ns × 20 comparisons = 80ns) is faster than DRAM lookup (100ns) when n ≪ maxPK. Count/rate is unaffected (uses `entryGIdx`, not `dictByPK`, so the entire 64MB allocation is avoided for that path already). Histogram is unaffected (has its own direct path using `accumulateHistogramDirect`).
**Invariant:** The compact path fallback (`streamAggN1Compact`) produces identical results to the direct path — same absent-row semantics, same bucket emission. This change is a pure dispatch optimization, not a correctness change.
**Impact:** M7 `{} | max_over_time(duration) by (resource.service.name)` and similar non-histogram agg queries on large files benefit by eliminating the 64MB dictByPK DRAM bottleneck.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect`

## NOTE-116: timeBucketByPos []int32 — 50% memory reduction for compact-path arrays
*Added: 2026-06-08*
**Decision:** Change `timeBucketByPos` from `[]int64` to `[]int32` in all compact-path accumulation functions: `streamCountRateN1Compact`, `streamCountRateN1CompactFromRefs`, `streamCountRateN1CompactCore` (signature), `streamAggN1Compact`, `streamAggN1CompactFromRefs`, `streamHistogramN1Compact`, `streamHistogramN1CompactFromRefs`, and `scanAggColHistogramCompact` (signature). Update all arithmetic sites to use `int64(bk)-1` where mixed with int64 operands.
**Rationale:** `timeBucketByPos[pos]` stores `timeBucketIndex+1` (sentinel: 0=absent) or `1..numSteps`. Using int64 (8 bytes/entry) was unnecessarily wide — int32 (max 2.1B) safely covers any practical numSteps. Companion to NOTE-115 which reduced `bucketByPK` from int64→int16 for the direct path; this applies the same principle to the compact path. For 7.2M in-range refs: timeBucketByPos 57MB → 29MB (50% reduction). For 3.75M filtered refs: 30MB → 15MB. The reduction decreases GC pressure from concurrent goroutines processing multiple files.
**Invariant:** `bk+1 ≤ numSteps+1`. int32 max is ~2.1B; numSteps is `(EndTime−StartTime)/StepSizeNanos`, at most a few hundred thousand for any practical query. No overflow possible. All read sites use `bk` directly as an integer (Go accepts int32 as slice index and for arithmetic with explicit int64 cast where mixed).
**Impact:** Compact-path queries: M4 `{} | rate() by (service.name)`, M6 `{span.kind=server} | rate() by (http.method)`, M7 `{} | max_over_time(duration) by (service.name)`, M8 histogram queries.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateN1CompactCore,streamAggN1Compact,streamHistogramN1Compact`

## NOTE-122: searchSortedUint32 — interpolation search replaces binary search
*Added: 2026-06-08*
**Decision:** Replace the binary search in `searchSortedUint32` with interpolation search. The function signature and semantics are unchanged; callers need no updates.
**Rationale:** Compact-path scan functions (`scanGroupByColCompact`, `scanAggColCompact`, `scanAggColHistogramCompact`) call `searchSortedUint32` O(N) times per file, where N is the number of column refs (7.5M for a 24h file). Binary search is O(log n) ≈ 22 comparisons for n=3.75M sorted PKs. Interpolation search is O(log log n) ≈ 4-5 comparisons average when the keys are uniformly distributed. PackKeys (`blockIdx<<16|rowIdx`) are approximately uniformly distributed across the 0..maxPK range when spans are distributed evenly across blocks — a reasonable assumption for production blockpack files written by multiple block-builders. The formula `lo + (hi-lo)*(pk-s[lo])/(s[hi]-s[lo])` uses uint64 arithmetic to prevent overflow. Falls back to standard comparison when `pk < s[lo]` or `pk > s[hi]` (out-of-range refs, already handled by the minPK/maxPK guards before this call in most callers).
**Expected speedup:** 22/4.5 ≈ 5x fewer comparisons per lookup. For M8 which does two full scans of 7.5M column refs per file (service.name + duration), this reduces lookup cost by ~5x, potentially bringing warm M8 from >120s to ~30-50s. M4, M6, M7 also benefit proportionally.
**Invariant:** Interpolation search is a correct subset of binary search — it narrows the search window using an interpolated position rather than the midpoint. Correctness is preserved: when the key is present, the function returns its index; when absent, it returns -1, false. The linear fallback at the end handles the edge case where the window converges but `s[lo] == pk` (the standard binary search termination condition).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:searchSortedUint32`

## NOTE-121: scanAggColHistogramCompact — bitset pre-filter for DeltaUint64 binary search
*Added: 2026-06-08*
**Decision:** For `IntrinsicFormatDeltaUint64` columns in `scanAggColHistogramCompact`, build a bitset of size `(maxPK>>6)+1` uint64s (2MB at maxPK=16M) from `sortedPKs` before the column scan, then use `pkBitset[pk>>6] & (1<<(pk&63))` to pre-filter refs before the expensive binary search. Bitset is skipped for `IntrinsicFormatFlat` (small files, < 10K rows).
**Rationale:** `IntrinsicFormatDeltaUint64` (used when rows > 10K, i.e., all production 24h files) stores refs sorted by VALUE (duration), not by packKey. This means consecutive `BlockRefs` entries have unrelated PKs — there is no locality to exploit for binary search in `sortedPKs` (15MB, 3.75M filtered refs). With 8 concurrent goroutines, each goroutine's 15MB `sortedPKs` competes in the shared L3 cache (30MB), causing DRAM-level misses (~20ns). For M8 at 50% selectivity (span.kind=server), ~50% of 7.5M duration refs are absent from `sortedPKs`. Without the bitset, all 7.5M refs incur binary-search cost. With the bitset (2MB, fits in L2), ~3.75M non-matching refs are rejected in ~5ns each, avoiding ~half the binary searches. The bitset build (O(n) over 3.75M sortedPKs) costs ~20ms per file — negligible.
**Math** (50% selectivity, 8 goroutines, 66 files): Binary search only: 7.5M × 22 × 20ns = 3300ms/file × 66/8 = 27.2s additional. Bitset + filtered search: (7.5M × 5ns) + (3.75M × 22 × 10ns) = 38ms + 825ms = 863ms/file × 66/8 = 7.1s additional. Net saving: ~20s per M8 query.
**Invariant:** Bitset pre-filter is conservative (no false negatives): if `pkBitset[pk>>6] & (1<<(pk&63)) == 0`, pk is guaranteed absent from `sortedPKs`. If bit=1, we still verify with `searchSortedUint32` (no false positives propagate to accumulation).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact`

## NOTE-120: IntrinsicFormatDeltaUint64 — histogram/agg scan functions handle large files
*Added: 2026-06-08*
**Decision:** Add `IntrinsicFormatDeltaUint64` to the format switch in `scanAggColHistogramCompact`, `countIntrinsicHistogramBoundaries`, and `streamAggColumnNoGroupBy`. The code body is identical to the `IntrinsicFormatFlat` case — just widen the `case` clause.
**Rationale:** When a flat column exceeds `IntrinsicPageSize = 10_000` rows, the writer switches from `encodeFlatColumn` to `encodeDeltaUint64Intrinsic` (NOTE-014). This resets the on-disk format byte from `IntrinsicFormatFlat (0x01)` to `IntrinsicFormatDeltaUint64 (0x04)`. Production 24h blockpack files always exceed 10K rows for `span:duration`, so `GetIntrinsicColumn("span:duration")` returns a column with `Format = IntrinsicFormatDeltaUint64`. The three functions above only matched `IntrinsicFormatFlat`, silently skipping all duration values on large files. The effect on M8: `countIntrinsicHistogramBoundaries` returned 0 → `actualStride = 1` → single histogram bucket; `scanAggColHistogramCompact` skipped all 7.5M duration refs → all spans ended up in the absent-row pass → all counts in boundary-0 bucket regardless of actual duration. The histogram appeared to work (correct group/time structure) but all duration values were collapsed into boundary 0.
**Invariant:** After decode, `IntrinsicFormatDeltaUint64` columns expose the same `Uint64Values` (sorted ascending) and parallel `BlockRefs` layout as `IntrinsicFormatFlat`. The ascending sort means BlockRefs are in value order (not packKey order), so binary search in `sortedPKs` is still required — there is no locality exploit available.
**Impact:** M8 `{span.kind = server} | histogram_over_time(duration) by (resource.service.name)` now returns correct histogram distributions. Computation adds `O(N_duration × log n_filtered)` binary searches per file (7.5M × log(3.75M) ≈ 165M comparisons; ~2s per file at L3 hit rate). Net M8 warm time: ~62s + ~2s overhead ≈ 64s (measured in prior session). Also fixes `{} | max_over_time(duration)` style N=0 agg queries on large files.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact,countIntrinsicHistogramBoundaries,streamAggColumnNoGroupBy`

## NOTE-115: bucketByPK []int16 — 75% memory reduction for direct-path arrays
*Added: 2026-06-08*
**Decision:** Change `bucketByPK` from `[]int64` to `[]int16` in all direct-path accumulation functions: `accumulateIntrinsicBucketsDirect`, `accumulateHistogramDirectN0`, and `streamByRefSliceHistogram`. Update all 6 function signatures that pass `bucketByPK` and all 12+ read/write/iterate sites.
**Rationale:** `bucketByPK[pk]` stores `timeBucketIndex+1` (sentinel: 0=absent) or `1..numSteps`. `numSteps` ≤ 1440 (24h/60s) fits in int16 (max 32767). Using int64 (8 bytes/entry) wasted 6 bytes/entry. With 16M entries at the `maxDirectArrayEntries` limit: 128MB (int64) → 32MB (int16), a 75% reduction. For `histogram/agg` paths that also allocate `dictByPK`: combined reduction from 192MB → 96MB. The smaller array significantly improves L3 cache hit rate for the `bucketByPK[pk]` random-access pattern (7.5M accesses per file), reducing cache miss latency.
**Invariant:** `bk+1 ≤ numSteps+1 ≤ 1441`. `int16` max is 32767. No overflow possible. All read sites convert `int64(bucketByPK[pk])` for arithmetic; range-iterate sites use `for pk, bk16 := range bucketByPK { bk := int64(bk16) }`. No semantic change — only memory layout.
**Impact:** Direct-path queries affected: M1 `{} | rate()`, M4 `{} | rate() by (service.name)`, M5 `{} | rate() by (span.kind)`, M7 `{} | max_over_time(duration) by (service.name)`, and histogram N=0/N=1 variants.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect,accumulateHistogramDirectN0,streamByRefSliceHistogram`

## NOTE-123: DeltaUint64 histogram scan — re-enabled with monotonic boundary detection
*Added: 2026-06-08*
**Decision:** Re-enable `IntrinsicFormatDeltaUint64` in `scanAggColHistogramCompact` (restoring NOTE-121 bitset pre-filter) and add a new DeltaUint64 case in `countIntrinsicHistogramBoundaries` using monotonic boundary-change detection.
**Rationale for revert of revert:** The previous revert (43caa8b8) was done in a session with heavy system instability (concurrent agents, Karpenter reshuffling). The HTTP500 attributed to "too slow" was actually OOM from `countIntrinsicHistogramBoundaries` returning `histFlatStride=64` (early exit) → `actualStride=65` → `groupCountsFlat=280×65×1440×8=209MB per goroutine × 8=1.67GB`. With the monotonic optimization, `countIntrinsicHistogramBoundaries` correctly returns the actual count (~20 for production durations 100μs-30s) → `groupCountsFlat=280×21×1440×8=67MB`, within GOMEMLIMIT=13GiB.
**Monotonic optimization:** DeltaUint64 values are sorted ascending. `intrinsicHistogramBoundary` is monotonically non-decreasing (pow(2, floor(log2(v/1e9)))). So boundary transitions can be detected by comparing consecutive values: O(numBoundaries) map insertions instead of O(numValues). For 7.5M values with 20 transitions: only 20 map lookups (vs 7.5M for the old Flat case code).
**scanAggColHistogramCompact DeltaUint64:** Restores the bitset pre-filter (NOTE-121). Bitset build from sortedPKs + check on each DeltaUint64 ref reduces binary searches by 50% at 50% selectivity. Combined with interpolation search (NOTE-122, already in place), DeltaUint64 scan cost: (7.5M × 5ns bitset) + (3.75M × 5 probes × 4ns search) ≈ 113ms per large file — acceptable.
**Queries fixed:** M8 `{span.kind = server} | histogram_over_time(duration) by (resource.service.name)` now returns correct histogram distributions. Before this fix, DeltaUint64 refs were skipped → all spans in boundary-0 (wrong). M8 warm 1h: 823ms (returns real histogram data). M8 24h cold: ~245s (I/O dominated).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact,countIntrinsicHistogramBoundaries`

## NOTE-124: groupCountsFlatPool — per-block flat accumulator array pooled to reduce GC pressure
*Added: 2026-06-08*
**Decision:** Pool the `numGroups × numSteps` (and `numGroups × stride1 × stride2`) `[]int64` flat accumulation arrays used by `streamCountRateN1CompactCore`, `streamHistogramN1CompactFromRefs`, `streamHistogramN1Compact`, and `accumulateCountRateDirect` using a `sync.Pool` (var `groupCountsFlatPool`) with `acquireGroupCountsFlat`/`releaseGroupCountsFlat` helpers.
**Rationale:** Warm multi-block queries process 100–400 blocks sequentially per goroutine. Each block allocates a `numGroups × numSteps × 8` byte array — ranging from 3.2 MB (rate, 281 groups, 1440 steps) to 67 MB (histogram, 281 groups, 20 boundaries, 1440 steps). Without pooling, 400 blocks × 67 MB = 26.8 GB of short-lived allocations per query, saturating the GC's tricolor marking bandwidth and causing stop-the-world pauses. The pool eliminates these allocations on the hot path; the GC collects pooled items at each cycle, so no permanent memory growth occurs. The `clear` in `acquireGroupCountsFlat` costs ~8 MB × 1 ns ≈ 8 ms but is required since the accumulator starts at zero and is never fully written.
**Acquire semantics:** If the pooled slice has sufficient capacity, reslice to `[:n]` and `clear`. Otherwise allocate fresh. Return full-capacity slice (`[:cap(s)]`) to pool on release so subsequent callers with smaller `n` can reuse without reallocation.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:groupCountsFlatPool,acquireGroupCountsFlat,releaseGroupCountsFlat`

## NOTE-125: compact-path per-block arrays pooled — sortedPKs, pkOrder, timeBucketByPos, dictIdxByPos, aggValByPos, aggPresentByPos, seenByPos
*Added: 2026-06-08*
**Decision:** Pool all per-block arrays allocated by compact-path functions (`streamCountRateN1Compact`, `streamCountRateN1CompactFromRefs`, `streamCountRateN1CompactCore`, `streamAggN1Compact`, `streamAggN1CompactFromRefs`, `streamHistogramN1Compact`, `streamHistogramN1CompactFromRefs`) using five typed `sync.Pool` vars with acquire/release helpers.
**Rationale:** At n=7.2 M in-range refs (typical large production file):
- `sortedPKs []uint32`: 28 MB per call, 6 allocation sites
- `timeBucketByPos []int32`: 29 MB per call, 6 allocation sites
- `pkOrder []uint64`: 57 MB per call (freed early after sort), 3 allocation sites
- `dictIdxByPos []uint32`: 28 MB per call, 7 allocation sites (including core)
- `aggValByPos []float64`: 57 MB per call, 4 allocation sites
- `aggPresentByPos []bool`: 7 MB per call, 4 allocation sites
- `seenByPos []bool`: 7 MB per call, 2 allocation sites

A 400-block query allocates: (28+29+57+28) × 400 × (count/rate) ≈ 57 GB of short-lived arrays per query, all in the multi-MB range — each one triggering a GC mark-sweep pass. Pooling these arrays eliminates the steady-state allocation pressure for warm queries, reducing GC CPU from ~15% to ~2% of query time.
**Pool design:** Five pools covering `[]uint32` (sortedPKs and dictIdxByPos share one pool — same type, same size), `[]int32` (timeBucketByPos), `[]uint64` (pkOrder), `[]float64` (aggValByPos), `[]bool` (aggPresentByPos and seenByPos). Acquire uses capacity-check reslice + `clear`; pkOrder acquire skips `clear` because callers fully overwrite before reading. Release returns `[:cap(s)]` so the next caller with different n can reuse. pkOrder is released at the end of its block scope (not via defer) to free 57 MB before the downstream `GetIntrinsicColumn` I/O calls.
**Correctness:** `timeBucketByPos`, `dictIdxByPos`, `aggPresentByPos`, `seenByPos` rely on zero-sentinel semantics (0 = absent/out-of-range). `clear` in acquire ensures these are reset. `sortedPKs` is fully overwritten by callers; `clear` is applied for safety and pattern consistency. `pkOrder` is fully overwritten; no `clear` needed.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateN1Compact,streamCountRateN1CompactFromRefs,streamCountRateN1CompactCore,streamAggN1Compact,streamAggN1CompactFromRefs,streamHistogramN1Compact,streamHistogramN1CompactFromRefs`

## NOTE-126: FullScan — lazy allMatchSet sentinel replaces eager []int allocation
*Added: 2026-06-08*
**Decision:** Change `blockColumnProvider.FullScan()` to return `allMatchRowSet(n)` instead of allocating and filling `[]int{0..N}` eagerly.
**Rationale:** Q1 `{}` (match-all) calls `FullScan()` once per block. For a 400-block query, this produced 400 × `make([]int, spanCount)` allocations before any guard checks. The `allMatchSet` sentinel satisfies `Size()`, `IsEmpty()`, and `Contains()` in O(1) with zero allocation; `ToSlice()` defers the `make` until materialisation is actually required. `collectMixedPlain` and `collectMixedTopK` call only `Contains()` — eliminating the allocation entirely on those paths. NOTE-107 scratch-lifetime invariant is unaffected.
Back-ref: `internal/modules/executor/column_provider.go:FullScan`

## NOTE-127: Match-all + limit fast path — ScanFlatColumnTopKRefs
*Added: 2026-06-08*
**Decision:** Add `collectMatchAllTopK` gated by `isMatchAllProgram` in `Collect`.
For `{}` with `MostRecent=true, Limit=N`, instead of `topKScanBlocks` (reads all blocks),
read only the `TimestampColumn` intrinsic blob (cached in `parsedIntrinsicCache`) and call
`ScanFlatColumnTopKRefs` to extract the top-N refs without decoding values. Then fetch only
the ~1-3 blocks containing those rows via `forEachBlockInGroups`.
**Why:** `hasSomeIntrinsicPredicates` returns false for match-all programs (no Nodes), so they
previously bypassed the intrinsic fast path entirely. `ScanFlatColumnTopKRefs` was effectively
unused on the match-all path — this wires it into production.
**Execution path:** `ExecPathMatchAllTopK = "match-all-topk"`.
**Fallback:** returns `errNeedBlockScan` if `GetIntrinsicColumnBlob` fails or `ScanFlatColumnTopKRefs`
returns nil (unsupported blob format). Full block scan proceeds normally in that case.
**Vector guard:** `isMatchAllProgram` returns false when `program.HasVector == true`, preventing
accidental activation for vector queries.
**Applies to:** trace queries (TimestampColumn == "span:start") and log queries (log:timestamp)
with an intrinsic section.
Back-ref: `internal/modules/executor/stream.go:collectMatchAllTopK,isMatchAllProgram`

## NOTE-128: mergeJoinFilteredRefsWithVals — idxPacked and filteredPKs pooled
*Added: 2026-06-08*
**Decision:** Replace `make([]uint64, len(inRangeRefs))` (`idxPacked`) and
`make([]uint32, len(filteredRefs))` (`filteredPKs`) in `mergeJoinFilteredRefsWithVals`
with `acquireCompactUint64` / `releaseCompactUint64` and `acquireCompactUint32` /
`releaseCompactUint32` respectively.
**Rationale:** `mergeJoinFilteredRefsWithVals` is called for every block in the M8
predicate-filtered path (`hasPreds=true`, `span.kind=server`). At N=7.2M in-range refs
(typical large production file), `idxPacked` costs ~57 MB per call and `filteredPKs`
costs ~15 MB per call. NOTE-125 pooled the structurally identical `pkOrder` scratch array
in the compact-path functions but missed `idxPacked` in `mergeJoinFilteredRefsWithVals`.
Both arrays are pure scratch (fully overwritten before any read, never returned to callers)
so they can share the existing `compactUint64Pool` and `compactUint32Pool` without adding
new pool variables. `idxPacked` skips `clear()` (matching `pkOrder` acquire semantics —
fully overwritten before `slices.Sort`). Both are released inline before `return` (not
via `defer`) to free 57+15 MB as early as possible. For a 400-block M8 query: eliminates
400 × (57+15) MB = 28.8 GB of short-lived allocations per query goroutine.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`

## NOTE-129: direct-path dense arrays pooled — bucketByPK, seenByPK, dictByPK
*Added: 2026-06-08*
**Decision:** Pool the direct-path dense arrays `bucketByPK []int16`, `seenByPK []bool`,
and `dictByPK []uint32` allocated in direct-path accumulation functions using two new
pools (`directInt16Pool`, `directBoolPool`) and the existing `compactUint32Pool`.
**Affected sites (9 total):**
- `buildDictIdxForRefs` line ~2229: `dictByPK []uint32` via `acquireCompactUint32`; release
  owned by `dispatchIntrinsicAccumulate` caller with size guard (cap <= 16_000_001).
- `accumulateIntrinsicBucketsDirect` line ~2370: `bucketByPK []int16` via `acquireDirectInt16` + defer.
- `accumulateIntrinsicBucketsDirect` line ~2410: `dictByPK []uint32` via `acquireCompactUint32` + defer (conditional).
- `accumulateHistogramDirect` line ~2619: `seenByPK []bool` via `acquireDirectBool` + inline release before emit.
- `accumulateHistogramDirectN0` line ~2682: `bucketByPK []int16` via `acquireDirectInt16` + inline release before emit.
- `accumulateHistogramDirectN0` line ~2718: `seenByPK []bool` via `acquireDirectBool` + inline release before emit.
- `accumulateAggDirect` line ~2769: `seenByPK []bool` via `acquireDirectBool` + defer.
- `streamByRefSliceHistogram` line ~3191: `bucketByPK []int16` via `acquireDirectInt16` + inline release before emit.
- `streamByRefSliceHistogram` line ~3248: `seenByPK []bool` via `acquireDirectBool` + inline release before emit.
**Rationale:** Per-block allocations for M8 histogram queries (400 blocks):
- `bucketByPK []int16`: 32 MB × 400 = 12.8 GB per query
- `seenByPK []bool`: 16 MB × 400 = 6.4 GB per query
- `dictByPK []uint32`: 64 MB × 400 = 25.6 GB per query (histogram/agg paths)
Total eliminated: up to 44.8 GB of short-lived allocations per M8 histogram query.
Expected improvement: 15-25% for M8 histogram, 10-20% for M4 rate (bucketByPK only).
**Pool design:** `acquireDirectInt16`/`acquireDirectBool` follow the identical pattern to
`acquireCompactUint32`/`acquireCompactBool` (NOTE-125): capacity-check reslice + `clear`.
`clear` is mandatory for all three types — zero-sentinel semantics require unwritten PKs to
stay at their zero value. Release returns `[:cap(s)]` for reuse at different sizes.
**Correctness:** `clear` in acquire covers both: (a) PKs not written in this call that must
remain 0 for sentinel checks, and (b) stale values from the previous pool user.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildDictIdxForRefs,
accumulateIntrinsicBucketsDirect,accumulateHistogramDirect,accumulateHistogramDirectN0,
accumulateAggDirect,streamByRefSliceHistogram`

## NOTE-130: mergeJoinFilteredRefsWithVals — outRefs and outVals pooled
*Added: 2026-06-08*
**Decision:** Replace `make([]modules_shared.BlockRef, 0, outCap)` (`outRefs`) and
`make([]uint64, 0, outCap)` (`outVals`) in `mergeJoinFilteredRefsWithVals` with
`acquireCompactBlockRef` / `releaseCompactBlockRef` and the existing `acquireCompactUint64`
/ `releaseCompactUint64` respectively. Adds `release func()` as a third return value;
the single call site (`executeTraceMetricsIntrinsic`) releases after `dispatchIntrinsicAccumulate` returns.
**Rationale:** Extends NOTE-128 (which pooled idxPacked/filteredPKs in the same function)
to cover the output slices. At outCap ≈ 3.5M (F ≤ N/2 at 50% selectivity for `span.kind=server`),
`outRefs` costs ~14 MB per call and `outVals` costs ~28 MB per call. For a 400-block M8 query:
eliminates 400 × 42 MB = 16.8 GB of short-lived allocations per query. Unlike idxPacked/filteredPKs
(which are pure scratch released before return), outRefs/outVals are returned to the caller and
consumed by `dispatchIntrinsicAccumulate`. The `release func()` return value delegates cleanup to
the call site, which is the only consumer of these slices. A no-op closure is returned when the
function exits early (empty inputs) so the call site needs no nil check.
New pool: `compactBlockRefPool` (mirrors `compactUint64Pool` but for `[]modules_shared.BlockRef`).
No `clear()` on acquire — callers use `[:0]+append`, so all positions are overwritten before read.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`

## NOTE-131: maxDirectCountRateEntries = 8M — compact path for count/rate when bucketByPK > L3 cache
*Added: 2026-06-08*
**Decision:** In `accumulateIntrinsicBucketsDirect`, add a threshold `maxDirectCountRateEntries = 8_000_000` for count/rate. When `maxPK > 8M`, return `false` to trigger `streamCountRateN1Compact` instead of proceeding with `accumulateCountRateDirect`.
**Rationale:** `bucketByPK` is `[]int16` (NOTE-115), costing `(maxPK+1)×2` bytes — 16MB at 8M entries, 32MB at 16M entries. The access pattern in `accumulateCountRateDirect` is random (one lookup per span ref in the group-by dict scan, scattered across 32MB), causing L3 thrashing and DRAM fetches at 300M+ probes per file. The compact path (`streamCountRateN1Compact`, NOTE-108) uses `sortedPKs` sized to n×4 bytes (n = in-range refs, not maxPK); for M4 with 3.5M in-range refs, `sortedPKs` = 14MB (L3-resident). Binary search in a 14MB L3-resident array is faster than random DRAM reads into 32MB `bucketByPK`. Previously, count/rate was excluded from the `maxDirectAggEntries` guard (NOTE-117) because it avoids `dictByPK` allocation — but `bucketByPK` itself is the bottleneck at high maxPK.
**Threshold 8M:** `bucketByPK` = 16MB ≤ typical L3 (24-30MB) → direct path OK. Beyond 8M entries, the 16MB+ `bucketByPK` competes with `groupCountsFlat` (3-4MB) and the group-by column data for L3 capacity, causing evictions. The compact path's `sortedPKs` scales with n (in-range refs), not maxPK, so it stays L3-resident even on large files.
**Invariant:** `streamCountRateN1Compact` produces identical results to `accumulateCountRateDirect` — same absent-row semantics, same bucket emission (NOTE-108). This is a pure dispatch optimization, not a correctness change. A wrong threshold only affects performance.
**Impact:** M4 `{} | rate() by (resource.service.name)` on large production files (maxPK ≈ 16M): estimated 60-70% reduction (13728ms → ~4000-5500ms) by eliminating DRAM-bound random reads from the hot accumulation loop.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect`

## NOTE-132: streamHistogramN1Compact compact fallback for no-predicate N=1 histogram
*Added: 2026-06-08*
**Decision:** In `dispatchIntrinsicAccumulate`, replace the `accumulateIntrinsicBucketsViaKeyMap` fall-through for the `filteredRefs == nil && len(agg.GroupBy) == 1 && agg.Function == HISTOGRAM` case with a direct call to `streamHistogramN1Compact`.
**Rationale:** The no-predicate N=1 switch block already has compact fallbacks for count/rate (NOTE-108: `streamCountRateN1Compact`) and general agg (NOTE-109: `streamAggN1Compact`). Histogram was the only function type still falling through to `accumulateIntrinsicBucketsViaKeyMap`, which allocates `keyToBucket map[uint32]int64` sized to `len(inRangeRefs)` (~3 GB for a 24h M8 file with 7.2M in-range refs) before calling `streamHistogramN1Compact` anyway. The compact path requires two computed values (`numSteps`, `groupByCol`) both already computed by `accumulateIntrinsicBuckets` (line 1813/1817) under the same conditions — the pattern is identical.
**Correctness:** `streamHistogramN1Compact` is already battle-tested on this input shape via `accumulateIntrinsicBuckets` (line 1827, NOTE-092). `inRangeRefs`/`inRangeVals` from the no-predicate path are timestamp-ordered (not packKey-sorted), which is handled correctly — `streamHistogramN1Compact` performs its own pkOrder sort at line 1427–1431. The `groupByCol` nil check inside `streamHistogramN1Compact` (line 999 equivalent) handles missing columns safely. `numSteps <= 0` guard is added to match the pattern used by `streamAggN1Compact` (line 955) and `streamCountRateN1Compact` (line 752).
**Impact:** M8 `{span.kind = server} | histogram_over_time(duration) by (resource.service.name)` — for files where `accumulateIntrinsicBucketsDirect` returns false (maxPK > maxDirectAggEntries = 4M), eliminates the 3 GB `keyToBucket` allocation per goroutine per file. At 400 files × 8 goroutines, this removes up to 9.6 TB of heap churn, reducing GC pause time and peak RSS.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:dispatchIntrinsicAccumulate`

## NOTE-133: maxDirectCountRateEntries lowered 8M → 4M — tighter L3 budget for co-resident arrays
*Added: 2026-06-08*
**Decision:** Lower `maxDirectCountRateEntries` from `8_000_000` to `4_000_000` in
`directAggExceedsL3Threshold`. Files with maxPK in [4M, 8M) now route to
`streamCountRateN1Compact` instead of `accumulateCountRateDirect`.
**Rationale:** NOTE-131 justified 8M on the assumption that `bucketByPK` (16MB) fits
in L3 (24–30MB). That analysis did not account for the two co-resident working sets
present during the same accumulation loop: `groupCountsFlat` (3–4MB for ~280 groups ×
1440 steps, pooled but in active use) and the group-by column blob (~0.5–2MB per file,
held in the reader cache). The effective L3 budget for `bucketByPK` is therefore ~20MB,
not 24MB. At 8M entries, `bucketByPK` = 16MB already causes L3 competition and
eviction-driven DRAM reads on the random-access inner loop of `accumulateCountRateDirect`.
At 4M entries, `bucketByPK` = 8MB — comfortably L3-resident alongside both co-residents.
Production files with maxPK in [4M, 8M) (the typical range for 2000 rows/block × 2–4
block layers) now take the compact path: `sortedPKs` at n×4 bytes (n = in-range refs,
typically 3–3.5M) = 12–14MB, also L3-resident. Binary search over a 14MB L3-resident
array is faster than random probes into a 16MB `bucketByPK` that is competing with
co-residents for L3 ways.
**Threshold alignment:** 4M matches `maxDirectAggEntries` (NOTE-117), making the direct
path boundary uniform across all function types (count/rate, agg, histogram). The
previous 8M was the outlier; this corrects it.
**Invariant preserved (NOTE-131):** `streamCountRateN1Compact` produces identical
results to `accumulateCountRateDirect`. A wrong threshold only affects performance, never
correctness. The change is safe to revert if production profiling shows no improvement.
**Expected impact:** 15–25% improvement on M4 (`{} | rate() by (resource.service.name)`,
baseline ~10500ms after r131). Files in the [4M, 8M) maxPK band (which represent the
common production file size) now avoid the DRAM-bound random-access inner loop.
See NOTE-131 for original threshold rationale.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:directAggExceedsL3Threshold`

## NOTE-134: scanAggColHistogramCompact pkBitset pooled — 800MB per M8 query eliminated
*Added: 2026-06-08*
**Decision:** Replace `make([]uint64, (maxPK>>6)+1)` for `pkBitset` in
`scanAggColHistogramCompact` with `acquireCompactUint64(n)` + `clear(pkBitset)`,
released inline at the end of the `IntrinsicFormatDeltaUint64` case block.
**Rationale:** `pkBitset` is a 2MB scratch buffer allocated once per block for M8 histogram
queries routed to the DeltaUint64 path. At 400 blocks × 2MB, this is 800MB of short-lived
heap per query. `compactUint64Pool` already pools `[]uint64` (NOTE-125/128/130); this extends
the same pool to cover `pkBitset`.
**Clear requirement:** `pkBitset` uses zero bits as the "absent" sentinel. Stale set bits
from a previous pool user would cause false-negative pre-filters, silently skipping valid
refs and producing wrong results. `clear(pkBitset)` is mandatory after acquire.
`acquireCompactUint64` is not modified; caller performs `clear` explicitly.
**Release placement:** Inline at end of the Flat/DeltaUint64 case block, guarded by
`if pkBitset != nil` to handle the Flat sub-path where the bitset is never allocated.
**Expected impact:** Eliminates 800MB of short-lived allocations per M8 histogram query;
reduces GC trigger frequency and pause time for warm repeated queries.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact`

## NOTE-135: Bitset pre-filter in scanGroupByColCompact and scanAggColHistogramCompact Dict paths
*Added: 2026-06-08*
**Decision:** Add a pkBitset pre-filter to the `IntrinsicFormatDict` case in both
`scanGroupByColCompact` and `scanAggColHistogramCompact`, using the same pool and pattern
as NOTE-123/134 (DeltaUint64 case in `scanAggColHistogramCompact`).
**Rationale:** The Dict inner loop iterates all `entry.BlockRefs` for each DictEntry and
calls `searchSortedUint32` (interpolation search, 3–5 probes) per ref. Dict refs within an
entry are not packKey-sorted, so there is no spatial locality with `sortedPKs`. At 50%
selectivity (M8: `kind=server` group by `resource.service.name`), ~50% of in-range Dict refs
fail the interpolation search — an estimated 3.6M failed lookups per block. Each costs
~15–20ns (3–5 cache-missing probes on a 3.75M-entry array). A 2MB bitset pre-filter
(one bit per packKey up to maxPK, L3-resident) eliminates these at ~2ns per rejected ref.
**Math:** 3.6M refs × 13–18ns saved × 400 blocks ≈ 18–28 seconds per M8 query. The
bitset build costs: N/64 uint64 writes + N index writes = ~3.75M writes ≈ 1µs per block,
negligible.
**Correctness:** False positives (bit set for a pk not in sortedPKs) are impossible by
construction. False negatives (bit cleared for a pk in sortedPKs) are impossible if
`clear(pkBitset)` is called before setting bits. The `searchSortedUint32` call is still
present as the authoritative check — the bitset is a pre-filter only.
**Clear requirement:** Same as NOTE-134. `clear(pkBitset)` is mandatory; `acquireCompactUint64`
does not clear.
**Two independent bitsets:** `scanAggColHistogramCompact` now has two pkBitset defers — one
for Dict (NOTE-135) and one for DeltaUint64 (NOTE-134). Both use the same pool; both are
released on function exit. Release order (LIFO) does not matter for pool puts.
**Expected impact:** 5–12% M8 improvement (M8 = histogram group-by with predicate). Minor
benefit to M4/M7 (those call `scanGroupByColCompact` for the group-by column scan).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanGroupByColCompact`,
`internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact`

## NOTE-136: streamCountRateN0HashFilter — replace filteredPKs map with pkBitset
*Added: 2026-06-08*
**Decision:** Replace `make(map[uint32]struct{}, len(filteredRefs))` + map lookup in
`streamCountRateN0HashFilter` with the pkBitset pattern (same as NOTE-134/135).
Build `pkBitset` from `filteredRefs` packKeys (not from `sortedPKs`), then use the
bitset check `pkBitset[pk>>6]&(uint64(1)<<(pk&63))==0` as the membership test in the
sequential `tsCol` scan.
**Rationale:** `filteredPKs` map[uint32]struct{} allocates ~24B/entry. For M2-style queries
(`{service.name="grafana"} | rate()`) with 500K filtered refs, this is ~12 MB per file.
At 400 files per query, this totals ~4.8 GB of short-lived heap allocations per query,
generating significant GC pressure. A bitset at `(maxPK>>6)+1` uint64s is ~2 MB per file
(for maxPK=16M), 6x smaller, pool-reusable, and eliminates map GC overhead entirely.
**Queries affected:** Only N=0 no-group-by count/rate with selective predicate (M2 pattern).
M6/M9 (N=1 group-by) and M8 (histogram) never enter this function.
**maxPK source:** Computed from filteredRefs. Single O(F) pass; F ≤ N/4 so always cheap.
**Clear requirement:** Same as NOTE-134/135. `clear(pkBitset)` mandatory after acquire.
**Edge case — maxPK==0:** Guard is `len(filteredRefs) > 0` (not `maxPK > 0`) to correctly
handle packKey=0 (blockIdx=0, rowIdx=0). When maxPK=0, bitset size is 1 uint64 (8 bytes).
**Expected impact:** 15-25% improvement for M2-style queries.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateN0HashFilter`

## NOTE-139: scanAggColHistogramCompact — POPCNT rank index replaces searchSortedUint32 in DeltaUint64/Flat scan
*Added: 2026-06-08*
**Decision:** After building `pkBitset` from `sortedPKs` in the `IntrinsicFormatFlat /
IntrinsicFormatDeltaUint64` case of `scanAggColHistogramCompact`, also build a parallel
`rankPrefix []uint32` where `rankPrefix[i]` = cumulative popcount of `pkBitset[0..i-1]`.
In the scan loop, replace `searchSortedUint32(sortedPKs, pk)` with the O(1) formula:
`pos = int(rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word] & ((1<<bit)-1))))`
where `word = pk>>6`, `bit = pk&63`. The pkBitset membership check (`pkBitset[word]&(1<<bit)!=0`)
guarantees pk is present — no `found` check needed.
**Rationale:** With NOTE-122 interpolation search (~5 probes), the DeltaUint64 scan costs
3.75M × 5 × ~10ns ≈ 187ms per block. At 50 blocks/goroutine: ~9.4s per M8 query on this
path alone. The O(1) rank reduces to 3.75M × ~4ns = 15ms per block → ~0.75s total: ~12x
faster on the DeltaUint64 path. Expected M8 warm improvement: ~30%.
**Build cost:** O(maxPK/64) ≈ 250K iterations per block, ~0.5µs. Total for 400 blocks: 0.2ms.
**Memory:** `rankPrefix` is `(maxPK>>6)+2` uint32s ≈ 1 MB at maxPK=16M, pooled via
`compactUint32Pool` (NOTE-125). No new pool needed. Peak per-goroutine: +1 MB vs existing 28 MB.
**`math/bits` import:** `bits.OnesCount64` compiles to a single POPCNT instruction on AMD64/ARM64.
Import `"math/bits"` added to `metrics_trace_intrinsic.go`.
**Guard change:** pkBitset/rankPrefix build guard changed from
`col.Format == IntrinsicFormatDeltaUint64 && maxPK > 0` to `len(sortedPKs) > 0`
(NOTE-136 pattern) to correctly handle packKey=0 (maxPK=0) and both column formats.
**Dict path:** Left with searchSortedUint32 + NOTE-135 bitset pre-filter. Dict is not the
M8 CPU bottleneck (span:duration is DeltaUint64 on large files). Rank extension to Dict
and scanGroupByColCompact deferred to NOTE-140.
**Validated by:** `TestRankIndexCorrectness` — exhaustive formula check against searchSortedUint32
for boundary cases (pk=0, bit=0, bit=63, word boundary, dense, sparse).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact`

## NOTE-140: POPCNT rank index extended to Dict paths in scanGroupByColCompact and scanAggColHistogramCompact
*Added: 2026-06-08*
**Decision:** Extend NOTE-139's POPCNT rank approach to the remaining `searchSortedUint32`
call sites in Dict and Flat/XORBytes/DeltaUint64 paths: `scanGroupByColCompact` (Dict case,
Flat/XORBytes/DeltaUint64 case) and `scanAggColHistogramCompact` Dict case. In each function,
build `rankPrefix []uint32` alongside the existing `pkBitset []uint64` using the same O(maxPK/64)
sweep. Replace all `searchSortedUint32(sortedPKs, pk)` calls in the inner loops with:
`r = rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word] & ((1<<bit)-1)))`, `pos = int(r)`.
Also adds a bitset pre-filter to the Flat/XORBytes/DeltaUint64 case of `scanGroupByColCompact`
(which previously had no pre-filter).
**Rationale:** `resource.service.name` is IntrinsicFormatDict with ~200 entries and ~18750 refs
each. At 50% selectivity: 1.875M rank lookups × 46ns saved (binary search → O(1)) ≈ 86ms per
block. At 400 blocks / 8 goroutines = 50 blocks per goroutine: ~4.3s saved on M4/M8/M9.
Expected: 10–20% M4 improvement, 5–15% M8/M9 improvement.
**No signature changes:** rankPrefix built independently inside each function (same as pkBitset).
Avoids touching 5 `scanGroupByColCompact` call sites and 3 `scanAggColHistogramCompact` call sites.
**Guard:** `scanGroupByColCompact` guard changed from `maxPK > 0` to `len(sortedPKs) > 0` to
align with NOTE-139 pattern and correctly handle packKey=0 edge case.
**Pool:** `acquireCompactUint32(n+1)` / `defer releaseCompactUint32` — same pool as NOTE-139.
Per-goroutine overhead +1 MB (sequential calls, only one rankPrefix alive at a time per function).
**Out of scope:** `scanAggColCompact` (min/max/sum/avg), which has no pkBitset today — NOTE-141.
**Validated by:** `TestRankIndexCorrectness` (existing) + `go test ./internal/modules/executor/...`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanGroupByColCompact`,
`internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact`

## NOTE-141: scanAggColCompact — POPCNT rank index + pkBitset pre-filter replaces searchSortedUint32
*Added: 2026-06-08*
**Decision:** Apply the NOTE-139/140 pattern to `scanAggColCompact` (min/max/sum/avg-by-group
aggregate scan), the last `searchSortedUint32` caller on the hot metrics path. Build
`pkBitset []uint64` + `rankPrefix []uint32` from `sortedPKs` once (O(maxPK/64) sweep) at the
top of the function, then in both the Dict and Flat/DeltaUint64 inner loops pre-filter
non-member refs with `pkBitset[pk>>6]&(1<<(pk&63))==0` and replace
`searchSortedUint32(sortedPKs, pk)` with the O(1) rank formula
`pos = int(rankPrefix[pk>>6] + uint32(bits.OnesCount64(pkBitset[pk>>6] & ((1<<(pk&63))-1))))`.
**Rationale:** The live Pyroscope profile (querier process_cpu, 19:00–20:00Z) shows
`scanAggColCompact`'s `searchSortedUint32` is the #2 CPU node at ~24.8% of total querier CPU —
the single biggest remaining search-bound sink after NOTE-139/140 removed it from
`scanGroupByColCompact` and `scanAggColHistogramCompact`. The aggregate column refs (Flat/DeltaUint64
sorted by value, Dict refs unsorted within an entry) have no locality with `sortedPKs`, so binary
search costs 3–5 cache-missing probes per ref; the bitset rejects non-members at ~2ns and the rank
lookup is O(1). NOTE-140 turned the identical swap into M4 -72% / M9 -89%.
**Write semantics preserved:** This function has NO `bk==0`/`seen` logic (unlike
`scanAggColHistogramCompact`); it only writes `aggValByPos[pos]=fval` and
`aggPresentByPos[pos]=true`. Parse failures still mark present with fval=0 (Dict case computes
`fval` before the ref loop, unchanged). The bitset pre-filter rejects exactly the refs the old
`searchSortedUint32` returned `found=false` for; for found refs the rank `pos` equals the binary-search
`pos` (`TestRankIndexCorrectness`), so all writes are byte-identical.
**No signature change:** `pkBitset`/`rankPrefix` built inside the function (same as NOTE-140);
both call sites (lines 1110, 1236) unchanged.
**Pools / clear:** `acquireCompactUint64(n)` + `defer releaseCompactUint64` for pkBitset (with
mandatory `clear(pkBitset)` — zero-bit membership sentinel, acquireCompactUint64 does not clear);
`acquireCompactUint32(n+1)` + `defer releaseCompactUint32` for rankPrefix (cleared internally).
Both defers cover the single normal return — this function has no ctx-cancel mid-loop return.
**Validated by:** `TestRankIndexCorrectness` (existing — covers the formula) +
`go test ./internal/modules/executor/...`.
**Expected impact:** M7 (`max_over_time by svc`, 16.5s baseline) -40–70%, plus all
min/max/sum/avg-by-group queries that route through `scanAggColCompact`. Directly attacks the
#2 profile node.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColCompact`

## NOTE-142: histogram flat-emit reuses pooled []byte composite-key scratch
**Date:** 2026-06-08

`streamByRefSliceHistogramFlatEmit` built the composite key with `strconv.FormatInt(timeIdx,10)`
+ a 3-way string concat per non-zero cell. For M8
(`{span.kind=server} | histogram_over_time(duration) by (resource.service.name)`) this is hundreds of
millions of short-lived string allocations across ~400 files (≈1583 active series × up to 1440 steps),
GC-bound and warm-cache-insensitive — the dominant cost of the 157s M8 baseline.

Replaced with a pooled `*[]byte` scratch (`acquireCompositeKeyScratch`, NOTE-067) built via
`strconv.AppendInt` + `append`. The map lookup uses `buckets[string(*scratch)]` so the conversion is
elided on hit, and the key string is allocated only on a genuine map miss (NOTE-073). The
`intrinsicGetOrCreateBucket` helper is inlined here because it takes a `string` (which would force a
per-cell allocation, defeating the optimization); the inlined miss-path matches the helper's exact
`aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}` initialization.

Keys are byte-identical to the previous format (`timeIdx\x00gk\x00boundaryStr`), so
`traceHistogramSeries` parsing (metrics_trace.go:617) and all bucket counts are unchanged. Does NOT
alter the scan path (NOTE-114/135/139/140), the absent-row pass (NOTE-088 bIdx=0 sentinel), the
`discardStride` clamp, the `groupCountsFlat` flat layout (NOTE-124), or boundary math.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamByRefSliceHistogramFlatEmit`

## NOTE-143: scanAggColHistogramCompact parallelized across min(NumCPU,8) workers (M8)
*Added: 2026-06-09*

**Decision:** Shard the histogram aggregate-column scan in `scanAggColHistogramCompact` across
`min(runtime.NumCPU(), 8)` goroutines. The shared, immutable post-build inputs
(`col.DictEntries`/`col.BlockRefs`/`col.Uint64Values`, `sortedPKs`, `timeBucketByPos`,
`dictIdxByPos`, and the `pkBitset`+`rankPrefix` POPCNT index from NOTE-139/140) are read
concurrently. The two written structures — `groupCountsFlat` (NOTE-124 pool) and `seenByPos`
(NOTE-125 pool) — get a private per-worker copy from those same pools, then are sum-reduced (`+=`)
and OR-reduced after `wg.Wait()`. The driver builds `pkBitset`/`rankPrefix` ONCE and shares them
read-only, eliminating the duplicate per-branch build that existed in the Dict and Flat/Delta arms.

**Why safe:** NOTE-048's "ParseBlockFromBytes not goroutine-safe on the same *Reader" covers block
*decode*; this scan touches no Reader parse state — the intrinsic column is already materialized
and cached read-only. Every loop input is immutable after build; only the two accumulators are
written, and they are per-worker.

The one mutable shared state is the caller's `getBoundaryIdx` closure, which appends to a shared
`boundaryCache`/`boundaries` and assigns boundary indices in first-encounter order (the `boundaries`
slice is read by the emit step, so the order is load-bearing — this is a correctness hazard, not
just a -race hazard). In the parallel path the driver runs `buildFrozenBoundaryIdx` SERIALLY before
any goroutine starts: it walks the column in legacy scan order **applying the SAME per-row filters
the scan applies** (pk range + pkBitset membership via the shared `histRefPassPos` helper, then
`timeBucketByPos[pos] != 0`) and drives the original closure ONLY for rows that pass — so the
caller's `boundaries`/`boundaryCache` end up identical to a serial run (emit unchanged), then
snapshots a frozen `map[float64]int64`. Workers use a read-only lookup over that frozen map — no
concurrent map write, clean under -race, and (since pre-warm visits every PASSING value the scan
will reach) it never falls back, so every `bIdx` equals the legacy closure's. Hence parallel output
== serial output. **The filter is load-bearing (C1):** a serial scan appends to `boundaries[]` in
first-encounter order of PASSING rows only; recording boundaries for filtered-out rows would corrupt
that order and produce wrong M8 output for every real filtered query (e.g. `{span.kind=server}`
restricts via `sortedPKs`/`pkBitset`). To keep the pre-warm's pk/pos computation byte-identical to
the scan's, both call the shared `histRefPassPos` helper so they cannot drift.

**Pre-warm cost:** `buildFrozenBoundaryIdx` must not negate the parallel win with a serial O(N)
transcendental pass (`intrinsicHistogramBoundary` is Log2+Floor+Pow). For DeltaUint64 it applies the
proven monotonic technique from `countIntrinsicHistogramBoundaries` (NOTE-123), now over PASSING rows
only: the column is value-sorted ascending and the boundary function is monotonic non-decreasing, so
among the (still ascending) passing rows the boundary is non-decreasing — `getBoundaryIdx` is driven
only when a passing row's boundary differs from the previously-recorded passing row's boundary
(~numBoundaries calls, not N). `getBoundaryIdx` is idempotent for an already-seen boundary, so this
yields a `boundaries[]` slice byte-identical to driving the closure on every passing row. The Flat
arm is NOT value-sorted, so it walks every PASSING value in order to preserve first-encounter
ordering exactly. The Dict arm is left unfiltered: the legacy Dict scan calls `getBoundaryIdx` once
per ENTRY before its inner ref loop, unconditionally, so recording per-entry in order is already
faithful (and Dict entries are few — cheap).

**Serial fallback (no fast-path regression):** when `NumCPU<=1` or `numItems < 65536`
(`histParallelMinItems`) the function runs the legacy single-threaded shard inline
(`scanAggColHistogramShard` over the full range), writing directly into the caller's
`groupCountsFlat`/`seenByPos` and using the original `getBoundaryIdx` closure — byte-identical to
the pre-NOTE-143 path.

**Regression confinement:** only M8 (`histogram_over_time`) reaches this function — its sole two
callers are `streamHistogramN1CompactFromRefs` and `streamHistogramN1Compact`. Rate (M9/M10) uses
`streamCountRateN1Compact*` → `scanGroupByColCompact`; general agg / M4 uses `scanAggColCompact`
(NOTE-141). Neither touches `scanAggColHistogramCompact`, so they are structurally unaffected.

**Signature:** added trailing `fieldName string` (callers pass `agg.Field`) so the parallel path can
build the read-only boundary lookup via `intrinsicHistogramBoundary`. No behavior widening.

**Worker count:** `min(NumCPU, 8)` — 8 mirrors `defaultPipelineWorkers` (NOTE-058) as the cap. This
scan is CPU-bound, and the intrinsic path is otherwise single-threaded, so workers consume idle
cores rather than oversubscribing.

**Validated by:** `TestHistParallelEquivalence_DeltaUint64/_Flat/_Dict` (all-pass inputs) and
`..._DeltaUint64_Filtered/_Flat_Filtered/_Dict_Filtered` (a meaningful fraction of rows filtered out
via pks absent from `sortedPKs` and `timeBucketByPos[pos]==0`) — all force the parallel path
(numItems ≥ 65536) and assert parallel == serial for groupCountsFlat, seenByPos, AND boundaries,
under `go test -race`. The filtered tests fail before the C1 filter fix and pass after.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramCompact,scanAggColHistogramShard,buildFrozenBoundaryIdx,histRefPassPos`

## NOTE-148: scanGroupByColCompact Dict path parallelized across min(NumCPU, 8) workers (M4/M6/M9/M10)
*Added: 2026-06-09*

**Decision:** Shard the Dict-format group-by scan in `scanGroupByColCompact` across
`min(runtime.NumCPU(), histParallelWorkers)` goroutines. The rate-by-group queries
(`{...} | rate() by (resource.service.name | span.http.request.method)` — M4/M6/M9/M10) group by
low-cardinality columns: a handful of `DictEntries` but millions of `BlockRefs`. NOTE-143 only
parallelizes the histogram scan, and its Dict path gates on *entry count* (≥65536 entries), so a
245-service column never parallelized anywhere before this. The work here is in the REFS, so we shard
the per-ref inner loop by ref count, not entry count.

**Structure (driver = `scanGroupByColCompact`, helper = `scanGroupByColCompactDictParallel`):** the
driver builds the read-only `pkBitset`/`rankPrefix` POPCNT index once (shared) and extracts the serial
body into `scanGroupByColCompactSerial`.
  - **Phase A (parallel):** workers take contiguous `DictEntries` ranges balanced by *cumulative ref
    count* (even-count chunking would hand one ref-heavy entry to one worker). For each owned entry
    with a non-empty value, write `entryIdx+1` into `dictIdxByPos[pos]` for every passing ref and
    record `entryPassed[e]`.
  - **Phase B (serial, O(numEntries)+O(n)):** build the group `dict` in entry order and translate the
    `entryIdx+1` markers in `dictIdxByPos` to the final `dictIdx+1`.

**Why race-free without per-worker copies + reduction (unlike NOTE-143):** each in-range span
position is owned by exactly one ref (a span has one value per column), so the `dictIdxByPos[pos]`
writes are disjoint across workers — no accumulator to reduce. Contiguous entry ranges mean each entry
is owned by one worker, so the per-entry `entryPassed[e]` writes are disjoint too. `pkBitset`,
`rankPrefix`, `col.DictEntries`, `intrinsicInt64ColToString`, and `packKey` are all read-only/pure.

**Why output is byte-identical:** the serial path assigns dict indices in first-passing-ref encounter
order; since it iterates `DictEntries` sequentially and assigns on the first passing ref, that order
IS entry order — exactly what Phase B reproduces. Only entries with a passing ref get a slot (Phase B
skips `!entryPassed`), so fully-filtered groups never enlarge `groupCountsFlat` — important for the
OOM-sensitive histogram group-by callers (`streamHistogramN1Compact*`). Proven by
`intrinsic_groupby_parallel_test.go`: full, filtered (~2/3 refs rejected), int64-value, and 3-group
ref-heavy-skew cases all assert `dict` AND `dictIdxByPos` are byte-identical to the serial path under
`go test -race`, plus a dispatcher test through the public `scanGroupByColCompact`.

**Gate — totalRefs ≥ 2·len(sortedPKs):** the per-ref work is memory-bandwidth-bound (range/bitset
filter + one array write), NOT CPU-bound like NOTE-143's per-value `Log2`/`Pow` boundary math.
Parallelism only pays when the column walks meaningfully more refs than it writes — i.e. the file
spans more time than the query window (a 2h window over a ~24h file walks ~12× the in-range refs). At
full coverage (totalRefs ≈ n) the goroutine + per-position-write contention + the O(n) translation
pass make it a net regression, so the driver stays serial. The Flat path stays serial unconditionally
(Flat group-by is high-cardinality and rare).

**Measurement (microbench, authoritative for a parallelism change — cluster wall-clock is I/O/cache
bound):** `BenchmarkScanGroupByColCompact` on 20 cores, 250 groups × 12000 refs (3M total):
full-coverage stays serial (4.6 ms serial vs 5.8 ms if forced parallel — gated out); wide-file
2×→2.0×, 4×→1.8×, 12×→2.7× faster (5.65 ms → 2.10 ms at 12×).

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanGroupByColCompact,scanGroupByColCompactSerial,scanGroupByColCompactDictParallel`

## NOTE-149: unfiltered N=0 count/rate via boundary binary search, not per-span loop (M1)
*Added: 2026-06-09*

**Decision:** For the unfiltered `{} | rate()` shape (no predicate, no group-by — M1), replace
`streamCountRateNoGroupBy`'s O(N) per-span `timeBucketIndex` loop (~150M iterations for M1, NOTE-068)
with `streamCountRateNoGroupBySorted`: one `sort.Search` per bucket boundary, O(numSteps·log N).

**Why it is correct:** `span:start` is a flat ascending-sorted intrinsic column (the same property
the lo/hi `sort.Search` range-narrowing in `executeTraceMetricsIntrinsic` already relies on), and
`timeBucketIndex(ts)=(ts-StartTime-1)/step` is monotonically non-decreasing in `ts`. So in-range
timestamps fall into contiguous, non-overlapping runs — one run per bucket. The count for bucket `b`
is the number of timestamps in `(StartTime+b·step, StartTime+(b+1)·step]`; the inclusive upper bound
is `StartTime+(b+1)·step`, and `val > boundary` places `val==boundary` into bucket `b` (right-closed),
matching `timeBucketIndex` exactly. A single `sort.Search` over the remaining sorted tail finds each
run boundary; `prev` advances monotonically so the searches shrink. Callers guarantee every value is
in `(StartTime, EndTime]` via the lo/hi binary search, so all timestamps land in `[0,numSteps)` with
no clamping.

**Why gated on `filteredRefs == nil`:** the predicate-filtered N=0 path receives its values from
`mergeJoinFilteredRefsWithVals`, which sorts by **packKey**, not timestamp — so its `inRangeVals` is
NOT time-sorted and must keep the linear `streamCountRateNoGroupBy` loop. The selective-filter case is
already handled earlier by `streamCountRateN0HashFilter` (NOTE-113/136). Dispatch added in
`dispatchIntrinsicAccumulate`; `streamCountRateNoGroupBy` is retained for the filtered path.

**Measurement (microbench, authoritative — cluster wall-clock is I/O/cache bound):**
`BenchmarkStreamCountRateNoGroupBy_SortedVsLoop` (5M spans, 240 buckets, 20 cores):
loop 12.65 ms/op, 134 allocs → sorted 0.54 µs/op, 14 allocs (−99.996% time). At M1's ~150M spans
the eliminated per-file count CPU is ~hundreds of ms; the boundary search is microseconds regardless
of span count. `intrinsic_groupby_parallel_test.go::TestStreamCountRateNoGroupBySorted_Equivalence`
asserts byte-identical per-bucket counts vs the legacy loop across dense, sparse-with-empty-buckets,
single-span, empty, and final-bucket-only inputs.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamCountRateNoGroupBySorted,dispatchIntrinsicAccumulate`

## NOTE-156: numeric dict-mask fast path for range comparisons (M9 status_code predicate)
*Added: 2026-06-09*

**Decision:** Range comparisons (`>`, `>=`, `<`, `<=`) on dict-encoded NUMERIC attribute columns
(Uint64/Int64/Float64 + Range variants) now evaluate the comparison once per DISTINCT dictionary
entry to build a `[]bool` mask, then scan rows with a single `Idx[i]` lookup + mask check —
mirroring the existing `scanStringDictFloat` string-dict fast path (NOTE-022). Previously these went
through `scanWith`→`rowCompare`, which re-ran a per-row interface type-switch (`value.(type)` ×
`col.Type` switch) plus an indirect `cond` closure call for EVERY row.

**Why this is the M9 hot path:** M9 = `{span.kind=server && span.http.response.status_code >= 400}
| rate() by (resource.service.name)`. `span.kind` is intrinsic (cheap); the `status_code >= 400`
arm is an attribute-column range scan executed over every span of every block that the planner
selects (queriers peg ~5 cores, 90s+ timeouts — confirmed CPU-bound, not I/O). `status_code` is a
low-cardinality numeric column (a handful of codes: 200, 301, 404, 500, …), so the dict mask is ~6
entries: the scan collapses from N interface comparisons to len(Dict) comparisons + N cheap index
lookups.

**Structure:** `scanNumericDict(col, value, keep, cb) (count, handled)` dispatches on `col.Type`,
builds the per-type mask via `cmpSign(dictVal, threshold)` (the sign half of `cmp3`), then
`scanNumericDictMask` runs the row loop. Wired into all four `StreamScan{Greater,Less}Than[OrEqual]`
after their existing string-dict fast path; `handled==false` (unsupported type or value kind not
comparable to the column) falls back to the legacy `scanWith`+`rowCompare` path unchanged.

**Why byte-identical:** `cmpSign` reproduces `rowCompare`'s exact per-type cross-comparison arms
(int64/uint64/float64, same `uint64(int64)` and `float64()` conversions). The row loop reproduces
`scanStringDictFloat`'s `IsPresent` + bounds-guarded `Idx`/`Dict` access, which is exactly what
`col.Uint64Value`/`Int64Value`/`Float64Value` (used by `rowCompare`) do internally. A `value` kind
not handled by a column's arms (e.g. a string threshold vs an int column) leaves the mask
unbuilt and returns `handled=false` → fall back → `rowCompare` returns `ok=false` for every row →
zero matches, same as before. Verified by `scan_numeric_dict_test.go` (int + float columns, int64 &
float64 thresholds, all 4 operators, present/absent rows, and the cross-type-skip case) asserting
identical sorted matched rows vs the legacy `scanWith`+`rowCompare` reference loop, under `-race`.

**Measurement (microbench):** `BenchmarkScanNumericDict_GTE` — 100k spans, 6 distinct codes,
float64(400) threshold: fast path 6.4 µs/op vs legacy loop ~20 µs/op (~3.1× faster), 0 allocs both
(the ~6-entry mask does not escape). On M9 the predicate column is scanned for every selected block,
so the per-block predicate-pass CPU drops proportionally.

Back-ref: `internal/modules/executor/column_provider.go:scanNumericDict,scanNumericDictMask,cmpSign,StreamScanGreaterThanOrEqual,StreamScanGreaterThan,StreamScanLessThan,StreamScanLessThanOrEqual`

## NOTE-159: per-row redundant-work removal on the metrics group-by hot path (2026-06-10)

Two small, byte-identical CPU reductions on `traceAccumulateRow` — the per-span loop body
for attribute-column group-by metrics (M4 `{}|rate() by(resource.service.name)`, M6
`{span.kind=server}|rate() by(span.http.request.method)`, M8 histogram by service). This path
is CPU-bound on the cluster (KEY FINDING 2, 2026-06-09: queriers peg ~5 cores on heavy metrics,
the earlier "33% CPU / 90% I/O wait" was a light window).

**Change 1 — drop the redundant `IsPresent` in `metricsColumnString`** (`block_label_set.go`).
The function previously opened with `if col == nil || !col.IsPresent(rowIdx)`. Every typed
accessor it then calls (`StringValue`/`Int64Value`/`Uint64Value`/`Float64Value`/`BoolValue`/
`BytesValue`) already runs `needsDecode()` (atomic load) + `expandDenseIdx()` (sync.Once
fast-path) + `IsPresent(idx)` (presence-bitmap read) internally and returns `ok=false` for absent
rows — which yields the same empty-string label. So the leading `IsPresent` doubled all three
on every span. Dropped it; kept only the `col == nil` guard (must precede the `col.Type` read).
Byte-identical: absent rows still return `""` (accessor `ok=false` → final `return ""`);
unhandled types (e.g. UUID, vector) still return `""` (fall through the switch), exactly as the
old `!IsPresent`→`""` and "type not in switch"→`""` arms did.

**Change 2 — hoist the composite-key scratch buffer to per-block** (`metrics_trace.go`).
`traceAccumulateRow` did `scratch := acquireCompositeKeyScratch(); defer releaseCompositeKeyScratch(scratch)`
on **every row** — one `sync.Pool` Get + one Put + one `defer` per span. The buffer is now
acquired once per block by the two caller loops (the `predicateCols==nil` single-pass path and
the two-pass matched-row path) and passed in as a parameter, reused across all rows, released
after the block's row loop. `traceAccumulateRow` resets it with `(*scratch)[:0]` before each key
build, so no stale bytes leak between rows; all early returns occur before scratch is touched.
Alloc behavior is unchanged (still 0 allocs/row on map-hit, 1 stored-key alloc on map-miss,
NOTE-073); the win is removing per-row Pool/defer overhead from the tight loop.

Both changes preserve the exact key format (NOTE-067/NOTE-073) and HISTOGRAM 3rd-segment
encoding (NOTE-033). Verified by the full executor suite under `-race` (incl.
`metrics_trace_test.go`, `metrics_trace_composite_key_test.go`, `metrics_trace_pool_test.go`).

Back-ref: `internal/modules/executor/block_label_set.go:metricsColumnString`,
`internal/modules/executor/metrics_trace.go:traceAccumulateRow,ExecuteTraceMetrics`

## NOTE-160: hoist per-block column resolution out of the metrics per-row loop (2026-06-10)

`traceAccumulateRow` resolved its columns via `block.GetColumn(name)` — a string-keyed map
lookup — on EVERY span: once for `span:start`, once per `GroupBy` attribute, and once for the
aggregate field (inside `traceUpdateBucket`/`traceHistogramBucket` via `traceFieldFloat64`).
The resolved `*Column` pointers are constant for every row in a block, so this was
O(rows × (2 + numGroupBy)) redundant map lookups + string hashing per block on the CPU-bound
metrics group-by path (M4/M6/M8; KEY FINDING 2: queriers peg ~5 cores on heavy metrics).

**Change.** Added `blockMetricsCols` (resolved span:start col + intrinsic-fallback flag,
`groupByCols[]`, aggregate `fieldCol`) and `resolveBlockMetricsCols`, called ONCE per block by
both `ExecuteTraceMetrics` caller loops (the `predicateCols==nil` single-pass path and the
two-pass matched-row path). `traceAccumulateRow` now takes `*blockMetricsCols` and reads the
pre-resolved pointers — zero `GetColumn` calls per row. The span:start block-column-first /
intrinsic-section-fallback PATTERN is preserved: `tsCol != nil` → use the block column;
otherwise `useIntrinsicTS` → `r.IntrinsicUint64At`. `traceFieldFloat64` was renamed to
`traceFieldFloat64Col` and now takes a resolved `*Column` instead of `block + fieldName`;
`traceHistogramBucket` and `traceUpdateBucket` likewise take the resolved `fieldCol`
(`fieldName` retained in `traceHistogramBucket` only for the `span:duration` ns→s special case).

**Correctness.** Byte-identical output: the field column is resolved from the same
`querySpec.Aggregate.Field` the old code passed to `GetColumn`; `fieldCol == nil` reproduces the
old "column absent → skip" behavior; absent rows still return `ok=false` via the typed
accessors; COUNT/RATE leave `fieldCol` nil (empty `Field`) and never touch the value path. Key
format (NOTE-067/073) and HISTOGRAM 3rd-segment (NOTE-033) unchanged. `groupByCols` is allocated
once per block (cheap relative to the per-row lookups it removes). Verified by the full executor
suite under `-race`.

Back-ref: `internal/modules/executor/metrics_trace.go:blockMetricsCols,resolveBlockMetricsCols,
traceAccumulateRow,traceUpdateBucket,traceHistogramBucket,traceFieldFloat64Col`

---

## NOTE-164: skip clear() on fully-overwritten sortedPKs pool buffers (2026-06-10)

The compact N=1 paths (`streamCountRateN1Compact`, `streamCountRateN1CompactFromRefs`,
`streamHistogramN1CompactFromRefs`, `streamAggN1Compact`, `streamAggN1CompactFromRefs`, and
the predicate-filtered N=1 count/rate path) each acquire a pooled `sortedPKs []uint32` of
length `n` (= in-range ref count, ~7.2 M on the warm M6/M8/M9 rate-by/histogram scan path) via
`acquireCompactUint32(n)`. That helper unconditionally `clear()`s the slice on acquire (pool
semantics for sentinel-using buffers, NOTE-125). But every one of these `sortedPKs` buffers is
written for EVERY index `[0,n)` — one `packKey` per ref — before any read, so the clear is pure
waste: ~28 MB of zeroing per file per query, repeated on every block-group, on the CPU-bound
metrics scan path (KEY FINDING: queriers peg ~5 cores on heavy metrics).

**Change.** Added `acquireCompactUint32NoClear(n)` (same pool, no `clear()`) and switched the
six `sortedPKs := acquireCompactUint32(n)` sites to it. Sentinel-using buffers from the same
pool (`dictIdxByPos`, `rankPrefix`, `entryGIdx`) keep `acquireCompactUint32` because they rely
on the zero value as an "absent" marker and are only partially written.

**Correctness.** Byte-identical output: each `sortedPKs[i]` is assigned in a loop that covers
all `i ∈ [0,n)` (either `for i, ref := range inRangeRefs` with `len==n`, or
`for i, packed := range pkOrder` with `len(pkOrder)==n`) before the buffer is read by
`scanGroupByColCompact` / `streamCountRateN1CompactCore` / the histogram scan. No stale pool
data can leak because no index is left unwritten. `releaseCompactUint32` is unchanged, so the
returned buffer re-enters the same pool. Verified by the full executor suite under `-race`.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:acquireCompactUint32NoClear,
streamCountRateN1Compact,streamCountRateN1CompactFromRefs,streamHistogramN1CompactFromRefs,
streamAggN1Compact,streamAggN1CompactFromRefs`

## NOTE-166: mergeJoinFilteredRefsWithVals — probe-then-sort-matches (drop O(N log N) sort)
*Added: 2026-06-10*
**Decision:** Replace the full-array packed-index sort in `mergeJoinFilteredRefsWithVals`
(NOTE-111/128: build `idxPacked []uint64` over all N in-range refs, then `slices.Sort` it,
then two-pointer-merge against the sorted `filteredPKs`) with a probe-then-sort-matches
strategy: walk `inRangeRefs` in natural order, range-gate each packKey against
`[filteredPKs[0], filteredPKs[last]]`, binary-search survivors against the already-sorted
`filteredPKs`, and collect only matches as packed `(pk<<32 | pos)` into `matched[]`. Sort
ONLY the matched subset (M ≤ F ≪ N) to restore the packKey-sorted output invariant that
downstream compact paths (NOTE-110/112/114) rely on (they build `sortedPKs` in O(n)).
**Rationale:** A 2026-06-09 querier CPU profile attributed `slices.partitionOrdered` +
`insertionSortOrdered` + `partitionCmpFunc` + `cmp.Less` — reached exclusively via this
function — at ~12% of total querier CPU, the single largest blockpack-attributable
self-cost. The dominant term was sorting the large N in-range array. The new form is
O(N log F) probe + O(M log M) sort instead of O(N log N); for the common selective case
M ≪ N so the sort cost collapses. The ~57 MB `idxPacked` allocation is replaced by an
M-sized matched buffer (still pooled via compactUint64Pool). The profile was also dominated
by GC (gcDrain/scanObject/markroot/wbBufFlush), so removing the 57 MB allocation directly
reduces GC CPU on the hot path.
**Correctness:** Output (ref,val) set is identical to the former merge-join: a ref is emitted
iff its packKey appears in filteredRefs. The `[flo, fhi]` range gate only skips packKeys that
binary search would reject anyway (filteredPKs is sorted). Output remains packKey-sorted
because `matched` is sorted by its high 32 bits (pk) before emission. Verified by
merge_join_test.go (order-insensitive set equality vs a reference map intersect AND
packKey-sorted-output assertion) under `-race`.
**Queries affected:** All predicate-filtered metrics queries that take the merge-join path
(M6, M9, M10 and any `... by (...)` with a predicate that is not the N=0 hash-filter fast
path of NOTE-113).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`

## NOTE-167: mergeJoinFilteredRefsWithVals — replace per-survivor sort.Search with bitset bit-test
*Added: 2026-06-10*
**Decision:** Replace NOTE-166's per-in-range-ref `sort.Search(filteredPKs, ...)` membership
probe with an O(1) presence-bitset bit-test. Before walking `inRangeRefs`, build a `[]uint64`
bitset over the sorted `filteredPKs` (one bit per packKey, indexed by `pk`, with the array
offset by `baseWord = flo>>6` so it spans only the active `[flo, fhi]` packKey range). For each
in-range ref, after the existing `[flo, fhi]` range gate, test membership with a single
`pkBitset[(pk>>6)-baseWord] & (1<<(pk&63)) != 0` — one word load + shift + mask + AND. `filteredPKs`
is released early (right after the bitset is built); the bitset is acquired/released from
`compactUint64Pool` like the other compact-path scratch.
**Rationale:** A 2026-06-10 querier CPU profile (taken AFTER NOTE-166 landed) attributed
`sort.Search` at **4.93% of total querier CPU** — the single largest blockpack-reachable
self-cost — plus `slices.partitionCmpFunc` (3.56%) and `slices.partitionOrdered` (1.26%), all
reached via this function. The closure-based `sort.Search` does ~log2(F) (~22 at F=3.75 M)
indirect-call probes per survivor; the bitset replaces that whole loop with a single O(1)
memory access. This is the identical `pkBitset` technique already proven on the group-by and
histogram compact scans (`scanGroupByColCompact` NOTE-135/140, `scanAggColHistogramCompact`).
**Correctness:** Output (ref,val) set is unchanged: a ref is emitted iff its packKey is present
in `filteredPKs`, which the bitset encodes exactly (bit set ⟺ packKey present). The `[flo, fhi]`
range gate is preserved and only skips packKeys the bitset would also reject (they map outside
the encoded range). The bitset offset (`baseWord = flo>>6`) is exact because every set bit's
word index `pk>>6 >= flo>>6 = baseWord`, so `(pk>>6)-baseWord >= 0` for all in-range refs that
pass the gate, and `(fhi>>6)-baseWord` is the last valid word index (allocated). Output remains
packKey-sorted: `matched` is still sorted by its high 32 bits before emission. Verified by
merge_join_test.go (order-insensitive set equality vs reference map intersect + packKey-sorted
output assertion, shuffled/duplicate inputs) under `-race`.
**Queries affected:** Same merge-join path as NOTE-166 (M6, M9, M10 and predicated `... by (...)`).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`

## NOTE-175: pkOrder / matched — closure-free LSD radix sort by packKey
*Added: 2026-06-10*
**Decision:** Replace `slices.Sort(pkOrder)` (3 call sites in the compact N=1 count/rate and
duration/histogram group-by scan paths: `streamCountRateN1Compact` and its variants) and
`slices.Sort(matched)` (the merge-join matched-subset reorder in
`mergeJoinFilteredRefsWithVals`) with `radixSortByPackKey` (new file `radix_pkorder.go`). Each
element is a packed `uint64` of `packKey<<32 | idx`; ordering is by the **high 32 bits**
(packKey) only, with the low-32 position index travelling alongside the key. The radix sort runs
4 LSD counting passes over those 4 key bytes (starting at bit 32), drawing its scratch buffer
from `compactUint64Pool` (the same pool that backs `pkOrder`), so the only extra cost is one
pooled acquire/release amortized across blocks. Four (even) passes leave the result in the input
slice — no copy-back.
**Rationale:** `slices.Sort` on `[]uint64` dispatches to the comparison-based `pdqsortOrdered`
(O(N log N)). On the CONFIRMED-CPU-BOUND queriers, the full N-element `pkOrder` array reaches up
to ~7.2 M entries on group-by/histogram queries (M6/M8), making this sort a large per-block self
cost. This applies the identical closure-free radix technique already profile-proven on the
ref-index unsorted fallback (NOTE-174, `blockio/shared.radixSortRefIndex`), which dropped that
sort path ~5.1pp → ~0.55pp of querier CPU. Microbench at N=2²⁰: `slices.Sort` 76.3 ms/op →
radix 8.5 ms/op (**~9x**), 1 (pooled) alloc.
**Correctness:** Ordering is by packKey alone; entries with equal packKey keep an
arbitrary-but-consistent relative order — exactly as with the prior non-stable `slices.Sort`. The
consumers (`streamCountRateN1CompactCore` via `scanGroupByColCompact`, and the merge-join output
assembly) iterate the sorted slice positionally and binary-search packKeys; they do not depend on
within-packKey tie order. Each element's low-32 index is preserved alongside its key, so the
`sortedPKs`/`timeBucketByPos` pair (built from `pkOrder[i]`) and the merge-join `inRangeRefs[pos]`
lookup remain consistent. Verified in `radix_pkorder_test.go`: permutation equality vs the input
multiset, ascending-by-packKey invariant, and key-projection equality vs `slices.Sort` over random
inputs (sizes 0..5000), heavy-duplicate keys, and 32-bit extremes (0, 0x7FFFFFFF, 0x80000000,
0xFFFFFFFF) under `-race`.
**Queries affected:** Compact group-by count/rate and histogram_over_time group-by (M6, M8) plus
the predicate-filtered merge-join path (M6/M9/M10 and `... by (...)`).
Back-ref: `internal/modules/executor/radix_pkorder.go`, `metrics_trace_intrinsic.go`

## NOTE-193: radixSortByPackKey — skip leading-zero key-byte passes by magnitude
*Added: 2026-06-11*
**Decision:** Apply the magnitude-skip already proven on the ref-index radix sort (NOTE-190,
`blockio/shared.radixSortRefIndex`) to `radixSortByPackKey`. A single O(N) scan ORs the high-32-bit
sort keys; the highest set key bit bounds how many LSD byte passes are significant, so leading-zero
key bytes are skipped as identity passes. The fixed four-pass form always ran all four byte passes
even when the top key bytes were uniformly zero.
**Rationale:** The sort key is `packKey = BlockIdx<<16 | RowIdx`. query-frontend shards to one block
per querier call (mission 2026-06-09), so `BlockIdx` is 0 (or a tiny single-block value) in the
dominant case and the top two packKey bytes are identically zero. For that case the four-pass sort
collapses to two passes — halving the per-block sort cost on the compact N=1 count/rate path
(`radixSortByPackKey` was ~0.38% querier self-time, profile 2026-06-11). When `keyOr==0` (all keys
zero, e.g. a single-row single-block shard) the slice is already trivially sorted by key and no pass
runs at all.
**Correctness:** Ordering and tie behavior are unchanged from NOTE-175 (by packKey only,
arbitrary-but-consistent within equal keys). The number of executed passes is now data-dependent:
with an **odd** number of significant passes the sorted data ends up in the pooled scratch buffer,
so a final `copy(s, src)` lands it back in `s` — the prior form was always even (4 passes) and
relied on `src==s` after the last pass with no copy-back. The OR-scan and copy-back mirror
`radixSortRefIndex` exactly. Verified by the existing `radix_pkorder_test.go` (permutation equality,
ascending-by-packKey invariant, key-projection equality vs `slices.Sort`, 32-bit extremes incl. all
keys zero and all keys 0xFFFFFFFF) under `-race`.
**Queries affected:** Same as NOTE-175 — compact group-by count/rate and histogram_over_time
group-by (M1/M4/M6/M8/M9) plus the predicate-filtered merge-join path.
Back-ref: `internal/modules/executor/radix_pkorder.go`

## NOTE-194: mergeJoinFilteredRefsWithVals — size `matched` to F, not N
*Added: 2026-06-11*
**Decision:** Size the `matched` packed-position scratch buffer to `min(N, F)` (F = len(filteredRefs),
N = len(inRangeRefs)) instead of unconditionally to N. A matched entry is an in-range ref whose
packKey is present in the predicate-filtered set, so the match count m is bounded by both the number
of in-range refs and the number of distinct filtered packKeys. Within a block group's intrinsic scan
inRangeRefs are per-span (distinct BlockIdx/RowIdx, hence distinct packKeys), so each match consumes a
distinct filtered packKey and m ≤ min(N, F).
**Rationale:** On the predicate-filtered metrics path (M6/M9/M10 and any `... by (...)` carrying a
predicate) F ≪ N: `filteredRefs` is just the predicate survivors while `inRangeRefs` spans every
span in the time window for the value column (e.g. span:duration). The old `acquireCompactUint64(N)`
allocated a buffer proportional to the full in-range population (NOTE-128: ~57 MB at N=7.2 M) of which
only m ≤ F entries were ever written. `mergeJoinFilteredRefsWithVals` drove ~2.4 GB of querier
alloc_space through this acquire (profile 2026-06-11). Sizing to F removes that excess: the pooled
buffer now matches the work actually done, cutting GC pressure on the heavy filtered group-by path.
**Correctness:** Output is byte-identical to the prior sizing. A defensive grow guard (when m reaches
the F-sized buffer's length) reallocates to the strict upper bound N and copies forward, so even if a
future caller passed inRangeRefs with duplicate packKeys (violating m ≤ F) no match is dropped — the
common path (unique packKeys, m ≤ F) never triggers the guard. The packKey bijection and uniqueness
argument is the same one NOTE-166/167 already rely on for the bitset membership test.
**Queries affected:** Predicate-filtered merge-join path — M6, M9, M10, and any `... by (...)` with a
predicate.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`

## NOTE-195 — rankPrefix POPCNT index uses the NoClear pool variant
**What:** All three POPCNT `rankPrefix` build sites (scanGroupByColCompact for N=1 rate-by,
scanAggColHistogramCompact for histograms, and the predicate-filtered histogram driver) now acquire
their `rankPrefix` buffer via `acquireCompactUint32NoClear(n+1)` instead of `acquireCompactUint32(n+1)`,
skipping the pool's internal `clear()`.
**Rationale:** `rankPrefix` is the prefix-sum of `pkBitset`'s per-word popcounts. Its build loop is
`for i, w := range pkBitset { rankPrefix[i] = cum; cum += popcount(w) }` followed by
`rankPrefix[len(pkBitset)] = cum`. Because the buffer is sized to `len(pkBitset)+1` (== n+1) and
`len(pkBitset) == n`, every index `[0, n]` — i.e. all n+1 elements — is written before `rankPrefix`
is ever read by the scan inner loops. The `clear()` inside `acquireCompactUint32` was therefore pure
waste: it zeroes a buffer that is immediately fully overwritten. `n = (maxPK>>6)+1` is the packKey
bitset word count (~250 K at maxPK=16 M), so the skipped clear is ~1 MB of zeroing per call on the
hot M4/M6/M9/M8 group-by/histogram scan path. This is the same fully-overwritten-before-read argument
NOTE-164 used to introduce `acquireCompactUint32NoClear` for the `sortedPKs` buffers; the original
NOTE-164 comment conservatively excluded `rankPrefix` ("sentinel-using"), but `rankPrefix` carries no
sentinel — every slot is written deterministically by the build loop — so it qualifies.
**Correctness:** `pkBitset` keeps its explicit `clear()` (it IS a zero-bit membership sentinel and is
only partially set by the `pk>>6` writes). Only `rankPrefix` changed. Output of every scan path is
byte-identical: the rank values are a pure function of the fully-built `pkBitset` and are computed the
same way regardless of the buffer's prior contents.
**Queries affected:** All group-by/histogram scan paths — M4, M6, M8, M9, M10.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanGroupByColCompact,scanAggColHistogramCompact`

## NOTE-197 — prefetch intrinsic working set before per-column scans

`executeTraceMetricsIntrinsic` now calls `prefetchIntrinsicWorkingSet` immediately after the
intrinsic-eligibility check, before the first `GetIntrinsicColumn`. It collects the union of
span:start, the aggregate field, every group-by column, and every predicate-leaf intrinsic
column (all normalized to full intrinsic names via normalizeIntrinsicFieldName), then issues one
batched `Reader.PrefetchIntrinsicColumns` so the subsequent per-column reads (predicate filter +
group-by) are served from cache with no per-column memcache round-trip. This attacks the kernel
networking cost (the dominant querier CPU sink per the 2026-06-11 profile) by cutting the per-file
intrinsic round-trip count, not the per-row decode cost.
**Queries affected:** all intrinsic metrics paths reading >1 intrinsic column — M4, M6, M8, M9.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:prefetchIntrinsicWorkingSet,executeTraceMetricsIntrinsic`,
`internal/modules/blockio/reader/intrinsic_reader.go:PrefetchIntrinsicColumns`.

## NOTE-198 — prefetch predicate-leaf column blobs before per-leaf intrinsic scans (search)

The search-side intrinsic pre-filter (`BlockRefsFromIntrinsicTOC` and
`blockRefsFromIntrinsicPartial`) evaluates each predicate leaf via `scanIntrinsicLeafRefs`,
which reads that leaf's blob with its own `GetIntrinsicColumnBlob` call. A multi-leaf query
(several AND/OR conditions) therefore pays one memcache round-trip per leaf. `prefetchPredicateLeafColumns`
collects the union of leaf column names (`collectNodeColumns`) and, when there is more than one,
issues a single `Reader.PrefetchIntrinsicColumns` (one `GetMulti`) before the per-leaf scans. The
batch's hits are written back into the in-process cache tier (ChainedCache.GetMulti → writeBack), so
the subsequent `GetIntrinsicColumnBlob(node.Column)` calls are served locally with no further
round-trip. Leaf names are passed verbatim (the same key the scan uses); names absent from the file's
intrinsic index — e.g. user-attribute leaves in a mixed query — are dropped by PrefetchIntrinsicColumns,
and any name that misses the batch falls through to its normal per-name fetch, so the result is byte-identical
to never prefetching. Single-leaf queries are skipped (one fetch is already minimal). This is the search
counterpart of the metrics-side NOTE-197, attacking the same kernel-networking round-trip cost.
**Queries affected:** intrinsic-only and mixed search queries with >1 intrinsic predicate leaf — Q9, Q10.
Back-ref: `internal/modules/executor/predicates.go:prefetchPredicateLeafColumns,BlockRefsFromIntrinsicTOC,blockRefsFromIntrinsicPartial`,
`internal/modules/blockio/reader/intrinsic_reader.go:PrefetchIntrinsicColumns`.

## NOTE-202 — fallback N=1 count/rate group-by uses one pooled contiguous accumulator

The N=1 count/rate group-by row-emission fallback paths (`streamByRefSliceCountRate`,
reached when the no-predicate direct path is skipped because maxPK exceeds the L3 direct-array
threshold — predicate-filtered M6/M9-class queries; and `streamCountRateGroupByIDSingle`, the
N=1 dict-ID fallback) allocated their per-group counters as `make([][]int64, len(dict))` plus a
separate `make([]int64, numSteps)` for *every* dict entry. That paid `len(dict)` distinct heap
allocations per file on the warm path and scattered the per-group counters across the heap, so the
emit-time scan over all groups chased pointers through unrelated cache lines.

Both now accumulate into a single contiguous `[]int64` of `len(dict)*numSteps` taken from
`groupCountsFlatPool` (the same pool NOTE-124 introduced for the direct path), indexed as
`groupCounts[dictIdx*numSteps+bucketIdx]`. This collapses `len(dict)` allocations into one pooled
reuse (zero allocation on warm queries — the pool's backing array is cleared and handed back),
and lays the counters out contiguously so the emit scan walks memory sequentially — the identical
layout `accumulateCountRateDirect` already uses. Output is byte-for-byte unchanged: the per-entry
slice `groupCounts[g][b]` maps exactly to flat index `g*numSteps+b`, the emit order (group ascending,
then bucket ascending) is preserved, and `streamByRefSliceCountRate`'s `count +=` vs
`streamCountRateGroupByIDSingle`'s `count =` bucket-write semantics are each preserved.
No benchmark-specific constants — a pure allocation/locality restructuring of the group-by
accumulator on the fallback path.
**Queries affected:** N=1 count/rate group-by queries whose maxPK exceeds the direct-path L3
threshold (predicate-filtered service/method group-bys) — M6/M9-class.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamByRefSliceCountRate,streamCountRateGroupByIDSingle`,
pool: `acquireGroupCountsFlat/releaseGroupCountsFlat` (NOTE-124).

## NOTE-203 — mergeJoinFilteredRefsWithVals: drop slices.Sort, fold min/max into the build pass

`mergeJoinFilteredRefsWithVals` built a `[]uint32` of the filtered refs' packKeys and ran a full
`slices.Sort` over it. The sort's only purpose was to read `flo = filteredPKs[0]` and
`fhi = filteredPKs[len-1]` — the min and max packKey that define the `[flo, fhi]` range gate
guarding the membership bit-set. Every other consumer of `filteredPKs` is order-independent: the
bit-set build loop (NOTE-167) sets one bit per packKey regardless of order, and `filteredPKs` is
released immediately afterwards.

min/max is an O(N) reduction, so it is now computed in the same single pass that derives the
packKeys, and the `slices.Sort` is removed entirely. This replaces an O(N log N) comparison sort
over the F-element filtered set with O(N) on every invocation of the predicate-filtered metrics
merge-join path. The querier CPU profile (2026-06-11) attributed ~1.6% self-time to
`mergeJoinFilteredRefsWithVals`, of which the sort over `filteredPKs` is the dominant component on
predicate-filtered group-by queries (M6/M9/M10 and any `... by (...)` with a predicate).

Output is byte-for-byte identical: bit-set membership does not depend on the order in which
`filteredPKs` was produced, and `flo`/`fhi` are the same min/max values whether read from a sorted
slice or computed by reduction. No benchmark-specific constants — a pure algorithmic O(N log N) → O(N)
reduction.
**Queries affected:** predicate-filtered count/rate group-by and merge-join metrics queries — M6, M9, M10.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`.

## NOTE-206 — radixSortByPackKey: fuse count scan into prior pass scatter (look-ahead histograms)

`radixSortByPackKey` (the LSD radix sort over the high-32-bit packKey on the N=1 count/rate,
duration/histogram, and merge-join matched-subset reorder paths) ran the classic LSD loop: one
O(N) OR scan to bound the significant byte count (NOTE-193), then for each byte pass one O(N)
count scan plus one O(N) scatter scan. That read every element `(2·passes+1)` times.

The scatter of pass k already reads every `src[i]` key, so it now tallies pass k+1's histogram
for free in the same scan ("look-ahead histograms"). The only standalone count scan is the
lowest byte's, which doubles as the NOTE-193 OR scan (tallies byte-0's histogram while ORing all
keys). Source reads drop from `(2·passes+1)·N` to `(passes+1)·N`. Work stays bounded by the
*actual* significant-pass count, so the dominant 2-pass single-block case (BlockIdx==0,
packKey==RowIdx) never tallies a byte it will not use.

This is the identical reorganization already profile-proven on the ref-index radix sort
(NOTE-205, `blockio/shared.radixSortRefIndex`). It is a pure restructuring of *when* counts are
tallied: the prefix sum, scatter order, pass count, and odd-pass copy-back are all unchanged, so
the output is byte-for-byte identical to the prior form and to `slices.Sort` projected onto the
key. No benchmark-specific constants — a general radix reorganization independent of column or
query shape.
**Queries affected:** N=1 count/rate (M1/M4/M9), duration/histogram group-by (M8), and the
merge-join matched-subset reorder (Q9/Q10-class), wherever `pkOrder` is large enough to dominate.
Back-ref: `internal/modules/executor/radix_pkorder.go:radixSortByPackKey`.

## NOTE-238 — intrinsicLeafMatchTyped: single column switch instead of chained type-group getters

`intrinsicLeafMatchTyped` evaluates one predicate leaf against a typed `intrinsicRowFields`
row. It runs once per CANDIDATE row on the search/structural and predicate-filtered metrics
paths — `rowSatisfiesIntrinsicNodesTyped` (and its OR variant) walk the predicate tree per
row over millions of rows for `{...}`-filtered queries and `>>`/`>` structural chains
(Q9/Q10/M6/M9). The leaf's `Column` is *constant* across every row in a scan, yet the former
implementation re-resolved it on every row by probing up to four type-group getters in
sequence — `intrinsicLeafGet{Bytes,Uint64,Int64,String}Typed`, each a `switch col` returning
`(value, ok)` — so a single int64 leaf (e.g. `span:kind`) paid a bytes switch + a uint64
switch + an int64 switch before reaching its field. `rowSatisfiesIntrinsicNodesTyped` +
`intrinsicLeafMatchTyped` measured ~0.24% of querier self-time (profile 2026-06-12) on a code
shape that is dominated by string-switch dispatch, not by the actual comparison.

The rewrite collapses the four chained getters into one `switch n.Column` whose arm reads the
correct field inline and dispatches to a per-type match helper (`matchIntrinsicU64/I64/Str/
Bytes`). The column is resolved once, the field is read with no getter-call indirection, and
the per-type value/range/pattern logic is unchanged. Microbench (`BenchmarkRowSatisfiesTyped`
4096-row scan, `span:kind` value match): **68.6µs → 17.5µs, ~3.9×**.

**Correctness:** byte-for-byte equivalent to the prior chained form. The four former getters
applied a uniform absent-field rule — a field whose present-bit is clear matches only an empty
predicate (`len(Values)==0 && Min==nil && Max==nil && Pattern==""`) — which the bytes getter
expressed as `matchIntrinsicBytesField(nil, n)` and the scalar/string getters expressed by
falling through to the "unknown intrinsic" tail; the rewrite makes this rule explicit and
shared (`absentMatchesEmptyPredicate`). Range/pattern-on-bytes still returns false on a present
bytes field; pattern-on-numeric still returns false; string pattern still compiles via
`cachedRegexCompile`. `TestIntrinsicLeafMatchTyped_EquivalentToReference` pins this against a
kept re-implementation of the old chained-getter logic across all 11 intrinsic columns + an
unknown column, present and absent, over value/range/pattern/empty predicate shapes. The dead
getters and `matchIntrinsicBytesField` (no other callers) were deleted. No benchmark-specific
constants — a general dispatch restructuring over the fixed intrinsic column set.
**Queries affected:** predicate-filtered search (Q2/Q5/Q9/Q10) and predicate-filtered metrics
(M6/M9), wherever a `{...}` filter has intrinsic-column leaves evaluated per candidate row.
Back-ref: `internal/modules/executor/predicates.go:intrinsicLeafMatchTyped`.

## NOTE-245: precompute per-timestep composite-key prefixes in group-by emit loops

The group-by rate/agg emit loops build a composite map key per occupied (group, timestep) cell
as `strconv.FormatInt(bk, 10) + "\x00" + gk` inside a `numGroups × numSteps` double loop. The
timestep portion (`FormatInt(bk) + "\x00"`) is identical across all groups, so the integer
formatting + intermediate string allocation was repeated up to `numGroups` times for each of
the `numSteps` distinct bucket indices. For a rate-by query like M4/M9 (~248 groups × ~120
steps over the bench window) that is ~30K `FormatInt`+concat calls when only ~120 distinct
prefixes exist.

`timeBucketKeyPrefixes(numSteps)` builds the `numSteps` prefix strings once per emit; the emit
loop then concatenates `prefix + gk` (the trailing `+gk` concat is unavoidable — the composite
key embeds the per-group value, which differs per group). Applied to the four group-by emit
loops: `streamCountRateN1CompactCore` (M4/M6 rate-by, unfiltered), `streamCountRateN1CompactFromRefs`
(predicate-filtered N=1 count/rate emit), `streamAggN1CompactFromRefs` (predicate-filtered N=1
general agg), and `accumulateAggDirect` (dense direct N=1 agg). The histogram emit paths use a
distinct key shape (`FormatInt(bk)+"\x00\x00"+FormatFloat(boundary)`) and are out of scope here.

**Correctness:** byte-for-byte equivalent. `timeBucketKeyPrefixes(numSteps)[i]` equals the
former `strconv.FormatInt(int64(i), 10) + "\x00"` for every `i ∈ [0, numSteps)`, and every emit
loop iterates `timeIdx`/`bk` over exactly `[0, numSteps)` (the `row` slices and `groupCountsFlat`
strides are sized to `numSteps`). The resulting map key is identical, so the emitted buckets and
their downstream series are unchanged. No benchmark-specific constants — a general string-build
hoist over the timestep dimension.
**Queries affected:** group-by rate/agg metrics (M4, M6, M9) emit. Back-ref:
`internal/modules/executor/metrics_trace_intrinsic.go:timeBucketKeyPrefixes`.

## NOTE-246: single-pass scatter for dense/histogram series build

`traceBuildDenseSeries` and `traceHistogramSeries` consume the per-block accumulation map
`map[string]*aggBucketState`, whose composite key is `bucketIdx\x00attrGroupKey` (dense) or
`bucketIdx\x00attrGroupKey\x00histBoundary` (histogram). Both formerly used a two-phase build:
(1) iterate the map once to enumerate the distinct series keys, then (2) for every
`series × bucketIdx` cell, rebuild the composite key string and probe the map for that cell's
bucket. For a sparse grid — the common case once a query has many groups but few populated
timesteps per group — phase (2) performed `numSeries × numBuckets` map lookups plus an equal
number of composite-key rebuilds, the vast majority returning nil (an empty cell). M4/M6/M9
(rate-by, ~248 groups) and especially M8 (`histogram_over_time ... by`, ~1987 series × numBuckets
steps) paid this cost in full.

The replacement iterates the populated buckets exactly once and scatters each cell's value
directly into its series' dense `values` slice, keyed by `attrGroupKey` (dense) or
`(attrGroupKey, histBoundary)` (histogram). The scatter touches only `len(buckets)` cells — the
number of *populated* cells — and rebuilds no composite keys. Series ordering remains
deterministic via the final `slices.SortFunc` over the full label string (SPEC-ETM-11); the
histogram path's former intermediate sort by (attrGroupKey, numeric boundary) was redundant with
that final sort and is removed.

**Correctness:** unpopulated cells must carry the per-function "empty" value (SPEC-ETM-2): 0 for
COUNT/RATE, NaN otherwise. The dense path pre-fills each freshly allocated series slice with
`traceRowValue(nil, ...)` (which yields exactly that) before scattering populated cells over it;
when the empty value is 0 the pre-fill is skipped (slice zero value already 0). The histogram
path uses COUNT semantics throughout (empty = 0 = slice zero value), so no pre-fill is needed.
Composite-key parsing is unchanged (first-`\x00` strips bucketIdx; last-`\x00` separates the
histogram boundary), and bucketIdx is range-checked to `[0, numBuckets)` before use. No
benchmark-specific constants — a general algorithmic change from dense probing to sparse scatter.
**Queries affected:** all group-by trace metrics series build (M4, M6, M9 rate-by; M8 histogram).
Back-ref: `internal/modules/executor/metrics_trace.go:traceBuildDenseSeries`,
`internal/modules/executor/metrics_trace.go:traceHistogramSeries`.

## NOTE-247: direct dense-array → series emit for single-dim count/rate group-by (skip the string map round-trip)

The single-dimension intrinsic count/rate group-by emit paths — `accumulateCountRateDirect`
(no-predicate direct, M4 `{} | rate() by (...)`), `streamCountRateN1CompactCore` (the shared core
behind `streamCountRateN1Compact` / `streamCountRateN1CompactFromRefs`, the compact and
predicate-filtered N=1 count/rate paths, M9 `{pred} | rate() by (...)`) — accumulate their result
into a dense `groupCountsFlat[gIdx*numSteps+bk]` array whose layout IS the final series grid:
group `gIdx` maps to `dict[gIdx]`, timestep `bk` to series value index `bk`. Each path then
serialized that grid into the string-keyed `buckets map[string]*aggBucketState` via
`FormatInt(bk,10) + "\x00" + gk` per occupied cell, and `traceBuildDenseSeries` immediately
deserialized it back (`IndexByte` + `ParseInt` per cell, hash lookup, scatter into a per-group
dense slice). The composite key existed only to carry the integer step index out of the dense
array and parse it straight back in — one hash insert + one string concat per occupied cell on
emit, then one hash lookup + one ParseInt per cell on rebuild, for zero semantic gain.

`executeTraceMetricsIntrinsic` runs **per file** and consumes its result immediately; the
`buckets` map is never shared across blocks on the intrinsic path (cross-block coalescing happens
in `metrics_trace.go`'s block-scan path, NOTE-246, not here). So for the count/rate N=1 case the
driver now passes a `*[]TraceTimeSeries` sink down through `dispatchIntrinsicAccumulate`; when the
sink is non-nil the emit functions call `emitFlatCountRateSeries`, which walks the dense array
once and appends one `TraceTimeSeries{Labels:[{intrinsicLabelName(groupBy[0]), dict[gIdx]}],
Values: row scaled to COUNT/RATE}` per non-empty group — no string map, no key formatting, no
re-parse. The driver then sorts the collected series once (`finalizeCountRateSeries`, identical
final `SortFunc` over the label string, SPEC-ETM-11) and skips `traceBuildDenseSeries` entirely.

**Scope/fallback:** the sink is supplied only for `isCountRate && len(GroupBy) == 1`. All N=1
count/rate dispatch branches (direct, compact, predicate-filtered-compact) thread and honor it;
when the sink is nil (every other query shape, and any future caller) the functions emit into
`buckets` exactly as before, so the change is transparent to histogram, agg, N=0, and N>1 paths.
Value semantics match `traceRowValue`: COUNT = count, RATE = count/stepSec, both 0 for empty
cells; the absent-row pass (group 0 / `dict[0]==""`) emits a series with empty label value, just
as `traceBuildDenseSeries` does. No benchmark-specific constants — a general structural change
removing a serialize/deserialize round-trip on the hottest non-histogram group-by emit path.
**Queries affected:** M4 (`{} | rate() by`), M9 (`{pred} | rate() by`), and any count/rate by a
single dimension. Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:emitFlatCountRateSeries`,
`finalizeCountRateSeries`, `accumulateCountRateDirect`, `streamCountRateN1CompactCore`,
`executeTraceMetricsIntrinsic`.

## NOTE-250: tighten the intrinsic metrics scan inner loops (branch-free exponent + AND-mask ctx check)

Two general per-ref kernel substitutions on the intrinsic metrics scan hot loops, neither tied
to any benchmark constant.

**(a) Branch-free IEEE-754 exponent.** `pow2Floor`, `boundaryIndexer.index`, and
`boundaryIndexer.lookup` derived a histogram cell's binary exponent from `math.Frexp(v)`. On the
M8 histogram aggregation scan that exponent is computed once per passing ref (`lookup` is the
per-row kernel the parallel workers call; `index` the serial analogue), a path NOTE-182 measured
among the top querier CPU sinks. `math.Frexp` is not inlined and front-loads branches for
NaN/Inf/zero/subnormal inputs that never occur here: both callers test `v <= 0` (and re-test
after the 1e9 nano scaling) BEFORE the Frexp call, so `v` is always positive finite at that
point. `frexpExpPos(v)` extracts the biased-exponent field directly: for a normal positive
double `v = mantissa * 2**(unbiasedExp-52)`, Frexp's returned exponent is
`unbiasedExp+1 == be-1022` where `be` is the 11-bit biased-exponent field. The only non-normal
positive case reachable past the `v<=0` guard is a subnormal (`be == 0`), which falls back to
`math.Frexp` so the result is bit-identical to the old form for every input.

**(b) AND-mask context-cancellation throttle.** Every intrinsic metrics scan loop polls
`ctx.Err()` once per `ctxCheckInterval` refs via `n % ctxCheckInterval == 0`. The Go compiler
compiles `%const` on a signed int into a magic-multiply + rotate + compare; since the throttle
period is not correctness-relevant (it only bounds cancellation-poll frequency) we round
`ctxCheckInterval` to the nearest power of two (2**17) and test `n & ctxCheckMask == 0` — a single
AND. All counters fed to the test are non-negative (`range` indices or a `spanCount` that starts
at 0 and only increments), so the AND is exactly equal to the former modulo.

Both changes are kernel substitutions across the count/rate, agg, and histogram intrinsic scan
loops — no benchmark-specific constants. **Queries affected:** M1/M4/M6/M9 (count/rate scan
loops, AND-mask) and M8 (histogram boundary lookup + scan loops, both).
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:frexpExpPos`, `pow2Floor`,
`boundaryIndexer.index`, `boundaryIndexer.lookup`, `ctxCheckInterval`, `ctxCheckMask`.

## NOTE-266: hoist per-boundary string formatting out of the group loop in the histogram flat emit

`streamByRefSliceHistogramFlatEmit` walks the flat histogram accumulator as a triple loop over
(group, boundary, timeStep) and builds the composite map key `timeIdx\x00gk\x00boundaryStr` per
non-zero cell. The boundary string `strconv.FormatFloat(boundary, 'g', -1, 64)` depends only on
the boundary index `bIdx`, never on the group or time index, yet it was computed inside the
group loop — recomputing the same `numBoundaries` conversions once per group. For group-by
histogram queries with many output series this is `O(numGroups*numBoundaries)` redundant
FormatFloat calls.

Precompute the boundary strings once into a `[]string` indexed by `bIdx` before the group loop,
then index it in the inner loop. The emitted map keys are byte-identical (same FormatFloat
output, same key layout), so all bucket counts and the downstream `traceHistogramSeries` parsing
are unchanged. This is `O(numBoundaries)` FormatFloat calls total. General to any group-by
histogram_over_time query; no benchmark-specific constants.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:streamByRefSliceHistogramFlatEmit`.

## NOTE-269: rebase the count/rate direct-path bucketByPK array to span only [minPK, maxPK]

`accumulateIntrinsicBucketsDirect` sizes its dense `bucketByPK` (an `int16` keyed by
`packKey = (blockIdx<<16)|rowIdx`) to `maxPK+1` over the time-range slice. A blockpack file
may contain several internal blocks, so any block with `blockIdx>0` puts a `blockIdx<<16`
dead prefix in front of the live keys: the prefix is zeroed on every pooled `acquireDirectInt16`
and is never read, and the live keys sit at a high, sparse offset that thrashes cache during the
per-ref `bucketByPK[pk]` gather in `accumulateCountRateDirect` (the M4/M9 hot loop — `{} | rate()
by` and `{pred} | rate() by` walk every span ref).

For the count/rate path the ONLY pk-keyed structure is `bucketByPK`, and every access in both
`accumulateIntrinsicBucketsDirect` and `accumulateCountRateDirect` is guarded by `[minPK, maxPK]`.
So we track the minimum packKey (`minSlicePK`) in the same single sequential scan that already
finds `maxPK`, set `pkOffset = minSlicePK`, size the array to `maxPK - pkOffset + 1`, and index
it by `pk - pkOffset` everywhere. The oversize cap (`maxDirectArrayEntries`) and the L3-threshold
check (`directAggExceedsL3Threshold`) now operate on the rebased `span = maxPK - pkOffset`, so a
file whose live keys fit but whose raw `maxPK` exceeded the cap can now stay on the fast direct
path instead of falling back to the compact keymap. Output is byte-identical (the rebase is a
pure index shift; the in-range guards are unchanged). The histogram/agg paths share several
pk-indexed dense arrays (`dictByPK`, `seenByPK`) and pass `maxPK` into helper scanners that index
by raw pk, so they keep `pkOffset==0` and remain byte-identical to before.

No benchmark-specific constants — a general structural reduction in zeroing volume, memory
footprint, and gather cache-miss rate on the most common metrics group-by shape.
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect`,
`accumulateCountRateDirect`. Test: `TestAccumulateCountRateDirect_MultiBlock_RebaseByteEquivalence`.

### NOTE-271: reuse one span-ID map across traces in resolveStructuralParentIndices

`resolveStructuralParentIndices` resolves each span's `parentIdx` (the row index of its parent
span within the same trace) by building a `map[[8]byte]int` of span-ID → row-index, then looking
up each span's parent ID. It previously `make`-d a fresh map **per trace**. On a structural query
(the `>>` / `<<` ancestor-descendant operators, e.g. the measured Q9
`{span.kind=server} >> {span.kind=client && ...}`) the `traceSpans` map holds one entry per
matched trace — typically MANY small traces — so the per-trace `make` was one heap allocation
(map header + bucket array) per trace, all on the structural hot path after the block scan.

**Mechanism:** the byID map is scoped strictly within a single trace's resolution — it is fully
populated from that trace's spans, then fully read to set `parentIdx`, before the loop advances to
the next trace. So a single map reused across iterations with `clear()` at the top of each trace
is semantically identical: `clear` is a cheap bucket reset that retains the backing array, so the
map grows once to the largest trace's span count and every later trace reuses that capacity. This
turns N per-trace allocations into one amortized allocation. The `clear` runs BEFORE the populate
loop so the first iteration starts empty regardless of any prior state.

**Correctness:** byte-identical parent resolution. The present-bit gating
(`structuralSpanIDPresent` / `structuralParentIDPresent`) is unchanged — an absent span ID is
never inserted and an absent parent ID is never looked up, exactly as before — and `clear` between
traces guarantees no cross-trace key leakage (a span ID from trace A can never resolve a parent in
trace B). General: no benchmark-specific constants; a pure allocation-count reduction proportional
to the number of matched traces.

**Measured (microbench, 200 traces × 200 spans, where the per-trace map escapes to heap):**
980800 B/op · 600 allocs/op · ~1.35 ms/op → 9352 B/op · 11 allocs/op · ~0.77 ms/op (−99% bytes,
−98% allocs, −43% time). For *small* traces (few spans each — Q9's typical shape) the Go compiler
already stack/fast-path-allocates the small `make(map, len)`, so the prior code reported 0 allocs
and this change is neutral (still 0 allocs) — never a regression. The win materializes on traces
large enough for the byID map to escape (~50+ spans), the worst case for the old per-trace make.
Back-ref: `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices`.
Test: `stream_structural_internal_test.go:TestResolveStructuralParentIndices_MultiTraceNoLeak`.

## NOTE-272: flat-backed groupBuckets matrix for high-cardinality intrinsic group-by aggregation

The general-aggregate (min/max/sum/avg) intrinsic group-by accumulation paths —
`streamAggN1Compact`, `streamAggN1CompactFromRefs`, and `accumulateAggDirect` in
`metrics_trace_intrinsic.go` — materialise a `numGroups × numSteps` matrix of
`*aggBucketState` to hold the per-(group, timestep) accumulator. The previous form
allocated this matrix as `numGroups + 1` separate heap allocations: one outer
`make([][]*aggBucketState, numGroups)` plus one `make([]*aggBucketState, numSteps)` per
group row. On a high-cardinality group-by (hundreds–thousands of distinct group values, the
shape of `... by (<high-card attr>)`) that is `numGroups` individual allocations per query
whose only purpose is to carve the same `numGroups*numSteps` pointer slots.

`makeGroupBuckets` (in `aggbucketstate.go`) allocates one flat backing
`[]*aggBucketState` of `numGroups*numSteps` and reslices each row as a capacity-capped
`flat[off:off+numSteps:off+numSteps]` window, collapsing the matrix to **two** allocations
regardless of group count. Indexing (`groupBuckets[gIdx][bk-1]`) and the nil-cell emit walk
are byte-for-byte unchanged: each row is a `[0:numSteps]` window into the flat array, so
reads/writes land in the same logical cell. The backing is a `[]*aggBucketState`, so every
pointer slot is zero-initialised, preserving the "nil until first write" cell contract the
accumulation and emit loops rely on. The capacity cap means a stray future append on a row
reallocates rather than clobbering the neighbouring row's storage (same safety contract as
the NOTE-150 value/ref arenas).

**Correctness:** identical series output — same cells, same nil-skip emit, same key
formatting (NOTE-245 prefixes). No benchmark-specific constants; a pure allocation-count
reduction proportional to the group count.

**Measured (microbench `BenchmarkIntrinsicMaxGroupBy_HighCard`, 800 groups × many steps):**
~25,619 → ~24,821 allocs/op (≈ −800 allocs/op, exactly the eliminated per-row slice
allocations), 1,467,623 → 1,463,160 B/op; wall-clock within noise (~3.5 ms). For low group
counts the saved allocations are few and the change is neutral — never a regression.
Back-ref: `aggbucketstate.go:makeGroupBuckets`, three call sites in
`metrics_trace_intrinsic.go`. Bench: `intrinsic_group_id_bench_test.go`.

## NOTE-276: arena-allocate the per-cell aggBucketState on group-by accumulation

`makeGroupBuckets` (NOTE-272) collapsed the matrix *scaffold* to two allocations, but every
*occupied* `(group, timestep)` cell still materialised its accumulator with an individual
`&aggBucketState{min: MaxFloat64, max: -MaxFloat64}` on first write. On a high-cardinality
group-by spanning a wide window that is thousands of separate ~80-byte heap objects per
query — each one a distinct object the GC must scan — and is the dominant residual alloc/GC
source on the count/rate-by, general-agg-by and direct-scan-by intrinsic paths.

`bucketArena.alloc()` vends each newly-occupied bucket from a single growing
`[]aggBucketState` slab and returns `&slab[len-1]`. The slab is **never grown in place once
vended from**: when the current slab fills (`len == cap`), the arena allocates a fresh slab
and continues there, so a pointer handed out earlier always remains valid (it points into a
slab that stays reachable through that pointer). This collapses N per-cell allocations into
`ceil(N/arenaSlabCap)` slab allocations — amortised ~one allocation per `arenaSlabCap`
occupied cells.

**Correctness:** each vended bucket is a zero-valued slab element (identical to a fresh
`&aggBucketState{}`); the caller sets `min`/`max` exactly as before. The emit walk and the
downstream `buckets` map hold these interior pointers and keep the slabs alive for the
per-query result lifetime, so there is no dangling reference. The nil-until-first-write cell
contract is unchanged: the matrix pointer slot is still nil until `alloc()` fills it. No
benchmark-specific constants. Threaded `*bucketArena` through `accumulateAggDirectScanCol`.
Back-ref: `aggbucketstate.go:bucketArena`, five matrix-cell sites in
`metrics_trace_intrinsic.go`. Bench: `BenchmarkIntrinsicMaxGroupBy_HighCard`.

---

## NOTE-281: compute the per-series label-string sort key once (decorate-sort) and build it in one allocation

The deterministic output ordering for trace-metrics results (SPEC-ETM-11) sorts the series by
`traceLabelString(Labels)`. All three sort sites (`traceBuildDenseSeries`, `traceHistogramSeries`,
`finalizeCountRateSeries`) passed `traceLabelString` *inside* the `slices.SortFunc` comparator, so
each series' key was rebuilt on every one of the O(n log n) comparisons — roughly `2·n·log n` key
builds for a query with n output series. The 2026-06-13 alloc profile of
`BenchmarkIntrinsicCountRateGroupBy_AllocCount` put `traceLabelString` at ~16% of hot-path
allocations, and it grows with series count (high-cardinality `rate() by`, histogram-over-time
group-by → M4/M6/M8).

**Two changes:**

1. `traceLabelString` now writes the `name=value` pairs (comma-separated) into a single
   pre-sized `strings.Builder` instead of `make([]string, n)` + per-element `name+"="+value`
   concat + `strings.Join`. That collapses n+2 transient allocations per call to exactly one
   (the final string). Byte-for-byte identical output (verified against the old form over
   empty/single/multi/empty-field cases).

2. New `sortSeriesByLabelString` helper does a decorate-sort: it computes each series' key
   exactly once into a single `[]struct{key; series}` backing slice, sorts the pairs by the
   precomputed key, then writes the reordered series back. So key builds drop from O(n log n)
   to n, and the decoration adds only one transient slice regardless of series count (no
   regression for the small-n case — verified: `BenchmarkIntrinsicCountRateGroupBy_AllocCount`
   stays at 25 allocs/op). All three sort sites now call the shared helper.

**Result:** `BenchmarkIntrinsicHistogramGroupBy_AllocCount` (the multi-series, multi-label
histogram path that drives M8) drops from 52 → 42 allocs/op (-19%); the high-card count/rate
microbench drops ~2 allocs/op; the small-series count/rate microbench is unchanged. No
benchmark-specific constants — a general decorate-sort + single-allocation key build.

**Correctness:** the sort order is identical (same key string, same `cmp.Compare`), only the
number of times each key is computed changes. `go test -race ./internal/modules/executor/...`
green. Back-ref: `metrics_trace.go:traceLabelString`, `metrics_trace.go:sortSeriesByLabelString`.

---

## NOTE-337: deduplicate the compact N=1 FromRefs preamble and agg/histogram bodies

Three predicate-filtered compact paths (`streamCountRateN1CompactFromRefs`,
`streamAggN1CompactFromRefs`, `streamHistogramN1CompactFromRefs`) each copy-pasted the same
~12-line preamble that builds `sortedPKs`/`timeBucketByPos` from the already-packKey-sorted
`inRangeRefs`/`inRangeVals` (from `mergeJoinFilteredRefsWithVals`). Because those refs are
pre-sorted, each ref's downstream rank equals its slice index, so unlike the unfiltered
`fillPKSetAndTimeBuckets*` helpers there is no bitset + POPCNT rank scatter — it is a straight
index-for-index fill. Extracted `fillPKSetAndTimeBucketsFromPreSortedRefs`, mirroring the
existing `fillPKSetAndTimeBuckets` / `fillPKSetAndTimeBucketsFromRefs` pattern.

The agg and histogram paths additionally duplicated their entire ~80-line accumulation body
between the unfiltered (`streamAggN1Compact` / `streamHistogramN1Compact`) and FromRefs
variants — the count/rate path had already split this out as `streamCountRateN1CompactCore`,
but agg and histogram had not followed suit. Extracted `streamAggN1CompactCore` and
`streamHistogramN1CompactCore`, each taking `sortedPKs`/`timeBucketByPos` and the agg spec;
both the set-building unfiltered entry point and the pre-sorted FromRefs entry point now call
the shared core. Net ~140 fewer lines, no behavior change.

**Verification:** new equivalence tests `TestStreamAggN1Compact_UnfilteredEqualsFromRefs` and
`TestStreamHistogramN1Compact_UnfilteredEqualsFromRefs` feed both entry points the same logical
spans (FromRefs gets a packKey-sorted copy) and assert identical bucket maps;
`TestFillPKSetAndTimeBucketsFromPreSortedRefs` pins the preamble's index-for-index fill and the
0=out-of-range sentinel. `go test -race ./internal/modules/executor/...` green; `make precommit`
fully green. Back-ref: `metrics_trace_intrinsic.go:fillPKSetAndTimeBucketsFromPreSortedRefs`,
`metrics_trace_intrinsic.go:streamAggN1CompactCore`,
`metrics_trace_intrinsic.go:streamHistogramN1CompactCore`.

## NOTE-339: intrinsic dict-range predicate scan — keep the pure-intrinsic refs fast path usable for numeric range filters
*Added: 2026-06-14*

**Problem.** `scanIntrinsicLeafRefs` (the per-leaf scan behind `BlockRefsFromIntrinsicTOC`)
bailed out with `return nil` for any range predicate (`>`, `>=`, `<`, `<=`) on a
dict-format intrinsic column ("range predicate on dict — not supported in raw scan").
A nil leaf result makes `evalNodeBlockRefs` return `ok=false`, which makes
`BlockRefsFromIntrinsicTOC` abandon the ENTIRE intrinsic pre-filter and fall back to the
full per-block column-decode + scan path. For a filtered rate-by query whose predicate
AND-arm is a numeric range over a low-cardinality dict-encoded numeric column (the dominant
filtered-metrics shape), this meant fetching and decoding the wide block for every block in
the window — the exact wide-block I/O the intrinsic-only refs path exists to avoid.
NOTE-156 had only sped up the *fallback's* per-row compare (`scanNumericDict`), not removed
the fallback; the intrinsic pre-filter still gave up entirely.

**Fix.** When the dict leaf is a range with no explicit value/pattern set AND the column is
an INTEGER-domain numeric type, evaluate the range through the existing dict page scanner
(`ScanDictColumnRefsWithBloom`) with a range matchFn. The scanner already exposes each
DISTINCT dict entry's value (`int64Val` for int64 columns, 8-byte-LE `valueBytes` for
uint64/duration columns) to its matchFn, so the comparison runs once per distinct entry —
a handful of comparisons per page — then matched entries' ref runs are emitted in bulk by
the existing `appendVariableWidthRefs` path. The returned refs are EXACT (the dict scanner
emits refs only for matching entries), so the pure-intrinsic no-VM-re-eval contract holds.

**Correctness gates.**
- `isIntegerDomainColType(meta.Type)` restricts the fast path to int64/uint64/duration dict
  columns. Float64 dict columns encode values as `math.Float64bits` LE — decoding those 8
  bytes as a uint64 integer would compare bit patterns, not magnitudes — and string dict
  columns are not numerically range-comparable; both fall back to the legacy path unchanged.
- `extractDictRangeBounds` accepts only integer/duration literals (rejects string/float/
  vector), normalizes exclusive bounds to inclusive (`>`→`+1`, `<`→`-1`) with overflow
  rejection at MaxInt64/MinInt64, and returns ok=false (skip fast path) for any unencodable
  bound or an empty constraint.
- `dictNumericInRange` compares int64 entries in signed int64 and 8-byte-LE entries in
  unsigned uint64, with the sign-mismatch edge cases handled explicitly: a negative lower
  bound is vacuously satisfied by every non-negative uint64 value; a negative upper bound
  excludes every non-negative uint64 value. A non-8-byte value is never range-comparable
  (returns false). Range predicates cannot use bloom pruning (a range spans many distinct
  values), so nil bloom keys are passed and every page is scanned — identical to the
  non-paged dict path.

**Verified.** `dict_range_test.go` exercises `extractDictRangeBounds` (all four operators,
inclusive/exclusive, overflow rejection, non-integer-literal rejection) and
`dictNumericInRange` against an independent oracle across int64 entries, 8-byte-LE uint64
entries (incl. negative-bound edge cases), and non-numeric entries. Full executor and
blockio suites green under `-race`.

Back-ref: `internal/modules/executor/predicates.go:scanIntrinsicLeafRefs,extractDictRangeBounds,valueToInt64Bound,dictNumericInRange,isIntegerDomainColType`

## NOTE-341: histogram Dict scan shares histRefPassPos — drops loop-invariant bitset guard

`scanAggColHistogramShard` (the per-worker M8 histogram scanner) had two near-identical
per-ref pre-filters: the Flat/Delta arm called `histRefPassPos` (pk-range → bitset
membership → POPCNT rank), while the Dict arm open-coded the same three steps inline and
added a `len(pkBitset) > 0` guard before the membership test.

That guard was loop-invariant **dead code on the hottest M8 loop**: the driver
`scanAggColHistogramCompact` returns early when `len(sortedPKs) == 0` and otherwise always
builds a non-empty `pkBitset` (≥1 bit set, since `maxPK ≥ minPK`) before dispatching any
shard — serial or parallel — and the standalone test callsite builds the bitset the same
way. So `pkBitset` is non-empty at every `scanAggColHistogramShard` entry, exactly the
precondition the Flat arm's `histRefPassPos` already assumed. The branch was evaluated once
per matching ref (millions on M8) for no effect.

Routing the Dict arm through `histRefPassPos` removes that branch and de-duplicates the
`word/bit/rank` math into the one helper. Output is byte-identical: `histRefPassPos` returns
`(pos, true)` exactly when the inline code computed the same `pos` and fell through, and
`(_, false)` exactly when the inline code `continue`d (pk out of range or non-member).

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanAggColHistogramShard,histRefPassPos`

## NOTE-345: per-column intrinsic scatter writes the field directly (no per-entry closure)

`populateTypedColumnForBlock` (the block-boundary intrinsic scatter, NOTE-100) used to
dispatch the uint64 and dict columns through three generic helpers —
`scatterUint64Field` / `scatterDictStringField` / `scatterDictInt64Field` — each taking a
`set func(*intrinsicRowFields, T)` closure that named the destination field. That closure
was invoked on **every scattered entry**: an indirect, non-inlinable call O(spans) times per
column. On the metrics intrinsic path (M4/M6/M8 read `span:duration`, `span:kind`,
`service.name` through these), it ran across every span in every block.

Replaced the three closure-taking helpers with eight dedicated, closure-free scatters
(`scatterSpanStart` / `…SpanEnd` / `…SpanDuration` / `…SpanName` / `…ServiceName` /
`…StatusMessage` / `…SpanKind` / `…SpanStatus`), each writing its destination field and
present bit DIRECTLY — mirroring the already-direct `scatterTraceID` / `scatterSpanID` /
`scatterParentID` byte scatters. This removes the per-entry indirect call and keeps the
bounds-checked loop tight (the compiler can no longer be blocked by the opaque closure).

Dispatch correctness is unchanged: the `colName` switch in `populateTypedColumnForBlock`
selects exactly one scatter, and each new function carries the identical present bit and
reads from the identical source array (`Uint64Values` / `DictEntries.Value` /
`DictEntries.Int64Val`) the former closure call passed. The loop body is otherwise
byte-for-byte the same (the closure was the only difference).

Back-ref: `internal/modules/executor/intrinsic_row_block.go:populateTypedColumnForBlock`

## NOTE-354 (executor side): dense flat scatter fast path in populateTypedColumnForBlock

`populateTypedColumnForBlock` now checks `col.DenseFlatRange(blockIdx)` first. When the column
is flat-dense (shared NOTE-354 dropped its refIndex slice — Pos == rank), it scatters via the
`scatter*Dense(minRow, count, ...)` variants which iterate the synthesized identity range
(rowIdx = minRow+i, valuePos = i) without materializing a `[]RefIndexEntry`. Only the flat
(uint64/bytes) intrinsic columns can be flat-dense (span:start/end/duration, trace:id/span:id/
parent_id); dict columns (span:name/service.name/status/kind) keep refIndex and take the
existing BlockRefRange path. Each `*Dense` body mirrors its general counterpart's per-row body
exactly (same bounds checks, present bit, source array), so output is byte-identical —
verified by TestPopulateTypedColumnForBlock_DenseEqualsGeneral.

## NOTE-355: retention cap on executor per-block scratch pools (bound post-spike RSS)

Every executor scratch pool in `metrics_trace_intrinsic.go` — `groupCountsFlat`,
`compactUint32` / `compactInt32` / `compactUint64` / `compactFloat64` / `compactBool` /
`compactBlockRef`, and `directInt16` / `directBool` — is sized to a block's row count `n`
(up to ~7.2 M) or its primary-key span `maxPK` (up to ~16 M). Their `release*` helpers did
an unconditional `Pool.Put(s[:cap(s)])`, so a single giant block grew the pooled backing
array to tens/hundreds of MiB (compactUint64 ~57 MB at n=7.2 M, groupCountsFlat up to 68 MB,
directInt16 ~32 MB at maxPK=16 M) and pinned it.

`sync.Pool` only ages out entries that sit a full GC cycle UNUSED — but a buffer this large
that keeps getting reused never ages out. Under sustained heavy M4/M6/M8/M9 load these pools
accumulated several giant arrays each (one per P plus the victim cache) and held querier RSS
elevated long after the spike that produced them had drained. The 2026-06-14 querier
inuse_space profile showed `acquireCompactUint64` (545 MB) and `acquireCompactUint32NoClear`
(361 MB) among the top live frames — that is pool residue, not active query working set.

Fix: each `release*` now drops a backing array whose byte size exceeds
`compactPoolMaxPooledBytes` (32 MiB) instead of pooling it, so the array GCs and the next
giant block re-grows from a fresh allocation. The 32 MiB cap covers the direct-aggregation
working set (`maxDirectAggEntries` = 4 M × 8 B = 32 MiB) so common-to-large blocks still
recycle with zero churn; only the p99 giants drop. Per-P × per-pool residue therefore settles
at ≤32 MiB rather than at the peak giant-block size. Mirrors `decompBufMaxPooledCap` (NOTE-346)
and `assembledBufMaxPooledCap` (NOTE-208) on the reader side. Verified by
`TestCompactPools_DropOversizedOnRelease` (over-cap buffers are never returned to their pool;
all capped releases are panic-free for both under- and over-cap inputs).

## NOTE-357: structuralSpanRec field-width narrowing (48 → 28 bytes/record)

`collectBlockStructuralSpanRecs` appends one `structuralSpanRec` per matching span across
EVERY selected block of a structural (TraceQL `>>`/`<`/`~` etc.) query, and retains the
result map (`map[[16]byte][]structuralSpanRec`) for the whole query. For a heavy multi-block
structural scan the record slices are a large per-query peak — it was a top querier
inuse_space frame (~414 MB live in the 2026-06-14 profile).

The record originally used three `int` (8-byte) fields — `parentIdx`, `blockIdx`, `rowIdx` —
giving a 48-byte struct (two `[8]byte` arrays + three `int` + two `uint8`, padded). All three
values have tight, provable bounds:
- `blockIdx`: a file block index, bounded by the file's block count (< 65535) — used elsewhere
  as `uint16(blockIdx)`. Now `uint16`.
- `rowIdx`: a span row index, bounded by `SpanCount` ≤ MaxBlockSpans (65535). Now `uint16`.
- `parentIdx`: an index into the per-trace `spans` slice (count of spans for ONE trace) with
  `-1` as the "no parent" sentinel. Now `int32` (signed, preserves the sentinel; ample range).

Reordered so the two 8-byte arrays lead, then the `int32`, then the two `uint16`s and two
`uint8`s, packing to **28 bytes (-42%)** with no interior padding. Field-width narrowing on a
retained per-row record is a pure inuse_space win: no extra work on the warm path, the slice
just allocates fewer bytes. Mirrors r132's `bucketByPK` int64→int16 narrowing.

Call-site impact: `SpanMatch.BlockIdx/RowIdx` and the `parsedBlocks` map key are `int`, so the
emit path converts back with `int(...)`. The structural-op evaluators index `spans` and several
`map[int]` sets by `parentIdx`; those take an explicit `int(...)` at the use site (indexing a
slice with an `int32` is legal Go, but the `map[int]` keys and `[]int` appends need conversion).

Verified: `BenchmarkStructuralSpanRecSliceFootprint` (100k records) 4.80 MB → 2.80 MB/op
(-41.7%, controlled bytes/op signal); `go test -race ./...` green (excl. pre-existing
env-only cmd/embed-server stress tests that need a live server); `make precommit` fully green.

---

## NOTE-372: Structural Queries Omit Intrinsic Predicate Columns from the Block Fetch (2026-06-14)

**Context:** `collectBlockStructuralSpanRecs` built the `wantColumns` fetch set from
`ProgramWantColumns(prog)` over the FULL compiled program of each structural node. For
intrinsic-section files that set still contained intrinsic columns (`span:kind`,
`span:duration`, `span:status`, `resource.service.name`, `span:name`, …) even though those
columns are served from the warm intrinsic section, NOT block payloads. The parser therefore
fetched and decoded a redundant block-payload copy of every intrinsic predicate column on every
selected block of a structural query — e.g. `span:kind` for `{kind=server} >> {kind=client &&
rpc.method != ""}` — pure wasted column I/O (cold S3 in production, where the page cache is
exhausted) and decode CPU.

**Decision:** For intrinsic-section files, build `wantColumns` from `userAttrProgram(prog)`
rather than the full program. `userAttrProgram` strips intrinsic predicate leaves (it is already
the program used for `ColumnPredicate` evaluation in `evaluateStructuralPrograms`), so
`ProgramWantColumns` on it yields only the genuine user-attribute leaves. Intrinsic predicates
are still enforced — via the per-program `nodesList` post-filter
(`computeNodeMatchForRow → rowSatisfiesIntrinsicNodesTyped`) reading the intrinsic-section
`idFields`. Legacy (no-intrinsic) files keep the full set: with no intrinsic section those
columns must be decoded from block payloads.

**Edge case — all-intrinsic node:** `userAttrProgram(prog)` returns nil when every predicate
leaf is intrinsic (e.g. node 0 `{span.kind=server}`). `ProgramWantColumns(nil)` returns nil and
that node contributes no columns, which is correct — `WantOnly(nil)` decodes no eager columns and
the node's predicate is satisfied entirely from the intrinsic section. If ALL nodes are
intrinsic-only, `wantColumns` stays nil and the parse decodes only the identity/intrinsic data.

**Verified:** `go test -race ./internal/modules/executor/...` green; the parquet parity tests
`TestStructuralParquetComparison` and `TestFormatComparisonCorrectness` (which exercise
multi-block traces and compare blockpack vs vparquet5 span-level results) remain green — proving
the intrinsic post-filter produces identical matches with the reduced fetch set.

Back-ref: `internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`

## NOTE-373: Hoist the structural block plan + flat-accumulate span records (2026-06-14)

**Context:** Two structural-path allocation hotspots on the `collectAllStructuralSpans` loop,
both scaling with the number of selected blocks / matched spans:

1. **Per-block plan rebuild.** `collectBlockStructuralSpanRecs` recomputed, on EVERY selected
   block: the `wantColumns` set (re-running `userAttrProgram` + `ProgramWantColumns` over all
   programs and rebuilding the map — ~16 MB/op in `BenchmarkExecuteStructural_AND_control`),
   the `intrinsicWant` map, and the `nodesList`. All three derive only from `programs` and
   `r.HasIntrinsicSection()` — they are identical for every block of one query.

2. **Per-row result-map append.** The row loop did `result[traceID] = append(result[traceID],
   rec)` per span across all blocks. Every distinct trace's slice started at cap 0 and was
   grown by repeated reallocation, plus a map probe per row. This was the single dominant
   structural allocation — ~368 MB flat (29.8% cum) in the alloc profile.

**Decision:**

1. Compute the block-independent plan ONCE in `collectAllStructuralSpans` via
   `buildStructuralBlockPlan` (returns a `structuralBlockPlan` carrying `programs`,
   `hasIntrinsic`, `wantColumns`, `intrinsicWant`, `nodesList`) and pass `*structuralBlockPlan`
   read-only into `collectBlockStructuralSpanRecs`. The NOTE-372 intrinsic-column-omission logic
   moved verbatim into `buildStructuralBlockPlan`.

2. Accumulate records into a single FLAT `[]structuralSpanRec` across all blocks
   (`flat = append(flat, rec)`), carrying the trace ID on the record (`structuralSpanRec.traceID`,
   NOTE-357 struct widened 32→44 bytes). After all blocks, `groupStructuralRecsByTrace` does one
   counting pass to size each trace's window exactly, then one fill pass scattering records into
   non-overlapping cap==len windows carved from a single shared backing array. This replaces
   O(spans) per-trace slice growations + per-row map probes with one contiguous allocation and
   exactly `len(traces)` final sub-slices. Downstream (`resolveStructuralParentIndices`,
   `evalStructuralMatches`) is unchanged — it still receives `map[[16]byte][]structuralSpanRec`
   with each trace's records contiguous.

**Trade-off:** the record widened 12 bytes (the `[16]byte` trace ID), so total bytes/op rose
slightly (~4.3→4.6 MB in the bench), but allocation COUNT dropped ~40% (3264→1970 allocs/op on
AND_control, 3337→2020 on OR_LHS) — the dominant GC-pressure metric on this path. The widened
backing array is a single contiguous allocation, not N growing per-trace slices.

**Also fixed:** the `lookupIntrinsicFieldsTypedForBlock` error path now releases the block column
provider before returning (it previously leaked the pooled provider on that error branch).

**Verified:** `go test -race ./internal/modules/executor/...` green incl.
`TestStructuralParquetComparison`, `TestFormatComparisonCorrectness`, and the
`TestResolveStructuralParentIndices_*` (multi-trace no-leak) suite — grouping preserves
per-trace contiguity and parent resolution is identical.

Back-ref: `internal/modules/executor/stream_structural.go:buildStructuralBlockPlan`,
`groupStructuralRecsByTrace`, `collectBlockStructuralSpanRecs`

## NOTE-375: Window the pkBitset clear + rank build to [minPK, maxPK] (2026-06-15)

**Problem:** the POPCNT rank index (`buildPKRankIndex`, and the inline copy in
`scanAggColHistogramCompact`) sizes `pkBitset`/`rankPrefix` by `maxPK` — the maximum *packKey
value* (`blockIdx*stride + rowIdx`), up to ~16 M → ~256 K words / ~2 MB. The bitset is a
zero-sentinel membership set drawn from a pool, so it was cleared in full on every acquire
(`clear(pkBitset)` over all `nWords`), and the cumulative-popcount `rankPrefix` pass also ran the
full `[0, maxPK]` word span. `runtime.memclrNoHeapPointers` was ~2.2% of querier CPU on the
metrics group-by/histogram paths, and the work was paid per block regardless of how few keys (or
how narrow a high window) the block actually contributed.

**Fix:** every member packKey lies in `[minPK, maxPK]`, and every scan/`rankOf` caller gates
`pk < minPK || pk > maxPK` *before* touching `pkBitset[word]`/`rankPrefix[word]`, so words below
`minWord = minPK>>6` are never read. Clear only `pkBitset[minWord : maxWord+1]` (leaving the
stale low words untouched — they are never read) and build `rankPrefix` cumulatively *within* the
window (`rankPrefix` resets to 0 at `minWord`). A member's rank still equals its 0-based position
among all members because no member lives below `minWord`, so the stale low/high words contribute
nothing to any rank that is actually looked up.

**Correctness:** `minPK` is derived by an O(N) scan of the key set (no sorted-input assumption,
matching the existing NOTE-223/225 behavior). For a single-key set `minWord == maxWord` and the
window is one word. `rankPrefix[maxWord+1]` still holds the total member count.

**Verified:** `go test -race ./internal/modules/executor/...` green incl. the existing
count/rate, agg, and histogram group-by equivalence suites (which already cover sparse, high-PK,
and single-block key sets). Cold/warm behavior unchanged — this is pure scratch-buffer hygiene.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildPKRankIndex`,
`scanAggColHistogramCompact`

## NOTE-376: Dense bitset match-set in collectIntrinsicTopKScan (2026-06-15)

**Problem:** `collectIntrinsicTopKScan` (the large-M intrinsic top-K path, taken when a
single-equality intrinsic predicate like `resource.service.name = X` matches more than
`SortScanThreshold` refs) built a `map[uint32]struct{}` over ALL M matching packed
(`blockIdx<<16|rowIdx`) refs, then scanned the entire timestamp blob calling one map lookup
per row. For a high-cardinality intrinsic equality covering a large fraction of the shard's
spans, M approaches the full span count: the hash map costs ~48 B/entry plus rehash growth on
build, and a hash+probe per row on the subsequent O(N) blob scan. This dominated the slow
high-match search (a `resource.service.name` equality ran ~18x slower than a low-cardinality
`span.kind` equality in the 3h benchmark).

**Fix:** replace the map with `packedRefSet`, a dense `[]uint64` bitset offset by the minimum
packed key. Packed keys are a bounded 32-bit value; for the single-block query-frontend shard
they span the tight contiguous range `[block<<16, block<<16+maxRow]`, so offsetting by `minPK`
sizes the bitset to `(maxPK-minPK+1)` bits (~`spanCount/8` bytes) no matter how high the block
index sits. Build is a sequential `words[pk>>6] |= 1<<(pk&63)` per ref; membership is a
branch-free word load + shift gated on `pk >= offset` and `word < len(words)`. Both are strictly
cheaper than the hash map on the dominant dense path. The backing `[]uint64` is pooled
(`packedRefSetWordsPool`); on a reused buffer only the in-range words are `clear()`'d because the
set is a zero-sentinel membership set and every member maps into `[0,nWords)`. Oversized buffers
(> `packedRefSetWordCap` = 8 MiB) are dropped on release to bound the pool footprint.

**Correctness:** `contains` returns false for any packed key below `offset` or beyond the word
span, so keys not present in the original ref set (which are exactly the keys outside
`[minPK,maxPK]` plus the unset bits inside it) are correctly non-members — identical to the map's
`_, found := matchSet[key]`. `release()` is deferred so the buffer returns to the pool on every
exit path; a zero-value set (empty refs) has nil words and both `contains` and `release` no-op.

**Verified:** `go test -race ./internal/modules/executor/...` green incl. new
TestPackedRefSet_MatchesMap (random ref sets, asserts `contains` == map membership across the
full key space, run twice through the pool so pass 2 acquires a dirty buffer). Cold/warm behavior
unchanged — this is membership-structure substitution on the existing scan path.

Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopKScan`, `packedRefSet`

## NOTE-377: Sort-group structural records + pre-size the flat record slice (2026-06-15)

**Problem:** the structural query path (`{...} >> {...}` etc.) builds a flat `[]structuralSpanRec`
across all selected blocks, then groups it by 16-byte trace ID for per-trace parent resolution and
operator evaluation. A CPU profile (process_cpu, 30m, 2026-06-15) put two of its frames among the
top blockpack self-time sinks:

- `collectBlockStructuralSpanRecs` → `runtime.growslice` ~0.98%: `flat` started at nil and grew by
  repeated reallocation+memmove as each block appended its per-row records.
- `groupStructuralRecsByTrace` → `runtime.mapassign` ~0.80%: the grouping ran two full passes over
  `flat`, each hashing the 16-byte trace ID per record — a counting pass (`counts[traceID]++`,
  mapassign) plus a fill pass whose `out[tid] = append(...)` did a map lookup AND a slice-header
  store per record. The rebuilt `out` map plus a separate `backing` array also doubled the
  grouping's allocation.

**Fix (two parts):**

1. *Pre-size `flat`.* `collectAllStructuralSpans` now sums `BlockMeta(blockIdx).SpanCount` over the
   selected blocks (read from the ToC, no parsing) and allocates `flat` with that exact capacity.
   Each block appends at most `SpanCount` records (one per row carrying a trace ID), so the sum is a
   tight upper bound; the single allocation eliminates the growslice/memmove chain. Rows without a
   trace ID never append, so the slice may finish shorter than capacity — the surplus is untouched.

2. *Sort-group instead of map-group.* `groupStructuralRecsByTrace` now `slices.SortStableFunc`s
   `flat` by trace ID (compared as two big-endian uint64 halves via `compareTraceID` — no slice
   headers, no per-byte loop, identical order to `bytes.Compare`) so records of one trace become
   contiguous, then carves the runs into windows aliasing `flat` directly. It returns
   `[][]structuralSpanRec` instead of `map[[16]byte][]structuralSpanRec`. No per-record map
   operation, no second backing array. The downstream consumers
   (`resolveStructuralParentIndices`, `evalStructuralMatches`) treat each trace window
   independently and do not depend on inter-trace ordering; `evalStructuralMatches` recovers the
   trace ID from `spans[0].traceID` (every record in a window shares it) instead of a map key.

**Correctness:** stability of the sort preserves the original (block, row) order within each trace.
Parent-index resolution keys on span ID and is order-independent, so even an unstable sort would be
correct, but stability avoids any behavioral surprise. Big-endian uint64 decode makes the integer
compare match the byte-lexicographic compare, so the grouping is independent of trace-ID layout.
A nil `flat` (no selected blocks) yields a nil window slice, which both consumers range over safely.

**Verified:** `go test -race ./internal/modules/executor/...` green incl. all
TestResolveStructuralParentIndices_* (root/chain/multi-trace-no-leak/duplicate/absent),
TestExecuteStructural_* (descendant/child/sibling/ancestor/parent/negations/multi-block/3-node-chain),
TestEvalStructuralMatches_Dedup, TestResolveStructuralParentIndices_ZeroStringAllocs (≤ numTraces+2
allocs holds — the byID map is the only alloc). `make precommit` fully green (deadcode,
fieldalignment, staticcheck clean). Microbench BenchmarkExecuteStructural: AND_control
2172904→1770342 ns/op (-18.5%), 4960611→4244548 B/op (-14.4%); OR_LHS 2270758→1721343 ns/op
(-24.2%), 5158716→4008297 B/op (-22.3%); OR_RHS 1977471→1921690 ns/op (-2.8%), 5403500→4508550
B/op (-16.6%).

Back-ref: `internal/modules/executor/stream_structural.go:collectAllStructuralSpans`,
`groupStructuralRecsByTrace`, `resolveStructuralParentIndices`, `evalStructuralMatches`,
`compareTraceID`

## NOTE-378: Hoist per-row bounds checks out of the dense scatter loops (2026-06-15)

`populateTypedColumnForBlock` routes flat-dense intrinsic columns (NOTE-354) through the
`scatter*Dense` family, which `lookupIntrinsicFieldsTypedForBlock` calls once per intrinsic
column per block over the full SpanCount row range on the structural path. Each former loop
body paid two bounds checks per row — `rowIdx >= len(result)` and `i >= len(vals)` — that the
compiler could not eliminate, so the checks ran O(SpanCount × columns) times per block.
`populateTypedColumnForBlock` was the top executor self-time frame (0.79% of querier CPU,
gcx profile 2026-06-15).

The dense invariant (NOTE-354) is `rowIdx == minRow + i` and value position `== i` for
`i in [0, count)`. Since `minRow`, `count` and both slice lengths are known up front, the
safe iteration count `n = min(count, len(vals), len(result)-minRow)` is computed once in
`denseScatterBound`, which also returns the destination sub-slice `result[minRow:minRow+n]`
aligned so that `res[i]` corresponds to source position `i`. With both `res` and the source
window re-sliced to length `n`, `res[i]` and `src[i]` are provably in range for `i < n`, so
the compiler discharges both bounds checks from the loop body — leaving a tight typed write.

**Correctness:** the clamp preserves the former per-row `continue` exactly. Rows where
`rowIdx >= len(result)` or `i >= len(vals)` were skipped before; now they are simply not
iterated (n stops at the shorter bound). When `minRow >= len(result)` (degenerate, not
expected on a valid dense column) `n` clamps to 0 and the loop is skipped, matching the
former all-`continue` behavior. The non-dense (`BlockRefRange`) path is untouched.

**Verified:** `go test -race ./internal/modules/executor/...` green incl. all
TestStructural/TestExecuteStructural/TestResolveStructuralParentIndices. `make precommit`
fully green (deadcode, fieldalignment, staticcheck clean).

Back-ref: `internal/modules/executor/intrinsic_row_block.go:denseScatterBound`,
`scatterTraceIDDense`, `scatterSpanIDDense`, `scatterParentIDDense`, `scatterSpanStartDense`,
`scatterSpanEndDense`, `scatterSpanDurationDense`

## NOTE-379 — DeltaUint64 sorted fast path in countIntrinsicHistogramBoundaries

`countIntrinsicHistogramBoundaries` sizes the M8 histogram accumulator (`groupCountsFlat`)
and is called once per accumulate. Its generic implementation allocates a 2100-element
`make([]bool, expHi-expLo)` dedup table on every call and stores/probes it once per value.
A 2026-06-15 querier CPU profile attributed ~0.30% self-time to this function, dominated by
that per-call allocation on the M8 `histogram_over_time(duration) by (...)` path.

DeltaUint64 columns are sorted ascending (NOTE-123) and the boundary exponent
(`frexpExpPos`) is monotonically non-decreasing in the value, so distinct boundaries appear
in a single contiguous run as the scan advances. A serial walk that remembers only the
previous boundary slot therefore counts distinct boundaries exactly — no dedup table, no
per-value table store/probe. The `1/1e9` nano scale is order-preserving so the post-scale
sequence is still sorted and the monotonicity argument holds; `v<=0` (only `u==0` here) maps
to the single boundary-0 bucket tracked by a one-shot `zeroSeen` flag. The `histFlatStride`
overflow cap is preserved.

This removes the allocation and the table traffic for the heaviest metrics query's count
pass. Output is byte-identical to the generic `seenExp` count for every sorted-ascending
input (verified against a 2000-iteration random-sorted parity harness and the existing
`duration_delta_sorted_ascending` case). The Dict and Flat arms (unsorted) keep the generic
`markValue`/`seenExp` path unchanged.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:countIntrinsicHistogramBoundaries`

## NOTE-380 — Unstable sort in groupStructuralRecsByTrace (2026-06-15)

`groupStructuralRecsByTrace` sorts the flat cross-block `[]structuralSpanRec` by 16-byte trace
ID so each trace's records form a contiguous run. It previously used `slices.SortStableFunc`,
which takes the symmerge path (`rotateCmpFunc` + `symMergeCmpFunc` were ~1.7% combined querier
self-CPU on the structural Q9 `{kind=server} >> {kind=client}` path, gcx profile 2026-06-15) to
preserve the original (block, row) order within each trace.

That intra-trace order is never observed: `resolveStructuralParentIndices` builds a per-trace
`map[[8]byte]int` (spanID → local index) and resolves parent indices order-independently, and
`evalStructuralMatches` re-sorts and dedups the matched right indices (`slices.Sort(rightIndices)`)
before emitting matches. Only contiguity by trace ID matters, not order within a trace.

Switched to `slices.SortFunc` (unstable pdqsort, O(n log n), no merge-buffer rotations). The run-
carving loop is unchanged — it still groups equal-trace-ID runs into windows aliasing `flat`.
Trace IDs are distinct across traces, so there are no equal full keys whose relative order a
stable sort would protect; the only "equal" comparisons are within one trace, exactly the order
nothing downstream depends on.

Back-ref: `internal/modules/executor/stream_structural.go:groupStructuralRecsByTrace`

## NOTE-381 — Decorate-sort-undecorate for MatchedRow timestamp ordering (2026-06-15)

Two identical sort sites in `stream.go` (the `forEachBlockInGroups` re-sort after the scan and
KLL intrinsic top-K paths) re-ordered `[]MatchedRow` by their timestamp column to restore the
newest-first ordering that block reads destroy. Both used `slices.SortStableFunc` with a
comparator that called `Block.GetColumn(tsColumn).Uint64Value(RowIdx)` for **both** operands on
**every** comparison — O(n log n) lazy-column probes per sort. Each `Uint64Value` is not free:
`needsDecode` check, `expandDenseIdx`, `IsPresent`, `dictIdxAt` (packed-index lookup), and a dict
array index. The stable comparator also routed through the symmerge path (`rotateCmpFunc` +
`symMergeCmpFunc` + `swapRangeCmpFunc` ~2.8% combined querier self-CPU, gcx 2026-06-15).

Replaced with `sortMatchedRowsByTimestamp`, a decorate-sort-undecorate:
1. Read each row's timestamp **once** into a parallel `[]uint64` keys slice — O(n) column probes.
2. Sort an index permutation (`[]int`) by the precomputed keys via `slices.SortFunc` (unstable
   pdqsort). The comparator is a single `cmp.Compare(keys[a], keys[b])` — no column access.
3. Gather rows into sorted order through the permutation.

Column probes drop from O(n log n) to O(n); the comparator is an integer compare. Stability is
not needed: the pre-sort order is `forEachBlockInGroups`' (BlockIdx, RowIdx) layout, which is not
a meaningful tie-break for a timestamp ordering — equal-timestamp rows have no semantic order to
preserve. The two call sites now share one helper instead of duplicating the comparator.

Back-ref: `internal/modules/executor/stream.go:sortMatchedRowsByTimestamp`

### NOTE-382: skip parent-index resolution for traces that cannot match

`ExecuteStructural` ran three phases over the collected per-trace span windows:
`resolveStructuralParentIndices` (build a per-trace spanID→rowIndex map, then resolve every
span's `parentIdx`), then `evalStructuralMatches` (gated per trace by `traceCanMatch`, a cheap
OR over the per-span `nodeMatch` bits that returns false when the trace cannot possibly satisfy
the chain). The resolution phase ran **unconditionally** over every trace window — including the
many traces whose spans match no relevant node — even though `evalStructuralMatches` then
immediately discards those same traces on the identical `traceCanMatch` check before reading
`parentIdx`.

On the structural workload this is pure waste: the measured Q9
(`{span.kind=server} >> {span.kind=client && span.rpc.method != ""}`) returns **0 traces** over a
large span population, so every trace window paid a `clear(byID)` + a populate pass + a
resolve pass (with a map probe per span) whose result was never observed.

**Mechanism:** thread `ops` into `resolveStructuralParentIndices` and run the same
`traceCanMatch` predicate FIRST, skipping the map work for any trace that cannot match. The skip
is sound because the resolved `parentIdx` of a skipped trace is never read — `evalStructuralMatches`
applies the identical `traceCanMatch` gate and `continue`s before touching `parentIdx`. The check
is a branch-free bitmask OR over `nodeMatch`; it adds nothing to traces that DO match (they were
already going to be scanned by the same predicate in the eval phase). A `nil` ops disables the
skip and resolves every trace unconditionally — used by the resolution-only unit tests that
exercise parent linking directly without populating `nodeMatch`.

**Correctness:** byte-identical results. Matching traces resolve exactly as before; non-matching
traces produce no output in either the old or new code (the eval-phase gate is unchanged). No
benchmark-specific constants — `traceCanMatch` derives its required-node bitmask purely from the
operator chain length and negation shape.

Back-ref: `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices`,
`ExecuteStructural`, `traceCanMatch`.

### NOTE-383: carry the qualified-trace subset out of resolution to drop the duplicate scan

`traceCanMatch` (an OR over every span's `nodeMatch` bits) was evaluated TWICE per trace:
`resolveStructuralParentIndices` ran it to gate parent resolution (NOTE-382), and
`evalStructuralMatches` ran it again to gate emission. On the dominant structural case — a query
where most traces contain no matching span (Q9 `{kind=server} >> {kind=client}` returns 0 traces
over a large span population) — this duplicated O(spans) work for every trace.

Fix: `resolveStructuralParentIndices` now compacts `traceSpans` IN PLACE (order-preserving, reusing
the backing array — no allocation) to the subset of traces that pass `traceCanMatch`, and returns
that prefix. `ExecuteStructural` rebinds `traceSpans` to the returned subset. `evalStructuralMatches`
no longer calls `traceCanMatch` at all — every trace it receives is already known to qualify, so the
second full scan is eliminated. When `ops == nil` (resolution-only unit tests) the gate is disabled
and the input slice is resolved and returned unchanged.

Correctness: the compaction predicate is identical to the gate `evalStructuralMatches` previously
applied, so the set of traces processed for emission is byte-for-byte the same; only the redundant
re-scan is removed. No benchmark-specific constants — `traceCanMatch` derives its required-node
bitmask purely from the operator chain length and negation shape.

Back-ref: `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices`,
`ExecuteStructural`, `evalStructuralMatches`.

### NOTE-384: pre-size the per-trace spanID→index map to the widest trace window

`resolveStructuralParentIndices` allocates `byID` ONCE and `clear()`s it per trace (NOTE-271), but
the map was created with no capacity hint. Go's map grew incrementally on the first trace it filled,
rehashing each time it crossed a load-factor threshold; the grown bucket array then survives the
`clear()` and is reused by later traces. Sizing the hint up front to the largest per-trace window
(an O(traces) scan over slice headers — no per-span work) means even the widest trace inserts its
spanID keys without a single rehash. The hint is an upper bound (a trace may carry fewer
span-ID-present rows than its length), never an under-size, so it cannot cause spill.

Back-ref: `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices`.

### NOTE-385: reuse one right-index scratch buffer across traces in the single-op path

The single-op structural evaluators (`evalOpDescendantStruct`, `evalOpChildStruct`, …) each
allocated `make([]int, 0, len(spans))` PER qualified trace — sized to the full per-trace span count
even though the realised match set is typically tiny or empty. With many qualifying traces this is
O(traces) allocations, each at span-count capacity.

Fix: `evalStructuralMatches` holds a single `scratch []int` and threads `scratch[:0]` into
`applyStructuralOps` → `applyStructuralOp` → the per-op evaluator, which appends into it instead of
allocating. After each trace `scratch` is rebound to the returned (possibly grown) slice so the next
trace reuses the high-water-mark backing array. The buffer is fully consumed — sorted, deduped, and
emitted — before the next trace overwrites it (`evalStructuralMatches` reads every `rightIndices`
entry into `result.Matches` within the same loop iteration), so no emitted match aliases it. The
N>1 chain path (`evalOpChain`) uses map-set semantics and ignores the buffer; only the dominant
2-node path is affected.

Back-ref: `internal/modules/executor/stream_structural.go:evalStructuralMatches`,
`applyStructuralOps`, `applyStructuralOp`, and the eight single-op evaluators.

### NOTE-386: drop whole non-matching traces before the structural group-by sort

`groupStructuralRecsByTrace` sorts the FULL flat span population by 16-byte trace ID (O(n log n)
two-uint64 compares) to carve contiguous per-trace windows. On the dominant structural workload
most traces match no node predicate at all — the measured Q9 (`{a} >> {b}`) returns 0 traces over a
large span population — so the entire sort is spent ordering records that the downstream
`traceCanMatch` gate (NOTE-382/383) will discard anyway.

Fix: `compactMatchingTraces` runs once before the sort. Pass 1 records every trace ID that carries
at least one matched span (`nodeMatch != 0`) into a set — one map insert per matched span, cheap
when few match. Pass 2 keeps only records whose trace ID is in that set, compacting `flat` in place
(order-preserving, aliases the same backing array). The sort then runs over only the surviving
records.

Correctness: a trace can satisfy ANY structural op only if some span matched a node. For positive
ops the chain requires every node bit present (a superset of "≥1 matched span"); for negation ops
`traceCanMatch` requires the RHS bit `0x02`, itself a nonzero `nodeMatch`. So "trace has ≥1 span
with `nodeMatch != 0`" is a conservative superset filter for every op type — it never drops a trace
that could match. Whole traces are kept or dropped as a unit: a kept trace retains ALL its spans,
including `nodeMatch==0` intermediate ancestors, so the parent-topology chain walked by
`resolveStructuralParentIndices` and the `>>`/`<<` ancestor-chain evaluators stays intact. The
all-unmatched case (`len(matched)==0`) returns `flat[:0]`, skipping the sort entirely.

Back-ref: `internal/modules/executor/stream_structural.go:compactMatchingTraces`,
`collectAllStructuralSpans` (call site before `groupStructuralRecsByTrace`).

### NOTE-387: fuse the matched-trace filter and group-by into a counting bucket scatter

NOTE-386 already paid one full hashing pass over the flat span population to build the
matched-trace set (`compactMatchingTraces`), then ran a second pass to drop non-matching records
in place and handed the survivors to `groupStructuralRecsByTrace`, which performed an O(n log n)
16-byte trace-ID sort to carve contiguous per-trace windows. That is: 1 hashing pass + 1
compaction pass + 1 comparison sort.

Fix: `groupMatchingStructuralTraces` replaces both `compactMatchingTraces` and
`groupStructuralRecsByTrace` with a single counting bucket scatter — no comparison sort. It reuses
the matched-trace hashing pass and turns it into a dense bucket grouping:

- Pass 1 assigns each trace carrying ≥1 matched span (`nodeMatch != 0`) a dense slot index on
  first sighting (`buckets map[[16]byte]*structuralTraceBucket`). `len(buckets)==0` ⇒ no trace can
  match ⇒ return nil (no scatter, no backing allocation).
- Pass 2 counts survivors per matched trace. Every span of a kept trace survives (including the
  `nodeMatch==0` intermediates the parent-topology chain needs), so the count is incremented for
  every record whose trace is in the set.
- A prefix sum over the per-slot counts produces bucket start offsets; each cursor is reset to its
  window start.
- Pass 3 scatters survivors directly into trace-contiguous windows of one freshly sized backing
  array. Non-matching records are never copied.

This replaces (compaction pass + O(n log n) sort) with (one scatter pass + one backing alloc). The
windows alias the new backing array, each clamped to its run length (`backing[lo:hi:hi]`) so a
downstream append could not bleed into the next trace. Inter-trace ordering is slot order
(first-matched-span order) instead of sorted-ID order, which is fine: `resolveStructuralParentIndices`
and `evalStructuralMatches` treat each trace independently and re-sort the matched right indices per
trace before emitting, so neither inter-trace order nor intra-trace order reaches the result.

Correctness of the "≥1 matched span" filter is unchanged from NOTE-386: it is a conservative
superset for every op type including negation (RHS bit `0x02` is itself a nonzero `nodeMatch`), and
whole traces are kept/dropped as a unit so intermediate ancestors stay intact.

Back-ref: `internal/modules/executor/stream_structural.go:groupMatchingStructuralTraces`,
`collectAllStructuralSpans` (single call site). Replaced and removed `compactMatchingTraces`,
`groupStructuralRecsByTrace`, and `compareTraceID` (NOTE-380's sort comparator, now dead).

### NOTE-391: reconstruct outRefs from the packKey instead of a second random gather

`mergeJoinFilteredRefsWithVals` was the #1 executor self-time frame after the decode hot loop
(4.64% of querier CPU, gcx 2026-06-15 24h M8 window). Its dominant cost is the final gather loop,
which walks `matched` (sorted by packKey via `radixSortByPackKey`) and reads
`inRangeRefs[pos]` and `inRangeVals[pos]`. Both `inRangeRefs` and `inRangeVals` are
**timestamp-sorted** (the caller slices `tsCol.BlockRefs[lo:hi]` / `tsVals[lo:hi]`, types.go:228),
not packKey-sorted, so after the radix sort `pos` jumps around — two independent cache-missing
random streams per match.

Fix: `matched[i] = packKey<<32 | pos`, and a `BlockRef` is exactly `{BlockIdx uint16, RowIdx uint16}`
with `packKey == BlockIdx<<16 | RowIdx` (blockref.go, packKey). The high 32 bits of each `matched`
entry therefore ARE the output BlockRef's bit layout, and `matched` is already ordered by that high
word. So `outRefs` is reconstructed directly from the packKey (sequential read of `matched`,
zero extra memory traffic) instead of re-fetching `inRangeRefs[pos]`. Only `inRangeVals[pos]`
remains a random read — the random-access pressure of the gather is halved. Output is byte-identical:
the reconstructed BlockRef equals `inRangeRefs[pos]` by construction (its packKey was what put it
in the bitset), validated by `TestMergeJoinRefs_CorrectnessVsMap` (incl. shuffled inputs and
duplicate packkeys, output compared against an independent map reference).

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:mergeJoinFilteredRefsWithVals`.

---

## NOTE-392: Memoize the node-0-ancestor existence walk in >> / !>> (2026-06-15)

**Context:** `evalOpDescendantStruct` (`>>`) and `evalOpNotDescendantStruct` (`!>>`) each test, per
RHS span, whether any strict ancestor carries the node-0 (LHS) bit by walking the span's parent
chain (`parentIdx`) up to the root or first match. When many RHS spans share an ancestor subtree
(a deep trace, or a wide fan-out under a common ancestor), the same chain prefix is re-walked for
every descendant — O(RHS × depth). A single 2000-span chain (every span a descendant of one LHS
root) cost ~4.55ms per evaluation in the microbench.

**Decision:** Memoize the result with a per-trace tri-state scratch slice `memo []uint8`
(0=unknown, 1=has-node-0-ancestor, 2=no-node-0-ancestor). `hasNode0AncestorMemo` walks up,
stopping early on a cached node or an LHS-matching ancestor, then backfills `memo` for every
previously-unknown node on the walked path with the resolved outcome. Each distinct span node is
therefore visited at most once across the whole trace, making both evaluators O(spans) amortized.
The same helper serves both ops (`!>>` is the negation of the same predicate; its prior per-span
`leftSet map[int]struct{}` was redundant with `nodeMatch&0x01` and is dropped).

**Result:** Deep-chain microbench `BenchmarkEvalOpDescendant_Deep` (2000-span chain): ~4.55ms →
~7.8µs (~580x). Shallow/flat traces (the common 20-span case) are unchanged at ~97ns/0 allocs —
the `make([]uint8, n)` memo is stack-allocated by escape analysis when it does not escape, so
small traces pay no allocation. Correctness is byte-identical: all executor `-race` tests pass,
plus `TestStructuralParquetComparison` / `TestFormatComparisonCorrectness` (multi-block fixtures,
parity vs the parquet engine across `>>`, `!>>`, `!~`, etc.).

**Generality:** A pure algorithmic complexity reduction (quadratic → linear) on the ancestor-chain
existence test; no benchmark-specific constants, adapts to any trace shape. Only the existence
operators (`>>`, `!>>`) benefit — `<<` (`evalOpAncestorStruct`) must collect ALL matching ancestors
(not just existence) so it cannot break early and is left unchanged.

Back-ref: `internal/modules/executor/stream_structural.go:hasNode0AncestorMemo`.

## NOTE-393: Memoize the ancestor-chain collection in << (2026-06-15)

**Context:** `evalOpAncestorStruct` (`<<`) is the mirror of `>>`: for each node-0 (LHS) match it
walks the `parentIdx` chain to the root, appending every node-1 (RHS) ancestor it crosses. Unlike
`>>` (existence), `<<` must collect ALL matching ancestors, so NOTE-392's early-break existence
memo did not apply and it was left unchanged. But the collect-all walk is still quadratic when
L-matches share an ancestor-chain prefix: a deep trace, or many leaf L-matches under one common
ancestor, re-walks the same prefix — and re-appends every node-1 node on it — once per L-match,
O(L × depth). A 2000-span chain where every span is both an L-match and an RHS candidate cost
~19.5ms and **~82 MB** of result-slice growth (the duplicate appends pile up before dedup).

**Decision:** A node X's strict-ancestor chain is identical for every span that walks through X.
Once any L-walk has entered X and ascended to the root, every node-1 ancestor strictly above X is
already in `result`. So maintain a per-trace `collected []bool`: a walk sets `collected[cur]=true`
the moment it *enters* `cur` (before emitting `cur`'s own node-1 bit and before ascending), then a
later walk that reaches an already-`collected` node breaks immediately. The downstream emit
(NOTE-079) sorts + dedups `rightIndices`, so the overlap a shared subtree would otherwise duplicate
is collapsed — output is byte-identical while each parent edge is traversed at most once globally →
O(spans) amortized. The first walk to reach X still emits X (if node-1) and seeds `collected` for
the whole prefix above; subsequent walks halt at X having already had those ancestors emitted.

**Result:** `BenchmarkEvalOpAncestor_Deep` (2000-span chain, every span L+RHS): ~19.5ms → ~17µs
(~1140x); allocs/result memory 82 MB → 62 KB (the result slice stops growing quadratically). All
executor `-race` tests pass, including `TestStructuralParquetComparison` /
`TestFormatComparisonCorrectness` (multi-block parity vs the parquet engine across structural ops).

**Generality:** Pure algorithmic complexity reduction (quadratic → linear) on the ancestor-chain
collection; no benchmark-specific constants. Completes the structural-op memoization pair started in
NOTE-392 (`>>`/`!>>` existence), now extended to `<<` collect-all. `<` (`evalOpParentStruct`) is a
single-hop test and needs no memo.

Back-ref: `internal/modules/executor/stream_structural.go:evalOpAncestorStruct`.

## NOTE-396: Galloping boundary count over sorted DeltaUint64 histogram values (2026-06-15)

`countIntrinsicHistogramBoundaries` is called once per accumulate to count the distinct histogram
boundary buckets so `streamHistogramN1CompactCore` can size `groupCountsFlat`
(`numGroups*actualStride*numSteps` int64). NOTE-352 replaced the per-value `map[float64]struct{}`
with an exponent-keyed dense table; NOTE-379 added a sorted-ascending DeltaUint64 fast path that
drops the dedup table. But that fast path still walked EVERY value (one `frexpExpPos` per row),
so on the M8 duration column it is an O(rows) pass over millions of rows whose ONLY purpose is to
size a buffer — separate from, and on top of, the real histogram scan. The 24h CPU profile put
`countIntrinsicHistogramBoundaries` self-time at ~1% of total querier CPU.

**Fix:** the boundary slot (`frexpExpPos` of the scaled value) is monotonically non-decreasing in a
sorted-ascending column, so distinct boundaries appear in contiguous runs. Instead of stepping
through every value, gallop: from each run start, exponentially probe forward while the slot is
unchanged, then binary-search the first differing index, and jump there. The whole column is counted
in O(distinct_slots * log(run_len)) value reads instead of O(rows); distinct_slots is capped at
`histFlatStride` (64), so this is at most ~64*log(n) `frexpExpPos` calls regardless of column size.
The leading `v<=0` run is a contiguous prefix for sorted data and is skipped the same way (single
zero bucket). Extracted into `countDeltaHistogramBoundariesGallop` to keep the parent under the
gocyclo budget.

**Correctness:** the count is identical to the linear walk for every input — it counts exactly the
distinct slots present plus one for the zero prefix. Covered by
`TestCountIntrinsicHistogramBoundaries_DeltaGallopMatchesReference` (4000 randomized sorted-ascending
columns: leading-zero prefixes of varied length, zero-delta same-exponent runs, wide exponent jumps,
cap-exceeded, duration + non-duration fields) asserting equality against the map-cardinality
reference, plus the existing `_MatchesMapReference` cases. All executor `-race` tests pass.

**Generality:** pure algorithmic complexity reduction (O(rows) → O(distinct*log)) over sorted data;
no benchmark-specific constants. Sizing-only, so it cannot affect query results.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:countDeltaHistogramBoundariesGallop`.

## NOTE-397: Hoist the per-position invariant scatter base out of the M8 histogram scan (2026-06-15)

The hottest M8 loop is the scatter write in `scanAggColHistogramShard`, executed once per matching
ref (O(SpanCount) per block). It wrote:

    groupCountsFlat[gIdx*stride1 + bIdx*stride2 + (bk-1)]++

where `gIdx = dictIdxByPos[pos]-1` (or 0) and `bk = timeBucketByPos[pos]`. Both `dictIdxByPos` and
`timeBucketByPos` are read-only, query-wide arrays keyed by the rank position `pos`, so the entire
`gIdx*stride1 + (bk-1)` term — and the `bk==0` skip — depend ONLY on `pos`. They are *constant*
across the (often many) refs that resolve to the same pos, yet were recomputed per ref: a multiply,
two array reads, and two branches (`bk==0`, `dictIdxByPos>0`) on every iteration.

**Fix:** `buildHistBaseOffsets` precomputes `baseOffsetByPos[pos] = gIdx*stride1 + (bk-1)` once in
the driver (`scanAggColHistogramCompact`), sized `len(seenByPos)`, shared read-only across all
parallel shards (same pattern as `timeBucketByPos`/`pkBitset`). Positions with `bk==0` get the
`histSkipPos` (-1) sentinel, folding the time-range skip into a single signed compare. The scan
becomes:

    base := baseOffsetByPos[pos]
    if base < 0 { continue }
    ...
    groupCountsFlat[base + bIdx*stride2]++

The arithmetic is byte-identical (`base + bIdx*stride2 == gIdx*stride1 + bIdx*stride2 + bk-1`). The
table is pooled via a new `compactInt64Pool` (acquired NoClear — `buildHistBaseOffsets` writes every
position before any read). The Dict and Flat/Delta arms are updated identically; the serial fallback
and the parallel worker call both pass the shared table.

**Correctness:** identical output — covered by the parallel-vs-serial parity tests
(`intrinsic_hist_parallel_test.go`, updated to build the table) and the full executor `-race` suite
(incl. multi-block parquet parity). The absent-row pass in the caller is unchanged (still reads
`dictIdxByPos`/`timeBucketByPos` once per position, not per ref).

**Generality:** loop-invariant code motion out of the O(SpanCount) hot loop; no benchmark-specific
constants. Cost moves from per-ref to per-position (always ≤ per-ref, since multiple refs share a
pos). Pure post-decode CPU, holds warm or cold.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildHistBaseOffsets`.

## NOTE-398: Boundary-innermost histogram accumulator layout to fix M8 scatter cache misses (2026-06-15)

The hottest M8 loop is the scatter store in `scanAggColHistogramShard`, O(SpanCount) per block.
NOTE-397 hoisted the per-position invariant address arithmetic out of the loop, but the residual
cost (~6% querier self-time, CPU profile) is the random STORE itself: a cache miss per ref.

The flat accumulator was laid out `[group][boundary][step]`:

    stride1 = actualStride*numSteps   (group stride)
    stride2 = numSteps                (boundary stride)
    index   = gIdx*stride1 + bIdx*stride2 + (bk-1)     // step innermost

For a fixed scan position `pos`, `gIdx` and `bk` are constant across the (often many) refs that
resolve to that pos, but `bIdx` (the value-bucket of the aggregate column) varies arbitrarily per
ref. So consecutive same-pos stores jumped by `bIdx*stride2 = bIdx*numSteps` — and `numSteps` is
large (1440 at 24h/60s). Each store landed in a different multi-KB stripe → a guaranteed L2/LLC
miss per ref into the multi-MB `groupCountsFlat`.

**Fix:** transpose the inner two dimensions to `[group][step][boundary]` (boundary innermost):

    stride1    = actualStride*numSteps   (group stride, unchanged)
    stepStride = actualStride            (per-time-step stride = boundary count)
    index      = gIdx*stride1 + (bk-1)*stepStride + bIdx     // boundary innermost

Now for a fixed `pos` the touched cells are the `stepStride` (=actualStride) **contiguous** int64s
of that (group,step) slot — a few cache lines — *regardless of numSteps*. Consecutive same-pos refs
with differing value-buckets land on the same cache line(s) instead of striding by numSteps. The
total array size and number of non-zero cells are identical; only the cell ADDRESS mapping changes.

`buildHistBaseOffsets` now folds `(bk-1)*stepStride` (instead of `(bk-1)`) into `baseOffsetByPos`,
so the per-ref store is still `groupCountsFlat[base+bIdx]++` — one read + one add + one store, no
extra arithmetic vs NOTE-397. The change is applied consistently to ALL four histogram
accumulation paths so emit stays correct: the compact predicate/parallel path
(`scanAggColHistogramCompact`/`scanAggColHistogramShard`), the dense direct N>=1 paths
(`streamByRefSliceHistogramScanDict`), and the N=0 path (`scanHistogramN0`). The two emitters
(`streamByRefSliceHistogramFlatEmitDirect`, `streamByRefSliceHistogramFlatEmit`) read with the
transposed gather `gIdx*stride1 + timeIdx*stepStride + bIdx`; emit is O(cells) one-time, not the
hot per-ref path, so its now-strided inner time loop is irrelevant. The absent-row passes write
the bIdx=0 sentinel at `...+(bk-1)*stepStride`.

**Correctness:** output byte-identical (only the dense index mapping changes; the multiset of
(group,boundary,step)→count is invariant). Covered by the parallel-vs-serial parity tests and the
order-independence test in `intrinsic_hist_parallel_test.go` (canonicalHistCells updated to the
transposed layout) plus the full executor `-race` suite incl. multi-block parquet parity.

**Generality:** AoS→SoA-style cache-locality transpose of an accumulation grid; no benchmark-specific
constants. Holds warm or cold (pure post-decode CPU + memory access pattern). Bounds: bIdx is capped
at `< actualStride = stepStride` by the existing discard guard, so `(bk-1)*stepStride + bIdx <
numSteps*stepStride = stride1` — within each group's slab.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildHistBaseOffsets`.

## NOTE-400: page-pruned flat-equality intrinsic leaf scan (+ evaluable-empty contract)

Issue #347 query shape (a), equality variant. `scanIntrinsicLeafRefs`'s flat-column equality
branch previously called `GetIntrinsicColumn` — a full eager decode of EVERY page of the column —
then binary-searched the materialized `Uint64Values`. Equality on a flat (sorted uint64) column is
a degenerate range `[target, target]`, so it now routes through `scanFlatEqualityRefs` →
`shared.ScanFlatColumnRefs(blob, target, target, true, true, rem)`, which prunes pages whose
`[Min, Max]` does not bracket the target using the PageTOC stats already present in the blob (the
same min/max page-skip the range path already used). Min/Max pruning is **exact** for a sorted
column: a page can only contain a row == target if `Min <= target <= Max`. Each requested value
scans independently and matches are concatenated, byte-identical to the rows the old full-decode
path consumed. `intrinsicFlatMatchRefs` is deleted (its range branch was already dead — the range
case routes through `ScanFlatColumnRefs` — and its equality branch is now the helper).

**Evaluable-empty contract fix:** `scanFlatPagedBlob` / `scanDeltaUint64PagedBlob` returned `nil`
when no row matched, conflating "evaluable, zero matches" with "decode error / not flat". The
v1 `ScanFlatColumnRefs` already returned a non-nil empty slice for the former. The executor leaf
path treats a `nil` result as "leaf unevaluable" → abandons the WHOLE intrinsic pre-filter and
falls back to a full block scan. So a selective range/equality query that legitimately matched
zero rows in a paged block was wrongly full-scanning it. Both paged scanners now return
`[]BlockRef{}` (non-nil) for the evaluable-empty case, matching the v1 contract. This is a
correctness/generality improvement on the paged range path too, not just equality.

**Correctness:** byte-identical results for the rows the caller consumes; pruning is exact (sorted
column min/max). No format change. Covered by `TestScanFlatColumnRefs_EqualityDegenerateRange_*`
(V1, V2 paged across a page boundary, limit) in `shared_test.go` plus the full executor `-race`
suite. **Generality:** no benchmark-specific constants; applies to any flat uint64 intrinsic column.

Back-ref: `internal/modules/executor/predicates.go:scanFlatEqualityRefs`,
`internal/modules/blockio/shared/intrinsic_codec.go:scanFlatPagedBlob/scanDeltaUint64PagedBlob`.

## NOTE-401: dict-index-native group-by for Dict-format intrinsic columns (issue #349)

The compact N=1 group-by scan (`scanGroupByColCompact`) formerly built a FRESH group dict
keyed by the decoded VALUE STRING via a `valToIdx map[string]uint32`, for every group-by query
including the unfiltered high-cardinality `rate() by (service.name)` hot path. For a Dict-encoded
column this is redundant: the parsed `col.DictEntries` already stores each distinct value exactly
once (the cross-page parse dedups by value / Int64Val — `intrinsic_codec.go`), so the native entry
index IS a valid dense group index and is guaranteed unique per value.

The Dict path now writes the **native entry index** (`entryIdx+1`; 0 = absent sentinel) directly
into `dictIdxByPos` (`scanGroupByColCompactDictSerial` / `scanGroupByColCompactDictParallel`), and
sizes the group `dict` to `len(DictEntries)+1` with all slots left `""`. The per-group value string
is materialized **lazily at emit time** (`resolveDictGroupKeys`) only for the groups that actually
accumulated a non-zero count (`groupHasNonZero` / non-nil bucket / non-zero histogram cell). This
removes, on the per-block group-by path:

- the per-entry `valToIdx` string-dedup map (entry index is already the group),
- the per-entry value-string conversion during scan (`intrinsicInt64ColToString` / `[]byte→string`,
  `strconv`) for every entry that never emits on a high-cardinality column, and
- the parallel Dict path's prior **Phase B + O(n) translation pass** over every `dictIdxByPos`
  position (the markers ARE the final indices — no remap).

The Flat/XOR/Delta path keeps the string-deduped dict (no native dictionary exists), built
internally by `scanGroupByColCompactFlatSerial` via a local `valToIdx` map. `scanGroupByColCompact`
no longer takes a `valToIdx` parameter — each caller's `make(map[string]uint32)` is gone.

**Correctness — empty-group equivalence:** the legacy path skipped empty-value entries entirely
(their spans fell to sentinel group 0, merged into the absent group). The new scan reproduces this
by skipping a Dict entry whose value resolves to empty: for a STRING-domain column that is just
`entry.Value == ""` (zero-alloc); for an INT64-domain column (`isInt64DomainColumn` mirrors the
decoder's `colType == ColumnTypeInt64 || ColumnTypeRangeInt64`) the value is NEVER empty
(`intrinsicInt64ColToString` returns a kind/status enum name or the `FormatInt` fallback, both
non-empty), so a zero-valued int64 entry (`span:kind` "unspecified", `span:status` "unset")
correctly forms its own group rather than being dropped. Emitted series are therefore identical to
the value-deduped path.

**No format change. No file-size impact. Identical results** — only the grouping-key representation
changes (native entry index vs decoded-string-deduped index), resolved to the same value at emit.

Covered by `intrinsic_groupby_parallel_test.go`: serial↔parallel byte-identical equivalence
(unchanged), `TestDictGroupByIndexNative_DirectEntryIndex` (native index in `dictIdxByPos`,
unresolved dict after scan), `TestDictGroupByEmptyStringSkipped` (empty string → sentinel 0),
`TestDictGroupByInt64ZeroFormsGroup` (int64 0 forms a group), `TestResolveDictGroupKeys_Lazy`
(only emitted groups resolved), `TestResolveDictGroupKeys_FlatNoOp`.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanGroupByColCompact /
scanGroupByColCompactDictSerial / scanGroupByColCompactDictParallel / scanGroupByColCompactFlatSerial
/ resolveDictGroupKeys / groupHasNonZero / isInt64DomainColumn`.

## NOTE-406: streaming Flat/XOR/Delta group-by scan (issue #348)

The N=1 group-by accumulators (count/rate `streamCountRateN1Compact`-class, agg
`streamAggN1CompactCore`, histogram `streamHistogramN1CompactCore`) formerly fetched the
group-by column via `GetIntrinsicColumn` (eager full-column decode — `Uint64Values`/
`BytesValues`/`BlockRefs` sized to the WHOLE column) and then scanned it once in
`scanGroupByColCompact`. For the value-decoupled Flat/XOR/Delta formats the column is consumed
exactly once and discarded, so the O(column) decode-side arrays exist only to be scanned here.

`streamGroupByColCompactFlat` now tries the streaming (decode-time push-down) scan FIRST
(`scanGroupByColCompactFlatStreaming` → `Reader.ScanIntrinsicColumn` → shared
`ScanPagedColumnBlob`, NOTE-406): it folds each decoded page straight into the group dict +
`dictIdxByPos`, so the column's value/ref arrays are never materialized. The group state
(`dict` + `valToIdx`) is O(groups), retained across pages; the per-page value/ref buffers are
O(one page), reused. The per-ref scatter body (`scatterFlatGroupByRef`) is shared with the
materialized path (`scanGroupByColCompactFlatSerial`), so the streamed `dict`/`dictIdxByPos`
output is byte-identical.

`streamGroupByColCompactFlat` returns `(false, nil)` for non-streamable columns (Dict — the
common low-cardinality `service.name` group-by — and legacy v1 single blobs); the caller then
falls back to the eager `GetIntrinsicColumn` + cached native-index Dict path (NOTE-401), which
is the right choice for those small, hot, cache-resident columns (issue #348 trade-off).

The histogram N=1 callers no longer pre-fetch and thread `groupByCol` through
`streamHistogramN1Compact`/`...Core` — they pass nothing and the core streams or lazily fetches
on fallback — so the O(column) eager materialization is avoided whenever the group-by column is
streamable. `resolveDictGroupKeys` (the only post-scan use of `groupByCol`) is a no-op for the
nil / Flat-streamed case (Flat dicts are fully populated by the scan).

Covered by `intrinsic_stream_groupby_test.go`: streamed-vs-eager byte-identical `dict` +
`dictIdxByPos` over a real multi-page v2 paged column (30k spans → DeltaUint64), the legacy
single-blob non-streamable fallback, and the Dict-column non-streamable fallback.

## NOTE-407: streaming Dict group-by scan (issue #356)

NOTE-406 streamed only Flat/XOR/Delta; **Dict columns — every rate-by-group column in the
bench set (`resource.service.name`, `span.http.request.method`, `span:kind`/`span:status`) —
still fell through to the eager `GetIntrinsicColumn`**, whose `decodeDictPagesArena` allocates a
contiguous `makeNoZeroBlockRef(totalRows)` arena plus a per-entry `[]BlockRef` for EVERY span
row, decoded once for the scatter and discarded. On the warm M4 (`{} | rate() by service`) path
this was the dominant remaining cost: the block I/O is cached, so M4's only work beyond M1
(`{} | rate()`, no group-by) is this service-name BlockRefs arena decode — 24h warm M4 ≈ 4.1 s
vs M1 ≈ 0.27 s over the same cached blocks.

`scanGroupByColCompactDictStreaming` (reached when `streamGroupByColCompactFlat` reports the
column is not Flat-streamable, BEFORE the eager `GetIntrinsicColumn` fallback) streams the Dict
column page-by-page via `Reader.ScanDictGroupByColumn` → shared `ScanDictPagedColumnBlob`. The
visitor dedups each distinct value into a group slot (string `valToIdx` / int64 key map for
`span:kind`/`span:status`, reproducing the merged first-appearance entry order of
`decodeDictPagesArena`) — a handful of interns, NOT O(rows) — and scatters that slot for every
ref in the page-local run by reading each ref straight out of the raw bytes via
`shared.DecodeRefAt`, so the `[]BlockRef` arena is NEVER materialized. Empty string values are
skipped so their refs stay at sentinel 0 (the absent group), matching NOTE-401's
`scanGroupByColCompactDictSerial`.

Wired into all three N=1 compact cores (count/rate `streamCountRateN1CompactCore`, agg
`streamAggN1CompactCore`, histogram `streamHistogramN1CompactCore`). `groupByCol` stays nil on
the streamed path, so the post-scan `resolveDictGroupKeys` is a no-op (the streaming scan
already fully populated `dict`); non-paged (legacy v1) Dict columns and any unexpected format
still fall back to the eager native-index Dict path (NOTE-401).

The streamed `dict` is built in first-appearance order (vs the eager native entry order), so the
two paths assign dict SLOTS differently and `dict`/`numGroups` may differ in size (streamed dict
holds only values actually seen) — but the per-row group ASSIGNMENT
(`dict[dictIdxByPos[pos]]`) and therefore every emitted series is identical. `numGroups =
len(dict)` and the `groupCountsFlat` sizing follow the streamed dict; unseen native entries
(absent from the streamed dict) would have had zero counts anyway, so dropping them is correct.

Covered by `intrinsic_stream_groupby_test.go`
(`TestStreamGroupByDict_DifferentialVsEager_MultiPage` /`_LegacySinglePage`): the streamed
per-position group VALUE equals the eager native-index Dict path's over a real multi-page
service.name column, and the legacy single-blob falls back. Shared-layer differential +
rejection + visitor-error tests in `intrinsic_stream_test.go`.

## NOTE-410: streaming histogram aggregate-field decode (issue #356)

NOTE-406/407 streamed the metrics group-by columns (`rate()`/`count_over_time` Flat & Dict
group-by) but the **histogram aggregate field** (`span:duration` in M8: `{kind=server} |
histogram_over_time(duration) by (resource.service.name)`) still fell through to the eager
`GetIntrinsicColumn(agg.Field)` in `accumulateHistogramDirect`. That materialized the WHOLE
`span:duration` column's O(rows) `Uint64Values` + `BlockRefs` arrays purely to do two sequential
forward scans — `countIntrinsicHistogramBoundaries` (boundary pre-count to size
`groupCountsFlat`) then `streamByRefSliceHistogramScanDict` (bucket accumulation) — used once and
discarded. `span:duration` is a paged DeltaUint64 column (sorted ascending, NOTE-123/396), hence
streamable. After NOTE-407 already streamed `resource.service.name`, this was the last O(rows)
eager decode on the M8 path.

`accumulateHistogramDirectStreaming` (tried first in `accumulateHistogramDirect`, falling through
to the eager path for non-streamable Dict / legacy / absent columns) replaces the eager decode
with **two streaming passes** over `Reader.ScanIntrinsicColumn` → shared `ScanPagedColumnBlob`
(NOTE-406): each decodes ONE page at a time into reused buffers, so transient allocation is O(one
page), not O(column). Pass 1 counts distinct boundaries via `histBoundaryCounter` (the streaming
analog of `countIntrinsicHistogramBoundaries` — same dense exponent-table dedup, persisted across
pages — so the streamed count is byte-identical to the eager count), right-sizing `actualStride`.
Pass 2 streams again, folding each page-local value/ref pair straight into `groupCountsFlat`
exactly as `streamByRefSliceHistogramScanDict`'s Flat branch did. The compressed blob stays in the
section cache between passes (`GetOrFetchIntrinsic`), so pass 2 re-decodes (cheap, pooled buffers)
but never re-fetches. The absent-row pass, boundary indexer (NOTE-352), and `emitHistogramFlat`
are unchanged — only the source of the values moved from an eager O(rows) array to a streamed
page walk, so emitted series are byte-identical.

While adding the multi-page differential test, the eager fallback was found to be **silently
wrong for a paged DeltaUint64 aggregate column** (the common shape of a large `span:duration`):
`streamByRefSliceHistogramScanDict` and `scanHistogramN0` switched only on
`IntrinsicFormatDict` / `IntrinsicFormatFlat`, so a Delta-decoded column (`col.Format ==
IntrinsicFormatDeltaUint64`, populating the SAME `Uint64Values`+`BlockRefs` as Flat) fell through
the switch with NO accumulation — every span was then counted into the boundary-0 sentinel by the
absent-row pass, collapsing the histogram to a single bucket. Both switches now treat
`IntrinsicFormatDeltaUint64` identically to `IntrinsicFormatFlat` (the value-decoupled branch), so
the eager fallback and the N=0 path are correct for Delta columns. The new streaming path handles
Delta natively (it streams via `ScanPagedColumnBlob`, which already decodes Delta pages), so the
production M8 path is correct independent of this fallback fix.

Covered by `intrinsic_hist_stream_test.go`
(`TestAccumulateHistogramDirect_StreamingVsEager`): the streaming path's emitted series equal the
eager `accumulateHistogramDirectEager` over a real multi-page paged DeltaUint64 `span:duration`
column, asserting >1 distinct boundary (so the Delta accumulation is actually exercised, not just
the boundary-0 sentinel). `TestHistBoundaryCounter_MatchesEager` pins the streaming boundary count
against `countIntrinsicHistogramBoundaries`, and `..._FallbackNonStreamable` pins the Dict
fall-through.


## NOTE-442: stream span:start decode in the unfiltered no-group-by count/rate hot path (issue #360)

NOTE-406/407 streamed the metrics **group-by** dimension and NOTE-410 streamed the histogram
**aggregate field** (`span:duration`), but the **time-bucketing** dimension (`span:start`) on the
hottest metrics shape — the unfiltered no-group-by count/rate query (M1 `{} | rate()`,
`count_over_time()`) — still materialized the WHOLE `span:start` `[]uint64` column via
`fetchSpanStartColumn` → `appendDeltaUint64PageOpt` across every page. That eager materialization
was the top self-time frame in the production querier profile (`appendDeltaUint64PageOpt` ~14.5%
of CPU), allocated only to binary-search a `[lo,hi]` in-window sub-slice and count it per bucket in
`streamCountRateNoGroupBySorted`.

`streamCountRateNoGroupByPaged` (tried first in `executeTraceMetricsIntrinsic` for the
`isCountRate && len(GroupBy)==0 && !hasPreds` shape, falling through to the eager materialized path
for legacy v1 / non-paged `span:start` blobs) replaces the eager decode with a single streaming
pass over `Reader.ScanIntrinsicColumn("span:start", …)` → shared `ScanPagedColumnBlob` (NOTE-406):
each page is decoded ONE AT A TIME into reused buffers (transient allocation O(one page), not
O(column)) and its in-window values are counted straight into a flat `[]int64` per-bucket counter,
emitted once at the end to the same string-keyed `buckets` map the eager path produces. The
per-value window-skip (`ts <= StartTime || ts > EndTime`) reproduces the eager lo/hi binary-search
narrowing, and the right-closed bucketing (`timeBucketIndex`) reduces to the same
"number of in-window timestamps with `timeBucketIndex == b`" as `streamCountRateNoGroupBySorted`,
so per-bucket counts and emitted series are byte-identical. The unfiltered path reads NOTHING but
`span:start` values (no refs, no group-by, no predicate), which is exactly why streaming is
unconditionally correct here: there is no second column to merge-join against.

This is the time-bucketing complement to NOTE-406/407 (group-by) and NOTE-410 (aggregate field) —
all three eager O(rows) intrinsic-column materializations are now streamed on the metrics hot path.
Predicate-filtered and group-by count/rate paths still need the materialized column (refs and
binary-searched sub-slices fed to the compact cores), so they are excluded and keep the eager
`fetchSpanStartColumn`. As a follow-on, issue #363 layers PageTOC `Min`/`Max` page-level pruning
onto this streaming visitor (skip whole pages outside the window, bulk-count pages entirely within
one step bucket).

Covered by `intrinsic_countrate_stream_test.go`: `..._VsEager` asserts streamed per-bucket counts
equal `streamCountRateNoGroupBySorted` over a real multi-page paged Delta `span:start` column with
a window strictly inside the column (so the per-value window-skip is exercised); `..._FullWindow`
asserts a window covering every span counts the full column exactly once; and
`TestExecuteTraceMetrics_CountRateNoGroupBy_StreamEquivalence` pins the public `ExecuteTraceMetrics`
`{} | count_over_time()` path (which now routes through the streamer) against the eager bucket total.

## NOTE-414: pool the >> / !>> ancestor-existence memo (eliminate per-block []uint8 alloc)

`evalOpDescendantStruct` (`>>`) and `evalOpNotDescendantStruct` (`!>>`) each began with
`memo := make([]uint8, len(spans))` — a fresh tri-state (memoUnknown/memoYes/memoNo) ancestor
memo allocated per block-eval on the structural query hot path (Q9). The memo is fully
overwritten/read within the single call and never escapes, so it is a textbook per-call scratch
buffer. Both now draw from a process-wide `compactUint8Pool` via `acquireCompactUint8(len(spans))`
(zeroed back to memoUnknown=0 on a pool hit) and return it with `defer releaseCompactUint8(memo)`.
This mirrors the existing `compactBoolPool`/`compactInt16Pool` family (NOTE-125/129) and the
structural `compactBool` reuse of NOTE-413, extending the per-block scratch-pooling discipline to
the last unpooled structural memo. The pool stores `*[]uint8` (pointer-wrapped) so `Put` is
SA6002-clean without a nolint. `releaseCompactUint8` honours the NOTE-355 oversized-drop cap
(`cap > compactPoolMaxPooledBytes`) so a pathological wide block does not pin a giant array in the
pool. Output is byte-identical (same memo states, same emit order); the only change is the backing
array is reused across block-evals instead of freshly heap-allocated and GC'd each call.

## NOTE-415: skip per-ref group-value materialization for out-of-time-range refs (flat streaming group-by)

`scanGroupByColCompactFlatStreaming` (the streaming Flat/XOR/Delta group-by rate path, NOTE-406/407)
decoded each page and, for EVERY ref in the page, materialized the group value as a fresh string
(`strconv.FormatUint(p.Uint64Values[i], 10)` for the uint64 case, `string(p.BytesValues[i])` for the
bytes case) BEFORE calling `scatterFlatGroupByRef`. But `scatterFlatGroupByRef` immediately discards
any ref whose packed key is outside `[minPK, maxPK]` (the query time-range window) — so for every
out-of-range ref the freshly-allocated value string was computed and then thrown away unused. On a
time-narrowed query (the common case: a wide block, a narrow step window) the majority of a block's
refs are out of range, so this is a per-ref wasted allocation dominated by the filtered-out spans.

The fix hoists the cheap `packKey` + `pk < minPK || pk > maxPK` range check into the caller's per-ref
loop, BEFORE the string materialization, and `continue`s on out-of-range refs. `scatterFlatGroupByRef`
still performs the identical range check internally (idempotent), so for in-range refs the behaviour —
and the emitted dict + dictIdxByPos — is byte-identical. Only the wasted `strconv.FormatUint` /
`string(bytes)` allocation for out-of-time-range refs is eliminated, cutting per-block allocation
volume (and the GC pointer-scan cost the production CPU profile is dominated by) on the streaming
flat group-by rate path. The materialization for in-range refs is unchanged.

## NOTE-416: pool buildAggValsForRef's dense valByPK/hasByPK scratch arrays

`buildAggValsForRef` (the N≥1 group-by non-count/rate aggregate path: min/max/avg/sum/quantile field
extraction) builds two dense arrays indexed by packed key (`packKey(blockIdx,rowIdx)`): `valByPK`
(`[]float64`) and `hasByPK` (`[]bool`), each sized `maxPK+1` where `maxPK` is the largest packed key
over `inRangeRefs`. For a wide block these can reach ~32M entries → ~256 MB for `valByPK` plus ~32 MB
for `hasByPK` (the ~288 MB figure the heap profile flagged). They were freshly `make`-allocated and
GC'd on every per-block aggregate eval.

Both are **pure local scratch** — they are never returned. The function scatters the aggregate column
into them by packed key, then in a final pass copies only the in-range positions into the small
returned `aggVals`/`aggPresent` slices (`len = len(inRangeRefs)`). So they can be drawn from the
existing compact pools (`acquireCompactFloat64` / `acquireCompactBool`, zeroed on a pool hit) and
released via `defer` before return. The defers fire after the named return values are evaluated, and
the returned slices are independent copies, so handing the scratch back to the pool is safe even under
concurrent block-evals. The pools' NOTE-355 oversized-drop guard caps what is retained, so a single
giant-maxPK block does not pin a 256 MB buffer in the pool forever. Output is byte-identical; this only
amortizes the large dense-array allocation across calls, cutting the per-block alloc volume that the
production CPU profile attributes to GC pointer-scan (scanObject/findObject).

## NOTE-422: cache per-record slot in groupMatchingStructuralTraces to halve [16]byte map probes

`groupMatchingStructuralTraces` (NOTE-387) groups the flat cross-block `[]structuralSpanRec` into
one contiguous window per matched trace via a counting-bucket scatter keyed on the 16-byte trace ID.
The prior body probed the `map[[16]byte]int` slot map for EVERY record in BOTH the count pass and the
scatter pass (~`2n` probes for `n` records, plus the `nMatched` first-sighting probes in the slot-assign
pass). The production CPU profile (2026-06-16) showed `runtime.mapaccess2` (2.05s) and `aeshashbody`
(1.80s) among the top overall self-time frames, and `groupMatchingStructuralTraces` itself at 0.62s —
the 16-byte trace ID is hashed through aeshashbody on every probe, so re-probing the same key in two
full passes was the dominant grouping cost.

Fix: the count pass now records each record's resolved slot into a `recSlot []int32` (`slot`, or `-1`
when the record's trace did not match). The scatter pass reads `recSlot[i]` directly instead of probing
the map again — a flat int32 load replaces a 16-byte AES hash + bucket walk. This drops the per-record
probe count from ~`2n` to ~`n` (the count pass keeps its single probe; the slot-assign pass is
unchanged). `recSlot` costs 4 bytes/record, far cheaper than the hash it elides. Output is identical:
the scatter visits exactly the same records (slot ≥ 0 ⇔ map-hit) and writes them to the same window
offsets in the same order.

## NOTE-429: hoist the colName switch out of populateTypedColumn's per-ref loop (kill `any` boxing)

*Added: 2026-06-16*

`populateTypedColumn` (the per-ref intrinsic-field lookup used by the predicate-filtered SEARCH
post-filter path: `filterRowSetByIntrinsicNodes` → `lookupIntrinsicFieldsTyped` → here, for queries
like `{resource.service.name = …}` / `{status = error}` / `{kind = server && …}`) previously did,
for each of the N selected refs:

    val, ok := col.LookupRefFast(packed)   // returns (any, bool)
    storeTypedField(colName, val, &result[i])

`LookupRefFast` returned `any`, so every uint64/int64/[]byte value was BOXED onto the heap (one
alloc per ref), and `storeTypedField` re-dispatched the identical `colName` type switch per row and
re-asserted the boxed dynamic type. Bench `BenchmarkPopulateTypedColumn_SpanIDAllocs` (100 refs,
span:id + span:parent_id) measured **103 allocs/op** — ~1 boxing alloc per ref.

Fix: dispatch the column type ONCE (the colName switch is now the outer structure), then run a tight
per-ref loop calling the concrete typed accessor — `LookupRefFastBytes` / `LookupRefFastString` /
`LookupRefFastUint64` / `LookupRefFastInt64` (zero-alloc, NOTE-015) — and write straight into the
typed `intrinsicRowFields` field. No `any`, no boxing, no per-row switch, no per-row type assertion.
This is the per-ref twin of the structural full-block scatter's hoisted-switch design
(`populateTypedColumnForBlock`, NOTE-100/423). Bench drops to **3 allocs/op** (the 3 are the pooled
result-slice acquire + column fetch, independent of ref count) — a 97% allocation reduction and a
genuinely flat per-ref inner loop.

Dead-code cleanup (rules: no dead code): `storeTypedField` (unexported) had no remaining caller and
was deleted. `(*IntrinsicColumn).LookupRefFast` — the `any`-returning accessor whose sole remaining
caller was `populateTypedColumn` (other call sites already used the typed family) — is no longer
referenced anywhere in the tree (tempo consumes blockpack only via vendoring), so it was deleted too;
the typed
`LookupRefFast{Uint64,Int64,String,Bytes}` family remains the supported accessor set.

## NOTE-423: dict intrinsic columns scatter directly from DictEntries — skip the sorted refIndex build

*Added: 2026-06-16*

`populateTypedColumnForBlock` (NOTE-100) fills one intrinsic column's values into the per-block
`[]intrinsicRowFields` for all spans. For DICT columns (span:kind / span:name / resource.service.name /
span:status / status message) it previously called `col.BlockRefRange(blockIdx)`, which triggers
`EnsureRefIndex` → `buildRefIndexDict` → (in the non-dense/multi-block case) `radixSortRefIndexPrepared`,
to obtain a **sorted-by-packed-ref** `[]RefIndexEntry` window for the block.

**The sort is pure waste on this path.** The dict scatter loop writes `result[rowIdx] = entry.value`
indexed by the ref's low-16 `RowIdx`; it is fully order-INDEPENDENT and never does a reverse
(packedRef → pos) lookup. So the only reason `BlockRefRange` builds and sorts an index — to support
binary-search point lookups — does not apply here.

`radixSortRefIndexPrepared` was the **#1 self-time frame** on the structural Q9 profile
(2026-06-16): Q9 unions the block sets selected by BOTH structural nodes (`{a} >> {b}`), so the
union spans many blocks and each block's dict ref-index is built per querier call. Even though
query-frontend shards one block per querier call (so the column is single-block), a sparse/optional
dict column (the attribute present on only some rows) falls to the `radixSortRefIndexLow16Prepared`
sort, and a merged multi-block column falls to the full four-pass `radixSortRefIndexPrepared`.

**Fix:** scatter directly from `col.DictEntries`. For each dict entry, every `BlockRef` whose
`BlockIdx == blockIdx` contributes one write `result[ref.RowIdx] = entry.value`. This visits the same
`(rowIdx, entryIdx)` pairs `BlockRefRange` would yield, just in decode order instead of sorted by
packed ref — irrelevant, because the scatter indexes `result` by `rowIdx`, not by iteration position.
No `EnsureRefIndex`, no histogram, no radix/low-16 sort, no `[]RefIndexEntry` materialization.

**Why safe:** dict columns keep EAGER refs at decode time (`decodePagedColumnBlobOpt`: only
Flat/XOR/Delta defer refs via `refsDecode`; dict refs share a cross-page arena, NOTE-152), so
`DictEntries[].BlockRefs` is always populated without `EnsureBlockRefs`. The per-row bounds check
(`rowIdx >= len(result)`) mirrors the general scatter variants exactly. Flat/uint64 identity columns
(trace:id / span:id / span:parent_id / span:start/end/duration) keep the `BlockRefRange` path — they
are usually flat-dense (handled up front via `DenseFlatRange`, NOTE-354) and the sorted-index path is
otherwise rare for them.

The five entries-based dict scatter helpers (`scatterSpanName`/`scatterServiceName`/
`scatterStatusMessage`/`scatterSpanKind`/`scatterSpanStatus`) are deleted — replaced by the
`*Dict` variants that take `(dictEntries, blockIdx, result)`.

Back-ref: `internal/modules/executor/intrinsic_row_block.go:populateTypedColumnForBlock`,
          `internal/modules/executor/intrinsic_row_block.go:scatterSpanKindDict`

## NOTE-424: route MULTI-BLOCK dict intrinsic columns through the cached sorted refIndex (revert NOTE-423 for that shape)

*Added: 2026-06-16*

NOTE-423 made the per-block dict intrinsic scatter (`populateTypedColumnForBlock`) walk
`col.DictEntries` directly — for each entry, every `BlockRef` with `BlockIdx == blockIdx`
emits one write `result[ref.RowIdx] = value`. That is O(totalRefs) **per block call** because
it re-scans the WHOLE column's refs and `continue`s on every ref belonging to another block.

`populateTypedColumnForBlock` is called **once per selected internal block** (a single querier
Reader holds the whole object file — many internal blocks — and structural `{a} >> {b}` queries
union the block sets selected by BOTH nodes, so the selected set is large). So the direct scatter
costs **O(N_blocks × totalRefs)** across the file — quadratic. The 2026-06-16 Q9 CPU profile showed
`scatterSpanKindDict` as the **#1 blockpack self-time frame (~5.0s)**, dwarfing everything else.

NOTE-423's premise ("query-frontend shards one block per querier call so the column is
single-block") is wrong for the internal-block dimension: sharding is at the object/file level,
not the internal-block level, and one file has many internal blocks.

**Fix:** gate on `col.DictMultiBlock()` (NOTE-424, cached one-time O(totalRefs) scan).
- **Single-block** dict column → keep the NOTE-423 direct `*Dict` scatter (no sort needed; this
  remains the dominant shape and the original NOTE-423 win).
- **Multi-block** dict column → build the sorted refIndex **once** (cached via `EnsureRefIndex` /
  `refIndexOnce`) and binary-search this block's contiguous window with `BlockRefRange(blockIdx)` —
  O(log totalRefs + blockRefs) per block. New `*DictRefIndex` scatter helpers consume that window:
  `entry.Pos` is the dict-entry index, `entry.Packed&0xFFFF` is the row index, so
  `result[rowIdx] = DictEntries[Pos].value`. Same (rowIdx, value) pairs as the direct scatter for
  this block, sourced from the cached sorted index instead of an O(totalRefs) re-walk.

The one-time `DictMultiBlock` scan costs the same O(totalRefs) the first block's direct scatter
already paid, so it is free relative to the (N_blocks − 1) full re-scans it eliminates. The radix
sort that NOTE-423 was avoiding now fires **once per query** (cached) instead of being re-paid
implicitly through the per-block re-walk — a strict win on multi-block files.

**Why safe:** `BlockRefRange` already returns the block-bounded, sorted window used by the
flat/uint64 identity columns; the dict scatter is order-INDEPENDENT (indexes `result` by rowIdx),
so reading the sorted window in sorted order is identical to reading DictEntries in decode order.
Validated by `TestLookupIntrinsicFieldsTypedForBlock_MultiBlock_AllMatch` (10 blocks, dict
span:name/service.name/span:kind), which compares `ForBlock` against the reference typed path
row-for-row across every block.

Back-ref: `internal/modules/executor/intrinsic_row_block.go:populateTypedColumnForBlock`,
          `internal/modules/blockio/shared/intrinsiccolumn.go:DictMultiBlock`

## NOTE-425: skip a structural node's predicate on blocks its own plan excluded (2026-06-16)

**Context:** `collectAllStructuralSpans` plans each structural node independently via
`planBlocks(prog_i)` and **unions** the selected block sets — a block matching ANY node is
fetched and decoded (NOTE-091). But `evaluateStructuralPrograms` then ran EVERY node's
`ColumnPredicate` against EVERY block in the union. For `{kind=server} >> {kind=client &&
span.rpc.method != ""}` (Q9), node 1's user-attr predicate (`span.rpc.method`) was decoded and
scanned even on blocks that only node 0's plan selected — where node 1's range/intrinsic-TOC
pruning had already proven no span can match. The structural CPU profile is dominated by exactly
this per-block column decode (radixSortRefIndexPrepared, buildRefIndex*, scatter*), and the union
of block sets across both nodes multiplies it.

**Decision:** Capture each node's OWN selected-block set (`progBlockSets[i]`) during planning,
carry it on the read-only per-query `structuralBlockPlan`, and in `evaluateStructuralPrograms`
return the `emptyRowSet` sentinel — skipping the `ColumnPredicate` call and its user-attribute
column decode/scan — for any node whose set does not contain the current block. The union of
fetched blocks is UNCHANGED (a block selected only by node 0 is still decoded for its identity
columns so cross-block parent linkage in NOTE-091 still works); only the wasted per-node predicate
evaluation on non-selected blocks is removed.

**Correctness:** A node's `planBlocks` `SelectedBlocks` is a superset of the blocks where that
node can match — block-level range and intrinsic-TOC pruning are exact, and bloom pruning carries
the same file-level FPR semantics the codebase already accepts (NOTE-091 "file-level rejection
safety"). So a block absent from node i's set has no span matching node i; its rowset is
necessarily empty and `emptyRowSet{}` is byte-identical to evaluating the predicate. The
optimization is gated to predicate-pruned ("gated") nodes only: a negation-LHS node uses a
time-range-only plan that does NOT bound its node-match set, so its `progBlockSets` entry is left
nil ("evaluate everywhere"), as is the all-blocks `{}` node — preserving negation semantics.

**Back-ref:** `internal/modules/executor/stream_structural.go:collectAllStructuralSpans`,
          `evaluateStructuralPrograms`,
          `internal/modules/executor/allmatchset.go:emptyRowSet`

## NOTE-426: scatter fixed-width identity IDs via array conversion, not the copy() builtin (2026-06-16)

**Context:** The structural identity scatter (`scatterSpanIDDense`/`scatterParentIDDense`/
`scatterTraceIDDense` and their non-dense `scatterSpanID`/`scatterParentID`/`scatterTraceID`
twins in `intrinsic_row_block.go`) writes each row's fixed-width OTel ID from `col.BytesValues`
(`[][]byte`) into the `[8]byte`/`[16]byte` field on `intrinsicRowFields`. The previous code used
`copy8(dst, b)` (and `copy(dst[:], b)` for trace IDs), whose `copy` builtin computes
`min(len(dst), len(b))` and lowers to a runtime memmove-style sequence even though the preceding
`len(b) == N` guard fixes the length. This per-row copy was the #1 blockpack self-time frame on
the structural Q9 profile (`copy8`, ~0.72s) because it runs O(SpanCount) per block over the union
of selected blocks.

**Decision:** After the `len(b) == N` guard, copy via a direct array conversion
`dst = [N]byte(b)` (Go 1.20+ slice→array conversion). The conversion compiles to a single
N-byte load+store with exactly one bounds check (the conversion's own `len(b) >= N` check, which
the guard already proves), eliminating the builtin's min-length computation and memmove dispatch.
The `len(b) == N` guard is preserved on every path so non-spec-width values are skipped exactly as
`copy8` did — semantics are byte-identical.

**Correctness:** Identical guarded write; `[N]byte(b)` panics only if `len(b) < N`, which the
`len(b) == N` guard makes unreachable. `copy8` itself is retained for the `intrinsic_row.go`
ref-driven path (`storeTypedField`/`identityFieldsFromBlockColsTyped`) where the same single-MOV
lowering applies but the guarded-write shape is already inline.

**Back-ref:** `internal/modules/executor/intrinsic_row_block.go:scatterSpanIDDense`,
          `scatterParentIDDense`, `scatterTraceIDDense`, `scatterSpanID`, `scatterParentID`,
          `scatterTraceID`

## NOTE-427: scatter fixed-width identity IDs via array conversion in the ref-driven path too (2026-06-16)

**Context:** NOTE-426 replaced `copy()` with a slice→array conversion in the *dense* scatter
twins (`scatterSpanIDDense` etc.) but explicitly LEFT `copy8` and the two `copy(row.traceID[:], …)`
sites in `intrinsic_row.go` on the `copy` builtin, asserting "the same single-MOV lowering applies
but the guarded-write shape is already inline." The post-NOTE-426 querier CPU profile (2026-06-16,
30m window) disproved that: `copy8` was still ~2.0s self-time (~0.6% of querier CPU), the residual
of the structural ref-driven path (`storeTypedField`) and the legacy no-intrinsic-section identity
build (`identityFieldsFromBlockColsTyped`). The `copy` builtin does NOT fold to a single MOV here:
it still computes `min(len(dst), len(b))` and dispatches a memmove-style sequence even with the
`len(b) == N` guard, because the guard's proven length is not propagated into the builtin.

**Decision:** Use the same Go 1.20+ slice→array conversion NOTE-426 used for the dense twins:
`*dst = [spanIDByteLen]byte(b)` in `copy8`, and `row.traceID = [traceIDByteLen]byte(b)` at both
trace-ID sites. Each compiles to a single fixed-width load+store with exactly one bounds check
(the conversion's own `len(b) >= N`, which the preceding `len(b) == N` guard already proves).

**Correctness:** The `len(b) == N` guard is preserved on every path, so non-spec-width values are
rejected exactly as before. `[N]byte(b)` panics only if `len(b) < N`, which the guard makes
unreachable. Byte-identical semantics.

**Back-ref:** `internal/modules/executor/intrinsic_row.go:copy8`, `storeTypedField` (trace:id),
          `identityFieldsFromBlockColsTyped` (trace:id)

## NOTE-432: Precompute structural per-row predicate-match bitmask (eliminate per-row binary search)

**Path:** `collectBlockStructuralSpanRecs` → per-row `computeNodeMatchForRow` (structural `>>`/`<<`
block scan). For every span of every block selected by EITHER node (the union-of-block-sets shape
that drives the Q9 9x-vs-parquet gap), the old `computeNodeMatchForRow` probed EACH program's
`vm.RowSet` with `rowSet.Contains` — a `slices.BinarySearch` — once per row. Cost was
O(spanCount × programs × log(matchedRows)) across the block, the dominant per-block CPU on the
structural path.

**Change:** Each `RowSet` is already a sorted ascending slice of its matched rows. Scatter each
set's matched rows directly into a per-row `[]uint8` bitmask in a single linear pass per program
(`computeStructuralPredBits`, O(sum of set sizes) — visits only matched rows, typically far fewer
than spanCount). The row loop then reads bit `i` with one array index instead of a binary search.
`computeNodeMatchForRow` now takes the precomputed `predBits uint8` for that row and only folds in
the per-row intrinsic-node check (iterating set bits via `bits.TrailingZeros8`).

**allMatchSet** (the `{}` node) matches every row — its bit is set in one tight loop WITHOUT
materializing `ToSlice()` (which would allocate a spanCount-sized `[]int`). `emptyRowSet`
contributes nothing. `*rowSet` exposes its sorted backing slice via `ToSlice()` (no copy).

**Pooling:** `structuralPredBitsPool` (`[]uint8`, mirrors `compactBoolPool`) amortizes the scratch
allocation; released after the row loop. Drops oversized outliers per NOTE-355.

**Correctness:** Byte-identical. predBits is computed before `releaseBlockColumnProvider` so any
scratch-backed single-predicate RowSet is fully consumed before pool reuse can overwrite it.
predBits is computed for all rows (incl. trace-ID-absent rows skipped by the loop) — harmless, the
surplus bits are never read. Tests `go test -race ./internal/modules/executor/...` green.

**Back-ref:** `internal/modules/executor/stream_structural.go:computeStructuralPredBits`,
`computeNodeMatchForRow`, `collectBlockStructuralSpanRecs` row loop.

## NOTE-434: Pool the structural-grouping recSlot scratch (drop a per-query len(flat) alloc)

`groupMatchingStructuralTraces` (NOTE-387/NOTE-422) builds `recSlot []int32` of length `len(flat)`
— the cached per-record dense slot used by pass 3 to scatter survivors without re-probing the
`[16]byte` trace-ID map. `flat` carries one record per trace-ID-bearing span across the WHOLE union
of selected blocks, so on a heavy structural query (a deep `>>`/`<<` chain over a large span
population) recSlot is one of the largest per-query allocations on the structural path, and it was
`make([]int32, len(flat))`'d fresh — memclr-zeroed then fully overwritten (every element is assigned
a slot or -1) — on every query.

**Change:** draw recSlot from the existing `compactInt32Pool` (`acquireCompactInt32` /
`releaseCompactInt32`, NOTE-355/oversize-drop) and release it on return. It is built in pass 2 and
fully consumed by pass 3 before the function returns; nothing retains it, so the pool reuse is
sound. `acquireCompactInt32` clears the prefix — redundant here since the loop assigns every
element, but harmless — and the pool amortizes the allocation and its GC churn across query traffic
on the structural hot path.

**Correctness:** Byte-identical grouping. recSlot is pointer-free, never escapes, and is the same
length/contents as before. Tests `go test -race ./internal/modules/executor/...` green.

**Back-ref:** `internal/modules/executor/stream_structural.go:groupMatchingStructuralTraces`.

## NOTE-435: Prune intrinsic-node tree once; map-free per-row predicate evaluation (2026-06-16)
**Context:** `rowSatisfiesIntrinsicNodesTyped` runs once per CANDIDATE row on the
search/structural and predicate-filtered metrics paths (Q9 walks every span of every block
selected by EITHER structural node — millions of rows over the union). Its first action on every
leaf was a `traceIntrinsicColumns[n.Column]` map probe to classify intrinsic vs non-intrinsic.
But the predicate node set — and therefore each node's intrinsic classification — is constant
across all rows in a scan. `mapaccess2_faststr` was ~4.2s of querier self-time on the 2026-06-16
CPU profile; ~0.38s of it attributed to this per-row, per-node probe (the largest map-lookup
consumer on the structural priority-#1 path).

**Change:** `prepareIntrinsicNodes` prunes the node tree ONCE (before the row loop) to the subset
the typed evaluator actually consults — only intrinsic-column leaves, and only group nodes that
retain at least one intrinsic leaf. The pruned tree is fed to map-free twins
`rowSatisfiesPreparedIntrinsicNodes` / `...OR`. Hoisted into `collectStructuralIntrinsicNodes`
(structural `nodesList[i]`, consumed by `computeNodeMatchForRow`) and into
`filterRowSetByIntrinsicNodes` (search/legacy filter), so the map probe leaves the row loop
entirely.

**Correctness:** Pruning is semantics-preserving. AND context: a non-intrinsic leaf was `continue`d
(no constraint) — dropping it is identical. OR context: a non-intrinsic leaf was `continue`d
WITHOUT setting `hadConstrainedChild` and without short-circuiting — dropping it changes neither the
constrained-child accounting nor any outcome. A group that retains no intrinsic leaf collapses to
"unconstrained" (empty AND returned true; empty OR's `hadConstrainedChild` stayed false → true),
which the parent treated as unconstrained — identical to dropping the group. Because every retained
OR child is constrained, the `hadConstrainedChild` bookkeeping collapses and the prepared OR simply
returns false on no match. Verified by `TestPrepareIntrinsicNodes_EquivalentToOracle`, which asserts
the prepared evaluator matches the original oracle byte-for-byte across AND/OR/nested/range/absent
trees mixing intrinsic and non-intrinsic columns. The original `rowSatisfiesIntrinsicNodesTyped`
/`...ORTyped` are retained as the test oracle.

**Back-ref:** `internal/modules/executor/predicates.go:prepareIntrinsicNodes`,
`stream_structural.go:collectStructuralIntrinsicNodes`/`computeNodeMatchForRow`,
`stream.go:filterRowSetByIntrinsicNodes`.

## NOTE-436: Pool the cross-block structuralSpanRec accumulator (2026-06-17)
**Context:** `collectAllStructuralSpans` sizes `flat := make([]structuralSpanRec, 0, totalSpans)`
to the summed SpanCount of EVERY block in the UNION of both structural nodes' selected-block
sets (NOTE-091 forbids intersecting the sets; the union is the sound block population). For a
heavy `>>`/`<<` query over a large span population (the priority-#1 Q9 9x-gap workload) this is
the single largest per-query allocation on the structural path: the 2026-06-17 alloc profile
attributed ~9.8 GB *self* to `collectAllStructuralSpans`, virtually all of it this one make. It
was allocated fresh (and zeroed) every query and discarded.

**Change:** draw `flat` from a `sync.Pool` (`acquireStructuralSpanRecs(totalSpans)` /
`releaseStructuralSpanRecs`). `flat` is fully consumed by `groupMatchingStructuralTraces`, which
scatters survivors into a FRESH `backing` array and returns windows aliasing THAT array — never
`flat` (see NOTE-387). So `flat` never escapes `collectAllStructuralSpans` and is released on
every exit path (after grouping, and on the per-block error path where `flat` holds the latest
possibly-reallocated backing). The pool stores `*[]structuralSpanRec` so boxing for `Put` does
not allocate the slice header (staticcheck SA6002-clean), and drops over-32-MiB backings on
release (NOTE-355) so one giant query cannot pin a multi-hundred-MB array in the pool.

**Correctness:** `structuralSpanRec` is pointer-free (NOTE-357: all-value [16]byte/[8]byte/
int32/uint16/uint8 fields), so a recycled backing pins no string/byte payload and carries no
live pointers for the GC to scan between uses. `acquire` returns length 0, so the caller appends
exactly as before and every emitted element is fully written — the recycled contents are never
observed. Verified by `TestStructuralSpanRecsPool_*` (zero-length adequate-cap reuse; oversized
drop) and the unchanged structural/format-comparison correctness suite
(`go test ./benchmark -run 'Structural|FormatComparison|Correctness'`).

**Back-ref:** `internal/modules/executor/structuralspanrec.go:acquireStructuralSpanRecs`/
`releaseStructuralSpanRecs`, `stream_structural.go:collectAllStructuralSpans`.

## NOTE-437: Single-pass / split-free direct metrics emit (2026-06-17)
**Context:** The directly-emitted metrics series builders (`emitFlatCountRateSeries` for
count/rate `by (...)` — M4-class; `streamByRefSliceHistogramFlatEmitDirect` for
`histogram_over_time by (...)` — M8-class) walked the per-group step cells TWICE: a
`hasAny` probe loop followed by a separate fill loop. For a high-cardinality group-by
(M8: thousands of service-name groups × tens of boundaries × many timesteps) this is a
redundant full scan of the dense `groupCountsFlat` grid. The histogram emitter additionally
called `strings.Split(gk, "\x00")` once per group (allocating a fresh 1-element slice for
every service on the single-dimension group-by that M4/M8 actually use) and re-resolved the
loop-invariant group-by label NAMES via `intrinsicLabelName` once per emitted series.

**Change:** Fold the probe+fill into one pass that defers the `values := make([]float64, numSteps)`
allocation until the first non-zero cell — so empty (group[,boundary]) series cost neither an
allocation nor a second scan. In the histogram emitter, hoist the group-by label names out of
the series loop (`labelNames` computed once) and skip `strings.Split` entirely when
`len(groupBy) == 1` (gk IS the sole dimension value; the `\x00` separator only appears in
multi-dimension composite keys). In `emitFlatCountRateSeries`, precompute the rate divisor
once (`rateScale`).

**Correctness:** For single-dimension group-by, `strings.Split(gk, "\x00")` returned `[gk]`
(values never contain the `\x00` separator — that is the invariant the composite key relies
on), so reading `gk` directly is byte-identical. The rate `stepSec<=0` corner is preserved:
`rateScale=0` still emits the series with zero values because the cell counts were non-zero
(values is allocated on the first non-zero cell). Deferring the allocation cannot change which
series are emitted — a series was emitted iff `hasAny` (≥1 non-zero cell), which is exactly the
condition under which `values` is now non-nil. Verified by the executor suite (incl.
`TestStreamByRefSliceHistogram_3D_DirectEmitEquivalence`, the multi-dimension path that
exercises the retained `strings.Split` branch) and the benchmark parity suite.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:emitFlatCountRateSeries`,
`streamByRefSliceHistogramFlatEmitDirect`.

## NOTE-438: Lazy `buckets` map on the direct-sink metrics paths (2026-06-17)
**Context:** `executeTraceMetricsIntrinsic` allocated `buckets := make(map[string]*aggBucketState)`
unconditionally, once per block per metrics query, before dispatching the accumulation. But the
single-dimension count/rate path (M4/M6/M9 → `seriesSink`) and the N≤1 `histogram_over_time by (...)`
path (M8 → `histSink`) never write that map: every accumulation core that receives a non-nil sink
early-returns into the dense sink grid (`streamCountRateN1CompactCore` returns right after
`emitFlatCountRateSeries`; `emitHistogramFlat` returns right after
`streamByRefSliceHistogramFlatEmitDirect`) and the `intrinsicGetOrCreateBucket(buckets, …)` calls are
only reached on the `seriesSink == nil` / `histSink == nil` branch. So on exactly the heavy 24h
metrics queries — the ones that fan out over 1500+ blocks (one Reader/goroutine each) — the map was a
pure live pointer-bearing GC object allocated and discarded per block. GC scan (`scanObject` +
`findObject` + `tryDeferToSpanScan`) is ~4.5% self on the metrics CPU profile (2026-06-17), and live
pointer-bearing maps are exactly what the scan walks.

**Change:** Allocate `buckets` lazily: only when `seriesSink == nil && histSink == nil`. The paths that
genuinely use the map (M1 `{} | rate()` no-group-by, the N>1 group-by histogram, and the default
`accumulateIntrinsicBucketsViaKeyMap` fallback) all leave both sinks nil per `intrinsicDirectSeriesSinks`,
so they still receive a real (non-nil) map and the nil-map write panic is structurally impossible.

**Correctness:** The post-dispatch consumers are sink-gated: `seriesSink != nil` →
`finalizeCountRateSeries(*seriesSink)`, `histSink != nil` → `finalizeCountRateSeries(*histSink)`, and
only the remaining `case`s read `buckets` (HISTOGRAM N>1 → `traceHistogramSeries`, default →
`traceBuildDenseSeries`) — all of which are unreachable when a sink is set. The lone other `buckets`
read, `len(buckets) == 0` for the empty count/rate-no-group-by short-circuit, is on the sinks-nil
branch (count/rate with `len(GroupBy)==0` never sets seriesSink), and `len(nil) == 0` is well-defined
in Go regardless. No accumulation core writes `buckets` while a sink is active (verified by reading
both sink-bearing cores). Verified by the executor suite + benchmark parity suite.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go:executeTraceMetricsIntrinsic`.


## NOTE-445: stream agg.Field (span:duration) in the 4 remaining compact/direct metrics scans (issue #361)

NOTE-410 streamed the histogram aggregate field on the N=1 group-by **direct** path
(`accumulateHistogramDirect`). An audit of all metrics execution paths found four remaining sites
where the aggregate column (`span:duration`) was still materialized eagerly via
`GetIntrinsicColumn(agg.Field)` purely to drive a sequential forward scan into a fixed-size dense
array. This note extends the same try-stream-then-fallback pattern to all four, eliminating the
last O(rows) eager `agg.Field` decodes from the metrics hot path:

1. **`streamAggN1CompactCore`** (N=1 compact agg: min/max/avg/sum/quantile by service) —
   `scanAggColCompactStreaming` builds the POPCNT `pkRankIndex` over `sortedPKs` once, then streams
   the column page-by-page, rank-scattering each page-local `(BlockRefs[i], Uint64Values[i])` pair
   into `aggValByPos`/`aggPresentByPos` exactly as `scanAggColCompact`'s Flat branch did.
2. **`accumulateAggDirect`** (N=1 direct agg) — `accumulateAggDirectStreaming` streams the column
   and scatters into the dense `groupBuckets` via O(1) `dictByPK`/`bucketByPK` lookups, identical
   per-value work to `accumulateAggDirectScanCol`'s Flat branch.
3. **`accumulateHistogramDirectN0`** (`{} | histogram_over_time(duration)`) —
   `scanHistogramN0Streaming` streams into the single-group `groupCountsFlat`, mirroring
   `scanHistogramN0`'s Flat branch. The N=0 path uses the fixed `histFlatStride`, so no separate
   boundary-count pass is needed (the indexer is pre-warmed lazily during the same scan).
4. **`streamHistogramN1CompactCore`** (compact M8 histogram, the hottest of the four) —
   `streamHistogramCompactAggCol` does **two** streaming passes: pass 1 counts distinct boundaries
   via `histBoundaryCounter` (the streaming analog of `countIntrinsicHistogramBoundaries`,
   byte-identical count) to right-size `actualStride`; pass 2 rank-scatters into `groupCountsFlat`
   via the prebuilt `pkRankIndex` + per-pos `baseOffsetByPos`, mirroring
   `scanAggColHistogramShard`'s Flat branch. Streaming is **serial** (page-by-page): the eager
   path's parallel sharding existed only to amortize a full-column array scan; with page-at-a-time
   decode there is no full array to shard. `streamHistogramCompactAggCol` returns the POOLED
   `groupCountsFlat` + `seenByPos` (caller releases) on `streamed=true`, and acquires nothing on
   `streamed=false` so the eager fallback owns its own buffers.

In every case transient allocation is O(one page), not O(column). All four keep the eager
`GetIntrinsicColumn` scan as the fallback for non-streamable Dict/legacy/absent columns (same
contract as NOTE-410): `Reader.ScanIntrinsicColumn` returns `streamed=false` for those and the
caller takes the original path unchanged. The compressed blob stays in the section cache between
the two histogram passes (`GetOrFetchIntrinsic`), so pass 2 re-decodes (cheap, pooled buffers) but
never re-fetches.

Covered by `intrinsic_aggfield_stream_test.go` (EX-ETM-445-01..08): each streamed scan is asserted
byte-identical to the eager scan it replaces over a real multi-page paged Delta `span:duration`
column (matching `aggValByPos`/`groupBuckets`/`groupCountsFlat`/`seenBy*`/`boundaries`), with a
`>1 distinct cell` assertion so the Delta accumulation is actually exercised, plus a Dict-field
fallback test for each.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go`:
`scanAggColCompactStreaming`, `accumulateAggDirectStreaming`, `scanHistogramN0Streaming`,
`streamHistogramCompactAggCol`.

## NOTE-359: unify four duplicated code paths in the compact N=1 metrics executor (issue #359)

Pure structural refactor of `metrics_trace_intrinsic.go` — no hot-loop algorithm change, no
new benchmark-specific constants. Four byte-for-byte-identical fragments were extracted into
shared unexported helpers (all output byte-identical to the inline code they replaced; pinned by
`intrinsic_n1_helpers_test.go` unit tests + the existing `intrinsic_n1_core_dedup_test.go`
differential equivalence tests that exercise the cores end-to-end):

1. **`unpackPreSortedRefs(refs, vals, tb) (sortedPKs, timeBucketByPos, numSteps, ok, release)`** —
   the identical preamble of the three compact `*FromRefs` wrappers (count/rate, agg, histogram):
   numSteps guard → two pooled acquires (NOTE-125) → `fillPKSetAndTimeBucketsFromPreSortedRefs`.
   Returns `ok=false` (nil slices, no-op release) for the empty-refs / numSteps<=0 guards so the
   caller returns early; otherwise the caller `defer release()`s the pooled slices.

2. **`resolveGroupByDict(r, colName, sortedPKs, dict, dictIdxByPos, rankIdx) (groupByCol, err)`** —
   the three-level group-by scan cascade shared by all three Core functions:
   `streamGroupByColCompactFlat` (NOTE-406 Flat/XOR/Delta stream) → `scanGroupByColCompactDictStreaming`
   (NOTE-407 Dict stream, no O(rows) arena) → eager `GetIntrinsicColumn` + `scanGroupByColCompact`
   fallback. The count/rate path passes a non-nil prebuilt `rankIdx` (NOTE-348); agg/histogram pass
   nil. The returned `groupByCol` is non-nil only on the eager Dict fallback and is consumed
   afterwards by `resolveDictGroupKeys` (a no-op for nil / Flat-streamed columns).

3. **`applyCountRateAbsentRowPass(groupCountsFlat, stepCounts, numGroups, numSteps, totalSeen, inRangeCount)`** —
   the absent-row credit pass (NOTE-091) duplicated in `accumulateCountRateDirect` and
   `accumulateCountRateDirectStreaming`: when `totalSeen < inRangeCount`, subtract each step's
   present (non-zero-group) count from `stepCounts[bk]` and credit the remainder to the absent
   group (slot 0). `//nolint:gosec` G602 on `stepCounts[bk]` — callers' contract is
   `len(stepCounts) >= numSteps` (the inline loop bounded bk by numSteps directly; the helper
   loses that correlation so the linter cannot prove it).

4. **`scatterDictRef(ref, dictIdx, minPK, maxPK, pkBitset, rankPrefix, dictIdxByPos)`** — the inner
   Dict scatter body shared by `scanGroupByColCompactDictSerial` and the parallel worker in
   `scanGroupByColCompactDictParallel`: packKey range gate + bitset membership test + O(1) POPCNT
   rank + `dictIdxByPos[rank] = dictIdx`. The pre-NOTE-359 serial path had a `len(pkBitset)>0`
   guard that the parallel path lacked; that guard was dead (every Dict scatter caller goes through
   `scanGroupByColCompact`, which returns early on `len(sortedPKs)==0` and otherwise always has a
   built bitset), so it was dropped to bring the helper under the inliner budget — verified
   inlinable (cost 78 < 80) via `go build -gcflags=-m=2`; both call sites show "inlining call to
   scatterDictRef", so the hot M4/M6 Dict scatter is not penalized by the extraction.

**Back-ref:** `internal/modules/executor/metrics_trace_intrinsic.go`: `unpackPreSortedRefs`,
`resolveGroupByDict`, `applyCountRateAbsentRowPass`, `scatterDictRef`.

## NOTE-447: Lazy per-program RHS column loading gate for structural queries (2026-06-19)

For structural queries like `{kind=server} >> {kind=client && span.rpc.method!=""}`,
`buildStructuralBlockPlan` previously unioned ALL programs' user-attr columns into
`bp.wantColumns`, causing `span.rpc.method` to be eagerly decoded for every block in
`progBlockSets[1]` — even blocks with zero `kind=client` spans. NOTE-425 already skipped
the ColumnPredicate call for blocks NOT in `progBlockSets[i]`, but blocks that ARE in the
set still paid the full column-decode cost if no actual span satisfied the intrinsic predicate.

This note splits the column union: programs where `nodesList[i]` is non-empty (have an
intrinsic gate like `kind=client`) no longer contribute to the eager `bp.wantColumns`. Their
user-attr columns for programs with non-empty `nodesList[i]` are excluded from `bp.wantColumns`
and lazily registered (not in the `WantOnly` set), so `ParseBlockFromBytes` registers them with
zero decode cost. Before running program i's `ColumnPredicate`, a new pre-check
`anySpanMatchesIntrinsicNodes(idFields, nodesList[i])` scans the already-loaded `idFields` slice
(O(spanCount), early exit) and returns `emptyRowSet{}` immediately if no span satisfies the
intrinsic constraint. When it passes, `ColumnPredicate` runs as today and triggers lazy decode
on first column access via `ensureDecompressed`.

`lookupIntrinsicFieldsTypedForBlock` is moved before `evaluateStructuralPrograms` in
`collectBlockStructuralSpanRecs` so `idFields` is available for the pre-check gate.

NOTE-373 is preserved: `nodesList` is computed once in `buildStructuralBlockPlan` per the plan-once
invariant. NOTE-425 is preserved: the block-set check fires first. Legacy files
(`hasIntrinsic=false`) are unaffected: both the column split and the pre-check gate are
guarded by `hasIntrinsic`.

**Back-ref:** `internal/modules/executor/stream_structural.go:anySpanMatchesIntrinsicNodes`,
`buildStructuralBlockPlan`, `evaluateStructuralPrograms`, `collectBlockStructuralSpanRecs`.

---

## NOTE-448: Pruning completeness — string/bytes range, regex prefix, Int64 and Float64 ColStats (issue #367)

*Added: 2026-06-19*

**Problem:** Blockpack stored string/bytes min/max bounds in `RangeBoundaries`, and stored
Int64 and Float64 bit patterns in `colMinMax`, but none of these were used for pruning.
String range predicates (`span.http.url > "http://z"`) and anchored regex patterns
(`span.service.name =~ "^checkout.*"`) always fell through `rangeRejectsFile` unchanged.
Int64 and Float64 columns never received `HasNumRange = true` from the writer, so
`colStatsRejects` only applied presence pruning to them.

**Gap 1 — String/Bytes file-level reject:** Added `rejectStringRange` and `rejectBytesRange`
helpers to `rangeRejectsFile`. They consume `bounds.StringBounds[0]`/`[last]` and
`bounds.BytesBounds[0]`/`[last]` as the file-wide min/max. These are exact: the writer's
KLL sketch was fed every block's min and max string. Conservative: `truncateBoundaryKey`
may shorten long keys, but only makes the reject criterion looser (false negatives only).

**Gap 2 — Anchored regex prefix rejection:** Added `extractAnchoredLiteralPrefix` (naive
byte scan, no regexp/syntax import, zero allocation) and `rejectRegexByStringBounds`.
`rejectByBoundary` now intercepts `Min==nil && Max==nil && Pattern!=""` leaf nodes and
routes them through `rejectRegexByStringBounds`. Only the `prefix > fileMax` direction is
exploited; the lower-bound direction requires computing `nextPrefix(prefix)` (byte overflow
handling) — omitted to keep the implementation conservative and correct.

**Gap 3 — Float64 ColStats:** Writer (`writer_block.go:finalize`) now emits `HasNumRange=true`
for `ColumnTypeFloat64` and `ColumnTypeRangeFloat64`. Executor added `colStatsRejectsFloat64`
which uses `math.Float64frombits` to decode `stat.MinNum`/`stat.MaxNum` with a NaN guard.

**Gap 4 — Int64 ColStats:** Writer now emits `HasNumRange=true` for `ColumnTypeInt64` and
`ColumnTypeRangeInt64`. Executor added `colStatsRejectsInt64` which casts `stat.MinNum`/
`stat.MaxNum` to `int64` for signed comparison. CRITICAL: the writer and executor changes are
atomic (same commit). If the writer emitted `HasNumRange` for Int64 while the executor still
used `numNodeBoundExceedsMax` (uint64 path), negative int64 values would be pruned incorrectly.

**Type dispatch in `colStatsRejects`:** The `if stat.HasNumRange` block now dispatches on
`bound.Type` before choosing the comparison path: `TypeFloat` → float64 path; `TypeInt` →
signed int64 path; all other types (uint64, duration) → existing uint64 path.

**Backward compatibility:** Old files have `HasNumRange=false` for Int64/Float64 columns;
the new dispatch branches never fire on old files. New files gain more aggressive block pruning.

**Back-ref:** `internal/modules/executor/plan_blocks.go:rejectStringRange`,
`rejectBytesRange`, `rejectRegexByStringBounds`, `extractAnchoredLiteralPrefix`,
`colStatsRejectsInt64`, `colStatsRejectsFloat64`, `colStatsRejects`.
`internal/modules/blockio/writer/writer_block.go:finalize` (switch extension).
