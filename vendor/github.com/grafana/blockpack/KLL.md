# KLL Quantile Sketch

The KLL sketch (Karnin-Lang-Liberty, 2016) is a streaming data structure that approximates the cumulative distribution function (CDF) of a sequence of values using a bounded-size summary. Blockpack uses KLL to build quantile bucket boundaries for the range index, enabling efficient block pruning for numeric range predicates like `span:duration > 5ms`.

## Source

| File | Purpose |
|---|---|
| [`internal/modules/blockio/writer/kll.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/kll.go) | KLL sketch implementation |
| [`internal/modules/blockio/reader/sketch_index.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/reader/sketch_index.go) | KLL read/lookup at query time |
| [`internal/modules/queryplanner/plan_blocks.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/plan_blocks.go) | Where KLL is used for pruning |

**External references:**
- [KLL paper (arXiv:1603.05346)](https://arxiv.org/abs/1603.05346) — Karnin, Lang, Liberty (FOCS 2016)
- [Apache DataSketches KLL reference](https://datasketches.apache.org/docs/KLL/KLLSketch.html)

---

## What It Is

A quantile sketch answers questions of the form: "What value divides the bottom X% of the data from the top (100-X)%?" For example, the median is the 50th-percentile quantile. An exact solution requires storing all values; KLL trades a small approximation error for O(k log n) memory regardless of stream length.

The KLL algorithm provides an **ε-approximate quantile guarantee**: for accuracy parameter k=200 and any query rank φ ∈ [0,1], the returned value has rank within ±(1/k) ≈ ±0.5% of the true rank, with high probability.

---

## Algorithm

KLL maintains a hierarchy of **compactors** (called levels). Each level holds a sorted list of items; item at level h represents 2^h original stream elements.

**Ingestion:** New items enter level 0 (the unsorted input buffer). When level 0 reaches capacity `kllLevelCap(k, numLevels, 0)`, it is sorted, randomly halved (keeping every other element starting at a random offset of 0 or 1), and the survivors are merged into level 1. If level 1 then overflows, the process recurses up the chain.

**Capacity formula** (from KLL'16 §4):

```
capacity(h) = max(M, floor(k × (2/3)^depth))
```

where `depth = numLevels - h - 1` (depth 0 = top level = most compacted), `M = 8` (minimum level capacity), and `k = 200` (accuracy parameter).

**Random halving** (via xorshift64 per-instance RNG) is the key to correctness against adversarial input — it ensures no systematic bias toward low or high values survives compaction. The per-instance RNG means concurrent writers do not race on a shared global.

**Querying quantiles:** The `Boundaries(nBuckets)` method merges all levels into one sorted weighted sequence (weight of item at level h = 2^h), then extracts nBuckets+1 evenly-spaced quantile boundary values via binary search.

**Memory:** O(k × log(n)) items retained. At k=200 and n=100M, roughly 200 × 27 ≈ 5 400 items.

---

## How Blockpack Uses It

Blockpack maintains one KLL sketch per range-indexed column (see `internal/modules/blockio/writer/range_index.go`). The sketch type matches the column type:

- `KLL[int64]` for `ColumnTypeRangeInt64` / `ColumnTypeRangeDuration`
- `KLL[uint64]` for `ColumnTypeRangeUint64` (includes `span:duration`)
- `KLL[float64]` for `ColumnTypeRangeFloat64`
- `KLLString` (wraps `KLL[string]`) for string columns
- `KLLBytes` for bytes columns

During block construction (`buildAndWriteBlock`), the block's per-column minimum and maximum observed values are fed into the column's KLL sketch via `addBlockRangeToColumn`. This means the sketch sees `2 × numBlocks` total additions per column — the min and max of each block, not every individual span value.

At `Flush()` time, `applyRangeBuckets` calls `sketch.Boundaries(1000)` to produce **1 000 quantile bucket boundaries** per column. Each block is then assigned to all KLL buckets that overlap its `[minKey, maxKey]` range. The resulting `map[bucketKey][]blockID` is the range index, serialized into the metadata section.

**At query time,** a predicate like `span:duration > X` is translated to an interval query `[X, MaxUint64]` against the range index. Only blocks assigned to at least one bucket within that interval are included in the candidate set.

Low-cardinality columns (≤ 100 distinct values across all blocks) bypass the KLL path entirely and use an exact-value index instead, giving zero false positives for those columns.

---

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Memory** | O(k log n) — bounded regardless of stream length |
| **Streamable** | Items are processed one at a time; no random access needed |
| **Mergeable** | Two KLL sketches can be merged into one (useful for compaction) |
| **Accuracy** | ε = O(1/k) per-quantile error with high probability |
| **Approximate** | Not exact; KLL bucket granularity limits pruning for small datasets |
| **Write overhead** | ~O(log n) amortized per Add; compaction is rare at upper levels |
| **Type-specific** | A separate sketch instance is needed per column type |

---

## Size

The KLL sketch is **not directly serialized** to disk. Instead, at `Flush()` the sketch's `Boundaries(1000)` output (1 001 boundary values) is used to populate the range index, which is what gets serialized. The sketch itself lives only in writer memory.

During writing, memory per KLL instance: roughly `k × log(n)` items. At k=200 and n=10 000 (blocks per file), the sketch retains ≈ 200 × 14 ≈ 2 800 items. For uint64 that is ~22 KB per column. Typical files have 5–20 range-indexed columns, so total KLL memory is in the hundreds of KB range while writing.

Serialized range index size (the on-disk result): approximately **200–500 bytes per block per range column**, dominated by the bucket-to-block-list mapping.

---

## Error Guarantee

For accuracy parameter k and n total items, the KLL sketch guarantees:

> For any query rank φ, the returned value has true rank in [φ − ε, φ + ε] with probability at least 1 − δ, where ε = O(1/k) and δ is a small failure probability.

At k=200 (blockpack's default), ε ≈ 0.5%. This means a query for the 90th-percentile value might return anything between the true 89.5th and 90.5th percentile. For block pruning this is conservative — the sketch may retain a few extra blocks near the boundary, but never prunes a block that should be included.

---

## Papers and Links

- Original KLL paper: [https://arxiv.org/abs/1603.05346](https://arxiv.org/abs/1603.05346) — Karnin, Lang, Liberty (FOCS 2016)
- Apache DataSketches KLL reference: [https://datasketches.apache.org/docs/KLL/KLLSketch.html](https://datasketches.apache.org/docs/KLL/KLLSketch.html)
- Blockpack implementation: [`internal/modules/blockio/writer/kll.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/kll.go)

---

Back to [Write-Path](Write-Path.md)
