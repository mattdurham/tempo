# queryplanner — Benchmark Specifications

This document defines the benchmark suite for the `queryplanner` package. Each benchmark is
specified as a `testing.B` function with required custom metrics. Benchmarks measure planning
cost independently of I/O by pre-building the `BlockIndexer` outside the timed loop.

---

## Metric Targets (Never Regress Below)

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| Plan latency (100 blocks, no predicates) | < 50 µs | 50–200 µs | > 200 µs |
| Plan latency (100 blocks, 1 predicate) | < 100 µs | 100–300 µs | > 300 µs |
| `FetchBlocks` io_ops (100 blocks) | = 1 coalesced read | — | > 5 reads |

---

## 1. Planning Benchmarks

### BENCH-QP-01: BenchmarkPlanNoPredicates

Measures the baseline cost of `Plan(nil)` — no bloom work, just `allBlocks`.

**Setup (outside timed loop):**
- Write N spans with `MaxBlockSpans=20` (N/20 blocks).
- Open reader; create `Planner`.

**Variants:**

| Sub-benchmark | Blocks |
|---------------|--------|
| `_10blocks`   | 10     |
| `_100blocks`  | 100    |
| `_1000blocks` | 1000   |

**Required custom metrics:**
```go
b.ReportMetric(float64(b.N)/elapsed.Seconds(), "plans/sec")
b.ReportMetric(float64(numBlocks), "blocks")
```

---

### BENCH-QP-02: BenchmarkPlanBloomOnly

Measures `Plan` cost when bloom pruning eliminates ~50% of blocks.

**Setup (outside timed loop):**
- Half the blocks have `"rare.attr"`, half do not.
- Predicate: `Column="resource.rare.attr"`.

**Variants:**

| Sub-benchmark | Blocks |
|---------------|--------|
| `_100blocks`  | 100    |
| `_1000blocks` | 1000   |

**Required custom metrics:**
```go
b.ReportMetric(float64(plan.PrunedByBloom)/float64(plan.TotalBlocks)*100, "pct_bloom_pruned")
b.ReportMetric(float64(b.N)/elapsed.Seconds(), "plans/sec")
```

---

## 2. FetchBlocks Benchmarks

### BENCH-QP-03: BenchmarkFetchBlocks

Measures end-to-end I/O for `FetchBlocks` using an in-memory provider.

**Setup (outside timed loop):**
- Write N spans; open reader with in-memory provider.
- Call `Plan(nil)` to select all blocks.

**Variants:**

| Sub-benchmark | Blocks |
|---------------|--------|
| `_10blocks`   | 10     |
| `_100blocks`  | 100    |

**Required custom metrics:**
```go
b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/op")
b.ReportMetric(float64(len(rawBlocks)), "blocks_fetched")
```

---

## 3. Removed Benchmarks

The following benchmarks were removed when dedicated column index support was removed
(see NOTES.md §7):

- **BENCH-QP-03 (old): BenchmarkPlanDedicatedIndex** — measured planning cost with
  dedicated index pruning for a single-value service-name predicate.
- **BENCH-QP-04 (old): BenchmarkPlanORValues** — measured planning cost with a multi-value
  OR predicate against a 100-block file.
- **BENCH-QP-06 (old): BenchmarkPlanAndFetch** — measured combined Plan + FetchBlocks cost
  with dedicated index pruning for a selective service-name predicate.
