# HyperLogLog (HLL)

HyperLogLog estimates the cardinality (number of distinct values) of a multiset using a compact probabilistic data structure. Blockpack stores one HLL per column per block — just 16 bytes — and uses the estimated distinct count at query time to score blocks by selectivity.

## Source

| File | Purpose |
|---|---|
| [`internal/modules/sketch/hll.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/hll.go) | HLL implementation |
| [`internal/modules/blockio/writer/sketch_index.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/sketch_index.go) | Where HLL is built per block |
| [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go) | Where HLL feeds cardinality scoring |

**External references:**
- [Original HyperLogLog paper (Flajolet et al., 2007)](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

---

## What It Is

Cardinality estimation answers: "how many distinct values appeared in this column in this block?" Exact counting requires O(n) memory; HyperLogLog achieves a 16-byte summary with a predictable error rate.

The intuition: if you hash elements uniformly, the maximum number of leading zeros in any hash is `log2(n)` on average. HyperLogLog generalizes this idea across m buckets (registers), using the harmonic mean to reduce variance.

---

## Algorithm

**Structure:** `regs [16]uint8` — 16 registers of 1 byte each (p=4, m=2^4=16).

**Add(value):**
1. Hash the value to a 64-bit integer (FNV-1a with finalizing bit-mix for uniform high-bit distribution).
2. Use the top `p=4` bits to select register index `reg` (0–15).
3. Shift out the top bits; count leading zeros in the remaining 60 bits, add 1 → `rho`.
4. If `rho > regs[reg]`, update `regs[reg] = rho`.

**Cardinality():**
1. Compute the harmonic mean across all 16 registers: `estimate = α × m² / Σ(2^{-regs[i]})`.
2. Apply small-range correction (linear counting) when the estimate is small and some registers are still zero.

The bias correction constant `α = 0.673` is specific to m=16.

**Error rate at p=4:** The standard error is approximately `1.04 / sqrt(m) = 1.04 / sqrt(16) ≈ 26%`. This is high by HLL standards — typical deployments use p=10 to p=14 for ~3% error. Blockpack deliberately uses p=4 (16 bytes) because cardinality estimates are used only for **relative scoring**, not for precise counting. A 26% error on cardinality does not affect correctness; it only affects the ranking of candidate blocks.

---

## How Blockpack Uses It

Every call to `updateMinMax` during block construction feeds the encoded value key into `blockSketchSet.add`, which calls `hll.Add(key)`. One HLL is maintained per column per block.

At `Flush()`, `hll.Cardinality()` is called for each column/block pair and the result stored as a `uint32` in the sketch index. The 16-register state is **not** serialized to disk — only the scalar cardinality estimate.

**At query time** (`scoreBlocks` in `internal/modules/queryplanner/scoring.go`):

The cardinality estimate is the denominator in the selectivity score:

```
score = freq / max(cardinality, 1)
```

A block with `cardinality=1` (only one distinct value) and a CMS frequency estimate of 2000 gets score 2000 — it is extremely selective for that single value. A block with cardinality=1000 and frequency=2 gets score 0.002 — it is a poor match and should be deprioritized.

Higher-scored blocks are read first, reducing total I/O for queries with a `limit` clause.

---

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Extremely compact** | 16 bytes per column per block; negligible overhead |
| **Streamable** | Works in a single pass; no random access needed |
| **Mergeable** | Two HLL sketches can be merged by taking the element-wise max across registers |
| **High error at p=4** | ~26% standard error; only suitable for relative comparisons, not precise counts |
| **No exact counts** | For small cardinalities (< 10), estimates can be significantly off |
| **Read-only after construction** | No deletion; the only supported operations are Add and Cardinality |

---

## Size

**Per column per block (serialized):** The sketch index stores the `uint32` cardinality estimate — **4 bytes** per column per block. The 16-register `[16]uint8` state is used only in memory during the write pass.

**Typical file overhead (serialized):** With ~50 blocks and ~10 columns, total HLL data ≈ 50 × 10 × 4 bytes = 2 KB. Negligible.

---

## Papers and Links

- Original HyperLogLog paper: [https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) — Flajolet, Fusy, Gandouet, Meunier (2007)
- HyperLogLog++ (Google, practical improvements): [https://research.google/pubs/hyperloglog-in-practice-algorithmic-engineering-of-a-state-of-the-art-cardinality-estimation-algorithm/](https://research.google/pubs/hyperloglog-in-practice-algorithmic-engineering-of-a-state-of-the-art-cardinality-estimation-algorithm/)
- Blockpack implementation: [`internal/modules/sketch/hll.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/hll.go)
- Query-time scoring: [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go)

---

Back to [Write-Path](Write-Path.md)
