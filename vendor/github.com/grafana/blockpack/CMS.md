# Count-Min Sketch (CMS)

The Count-Min Sketch is a probabilistic frequency counter that estimates how often each value appears in a stream, using a fixed-size 2D grid of counters. Blockpack stores one CMS per column per block and uses it to catch bloom filter false positives: a zero CMS estimate means the value is **definitely absent** from that block.

## Source

| File | Purpose |
|---|---|
| [`internal/modules/sketch/cms.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/cms.go) | CMS implementation |
| [`internal/modules/blockio/writer/sketch_index.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/sketch_index.go) | Where CMS is built per block |
| [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go) | `pruneByCMSAll`, `pruneByCMSPred` |

**External references:**
- [Original paper (Cormode & Muthukrishnan, 2005)](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)
- [Wikipedia: Count-min sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch)

---

## What It Is

A Count-Min Sketch answers: "approximately how many times did value X appear in the stream?" It is a 2D array of `depth × width` integer counters, where each counter is shared across many values (by design). The trade-off: estimates are always ≥ the true count (never undercounts), but may overcount when hash collisions occur.

**Key property:** `Estimate(X) == 0` if and only if X was never added. This no-false-negatives-for-zero property is what blockpack exploits for pruning.

---

## Algorithm

**Structure:** `rows[depth][width]` — a 2D array of counters. Blockpack uses depth=4, width=64, with `uint16` saturating counters (512 bytes total).

**Add(value, count):**
1. Compute a base hash `h = FNV-32a(value)`.
2. For each row `i` in `[0, depth)`, compute column `col = (h XOR (i × 2654435761)) mod 64`.
3. Increment `rows[i][col]` by `count`, saturating at `MaxUint16`.

**Estimate(value):**
1. Compute `h` as above.
2. For each row `i`, read `rows[i][col]`.
3. Return `min(rows[0][col_0], ..., rows[depth-1][col_{depth-1}])`.

The minimum across rows gives the tightest upper bound, since any individual row may have been inflated by hash collisions with other values. The minimum is less likely to be inflated by multiple independent hash functions all colliding on the same bucket.

**Why saturating counters?** A span block contains at most 2 000 spans. With `uint16` (max 65 535) and the 1-per-occurrence Add calls, saturation is essentially impossible for a single block.

---

## How Blockpack Uses It

Every time `updateMinMax` is called during block construction (once per non-null value in each column), the encoded value key is fed to both the block's HLL and CMS via `blockSketchSet.add`. The CMS `Add(key, 1)` increments the counters for that value.

At `Flush()`, per-block CMS data is serialized into the sketch index section: 512 bytes per column per block (4 rows × 64 counters × 2 bytes each).

**At query time** (`pruneByCMSAll` in `internal/modules/queryplanner/scoring.go`):

After the BinaryFuse8 bloom filter pass (which uses a ~0.39% false positive rate), the CMS provides a secondary check. For each candidate block and each query value, if `Estimate(value) == 0`, the value is definitely absent from that column in that block — the block can be pruned. This catches bloom false positives without ever reading block data.

**Scoring:** The CMS also contributes to block scoring: `score = freq / max(cardinality, 1)`, where `freq` comes from the CMS estimate and `cardinality` from the HLL. Higher selectivity (high frequency, low cardinality) means the block is more likely to contain matching spans and should be prioritized for early query termination.

**Pipeline:** Fuse filter → CMS zero-prune → score → prioritized block reads.

---

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Fixed size** | Always 512 bytes per column per block regardless of cardinality |
| **No false negatives for zero** | `Estimate(X) == 0` → X is definitely absent; safe to prune |
| **Fast** | O(depth) = O(4) hash lookups per query |
| **Overcounts** | Estimates are upper bounds; may be inflated by collisions |
| **No deletion** | Counters can only increase; removing values is not supported |
| **Approximate** | Cannot give exact counts; only useful for "present or not" and rough frequency |
| **Width sensitivity** | With width=64, collision probability ≈ 1.6% per row; four independent rows reduce false inflation |

---

## Key Property for Blockpack

The **zero-estimate guarantee** is the critical property: because counters only increase (never decrease), if `Estimate(X) == 0` across all four rows, then X's hash never collided with any added value in any row. This means X was definitely never added — no false negatives for zero estimates.

This makes CMS zero-pruning **safe**: it never incorrectly prunes a block that should be included, while providing a practical speedup by catching bloom filter false positives.

---

## Size

**Per column per block:** 512 bytes fixed.
- 4 rows × 64 counters × 2 bytes per `uint16` counter = 512 bytes exactly.

**Typical file overhead:** With ~50 blocks and ~10 columns each, total CMS data ≈ 50 × 10 × 512 bytes = 256 KB. This is stored in the sketch index section of the metadata, which is snappy-compressed.

---

## Papers and Links

- Original paper: [https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf) — Cormode & Muthukrishnan (2005)
- Wikipedia: [https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch)
- Blockpack implementation: [`internal/modules/sketch/cms.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/cms.go)
- Query-time usage: [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go)

---

Back to [Write-Path](Write-Path.md)
