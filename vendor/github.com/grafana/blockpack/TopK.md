# TopK: Approximate Top-K Frequency Tracker

TopK tracks the most frequently occurring values in a column within a single block. Blockpack stores the top 20 values per column per block and uses them at query time for block scoring — blocks that contain a high-frequency match for the query predicate are prioritized for reading, enabling faster early query termination.

## Source

| File | Purpose |
|---|---|
| [`internal/modules/sketch/topk.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/topk.go) | TopK implementation |
| [`internal/modules/blockio/writer/sketch_index.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/sketch_index.go) | Where TopK is built per block |
| [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go) | Where TopK feeds scoring |

---

## What It Is

A Top-K data structure maintains a bounded set of the K most frequent items seen in a stream. The exact answer requires a full frequency map of all distinct values; approximate solutions trade some accuracy for bounded memory.

Blockpack's implementation uses an **exact frequency map** (not approximate), which is safe because a single block contains at most 2 000 spans. The map is bounded in practice by block size, so memory is not a concern.

---

## Algorithm

**Structure:** `counts map[string]uint32` — a simple exact frequency map.

**Add(key):** Increment `counts[key]` by 1. Keys longer than 100 bytes are truncated before insertion.

**Entries():** Drain the map into a `[]TopKEntry{Key, Count}` slice, sort descending by count (ties broken alphabetically), and return at most 20 entries.

**Wire format** (`MarshalBinary`):
```
topk_count[1] + per_entry {
    key_len[2 LE] + key[N bytes] + count[4 LE uint32]
}
```
The stored count is truncated to `uint16` for compactness in the sketch index wire format.

At query time, stored entries are compared using `sketch.HashForFuse(key)` fingerprints — the exact key bytes are not preserved in the sketch index (only the hash fingerprint and count), so comparison is probabilistic with a negligible collision rate.

---

## How Blockpack Uses It

During block construction, every call to `updateMinMax` (once per non-null value per column) feeds the encoded value key into `blockSketchSet.add`, which calls `topk.Add(key)`.

At `Flush()`, `Entries()` is called and the top-20 results are serialized into the sketch index section, stored column-major across all blocks.

**At query time,** the TopK entries are read from the sketch index and used in `scoreBlocks` (see `internal/modules/queryplanner/scoring.go`). The scoring formula is:

```
score = freq / max(cardinality, 1)
```

where:
- `freq` = CMS estimate of the queried value's frequency in this block (or TopK exact count if the value appears in the top 20)
- `cardinality` = HLL estimate of distinct values in this column in this block

A higher score means the block has a high concentration of the queried value relative to its overall diversity — it is more selective and likely to produce matches. Blocks are sorted by score so that the most relevant blocks are read first, enabling early query termination when a `limit` is specified.

---

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Simple** | Exact frequency map per block; no approximation needed given block size bounds |
| **Fast scoring** | O(1) lookup per query value against the top-20 set |
| **Selective** | High-selectivity blocks get higher scores → fewer total block reads for limit queries |
| **Bounded coverage** | Only the top 20 values are tracked; long-tail values fall back to CMS estimates |
| **Block-scoped** | Per-block, not file-level; a common value that is rare in one block is scored low for that block |
| **Truncation** | Keys > 100 bytes are truncated; very long attribute values may produce incorrect matches |

---

## Size

**Per column per block:**
- Header: 1 byte (entry count)
- Per entry: 2 bytes (key length) + key bytes (≤ 100) + 4 bytes (count)
- Maximum: 1 + 20 × (2 + 100 + 4) = 1 + 20 × 106 = 2 121 bytes
- Typical (short keys, ~10 byte average): 1 + 20 × (2 + 10 + 4) ≈ 321 bytes

**Typical file overhead:** With ~50 blocks and ~10 columns, total TopK data ≈ 50 × 10 × 321 bytes ≈ 161 KB (before snappy compression in the metadata section).

---

## Related Reading

- Space-Saving algorithm (related approximate approach): [https://www.cs.ucsb.edu/research/tech-reports/2005-23](https://www.cs.ucsb.edu/research/tech-reports/2005-23)
- Blockpack implementation: [`internal/modules/sketch/topk.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/topk.go)
- Query-time scoring: [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go)
- Block sketch accumulation: [`internal/modules/blockio/writer/sketch_index.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/sketch_index.go)

---

Back to [Write-Path](Write-Path.md)
