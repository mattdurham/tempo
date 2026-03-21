# Bloom Filters (BinaryFuse8)

Blockpack uses **BinaryFuse8** filters — not classic Bloom filters — at two levels of granularity: per column per block, and per file. Both answer "is value X possibly in this set?" with no false negatives and a ~0.39% false positive rate. They are the first and fastest pruning step in the query pipeline.

## Source

| File | Purpose |
|---|---|
| [`internal/modules/sketch/fuse.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/fuse.go) | BinaryFuse8 wrapper |
| [`internal/modules/blockio/writer/sketch_index.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/sketch_index.go) | Per-block bloom construction |
| [`internal/modules/blockio/writer/writer.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/writer.go) | File-level FBLM construction |
| [`internal/modules/queryplanner/plan_blocks.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/plan_blocks.go) | `pruneByFuseAll` usage |

**External references:**
- [BinaryFuse8 paper (arXiv:2201.01174)](https://arxiv.org/abs/2201.01174) — Graf & Lemire (2022)
- [FastFilter/xorfilter (Go implementation)](https://github.com/FastFilter/xorfilter)

---

## What It Is

A Bloom filter is a space-efficient probabilistic data structure for set membership: it answers "is X in the set?" with no false negatives (if X was added, the answer is always yes) but a configurable false positive rate (sometimes says yes for values that were not added).

Classic Bloom filters use multiple hash functions over a bit array. **BinaryFuse8** is a newer construction that achieves better compression and faster lookup using a technique called "binary fuse filtering" (also called a 3-wise XOR filter with a fingerprint array).

---

## BinaryFuse8 vs Standard Bloom

| Property | Standard Bloom (optimal) | BinaryFuse8 |
|----------|--------------------------|-------------|
| Space per entry | ~1.44 bytes | ~1.08 bytes (~25% smaller) |
| Lookup cost | k hash functions (k ≈ 7) | 3 array accesses, fixed |
| False positive rate | tunable (e.g. 0.39%) | ~0.39% fixed |
| Construction | O(n) inserts | O(n) build (requires all keys upfront) |
| Deletion | No | No |
| Ordered lookup | No | No |

BinaryFuse8's key advantages for blockpack are:
- **Lower space**: ~1 byte per distinct value vs ~1.44 bytes for optimal Bloom
- **Cache-friendly**: exactly 3 array accesses per lookup, predictable memory access pattern
- **Same FPR**: both achieve ~0.39% at this size setting

The tradeoff is that BinaryFuse8 requires all keys to be known at construction time (static filter), which is fine for blockpack because filters are built at block-write time when the block is complete.

Blockpack uses the `FastFilter/xorfilter` library, wrapped in `internal/modules/sketch/fuse.go`.

---

## Two Levels in Blockpack

### Block-Level: One Filter Per Column Per Block

During block construction (`blockSketchSet.add`), every non-null value for each column is hashed via `sketch.HashForFuse(key)` and appended to a `[]uint64` keys slice. At the end of `writeSketchIndexSection`, a `BinaryFuse8` filter is built from those keys and serialized for each (column, block) pair.

**At query time** (`pruneByFuseAll` in `internal/modules/queryplanner/scoring.go`), this is the **first** pruning stage. For each query predicate:
- Fetch the full `FuseContains` result slice (one bool per block) for each queried value
- A block is pruned if the filter says "definitely not present" for all queried values in the predicate column

This step has zero false negatives — if a block contains matching values, it always passes. The ~0.39% FPR means roughly 1 in 250 blocks is a false positive and will be read unnecessarily. That false positive is caught by the CMS zero-estimate check in the next pipeline stage.

**AND semantics:** For multi-predicate queries, a block must pass the fuse check for ALL predicates to remain a candidate.

### File-Level: FBLM (File-Level Bloom)

The FBLM section (magic `FBLM`) contains one BinaryFuse8 filter over `resource.service.name` across the entire file — covering every span in every block. It is checked once when the query planner evaluates a file against a `service.name` predicate.

If the queried service name is not present in the file's filter, the file is skipped entirely: **zero block reads**. This is checked in `fileLevelBloomReject` in `plan_blocks.go`.

The FBLM is particularly valuable for multi-tenant environments where each file predominantly contains spans from a small set of services. A query for `{service.name="payments"}` can skip any file that never received payments spans without opening it.

---

## Hash Function

Both block-level and file-level filters use the same hash: `sketch.HashForFuse(value)`, which is FNV-1a 64-bit with a finalizing bit-mix step. The same hash function is used at write time and query time (enforced by `SPEC-SK-16`):

```go
func HashForFuse(v string) uint64 {
    h := fnv1a64(v)
    // Finalization mix for uniform high-bit distribution:
    h ^= h >> 33
    h *= 0xff51afd7ed558ccd
    h ^= h >> 33
    h *= 0xc4ceb9fe1a85ec53
    h ^= h >> 33
    return h
}
```

The finalization mix is critical: BinaryFuse8 uses the high bits for register selection, so the hash must be well-distributed in the upper bits.

---

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **No false negatives** | A value that was added is always found; no data loss from pruning |
| **Space-efficient** | ~1 byte per distinct value — much cheaper than storing the values |
| **Fast** | 3 array accesses per lookup; extremely cache-friendly |
| **Static** | Must rebuild to add new values; built once at block-write time |
| **False positives** | ~0.39% FPR; caught by CMS zero-prune in the next stage |
| **Size grows with cardinality** | High-cardinality columns (many distinct values) produce larger filters |
| **Construction requires all keys** | Cannot add values incrementally; all keys must be present before building |

---

## Size

**Block-level filter:** ~1 byte per distinct value per column per block. The exact size depends on the xorfilter library's fingerprint array size, which scales linearly with the number of deduplicated keys. A column with 500 distinct values in a block takes approximately 500 bytes.

**File-level filter (FBLM):** ~1 byte per distinct `resource.service.name` value across the entire file. A file with 50 distinct service names takes approximately 50 bytes.

**Typical block overhead:** With 2 000 spans and ~20 attribute columns, each column averaging 100 distinct values, block-level bloom total ≈ 20 × 100 bytes = 2 KB per block. Actual size varies greatly with column cardinality.

---

## Papers and Links

- BinaryFuse8 paper: [https://arxiv.org/abs/2201.01174](https://arxiv.org/abs/2201.01174) — Graf & Lemire (2022)
- Original Bloom filter: [https://dl.acm.org/doi/10.1145/362686.362692](https://dl.acm.org/doi/10.1145/362686.362692) — Bloom (1970)
- FastFilter/xorfilter (Go implementation used by blockpack): [https://github.com/FastFilter/xorfilter](https://github.com/FastFilter/xorfilter)
- Blockpack wrapper: [`internal/modules/sketch/fuse.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/sketch/fuse.go)
- Query-time usage: [`internal/modules/queryplanner/scoring.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/queryplanner/scoring.go) (`pruneByFuseAll`)

---

Back to [Write-Path](Write-Path.md)
