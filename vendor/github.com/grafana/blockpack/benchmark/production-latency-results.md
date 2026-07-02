# Blockpack vs Parquet — Production Latency Benchmark Results

## Latest: 2026-03-31 (48 GB full dataset)

**Dataset:** ~85 GB source traces (full Grafana Cloud production traffic), 48 GB on-disk
**S3 latency:** 36 ms average per GET request
**Configuration:** GOMEMLIMIT=20 GiB (no GOGC set — default)
**Compaction level:** Level 2 (92 compacted blocks)

### Storage Footprint

| Backend | On-disk size | Block count |
|---------|-------------|-------------|
| Parquet (vParquet5) | 47 GB | ~100 |
| Blockpack | 48 GB | 92 |

**Storage parity** at production scale.

### Single-Query p50 Latency (warm cache, limit=20)

| ID | Query | PQ p50 | BP p50 | Ratio |
|----|-------|--------|--------|-------|
| 1 | `{}` match-all | 604ms | **57ms** | **0.09x** |
| 2 | `{svc=CockroachDB}` | 679ms | **57ms** | **0.08x** |
| 6 | `{status=error}` | 681ms | **67ms** | **0.10x** |
| 8 | `{duration>100ms}` | 646ms | **239ms** | **0.37x** |
| 20 | `{svc=grafana && kind=server && status=error}` | 946ms | **169ms** | **0.18x** |
| 21 | `{svc=grafana && http.method=GET && status=error}` | 1123ms | **356ms** | **0.32x** |
| 15 | `{svc=grafana && http.status_code>=500}` | 1326ms | **357ms** | **0.27x** |
| 17 | `{svc=~loki-.* && rpc.system=grpc && status=error}` | 4ms | **4ms** | 0.90x |
| 23 | `{db.system != nil}` | 3ms | 3ms | 1.03x |
| 22 | `{svc=loki-querier && status=error && duration>50ms}` | 1179ms | 1960ms | **1.66x** |

**Result: 8 wins, 1 loss, 1 tie.**

The only loss (q22) is a 3-condition AND with a range predicate where the `overFetch`
intersection yields few matches per file, requiring more files to be processed.

### Architecture (as of this benchmark)

- **Intrinsic section:** predicate evaluation ONLY (SPEC-INTRINSIC-001). Finds matching
  refs via sorted-by-value binary search on dict/flat columns.
- **Field population:** block reads via `forEachBlockInGroups` (O(M) where M = result count).
  At scale, this is 200x faster than the previous `lookupIntrinsicFields` O(N×C) scan.
- **Identity columns** (trace:id, span:id, span:parent_id, span:status_message) removed from
  intrinsic section — 48% storage reduction. Values come from block columns (dual storage).
- **RefBloom** removed (100% FPR at 10K entries/page). Value bloom kept for dict pruning.
- **FileBloom** checked before intrinsic scan (SPEC-INTRINSIC-004) — O(1) file rejection.
- **Strong-reference objectcache** (sync.Map replaces weak.Pointer) — prevents filecache
  re-reads caused by GC eviction of weak pointers.
- **SPEC-ROOT-010:** errors are never swallowed. `slog.Error` on intrinsic fast path fallback.

---

## Previous: 2026-03-24 (3.3 GB dataset)

**Dataset:** ~6.7 M traces, real-world Grafana Cloud production traffic
**S3 latency:** 36 ms average per GET request
**Configuration:** GOGC=100, GOMEMLIMIT=20 GiB
**Compaction level:** Level 2 (fully compacted)

### Storage Footprint

| Backend | On-disk size | Block count |
|---------|-------------|-------------|
| Parquet (vParquet5) | 3.29 GB | 16 |
| Blockpack (pre-#172) | 3.64 GB | 14 |
| Blockpack (post-#172) | 2.64 GB | 15 |

After PR #172 (intrinsic column index with dual-storage), blockpack is **20%
smaller** than parquet on the same dataset.

### Single-Query p50 Latency (warm cache, limit=20)

| Query Category | Parquet p50 | Blockpack p50 | Ratio |
|----------------|------------|---------------|-------|
| Match-all (no predicates) | 603 ms | 121 ms | **0.20x** |
| Intrinsic range predicate | 641 ms | 583 ms | 0.91x |
| Intrinsic equality — single field | 678 ms | 61 ms | **0.09x** |
| Intrinsic equality — categorical field | 674 ms | 62 ms | **0.09x** |
| Service name equality | 717 ms | 79 ms | **0.11x** |
| Service name regex | 723 ms | 184 ms | **0.25x** |
| User attribute filter | 770 ms | 292 ms | **0.38x** |
| Service + intrinsic AND | 861 ms | 783 ms | 0.91x |
| Service + user attribute AND | 857 ms | 462 ms | **0.54x** |
| Multi-condition AND (regex + attribute + intrinsic) | 1856 ms | 1421 ms | **0.77x** |

**Blockpack wins all 10 categories.**

---

## Parallel Load Throughput (1500 queries × 50 concurrent)

*Measured on the 3.3 GB dataset (2026-03-24).*

### With blockpack filecache enabled (bbolt disk cache)

| Metric | Parquet | Blockpack warm | Blockpack cold (file-per-entry) |
|--------|---------|----------------|--------------------------------|
| Wall time | 16.3 s | **17.8 s (+9%)** | **19.0 s (+16%)** |
| Peak CPU | ~1490% | ~1032% | ~1032% |
| Post-stress memory | 7.0 GiB | **3.5 GiB (−50%)** | — |

### Apples-to-apples: no persistent cache on either side

| Metric | Parquet | Blockpack | Delta |
|--------|---------|-----------|-------|
| Wall time | 18.1 s | **17.0 s** | **−6%** |
| Peak CPU | ~1490% | ~1032% | −31% |
| Post-stress memory | 6.4 GiB | **4.9 GiB** | **−23%** |

Without any caching advantage, blockpack is still 6% faster and 23% less memory
at production S3 latency.

---

## S3 Request Efficiency

| Query Category | Parquet range GETs | Blockpack range GETs |
|----------------|--------------------|----------------------|
| Match-all | many (all row groups) | few (intrinsic fast path) |
| Intrinsic range | many | **3** (intrinsic blob only) |
| Intrinsic equality | many | ~18–20 |
| User attribute filter | many | ~55–60 (Fuse8 pruned) |

Blockpack's lower request count is the primary reason it outperforms Parquet
at 36 ms/request latency — fewer round-trips directly reduces wall time.

---

## Notes

- Results measured on localhost with s3fs (single-node, no network jitter).
- Parquet uses vParquet5 with default configuration (no dedicated columns).
- GC settings reflect the recommended production configuration.
- The 48 GB benchmark uses the full 85 GB Grafana Cloud trace dataset.
