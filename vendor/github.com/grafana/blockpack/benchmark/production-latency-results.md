# Blockpack vs Parquet — Production Latency Benchmark Results

**Date:** 2026-03-24
**Dataset:** ~6.7 M traces, real-world Grafana Cloud production traffic
**S3 latency:** 36 ms average per GET request (measured on production Grafana Cloud Tempo
queriers: `tempodb_backend_request_duration_seconds`, operation=GET status_code=206)
**Configuration:** GOGC=100, GOMEMLIMIT=20 GiB
**Compaction level:** Level 2 (fully compacted)

---

## Storage Footprint

| Backend | On-disk size | Block count |
|---------|-------------|-------------|
| Parquet (vParquet5) | 3.29 GB | 16 |
| Blockpack (pre-#172) | 3.64 GB | 14 |
| Blockpack (post-#172) | 2.64 GB | 15 |

After PR #172 (intrinsic column index with dual-storage), blockpack is **20%
smaller** than parquet on the same dataset. The earlier measurement (3.64 GB,
+11% vs parquet) predates the dual-storage intrinsic layout. Both use default
configuration with no dedicated columns.

---

## Single-Query p50 Latency (warm cache, limit=20)

Each figure is the median of 5 runs against a fully warmed filecache.
All queries issued against compacted level-2 blocks.

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

**Blockpack wins all 10 categories.** The advantage is most pronounced for
intrinsic-column queries (status, kind, service name) because blockpack's
per-file intrinsic TOC eliminates full block reads entirely — results come
from pre-sorted flat column blobs. Parquet must scan row groups for every
predicate regardless of column type.

---

## Parallel Load Throughput (1500 queries × 50 concurrent)

Stress test: 30 representative query patterns, each fired 50 times in
parallel (1500 total). Measures sustained throughput under realistic concurrent load.

### With blockpack filecache enabled (bbolt disk cache)

| Metric | Parquet | Blockpack warm | Blockpack cold (file-per-entry) | Blockpack cold (bbolt, old) |
|--------|---------|----------------|---------------------------------|-----------------------------|
| Wall time | 16.3 s | **17.8 s (+9%)** | **19.0 s (+16%)** | 32.9 s (+102%) |
| Peak CPU | ~1490% | ~1032% | ~1032% | ~1032% |
| Post-stress memory | 7.0 GiB | **3.5 GiB (−50%)** | — | — |

The file-per-entry filecache (PR #169) eliminated the bbolt write-lock bottleneck.
Cold start improved from **32.9s → 19.0s (42% faster)**. With warm cache, blockpack
is within 9% of parquet at production latency and uses 50% less memory.

The previous bbolt cold-start penalty (32.9s, 2× slower than parquet) was caused by
~500 concurrent `Put()` calls serialising behind bbolt's single write lock. The
file-per-entry replacement writes different keys in parallel at the OS level.

### Apples-to-apples: no persistent cache on either side

Both backends hit S3 directly on every request (blockpack filecache disabled,
parquet has no equivalent persistent disk cache in this configuration).

| Metric | Parquet | Blockpack | Delta |
|--------|---------|-----------|-------|
| Wall time | 18.1 s | **17.0 s** | **−6%** |
| Peak CPU | ~1490% | ~1032% | −31% |
| Post-stress memory | 6.4 GiB | **4.9 GiB** | **−23%** |

Without any caching advantage, blockpack is still 6% faster and 23% less memory
at production S3 latency. The advantage comes from fewer S3 requests (intrinsic
TOC pruning) and lower in-flight allocation cost (read buffer pooling, pre-allocated
column merge).

### Why blockpack is faster without a cache

Parquet makes ~124,000 S3 range requests per 1500-query run (many small column reads
per row group). Blockpack makes ~29,000 (4.3× fewer), because the intrinsic TOC allows
file-level and per-block rejection before any block data is fetched. At 36 ms/request,
fewer requests directly reduces wall time even though individual reads are larger.

### Memory breakdown (in-flight during load)

The dominant memory consumers (from heap profile during load):

| Source | Allocation rate | Description |
|--------|----------------|-------------|
| Read buffers (pooled) | ~37 GB/25 s | S3 block data in flight; pooled via `sync.Pool` |
| Filecache returns | ~24 GB/25 s | Block bytes returned from disk cache |
| Intrinsic column decode | ~24 GB/25 s | Paged intrinsic blobs (snappy → decoded) |
| Sketch summary build | ~21 GB/25 s | Per-file CMS + Fuse8 aggregation |
| Column decode | ~90 GB/25 s | zstd/XOR/delta column decompression |

---

## Key Optimisations Applied (this branch)

Performance improvements measured against the unoptimised baseline:

| Optimisation | Wall time | Memory |
|-------------|-----------|--------|
| Baseline (GOGC=80, no pool) | 33 s | 16 GiB peak |
| Remove SharedLRU double-alloc | 23 s | — |
| GOGC=200 | 18 s | 11 GiB |
| Cache FileSketchSummary | 18 s | — (−31% CPU) |
| Parallel block I/O in forEachBlockInGroups | 18 s | — |
| GOGC=100 | 20 s | 6.2 GiB |
| Read buffer sync.Pool + early-free | 18.4 s | 5.3 GiB |
| Pre-allocate intrinsic column merge | **16.6 s** | **4.1 GiB** |

---

## S3 Request Efficiency

Blockpack makes far fewer S3 range requests per query than Parquet because
its intrinsic TOC enables file-level rejection and per-block pruning before
any block data is fetched.

| Query Category | Parquet range GETs | Blockpack range GETs |
|----------------|--------------------|----------------------|
| Match-all | many (all row groups) | few (intrinsic fast path) |
| Intrinsic range | many | **3** (intrinsic blob only) |
| Intrinsic equality | many | ~18–20 |
| User attribute filter | many | ~55–60 (Fuse8 pruned) |

Blockpack's lower request count is the primary reason it outperforms Parquet
at 36 ms/request latency — fewer round-trips directly reduces wall time.
Parquet's advantage at lower latencies (< 20 ms) comes from column-level I/O
projection: it reads only the needed column bytes per row group, whereas
blockpack reads the full block and filters at decode time.

---

## Notes

- Results measured on localhost with s3fs (single-node, no network jitter).
  Production results may vary based on S3 region, object size distribution,
  and query concurrency.
- Parquet uses vParquet5 with default configuration (no dedicated columns).
- Blockpack uses the `parallel-block-reads` branch incorporating all
  optimisations listed above.
- GC settings (GOGC=100, GOMEMLIMIT=20 GiB) reflect the recommended
  production configuration for this workload.
