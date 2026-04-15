# TraceQL Query Reference

**Environment:** dev-us-east-0 / tempo-dev-test-03  
**Tenant:** `X-Scope-OrgID: 11638`  
**Generated:** 2026-04-12 (updated 2026-04-14 with r60/r62 results; updated 2026-04-15 with disk cache + parquet comparison)  
**Time range queried:** last 3 hours (search), last 6 hours (metrics, r62+disk)  
**Services active:** alloy, CockroachDB, apiserver, grafana

API endpoint: `http://<query-frontend>:3100/tempo/api/`

## Org IDs (from jaeger/mirror.libsonnet in deployment_tools)

| Cell | Org ID | Format | Endpoint |
|------|--------|--------|----------|
| tempo-dev-test-03 (dev-us-east-0) | `11638` | blockpack (vblockpack) | tempo-dev-test-03-dev-us-east-0.grafana-dev.net |
| tempo-dev-02 (dev-us-east-0) | `6121` | parquet | tempo-dev-02-dev-us-east-0.grafana-dev.net |

Source: `deployment_tools/ksonnet/environments/jaeger/mirror.libsonnet` (`TEMPO_MIRROR_USERNAME` / `TEMPO_MIRROR2_USERNAME`)

---

## Search Queries

Executed via `GET /tempo/api/search` with `limit=20`.  
Results: `returned` = traces in response, `inspectedBytes` = data scanned.

> **Observation:** Duration and status predicates (`{status=error}`, `{duration > 100ms}`) scan ~1MB because
> they hit the index directly. OR conditions across service + http attributes force full block scans (1–1.7GB).

### Latency Summary

r28 numbers = cold cache. r29 (cold) = measured immediately after pod restart. r29 (warm) = after 3 warmup passes, caches populated.

| # | Query | r28 ms | r29 cold | r29 warm | delta (warm) |
|---|---|---|---|---|---|
| 1 | `{}` | 1400 | 1571 | **192** | -1208 |
| 2 | `{span.http.method="GET"}` | 1112 | 1994 | **199** | -913 |
| 3 | `{resource.service.name="CockroachDB"}` | 1360 | 1359 | **196** | -1164 |
| 4 | `{status=error}` | 193 | 1161 | **194** | +1 |
| 5 | `{duration > 100ms}` | 206 | 230 | **197** | -9 |
| 6 | `{span.http.method="GET" && resource.service.name="CockroachDB"}` | 2387 | 2400 | **193** | -2194 |
| 7 | `{span.http.method="GET" \|\| span.http.method="POST"}` | 1137 | 1419 | **188** | -949 |
| 8 | `{duration > 500ms && status=error}` | 1616 | 1454 | **195** | -1421 |
| 9 | `{(GET \|\| POST) && status=error}` | 224 | 1573 | **193** | -31 |
| 10 | `{(CockroachDB \|\| tempo) && (error \|\| >1s) && db.system=postgresql}` | 3472 | 3319 | **197** | -3275 |

**Key result:** With warm caches, all queries converge to ~190–200ms regardless of complexity. The selectivity-ordered pruning (NOTE-019) reduces blocks opened, which increases cache hit rates, collapsing the 2–3.5s complex query latency to the same ~200ms as simple indexed queries.


### 1. All spans

```traceql
{}
```

**Result:** `returned=20  inspectedBytes=56.9MB`

---

### 2. GET method spans

```traceql
{span.http.method="GET"}
```

**Result:** `returned=20  inspectedBytes=95.6MB`

---

### 3. CockroachDB service spans

```traceql
{resource.service.name="CockroachDB"}
```

**Result:** `returned=20  inspectedBytes=89.9MB`

---

### 4. Error status spans

```traceql
{status=error}
```

**Result:** `returned=20  inspectedBytes=1.2MB`  
Cheap — status is indexed.

---

### 5. Slow spans (>100ms)

```traceql
{duration > 100ms}
```

**Result:** `returned=20  inspectedBytes=0.8MB`  
Cheap — duration is indexed.

---

### 6. CockroachDB GET spans (AND)

```traceql
{span.http.method="GET" && resource.service.name="CockroachDB"}
```

**Result:** `returned=0  inspectedBytes=1615.9MB`  
CockroachDB spans are DB-layer spans and carry no `http.method` attribute — intersection is empty despite a large scan.

---

### 7. GET or POST spans (OR)

```traceql
{span.http.method="GET" || span.http.method="POST"}
```

**Result:** `returned=20  inspectedBytes=90.3MB`

---

### 8. Slow errors: >500ms AND error

```traceql
{duration > 500ms && status=error}
```

**Result:** `returned=20  inspectedBytes=54.9MB`  
Both predicates are indexed — AND narrows the scan efficiently.

---

### 9. (GET OR POST) AND error — nested OR inside AND

```traceql
{(span.http.method="GET" || span.http.method="POST") && status=error}
```

**Result:** `returned=20  inspectedBytes=1396.7MB`  
The inner OR on a non-indexed span attribute forces a wide scan before the `status=error` filter applies.

---

### 10. Multi-service, multi-condition — fully nested OR and AND

```traceql
{(resource.service.name="CockroachDB" || resource.service.name="tempo") && (status=error || duration > 1s) && span.db.system="postgresql"}
```

**Result:** `returned=0  inspectedBytes=1705.1MB`  
Most expensive query: two OR groups joined by AND, plus an attribute filter. Returns no results because `span.db.system` is not present in this dataset — but scans the most data to prove it.

---

## Metrics Queries

Executed via `GET /tempo/api/metrics/query_range` with `step=60s` over a 3-hour window (180 data points per series).

> **Note:** `avg()`, `max()`, `min()`, `sum()` are not supported in this build.
> Use `histogram_over_time(duration)` for duration distributions.

### Latency Summary

r28 numbers = cold cache. r29 (warm) = after 3 warmup passes. Metrics queries must aggregate over all 180 time steps regardless of result count, so they don't converge to ~200ms like search queries do.

| # | Query | r28 ms | r29 warm | delta |
|---|---|---|---|---|
| 1 | `{} \| rate()` | 1855 | **1765** | -90 |
| 2 | `{} \| count_over_time()` | 1420 | **1489** | +69 |
| 3 | `{span.http.method="GET"} \| rate()` | 3463 | **3863** | +400 |
| 4 | `{resource.service.name="CockroachDB"} \| rate()` | 1962 | **1890** | -72 |
| 5 | `{status=error} \| rate()` | 2087 | **1854** | -233 |
| 6 | `{} \| histogram_over_time(duration)` | 2943 | **4266** | +1323 |
| 7 | `{duration > 100ms} \| rate()` | 1692 | **1792** | +100 |
| 8 | `{GET \|\| POST} \| rate()` | 3530 | **4152** | +622 |
| 9 | `{CockroachDB && duration > 50ms} \| histogram_over_time(duration)` | 2098 | **1684** | -414 |
| 10 | `{(GET \|\| POST) && (error \|\| unset) && CockroachDB} \| rate()` | 3968 | **4053** | +85 |

**Key result (metrics):** Most queries within noise margin of r28. M9 (AND with two indexed predicates) shows the clearest win: -414ms. M6 (histogram over all spans, no pruning possible) regresses +1323ms — investigation warranted.


### 1. Overall span rate

```traceql
{} | rate()
```

**Result:** `series=2  datapoints=181  last_value=0.117 spans/s`

---

### 2. Overall span count per step

```traceql
{} | count_over_time()
```

**Result:** `series=2  datapoints=181  last_value=7 spans/step`

---

### 3. GET request rate

```traceql
{span.http.method="GET"} | rate()
```

**Result:** `series=2  datapoints=181  last_value=0.117 spans/s`

---

### 4. CockroachDB span rate

```traceql
{resource.service.name="CockroachDB"} | rate()
```

**Result:** `series=2  datapoints=181  last_value=4.38 spans/s`  
CockroachDB is the highest-traffic service by span count.

---

### 5. Error span rate

```traceql
{status=error} | rate()
```

**Result:** `series=2  datapoints=181  last_value=0.65 spans/s`

---

### 6. Duration histogram — all spans

```traceql
{} | histogram_over_time(duration)
```

**Result:** `series=36 buckets  datapoints_per_series=180`  
Returns one time series per duration bucket (e.g. 0.12ms, 0.24ms, … up to seconds). Use with Grafana heatmap panel.

---

### 7. Slow spans (>100ms) rate

```traceql
{duration > 100ms} | rate()
```

**Result:** `series=2  datapoints=181  last_value=0.117 spans/s`

---

### 8. GET or POST rate — OR in filter

```traceql
{span.http.method="GET" || span.http.method="POST"} | rate()
```

**Result:** `series=2  datapoints=181  last_value=0.117 spans/s`

---

### 9. CockroachDB slow span duration histogram — AND filter with histogram

```traceql
{resource.service.name="CockroachDB" && duration > 50ms} | histogram_over_time(duration)
```

**Result:** `series=5 buckets  datapoints_per_series=181`  
Narrower bucket spread than query 6 because the `duration > 50ms` pre-filter eliminates short-duration buckets.

---

### 10. (GET OR POST) AND (error OR unset) AND CockroachDB — fully nested rate

```traceql
{(span.http.method="GET" || span.http.method="POST") && (status=error || status=unset) && resource.service.name="CockroachDB"} | rate()
```

**Result:** `series=2  datapoints=181  last_value=0.100 spans/s`  
Most complex metrics query: two OR groups joined with AND across span attribute, status, and resource attribute, then aggregated as a rate over time.

---

## r59 → r60 Optimization Results

**Deployed:** 2026-04-14  
**Image:** `mrdgrafana/tempo:blockpack-f335840e9-r60`  
**Environment:** tempo-dev-test-03 (24h time range, 147–149 blocks, ~7.9 GB block data)

### Optimizations

Two allocation optimizations targeting the `executeTraceMetricsIntrinsic` hot path:

1. **Snappy decode buffer pool** (`internal/modules/blockio/shared/intrinsic_codec.go`):
   `sync.Pool` of `*[]byte` reuses snappy decode buffers across pages. Eliminates ~6.4 GB/query
   of heap allocations in the paged column decode path.

2. **buildGroupKeyMap fast path** (`internal/modules/executor/metrics_trace_intrinsic.go`):
   For single-group-by queries, writes directly to the output map, eliminating the `colVals`
   intermediate `map[uint32]string` allocation (~1.5 GB/query).

### Performance Summary

Time range: 24 hours, step=1h (25 points), tenant `11638`.

| Query | r58 | r59 | r60 (cold) | r60 (warm) | delta (r59→r60 warm) |
|---|---|---|---|---|---|
| `{}⎮histogram_over_time(duration) by (resource.service.name)` | ~22s | 17.5s | 10.4s | **7.2s** | **-59%** |
| `{kind=server}⎮histogram_over_time(duration) by (resource.service.name)` | — | — | — | **3.9s** | — |
| `{}⎮rate() by (resource.service.name)` | — | — | — | **4.5s** | — |

### New Metric Queries (r60 baseline)

These queries use the `by (resource.service.name)` aggregation, the key use case for the
group key map optimization.

#### M11. All-span duration histogram by service

```traceql
{} | histogram_over_time(duration) by (resource.service.name)
```

**Result (r60 warm):** `series=2978  services=210  buckets=39  time=7.2s  blocks=149`

---

#### M12. Server-span duration histogram by service (M8 target query)

```traceql
{kind=server} | histogram_over_time(duration) by (resource.service.name)
```

**Result (r60):** `series=1415  services=146  buckets=30  time=3.9s  blocks=149`

This is the M8 production query (`{span.kind=server}` maps to the `kind` intrinsic column).
The `kind=server` filter halves the series count vs M11 (only server-span buckets) and
cuts query time by ~46% relative to M11 warm.

---

#### M13. All-span rate by service

```traceql
{} | rate() by (resource.service.name)
```

**Result (r60):** `series=210  services=210  time=4.5s  blocks=147`

---

## r61 → r62 Optimization Results

**Deployed:** 2026-04-14  
**Image:** `mrdgrafana/tempo:blockpack-8742c344e-r62`  
**Environment:** tempo-dev-test-03 (24h range, `GOMEMLIMIT=13GiB`, pod limit 18Gi)

### Optimizations

One allocation optimization targeting `streamHistogramGroupBy`:

1. **streamHistogramGroupBy absent-pk guard** (`internal/modules/executor/metrics_trace_intrinsic.go`):
   Skip the O(N) absent-row pass when `len(seen) == len(keyToBucket)`. For universal columns
   like `span:duration` (FLAT format — value present for every span), this pass was always
   redundant and added measurable overhead.

### Context: r61 OOM root cause and fix

r61 pods were OOMKilled because `GOMEMLIMIT` was not set → `objectcache.Cache` budget = 0
(unbounded) → `parsedIntrinsicCache` grew without eviction to 6.8–7 Gi per pod, hitting the
9 Gi limit after 4–6 M8 queries.

**Fix:** `GOMEMLIMIT=13GiB` added to querier deployment env. Budget = 20% of 13 GiB = 2.6 GiB.
Pods now stable at 13–13.5 Gi under sustained load (18 Gi limit, no OOMKill).

### Performance Summary (r62, warm cache, GOMEMLIMIT=13GiB)

| Query | r62 warm | series |
|---|---|---|
| M8: `{kind=server}⎮histogram_over_time(duration) by (resource.service.name)` | **5.15s** (5-run avg) | 1793 |
| M11: `{kind=server}⎮rate() by (resource.service.name)` | **3.45s** (3-run avg) | 178 |
| M13: `{kind=server}⎮count_over_time() by (span.status_code)` | **2.56s** (5-run avg) | 3 |

> **Note:** Absolute numbers are not directly comparable to r60 baselines — the data set grows
> continuously and these benchmarks were taken hours later in the day. Relative ordering (r62
> faster than r61 on M8/M11/M13) is the meaningful comparison; a side-by-side on the same cluster
> in the same session is needed for precise delta measurement.

---

## r62 + Disk Cache Results (2026-04-15)

**Deployed:** 2026-04-15  
**Image:** `mrdgrafana/tempo:blockpack-89035c852-r62` (same code, disk cache config added)  
**Disk cache:** `file_cache_path: /var/tempo/blockpack-cache`, `file_cache_max_bytes: 8589934592` (8 GiB, 80% of 10 Gi emptyDir)  
**Environment:** tempo-dev-test-03, 3 querier replicas, each with independent 8 GiB disk cache

### Disk Cache Metrics (single pod, after warm-up)

| Tier | Hits | Misses | Hit rate | Bytes served |
|------|------|--------|----------|--------------|
| memory (256 MB) | 8,787 | 15,850 | 36% | 299 MB |
| **disk (8 GiB)** | **11,768** | **4,082** | **74%** | **32 GB** |
| remote memcached | 3,158 | 924 | 77% | 30 MB |

### GetTraceByID Latency (vulture-tenant, 24h-8h ago window)

| Pass | Avg latency | Notes |
|------|-------------|-------|
| Cold (S3) | 368ms | Block fetched from S3, written to disk cache |
| Warm pass 1 | 216ms | **−41%** — disk cache hit |
| Warm pass 2 | 214ms | Stable |

### Metrics Queries — 6h window, step=6m (2026-04-15)

**Time range:** last 6 hours, step=360s, tenant `11638`, 113 blocks

| Query | blockpack cold | blockpack warm | ms/block (warm) |
|-------|---------------|----------------|-----------------|
| M8: `{kind=server}⎮histogram_over_time(duration) by (resource.service.name)` | ~11-23s | — | ~151ms |
| M11: `{kind=server}⎮rate() by (resource.service.name)` | ~13s | ~13s | ~116ms |
| M13: `{kind=server}⎮count_over_time() by (span.status_code)` | ~22s | ~12s | ~155ms |

> **Note:** High run-to-run variance (~2x) in 6h benchmarks is due to round-robin load balancing
> across 3 querier replicas with independent disk caches. Each pod individually converges to ~150ms/block
> once warm; the variation reflects different pods being hit per request.

---

## Blockpack vs Parquet Comparison (2026-04-15)

**Method:** Same queries, same 6h window (step=6m), different cells.  
**blockpack:** tempo-dev-test-03, org `11638`, 113 blocks  
**parquet:** tempo-dev-02, org `6121`, 14 blocks  

Org IDs sourced from `deployment_tools/ksonnet/environments/jaeger/mirror.libsonnet`.

### Raw Results (2 passes each, warm cache)

| Query | blockpack avg | parquet avg | blockpack blocks | parquet blocks |
|-------|--------------|-------------|-----------------|----------------|
| M8: histogram_over_time by service | 17,117ms | 6,111ms | 113 | 14 |
| M11: rate() by service | 13,100ms | 1,331ms | 113 | 14 |
| M13: count_over_time() by status_code | 17,555ms | 1,215ms | 113 | 14 |

### Per-Block Normalized

| Query | blockpack ms/block | parquet ms/block | winner |
|-------|--------------------|-----------------|--------|
| M8: histogram_over_time | **151ms** | 437ms | **blockpack 2.9×** |
| M11: rate() | 116ms | **95ms** | parquet 1.2× |
| M13: count_over_time() | 155ms | **87ms** | parquet 1.8× |

**Key result:** Blockpack is significantly faster per block on M8 (histogram over time by service —
the production query). Parquet is faster per block on rate/count aggregations. The raw absolute
numbers favor parquet only because dev-02 had 8× fewer blocks in the 6h window; the per-block
comparison is the meaningful metric for format efficiency.

---

## Post-Compaction Benchmark (2026-04-15, r62)

**Change:** `compaction_window` updated from `5m → 15m` in both backend-scheduler and backend-worker  
**Block counts (total blocklist):** blockpack `11638` = **493 blocks**, parquet `6121` = **865 blocks**  
**Method:** 3 warm-up passes to prime all 3 querier replicas, then benchmark pass. 6h window, step=60s.  
**Note:** Parquet now has _more_ total blocks than blockpack — raw times are a fair comparison.

| Query | TraceQL | bp (ms) | pq (ms) | bp/pq |
|-------|---------|---------|---------|-------|
| M1  | `{} \| rate()` | 397 | 595 | **0.7×** |
| M2  | `{} \| count_over_time()` | 390 | 497 | **0.8×** |
| M3  | `{span.http.method="GET"} \| rate()` | 1701 | 364 | 4.7× |
| M4  | `{resource.service.name="CockroachDB"} \| rate()` | 1037 | 264 | 3.9× |
| M5  | `{status=error} \| rate()` | 1602 | 429 | 3.7× |
| M6  | `{} \| histogram_over_time(duration)` | 1993 | 1239 | 1.6× |
| M7  | `{duration > 100ms} \| rate()` | 1850 | 337 | 5.5× |
| M8  | `{GET\|\|POST} \| rate()` | 2053 | 414 | 5.0× |
| M9  | `{CockroachDB && dur>50ms} \| histogram_over_time(duration)` | 1140 | 495 | 2.3× |
| M10 | `{(GET\|\|POST) && (err\|\|unset) && CockroachDB} \| rate()` | 1386 | 661 | 2.1× |

**Key findings:**
- **M1/M2 (full-scan aggregations):** blockpack is _faster_ than parquet (0.7–0.8×) — the single-I/O-per-block design wins when all spans must be read anyway
- **M6 (histogram over all spans):** blockpack 1.6× slower — expected, reads full block vs parquet column pruning
- **M3/M4/M5/M7/M8 (selective attribute filters):** blockpack 3.7–5.5× slower — parquet's column-selective I/O has a large advantage when the filter column is small relative to the full block
- **M9/M10 (compound filters + histogram):** blockpack 2.1–2.3× slower — predicate pushdown partially closes the gap
- **Root cause of remaining gap:** blockpack reads entire blocks (single-I/O invariant); parquet reads only the queried columns. On selective queries, parquet fetches ~10–50× fewer bytes per block.

---

## r63 Benchmark — V14 Lazy Decompression + Pool Optimization (2026-04-15)

**Image:** `mrdgrafana/tempo:blockpack-7d21f9441-r63`  
**Key changes vs r62:** V14 lazy decompression (non-queried columns not decompressed until first access), snappy decode buffer pool for per-page allocations, objectcache fixes  
**Block counts:** same as above (bp=493, pq=865)  
**Method:** 3 warm-up passes, then 1 measured pass. 6h window, step=60s.

| Query | TraceQL | bp (ms) | pq (ms) | bp/pq | vs r62 bp/pq |
|-------|---------|---------|---------|-------|--------------|
| M1  | `{} \| rate()` | 499 | 530 | **0.9×** | was 0.7× |
| M2  | `{} \| count_over_time()` | 408 | 488 | **0.8×** | was 0.8× |
| M3  | `{span.http.method="GET"} \| rate()` | 1269 | 386 | 3.3× | was 4.7× ↑ |
| M4  | `{resource.service.name="CockroachDB"} \| rate()` | 474 | 329 | 1.4× | was 3.9× ↑↑ |
| M5  | `{status=error} \| rate()` | 788 | 364 | 2.2× | was 3.7× ↑ |
| M6  | `{} \| histogram_over_time(duration)` | 1691 | 1352 | 1.3× | was 1.6× ↑ |
| M7  | `{duration > 100ms} \| rate()` | 786 | 440 | 1.8× | was 5.5× ↑↑ |
| M8  | `{GET\|\|POST} \| rate()` | 1053 | 558 | 1.9× | was 5.0× ↑↑ |
| M9  | `{CockroachDB && dur>50ms} \| histogram_over_time(duration)` | 660 | 499 | 1.3× | was 2.3× ↑ |
| M10 | `{(GET\|\|POST) && (err\|\|unset) && CockroachDB} \| rate()` | 966 | 714 | 1.4× | was 2.1× ↑ |

**Key findings:**
- **All selective queries improved significantly** — lazy decompression means non-queried columns are never decompressed, cutting CPU and memory on selective reads
- **M4 (service name filter):** 3.9× → 1.4× — biggest improvement; service.name is a resource column so only 1 column decompressed instead of all
- **M7/M8 (duration/method filters):** 5.5× → 1.8× and 5.0× → 1.9× — large wins; these columns are small relative to full block payload
- **M5 (status=error):** 3.7× → 2.2× — status is indexed via TOC so gain is partly from skipping column decompression for non-matching blocks
- **M1/M2 (full-scan):** near-parity with parquet (0.8–0.9×) — lazy decompression adds slight overhead when all columns are needed anyway
- **Remaining gap (M3, M5, M6, M7, M8):** blockpack still slower because full blocks are fetched from S3 (single-I/O invariant) vs parquet's per-column fetches; lazy decompression removes the CPU overhead but not the I/O volume
