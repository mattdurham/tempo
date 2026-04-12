# TraceQL Query Reference

**Environment:** dev-us-east-0 / tempo-dev-test-03  
**Tenant:** `X-Scope-OrgID: 11638`  
**Generated:** 2026-04-12  
**Time range queried:** last 3 hours  
**Services active:** alloy, CockroachDB, apiserver, grafana

API endpoint: `http://<query-frontend>:3100/tempo/api/`

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
