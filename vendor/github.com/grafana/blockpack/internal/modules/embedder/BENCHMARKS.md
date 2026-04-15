# embedder — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/embedder` package.
Benchmarks reflect the parallelization, retry logic, and batch chunking introduced in
recent performance commits (backend_http.go: concurrency, chunking, retry).

> **Implementation status:** All three benchmark functions are implemented in
> `embedder/embedder_bench_test.go`.

---

## BENCH-EMBED-001: EmbedBatch Throughput (Varying Batch Sizes)

**Target:** EmbedBatch throughput scales with batch size; 128-text batch must achieve >= 4×
throughput vs. 1-text batch at the same server concurrency.

**Setup:** Test HTTP server returning fixed-size embeddings; measure texts/sec across batch sizes.

**Variants:**
| Sub-benchmark | Batch size |
|---------------|-----------|
| `_1`          | 1 text    |
| `_32`         | 32 texts  |
| `_128`        | 128 texts |

**Required custom metrics:**
```
b.ReportMetric(float64(totalTexts)/elapsed.Seconds(), "texts/sec")
b.ReportMetric(float64(totalBytes)/elapsed.Seconds()/1024/1024, "MB/sec")
```

**Implementation:** `BenchmarkEmbedBatchThroughput` in `embedder/embedder_bench_test.go`.

---

## BENCH-EMBED-002: Concurrent Batch Throughput (maxConcurrentBatches)

**Target:** Throughput scales near-linearly from maxConcurrentBatches=1 to 4; diminishing
returns beyond 4 (matching the default reduced from 8 to 4 in recent commit).

**Setup:** Test HTTP server with 10 ms simulated latency; fixed 32-text batch; vary concurrency.

**Variants:**
| Sub-benchmark | maxConcurrentBatches |
|---------------|---------------------|
| `_1`          | 1                   |
| `_4`          | 4                   |
| `_8`          | 8                   |

**Implementation:** `BenchmarkEmbedConcurrentBatches` in `embedder/embedder_bench_test.go`.

---

## BENCH-EMBED-003: Retry Overhead for Transient Errors

**Target:** Two transient 503 retries add < 2× latency vs. a clean request at the same
batch size (exponential backoff must not dominate throughput under low error rates).
Note: 4xx responses are not retried; `_4xx_fail` measures the immediate-failure path.

**Setup:** Test HTTP server with an atomic call counter; the first two requests return the
error code, the third and beyond return 200. For `_4xx_fail`, sendBatch breaks immediately
on the first 4xx and returns an error (no retry). For `_503`, sendBatch retries on 5xx,
so the first two requests fail and the third succeeds.

**Variants:**
| Sub-benchmark | Error code | Behavior                                         |
|---------------|-----------|--------------------------------------------------|
| `_4xx_fail`   | 429       | Immediate failure after one request (no retry)   |
| `_503`        | 503       | Two failures then success (sendBatch retries 5xx)|

**Implementation:** `BenchmarkEmbedRetryOverhead` in `embedder/embedder_bench_test.go`.
