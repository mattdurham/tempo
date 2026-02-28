# executor — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/executor` package.
All benchmarks pre-build the reader outside the timed loop.

---

## Metric Targets (Never Regress Below)

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| Execute latency (100 spans, 1 block, match-all) | < 500 µs | 500 µs–2 ms | > 2 ms |
| Execute latency (1000 spans, 10 blocks, match-all) | < 5 ms | 5–20 ms | > 20 ms |
| Matches/sec (match-all, in-memory) | > 500K | 100K–500K | < 100K |

---

## BENCH-EX-01: BenchmarkExecute_MatchAll

Measures end-to-end Execute cost for `{}` against an in-memory file.

**Setup (outside timed loop):**
- Write N spans with `MaxBlockSpans=100`.
- Open reader with in-memory provider.
- Compile `{}` program once.

**Variants:**

| Sub-benchmark | Spans | Blocks |
|---------------|-------|--------|
| `_100spans`   | 100   | 1      |
| `_1000spans`  | 1000  | 10     |

**Required custom metrics:**
```go
b.ReportMetric(float64(len(result.Matches)), "matches")
b.ReportMetric(float64(b.N*len(result.Matches))/elapsed.Seconds(), "matches/sec")
```

---

## BENCH-EX-02: BenchmarkExecute_ServiceFilter

Measures Execute cost for a service-name equality filter that selects ~10% of spans.

**Setup (outside timed loop):**
- Write 1000 spans across 10 service names (100 each), `MaxBlockSpans=100`.
- Compile `{ resource.service.name = "svc-0" }` program.

**Required custom metrics:**
```go
b.ReportMetric(float64(result.BlocksScanned), "blocks_scanned")
b.ReportMetric(float64(result.BytesRead)/float64(b.N), "bytes_read/op")
```

---

## BENCH-EX-03: BenchmarkExecute_Limit

Measures the early-exit benefit of `Options.Limit` for large result sets.

**Setup (outside timed loop):**
- Write 10000 spans, `MaxBlockSpans=100`.
- Compile `{}` program.

**Variants:**

| Sub-benchmark | Limit |
|---------------|-------|
| `_limit1`     | 1     |
| `_limit10`    | 10    |
| `_nolimit`    | 0     |

**Required custom metrics:**
```go
b.ReportMetric(float64(result.BlocksScanned), "blocks_scanned")
b.ReportMetric(float64(len(result.Matches)), "matches")
```
