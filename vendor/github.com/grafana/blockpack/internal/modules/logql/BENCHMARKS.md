# logql — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/logql` package.

> **Implementation status:** No benchmarks are implemented yet. All entries below are planned
> targets — they define the performance contract for future work. Do not remove them.

---

## BENCH-LQL-001: Pipeline Execution Throughput

**Target:** LogQL pipeline processes >= 500 K log entries/sec for a 3-stage pipeline (label
filter + JSON parsing + value filter) on representative log lines.

**Setup:** Build a 3-stage pipeline; feed N log entries in a loop; measure throughput.

**Variants:**
| Sub-benchmark | Log line size | Pipeline stages |
|---------------|--------------|----------------|
| `_100B_3stage` | 100 B       | 3              |
| `_1KB_3stage`  | 1 KB        | 3              |
| `_1KB_5stage`  | 1 KB        | 5              |

**Implementation:** Not yet implemented — `BenchmarkLogQLPipeline` in `logql/logql_bench_test.go`.

---

## BENCH-LQL-002: Label Map Allocation

**Target:** Label map allocation and reuse overhead < 100 ns/entry when a pool is used.

**Setup:** Measure allocation cost per log entry with and without map reuse.

**Implementation:** Not yet implemented — `BenchmarkLogQLLabelAlloc` in `logql/logql_bench_test.go`.

---

## BENCH-LQL-003: Predicate Evaluation Latency

**Target:** A single-predicate label filter evaluates in < 50 ns per log entry.

**Setup:** Single-stage label filter pipeline; fixed log line; measure per-entry latency.

**Implementation:** Not yet implemented — `BenchmarkLogQLPredicate` in `logql/logql_bench_test.go`.
