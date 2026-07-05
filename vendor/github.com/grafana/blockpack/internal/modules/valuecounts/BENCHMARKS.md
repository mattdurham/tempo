# valuecounts — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/valuecounts` package.
Baseline numbers serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `BENCH-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecountscompactor/BENCHMARKS.md`'s own separate `BENCH-VC-N` sequence).
IDs are assigned in ascending order and never reused or renumbered.

Next free ID: **BENCH-VC-2**.

---

## BENCH-VC-1: BenchmarkEncodeDecodeVCNTFile_RoundTrip
*Added: 2026-07-02*

**Measures:** Combined `EncodeVCNTFile` + `DecodeVCNTFile` cost (the self-describing format,
SPEC-VC-2) over a realistic multi-chunk batch, including chunk-directory encode/decode.

**Setup:** 6,000 records, 500 distinct values (spans multiple chunks, exercises the chunk
directory rather than a single-chunk path).

**Baseline (confirmed stable across repeated runs — B/op and allocs/op exactly reproducible,
ns/op varies with machine load):** 1,596,846 ns/op (~1.6 ms/op), 3,006,971 B/op, 12,032
allocs/op.

**Environment:** Go 1.26.1, linux/amd64, 13th Gen Intel Core i5-13500, `go test -bench <name>
-benchmem -run '^$'`, single machine, un-pinned CPU count — first baseline pending a proper
multi-run/pinned-CPU measurement.

**Regression threshold:** flag if `B/op` or `allocs/op` increases > 15%, or `ns/op` roughly
doubles (single-run baseline, so a smaller `ns/op` delta may just be noise).

Back-ref: `internal/modules/valuecounts/selfdescribing_bench_test.go:BenchmarkEncodeDecodeVCNTFile_RoundTrip`.
