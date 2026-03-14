# Sketch Package — Benchmarks

## BENCH-SK-01: BenchmarkHLLAdd
Benchmark HLL.Add() throughput. Target: < 100 ns/op.
Measures cost of FNV-1a hash + register update per element.

## BENCH-SK-02: BenchmarkCMSAdd
Benchmark CMS.Add(v, 1) throughput. Target: < 200 ns/op.
Measures cost of d=4 independent FNV-32a hashes + 4 counter updates per element.

## BENCH-SK-03: BenchmarkFuse8Contains
Benchmark BinaryFuse8.Contains() throughput. Target: < 50 ns/op.
Measures cost of 3 indexed reads in a ~1000-key filter.

## BENCH-SK-04: BenchmarkFuse8Build
Benchmark NewBinaryFuse8(keys) construction time for 10000 keys. Target: < 5 ms.
Measures peeling algorithm construction cost.
This benchmark is most relevant for understanding block-flush latency.
