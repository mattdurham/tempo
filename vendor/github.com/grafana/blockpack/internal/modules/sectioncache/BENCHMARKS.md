# BENCHMARKS: sectioncache

## BENCH-SC-001: BlockColumnsKeyFast vs BlockColumnsKey

**Target:** `BlockColumnsKeyFast` should be faster than `BlockColumnsKey` for small
block indices (strconv.Itoa vs fmt.Sprintf overhead).

No baseline established yet. Add benchmarks if block column cache call latency becomes
a regression concern.

## BENCH-SC-002: NopSectionCache Overhead

**Target:** All NopSectionCache method calls should be < 5 ns/op (no allocation,
no I/O, function just calls fetch or returns zero values).

No baseline established yet.
