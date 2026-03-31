# Investigate: Metric Stream High IOPS

**Status:** DONE
**Created:** 2026-02-15
**Priority:** HIGH
**Type:** Investigation/Performance

## Overview

Investigate why metric streams may have high IOPS (Input/Output Operations Per Second) during write operations. While read performance is excellent (1 IOPS vs 337 IOPS for computed queries), there may be inefficiencies in how metric stream blocks are written during ingestion.

## Context

**Current State:**
- Read performance: **Excellent** (1 IOPS, 161 KB read, sub-millisecond latency)
- Write performance: **Unknown/Suspected Issue** (needs profiling)
- Storage target: <5% of file size (alert if >10%)
- High-cardinality risk: >1000 groups may cause issues

**From `PRECOMPUTED_METRIC_STREAMS_REPORT.md`:**
- 337x reduction in I/O ops for reads (1 vs 337)
- 2% storage overhead for 10 streams
- Warning: high-cardinality dimensions (>1000 groups) need review

**From `BINARY_METRIC_STREAM_FORMAT.md:237-240`:**
- Write performance: O(buckets × aggregates) = O(60 × 40) = O(2400)
- Memory: ~600 KB per stream for working data structures
- Binary serialization: Linear in size of output

## Problem Statement

Potential causes of high write IOPS:

1. **Many Small Writes**
   - Each time bucket written separately?
   - Unbuffered writes to disk?
   - No batch flushing strategy?

2. **High Cardinality**
   - >1000 unique group keys per stream
   - Excessive memory → frequent flushes
   - Many small blocks instead of coalesced writes

3. **Fine-Grained Time Buckets**
   - Small step sizes (e.g., 10s vs 60s)
   - More buckets = more write operations
   - Amplification: buckets × streams × cardinality

4. **Inefficient Serialization**
   - Multiple passes over data structures
   - Repeated file seeks
   - Missing write buffering

5. **No Write Coalescing**
   - Each metric stream written independently
   - Could batch multiple streams in single I/O op
   - Missing fsync optimization

## Investigation Tasks

### 1. Profile Write Operations

**Objective**: Measure actual IOPS during metric stream writes

**Actions**:
- [ ] Add I/O tracking to writer (syscall counters)
- [ ] Benchmark write performance with 1, 3, 5, 10 streams
- [ ] Measure IOPS for varying cardinalities (10, 100, 1000, 5000 groups)
- [ ] Profile step sizes: 10s, 30s, 60s, 300s
- [ ] Compare buffered vs unbuffered writes

**Files to instrument**:
- `internal/blockio/writer/writer_metrics.go`
- `internal/blockio/writer/writer.go`
- `internal/blockio/streams/metric_stream.go`

**Expected output**:
- IOPS per stream configuration
- Write amplification factor
- Correlation: cardinality vs IOPS
- Correlation: step size vs IOPS

### 2. Analyze Write Path

**Objective**: Understand current write implementation

**Questions**:
- How are metric stream blocks flushed to disk?
- Are writes buffered or direct?
- Is there write coalescing across streams?
- How many syscalls per metric stream block?
- Are fsyncs batched or per-stream?

**Files to review**:
- `internal/blockio/writer/writer_blocks.go`
- `internal/blockio/writer/writer_helpers.go`
- `internal/blockio/writer/writer.go` (flush logic)
- `internal/blockio/streams/metric_stream.go`

**Deliverable**: Sequence diagram of write path

### 3. Identify Inefficiencies

**Objective**: Find optimization opportunities

**Check for**:
- [ ] Multiple file seeks per stream
- [ ] Small writes (<4KB blocks)
- [ ] Unbuffered writes (missing bufio.Writer)
- [ ] Excessive memory copies
- [ ] No write-ahead buffering
- [ ] Synchronous writes blocking ingestion
- [ ] Missing batch flush logic

**Benchmark**: Current vs optimized write path

### 4. High-Cardinality Scenarios

**Objective**: Quantify impact of high cardinality

**Test cases**:
- 10 groups × 10 streams = 100 time series
- 100 groups × 10 streams = 1,000 time series
- 1,000 groups × 10 streams = 10,000 time series
- 5,000 groups × 10 streams = 50,000 time series

**Metrics**:
- IOPS per scenario
- Memory usage
- Write latency (p50, p95, p99)
- Storage overhead vs span data

**Threshold**: Identify cardinality where IOPS becomes problematic

### 5. Compare with Other Systems

**Objective**: Learn from production systems

**Research**:
- Prometheus write path (TSDB)
- InfluxDB write optimization
- Thanos/Cortex metric ingestion
- VictoriaMetrics batching strategy

**Key patterns to identify**:
- Write-ahead log (WAL)
- Batching and coalescing
- Async flushing
- Compaction triggers

## Potential Optimizations

### Quick Wins (Low Effort)
1. **Buffered Writes**: Use `bufio.Writer` (4KB-32KB buffer)
2. **Batch Fsyncs**: Group multiple stream writes before sync
3. **Threshold-Based Flushing**: Only flush when buffer >X KB

### Medium Effort
4. **Write Coalescing**: Combine multiple metric stream blocks in single write
5. **Async Flush**: Background goroutine for disk writes
6. **Pre-allocation**: Reserve file space upfront (fallocate)

### High Effort
7. **Write-Ahead Log**: Decouple ingestion from disk writes
8. **Streaming Serialization**: Incremental writes vs full buffer
9. **Compression**: Reduce bytes written (LZ4/Snappy)

## Success Criteria

- [ ] IOPS per stream configuration measured and documented
- [ ] Write amplification factor quantified
- [ ] High-cardinality threshold identified (where IOPS becomes problem)
- [ ] Write path inefficiencies catalogued
- [ ] Optimization recommendations prioritized by impact/effort
- [ ] Benchmark comparison: current vs optimized (if fixes implemented)

## Expected Findings

**Hypothesis 1**: Many small writes
- **If true**: Implement buffering + batch fsyncs
- **Impact**: 10-50x reduction in IOPS

**Hypothesis 2**: High cardinality amplification
- **If true**: Implement cardinality limits + warnings
- **Impact**: Prevent pathological cases

**Hypothesis 3**: No write coalescing
- **If true**: Batch multiple streams in single I/O
- **Impact**: 3-10x reduction in IOPS

**Hypothesis 4**: Synchronous writes blocking ingestion
- **If true**: Async flush to background
- **Impact**: Improved ingestion latency

## Related Files

- `internal/blockio/writer/writer_metrics.go` (metric stream write logic)
- `internal/blockio/writer/writer_blocks.go` (block flushing)
- `internal/blockio/streams/metric_stream.go` (stream data structures)
- `benchmark/metric_stream_io_bench_test.go` (existing IOPS benchmark)
- `benchmark/metric_streams_performance_bench_test.go` (write perf test)
- `doc/PRECOMPUTED_METRIC_STREAMS_REPORT.md` (read performance data)
- `doc/BINARY_METRIC_STREAM_FORMAT.md` (format and perf notes)

## Estimated Effort

**Investigation**: 8-12 hours
- Profiling and instrumentation: 4-5 hours
- Write path analysis: 2-3 hours
- High-cardinality testing: 2-3 hours
- Comparison research: 1-2 hours

**Report**: 2-3 hours

**Total**: 10-15 hours (investigation only, not including fixes)

## Deliverables

1. **IOPS Profiling Report**:
   - Measured IOPS for various configurations
   - Write amplification analysis
   - Cardinality impact analysis

2. **Write Path Analysis**:
   - Sequence diagram of current implementation
   - List of identified inefficiencies
   - Syscall trace analysis

3. **Optimization Recommendations**:
   - Prioritized by impact (IOPS reduction)
   - Ranked by effort (LOC, risk, testing)
   - Benchmark projections for each optimization

4. **Implementation Plan** (if optimizations needed):
   - Phase 1: Quick wins (buffering, batch fsyncs)
   - Phase 2: Medium effort (coalescing, async)
   - Phase 3: High effort (WAL, streaming)

## Next Steps

1. Add I/O tracking to writer (instrument syscalls)
2. Run benchmark suite with varying configurations
3. Analyze write path with strace/dtrace
4. Document findings in separate report
5. Create optimization tasks based on findings

---

**Note**: Focus is on **measurement first**. Only proceed with optimizations if IOPS is proven to be a real bottleneck in production scenarios.
