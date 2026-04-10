# Blockpack blockio — Benchmark Specifications

This document defines the benchmark suite for the writer and reader packages. Each benchmark
is specified as a `testing.B` function, with required custom metrics reported via
`b.ReportMetric`. Benchmarks complement the correctness tests in TESTS.md.

For design rationale behind the performance targets see NOTES.md.

> **Implementation status:** Many benchmarks in this document are aspirational targets and
> have not yet been implemented. Entries without a `Back-ref:` are planned targets — they
> define the performance contract for future work. Do not remove them.
>
> Currently implemented: BENCH-W-01 through BENCH-W-05 (writer/writer_bench_test.go),
> BENCH-R-08 (reader/reader_test.go).

> **NOTE (2026-04-10) — V14 format change invalidates prior baselines.** The V14 format
> redesign (per-column outer snappy, sectioned metadata, FooterV5) changes compression
> characteristics, block layout, and I/O patterns significantly. All numeric baseline targets
> in this document were measured against V12/V13 format files. After V14 implementation is
> complete, all benchmarks listed below MUST be re-run against V14 files and the baseline
> numbers updated accordingly. Until then, treat all specific numeric targets as placeholders
> subject to revision. The qualitative targets (e.g. `compress_ratio >= 5.0`, `io_ops == 1`,
> `ms/open < 50`) remain valid design goals; only the measured numbers may shift.

---

## Metric Targets (Never Regress Below)

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| `io_ops` per query | < 500 | 500–1000 | > 1000 |
| `bytes/io` | > 100 KB | 10–100 KB | < 10 KB |
| `io_ops` per block | = 1 | — | > 1 |
| Compression ratio | > 5× | 3–5× | < 3× |

---

## 1. Write Path Benchmarks

### BENCH-W-01: BenchmarkWriterAddSpan

Measures the per-span cost of buffering spans in the Writer (no flush).

**Setup:**
- Construct a `Writer` with `MaxBlockSpans=65535` (so no automatic block flush occurs)
- Prepare a synthetic span with N attributes

**Variants:**
| Sub-benchmark | Span attributes |
|---------------|----------------|
| `_10attrs`    | 10 span attrs  |
| `_50attrs`    | 50 span attrs  |
| `_100attrs`   | 100 span attrs |

**Required custom metrics via `b.ReportMetric`:**
```
b.ReportMetric(float64(b.N)/elapsed.Seconds(), "spans/sec")
b.ReportMetric(float64(bytesWritten)/float64(b.N), "bytes/span")
```

**Acceptance:** `ns/op` must not regress by > 20% between runs.

---

### BENCH-W-02: BenchmarkWriterFlush_SmallBatch

Measures end-to-end flush latency for a small span batch.

**Setup:**
- 100 spans, each with 10 attributes
- `MaxBlockSpans=2000` (default)
- Discard output (write to `io.Discard`)

**Required custom metrics:**
```
b.ReportMetric(float64(bytesWritten), "bytes_written")
b.ReportMetric(float64(uncompressedBytes)/float64(bytesWritten), "compress_ratio")
b.ReportMetric(elapsed.Milliseconds(), "ms/flush")
```

**Acceptance:** `compress_ratio >= 3.0` for typical span data.

---

### BENCH-W-03: BenchmarkWriterFlush_LargeBatch

Measures end-to-end flush latency for a large batch, exercising KLL sketches
and dedicated index construction.

**Setup:**
- 10,000 spans, each with 20 attributes
- `MaxBlockSpans=2000` (default, so ~5 blocks created)
- Discard output

**Required custom metrics:**
```
b.ReportMetric(float64(blockCount), "blocks_written")
b.ReportMetric(float64(uncompressedBytes)/float64(bytesWritten), "compress_ratio")
b.ReportMetric(elapsed.Milliseconds(), "ms/flush")
b.ReportMetric(float64(b.N*10000)/elapsed.Seconds(), "spans/sec")
```

**Acceptance:** `compress_ratio >= 5.0` for a homogeneous span set; `ms/flush < 500`.

---

### BENCH-W-04: BenchmarkWriterFlush_SortKey

Isolates the cost of `sortSpans` and `computeSortKey` for varying batch sizes.

**Variants:** `_100`, `_1000`, `_10000`, `_100000` spans.

**Required custom metrics:**
```
b.ReportMetric(float64(b.N*batchSize)/elapsed.Seconds(), "spans/sec")
b.ReportMetric(elapsed.Nanoseconds()/int64(batchSize), "ns/span")
```

**Acceptance:** Sort must scale as O(n log n).

---

### BENCH-W-05: BenchmarkWriterCurrentSize

Measures the accuracy of `CurrentSize()` after batches of different sizes.

**Purpose:** `CurrentSize()` is called externally to decide when to flush; it must be
fast (O(1)) and accurate to within 2× the actual compressed output.

**Setup:**
- Add 1, 10, 100, 1000, 10000 spans
- Call `CurrentSize()` after each batch

**Required custom metrics:**
```
b.ReportMetric(estimatedBytes, "estimated_bytes")
b.ReportMetric(actualBytes, "actual_bytes_after_flush")
b.ReportMetric(float64(estimatedBytes)/float64(actualBytes), "estimate_ratio")
```

**Acceptance:** `estimate_ratio` in range [0.5, 2.0].

---

## 2. Encoding Benchmarks

All encoding benchmarks use 1000-row synthetic columns and measure both encode and decode
roundtrips separately. They report bytes per encoded output.

### BENCH-E-01: BenchmarkEncodingDictionary
*Status: not yet implemented*

**Setup:** 1000 string values drawn from a 20-entry vocabulary (forces dictionary encoding).

**Sub-benchmarks:** `_Encode`, `_Decode`

**Required custom metrics:**
```
b.ReportMetric(float64(encodedLen), "encoded_bytes")
b.ReportMetric(float64(uncompressedLen)/float64(encodedLen), "compress_ratio")
```

---

### BENCH-E-02: BenchmarkEncodingRLE
*Status: not yet implemented*

**Setup:** 1000 uint64 values with cardinality ≤ 3 (forces RLE index encoding, kind 6).

**Required custom metrics:** same as BENCH-E-01 + `b.ReportMetric(float64(runCount), "rle_runs")`.

---

### BENCH-E-03: BenchmarkEncodingDeltaUint64
*Status: not yet implemented*

**Setup:** 1000 monotonically increasing timestamps with range < 65535 (forces kind 5).

**Required custom metrics:**
```
b.ReportMetric(float64(encodedLen), "encoded_bytes")
b.ReportMetric(float64(deltaWidth), "delta_width_bytes")
```

---

### BENCH-E-04: BenchmarkEncodingXOR
*Status: not yet implemented*

**Setup:** 1000 16-byte span IDs with adjacent IDs sharing the first 12 bytes.

**Required custom metrics:**
```
b.ReportMetric(float64(encodedLen), "encoded_bytes")
b.ReportMetric(float64(16000)/float64(encodedLen), "compress_ratio")  // 16 bytes × 1000 rows
```

---

### BENCH-E-05: BenchmarkEncodingPrefix
*Status: not yet implemented*

**Setup:** 1000 URLs sharing a common 30-character prefix.

**Required custom metrics:**
```
b.ReportMetric(float64(encodedLen), "encoded_bytes")
b.ReportMetric(float64(prefixCount), "prefix_dict_entries")
```

---

### BENCH-E-06: BenchmarkEncodingDeltaDictionary
*Status: not yet implemented*

**Setup:** 1000 16-byte trace IDs, sorted, adjacent entries differing by ≤ 4 bytes.

**Required custom metrics:** same as BENCH-E-04.

---

### BENCH-E-07: BenchmarkEncodingSelection_Uint64Timestamp
*Status: not yet implemented*

Verifies that the encoding selection logic selects `DeltaUint64` (kind 5) for timestamps.

**Setup:** 1000 unix-nanosecond timestamps within a 60-second window.

**Required custom metrics:**
```
b.ReportMetric(float64(encodingKind), "encoding_kind_selected")  // must be 5
b.ReportMetric(float64(b.N)/elapsed.Seconds(), "selections/sec")
```

**Assertion:** `encodingKind == 5` — fail the benchmark if wrong kind is selected.

---

### BENCH-E-08: BenchmarkEncodingSelection_SpanID
*Status: not yet implemented*

Verifies that `isIDColumn("span:id")` selects XOR encoding (kind 8).

**Setup:** 1000 random 8-byte span IDs.

**Required custom metrics:**
```
b.ReportMetric(float64(encodingKind), "encoding_kind_selected")  // must be 8
```

**Assertion:** `encodingKind == 8`.

---

### BENCH-E-09: BenchmarkEncodingSelection_URL
*Status: not yet implemented*

Verifies prefix encoding (kind 10) is selected for URL columns.

**Setup:** 1000 URLs in the form `https://api.example.com/v1/users/N`.

**Required custom metrics:**
```
b.ReportMetric(float64(encodingKind), "encoding_kind_selected")  // must be 10
```

---

### BENCH-E-10: BenchmarkPresenceRLEEncode / BenchmarkPresenceRLEDecode
*Status: not yet implemented*

**Sub-benchmarks:**
- `_Dense` — all 1000 rows present (no nulls)
- `_Half` — 500 random rows present
- `_Sparse` — 50 of 1000 rows present

**Required custom metrics:**
```
b.ReportMetric(float64(encodedLen), "encoded_bytes")
b.ReportMetric(float64(runCount), "rle_runs")
```

---

## 3. Read Path Benchmarks

### BENCH-R-01: BenchmarkReaderParseMetadata
*Status: not yet implemented*

Measures time to open a reader (read footer + header + metadata section).

**Setup:**
- Pre-written file with 1000 blocks, 10 dedicated columns each with 100 values
- All bytes served from an in-memory provider (no I/O latency)

**Required custom metrics:**
```
b.ReportMetric(float64(blockCount), "blocks_indexed")
b.ReportMetric(float64(dedicatedCols), "dedicated_cols")
b.ReportMetric(elapsed.Milliseconds(), "ms/open")
```

**Acceptance:** `ms/open < 50` for 1000-block file.

---

### BENCH-R-02: BenchmarkGetBlockWithBytes_AllColumns
*Status: not yet implemented*

Measures decode time for a single block with all columns requested.

**Setup:**
- Block with 2000 spans and 20 columns of mixed types
- Served from in-memory provider

**Required custom metrics:**
```
b.ReportMetric(1.0, "io_ops")                          // must be exactly 1
b.ReportMetric(float64(blockBytes), "block_bytes")
b.ReportMetric(float64(b.N*2000)/elapsed.Seconds(), "spans/sec_decoded")
b.ReportMetric(elapsed.Milliseconds(), "ms/block")
```

**Assertion:** `io_ops == 1` per block — fail if more than one I/O call is made.

---

### BENCH-R-03: BenchmarkGetBlockWithBytes_FilteredColumns
*Status: not yet implemented*

Verifies no per-column I/O occurs when only 3 of 20 columns are requested.

**Setup:** Same as BENCH-R-02. Request only `["span:name", "span:duration", "service.name"]`.

**Required custom metrics:**
```
b.ReportMetric(1.0, "io_ops")  // MUST be 1 — fail if 3 is observed
b.ReportMetric(float64(blockBytes), "bytes_read")
b.ReportMetric(float64(blockBytes-usefulBytes), "bytes_wasted")
```

**Critical assertion:** `io_ops == 1`. This validates the core I/O invariant from NOTES §1.

---

### BENCH-R-04: BenchmarkGetBlockWithBytes_Reuse
*Status: not yet implemented*

Measures allocation reduction when reusing a `*BlockWithBytes` across iterations.

**Sub-benchmarks:** `_NoReuse`, `_WithReuse`

**Required custom metrics:**
```
b.ReportMetric(float64(allocsPerOp), "allocs/op")
```

**Acceptance:** `WithReuse` must have ≥ 40% fewer `allocs/op` than `NoReuse`.

---

### BENCH-R-05: BenchmarkReaderGetColumn_StringValue
*Status: not yet implemented*

Measures per-value accessor cost for a decoded string column.

**Setup:** 2000-row string column with 50-entry dictionary. Call `StringValue(i)` for all rows.

**Required custom metrics:**
```
b.ReportMetric(float64(b.N*2000)/elapsed.Seconds(), "lookups/sec")
```

---

### BENCH-R-06: BenchmarkLazyDedicatedParse_FirstAccess
*Status: not yet implemented*

Measures first-access parse cost for a dedicated column index.

**Setup:** Dedicated index with 10,000 distinct string values across 1000 blocks.

**Required custom metrics:**
```
b.ReportMetric(elapsed.Microseconds(), "us/first_parse")
b.ReportMetric(float64(valuesIndexed), "values_indexed")
```

---

### BENCH-R-07: BenchmarkLazyDedicatedParse_CachedAccess
*Status: not yet implemented*

Measures repeated-access cost (result already cached).

**Required custom metrics:**
```
b.ReportMetric(elapsed.Nanoseconds(), "ns/cached_access")
```

**Acceptance:** `ns/cached_access < 1000` (must be a simple map lookup).

---

## 4. Block Pruning Benchmarks

### BENCH-P-01: BenchmarkBloomFilterPruning
*Status: not yet implemented*

Measures bloom-based block skipping effectiveness.

**Setup:**
- 1000 blocks with bloom filters populated for 20 column names each
- Query column present in exactly 10% of blocks (100 blocks)

**Required custom metrics:**
```
b.ReportMetric(float64(blocksScanned), "blocks_scanned")
b.ReportMetric(float64(blocksPruned), "blocks_pruned")      // must be ≥ 800
b.ReportMetric(float64(falsePositives), "false_positives")  // bloom FPs
```

---

### BENCH-P-02: BenchmarkDedicatedIndexLookup_Exact
*Status: not yet implemented*

Measures exact-string predicate lookup over the dedicated index.

**Setup:** 10,000 unique string values, each mapped to 1–3 blocks out of 1000 total.

**Required custom metrics:**
```
b.ReportMetric(elapsed.Nanoseconds(), "ns/lookup")
b.ReportMetric(float64(blocksReturned), "blocks_returned")
```

---

### BENCH-P-03: BenchmarkDedicatedIndexLookup_Range
*Status: not yet implemented*

Measures range-predicate lookup over a KLL-bucketed numeric column.

**Setup:** `span:duration` column with KLL boundaries across 1000 blocks. Query
duration range covering approximately 20% of all spans.

**Required custom metrics:**
```
b.ReportMetric(elapsed.Nanoseconds(), "ns/lookup")
b.ReportMetric(float64(blocksReturned), "blocks_returned")
b.ReportMetric(float64(bucketsScan), "buckets_scanned")
```

---

### BENCH-P-04: BenchmarkTimestampRangePruning
*Status: not yet implemented*

Measures how many blocks are skipped by `MinStart/MaxStart` range checks.

**Setup:**
- 1000 blocks with non-overlapping 1-hour time windows
- Query covering 5% of the total time range (50 blocks)

**Required custom metrics:**
```
b.ReportMetric(float64(blocksScanned), "blocks_scanned")  // must be ~50
b.ReportMetric(float64(blocksPruned), "blocks_pruned")    // must be ~950
```

---

### BENCH-P-05: BenchmarkTraceIDBloomLookup

Measures the throughput of `BlocksForTraceIDCompact` for bloom-hit (trace present)
and bloom-miss (trace absent) paths, establishing a regression baseline for the O(k)
bloom check fast path.

**Setup:**
- Write a file with 5000 unique trace IDs across multiple blocks; flush.
- Open with `NewLeanReaderFromProvider` (compact index parsed eagerly).
- Prepare two lookup ID sets: 1000 present IDs and 1000 absent IDs.

**Metrics:**
```
b.ReportMetric(float64(ops), "lookups/op")
b.ReportMetric(float64(hits)/float64(ops)*100, "hit_pct")
```

**Baseline targets (to be measured; update after first run):**
- Bloom-miss path: < 200 ns/op (k=7 hash computations on random bytes, no map access).
- Bloom-hit path: < 500 ns/op (k=7 checks + hash map lookup).

---

## 5. Coalescing Benchmarks

### BENCH-C-01: BenchmarkCoalesceBlocks_Adjacent
*Status: not yet implemented*

All blocks lay adjacent in the file (gap = 0).

**Setup:** 100 blocks each of 512 KB, no gaps.

**Required custom metrics:**
```
b.ReportMetric(float64(coalescedReads), "coalesced_reads")  // should be 1
b.ReportMetric(float64(100), "individual_reads_saved")
```

---

### BENCH-C-02: BenchmarkCoalesceBlocks_SmallGaps
*Status: not yet implemented*

Blocks separated by 1 KB gaps (well within 4 MB `AggressiveCoalesceConfig` threshold).

**Setup:** 100 blocks of 512 KB with 1 KB gaps.

**Required custom metrics:**
```
b.ReportMetric(float64(coalescedReads), "coalesced_reads")  // should be 1
b.ReportMetric(float64(wastedBytes), "wasted_bytes")
```

---

### BENCH-C-03: BenchmarkCoalesceBlocks_LargeGaps
*Status: not yet implemented*

Blocks separated by 10 MB gaps (exceeds `AggressiveCoalesceConfig.MaxGapBytes = 4 MB`).

**Required custom metrics:**
```
b.ReportMetric(float64(coalescedReads), "coalesced_reads")  // should equal block count
```

---

### BENCH-C-04: BenchmarkReadCoalescedBlocks_Merged_vs_Individual
*Status: not yet implemented*

Compares latency of 1 merged read vs 10 individual reads using a simulated-latency provider.

**Setup:** `NewDefaultProviderWithLatency(storage, 15*time.Millisecond)` (simulates S3 latency).

**Sub-benchmarks:** `_Merged`, `_Individual`

**Required custom metrics:**
```
b.ReportMetric(elapsed.Milliseconds(), "ms/batch")
b.ReportMetric(float64(ioOps), "io_ops")
```

**Acceptance:** `_Merged/ms_batch * 10 < _Individual/ms_batch` (merged should be ~10× faster).

---

## 6. I/O Metrics Validation Benchmarks

### BENCH-IO-01: BenchmarkIOOpsPerBlock
*Status: not yet implemented*

Validates the single-I/O-per-block invariant from NOTES §1 using `TrackingReaderProvider`.

**Setup:**
- Write a 100-column, 2000-span file
- Read every block requesting 5 specific columns
- Use `TrackingReaderProvider` to count actual `ReadAt` calls

**Required custom metrics:**
```
b.ReportMetric(float64(tracker.IOOps())/float64(blockCount), "io_ops/block")  // MUST = 1.0
b.ReportMetric(float64(tracker.BytesRead())/float64(ioOps), "bytes/io")
```

**Critical assertion:** `io_ops/block == 1.0`. Any value > 1.0 indicates a regression.

---

### BENCH-IO-02: BenchmarkIOOpsPerQuery
*Status: not yet implemented*

Measures total I/O operations for a full query across all blocks.

**Setup:**
- 50-block file
- Query requesting 3 of 30 columns, all blocks must be scanned (no bloom pruning)

**Required custom metrics:**
```
b.ReportMetric(float64(ioOps), "io_ops")              // must equal block count (50)
b.ReportMetric(float64(bytesRead)/float64(ioOps), "bytes/io")
```

---

### BENCH-IO-03: BenchmarkProviderStackOverhead
*Status: not yet implemented*

Measures overhead added by the DefaultProvider wrapper chain vs a bare provider.

**Sub-benchmarks:** `_BareProvider`, `_DefaultProvider`

**Required custom metrics:**
```
b.ReportMetric(elapsed.Nanoseconds()/int64(b.N), "ns/readat")
```

**Acceptance:** DefaultProvider overhead < 5% over bare provider for reads ≥ 100 KB.

---

## 7. KLL Sketch Benchmarks

### BENCH-K-01: BenchmarkKLLAdd_Uint64
*Status: not yet implemented*

Measures cost of adding values to a `KLL[uint64]` sketch.

**Required custom metrics:**
```
b.ReportMetric(float64(b.N)/elapsed.Seconds(), "values/sec")
```

---

### BENCH-K-02: BenchmarkKLLBoundaries_Uint64
*Status: not yet implemented*

Measures cost of computing bucket boundaries from a populated sketch.

**Setup:** 100,000 values added, compute 100 bucket boundaries.

**Required custom metrics:**
```
b.ReportMetric(elapsed.Microseconds(), "us/boundaries")
b.ReportMetric(float64(nBuckets), "buckets")
```

---

### BENCH-K-03: BenchmarkKLLBytes_Boundaries
*Status: not yet implemented*

Measures boundary computation for the bytes/string KLL (reservoir-based).

**Setup:** 100,000 32-byte values, compute 100 boundaries.

**Required custom metrics:**
```
b.ReportMetric(elapsed.Microseconds(), "us/boundaries")
```

---

## 8. Round-Trip Benchmarks

### BENCH-RT-01: BenchmarkRoundTrip_SmallFile
*Status: not yet implemented*

Full write + read cycle for a small file.

**Setup:** 500 spans, 15 columns each. Write to `bytes.Buffer`, then read back.

**Required custom metrics:**
```
b.ReportMetric(float64(bytesWritten), "file_bytes")
b.ReportMetric(elapsed.Milliseconds(), "ms/roundtrip")
b.ReportMetric(float64(spansRead), "spans_read")
```

---

### BENCH-RT-02: BenchmarkRoundTrip_LargeFile
*Status: not yet implemented*

Full write + read cycle for a large file.

**Setup:** 50,000 spans, 25 columns each (multiple blocks). Write + read all blocks.

**Required custom metrics:**
```
b.ReportMetric(float64(bytesWritten), "file_bytes")
b.ReportMetric(float64(blockCount), "blocks")
b.ReportMetric(float64(uncompressedBytes)/float64(bytesWritten), "compress_ratio")
b.ReportMetric(elapsed.Milliseconds(), "ms/roundtrip")
```

---

## 9. Metadata Compression Benchmarks

### BENCH-R-08: BenchmarkReaderOpen_V12

Measures reader open latency (footer + header + metadata parse, including snappy
decompression) for a V12 file with 1000 spans across 10 blocks.

**Setup:**
- Write 1000 spans with `MaxBlockSpans=100` to produce 10 blocks.
- Write to in-memory buffer (no I/O latency).
- Benchmark calls `reader.NewReaderFromProvider` in a loop.

**Required custom metrics via `b.ReportMetric`:**
```go
b.ReportMetric(float64(len(data)), "file_bytes")
```

**Acceptance:**
- Open time `ms/open < 10` for a 10-block file (snappy decompression at ~1.5 GB/s is
  negligible for metadata sizes up to 10 MB).
- No allocation regression compared to V11 open time.

**Implementation:** `BenchmarkReaderOpen_V12` in `reader/reader_test.go`.

---

## Benchmark File Location and Conventions

- Benchmarks live in `internal/modules/blockio/<package>/<name>_bench_test.go`
- All benchmarks use `testing.B` and must call `b.ReportAllocs()` to track allocations
- Benchmarks that assert correctness (like `io_ops == 1`) must call `b.Fatal()` on failure
- Use `b.RunParallel` for benchmarks that measure concurrent throughput
- Benchmarks must reset the timer (`b.ResetTimer()`) after setup

```go
func BenchmarkWriterFlush_LargeBatch(b *testing.B) {
    b.ReportAllocs()
    // ... setup ...
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // ... benchmark body ...
    }
    b.StopTimer()
    b.ReportMetric(float64(blockCount), "blocks_written")
    // ...
}
```
