# Blockpack blockio — Test Specifications

This document defines the required tests for the writer and reader packages. Each test is
described with its scenario, preconditions, steps, and expected outcomes. Tests are grouped
by concern. All round-trip tests write with the writer then read back with the reader.

---

## 1. Round-Trip Tests

### RT-01: Basic round-trip — single block

**Scenario:** Write a small set of spans that fits in one block and read them back.

**Setup:**
- Create a Writer with default config and an in-memory buffer.
- Add 10 spans with varied service names, span names, and attributes.
- Call `Flush`.

**Assertions:**
- Reader opens successfully from the buffer.
- `reader.BlockCount()` == 1.
- For each written span, a corresponding span exists in the read block with identical
  values for all intrinsic fields and all attributes.
- `block.SpanCount()` == 10.

---

### RT-02: Basic round-trip — multiple blocks

**Scenario:** Force block boundaries and verify data integrity across blocks.

**Setup:**
- Create a Writer with `MaxBlockSpans = 5`.
- Add 13 spans.
- Call `Flush`.

**Assertions:**
- Reader has 3 blocks (ceil(13/5) = 3).
- Total span count across all blocks == 13.
- No span data is duplicated or missing across blocks.

---

### RT-03: All column types

**Scenario:** Verify each of the 6 primitive column types round-trips correctly.

**Setup:**
- Write one span with custom attributes of each type:
  - String: `"custom.str" = "hello"`
  - Int64: `"custom.int" = -42`
  - Uint64: `"custom.uint" = 12345678901234`
  - Float64: `"custom.float" = 3.14159`
  - Bool: `"custom.bool" = true`
  - Bytes: `"custom.bytes" = []byte{0x01, 0x02, 0x03}`
- Flush and read back.

**Assertions:**
- Each column is present in the block.
- Each value matches exactly (exact float64 bit equality for float).
- Presence bits are set for all rows.

---

### RT-04: Null / absent values

**Scenario:** Columns with missing values for some spans round-trip correctly.

**Setup:**
- Write 5 spans. Only spans 0, 2, 4 have attribute `"sparse.attr" = "value"`.
- Spans 1 and 3 do not have this attribute.
- Flush and read back.

**Assertions:**
- Column `"sparse.attr"` is present in the block.
- `col.StringValue(0)` returns `("value", true)`.
- `col.StringValue(1)` returns `("", false)`.
- `col.StringValue(2)` returns `("value", true)`.
- `col.StringValue(3)` returns `("", false)`.
- `col.StringValue(4)` returns `("value", true)`.

---

### RT-05: All-null column

**Scenario:** A column where no span has a value is still written and read back.

**Setup:**
- Write 3 spans. None have attribute `"absent.attr"`.
- Flush and read back (request `"absent.attr"` column explicitly).

**Assertions:**
- Column `"absent.attr"` is NOT present in the block (never written, so not encoded).
- `block.GetColumn("absent.attr")` returns `nil`.

---

### RT-06: Intrinsic fields round-trip

**Scenario:** OTLP intrinsic fields (span:id, trace:id, span:name, timestamps, etc.)
round-trip correctly.

**Setup:**
- Write one span with all OTLP intrinsic fields populated:
  - TraceId, SpanId, ParentSpanId set to known 16/8-byte values.
  - Name = `"my.operation"`.
  - StartTimeUnixNano = 1_000_000_000.
  - EndTimeUnixNano = 2_000_000_000.
  - Kind = SPAN_KIND_SERVER.
  - Status.Code = STATUS_CODE_OK, Status.Message = `"all good"`.
- Flush and read back.

**Assertions:**
- `trace:id` column returns the 16-byte TraceId.
- `span:id` column returns the 8-byte SpanId.
- `span:parent_id` column returns the 8-byte ParentSpanId.
- `span:name` column returns `"my.operation"`.
- `span:start` column returns 1_000_000_000.
- `span:end` column returns 2_000_000_000.
- `span:duration` column returns 1_000_000_000 (end - start).
- `span:kind` returns the numeric kind value.
- `span:status` returns the numeric status code.
- `span:status_message` returns `"all good"`.

---

## 2. Encoding-Specific Tests

### ENC-01: Dictionary encoding — string column

**Scenario:** A string column with low cardinality uses dictionary encoding.

**Setup:**
- Write 20 spans all with `"http.method"` cycling through `["GET", "POST", "DELETE"]`.
- Flush and read back.

**Assertions:**
- Column `"http.method"` is present and all 20 values match.
- Internal encoding kind is dictionary (verify via `WriterExportedConstants`).

---

### ENC-02: Delta encoding — timestamp column

**Scenario:** `span:start` uses delta uint64 encoding when timestamps are close together.

**Setup:**
- Write 10 spans with start times 1_000_000_000 through 1_000_000_009 (10 ns apart).
- Flush and read back.

**Assertions:**
- All 10 `span:start` values match exactly.
- When inspecting raw column bytes, encoding_kind byte == 5 (DeltaUint64).

---

### ENC-03: XOR encoding — span:id column

**Scenario:** `span:id` uses XOR encoding.

**Setup:**
- Write 5 spans with distinct 8-byte SpanIds sharing the same first 6 bytes.
- Flush and read back.

**Assertions:**
- All span IDs round-trip correctly.
- Column encoding kind is XOR (kind 8 or 9).

---

### ENC-04: Prefix encoding — URL column

**Scenario:** A column named `"http.url"` uses prefix compression.

**Setup:**
- Write 10 spans with `"http.url"` values all starting with `"https://api.example.com/"`,
  varying only in the path segment.
- Flush and read back.

**Assertions:**
- All URL values match exactly.
- Column encoding kind is Prefix (kind 10 or 11).

---

### ENC-05: RLE encoding — low cardinality

**Scenario:** A column with a single repeated value uses RLE index encoding.

**Setup:**
- Write 100 spans all with `"deployment.env" = "production"`.
- Flush and read back.

**Assertions:**
- All 100 values are `"production"`.
- Encoding kind is RLE (kind 6 or 7).

---

### ENC-06: Sparse encoding — >50% nulls

**Scenario:** A column with majority null rows uses sparse encoding.

**Setup:**
- Write 10 spans. Only span 2 has `"rare.attr" = "present"`.
- Flush and read back.

**Assertions:**
- `col.StringValue(2)` returns `("present", true)`.
- All other rows return `("", false)`.
- Encoding kind is a sparse variant (even number in 2/4/7/9/11/13).

---

### ENC-07: Delta dictionary — trace:id column

**Scenario:** `trace:id` column uses DeltaDictionary encoding.

**Setup:**
- Write 10 spans from 3 different traces (4+4+2 span distribution).
- Flush and read back.

**Assertions:**
- `trace:id` values round-trip to exact 16-byte IDs.
- Encoding kind is DeltaDictionary (kind 12 or 13).

---

### ENC-08: Inline bytes encoding — array column

**Scenario:** Array columns (event:name, link:trace_id) use dictionary or inline encoding.

**Setup:**
- Write 5 spans each with 2 events named `"exception"` and `"log"`.
- Flush and read back.

**Assertions:**
- `event:name` column values decode to the expected array byte blobs.
- No panic or encoding error.

---

### ENC-09: UUID auto-detection

**Scenario:** A string column whose values are all UUIDs is stored as 16-byte Bytes.

**Setup:**
- Write 5 spans with `"request.id" = "550e8400-e29b-41d4-a716-446655440000"` (valid UUID).
- Flush and read back.

**Assertions:**
- Column `"request.id"` is stored as ColumnTypeBytes (not String).
- The value round-trips and is equal to the 16-byte binary form of the UUID.

---

## 3. Block Index and Metadata Tests

### META-01: Block index timestamps

**Scenario:** `blockIndexEntry.MinStart` and `MaxStart` reflect actual span start times.

**Setup:**
- Write 5 spans with start times: 100, 300, 200, 500, 400 (nanoseconds).
- Flush and read back.

**Assertions:**
- `reader.BlockMeta(0).MinStart` == 100.
- `reader.BlockMeta(0).MaxStart` == 500.

---

### META-02: Block index trace ID range

**Scenario:** `MinTraceID` and `MaxTraceID` reflect the lexicographic range of trace IDs.

**Setup:**
- Write 3 spans with known trace IDs (can be crafted as byte arrays).
- Flush and read back.

**Assertions:**
- `blockMeta.MinTraceID` equals the lexicographically smallest trace ID.
- `blockMeta.MaxTraceID` equals the lexicographically largest trace ID.

---

### META-03: *(Removed)* Column-name bloom filter — 2026-03-07

`TestBlockMeta_ColumnNameBloom` has been removed. `ColumnNameBloom` no longer exists in
`BlockMeta`. The CMS equivalent is covered by sketch index tests (see queryplanner TESTS.md
§QP-T-17/QP-T-18).

---

### META-04: Column stats min/max

**Scenario:** Column stats (min/max) are written and read back correctly.

**Setup:**
- Write 5 spans with `"latency"` (int64): values [-5, 0, 3, -100, 50].
- Flush and read back.

**Assertions:**
- `col.Stats.IntMin` == -100.
- `col.Stats.IntMax` == 50.
- `col.Stats.HasValues` == true.

---

### META-05: Dedicated column index — string predicate

**Scenario:** Dedicated index allows finding blocks by attribute value.

**Setup:**
- Write spans in 2 flush batches (2 files or use compaction if needed). In block 0:
  spans with `"http.method" = "GET"`. In block 1: spans with `"http.method" = "POST"`.
- Query `BlocksForRange("http.method", StringValueKey("GET"), arena)`.

**Assertions:**
- Returns only the block index for block 0.
- Does not return block 1.

---

### META-06: Dedicated column index — numeric predicate

**Scenario:** Dedicated range-bucketed index handles numeric column.

**Setup:**
- Write spans with `"duration_ms"` (RangeUint64 or Uint64) ranging from 1–1000.
- Flush and read back.

**Assertions:**
- Bucket metadata (boundaries) is present for the column.
- `BlocksForRange` with a bucket value returns the correct block set.

---

### META-07: BlocksForRangeInterval — string interval lookup

**Scenario:** `BlocksForRangeInterval` returns blocks from all buckets whose lower boundary
falls within [min, max]. NOTE-011: interval matching for case-insensitive regex prefix lookups.

**Setup:**
- Write 3 blocks with distinct `service.name` values: `"alpha-svc"`, `"beta-svc"`, `"gamma-svc"`.
- 5 spans per block (to force block boundaries).

**Assertions:**
- Interval `["alpha", "beta\xff"]` returns blocks for alpha-svc and beta-svc, not gamma-svc.
- Interval `["a", "z"]` returns at least as many blocks as the narrower interval.
- Interval `["000", "111"]` (below all boundaries) returns empty.

Back-ref: `internal/modules/blockio/reader/reader_test.go:TestBlocksForRangeInterval_StringColumn`

---

## 4. Trace Block Index Tests

### TBI-01: Single trace across one block

**Scenario:** A trace with multiple spans that all fit in one block.

**Setup:**
- Write 3 spans all sharing the same trace ID.
- Flush.

**Assertions:**
- `reader.BlocksForTraceID(traceID)` returns `[0]`.
- `reader.TraceEntries(traceID)` returns one entry with `BlockID == 0`.

---

### TBI-02: Single trace split across blocks

**Scenario:** A trace with many spans that span multiple blocks.

**Setup:**
- Set `MaxBlockSpans = 3`.
- Write 6 spans all sharing the same trace ID.
- Flush.

**Assertions:**
- `reader.BlocksForTraceID(traceID)` returns `[0, 1]`.
- `reader.TraceEntries(traceID)` returns two entries, one per block.

---

### TBI-03: Multiple traces in one block

**Scenario:** Multiple distinct trace IDs in the same block are all indexed.

**Setup:**
- Write 6 spans: 2 each from 3 different trace IDs.
- Flush.

**Assertions:**
- `reader.BlocksForTraceID(traceID1)` returns `[0]`.
- `reader.BlocksForTraceID(traceID2)` returns `[0]`.
- `reader.BlocksForTraceID(traceID3)` returns `[0]`.
- Unknown trace ID returns empty slice.

---

### TBI-04: v2 format round-trip via GetTraceByID

**Scenario:** Writing a file (v2 trace index) and calling `GetTraceByID` returns all spans.

**Setup:**
- Write N spans sharing one trace ID across one or more blocks.
- Flush.
- Open reader and call `GetTraceByID`.

**Assertions:**
- All N spans are returned (verified by span ID set equality).
- No spans from other traces are returned.
- The in-block scan correctly skips rows where `trace:id` does not match.

---

### TBI-05: v1 backward compatibility — old file readable with new reader

**Scenario:** A file written by a v1 writer (with span indices in trace block index) is
opened by the current reader and `GetTraceByID` returns correct spans.

**Setup:**
- Construct a v1-format trace block index byte slice manually (fmt_version=0x01,
  with span_count and span_indices per block ref).
- Parse it with `parseTraceBlockIndex`.

**Assertions:**
- `parseTraceBlockIndex` succeeds.
- The returned map contains the correct block IDs (span indices are discarded).
- `BlocksForTraceID` returns the correct block indices.

---

### TBI-06: GetTraceByID with absent trace:id column is skipped gracefully

**Scenario:** A block that lacks the `trace:id` column does not panic or error.

**Setup:**
- Construct or mock a `BlockWithBytes` whose decoded `Block` has no `trace:id` column.
- Call the `GetTraceByID` path (or a unit covering the nil-column branch).

**Assertions:**
- No panic; no error returned.
- The block is skipped (zero spans emitted for that block).

---

## 5. Compact Trace Index Tests

### CTI-01: Compact index is written in v3 footer files

**Scenario:** Verify compact trace index is present and parseable.

**Setup:**
- Write spans and Flush with a writer that produces v3 footer.

**Assertions:**
- Footer `compact_len > 0`.
- `parseCompactTraceIndex` returns valid block table and trace entries.
- Magic == 0xC01DC1DE, version == 2 (current writer always emits version 2 with bloom).
- `bloom_bytes > 0` and `bloom_data` is non-nil.

Note: Version 1 (no bloom) is the legacy format; new files always use version 2.

---

### CTI-02: Compact index matches trace block index

**Scenario:** Compact index and main metadata trace block index agree.

**Setup:**
- Write 10 spans from 5 traces and Flush.

**Assertions:**
- For every trace ID, `BlocksForTraceID` returns the same block IDs whether using the
  compact index or the main metadata trace block index.
- `TraceEntries` via full reader and lean reader return the same set of block IDs per trace.

---

### TIDBLOOM-01: Present trace IDs always pass bloom check

**Scenario:** All trace IDs written to a file must be found via `BlocksForTraceID` after
opening with `NewLeanReaderFromProvider` (which uses the compact index with bloom).

**Setup:**
- Write 50 spans with distinct trace IDs and Flush.
- Open with `NewLeanReaderFromProvider`.

**Assertions:**
- `BlocksForTraceID(tid)` returns a non-nil, non-empty slice for every inserted trace ID.
- No false negatives: inserted traces must never be pruned by the bloom filter.

Back-ref: `reader/reader_test.go:TestTraceIDBloom_PresentTraces`

---

### TIDBLOOM-02: Absent trace ID returns nil

**Scenario:** A trace ID never written to the file returns nil from `BlocksForTraceID`.

**Setup:**
- Write 100 spans with trace IDs having `tid[1] = 0xAA`. Flush.
- Open with `NewLeanReaderFromProvider`.

**Assertions:**
- The all-0xFF trace ID (`[16]byte{0xFF,...,0xFF}`) is guaranteed absent and must return nil.

Back-ref: `reader/reader_test.go:TestTraceIDBloom_AbsentTrace`

---

### TIDBLOOM-03: LeanReader uses bloom-enabled compact index (v2)

**Scenario:** `NewLeanReaderFromProvider` correctly handles version-2 compact indexes.

**Setup:**
- Write one span (traceID `{0xDE,0xAD,0xBE,0xEF,...}`). Flush.
- Open with `NewLeanReaderFromProvider`.

**Assertions:**
- Present trace ID returns non-nil blocks.
- Absent trace ID (`{0xFF,...,0xFF}`) returns nil.

Back-ref: `reader/reader_test.go:TestTraceIDBloom_LeanReader`

---

### TIDBLOOM-04: Full and lean readers agree on all trace lookups

**Scenario:** Both `NewReaderFromProvider` and `NewLeanReaderFromProvider` return
identical block lists for every inserted trace ID.

**Setup:**
- Write 20 spans with sequential trace IDs `{i+1, 0, ..., 0}`. Flush.
- Open with both reader constructors.

**Assertions:**
- For every trace ID: `BlocksForTraceID` returns the same blocks from both readers.
- All 20 trace IDs return non-empty results from both readers.

Back-ref: `reader/reader_test.go:TestTraceIDBloom_VersionV2`

---

### TIDBLOOM-UNIT-01 through TIDBLOOM-UNIT-05: Trace ID bloom unit tests

**Scenario:** Unit tests for `TraceIDBloomSize`, `AddTraceIDToBloom`, and `TestTraceIDBloom`.

Back-ref: `shared/shared_test.go:TestTraceIDBloomSize`, `TestTraceIDBloomNoFalseNegatives`,
`TestTraceIDBloomEmptyBloom`, `TestTraceIDBloomAbsentTrace`, `TestTraceIDBloomFalsePositiveRate`

- **Size:** `TraceIDBloomSize(0)` = `TraceIDBloomMinBytes`; grows with count; capped at `TraceIDBloomMaxBytes`.
- **No false negatives:** All 200 inserted random trace IDs test true in their bloom.
- **Empty bloom:** `TestTraceIDBloom(nil, id)` = true (vacuous); `TestTraceIDBloom([]byte{}, id)` = true.
- **All-zero bloom:** No trace ID tests true in a zero-filled bloom.
- **FP rate:** With 1000 inserts and 10,000 absent tests, observed FP rate < 3%.

---

## 6. Footer Version Compatibility Tests

### COMPAT-01: Read v2 footer

**Scenario:** A file with a v2 footer (10-byte, no compact index) is read correctly.

**Setup:**
- Construct a minimal valid blockpack file with a v2 footer manually (or by using a
  writer that explicitly writes v2).

**Assertions:**
- `readFooter` returns `version = 2`, correct `headerOffset`.
- `compactOffset` and `compactLen` are zero (not present).
- Reader opens and reads block data normally.

---

### COMPAT-02: v3 footer detection takes priority

**Scenario:** When the last 22 bytes look like a v3 footer, it is parsed as v3.

**Setup:**
- Read a valid v3 footer file.

**Assertions:**
- `readFooter` returns `version = 3`.
- `compactOffset` and `compactLen` are valid and non-zero.

---

### COMPAT-03: Invalid footer returns error

**Scenario:** Corrupted or truncated footer bytes return a clear error.

**Setup:**
- Construct a byte slice of 22 bytes filled with `0xFF` (invalid version numbers).

**Assertions:**
- `readFooter` returns an error containing both the v3 and v2 version bytes seen.

---

## 7. Read Path Tests

### READ-01: Column filtering — only requested columns decoded

**Scenario:** `GetBlockWithBytes` with a `want` column set only decodes requested columns.

**Setup:**
- Write spans with 20 distinct attribute columns.
- Flush.
- Open reader and call `GetBlockWithBytes` requesting only 3 columns.

**Assertions:**
- Returned `Block.Columns()` contains exactly those 3 requested columns (plus any
  always-present intrinsic columns if applicable).
- Columns not in `want` are absent from the block.

---

### READ-02: Full block read — no want set

**Scenario:** `GetBlockWithBytes` with `nil` want set returns all columns.

**Setup:**
- Write spans with 5 attribute columns.
- Flush.
- Call `GetBlockWithBytes(0, nil, ...)`.

**Assertions:**
- Returned block contains all 5 attribute columns plus all intrinsic columns written.

---

### READ-03: AddColumnsToBlock — incremental column loading

**Scenario:** Additional columns can be added to an already-loaded block without re-reading.

**Setup:**
- Write spans with columns `"a"`, `"b"`, `"c"`.
- Flush.
- Call `GetBlockWithBytes(0, {"a"}, ...)` → gets block with column `"a"`.
- Call `AddColumnsToBlock(bwb, {"b", "c"}, ...)`.

**Assertions:**
- Block now contains columns `"a"`, `"b"`, `"c"`.
- No additional I/O was performed (no new `readRange` calls).

---

### READ-04: CoalesceBlocks — adjacent block merging

**Scenario:** Adjacent blocks in a query are merged into a single I/O.

**Setup:**
- Write 5 blocks.
- Call `CoalesceBlocks([0, 1, 2, 3, 4], 4*1024*1024)` with aggressive config.

**Assertions:**
- Returns 1 `CoalescedRead` covering all 5 blocks.
- `ReadCoalescedBlocks` returns all 5 per-block byte slices.

---

### READ-05: CoalesceBlocks — gap between blocks

**Scenario:** Blocks with large gaps are not merged beyond the configured maximum.

**Setup:**
- Construct a reader with block offsets spread 10 MB apart.
- Call `CoalesceBlocks` with a 4 MB gap limit.

**Assertions:**
- Blocks farther than 4 MB apart are NOT coalesced into a single read.
- Each gets its own `CoalescedRead`.

---

### READ-06: Block reuse in parseBlockColumnsReuse

**Scenario:** Reusing a `*Block` allocation across two `parseBlockColumnsReuse` calls does
not corrupt data.

**Setup:**
- Decode block 0 into `block`.
- Reuse `block` to decode block 1 (different column values).

**Assertions:**
- Block 1's column values are correct.
- Block 0's values are not visible in the reused block.

---

## 8. Writer Config and Validation Tests

### CFG-01: Missing OutputStream returns error

**Assertions:**
- `NewWriterWithConfig(WriterConfig{})` returns a non-nil error.

---

### CFG-02: MaxBlockSpans = 0 uses default

**Setup:**
- Create writer with `MaxBlockSpans = 0`.

**Assertions:**
- Writer uses the default max span count (2000).
- No error on construction.

---

### CFG-03: MaxBlockSpans respects uint16 limit

**Setup:**
- Create writer with `MaxBlockSpans = 65536` (exceeds uint16 max).

**Assertions:**
- `NewWriterWithConfig` returns an error, OR
- The writer clamps to 65535.

---

### CFG-04: Concurrent AddSpan panics

**Scenario:** Calling `AddSpan` concurrently panics immediately.

**Assertions:**
- Calling `AddSpan` from two goroutines simultaneously causes a panic.
- The panic message indicates concurrent access.

---

## 9. Block Format Validation Tests

### VAL-01: Wrong magic number returns error

**Setup:**
- Write a valid block, then corrupt the first 4 bytes.

**Assertions:**
- `parseBlockColumnsReuse` returns an error mentioning invalid magic.

---

### VAL-02: Unsupported block version returns error

**Setup:**
- Write a valid block, then change byte 4 (version) to 99.

**Assertions:**
- `parseBlockColumnsReuse` returns an error mentioning unsupported version.

---

### VAL-03: Column count mismatch

**Setup:**
- Write a valid block, then change `column_count` at bytes 12–15 to a larger value.

**Assertions:**
- `parseBlockColumnsReuse` returns an error (truncated metadata or out-of-bounds).

---

### VAL-04: Column data out of bounds

**Setup:**
- Write a valid block, then set a column's `data_offset` to exceed the block length.

**Assertions:**
- `parseBlockColumnsReuse` returns an out-of-bounds error.

---

### VAL-05: Limits enforced — MaxSpans

**Setup:**
- Attempt to parse a block header with `span_count = MaxSpans + 1`.

**Assertions:**
- Returns a validation error (span count exceeds limit).

---

### VAL-06: Limits enforced — MaxColumns

**Setup:**
- Construct a block header with `column_count = MaxColumns + 1`.

**Assertions:**
- Returns a validation error (column count exceeds limit).

---

## 10. Presence RLE Tests

### RLE-01: All present

**Setup:**
- Encode a presence bitset of 10 rows, all set.

**Assertions:**
- `EncodePresenceRLE` produces a valid RLE buffer.
- `DecodePresenceRLE` returns a bitset with all 10 bits set.

---

### RLE-02: All absent

**Setup:**
- Encode a presence bitset of 10 rows, all unset.

**Assertions:**
- `DecodePresenceRLE` returns a bitset with all 10 bits unset.

---

### RLE-03: Alternating pattern

**Setup:**
- Presence = `[true, false, true, false, ...]` for 20 rows.

**Assertions:**
- Round-trips exactly.

---

### RLE-04: Single run

**Setup:**
- Presence = first 5 of 10 rows set.

**Assertions:**
- Encoded RLE has 2 runs (present×5, absent×5).
- Decode matches.

---

## 11. Sort Order Tests

### SORT-01: Spans sorted by service name

**Setup:**
- Write 6 spans with service names `"svc-c"`, `"svc-a"`, `"svc-b"`, `"svc-a"`, `"svc-c"`, `"svc-b"`.
- Flush.

**Assertions:**
- Block 0 starts with the `"svc-a"` spans.
- `"svc-b"` spans follow.
- `"svc-c"` spans are last.
- (Exact row order within a service group is MinHash-dependent and not asserted.)

---

### SORT-02: MinHash clustering

**Setup:**
- Write spans where spans 0, 2, 4 share identical attributes and spans 1, 3, 5 share a
  different identical attribute set.
- Flush with `MaxBlockSpans = 3`.

**Assertions:**
- Block 0 contains primarily spans from one MinHash group.
- Block 1 contains primarily spans from the other MinHash group.

---

## 12. I/O Metrics Tests

### IO-01: Single block = single I/O operation

**Setup:**
- Write 100 spans, Flush.
- Open reader with tracking provider.
- Call `GetBlockWithBytes(0, nil, ...)`.

**Assertions:**
- Tracking provider records exactly 1 read call for the block data.
- (Footer, header, metadata reads are separate but should be bounded.)

---

### IO-02: No per-column I/O

**Setup:**
- Write 20 columns per span, Flush.
- Open reader.
- Call `GetBlockWithBytes(0, {"col.1"}, ...)`.

**Assertions:**
- The number of I/O calls equals the calls for the full block — NOT 1 per column.
- Column filtering happens in memory, not at I/O time.

---

## 13. OTLP Full-Field Coverage Tests

**Requirement:** Every field defined in the OTLP trace proto MUST be captured as a
column and must round-trip without loss. If a field is not yet stored (e.g. dropped
counts), a failing test or a `t.Skip` with a TODO comment must mark the gap so it is
not silently forgotten.

**Attribute value types:** OTLP attributes may carry any of: `string`, `int64`,
`double` (float64), `bool`, `bytes`. All five types MUST be tested for both span
attributes and resource attributes.

### OFT-01: All intrinsic fields

**Scenario:** Every OTLP Span intrinsic (trace:id, span:id, span:parent_id,
span:name, span:kind, span:start, span:end, span:duration, span:status,
span:status_message, trace:state, resource:schema_url, scope:schema_url) round-trips
correctly when all are populated.

**Setup:**
- Write one span with every intrinsic field set to a known non-zero/non-empty value.
- Set `Status.Code = STATUS_CODE_ERROR`, `Status.Message = "something went wrong"`.
- Set `TraceState = "vendor=value"`.
- Set non-empty `resourceSchemaURL` and `scopeSchemaURL`.

**Assertions:**
- `trace:id` returns the 16-byte TraceId.
- `span:id` returns the 8-byte SpanId.
- `span:parent_id` returns the 8-byte ParentSpanId.
- `span:name` returns the span name string.
- `span:kind` returns the numeric kind value (int64).
- `span:start` returns StartTimeUnixNano.
- `span:end` returns EndTimeUnixNano.
- `span:duration` returns EndTimeUnixNano − StartTimeUnixNano.
- `span:status` returns int64(STATUS_CODE_ERROR).
- `span:status_message` returns `"something went wrong"`.
- `trace:state` returns `"vendor=value"`.
- `resource:schema_url` returns the resource schema URL.
- `scope:schema_url` returns the scope schema URL.

---

### OFT-02: Conditional intrinsics absent when zero/empty

**Scenario:** `span:status`, `span:status_message`, `trace:state`,
`resource:schema_url`, and `scope:schema_url` are NOT written when their values are
the zero/empty default.

**Assertions:**
- `block.GetColumn("span:status")` == nil.
- `block.GetColumn("span:status_message")` == nil.
- `block.GetColumn("trace:state")` == nil.
- `block.GetColumn("resource:schema_url")` == nil.
- `block.GetColumn("scope:schema_url")` == nil.

---

### OFT-03: All span attribute value types

**Scenario:** Span attributes of every OTLP value type round-trip correctly.

**Setup:**
- Write one span with span attributes: `string`, `int64`, `double`, `bool`, `bytes`.

**Assertions:**
- `span.attr.str` → `StringValue` == `"hello"`.
- `span.attr.int` → `Int64Value` == `-42`.
- `span.attr.float` → `Float64Value` == `3.14159` (exact bits).
- `span.attr.bool` → `BoolValue` == `true`.
- `span.attr.bytes` → `BytesValue` == `{0xDE, 0xAD, 0xBE, 0xEF}`.

---

### OFT-04: All resource attribute value types

**Scenario:** Resource attributes of every OTLP value type round-trip correctly
through the `map[string]any` path.

**Setup:**
- Write one span with resource attributes: `string`, `int64`, `float64`, `bool`, `bytes`.

**Assertions:**
- `resource.res.int` → `Int64Value` == `99`.
- `resource.res.float` → `Float64Value` == `1.23` (exact bits).
- `resource.res.bool` → `BoolValue` == `false`.
- `resource.res.bytes` → `BytesValue` == `{0x01, 0x02, 0x03}`.

---

### OFT-05: Scope attributes

**Scenario:** Scope attributes appear under the `scope.` column prefix.

**Setup:**
- Pass `scopeAttrs = {"version": "v1.0.0", "weight": int64(7)}` to `AddSpan`.

**Assertions:**
- `scope.version` → `StringValue` == `"v1.0.0"`.
- `scope.weight` → `Int64Value` == `7`.

---

### OFT-06: Events and links (gap tracking)

**Scenario:** Spans with events and links are written without error. Columns
`event:name` and `link:trace_id` are checked if present; otherwise the test is
skipped with a note that these fields are not yet captured as columns.

**Assertions (when column is present):**
- `event:name` blob contains the null-separated event names.
- `link:trace_id` blob contains the 16-byte trace ID of each link.

**Gap:** If `event:name` or `link:trace_id` columns are absent, this test MUST use
`t.Skip(...)` to mark the gap so it is visible in test output.

---

## 14. Metadata Compression Tests (V12)

### RT-MC-01: V12 round-trip — compressed metadata

**Scenario:** A V12 file (snappy-compressed metadata) is written and read back
correctly; all block index, trace index, and range index data survive compression.

**Setup:**
- Writer produces a V12 file with `MaxBlockSpans=5` and 13 spans across 3 blocks.
- Flush to an in-memory buffer.

**Assertions:**
- `reader.BlockCount()` == 3.
- `reader.TraceEntries(traceID)` returns 3 entries (one per block).
- `BlockMeta(0).MaxStart > BlockMeta(0).MinStart`.
- No error opening the reader.

**Implementation:** `TestRoundTrip_MetadataCompression` in `reader/reader_test.go`.

---

### RT-BC-01: V11 backward compatibility

**Scenario:** A V11 file (uncompressed metadata) opens correctly with the updated
reader that supports both V11 and V12.

**Setup:**
- Write a single-span file using the current writer (produces V11 or V12 depending
  on the current writer version; this test validates the older format remains readable
  by patching the file header version to V11 or by using a stored V11 fixture).
- For simplicity: write with the writer and verify the reader opens successfully.

**Assertions:**
- `reader.BlockCount()` == 1.
- `reader.BlockMeta(0)` is non-nil.
- No error.

**Implementation:** `TestBackwardCompat_V11Files` in `reader/reader_test.go`.

---

### RT-BG-01: Decompression bomb guard

**Scenario:** A crafted V12 file with a snappy varint header claiming a decoded length
exceeding `MaxMetadataSize` (100 MiB) is rejected before any allocation.

**Setup:**
- Write a valid V12 file with one span.
- Replace the metadata blob with a crafted snappy payload whose varint header claims
  `MaxMetadataSize + 1` bytes of decoded output.
- Reconstruct the file with updated header/footer offsets.

**Assertions:**
- `reader.New` returns an error.
- Error message contains `"snappy decoded size"`.

**Implementation:** `TestDecompressionBombGuard` in `reader/reader_test.go`.

---

## 15. File Layout Analysis Tests

These tests verify `Reader.FileLayout()`, which computes a byte-level map of every
section in a blockpack file. The core invariant is:
`sum(section.CompressedSize) == FileSize` — every byte must be accounted for.

### LAY-01: Byte invariant — single block

**Scenario:** A small file with one block passes the byte invariant.

**Setup:**
- Write 8 spans with varied attribute types (string, int64, float64, bool, bytes).
- Flush and call `FileLayout()`.

**Assertions:**
- `sum(section.CompressedSize) == report.FileSize`.
- Sections are sorted by `Offset` ascending.
- Required sections present: `footer`, `file_header`, `metadata.block_index`, `block[0].header`.
- All column data sections (suffix `.data`) have `Encoding` populated.
- All column sections with `ColumnName != ""` have `ColumnType` populated.
- JSON round-trip preserves `FileSize`, `BlockCount`, and section count.

---

### LAY-02: Byte invariant — empty file

**Scenario:** A file with zero spans still passes the byte invariant.

**Setup:**
- Flush an empty writer and call `FileLayout()`.

**Assertions:**
- `BlockCount == 0`.
- `sum(section.CompressedSize) == FileSize`.
- Required sections present: `footer`, `file_header`, `metadata.block_index`.

---

### LAY-03: Byte invariant — multiple blocks

**Scenario:** A multi-block file passes the byte invariant.

**Setup:**
- Write 9 spans with `MaxBlockSpans = 3` (produces 3 blocks).
- Flush and call `FileLayout()`.

**Assertions:**
- `BlockCount == reader.BlockCount()`.
- `sum(section.CompressedSize) == FileSize`.
- Every block has a `block[N].header` section.

---


### LAY-04: Large-scale byte accounting with compressed and uncompressed sizes

**Scenario:** Build 100 traces with 500 spans each (50,000 total) and verify complete
byte accounting including both compressed and uncompressed sizes for column data.

**Setup:**
- Generate 100 traces × 500 spans with diverse, realistic attributes:
  - 10 service names, 4 HTTP methods, 10 status codes, 5 span kinds.
  - String, int64, float64, bool, and bytes attribute types.
  - Multiple resource attributes per trace.
- Write with `MaxBlockSpans = 2000` (default) → ~25 blocks.
- Flush and call `FileLayout()`.

**Assertions:**
- **Byte invariant:** `sum(section.CompressedSize) == FileSize`.
- **No overlaps:** Sections sorted by offset; `prev.Offset + prev.CompressedSize <= curr.Offset` for all adjacent pairs.
- **Column data completeness:** Every column data section (suffix `.data`) has:
  - `CompressedSize > 0`.
  - `UncompressedSize > 0` (inflated wire size with all zstd chunks streamed to discard).
  - `Encoding`, `ColumnType`, and `ColumnName` populated.
- **Block structure:** Every block has `.header`, `.column_metadata`, and at least one `.column[...].data` section.
- **Required metadata:** `footer`, `file_header`, `metadata.block_index`, `metadata.trace_index`, `metadata.column_index` all present.
- **JSON round-trip:** `FileSize`, `BlockCount`, `FileVersion`, section count, and all `CompressedSize`/`UncompressedSize` values survive marshal/unmarshal.

---

## 16. Compaction Tests

These tests live in `internal/modules/blockio/compaction/compaction_test.go` and validate
the native columnar compaction path (NOTES.md §30). Compaction reads block columns directly
without materializing OTLP proto objects.

### CT-01: Native columns — multi-block round-trip

**Scenario:** Write N blocks with known span data via the writer, compact them via
`CompactBlocks`, read back the output, and verify all column values are preserved exactly.

**Setup:**
- Write 3 blocks × 5 spans each.
- Each span has distinct: traceID, spanID, span name, start/end times, service name,
  `span.http.method` (string attribute), and `resource.env` (resource attribute).
- All span and resource attributes use string values.
- Compact all 3 blocks with `MaxSpansPerBlock=100`.

**Assertions:**
- Exactly 1 output file is produced.
- Total span count in output == 15 (3 × 5).
- For every input span, the compacted output contains an entry with matching:
  - `trace:id` (16 bytes)
  - `span:name`
  - `span:start`, `span:end`
  - `resource.service.name`
  - `span.http.method`
  - `resource.env`

**Implementation:** `TestCompactBlocks_NativeColumns` in `compaction/compaction_test.go`.

---

### CT-02: Deduplication across blocks

**Scenario:** The same span appears in two separate input blocks. Compaction must emit
it exactly once.

**Setup:**
- Write the same single span to two separate blockpack buffers.
- Compact both buffers via `CompactBlocks`.

**Assertions:**
- Total span count across all output blocks == 1.

**Implementation:** `TestCompactBlocks_NativeColumns_Dedup` in `compaction/compaction_test.go`.

---

## 17. End-to-End Parity Smoke Tests

These tests live in `internal/parity/` and are run both as part of `make test` (via
`$(PACKAGES)`) and as a dedicated step in the CI workflow (`go test ./internal/parity
-run TestParitySmokeTest`). They serve as a CI-gated canary for field-coverage
regressions — if a new OTLP field is added, extending PST-01 is required.

### PST-01: Column parity — intrinsics and all attribute types

**Scenario:** Write a single span with all intrinsic fields that are captured as columns
and all five attribute value types (string, int64, float64, bool, bytes) for span,
resource, and scope attribute namespaces. Read back via the blockpack reader and assert
every column value matches the written input.

**Setup:**
- Create a span with all column-mapped intrinsic fields:
  - `TraceId`, `SpanId`, `ParentSpanId`, `Name`, `Kind`, `StartTimeUnixNano`,
    `EndTimeUnixNano`, `TraceState`, `Status.Code`, `Status.Message`.
- Span attributes (all 5 types): `parity.str`, `parity.int`, `parity.float`,
  `parity.bool`, `parity.bytes`.
- Resource attributes (all 5 types): `service.name`, `res.int`, `res.float`,
  `res.bool`, `res.bytes`.
- Scope attributes (all 5 types): `version` (string), `weight` (int64), `ratio`
  (float64), `enabled` (bool), `sig` (bytes).
- Non-empty `resourceSchemaURL` and `scopeSchemaURL`.
- Flush and open reader.

**Assertions (intrinsic columns):**
- `trace:id`, `span:id`, `span:parent_id`, `span:name`, `span:kind`, `span:start`,
  `span:end`, `span:duration`, `span:status`, `span:status_message`, `trace:state`,
  `resource:schema_url`, `scope:schema_url` — each returns the written value.

**Assertions (attribute columns):**
- All 5 span attribute types, all 5 resource attribute types, all 5 scope attribute
  types round-trip with exact value equality.

**Scope of assertions:**
- Covers all intrinsic fields that are captured as block columns.
- Assertions for `event:name` and `link:trace_id` are added once reader support for
  those columns is implemented.

## TS Index Tests

### TSI-01: TestWriteTSIndexSection_Empty
**Scenario:** writeTSIndexSection with nil metas produces a 9-byte header with count=0.
**Assertions:** len == 9, magic correct, version correct, count == 0.

### TSI-02: TestWriteTSIndexSection_SortedByMinTS
**Scenario:** 3 blocks with out-of-order minTS values. Verify entries sorted by minTS
ascending and blockID tracks original position.
**Assertions:** Entry 0 has smallest minTS; blockID matches original index.

### TSI-03: TestBlocksInTimeRange_* (6 cases)
Unit tests covering: nil entries, all match, partial match, none match,
zero-time blocks always included, exact boundary values.

### TSI-04: TestTSIndexRoundTrip_LogFile (integration)
Full write→flush→read round-trip. 6 log records in 3 blocks with distinct time ranges.
Verify TimeIndex() is non-nil, len==3, and BlocksInTimeRange for a window covering one
block returns exactly that block with correct metadata overlap.

---

## 18. Log Body Auto-Parse Tests

### LOG-01: TestParseLogBody_JSON
**Scenario:** JSON log body is parsed and all top-level scalar fields are returned.
**Setup:** Call `parseLogBody` with `{"level":"info","msg":"started","count":42}`.
**Assertions:**
- `result["level"]` == "info".
- `result["msg"]` == "started".
- `result["count"]` == "42" (numeric stringified).

### LOG-02: TestParseLogBody_Logfmt
**Scenario:** Logfmt log body is parsed and all key=value pairs are returned.
**Setup:** Call `parseLogBody` with `level=info msg=started count=42`.
**Assertions:**
- `result["level"]` == "info", `result["msg"]` == "started", `result["count"]` == "42".

### LOG-03: TestParseLogBody_InvalidBody_ReturnsNil
**Scenario:** Plain text that is neither JSON nor logfmt returns nil (silent failure).
**Setup:** Call `parseLogBody` with `"this is not json or logfmt structured"`.
**Assertions:**
- Return value is nil.

### LOG-04: TestParseLogBody_EmptyBody_ReturnsNil
**Scenario:** Empty string body returns nil — no columns created.
**Setup:** Call `parseLogBody("")`.
**Assertions:**
- Return value is nil.

### LOG-05: TestParseLogBody_NestedJSON_OnlyTopLevel
**Scenario:** Nested JSON objects are stringified, not recursed.
**Setup:** Call `parseLogBody` with `{"level":"info","nested":{"a":1}}`.
**Assertions:**
- `result["level"]` == "info".
- `result["nested"]` is non-empty (stringified object value).

### LOG-06: TestParseLogBody_ManyFields_AllExtracted
**Scenario:** Logfmt body with multiple fields — all extracted into the result map.
**Setup:** Call `parseLogBody` with `level=info service=api region=us-east-1 status=200 latency=42ms`.
**Assertions:**
- All five keys present with correct values.

### LOG-07: TestParseLogBody_JSONInvalidNotObject_ReturnsNil
**Scenario:** JSON array (top-level non-object) returns nil.
**Setup:** Call `parseLogBody` with `["a","b","c"]`.
**Assertions:**
- Return value is nil.

### LOG-08: TestAddLogRecordFromProto_JSONBodyExtracted
**Scenario:** JSON log body produces `log.{key}` columns of type `ColumnTypeRangeString`.
**Setup:** Create a `logBlockBuilder`. Add a `pendingLogRecord` with JSON body
`{"level":"error","service":"api"}`. Call `addLogRecordFromProto`.
**Assertions:**
- Column key `{Name:"log.level", Type:ColumnTypeRangeString}` exists in `bb.columns`.
- Column key `{Name:"log.service", Type:ColumnTypeRangeString}` exists in `bb.columns`.
- `rowCount()` on the level column == 1.

### LOG-09: TestAddLogRecordFromProto_LogfmtBodyExtracted
**Scenario:** Logfmt log body produces `log.{key}` range columns.
**Setup:** Create a `logBlockBuilder`. Add a pendingLogRecord with logfmt body
`level=warn msg=timeout duration=150ms`. Call `addLogRecordFromProto`.
**Assertions:**
- Column key `{Name:"log.level", Type:ColumnTypeRangeString}` exists with rowCount==1.

### LOG-10: TestAddLogRecordFromProto_UnparsedBodyNoExtraColumns
**Scenario:** Unparseable body (plain text) produces no extra `log.{key}` columns.
**Setup:** Create a `logBlockBuilder`. Record column count before call. Add a pendingLogRecord
with plain-text body. Call `addLogRecordFromProto`.
**Assertions:**
- `len(bb.columns)` is unchanged from before the call (no new log.{key} columns added).

### LOG-11: TestAddLogRecordFromProto_NilBodyNoExtraColumns
**Scenario:** Nil body field produces no extra columns.
**Setup:** Create a `logBlockBuilder`. Record column count before. Add a pendingLogRecord
with `Body: nil`. Call `addLogRecordFromProto`.
**Assertions:**
- `len(bb.columns)` is unchanged.

---

## 19. SpanFields Allocation Tests

### ALLOC-01: TestIterateFieldsNoSeenMapAllocsAfterBuildIterFields
**Scenario:** Verify that `IterateFields` on a block with a pre-built `iterFields` slice
does not allocate a per-call seen-map (fast path, NOTE-049).
Remaining allocs are numeric value boxing (int64/uint64 → any) which are unrelated to the seen-map fix.
**Setup:**
- Build a test block with `buildAllocTestBlock`.
- Confirm `block.IterFields()` is non-nil (BuildIterFields was called during parse).
- Acquire a pooled adapter via `NewSpanFieldsAdapter`.
**Assertions:**
- `testing.AllocsPerRun(100, ...)` calling `adapter.IterateFields` with a no-op visitor ≤ 10
  (one boxing alloc per numeric column at most — no seen-map overhead).
- Verified in `internal/modules/blockio/span_fields_allocs_test.go:TestIterateFieldsNoSeenMapAllocsAfterBuildIterFields`.

### ALLOC-02: TestSpanMatchCloneAllocsPerSpan
**Scenario:** Verify that `SpanMatch.Clone()` allocates significantly fewer heap objects after
switching the fields container from `map[string]any` to a pooled `[]kvField` slice (NOTE-ALLOC-2).
**Setup:**
- Build a test block. Acquire adapter for row 0. Construct a `SpanMatch`.
- Warm the `kvFieldSlicePool` with one Clone call.
**Assertions:**
- `testing.AllocsPerRun(100, ...)` for `sm.Clone()` is ≤ N_columns + 2 (N ≈ 10 for test block).
- Result is strictly lower than the pre-optimization baseline of N + 3 (map header + buckets).
- Verified in `internal/modules/blockio/span_fields_allocs_test.go:TestSpanMatchCloneAllocsPerSpan`.

### ALLOC-03: TestNewSpanFieldsAdapterPooled
**Scenario:** Verify that a pooled get+release round-trip of `NewSpanFieldsAdapter` /
`ReleaseSpanFieldsAdapter` allocates zero heap objects after the pool is warmed (NOTE-ALLOC-4).
**Setup:**
- Build a test block. Warm the pool: call `NewSpanFieldsAdapter` then `ReleaseSpanFieldsAdapter`.
**Assertions:**
- `testing.AllocsPerRun(100, ...)` for get+release loop ≤1 (allows for one cold-pool alloc after GC sweep; sync.Pool may drop entries between AllocsPerRun iterations).
- Verified in `internal/modules/blockio/span_fields_allocs_test.go:TestNewSpanFieldsAdapterPooled`.

### ALLOC-04: BenchmarkExtractIDsHex
**Scenario:** Measure the allocation profile of `hex.EncodeToString` replacing
`fmt.Sprintf("%x", v)` for trace/span ID hex encoding (NOTE-ALLOC-1).
**Setup:**
- Fixed 16-byte trace ID and 8-byte span ID byte slices.
**Assertions (benchmark):**
- `b.ReportAllocs()` output; expected 1 alloc per call (one string allocation per encode).
- Verified in `internal/modules/blockio/span_fields_allocs_test.go:BenchmarkExtractIDsHex`.

---

## 20. Bug Regression Tests (BUG-02, BUG-07, BUG-08)
*Added: 2026-03-23*

### BUG-02-01: TestTryApplyExactValues_Uint64_HighBit
**File:** `internal/modules/blockio/writer/range_index_bug02_test.go`
**Scenario:** `tryApplyExactValues` for `ColumnTypeRangeUint64` must sort boundaries in
unsigned order. Values straddling 2^63 (e.g. `0x7FFFFFFFFFFFFFFF` and `0x8000000000000000`)
were previously sorted as int64, making the high-bit value appear negative and sort first.
**Setup:** Build a `rangeColumnData` with two blocks whose exact values are `math.MaxInt64`
and `math.MaxInt64 + 1`. Call `tryApplyExactValues`.
**Assertions:**
- Returns `true` (exact-value path taken).
- `len(cd.boundaries) == 2`.
- `uint64(cd.boundaries[0]) < uint64(cd.boundaries[1])` (unsigned order preserved).
- `uint64(cd.boundaries[0]) == math.MaxInt64`, `uint64(cd.boundaries[1]) == math.MaxInt64 + 1`.

### BUG-07-01: TestCompareRangeKey_Float64_NaN
**File:** `internal/modules/blockio/reader/range_index_bugs_test.go`
**Scenario:** `compareRangeKey` for `ColumnTypeRangeFloat64` must not return 0 when either
operand is NaN. IEEE 754: `NaN < x`, `NaN > x`, and `NaN == x` are all false — the old
manual `<`/`>` branches both failed, returning 0 and corrupting binary search.
**Setup:** Encode `math.NaN()`, `1.0`, and `math.Inf(-1)` as 8-byte LE keys.
**Assertions:**
- `compareRangeKey(NaN, 1.0) != 0`.
- `compareRangeKey(1.0, NaN) != 0`.
- `compareRangeKey(NaN, NaN) == 0` (stable total order).
- `compareRangeKey(NaN, -Inf) < 0` (NaN sorts below all non-NaN per `cmp.Compare`).
- Antisymmetry: `sign(NaN, 1.0) == -sign(1.0, NaN)`.

### BUG-08-01: TestDecodeFloat64Key_ShortKey_ReturnsNaN
**File:** `internal/modules/blockio/reader/range_index_bugs_test.go`
**Scenario:** `decodeFloat64Key` must return `math.NaN()` for short keys (len < 8), not
`0.0`. Returning `0.0` silently treats malformed data as a valid zero-value.
**Setup:** Call `decodeFloat64Key("")` and `decodeFloat64Key("short")`.
**Assertions:** Both calls return a value for which `math.IsNaN(v)` is true.

### BUG-08-02: TestDecodeInt64Key_ShortKey_ReturnsSentinel
**File:** `internal/modules/blockio/reader/range_index_bugs_test.go`
**Scenario:** `decodeInt64Key` must return `math.MinInt64` for short keys, not `0`.
`0` is a valid int64 value; `math.MinInt64` is the minimum sentinel (sorts below all valid).
**Setup:** Call `decodeInt64Key("")` and `decodeInt64Key("short")`.
**Assertions:** Both calls return `math.MinInt64`.

### BUG-08-03: TestDecodeUint64Key_ShortKey_ReturnsSentinel
**File:** `internal/modules/blockio/reader/range_index_bugs_test.go`
**Scenario:** `decodeUint64Key` already returns `0` for short keys, which is the correct
minimum uint64 sentinel. This test confirms the existing behavior is correct.
**Setup:** Call `decodeUint64Key("")` and `decodeUint64Key("short")`.
**Assertions:** Both calls return `uint64(0)`.
