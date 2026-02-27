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

### META-03: Column name bloom filter

**Scenario:** Bloom filter correctly reports column presence.

**Setup:**
- Write spans with columns `"a.b.c"` and `"x.y.z"`.
- Flush and read back.

**Assertions:**
- `blockMeta.ColumnNameBloom` tests as present for `"a.b.c"` and `"x.y.z"`.
- Bloom tests as absent for a column name not added (with very high probability).
- No false negatives: columns actually present must always pass the bloom test.

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

## 4. Trace Block Index Tests

### TBI-01: Single trace across one block

**Scenario:** A trace with multiple spans that all fit in one block.

**Setup:**
- Write 3 spans all sharing the same trace ID.
- Flush.

**Assertions:**
- `reader.BlocksForTraceID(traceID)` returns `[0]`.
- The `TraceBlockEntry` for block 0 has `SpanIndices` containing the row indices of all 3 spans.

---

### TBI-02: Single trace split across blocks

**Scenario:** A trace with many spans that span multiple blocks.

**Setup:**
- Set `MaxBlockSpans = 3`.
- Write 6 spans all sharing the same trace ID.
- Flush.

**Assertions:**
- `reader.BlocksForTraceID(traceID)` returns `[0, 1]`.
- Both blocks have `TraceBlockEntry` entries for the trace.
- Total `SpanIndices` across both blocks == 6 unique row indices.

---

### TBI-03: Multiple traces in one block

**Scenario:** Multiple distinct trace IDs in the same block are all indexed.

**Setup:**
- Write 6 spans: 2 each from 3 different trace IDs.
- Flush.

**Assertions:**
- `reader.BlocksForTraceID(traceID1)` returns `[0]` with 2 span indices.
- `reader.BlocksForTraceID(traceID2)` returns `[0]` with 2 span indices.
- `reader.BlocksForTraceID(traceID3)` returns `[0]` with 2 span indices.
- Unknown trace ID returns empty slice.

---

## 5. Compact Trace Index Tests

### CTI-01: Compact index is written in v3 footer files

**Scenario:** Verify compact trace index is present and parseable.

**Setup:**
- Write spans and Flush with a writer that produces v3 footer.

**Assertions:**
- Footer `compact_len > 0`.
- `parseCompactTraceIndex` returns valid block table and trace entries.
- Magic == 0xC01DC1DE, version == 1.

---

### CTI-02: Compact index matches trace block index

**Scenario:** Compact index and main metadata trace block index agree.

**Setup:**
- Write 10 spans from 5 traces and Flush.

**Assertions:**
- For every trace ID, `BlocksForTraceID` returns the same blocks whether using the
  compact index or the main metadata trace block index.
- Span indices match.

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
