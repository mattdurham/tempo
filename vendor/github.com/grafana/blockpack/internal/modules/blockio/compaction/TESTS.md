# compaction — Test Specifications

This document defines the required tests for the `internal/modules/blockio/compaction` package.
All tests use in-memory blockpack data to exercise the full write → compact → read round-trip.

---

## COMP-01: TestCompactBlocks_NativeColumns

**Scenario:** Multiple input blocks from separate providers are merged into a single output
file. All column values are preserved verbatim through the native columnar copy path.

**Setup:** 3 providers, each with 5 spans (distinct trace IDs, span IDs, names, start/end
times, resource and span attributes). Use `MaxSpansPerBlock = 100` (no block rotation).

**Assertions:**
- Exactly 1 output file is produced.
- Output contains all 15 spans (3 × 5).
- For each span: `trace:id`, `span:id`, `span:name`, `span:start`, `span:end`,
  `resource.service.name`, `span.http.method`, `resource.env` all match input values exactly.

Back-ref: `compaction_test.go:TestCompactBlocks_NativeColumns`

---

## COMP-02: TestCompactBlocks_NativeColumns_Dedup

**Scenario:** The same span (same trace:id and span:id) appears in two separate input
providers. The compacted output contains only one copy.

**Setup:** 2 providers, each containing the same span data (identical `trace:id`, `span:id`,
`name`, start/end, service name).

**Assertions:**
- Exactly 1 output file is produced.
- Total span count across all output blocks equals 1 (not 2).

Back-ref: `compaction_test.go:TestCompactBlocks_NativeColumns_Dedup`

---

## Coverage Requirements

- `CompactBlocks` with multiple providers and multiple spans must be exercised (COMP-01).
- Deduplication path (`seenSpans` map) must be exercised (COMP-02).
- Silent-drop path (`dedupeKey` returns false) should be exercised (missing trace:id/span:id).
- Context cancellation path should be exercised.
- Empty providers input should be exercised (returns nil, 0, nil).
- `MaxOutputFileSize` rotation (multiple output files) should be exercised.

---

## COMP-03: TestCompactBlocks_EmptyProviders

**Scenario:** `CompactBlocks` is called with an empty (nil) providers slice.

**Setup:** Call `CompactBlocks` with `nil` providers and a valid `memOutputStorage`.

**Assertions:**
- Returns `(nil, 0, nil)` — no output paths, zero dropped spans, no error.

Back-ref: `compaction_test.go:TestCompactBlocks_EmptyProviders`

---

## COMP-04: TestCompactBlocks_NilOutputStorage

**Scenario:** `CompactBlocks` is called with a nil `outputStorage`.

**Setup:** One valid provider; `outputStorage` argument is `nil`.

**Assertions:**
- Returns a non-nil error immediately.

Back-ref: `compaction_test.go:TestCompactBlocks_NilOutputStorage`

---

## COMP-05: TestCompactBlocks_DroppedSpans

**Scenario:** A block contains spans with missing or invalid `trace:id` / `span:id`.
  Those spans are silently dropped, counted in `droppedSpans`, and valid spans still appear.

**Setup:** One provider with three spans: one valid (16-byte traceID, 8-byte spanID),
  one with no IDs set (zero-length slices), one with a 2-byte traceID (wrong length).

**Assertions:**
- `droppedSpans >= 2`.
- Exactly 1 span appears in the compacted output.

Back-ref: `compaction_test.go:TestCompactBlocks_DroppedSpans`

---

## COMP-06: TestCompactBlocks_ContextCancellation

**Scenario:** `CompactBlocks` is called with a pre-canceled context.

**Setup:** One valid provider; context canceled before the call.

**Assertions:**
- Returns a non-nil error.
- Returned error wraps `context.Canceled`.

Back-ref: `compaction_test.go:TestCompactBlocks_ContextCancellation`

---

## COMP-07: TestCompactBlocks_MaxOutputFileSize

**Scenario:** `MaxOutputFileSize` is set to 1 byte, causing rotation after every span
  (estimated size per span is 2048 bytes).

**Setup:** 5 distinct spans in a single provider; `MaxOutputFileSize = 1`.

**Assertions:**
- More than 1 output file is produced.
- Total span count across all output files equals 5.

Back-ref: `compaction_test.go:TestCompactBlocks_MaxOutputFileSize`
