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
providers. The compacted output contains only one copy, and the second occurrence is
counted as a genuine dedupe drop.

**Setup:** 2 providers, each containing the same span data (identical `trace:id`, `span:id`,
`name`, start/end, service name).

**Assertions:**
- Exactly 1 output file is produced.
- Total span count across all output blocks equals 1 (not 2).
- `droppedSpans == 1` (holistic-review Fix 2, 2026-07-07 — `droppedSpans` now counts only
  genuine `(trace:id, span:id)` duplicates; see NOTE-104).

Back-ref: `compaction_test.go:TestCompactBlocks_NativeColumns_Dedup`

---

## Coverage Requirements

- `CompactBlocks` with multiple providers and multiple spans must be exercised (COMP-01).
- Deduplication path (`seenSpans` map, counted in `droppedSpans`) must be exercised (COMP-02).
- Legacy-identity-block error path (`dedupeKey` returns the NOTE-469 family error) must be
  exercised at the `dedupeKey` unit level (COMP-08).
- Malformed-row-identity error path (`dedupeKey` returns the distinct row-scoped error)
  must be exercised, both at the `dedupeKey` unit level (COMP-08) and end-to-end through
  `CompactBlocks` (COMP-05).
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

## COMP-05: TestCompactBlocks_MalformedRowIdentity_ReturnsTypedError

**Scenario (rewritten 2026-07-07, holistic-review Fix 2 — supersedes the original
`TestCompactBlocks_DroppedSpans`):** A block contains a span with a missing `trace:id`
(the block-payload column exists — the writer always creates it — but this row's value is
absent). Per NOTE-104, this now aborts compaction with a typed, greppable error instead of
being silently dropped, mirroring `writer.AddRowFromReader`'s treatment of the identical
row-level condition.

**Setup:** One provider with two spans: one valid (16-byte traceID, 8-byte spanID), one
with no IDs set.

**Assertions:**
- `CompactBlocks` returns a non-nil error.
- The error contains `"trace:id missing or malformed at row"`.

The block-entirely-lacks-trace:id-column branch (the NOTE-469 legacy family error) is
pinned separately at the `dedupeKey` unit level — see COMP-08 — since constructing a
serialized legacy-shaped file for the full `CompactBlocks` pipeline is impractical (the
production writer always writes the `trace:id` column).

Back-ref: `compaction_test.go:TestCompactBlocks_MalformedRowIdentity_ReturnsTypedError`

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

---

## COMP-08: dedupeKey typed-error family (rewritten 2026-07-07, holistic-review Fix 2)

**Scenario:** issue #490, task A-10/#104 — after `dedupeKey`'s dead `idIndex`-based fallback was
removed (NOTE-104), `dedupeKey` reads `trace:id`/`span:id` exclusively from block columns.
A later holistic review found the original landing's "cleanly dropped via `droppedSpans`"
treatment of a genuinely legacy-shaped block made `writer.AddRowFromReader`'s equivalent
typed-error guard for the identical condition unreachable from compaction. `dedupeKey`'s
signature changed from `(key, bool)` to `(key, error)`, mirroring `AddRowFromReader`'s
two-branch pattern exactly, plus an additional `span:id`-scoped error branch (`span:id`
is not covered by `AddRowFromReader`, which never validates it — see NOTE-104).

**Setup / Assertions:**
- `TestDedupeKey_BlockColumnsOnly` (modern shape, via `reader.BuildSyntheticIdentityBlock`):
  `dedupeKey` returns the expected `(trace:id, span:id)` key and a nil error.
- `TestDedupeKey_MissingIdentityColumns_ErrorsOnLegacyIntrinsicOnlySourceBlock` (via
  `reader.BuildBlockMissingIdentityColumns`, the `trace:id` column genuinely absent):
  `dedupeKey` returns a non-nil error containing `"legacy intrinsic-only source block
  unsupported"`.
- `TestDedupeKey_MalformedRowTraceID_ErrorsDistinctlyFromLegacyBlock` (via
  `reader.BuildSyntheticIdentityBlock` with a nil `trace:id` value for row 0, column
  present): `dedupeKey` returns a non-nil error containing `"trace:id missing or malformed
  at row 0"` and explicitly NOT containing `"legacy intrinsic-only source block
  unsupported"` — the mutual-exclusion assertion pinning that the two branches never both
  fire for the same input (mirrors `writer_test.go`'s
  `TestAddRowFromReader_ErrorsOnLegacyIntrinsicOnlySourceBlock` /
  `TestAddRowFromReader_ErrorsOnMalformedRowTraceID` pair).
- `TestDedupeKey_MissingSpanIDColumn_Errors`: a block with `trace:id` present but no
  `span:id` values for the row returns `"span:id missing or malformed at row 0"`.

**Spec invariants tested:** NOTE-104 (issue #490); SPECS.md §4 cases 1 and 2.

Back-ref: `internal/modules/blockio/compaction/dedupekey_test.go`
(`TestDedupeKey_BlockColumnsOnly`,
`TestDedupeKey_MissingIdentityColumns_ErrorsOnLegacyIntrinsicOnlySourceBlock`,
`TestDedupeKey_MalformedRowTraceID_ErrorsDistinctlyFromLegacyBlock`,
`TestDedupeKey_MissingSpanIDColumn_Errors`).
