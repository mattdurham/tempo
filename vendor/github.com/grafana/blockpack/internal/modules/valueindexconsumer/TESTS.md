# valueindexconsumer — Test Specifications

This document defines the required tests for the `internal/modules/valueindexconsumer`
package. Each test is described with its scenario, setup, and expected assertions, per root
`SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, and distinct from
`valueindex/TESTS.md`'s and `valueindexcompactor/TESTS.md`'s own independent `TEST-VI-N`
sequences despite the shared prefix). IDs are assigned in ascending order and never reused or
renumbered.

**This file did not exist before 2026-07-04** — created as part of task #85 (wiring
`internal/modules/valueindex/traceindex.go` into the trace-by-id path), per CLAUDE.md's
standing permission to create spec files under `internal/modules/`.

Next free ID: **TEST-VI-9**.

---

## TEST-VI-1: `ParentSpanID` survives the extraction → `ColumnEntry` mapping hop
*Added: 2026-07-04*

**Scenario:** The new `ParentSpanID` field on root `ValueIndexEntry` must reach
`valueindexconsumer.ColumnEntry` unchanged via the `s3Extractor.Extract` mapping, including
the root-span zero-value case.

**Setup:** `TestS3Extractor_ExtractYieldsParentSpanID` and
`TestS3Extractor_ExtractYieldsZeroParentSpanIDForRootSpan` (new file
`cmd/value-index-consumer/main_test.go`) exercise the new unexported
`columnEntryFromValueIndexEntry(e blockpack.ValueIndexEntry, sourceRef string) vicconsumer.ColumnEntry`
helper directly (no S3/minio dependency needed), constructing a `ValueIndexEntry` with a
non-zero and a zero `ParentSpanID` respectively.

**Assertions:** The returned `ColumnEntry.ParentSpanID` matches the input exactly in both
cases; the root-span case is exactly `[8]byte{}`, not merely "falsy."

**Spec invariants tested:** SPEC-VI-1 (`SPECS.md`).

**Cross-package note:** these tests live in `cmd/value-index-consumer/` (not this package's own
test files) because the mapping helper itself lives there — `cmd/` packages are not
spec-driven modules in this repo, but the behavior being tested is this package's own
`ColumnEntry` contract, so it is documented here.

Back-refs: `cmd/value-index-consumer/main_test.go:TestS3Extractor_ExtractYieldsParentSpanID`,
`TestS3Extractor_ExtractYieldsZeroParentSpanIDForRootSpan`. Root-package companion:
`valueindex_extract_test.go:TestExtractValueIndexEntries_YieldsParentSpanID` (documents the
upstream `ValueIndexEntry` side of the same field; not part of this package's own test suite).

---

## TEST-VI-2: Trace-group flush produces a file `DiscoverIndexFiles` finds and decodes correctly
*Added: 2026-07-04*

**Scenario:** A tenant's trace-group buffer, once flushed, must be discoverable via the same
generic `DiscoverIndexFiles` mechanism every other indexed column uses, with zero
discovery-side code changes, and must decode back to the exact spans that were buffered.

**Setup:** `TestFlushTraceGroups_ProducesDiscoverableFile` (`traceflush_test.go`) feeds a fake
`Extractor` yielding `ColumnEntry`s for two spans of one trace across two `ingest` calls
(simulating two source blocks), triggers a flush, then: (a) asserts the fake store received a
PUT under `path.Join(tenant, indexPrefix, valueindex.ColHash("trace:id"), "uuid", ...)`, (b)
runs `DiscoverIndexFiles` against the fake store's `List` with that exact
`(tenant, indexPrefix, colHash, colTypeName)` and confirms it finds the file, (c) runs
`valueindex.DecodeTraceGroups` on the fetched bytes and confirms it recovers a single
`TraceGroup` with both spans, correct `ParentSpanID` linkage, correct
`SourceRef`/`BlockRef`/`RowIdx` per span.

**Assertions:** PUT key matches the standard column-directory convention exactly; discovery
finds it with no special-casing; decode round-trips both spans correctly.

**Spec invariants tested:** SPEC-VI-2 (key layout, "no discovery-side changes needed" claim).

Back-ref: `internal/modules/valueindexconsumer/traceflush_test.go:TestFlushTraceGroups_ProducesDiscoverableFile`.

---

## TEST-VI-3: Dedup within one flush window is deferred to compaction, not done at flush time
*Added: 2026-07-04*

**Scenario:** Two `ingest` calls that yield the exact same `(TraceID, SpanID)` pair within one
flush window (simulating a block re-processed, or the same span observed via two overlapping
extraction windows) must **not** be deduplicated by the flush path itself — the flushed file's
decoded `TraceGroup` retains both `SpanEntry`s.

**Setup:** `TestFlushTraceGroups_DedupWithinOneFlushWindow` (`traceflush_test.go`) feeds the
same `(TraceID, SpanID)` observation twice within one flush window, flushes, decodes the
result.

**Assertions:** the decoded `TraceGroup` has **two** `SpanEntry`s for that `SpanID`, not one —
this is the deliberate, documented behavior (see SPECS.md SPEC-VI-2's "Dedup scope" paragraph
for the resolved ambiguity in `plan.md`'s own self-contradictory wording for this test).
`valueindex.MergeTraceGroups`'s own dedup contract (tested in `valueindex/TESTS.md`, this
package's compaction-time counterpart) is what eventually collapses this to one entry once the
file is compacted — this test exists to lock in that the flush path does NOT duplicate that
work.

**Spec invariants tested:** SPEC-VI-2 (dedup scope decision).

Back-ref: `internal/modules/valueindexconsumer/traceflush_test.go:TestFlushTraceGroups_DedupWithinOneFlushWindow`.

---

## TEST-VI-4: Trace-group messages are acked only after their PUT succeeds
*Added: 2026-07-04*

**Scenario:** A message whose only touched buffer this ingest call was the trace-group buffer
must be acked only after that buffer's flush `Put` succeeds — mirrors the existing per-column
`pendingCol`/ack contract exactly, now sharing the `resolvePendingAcks` helper with
`flushColumn`.

**Setup:** `TestFlushTraceGroups_AcksOnlyAfterPut` (`traceflush_test.go`) wraps the fake
store's `Put` to fail on the first attempt, runs an ingest+flush cycle, and asserts the message
is NOT acked while `Put` fails, then succeeds and is acked once `Put` succeeds on a retry.

**Assertions:** no `Ack` call before a successful `Put`; exactly one `Ack` call after.

**Spec invariants tested:** SPEC-VI-2 (ack contract).

Back-ref: `internal/modules/valueindexconsumer/traceflush_test.go:TestFlushTraceGroups_AcksOnlyAfterPut`.

---

## TEST-VI-5: Extraction yields the correct `TraceID` for every entry, not just zero
*Added: 2026-07-04*

**Scenario:** `extractBlockColumns` must populate `TraceID` on every yielded
`ValueIndexEntry`/`ColumnEntry`, for every column — this is a regression test for a real,
previously-silent bug where `TraceID` was always the zero value in production (SPECS.md
SPEC-VI-3).

**Setup:** `TestExtractValueIndexEntries_YieldsTraceID` (root `valueindex_extract_test.go`)
constructs a block with a known, non-zero `trace:id` value and asserts every yielded entry for
that block's rows carries the matching `TraceID`, cross-checked the same way the existing
`ParentSpanID` test (TEST-VI-1's root-side companion) verifies its field.

**Assertions:** `TraceID` is non-zero and correct on every entry, not only entries for the
`trace:id` column itself.

**Spec invariants tested:** SPEC-VI-3.

Back-ref: `valueindex_extract_test.go:TestExtractValueIndexEntries_YieldsTraceID` (root
package).

---

## TEST-VI-6: `trace:id` never reaches the standard per-column buffer
*Added: 2026-07-04*

**Scenario:** `ingest` must exclude `trace:id` from the standard `columnBuffer`/`flushColumn`
path unconditionally — even if an operator's `s.columns` allowlist configuration would
otherwise include it — to prevent a `BucketGroup`-format file from ever being written into the
colDir the compactor's format-dispatch treats as 100% `TraceGroup` format (SPECS.md SPEC-VI-4).

**Setup:** `TestIngest_TraceIDColumnExcludedFromStandardIndexing`
(`traceflush_test.go`) configures `s.columns` to explicitly include `trace:id` (simulating an
operator who added it to their allowlist, deliberately or by mistake), feeds a fake `Extractor`
yielding a `trace:id` `ColumnEntry`, and asserts no standard `columnBuffer` for `trace:id` is
ever created — the trace-group buffer (a separate mechanism, gated on the `span:id` sentinel
column per SPEC-VI-2, not on `s.columns`) is unaffected.

**Assertions:** `s.buffers` contains no entry keyed by `trace:id`; a subsequent flush produces
no standard-path PUT under `colHash("trace:id")`.

**Spec invariants tested:** SPEC-VI-4.

Back-ref: `internal/modules/valueindexconsumer/traceflush_test.go:TestIngest_TraceIDColumnExcludedFromStandardIndexing`.

**See also:** `traceindex_pipeline_test.go:TestTraceIndexPipeline_ExtractFlushCompactQuery`
(root package) is the full end-to-end composition test exercising extraction → consumer flush
→ compactor merge → `GetTraceByID` query against real (non-fake) implementations of each
stage — the canonical reference for how Stages 1-4 fit together, not itself a
`valueindexconsumer`-scoped unit test so it has no dedicated `TEST-VI-N` entry of its own here.

---

## TEST-VI-7: colHash-manifest hook — happy path, nil-store no-op, fast-error tolerance
*Added: 2026-07-13 (task #216)*

**Scenario:** `flushColumn`'s best-effort colHash-manifest hook (`recordManifestEntry`,
SPEC-VI-5) records an entry when configured, is a pure no-op when unconfigured, and never fails
the real flush when the manifest store returns a fast error.

**Setup/Assertions (`manifest_hook_test.go`):**

- `TestFlushColumn_RecordsManifestEntry` — a configured `ManifestStore` receives exactly one
  `Put`; the resulting manifest entry decodes to the correct `ColumnName`/`ColumnHash`/
  `FirstSeenBy`.
- `TestFlushColumn_NilManifestStoreIsNoOp` — the default (unset) `ManifestStore` leaves
  `flushColumn`'s real S3 write entirely unaffected.
- `TestFlushColumn_ManifestPutFailureDoesNotFailFlush` — a `ManifestStore` whose `Put` returns a
  FAST error (`context.DeadlineExceeded`, not a hang) never fails `flushColumn`; the real index
  file is still written.

**Spec invariants tested:** SPEC-VI-5.

Back-ref: `internal/modules/valueindexconsumer/manifest_hook_test.go`.

---

## TEST-VI-8: colHash-manifest hook — hanging-store bound and in-process cache (task #216 post-review fix pass)
*Added: 2026-07-13*

**Scenario:** Two follow-up findings from task #216's review pass (CRITICAL + HIGH):
`flushColumn` must remain bounded even when the manifest store HANGS (not merely errors fast),
and repeated flushes of an already-recorded column must not re-issue the manifest `Get`.

**Setup/Assertions (`manifest_hook_test.go`):**

- `TestFlushColumn_HangingManifestStoreDoesNotBlockFlush` — a `hangingManifestStore` whose
  `Get`/`Put` block on `<-ctx.Done()` must still let `flushColumn` return within a bounded time
  (via `colhashmanifest.manifestOpTimeout`, SPEC-COLMANIFEST-5) and still write the real index
  file. Confirmed red (hung indefinitely) before `colhashmanifest`'s timeout fix landed, green
  (~6s, 2x `manifestOpTimeout`) after.
- `TestFlushColumn_ManifestCacheSkipsRepeatedGet` — flushing the same column 3 times against a
  call-counting `fakeManifestStore` must issue exactly 1 manifest `Get` total (SPEC-VI-6).
  Confirmed red (3 calls) before `Service.manifestSeen` was added, green (1 call) after.

**Spec invariants tested:** SPEC-VI-6, `colhashmanifest/SPECS.md` SPEC-COLMANIFEST-5.

Back-ref: `internal/modules/valueindexconsumer/manifest_hook_test.go`. See `colhashmanifest/
TESTS.md` TEST-COLMANIFEST-15 (the package-level counterpart of the hanging-store test) and
`NOTES.md` NOTE-VI-108.
