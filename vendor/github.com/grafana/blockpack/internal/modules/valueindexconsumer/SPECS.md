# valueindexconsumer — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/valueindexconsumer` package. It complements `NOTES.md` (design rationale)
and `TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, which is shared/global
across the whole value-index pipeline's NOTES.md files by established convention, and distinct
from `valueindex/SPECS.md`'s and `valueindexcompactor/SPECS.md`'s own independent `SPEC-VI-N`
sequences — despite the identical prefix string, each module's SPECS.md maintains its own
file-scoped numbering). IDs are assigned in ascending order and never reused or renumbered;
superseded entries are marked `[SUPERSEDED by SPEC-VI-N]` rather than deleted.

**This file did not exist before 2026-07-04** — the module previously had only `NOTES.md`.
Created as part of wiring `internal/modules/valueindex/traceindex.go` into the trace-by-id path
(task #85), per CLAUDE.md's standing permission to create spec files under `internal/modules/`.

Next free ID: **SPEC-VI-5**.

---

## SPEC-VI-1: `ColumnEntry.ParentSpanID` — additive identity field
*Added: 2026-07-04*

**Contract:** `ColumnEntry` (`consumer.go:63-75`) carries `ParentSpanID [8]byte` alongside the
existing `SpanID`/`TraceID`/`BlockRef`/`RowIdx` identity fields. It is the zero value
(`[8]byte{}`) when the span is a root span, or when the source block's payload lacks a
`span:parent_id` column entirely (legacy/old-format blocks). Any `Extractor` implementation
that does not populate it (e.g. a test fake) implicitly satisfies this contract via Go's
zero-value default — a root span and an unpopulated field are indistinguishable, which is
correct: both mean "no parent."

**Producer:** the root-package `s3Extractor.Extract` (`cmd/value-index-consumer/main.go`)
populates it via the new unexported `columnEntryFromValueIndexEntry` mapping helper, copying
`ValueIndexEntry.ParentSpanID` (root `valueindex_extract.go:40-41`) unchanged. The field
travels through this one hop with no transformation.

**Consequence:** this is a purely additive change — every existing `ColumnEntry` consumer
that does not read `ParentSpanID` is unaffected. It exists solely to give `bufferTraceRow`
(SPEC-VI-2) enough information to build a `valueindex.SpanEntry` per row without a separate
extraction pass.

Back-refs: `internal/modules/valueindexconsumer/consumer.go:ColumnEntry`,
`cmd/value-index-consumer/main.go:columnEntryFromValueIndexEntry`,
`valueindex_extract.go:ValueIndexEntry` (root package, the upstream source of the field).

---

## SPEC-VI-2: Trace-group flush path — unconditional buffering, dedup scope, key layout
*Added: 2026-07-04*

**Contract:** `Service.ingest` (`service.go:215-324`) buffers every span row into a
per-tenant `traceGroupBuffer` (`traceflush.go`) **unconditionally on the `span:id` sentinel
column**, independent of the `s.columns` configured allowlist that gates the standard
per-column buffers. Trace-by-id coverage must not depend on which attribute columns an
operator chose to index — this is a deliberate widening of what gets buffered beyond the
allowlist, not an oversight.

**Dedup scope (deliberate, not deferred-by-omission):** `flushTraceGroups`
(`traceflush.go:115-209`) groups spilled rows by `TraceID` only. It does **not** deduplicate
`(TraceID, SpanID)` pairs within one flush window — a trace observed twice in the same window
(e.g. the same span re-processed, or overlapping extraction windows) survives as two
`SpanEntry`s in the flushed L0 file. That contract belongs to
`valueindex.MergeTraceGroups` at compaction time (see `valueindex/NOTES.md` NOTE-VI-038's
"Compaction" section: "`(TraceID, SpanID)` pairs are deduplicated — first occurrence wins").
This keeps L0 file production simple and avoids maintaining two divergent copies of the same
dedup logic. **Note for future readers:** `.bob/state/plan.md`'s own Stage 2 TDD item 5 text
is internally self-contradictory (it asks to assert "exactly one SpanEntry... not two" in the
same paragraph that recommends deferring dedup to compaction) — the implementation and its
test (`TestFlushTraceGroups_DedupWithinOneFlushWindow`, see TESTS.md TEST-VI-3) follow the
plan's explicit recommendation (defer to compaction), not the contradictory assertion wording.
This SPECS.md entry is the authoritative resolution of that ambiguity.

**Grouping semantics:** a `TraceGroup`'s `TimeSec` is the minimum `TimeSec` across its
constituent spilled rows (the earliest bucket the trace was seen in this flush window),
matching `MergeTraceGroups`'s own "earliest bucket" semantics so a freshly-flushed L0 file is
already consistent with what a later merge would produce.

**Key layout:** identical convention to the standard per-column flush path —
`path.Join(tenant, IndexPrefix, valueindex.ColHash("trace:id"), valueindex.ColTypeName(ColumnTypeUUID), FormatFilenameV2(0, wallMinSec, wallMaxSec, NewID()))`.
`wallMinSec`/`wallMaxSec` are computed directly from the in-memory `[]TraceGroup` (min/max
`TimeSec`), not decoded back out of the encoded payload — `TraceGroup` files carry no footer
(unlike `BucketGroup` files), so there is nothing to decode post-encode. No discovery-side
code changes are required or were made: `DiscoverIndexFiles` is already fully generic over
`(tenant, colHash, colTypeName)` and finds these files with zero changes (verified directly by
`TestFlushTraceGroups_ProducesDiscoverableFile`, TESTS.md TEST-VI-2).

**Ack contract:** a message is only included in `Ack` after its buffer's `Put` succeeds.
`resolvePendingAcks` (`service.go:521+`) is a shared helper now used by both `flushColumn` and
`flushTraceGroups` so the two buffer kinds (per-column, per-tenant-trace-group) participate in
one consistent `pendingCol` bookkeeping contract — `ingest` tracks `pendingCol[msg.ID] =
len(touched) + len(touchedTrace)` across both buffer kinds together.

**Empty-payload guard:** zero trace rows observed in a flush window (e.g. a block with no
spans, or between flushes with no new trace data) produces zero groups; `flushTraceGroups`
returns early with no `Put` call — mirrors `flushColumn`'s existing empty-payload guard.

**String-table overflow:** `EncodeTraceGroups` returning `ErrStringTableOverflow` within one
flush window is treated as a hard, surfaced error (`consumerOpFlush` metric incremented, no
data silently dropped) rather than attempted batch-splitting — an accepted, documented
low-probability v1 scope boundary, since flush windows are time-bounded (`FlushInterval`,
default 15m) and far smaller than a full compaction batch. Revisit only if telemetry shows
this actually occurs in practice.

**Caveat — REAL, LIVE format collision with the synchronous write path, not merely latent
(corrected 2026-07-05 — see below for the original, incorrect assessment):** the root-package
`WriteValueIndexL0` (`valueindex_l0write.go`, NOTE-VI-042) is a separate, parallel write path
with **real, live production callers**: `/home/mdurham/source/blockpack_collection/tempo`'s
`tempodb/encoding/vblockpack/compactor.go` and `create.go` both call it on every block
write/compaction, gated behind `value_index_enabled` (`valueindex.go:ConfigureValueIndex`).
Its `ExtractValueIndexEntries(r, nil, ...)` call indexes **every** column including `trace:id`
(nil denylist, policy-free extraction by design), and writes it via the **generic**
`BucketGroup` format (`flushAndPutL0`, NOTE-VI-045) into the *same* `colHash("trace:id")`
colDir this flush path uses for `TraceGroup`-format files — producing two incompatible file
formats in one colDir, **today, for any tenant with `value_index_enabled=true`**.
`valueindexcompactor`'s format-dispatch branch (`valueindexcompactor/SPECS.md` SPEC-VI-7)
treats a decode failure on one input file as "skip, don't abort the whole merge, leave the
undecodable file in place" (SPEC-VI-7, NOTE-VI-066), so this does not crash the compactor or
corrupt other data — but the colliding `BucketGroup`-format file will never contribute to the
trace index and will accumulate indefinitely, un-mergeable, for as long as `WriteValueIndexL0`
lacks a `trace:id` exclusion. **This is an open, real, currently-unfixed gap as of 2026-07-05,
not a theoretical future risk** — flagged to the team for a code fix (add the same `trace:id`
exclusion `valueindexconsumer.ingest` already has, SPEC-VI-4) as a follow-up outside this
spec-oracle's own remit (spec docs only, not `.go` changes).

**Original assessment (2026-07-04, now known incorrect):** this caveat originally stated
`WriteValueIndexL0` had "zero production callers today," based on a repo-wide grep that only
checked `/home/mdurham/source/tempo-mrd` — a separate, apparently-inactive checkout of the same
`mattdurham/tempo` repository. The actual, live checkout,
`/home/mdurham/source/blockpack_collection/tempo`, was not checked at the time and does have
real callers, as corrected above. Preserved here, not deleted, per this repo's append-only
NOTES/SPECS correction convention, so the investigation gap itself is visible to future
readers.

Back-refs: `internal/modules/valueindexconsumer/service.go:ingest`,
`internal/modules/valueindexconsumer/traceflush.go` (`traceGroupBuffer`, `bufferTraceRow`,
`flushTraceGroups`, `readTraceGroups`, `traceGroupTimeRange`),
`internal/modules/valueindexconsumer/service.go:resolvePendingAcks`.

---

## SPEC-VI-3: `ColumnEntry.TraceID`/`ValueIndexEntry.TraceID` — every entry carries its row's TraceID
*Added: 2026-07-04*

**Contract:** every `ColumnEntry` yielded by `Extractor.Extract` (for every column, not only
`trace:id` itself) carries the `TraceID [16]byte` of the span row it describes, populated via
a `traceIDCol` lookup in root `extractBlockColumns` mirroring the existing
`spanIDCol`/`parentSpanIDCol` per-block lookups (SPEC-VI-1). Zero when the block lacks a
`trace:id` column entirely (legacy/old-format blocks) — same fallback shape as `SpanID`.

**This corrects a real, previously-silent bug, not a new feature:** `ColumnEntry.TraceID` and
root `ValueIndexEntry.TraceID` fields already existed before this fix but were **never
populated** by `extractBlockColumns` — every real production entry carried a zero `TraceID`,
system-wide, across every column, not just the trace-group path. This went undetected because
only hand-built test fixtures ever set `TraceID` directly on a literal `ValueIndexEntry`/
`ColumnEntry`; nothing exercised the actual extraction path's `TraceID` output before Stage 5's
integration test (`TestTraceIndexPipeline_ExtractFlushCompactQuery`) needed it to be genuinely
correct end-to-end. The trace-group buffer's per-row grouping (SPEC-VI-2) was silently broken
by this until Stage 5 caught and fixed it — every `TraceGroup` built before this fix would have
grouped every span under the same zero `TraceID`, an entirely wrong result. Confirmed fixed and
covered by `TestExtractValueIndexEntries_YieldsTraceID`.

**Caveat (code fix landed 2026-07-04) — one known caller previously did not use this field:**
root `WriteValueIndexL0` (`valueindex_l0write.go`, NOTE-VI-042) previously hardcoded a local
zero `traceID` rather than reading `e.TraceID` from the now-populated `ValueIndexEntry`. This
was initially flagged here as a stale-doc-comment-only issue, incorrectly reasoned to have "no
live query-path impact" on the (also incorrect, see SPEC-VI-2's own corrected caveat) premise
that the function had zero production callers. **Further investigation found two compounding,
corrected facts, not one:** (a) `SpanRef.TraceID` is a real dedup/merge key in
`bucketmerge.go`/`stream_compaction.go`'s compaction path, so a hardcoded zero caused distinct
traces sharing the same `(SourceRef, BlockRef)` to silently collide under one merge key,
losing span data on the first compaction pass; and (b) `WriteValueIndexL0` has real production
callers in `/home/mdurham/source/blockpack_collection/tempo` (SPEC-VI-2), gated behind
`value_index_enabled` — meaning **this was a live, currently-active data-loss bug for any
tenant with that flag enabled, not a dormant one that would only have mattered if a future
caller were wired up.** How much historical data loss actually occurred (which tenants had
`value_index_enabled=true`, for how long, prior to today's fix) is an operational question
outside this spec-oracle's ability to answer from source alone — flagged to the team for
follow-up. **Fixed** as of 2026-07-04: all three `AddEntry`/`AddEntryV2`/`AddEntryV4` call
sites now pass `e.TraceID`. See `NOTES.md` NOTE-VI-069 for the full mechanism and fix, and the
addendum to NOTE-VI-042.

Back-refs: root `valueindex_extract.go:extractBlockColumns` (the `traceIDCol` lookup),
`internal/modules/valueindexconsumer/consumer.go:ColumnEntry`,
`cmd/value-index-consumer/main.go:columnEntryFromValueIndexEntry`.

---

## SPEC-VI-4: `trace:id` is excluded from the standard per-column value-index buffering path
*Added: 2026-07-04*

**Contract:** `Service.ingest` (`service.go`) returns early for any `ColumnEntry` with
`ColName == shared.TraceIDColumnName`, **before** the `s.columns` allowlist check — `trace:id`
is never buffered into the standard per-column `columnBuffer`/`flushColumn` path, regardless of
whether an operator's `s.columns` configuration would otherwise include it.

**Rationale (necessary corollary to Finding 2 / `valueindexcompactor/NOTES.md` NOTE-VI-065):**
both the standard per-column path and the trace-group path (SPEC-VI-2) key their L0 files under
the *identical* `path.Join(tenant, indexPrefix, valueindex.ColHash("trace:id"),
valueindex.ColTypeName(ColumnTypeUUID), ...)` directory — SPEC-VI-2's design deliberately reuses
that exact convention so `DiscoverIndexFiles` needs no changes. But `valueindexcompactor`'s
format-dispatch (`internal/modules/valueindexcompactor/SPECS.md` SPEC-VI-7) treats that whole
colDir as 100% `TraceGroup` format once dispatch matches on it. Without this exclusion, a
standard `BucketGroup`-format `trace:id` L0 file (which `ingest` would otherwise happily write,
since the extraction layer is policy-free and indexes every column — NOTE-VI-027) would be
silently misrouted into `mergeTraceLevel`, fail `DecodeTraceGroups`, and be permanently stuck at
L0 (left in place per `mergeTraceLevel`'s corrupt-input-is-not-deleted contract, NOTE-VI-066 —
never cleaned up, never contributing to either index). **This exclusion is what prevents that
scenario from ever arising in the first place**, rather than relying on `mergeTraceLevel`'s
defensive corrupt-input handling to merely contain the damage after the fact.

**No user-visible cost:** no code anywhere queries `trace:id` via the standard value-index scan
path — the dedicated `TraceGroup` index (SPEC-VI-2) exists precisely to serve that query shape.
This exclusion removes indexing of a column that had no real consumer via the standard path
anyway.

Back-ref: `internal/modules/valueindexconsumer/service.go:ingest`. See
`valueindexcompactor/NOTES.md` NOTE-VI-065 (the format-dispatch ordering fix this is a
corollary to) and NOTE-VI-066 (what would otherwise silently absorb a misrouted file, containing
but not preventing the underlying problem).
