# compaction — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/blockio/compaction` package.

---

## 1. Native Columnar Copy via AddRow
*Added: 2026-03-05*

**Decision:** `CompactBlocks` copies spans using `Writer.AddRow(block, rowIdx)` rather than
reconstructing OTLP objects and calling `Writer.AddTracesData`.

**Rationale:** Reconstructing OTLP proto objects from column values requires allocating
intermediate struct trees for every span — an O(spans) allocation cost. The native columnar
path copies column values directly from the source block into the destination writer without
any intermediate objects. For typical compaction workloads (millions of spans), this is
significantly more memory-efficient.

**Consequence:** The compaction package depends on `Writer.AddRow`, which requires that the
writer and reader agree on the column schema. New columns added to the writer are automatically
included; columns present in input files but unknown to the writer are silently dropped (this
is the writer's normal handling of unknown columns).

Back-ref: `internal/modules/blockio/compaction/compaction.go:addSpanFromBlock`

---

## 2. Deduplication Key is (trace:id, span:id)
*Added: 2026-03-05*

**Decision:** Spans are deduplicated using a 24-byte composite key: 16-byte `trace:id`
followed by 8-byte `span:id`.

**Rationale:** The combination of trace ID and span ID uniquely identifies a span within
the OpenTelemetry data model. Using both prevents false deduplication when spans from
different traces share the same span ID.

**Alternative considered:** Using a hash of all column values would detect content changes
but requires significantly more CPU and memory. For compaction (merge of redundant replicas),
identity-based deduplication is correct.

**Invariant:** Spans with invalid or missing IDs (non-16-byte trace:id, non-8-byte span:id)
are dropped and counted in `droppedSpans`. See SPECS.md §4.

Back-ref: `internal/modules/blockio/compaction/compaction.go:dedupeKey`

---

## 3. Staging Directory for Transactional Output
*Added: 2026-03-05*

**Decision:** Output files are written to a temporary staging directory and then pushed to
`OutputStorage` only after all content is written. The staging directory is always cleaned
up on return.

**Rationale:** Writing directly to `OutputStorage` (e.g. object storage) would require
either streaming writes (not supported by the `Writer` abstraction) or holding full file
contents in memory. The staging approach:
1. Allows `Writer.Flush` to produce complete, valid files before pushing.
2. Ensures no partial files are pushed if `Writer.Flush` fails.
3. Cleans up staging storage even on error.

**Consequence:** Disk space is required for staging. In environments with constrained local
storage, `Config.StagingDir` should point to a volume with sufficient capacity.

Back-ref: `internal/modules/blockio/compaction/compaction.go:prepareStagingDir`

---

## 4. Output File Rotation on Size Limit
*Added: 2026-03-05*

**Decision:** When `Config.MaxOutputFileSize > 0`, `CompactBlocks` checks
`Writer.CurrentSize()` after each span and rotates to a new output file when the estimate
exceeds the limit.

**Rationale:** Object storage systems (S3, GCS) have per-object size limits and performance
characteristics that favor objects in a bounded size range (typically 100MB–1GB for
sequential scan). Bounded output file sizes make the compaction output predictable.

**Note on estimate accuracy:** `Writer.CurrentSize()` returns an estimate based on
uncompressed column sizes. The actual compressed file may be smaller. The rotation
threshold is therefore a loose upper bound, not a hard limit.

Back-ref: `internal/modules/blockio/compaction/compaction.go:addSpanFromBlock`

---

## NOTE-37: Log File Re-sort by MinHash+Timestamp
*Added: 2026-03-18*

`CompactLogFile` globally re-sorts a log blockpack file by `(minHash[0..3], timestamp)`
to produce tight label-value boundaries per block. The MinHash signature is computed over
all `"key=value"` attribute pairs for each log record.

**Why minHash clustering:** Log files written by ingest have arbitrary row order within
each block. Without re-sorting, the per-block label ranges (min/max service_name, etc.)
span many different label values, making range index pruning ineffective — almost every
block passes the bloom/range check. After re-sorting, blocks contain log records with
similar label sets, so per-block label ranges are tight and queries for a specific
`service_name` can skip the majority of blocks.

**Why timestamp as secondary key:** Within each minHash cluster, timestamp ordering
preserves time-locality. This is important for log queries that filter by time range:
the per-block min/max timestamp bounds stay narrow, enabling block-level time pruning.

**Tradeoff:** The sort is in-memory and O(N) in total rows. `CompactLogFile` is not
suitable for arbitrarily large files; it is intended for moderate-size log segments.

Back-ref: `internal/modules/blockio/compaction/log_compaction.go:CompactLogFile`

---

## NOTE-459: Streaming compaction bounds peak memory to one input block
*Added: 2026-06-21*

`CompactBlocks` previously required the caller to pass every input provider already
materialized (`[]ReaderProvider`). The Tempo callsite downloaded all N input blocks
into memory in parallel before calling it, so peak input-side memory was
`sum(all blocks)` — at L1 ~730 MB/block and `max_input_blocks=4` that is ~3 GB,
causing OOMKills on 20 Gi workers and forcing `max_input_blocks: 2` as a workaround.

`CompactBlocksStreaming` takes `[]ProviderFunc` (lazy factories) instead. It invokes
each factory just-in-time, feeds that single block's spans into the output writer via
`processProvider`, then drops the provider reference before invoking the next factory.
Peak input-side memory is therefore `~max(largest single block)` regardless of input
count. `openAndProcess` is a separate function specifically so the opened provider does
not outlive a single loop iteration: it returns (and the block bytes become GC-eligible)
before the next factory runs.

The only state spanning all inputs is `seenSpans` — the dedup set of 24-byte
`(trace:id, span:id)` keys — which is far smaller than the raw block bytes. Raising the
input-block count grows the dedup set linearly in span count but does not scale peak
memory by block size, so `max_input_blocks` can be raised (8, 16+) without OOM risk.

**Trade-off:** parallel downloads are no longer possible on this path (blocks are
consumed sequentially). Memory safety was prioritized over download parallelism because
OOMKills abort the entire compaction job whereas serial downloads only lengthen it.

`CompactBlocks` is retained as a thin adapter: it wraps each pre-materialized provider in
a trivial factory and delegates to `CompactBlocksStreaming`, keeping one merge/dedup path.

Back-ref: `compaction.go:CompactBlocksStreaming`, `compaction.go:openAndProcess`,
`storage.go:CompactBlocksStreaming`

---

## NOTE-104 — dedupeKey block-column-first fallback removed as dead code (issue #490, task A-10/#104)

Date: 2026-07-07

**Addendum (2026-07-07, holistic-review Fix 2 — supersedes the "cleanly dropped via
droppedSpans" framing below):** the original landing of this task (recorded unchanged
further down) left the NOTE-469 legacy condition — a source block entirely lacking the
`trace:id` block column — as a silent per-row skip counted in `droppedSpans`. A holistic
cross-task review found this made `writer.go:AddRowFromReader`'s equivalent typed-error
guard for the identical condition structurally unreachable from compaction (its only
production caller always calls `dedupeKey` first, which drained every row of a legacy
block before `AddRowFromReader` was ever invoked), and that the public
`blockpack.CompactBlocks`/`CompactBlocksStreaming` wrappers discarded `droppedSpans`
entirely, so this "clean drop" was actually silent, unrecoverable data loss for any caller
of the sanctioned public API.

`dedupeKey`'s signature changed from `(key [24]byte, ok bool)` to
`(key [24]byte, err error)`. It now mirrors `AddRowFromReader`'s two-branch treatment of
the identical condition exactly:
- `trace:id` block column entirely absent → the shared greppable NOTE-469 error family
  (`"...legacy intrinsic-only source block unsupported...— re-compact"`, prefixed
  `"compaction: dedupeKey:"`), propagated up through `addSpanFromBlock` and aborting
  `CompactBlocks`/`CompactBlocksStreaming`.
- `trace:id`/`span:id` column present but this row's value absent/malformed → a distinct
  row-scoped error (`"trace:id missing or malformed at row %d"` /
  `"span:id missing or malformed at row %d"`).

`droppedSpans` now counts **only** the genuine-duplicate case in `addSpanFromBlock` (the
`(trace:id, span:id)` pair was already seen) — never legacy-input or malformed-row loss.
See `SPECS.md` §4 for the full three-case breakdown. `storage.go`'s public
`CompactBlocks`/`CompactBlocksStreaming` wrappers now also return this count instead of
discarding it (holistic-review Fix 3).

Back-refs (addendum): `compaction.go:dedupeKey`, `compaction.go:addSpanFromBlock`,
`storage.go:CompactBlocks`, `storage.go:CompactBlocksStreaming`. Tests:
`dedupekey_test.go:TestDedupeKey_MissingIdentityColumns_ErrorsOnLegacyIntrinsicOnlySourceBlock`,
`TestDedupeKey_MalformedRowTraceID_ErrorsDistinctlyFromLegacyBlock`,
`TestDedupeKey_MissingSpanIDColumn_Errors`,
`compaction_test.go:TestCompactBlocks_MalformedRowIdentity_ReturnsTypedError`,
`TestCompactBlocks_NativeColumns_Dedup` (now also asserts `droppedSpans == 1`).

**Reframing note: this landed as a dead-code deletion, not a typed-error conversion** — the
original task description expected `dedupeKey`'s `idIndex`-based fallback to need converting to
a typed error for a reachable legacy case. Investigation confirmed `buildDedupeIndex` had
unconditionally returned `nil` since #433/#434/#436 (its own doc comment already said so
accurately) — the fallback was provably, permanently unreachable by construction, not merely
"assumed migrated" or "legacy-data reachable." Deleted outright instead: `buildDedupeIndex()`,
the `blockIDPair` type (`blockidpair.go`, whole file removed), and the `idIndex` parameter
threaded through `dedupeKey`/`processBlock`/`addSpanFromBlock`. `dedupeKey` now reads
`trace:id`/`span:id` exclusively from block columns, unconditionally.

The equivalent `writer/writer.go:AddRowFromReader` site (originally scoped to this task) was
instead completed as a direct consequence of task A-9/#103's fix (same underlying
`buildIntrinsicBlockIndex` root cause, two call sites) — see `blockio/writer/NOTES.md` NOTE-469's
addendum. `executor/executor.go:SpanMatchFromRow` was already clean (NOTE-436).
`executor/metrics_trace.go` was re-verified via broad grep and confirmed to have no
identity-dedup pattern at all — not a target.

**Separately, unrelated to the above (a wholly different dead config knob, not part of the
identity-mechanism confusion this note's main entry addresses):** `compaction.go:334-337`'s
`Config.OmitIntrinsicIdentityColumns` threading was deleted (task A-12/#106) along with the
`Config` field itself and its counterparts at `compaction/config.go:28-33`/
`writer/config.go:173-178` — already non-functional per its own docs, SpanTree is the sole
identity store for every writer output unconditionally. See `blockio/writer/NOTES.md` NOTE-476's
addendum for the full detail; this is recorded there (not here) since NOTE-476 is that
change's home note.

Back-refs: `internal/modules/blockio/compaction/compaction.go:dedupeKey` (deleted
`buildDedupeIndex`, simplified signature), deleted `blockidpair.go`. Test:
`dedupekey_test.go:TestDedupeKey_BlockColumnsOnly`,
`TestDedupeKey_MissingIdentityColumns_ErrorsOnLegacyIntrinsicOnlySourceBlock` (using
`reader.BuildSyntheticIdentityBlock` and `reader.BuildBlockMissingIdentityColumns`
respectively). **Correction (see the 2026-07-07 addendum above):** this originally claimed
the latter test confirms a genuinely legacy-shaped block is "cleanly dropped via the
existing `droppedSpans` counter, not silently mishandled" — that was true only in
isolation, not for compaction's only production caller graph, and not for the public
`storage.go` wrappers. As of the addendum, this case is a typed error (the shared NOTE-469
family), not a `droppedSpans` increment; see `SPECS.md` §4 for the corrected semantics.
See `.bob/state/identity-investigation.md` for the full empirical basis (same
investigation as writer NOTE-469/NOTE-V2-004 addenda and executor NOTE-VI-080).
