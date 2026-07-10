# valuecountscompactor — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/valuecountscompactor` package. It complements `NOTES.md` (design rationale)
and `TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecounts/SPECS.md`'s own separate `SPEC-VC-N` sequence — per spec-oracle's
ruling matching the established `valueindex/SPECS.md`/`valueindexcompactor/SPECS.md` precedent
of sharing the `SPEC-VI-N` prefix with independent per-file counters). IDs are assigned in
ascending order and never reused or renumbered; superseded entries are marked
`[SUPERSEDED by SPEC-VC-N]` rather than deleted.

Next free ID: **SPEC-VC-4**.

---

## SPEC-VC-1: mergeLevel merge/dedup/net-accounting contract
*Added: 2026-07-02*

**Contract:** `(*Service).mergeLevel(ctx, colDir string, files []levelFile) error` merges one
compaction level's files into a single output at `level+1`:

1. Reads and decodes files one at a time via `store.Get` + `valuecounts.DecodeVCNTObject` (the
   self-describing `EncodeVCNTFile` format — the sole decode path since `valuecounts` SPEC-VC-4).
   Inputs are decoded one at a time and the raw compressed bytes go out of scope immediately
   after decode; `mergeLevel` never holds all inputs' raw bytes in memory simultaneously.
2. Accumulates all decoded records, then merges/sums/nets them in a single call to
   `valuecounts.Compact` — records are grouped by `(ColumnName, TimeStart, TimeEnd, Value)`,
   `Count` is summed per group, and any group whose summed `Count` is `<= 0` is dropped
   (`valuecounts.Compact`'s own retention rule, `valuecounts` NOTE-VC-001).
3. If `Compact` returns a non-empty result, writes it via `valuecounts.EncodeVCNTFile` (always
   the self-describing format for this package's own output — never `EncodeRecords`) to a fresh
   key at `level+1`. **[Updated by SPEC-VC-3, issue #494, 2026-07-10]** The output filename is
   now always `valuecounts.FormatFilenameV2(outputLevel, minSec, maxSec, valuecounts.NewID())`,
   where `(minSec, maxSec)` is the genuine range returned by `valuecounts.TimeRange(merged)` over
   the just-`Compact`-ed output — never a copy of one input's range or an assumed/last-sorted
   value. See SPEC-VC-3 for the full v2-filename-selection contract this pairs with on the input
   side. If `Compact` returns empty (every group net-`<=`-0), **no output file is written**.
4. Deletes only the inputs actually processed (`processed`, not necessarily all of `files` —
   see SPEC-VC-2), and only after the output `Put` (if any) has succeeded.

**Rules:**

- **Write-then-delete crash safety:** input keys are deleted only after the merged output's
  `Put` succeeds (when there is output to write). A `Put` failure returns immediately with no
  deletes attempted, leaving every input untouched for the next pass to retry.
- **No retention/existence-probe step:** unlike `valueindexcompactor.Service.mergeLevel`, this
  function does not consult any `SourceExister`-equivalent. `valuecounts.Compact`'s net-sum-
  `<=`-0 rule is the sole retention signal (see `valuecountscompactor` NOTE-VC-007).
- Any `store.Get` or `valuecounts.DecodeVCNTObject` error aborts the merge immediately (returns
  the wrapped error); no partial output is written, no inputs are deleted.
- A `store.Delete` failure on one input does not abort deletion of the remaining processed
  inputs. Each failing `Delete` is retried up to `deleteMaxAttempts` (3) times via the
  `deleteWithRetry` helper, with a fixed `deleteRetryBackoff` (20ms) pause between attempts,
  before that key is given up on; the first delete error encountered (after its own retries are
  exhausted) is returned to the caller after all inputs' deletes have been attempted.
- **Known limitation — partial-delete double-count risk (NOTE-VC-009):** unlike
  `valueindexcompactor` (safe against a partial `Delete` failure because `valueindex.CompactFiles`
  dedups by identity), `valuecounts.Compact` sums `Count` with no identity field on `Record`. If a
  `Delete` still fails after exhausting retries, that surviving input can be summed a *second*
  time by a future merge, permanently double-counting its value. This is mitigated but not
  eliminated by the retry above; an exhausted-retry failure is surfaced via both the returned
  error and the dedicated
  `blockpack_value_count_compactor_merge_delete_failed_after_retry_total` metric, which operators
  must alert on and reconcile manually. This is a documented residual risk, not a design
  guarantee — see NOTE-VC-009 for the full analysis.

**Rationale:** Mirrors `valueindexcompactor.Service.mergeLevel`'s streaming, write-then-delete
design (see `valueindexcompactor` SPEC-VI-1/NOTE-VI-046) at the level of overall shape, adapted
for VCNT's simpler (no external source-existence check) retention model. The retry mitigation is
this package's own addition, not ported from VI, since VI does not need it (see NOTE-VC-009).

Back-ref: `internal/modules/valuecountscompactor/service.go:310` (func `mergeLevel`),
`internal/modules/valuecountscompactor/service.go:414` (func `deleteWithRetry`).

---

## SPEC-VC-2: MaxRecordsPerMerge decoded-record-count admission gate
*Added: 2026-07-02*

**Contract:** `mergeLevel` bounds the number of decoded records it admits into a single merge
via `Config.MaxRecordsPerMerge` (default `DefaultMaxRecordsPerMerge = 3_000_000`), independent
of and complementary to `compactColumn`'s byte-based `CompactBatchBytes` admission gate.

**Rules:**

- `CompactBatchBytes` (applied inside `compactColumn`'s `capBatchBytes` helper, before
  `mergeLevel` is ever called) bounds **compressed input bytes** selected into a batch — it is
  checked against `Object.Size` without decoding anything.
- `MaxRecordsPerMerge` (applied inside `mergeLevel`'s per-file decode loop) bounds **decoded
  record count** directly, since record counts are not knowable from `Object.Size` alone — VCNT
  files can compress unusually well at small per-file sizes, so a byte-capped batch can admit
  far more decoded records than the byte number alone suggests.
- **Both gates must pass independently** — `CompactBatchBytes` selects the candidate file set in
  `compactColumn`; `MaxRecordsPerMerge` may still stop `mergeLevel` early within that set once
  the decoded-record ceiling is reached.
- **Progress floor:** the `MaxRecordsPerMerge` gate only takes effect once at least
  `CompactThresholdFiles` files have already been processed in the current merge — mirroring
  `compactColumn`'s own `CompactBatchBytes` "always include >= threshold" floor. This guarantees
  a merge pass always makes forward progress even if the very first file already exceeds the
  ceiling alone. **[Updated, issue #494, 2026-07-10]** As of SPEC-VC-3's clustering rewrite, both
  progress floors are now evaluated against the WINNING CLUSTER's file count, not the level's
  raw total — see SPEC-VC-3.
- **Deferred, never dropped or corrupted:** files left unprocessed once the ceiling is reached
  are excluded from `processed` entirely — they are not read, not included in the merge, and
  not deleted. They remain in place for a subsequent pass to pick up. The deferred count is
  reported via the `blockpack_value_count_compactor_merge_deferred_files_total` counter
  (`incDeferred`).

**Rationale:** `CompactBatchBytes` alone caps compressed bytes, not decoded record count. Weak
compression at VCNT's small per-file sizes (low-cardinality columns, short values) can let a
byte-capped batch admit far more records than the byte number suggests — a real risk of
decode-time memory blowup that a compressed-byte budget alone does not catch (see
`valuecountscompactor` NOTE-VC-005 in `valuecounts/NOTES.md`, and the ~300B RSS/record sizing
estimate behind `DefaultMaxRecordsPerMerge`).

Back-ref: `internal/modules/valuecountscompactor/service.go` (`mergeLevel`'s per-file decode
loop and its `MaxRecordsPerMerge` gate check, `compactColumn`'s complementary `CompactBatchBytes`
gate via `capBatchBytes`), `internal/modules/valuecountscompactor/config.go`
(`Config.MaxRecordsPerMerge`, `DefaultMaxRecordsPerMerge`).

---

## SPEC-VC-3: Time-cluster-based file selection — clusterByTimeRange / pickCluster / v2 output filenames
*Added: 2026-07-10*

**Contract:** `compactColumn` selects merge candidates for one column directory by clustering
same-level files on their wall-clock time range (parsed from the v2 filename, no decode
required) rather than treating every same-level file as one undifferentiated pool, so that
compaction actually merges files whose time ranges overlap or sit close together (issue #494).

**Selection pipeline (`compactColumn`):**

1. Every object's filename is parsed via `valuecounts.ParseFilenameV2`. **A parse failure —
   including any straggler v1-format file (see NOTES.md's new dated entry) — increments the
   `filesSkipped` metric and leaves the file untouched in place.** There is no v1 fallback; this
   is the sole mechanism by which a legitimate old-format file is permanently, safely skipped
   rather than corrupted or force-migrated.
2. Successfully-parsed files are grouped by `Level` (`byLevel map[int][]levelFile]`, where
   `levelFile.minSec`/`maxSec` are populated straight from the parsed `FileMeta`), and levels are
   visited in ascending order — same single-pass-per-level discipline as before this change.
3. For each level, `clusterByTimeRange(byLevel[lvl], Config.MaxTimeSpanPerMerge)` greedily walks
   the level's files (pre-sorted by `(minSec ASC, maxSec ASC, key ASC)` for determinism) and
   starts a new cluster whenever admitting the next file would push the running cluster's
   `[minSec, maxSec]` span past `maxSpan`. `maxSpan == 0` means "no cap" (always exactly one
   cluster) as a pure-function property; in production, `Config.MaxTimeSpanPerMerge` is never
   left at `0` after `withDefaults()` runs (`0` there means "apply
   `DefaultMaxTimeSpanPerMerge`", the same "`<=0`/`0` means default" convention every other
   numeric knob in `Config` uses — NOT "disable the cap"). **No file is ever split across two
   clusters** — every input file appears in exactly one output cluster.
4. `pickCluster(clusters)` selects exactly one cluster to hand to `mergeLevel`: **the cluster
   with the most files wins; ties are broken by the smallest (earliest) `minSec` across the
   tied clusters** (Design Decision 1). This is a fixed, deterministic tie-break — it does not
   consider total byte size, cluster span width, or file recency.
5. **Threshold-gate generalization:** the chosen cluster's own file count — not the level's raw
   total file count — must meet `Config.CompactThresholdFiles`, or that level is skipped entirely
   (the loop moves to the next level, if any). A level whose files are split across several
   disjoint clusters, none of which individually meets the threshold, performs no merge that
   pass even if the level's combined total across all clusters would have met it.
6. The chosen cluster is then passed through the existing `capBatchBytes` `CompactBatchBytes`
   trim (SPEC-VC-2) — applied WITHIN the chosen cluster only; a disjoint cluster's files are
   never touched by this trim regardless of the cap — before being handed to `mergeLevel`.

**`clusterByTimeRange(files []levelFile, maxSpan uint64) [][]levelFile` contract:**

- Deterministic given its input: sorts a COPY of `files` (never mutates the caller's slice) by
  `(minSec ASC, maxSec ASC, key ASC)`, so re-running on a shuffled-order input with identical
  file data produces byte-identical cluster output — the `key` tiebreak exists specifically to
  make ties on `(minSec, maxSec)` deterministic.
- The `candMax - candMin` span-width subtraction is guarded (`candMax > candMin &&
  candMax-candMin > maxSpan`) rather than assumed safe — a `levelFile` with `minSec > maxSec`
  (a reversed range) cannot occur via `valuecounts.ParseFilenameV2` in production (SPEC-VC-7 in
  `valuecounts/SPECS.md` rejects it at parse time) but `clusterByTimeRange` is a pure function
  that must not silently underflow its own `uint64` arithmetic if some future caller constructs
  a `levelFile` by another path (task #100, 2026-07-10 finding — see NOTES.md's new dated entry
  for the full history).
- `maxSpan == 0` is a pure-function-only "no cap" behavior (always exactly one cluster); no
  production caller can reach this since `Config.withDefaults()` never leaves
  `MaxTimeSpanPerMerge` at `0`.

**`pickCluster(clusters [][]levelFile) []levelFile` contract:** returns the cluster with
`len(cluster)` maximal; ties broken by the smallest `minSec` across the tied clusters
(`clusterMinSec`, computed defensively via its own scan rather than assuming its input is
pre-sorted by `minSec`, even though `clusterByTimeRange`'s own output always is). Returns `nil`
for empty input.

**Rationale:** See NOTES.md's new dated entry for the full design rationale — why input-side,
pre-decode clustering (this design) was chosen over a previously-considered post-decode
partitioning approach, and why the tie-break rule is "most files, then earliest start" rather
than some other ordering.

Back-refs: `internal/modules/valuecountscompactor/cluster.go` (`clusterByTimeRange`,
`pickCluster`, `clusterMinSec`), `internal/modules/valuecountscompactor/service.go`
(`compactColumn`, `levelFile`, `capBatchBytes`), `internal/modules/valuecountscompactor/config.go`
(`Config.MaxTimeSpanPerMerge`, `DefaultMaxTimeSpanPerMerge`). `valuecounts/SPECS.md` SPEC-VC-7
(the v2 filename format this clustering is built on). Tests: `cluster_test.go`, and the new
`compactColumn`/`mergeLevel` tests in `service_internal_test.go`/`service_test.go` — see TESTS.md
TEST-VC-25 through TEST-VC-29.
