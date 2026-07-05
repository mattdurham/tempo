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

Next free ID: **SPEC-VC-3**.

---

## SPEC-VC-1: mergeLevel merge/dedup/net-accounting contract
*Added: 2026-07-02*

**Contract:** `(*Service).mergeLevel(ctx, colDir string, files []levelFile) error` merges one
compaction level's files into a single output at `level+1`:

1. Reads and decodes files one at a time via `store.Get` + `valuecounts.DecodeVCNTObject`
   (accepts either the self-describing `EncodeVCNTFile` format or the legacy single-chunk
   `EncodeRecords` format — see `valuecounts` SPEC-VC-2). Inputs are decoded one at a time and
   the raw compressed bytes go out of scope immediately after decode; `mergeLevel` never holds
   all inputs' raw bytes in memory simultaneously.
2. Accumulates all decoded records, then merges/sums/nets them in a single call to
   `valuecounts.Compact` — records are grouped by `(ColumnName, TimeStart, TimeEnd, Value)`,
   `Count` is summed per group, and any group whose summed `Count` is `<= 0` is dropped
   (`valuecounts.Compact`'s own retention rule, `valuecounts` NOTE-VC-001).
3. If `Compact` returns a non-empty result, writes it via `valuecounts.EncodeVCNTFile` (always
   the self-describing format for this package's own output — never `EncodeRecords`) to a fresh
   key at `level+1` (`valuecounts.FormatFilename(outputLevel, valuecounts.NewID())`). If
   `Compact` returns empty (every group net-`<=`-0), **no output file is written**.
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

Back-ref: `internal/modules/valuecountscompactor/service.go:278` (func `mergeLevel`),
`internal/modules/valuecountscompactor/service.go:370` (func `deleteWithRetry`).

---

## SPEC-VC-2: MaxRecordsPerMerge decoded-record-count admission gate
*Added: 2026-07-02*

**Contract:** `mergeLevel` bounds the number of decoded records it admits into a single merge
via `Config.MaxRecordsPerMerge` (default `DefaultMaxRecordsPerMerge = 3_000_000`), independent
of and complementary to `compactColumn`'s byte-based `CompactBatchBytes` admission gate.

**Rules:**

- `CompactBatchBytes` (`internal/modules/valuecountscompactor/service.go:232`, applied inside
  `compactColumn` before `mergeLevel` is ever called) bounds **compressed input bytes** selected
  into a batch — it is checked against `Object.Size` without decoding anything.
- `MaxRecordsPerMerge` (`internal/modules/valuecountscompactor/service.go:308`, applied inside
  `mergeLevel`'s per-file decode loop) bounds **decoded record count** directly, since record
  counts are not knowable from `Object.Size` alone — VCNT files can compress unusually well at
  small per-file sizes, so a byte-capped batch can admit far more decoded records than the byte
  number alone suggests.
- **Both gates must pass independently** — `CompactBatchBytes` selects the candidate file set in
  `compactColumn`; `MaxRecordsPerMerge` may still stop `mergeLevel` early within that set once
  the decoded-record ceiling is reached.
- **Progress floor:** the `MaxRecordsPerMerge` gate only takes effect once at least
  `CompactThresholdFiles` files have already been processed in the current merge — mirroring
  `compactColumn`'s own `CompactBatchBytes` "always include >= threshold" floor
  (`internal/modules/valuecountscompactor/service.go:238-240`). This guarantees a merge pass
  always makes forward progress even if the very first file already exceeds the ceiling alone.
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

Back-ref: `internal/modules/valuecountscompactor/service.go:278-312` (`mergeLevel`'s per-file
decode loop, gate check at line 308), `internal/modules/valuecountscompactor/service.go:203-245`
(`compactColumn`'s complementary `CompactBatchBytes` gate),
`internal/modules/valuecountscompactor/config.go` (`Config.MaxRecordsPerMerge`,
`DefaultMaxRecordsPerMerge`).
