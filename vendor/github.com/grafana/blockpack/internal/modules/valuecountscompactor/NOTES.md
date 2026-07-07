# valuecountscompactor — Design Notes

Non-obvious design decisions, rationale, and invariants for the
`internal/modules/valuecountscompactor` package (VCNT value-counts compactor service).

## ID convention

Entries in this file use the sequential prefix `NOTE-VC-N`. Unlike this module's `SPECS.md`/
`TESTS.md`/`BENCHMARKS.md` (each of which has its own independent, module-local counter), this
prefix is a **shared/global counter across both `internal/modules/valuecounts/NOTES.md` and
this file** — mirroring the value-index pipeline's shared `NOTE-VI-N` numbering across
blockevents/valueindex/valueindexconsumer/valueindexcompactor/vibuilder. See spec-oracle's
ruling (2026-07-02): `valuecounts` and `valuecountscompactor` are the analogous
core-format/compactor-service pair for VCNT that `valueindex`/`valueindexcompactor` are for VI.

`valuecounts/NOTES.md` currently holds NOTE-VC-001 through NOTE-VC-006, NOTE-VC-008, and
NOTE-VC-015 (NOTE-VC-007 was left explicitly reserved for this file — see the inline note in
`valuecounts/NOTES.md` at that point in the sequence). This file also holds NOTE-VC-009 and
NOTE-VC-010. Next free ID: **NOTE-VC-011**.

---

## NOTE-VC-007 — VCNT compactor design decisions distinct from the valueindexcompactor precedent

Date: 2026-07-02

This compactor's overall shape mirrors `valueindexcompactor.Service` (see that module's
NOTE-VI-017), but several design points are deliberately different because VCNT's retention
model and key layout are simpler than the value-index pipeline's. Recorded here rather than
re-deriving from the code on every review:

**Why no `SourceExister`:** `valueindexcompactor` probes `SourceExister.Exists` per unique
`SourceRef` to drop entries whose originating blockpack file has been deleted by retention.
VCNT records carry no such source pointer. Retention/liveness for VCNT is entirely expressed by
`valuecounts.Compact`'s own net-sum-`<=`-0 rule (`valuecounts` NOTE-VC-001): a value's positive
introduction is canceled by a later negative retention delta, and once every file that ever
introduced a value has been superseded or retention-deleted upstream, its net count reaches
zero and `Compact` drops it — no separate existence probe is needed or possible from this
package's data alone. `Store` (this module's storage interface) accordingly has no
`SourceExister` equivalent and no `Peek` method (VCNT files carry no magic header to sniff,
unlike VI's bucket-file format) — see `store.go`'s own doc comment.

**One-level-shallower directory walk than VI:** VCNT's key layout is
`<tenant>/<indexPrefix>/unique_values/<colHash>/L<level>-<id>.vcnt` — there is no `<type>`
path segment. `buildWorkList` therefore does a single `ListDirs` call under
`<tenant>/<indexPrefix>/unique_values/` to enumerate column-hash directories directly, instead
of VI's three-level (tenant → colHash → type) walk (`valueindexcompactor` NOTE-VI-017-b).
`Store.ListDirs` still exists and is required for this — the walk is shallower, not absent.

**The `ColHash` cross-module coupling risk (brainstorm Risk 3):** `valuecounts.ColHash` and
`valueindex.ColHash` independently implement identical SHA-256[:16]-hex construction, with no
compile-time link between the two packages. `ownsShard`'s sharding decision (which replica owns
a given column) depends on both packages producing byte-identical hashes for the same column
name — if either package's hash construction ever drifts, sharding assumptions silently break
with no compiler error. `TestOwnsShard_MatchesValueIndexConvention` (TEST-VC-8) pins this
cross-module equality directly so future drift in either package fails a test instead of
silently breaking co-location/sharding.

**The deliberately-omitted `MaxOutputBytes` field:** `valueindexcompactor.Config` carries a
`MaxOutputBytes` knob that is dead code on its current (v2 BucketGroup) write path. This
module's `Config` intentionally does not carry that field forward at all — per
brainstorm.md's Implementation Strategy step 6, there is no reason to port forward a
config knob that has no live effect in the module it was copied from.

**Compact does not report per-group retained/dropped counts:** unlike
`valueindexcompactor.CompactFiles`, which returns `CompactStats{Retained, Dropped}` from its
retention `Checker`, `valuecounts.Compact` returns only the merged, already-filtered record
slice — it does not report how many groups were dropped by the net-`<=`-0 rule versus retained.
`compactorMetrics.recordsRead`/`recordsWritten` therefore count total decoded input records vs.
merged output records (a coarser signal than a retained/dropped split) — see `metrics.go`. If a
retained/dropped breakdown is ever needed for VCNT (e.g. to alert on unexpectedly high churn),
`valuecounts.Compact`'s signature would need to grow a stats-return value, mirroring
`valueindexcompactor`'s `CompactStats` precedent.

**Open question — unmet precondition, not an assumed fact:** the delta-accounting mechanism
this compactor's net-sum-`<=`-0 rule depends on (`valuecounts` NOTE-VC-001: a source block's
deletion is expressed as a negative-count record via `valuecounts.Negate`) has **no producer
wired up anywhere in this codebase today**. Nothing calls `valuecounts.Negate` or publishes
retention-delta records for VCNT. This compactor is therefore correct for merging and dropping
already-net-negative input (as exercised by its tests), but the actual retention-triggering
event — something calling `Negate` when a source block is deleted — does not exist yet. Flag
this explicitly to whoever wires up the VCNT publish path: until a producer exists, VCNT counts
will only ever grow, never self-heal under retention, in production.

Back-ref: `internal/modules/valuecountscompactor/service.go` (`mergeLevel`, `buildWorkList`,
`ownsShard`, `compactColumn`), `internal/modules/valuecountscompactor/store.go` (`Store`),
`internal/modules/valuecountscompactor/config.go` (`Config`),
`internal/modules/valuecountscompactor/metrics.go` (`compactorMetrics`),
`internal/modules/valuecountscompactor/service_internal_test.go:TestOwnsShard_MatchesValueIndexConvention`.

---

## NOTE-VC-009 — Partial-delete-failure double-count risk in mergeLevel, and the retry mitigation

Date: 2026-07-02

**The gap:** NOTE-VC-007 and `doc.go` originally claimed this module's write-then-delete
crash-safety pattern was "identical in spirit" to `valueindexcompactor`'s and that "multiple
instances are safe." A 2026-07-02 go-presubmit review (CRITICAL) found this overstated:
`valueindexcompactor`'s write-then-delete is safe against a *partial* `Delete` failure (some
inputs deleted, some not, in the same batch) because `valueindex.CompactFiles` dedups by identity
— reprocessing a surviving un-deleted input in a future merge is a no-op. `valuecounts.Record` has
no identity field at all (`ColumnName, Value, TimeStart, TimeEnd, Count`), and `Compact` SUMS
`Count` per merge key rather than deduping. If `store.Delete` fails for even one input in a batch
while the merged output `Put` already succeeded and the rest of the batch's deletes succeed, that
surviving input stays at its old level with no marker that it was already merged — the next time
any future pass sweeps it into a merge, its `Count` contribution is summed AGAIN, permanently and
silently double-counting that value's cardinality/TopN contribution, with no self-healing path. A
full crash before any delete, or a total delete failure, still degrades gracefully (same as VI —
nothing was consumed, the whole batch is retried next pass); it is specifically the "some
succeeded, some didn't" case within one `mergeLevel` call that is unsafe.

**Mitigation implemented (pragmatic, not a full identity-based rearchitecture):**

1. `mergeLevel`'s delete loop now retries each failed `Delete` up to `deleteMaxAttempts` (3)
   times, via the `deleteWithRetry` helper, with a fixed short backoff (`deleteRetryBackoff`,
   20ms) between attempts, before giving up on that key. This closes the window for the vast
   majority of real transient object-storage failures (throttling, transient 5xx, timeout)
   without requiring a design change.
2. If a delete still fails after retries, this is surfaced two ways: the error is returned via
   `mergeLevel`'s existing `firstErr` path (not swallowed), and a dedicated counter,
   `blockpack_value_count_compactor_merge_delete_failed_after_retry_total`
   (`compactorMetrics.mergeDeleteFailedAfterRetry`), is incremented so operators can alert on it
   and manually reconcile.
3. This reduces but does NOT eliminate the residual risk — a sufficiently persistent
   object-storage outage (outlasting 3 short retries) still leaves a double-counting exposure.
   This is a known, documented limitation, not a design guarantee, following the same pattern
   already used in this module for the unwired-`Negate` gap (NOTE-VC-007's "open question" —
   flagged explicitly, not silently assumed away).

**Why not a full fix in this pass:** a real fix requires either (a) treating any `Delete` failure
in a batch as making the whole merge's survivors unsafe to re-admit until confirmed removed, or
(b) giving merged records a provenance/manifest of consumed input keys checked before a survivor
is re-merged. Both are a larger design change than this pass's scope; the retry+visibility
mitigation above is the accepted minimum bar for this iteration. Revisit if the
`merge_delete_failed_after_retry_total` metric is ever observed non-zero in production.

`doc.go`'s "Stateless … multiple instances are safe" bullet has been corrected to state this
caveat explicitly rather than the blanket "identical in spirit"/"multiple instances are safe"
claim; `valuecounts/NOTES.md` NOTE-VC-005's analogous bullet has been corrected the same way.

Regression-tested by `TestMergeLevel_PartialDeleteFailure_RetriesThenSucceeds` (retry recovers
within budget) and `TestMergeLevel_PartialDeleteFailure_ExhaustsRetriesAndReportsMetric` (retry
exhausts, error surfaces, metric increments, partial success is not rolled back) — see
TEST-VC-21/TEST-VC-22.

Back-ref: `internal/modules/valuecountscompactor/service.go` (`mergeLevel`, `deleteWithRetry`,
`deleteMaxAttempts`, `deleteRetryBackoff`), `internal/modules/valuecountscompactor/metrics.go`
(`compactorMetrics.mergeDeleteFailedAfterRetry`, `incDeleteFailedAfterRetry`),
`internal/modules/valuecountscompactor/doc.go` (corrected "multiple instances are safe" bullet).

---

## NOTE-VC-010 — Test fixture helpers were writing legacy EncodeRecords format; fixed to EncodeVCNTFile (issue #490, task A-3/#110 fallout)

Date: 2026-07-07

**The bug:** `service_test.go`'s `putL0`/`putL0Multi` and `service_internal_test.go`'s `putVCNT`
test helpers wrote fixture files using `valuecounts.EncodeRecords` (the plain, non-self-describing
wire format), with comments explicitly stating this simulated "today's `vcntwriter.go` write
path." This was accurate when written (2026-07-02) but became stale once task A-Tempo-1/#111
(tempo-mrd, out of this repo's scope) shipped: `vcntwriter.go` now writes self-describing
`EncodeVCNTFile` output unconditionally, so no real production write path produces the
`EncodeRecords` shape anymore.

The staleness became a real, silent test-suite bug once `valuecounts`'s own legacy decode
fallback (`DecodeLegacyVCNTFile`/`DecodeVCNTObject`'s dispatch) was removed under task A-3/#110
(see `valuecounts/NOTES.md` NOTE-VC-005's addendum): with the fallback gone, `EncodeRecords`-shaped
fixtures became flatly undecodable. This broke `TestRunOnce_MultipleTenantsAndColumns` and, more
seriously, `TestRunOnce_UndecodableInputQuarantinedNotBlocking` — the latter's fixtures were ALL
undecodable (not just its one deliberately-corrupt input), so it was silently passing for the
wrong reason: it looked like it was testing "one corrupt input among valid ones is quarantined
without blocking the rest," but every input was actually being quarantined, masking a bug in the
quarantine test's own blast radius.

**Fix:** all three helpers (`putL0`, `putL0Multi`, `putVCNT`) switched to
`valuecounts.EncodeVCNTFile`; their doc comments updated to describe the current (self-describing)
write path instead of the retired one.

**General lesson (see `valuecounts/NOTES.md` NOTE-VC-015 for the cross-reference to a second,
independent occurrence of the same bug class in tempo's own test suite):** a test-fixture helper
that hand-constructs a wire format can silently drift out of sync with the real write path it
claims to simulate, and that drift is invisible until something downstream (here, a legacy decode
fallback's removal) stops tolerating the stale shape. Fixture-generation helpers that hand-build a
wire format should be treated as a first-class audit target whenever the corresponding write or
decode path changes, not just the production code paths themselves.

Back-refs: `internal/modules/valuecountscompactor/service_test.go` (`putL0`, `putL0Multi`),
`internal/modules/valuecountscompactor/service_internal_test.go` (`putVCNT`). See
`internal/modules/valuecounts/NOTES.md` NOTE-VC-005 (addendum), NOTE-VC-015; `SPECS.md` SPEC-VC-4.
