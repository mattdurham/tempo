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

`valuecounts/NOTES.md` currently holds NOTE-VC-001 through NOTE-VC-006, NOTE-VC-008,
NOTE-VC-011 through NOTE-VC-017, and (as of 2026-07-13, task #216) NOTE-VC-020 (NOTE-VC-007
was left explicitly reserved for this file — see the inline note in `valuecounts/NOTES.md` at
that point in the sequence). This file also holds NOTE-VC-009, NOTE-VC-010, NOTE-VC-018, and
(as of 2026-07-13, task #216) NOTE-VC-019 and NOTE-VC-021. Next free ID: **NOTE-VC-022**.

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

## NOTE-VC-018 — Time-cluster-based compaction: design rationale, tie-break, deployment prerequisite (issue #494)

Date: 2026-07-10

### Why input-side, pre-decode clustering — and why the previously-considered post-decode approach was superseded

Issue #494 asked for compaction to actually merge files whose time ranges overlap or sit close
together, not just any same-level files. An earlier brainstorm considered a post-decode
partitioning approach: decode every same-level candidate file first, then partition the
resulting records by time range before writing outputs. That design existed specifically to
cope with a permanent-unknown-range case — VCNT filenames carried no time information at all
(v1 shape), so there was no way to know a file's range without decoding it, and V1 files with
no way to acquire a range would need to stay in that "unknown, must-decode-to-find-out" state
indefinitely.

Two decisions eliminated that permanent-unknown-range case and made input-side (pre-decode)
clustering both simpler and a more literal fulfillment of "merge files with overlapping/similar
time ranges": (1) v2 filenames (`valuecounts.FormatFilenameV2`/`ParseFilenameV2`, this blockpack
change) embed the genuine range directly in the object key, so a candidate's range is knowable
in O(1) without any decode; and (2) the mandatory full VCNT data wipe (see the deployment
prerequisite below) retires every pre-existing v1 file, so there is no long-lived population of
genuinely-unknown-range files this design needs to accommodate — any v1 straggler that survives
the wipe is a narrow, temporary, self-limiting edge case (see below), not the steady-state this
compactor must be designed around. Once the "permanently unknown range" case is off the table,
clustering on the already-known, already-cheap filename-embedded range before ever touching
object storage for a `Get` is strictly simpler than decoding first and partitioning after: it
lets `compactColumn` decide which files to merge, and how many `mergeLevel` calls a level needs,
without any decode at all for files that end up in a losing (unmerged) cluster.

### The tie-break rule: most files wins, ties broken by earliest start

`pickCluster` selects the cluster with the most files; ties are broken by the smallest (earliest)
`minSec` across the tied clusters. Rationale: the primary goal of compaction is reducing file
count (fewer, larger files), so all else equal the cluster that reduces file count the most
should be preferred. The earliest-start tie-break is a secondary, deterministic preference for
processing older data first — older clusters are more likely to represent columns that have
stopped receiving new same-range writes and are therefore "settled" (unlikely to gain more
files that would have merged even more advantageously later), whereas a very recent cluster may
still be actively accumulating new L0 files from an in-progress ingest window. Neither factor
(byte size, cluster span width, file recency beyond this tie-break) is considered — this is a
deliberately simple two-factor rule, not a full cost model.

### Known limitation — potential cluster starvation under skewed, continuous ingest

The "most files wins" tie-break has no age/staleness factor beyond the same-count tie-break
above. Under a continuous, sufficiently skewed ingest pattern (e.g. one hot time range
perpetually accumulating new L0 files faster than a level's other, older/smaller clusters can
grow), it is plausible — though not proven or demonstrated by any current test — that a level
could keep favoring the largest cluster indefinitely, leaving a smaller, older cluster
perpetually short of `CompactThresholdFiles` and never selected. This is a known, accepted
design tradeoff for this iteration, not a bug: `pickCluster` is deliberately a simple two-factor
rule (SPEC-VC-3), not a full cost model, and the scenario requires a specific, sustained ingest
skew to manifest at all. If this is ever observed in production (e.g. via a persistently
low-level-count metric for a specific column combined with growing file counts), the fix would
likely add an age/staleness factor to `pickCluster`'s selection (e.g. preferring a cluster whose
oldest file exceeds some age threshold, independent of file count) — flagged here as a future
consideration, not committed to in this design.

### MaxTimeSpanPerMerge's 86400s default is a tuning placeholder

`DefaultMaxTimeSpanPerMerge = 86400` (24h) bounds how wide a single cluster's `[minSec, maxSec]`
span may grow before `clusterByTimeRange` starts a new cluster. This value is a conservative
starting default, not a tuned production number — real tuning data from production
query-window/retention observation does not exist yet at the time this shipped. Widening it
merges more files per pass (fewer total files, more decode work per merge); narrowing it does
the opposite. Revisit once production file-count/cluster-width telemetry is available.

### Hard deployment prerequisite: full VCNT data wipe, coordinated with tempo's L0 writer change

This design has **zero backward-compatibility code**, per this project's standing directive:
`valuecounts.ParseFilenameV2` (SPEC-VC-7 in `valuecounts/SPECS.md`) has no v1 fallback — a
v1-shaped filename is a defined parse error, not a degenerate case handled specially. This means
three changes must land together as a single all-or-nothing deployment, not a staged rollout:

1. This blockpack change (`compactColumn`/`mergeLevel` now require v2 filenames to select or
   produce anything).
2. tempo's `vcntwriter.go` L0 writer switching to `VCNTObjectKeyV2`/`VCNTFormatFilenameV2` (Part
   B of #494, out of this repo's scope) — otherwise every newly-written L0 file becomes
   invisible to the compactor the moment Part A deploys, accumulating unprocessed files forever.
3. A full VCNT data wipe (every existing `.vcnt` file, every tenant, every environment) —
   otherwise every pre-existing v1-format file becomes a permanent straggler (see below).

This is flagged prominently here because it is an unusual, one-time operational requirement, not
a normal "deploy and forget" change — see `.bob/state/plan.md`'s "Deployment Notes" section for
the full recommended sequencing.

### v1-straggler-file safety: permanently, safely skipped — not corrupted, not force-migrated

If the wipe/coordination is not perfectly simultaneous (e.g. a v1 file written just before the
wipe, or the wipe missing an object due to an operational gap), that file is not corrupted, not
force-migrated, and never merged: `ParseFilenameV2`'s defined error on a v1-shaped name causes
`compactColumn`'s parse loop to increment `filesSkipped` and leave the file exactly where it is,
indefinitely, until it is either manually cleaned up or naturally expired by whatever upstream
retention process governs `.vcnt` objects generally. It can never re-enter the merge pipeline
under this design (there is no path by which a `ParseFilenameV2` failure gets a second, more
lenient parse attempt). `TestCompactColumn_V1FormatFilesAreSkippedNotMerged` (TESTS.md
TEST-VC-27) locks in exactly this behavior.

### Out of scope: listing-time pruning on the tempo side

A companion tempo-side follow-up (R8, out of scope for this blockpack change) would let tempo's
own listing/read paths use the v2 filename's embedded range to prune candidate files at
listing time before ever fetching them — analogous to how block-level min/max pruning already
works for blockpack data files. That is a read-path optimization independent of this
compaction-time clustering change and is tracked separately.

Back-refs: `internal/modules/valuecountscompactor/cluster.go` (`clusterByTimeRange`,
`pickCluster`), `internal/modules/valuecountscompactor/service.go` (`compactColumn`),
`internal/modules/valuecountscompactor/config.go` (`MaxTimeSpanPerMerge`,
`DefaultMaxTimeSpanPerMerge`). `SPECS.md` SPEC-VC-3. `valuecounts/NOTES.md` NOTE-VC-017 (the
filename-format side of this change). `.bob/state/plan.md` "Deployment Notes" section (full
sequencing detail, out of this file's scope to duplicate).

## NOTE-VC-019 — colHash-manifest hook lives at compaction time, not L0 write time, because VCNT has no blockpack-owned L0 write path (task #216)

Date: 2026-07-13

**The gap this closes:** VCNT keys its entire on-disk layout by `colHash`
(`valuecounts.ColHash`) with no metadata file anywhere mapping a hash back to its source column
name. `internal/modules/colhashmanifest` (new sibling package, see its own SPECS.md/NOTES.md)
closes this for both VI and VCNT with one shared per-tenant manifest.

**Why `mergeLevel`, not an L0 write path, for VCNT specifically:** task #216 requires this
manifest to populate via blockpack's OWN write paths, with no new tempo-side code required.
For VI, blockpack genuinely owns an L0 write path
(`internal/modules/valueindexconsumer/service.go:flushColumn` — see that package's NOTES.md
NOTE-VI-106). For VCNT, blockpack owns NO equivalent L0 write path: the root `vcnt.go`'s own
doc comment states plainly that it "[exposes] the minimal API tempo needs to accumulate
per-column span counts during block writes and write L0 .vcnt files to S3" — tempo itself
performs the actual object-storage `Put` for L0 `.vcnt` files, using blockpack's encode/key
helper functions (`EncodeVCNTFile`, `VCNTObjectKey`/`VCNTObjectKeyV2`) but never blockpack's own
code. `mergeLevel` (this file) is therefore the EARLIEST point blockpack's own code ever
touches a given colHash's VCNT data via object storage at all.

**Practical consequence, stated plainly:** a newly-observed VCNT column's manifest entry lags
its true first L0 write by however long until that column's first L0→L1 compaction pass runs
(bounded by `Config.CompactInterval`/`CompactThresholdFiles`) — this is an accepted,
documented limitation for a LOW-priority observability feature, not a bug. `recordManifestEntry`
only fires when `mergeLevel` actually writes a non-empty merged output
(`len(merged) > 0`), matching this file's own established "only touch state when real work
happened" discipline elsewhere in this package.

**Why `colDir` alone is enough to derive `(tenant, colHash)` without threading new parameters
through `compactColumn`/`mergeLevel`'s existing signatures:** `colDir` is always shaped
`"<tenant>/<indexPrefix>/unique_values/<colHash>"` — `buildWorkList` already parses this same
shape to populate `columnWork.tenant`/`columnWork.colDir}` separately, but that tenant is never
threaded down into `mergeLevel` today. Rather than changing `compactColumn`/`mergeLevel`'s
signatures (which would ripple through every existing call site and test), `tenantFromColDir`
(new, `service.go`) re-derives `tenant` from `colDir` directly — the same "first path segment"
extraction `valueindexconsumer.tenantFromPath` already uses for the analogous VI case, just
applied to a different string shape.

**Why `ManifestStore` is its own interface rather than reusing `Store` directly in `Config`:**
mirrors this package's own established `Object`/`IndexObject` precedent
(`store.go`'s doc comment: two structurally-identical-but-independently-named types across
`valuecountscompactor` and `valueindexcompactor`, deliberately not shared) — even though any
`Store` value already satisfies `ManifestStore` structurally with zero adapter code, giving the
optional dependency its own minimal, purpose-scoped interface keeps `Config.ManifestStore`'s
contract self-documenting independent of `Store`'s full (List/ListDirs/Get/Put/Delete) surface.

Back-refs: `internal/modules/valuecountscompactor/service.go:recordManifestEntry,ManifestStore,tenantFromColDir,mergeLevel`,
`internal/modules/valuecountscompactor/config.go:Config.ManifestStore`,
`internal/modules/colhashmanifest` (the shared registry implementation),
`internal/modules/valueindexconsumer/NOTES.md` NOTE-VI-106 (VI's symmetric hook),
`internal/modules/valuecounts/NOTES.md` NOTE-VC-020 (the cross-reference entry in the module
this feature is conceptually "for," even though its call site lives here), `vcnt.go` (root
package doc comment confirming tempo, not blockpack, performs VCNT's L0 write).

## NOTE-VC-021 — `recordManifestEntry` caches confirmed-recorded `(tenant, colHash)` pairs, skipping the manifest `Get` on every repeat merge (task #216 HIGH follow-up)

Date: 2026-07-13

**The gap this closes (HIGH finding, go-presubmit-reviewer):** `recordManifestEntry`
(NOTE-VC-019) originally called `colhashmanifest.RecordColumn` unconditionally on every
non-empty `mergeLevel` merge, and `RecordColumn` performs a full `Get` of the tenant's aggregate
manifest file on every call — even in the steady-state case where `(tenant, colHash)` is already
recorded and `RecordColumn`'s own no-op branch only skips the `Put`, never the `Get`. This added
a permanent extra object-storage round trip to every merge of every already-known column,
forever, sitting between the real compacted-output `Put` and the delete-of-inputs loop.

**Fix:** `Service.manifestSeen` (a plain `map[string]struct{}` keyed by `tenant+"\x00"+colHash`
— no locking needed since `Run`/`RunOnce` drive compaction sequentially with no concurrent
goroutines touching `Service` state) is checked BEFORE `recordManifestEntry` ever calls into
`colhashmanifest.RecordColumn`. Once a pair is confirmed recorded, every subsequent merge of
that same column for the rest of this process's lifetime is a zero-I/O no-op. A `RecordColumn`
failure does not populate the cache, so the next merge naturally retries against the real store.

**Regression test:** `TestMergeLevel_ManifestCacheSkipsRepeatedGet`
(`valuecountscompactor/manifest_hook_test.go`) runs two independent merges of the same column
and asserts the fake store's manifest-path `Get` is called exactly once total (confirmed red
before this fix — 2 calls — and green after).

Back-refs: `internal/modules/valuecountscompactor/service.go:Service.manifestSeen,recordManifestEntry`.
See `SPECS.md` SPEC-VC-5.
