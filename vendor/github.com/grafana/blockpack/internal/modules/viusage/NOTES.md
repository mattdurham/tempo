# viusage — Design Notes

This document records the design decisions and rationale behind `internal/modules/viusage`
(blockpack/#496: VI dedicated columns + query-usage-driven backfill). Entries are append-only
and dated; never delete or rewrite an existing entry — add an `*Addendum (date):*` note if a
decision is later corrected or superseded.

## ID convention

Entries use the module-local prefix `NOTE-VIUSAGE-N`, independent of the shared `NOTE-VI-N`
space the three existing value-index modules (`valueindex`, `valueindexcompactor`,
`valueindexconsumer`) maintain (confirmed with team lead 2026-07-10 — see
`internal/modules/viusage/SPECS.md`'s own ID-convention section for the full reasoning).

---

## NOTE-VIUSAGE-1 — Why this module exists at all, and why it does not reuse `cube`'s registry (R1)

Date: 2026-07-10

`internal/modules/cube` already implements a registry/backfill pattern (conditional-PUT-
with-retry object storage, a `RegistryEntry` with watermark tracking, a `Backfiller.Run`
progress-callback shape) for a structurally similar problem: tracking which time ranges of
derived data are known-complete and backfilling the rest. #496 needed the same SHAPE of
solution for a genuinely different domain (per-column usage tracking + repeated-use
triggering + raw-historical-block backfill, vs. cube's per-cube-definition aggregation
rollup), and the team lead ruled (plan.md Section 0, R1) that VI gets its OWN registry/
trigger/backfill engine — own types, own package — rather than either (a) importing and
reusing `cube.Registry`/`cube.RegistryEntry` directly, or (b) extracting a shared generic
registry helper both modules would depend on.

**Why not (a):** `cube.RegistryEntry` carries `Dimensions`/`Filters`/`AggAttrs`/a
per-resolution `Watermarks` map — cube-specific aggregation concepts with no VI analog. VI's
`Entry` is keyed one-per-column (not one-per-metric-cube), needs a use-count/timestamp window
cube never needed (cube triggers on first use, not repeated use), and needs an explicit lease
(`BackfillState.BackfillInProgress`/`LeaseExpiresAt`) cube's own `RegistryEntry` has no
equivalent of (R8, a deliberate improvement over cube's pattern — see NOTE-VIUSAGE-4 below).

**Why not (b):** extracting a shared generic registry helper out of cube's ALREADY-SHIPPED
module was explicitly ruled out of scope for this task — refactoring a shipped module to
extract a shared abstraction is real, separate work with its own risk, and doing it
speculatively for a SECOND consumer that has not yet proven the abstraction is right would be
premature. If duplication between `cube`'s and `viusage`'s conditional-PUT-retry loops
becomes a real maintenance pain later, that is a future decision informed by two working
implementations, not a decision to make now with only one implementation actually shipped.

**What WAS copied:** the conditional-PUT-with-retry `ObjectStore` interface SHAPE
(`Get`/`ConditionalPut`, ETag-based optimistic concurrency, `ErrConflict` on mismatch) and the
5-attempt/50ms-doubling-backoff retry LOOP structure — copied as a design pattern, written
fresh, independently, in this package (`registry.go`), with zero import of
`internal/modules/cube`.

Back-refs: `internal/modules/viusage/registry.go:ObjectStore,Registry.updateEntryWithRetry`.
See `internal/modules/cube/registry.go` for the pattern being mirrored (not imported). See
`SPECS.md` SPEC-VIUSAGE-4.

---

## NOTE-VIUSAGE-2 — R2/A0: the dedicated-column bootstrap list, and why all 4 legacy HTTP aliases were kept

Date: 2026-07-10

#496's dedicated-column list (`DefaultDedicatedColumns`, `dedicated_columns.go`) bootstraps
from Tempo's Parquet-14 `defaultDedicatedColumns` (`tempo/tempodb/backend/block_meta.go:
151-169`). Before finalizing the list, task A0 (#103) was required to empirically verify
whether the 4 "legacy alias" HTTP semconv columns (`http.method`, `http.url`, `http.route`,
`http.status_code`) still matter for VI's dedicated list, or whether current-semconv attribute
names alone suffice — per the team-lead ruling (plan.md R2), decide from evidence, not
assumption.

**Finding: KEEP all 4.** Evidence gathered:
- `tempo/tempodb/backend/block_meta.go:151-169`'s `defaultDedicatedColumns` function still
  actively returns all 4 legacy aliases alongside the current-semconv names as CURRENT
  defaults, not merely historical/deprecated entries kept for backward-read compatibility.
- vparquet4's `WellKnownColumnLookups` still statically maps 3 of the 4 legacy names to live,
  production query-execution code paths — these are not dead lookups.
- No TraceQL attribute-name canonicalization/normalization layer exists ANYWHERE in tempo's
  `pkg/traceql`, `pkg/tempopb`, or blockpack's own `internal/traceqlparser` that would rewrite
  a query literally naming `http.method` into `http.request.method` (or vice versa) before the
  query reaches VI's index. A user query naming the legacy attribute name is not silently
  translated — it reaches VI's predicate matching exactly as written.

Consequence: dropping the 4 legacy aliases from VI's dedicated list would silently degrade
(or entirely disable) dedicated-column-speed lookups for any TraceQL query that still names
the legacy attribute directly — a real, present-day query shape, not a historical-data-only
concern. All 4 are kept as `span.http.method`, `span.http.url`, `span.http.route`,
`span.http.status_code` in `DefaultDedicatedColumns` (blockpack's `span.`-prefixed convention,
`internal/modules/blockio/writer/config.go:15`).

**This list is explicitly PROVISIONAL, not a final data-driven answer** (R2's own binding
text) — it is a reasonable, evidence-checked starting point pending real production
usage-registry telemetry, not a claim that this is the optimal steady-state dedicated-column
set. Expect it to be tuned once the usage registry (this module) has accumulated real
distinct-query-count data per non-dedicated column.

Back-refs: `internal/modules/viusage/dedicated_columns.go:DefaultDedicatedColumns`. Full
evidence trail in task #103's own metadata (TaskGet). See `SPECS.md` SPEC-VIUSAGE-7.

---

## NOTE-VIUSAGE-3 — R4: repeated-use threshold and backfill window are unmeasured starting defaults, disclosed as such

Date: 2026-07-10

Two of #496's most consequential tunables have NO production telemetry to calibrate against,
because the usage registry this module implements is the FIRST thing that will ever produce
that telemetry — there is a genuine chicken-and-egg gap here, disclosed deliberately rather
than papered over with a false-precision default:

- **Trigger threshold: 3 distinct queries within a rolling 1-hour window**
  (`TriggerConfig{Threshold: 3, WindowSeconds: 3600}`). Repeated-use, not first-use, was
  chosen deliberately (unlike cube, which triggers on first use) — a single one-off query
  referencing a non-dedicated column should not itself justify a 48h historical backfill's
  full-block-read cost; the threshold exists specifically to filter out one-off/exploratory
  queries from triggering expensive work.
- **Backfill window: 48 hours** (`defaultBackfillWindowSeconds`), narrower than cube's 7-day
  backfill window. This is a deliberate cost-tradeoff, not an oversight: VI's backfill reads
  RAW historical blocks (full block I/O per this repo's core I/O invariant, ARCH-002/003) for
  every block in the window, unlike cube's backfill, which reads cheap, already-extracted VI
  files. A 48h window at this repo's typical query-usage-driven cardinality is a deliberately
  conservative starting point that trades "may not cover the full useful history for a
  newly-triggered column" against "does not impose an unbounded full-block-read cost on the
  cluster for every triggered column."
- **Lease TTL: 30 minutes** (`LeaseTTLSeconds=1800`) — chosen to comfortably exceed the
  expected duration of one unit of real backfill work (a single block fetch+extract+PUT cycle)
  with margin, while still being short enough that a crashed worker's lease self-heals within
  a bounded, observable time (R8).

**Both the threshold/window pair and the lease TTL are config-overridable** (A6/tempo's B3
config wiring, Part B) — these are starting points meant to be tuned once real telemetry
exists, not fixed constants. Revisit once the usage registry itself has accumulated enough
real distinct-query and real-backfill-duration data to calibrate against.

Back-refs: `internal/modules/viusage/trigger.go:TriggerConfig`,
`valueindex_backfill.go:defaultBackfillWindowSeconds,defaultBackfillWorkers` (root package).
See `SPECS.md` SPEC-VIUSAGE-3/5.

**Addendum (2026-07-11, task #154): this decision is reversed — see NOTE-VIUSAGE-12.**
The repeated-use threshold documented above (3 distinct queries / 1h window) no longer
exists in code. Team-lead ruling 2026-07-11: any query against a non-dedicated column is
worth indexing immediately, so the threshold's one-off-query filter was removed in favor
of an unconditional first-use trigger. The 48h backfill-window default and the 30-minute
lease TTL discussed above are UNCHANGED and still accurate -- only the trigger-threshold
portion of this entry is superseded.

---

## NOTE-VIUSAGE-4 — R5: no eviction in v1, and why VI's asymmetry with cube is deliberate

Date: 2026-07-10

Once a column's backfill is `Triggered`, `BackfillState.Triggered` never reverts to `false` —
there is no eviction policy in v1, and a triggered column stays indexed forever going forward,
incurring the same ordinary `valueindexcompactor` upkeep cost as any dedicated column, with no
special-casing.

**Why this differs from cube (which DOES evict/re-backfill under some conditions):** cube's
derived data (rollup aggregates) is CHEAP to rebuild — evict-and-re-backfill is an affordable
operation there, so an eviction policy trading storage/compute for freshness makes sense. VI's
backfill is NOT cheap to repeat: it reads full raw historical blocks (this repo's core I/O
invariant — one full block read per historical block in the window, not a cheap
pre-extracted-file read). An eviction policy for VI would risk real thrashing cost (repeatedly
re-triggering an expensive full-block-read backfill for a column whose usage pattern
oscillates near the threshold) with no clear correctness or cost benefit to offset it. The
asymmetry is a direct consequence of the two systems' underlying cost models, not an
oversight or a "we'll add it later" placeholder — v1 deliberately omits eviction because
adding it would very plausibly make things worse, not better, given VI's cost profile.

Back-ref: `internal/modules/viusage/entry.go:BackfillState.Triggered` (doc comment: "Never
reverts to false"). See `SPECS.md` SPEC-VIUSAGE-1/3.

---

## NOTE-VIUSAGE-5 — R9: cube's own backfill→registry watermark wiring has a real, independently-confirmed gap; VI's design does not model it

Date: 2026-07-10

Before implementing VI's own R7/R8 mechanisms, the team-lead ruling (plan.md R9) required
independently re-verifying — by direct code read, not by trusting the brainstorm's own
research — whether tempo's `cube_backfill.go`'s `launchBackfill`/`RunCubeBackfill` actually
persists a watermark to the cube registry from its `progressFn` callback.

**Confirmed finding (plan.md Section 1, direct read of `tempo/tempodb/encoding/vblockpack/
cube_backfill.go` lines 231-317 and `blockpack/internal/modules/cube/backfill.go` lines
26-58/74-122 and `blockpack/internal/modules/cube/registry.go` lines 180-238):**
`blockpack.cube.Registry.UpdateWatermarks` DOES exist as a real, tested, conditional-PUT-
guarded persistence path — the mechanism itself is not missing from blockpack. But tempo's
OWN wiring never calls it from either backfill entry point: both `launchBackfill`'s and
`RunCubeBackfill`'s `progressFn` callbacks only log on `Done`, never call
`Registry.UpdateWatermarks` or anything else that writes to `<tenant>/cubes/index.json`. A
cube's L0-resolution watermark is therefore only ever advanced by a LATER compaction pass
(`definition.go`'s Compactor), never by the backfill pass that actually did the work — a real
gap between "backfill completed" and "the registry reflects that," confirmed by direct read,
not merely inferred.

**This gap is genuinely OUT OF SCOPE for #496** — cube's own code and tempo's own cube-backfill
wiring are not touched by this feature. The team lead was asked to consider filing a
STANDALONE follow-up issue for this finding (suggested framing: "cube backfill's `progressFn`
never persists a watermark to the registry — L0 coverage may be silently stale between
backfill completion and the next compaction pass"). Cube's own aggregation semantics tolerate
this today (an approximate coverage window degrades correctness gracefully for an aggregate
rollup rather than corrupting an answer outright) — presumably why it has shipped without
anyone treating it as a hard bug yet — but it is a real gap, not a non-issue, and this note
exists so it is not silently lost once #496 ships.

**Design consequence for VI (binding):** VI's OWN watermark-persistence and query-time
enforcement (`BackfillEngine.Run`'s `progressFn` contract, SPEC-VIUSAGE-5; R7's query-time
gate, `vibuilder/SPECS.md` SPEC-VB-4) is deliberately NOT modeled on copying cube's
backfill→registry wiring, precisely BECAUSE that wiring was just confirmed not to actually
exist for cube's L0 case. `BackfillEngine.Run`'s own contract states explicitly, in its own
doc comment, that persisting `WatermarkSec`/`Done` is the CALLER's job — Part B's tempo-side
launcher (not yet landed as of this writing) MUST actually call the registry's update path
from inside `progressFn`, unlike cube's tempo-side caller. This is the one piece of cube's
pattern this module's design explicitly does NOT copy as-is.

Back-refs: `plan.md` Section 1 (the full verification, reproduced there in detail — this note
cross-references rather than duplicates it). `valueindex_backfill.go:Run`'s own doc comment
(root package). See `SPECS.md` SPEC-VIUSAGE-5.

**Addendum (2026-07-10, same day): the concrete fix now exists in code as `Registry.
UpdateWatermark`.** This note originally described the persistence mechanism only in prose
("VI's design makes the backfill engine's own progressFn-equivalent responsible for
persisting the watermark via the registry's conditional-PUT retry loop directly"). A named,
tested method now implements exactly this: `Registry.UpdateWatermark(ctx, tenant, colHash,
colType, watermarkSec, windowStartSec, windowEndSec, done) error` (`registry.go`, SPEC-
VIUSAGE-4) persists `WatermarkSec`/window bounds on every call, and additionally sets
`Done=true` + releases the lease (`BackfillInProgress=false`) in the same conditional-PUT
when `done=true` — closing R9's flagged gap with a concrete, callable method rather than only
a documented expectation. Part B's tempo-side launcher (task #112/B2, in progress as of this
writing) is the intended caller, from inside `BackfillEngine.Run`'s `progressFn`, on every
progress update, not just on the terminal one — this is what actually closes the gap R9
identified in cube's own equivalent wiring, once B2 lands and calls it. Tested by
`TestRegistry_UpdateWatermark_PersistsWatermarkSec`, `_DoneReleasesLeaseInSamePut`,
`_NotFoundReturnsError` (`TESTS.md` TEST-VIUSAGE-35 through -37).

**Fixed (2026-07-10, task #127, per direct user request to close the gap rather than leave
it standing as a disclosed-but-unfixed finding):** cube's own backfill→registry
watermark-persistence gap described above is no longer merely documented — it has been
fixed, in tempo's own `tempodb/encoding/vblockpack/cube_backfill.go`. A new
`runCubeBackfillCore` function (mirroring VI's own `runViBackfillCore` split) is now what
`launchBackfill`/`RunCubeBackfill` call instead of running `blockpack.NewCubeBackfiller(...).
Run()` inline; its `progressFn` calls `registry.UpdateWatermarks(ctx, entry.CubeID,
blockpack.CubeRollupL0, wm.WatermarkMinute, wm.WatermarkMinute)` on every successful
(`LastError == nil`) progress callback, not just on `Done` — the exact VI-side R9 pattern
(`Registry.UpdateWatermark`, SPEC-VIUSAGE-4) mirrored back onto cube's own registry.

**One deliberate, documented deviation from VI's own pattern:** the `LastError == nil` gate
exists because cube's `Backfiller` and VI's `BackfillEngine` react to a per-unit failure
differently. VI's engine (`valueindex_backfill.go:Run`) ABORTS the whole run on the first
error — `processBlocks` returns the error immediately, so `progressFn` is never called again
after a failure, and there is nothing for a caller-side `LastError` check to gate. Cube's
`Backfiller`, by contrast, CONTINUES past a per-minute failure (a design choice predating
#496, unrelated to and not modified by this fix) and reports it via
`BackfillProgress.LastError` on that one call while still calling `progressFn` again on
subsequent minutes. Persisting a watermark from a call whose `LastError != nil` would
advance cube's registry state past a minute cube itself does not consider successfully
processed — the exact "false complete" shape R7/R9 both exist to prevent, just for cube's
own per-minute unit of work rather than VI's per-block unit. `runCubeBackfillCore`'s gate is
therefore not an arbitrary implementation detail but the necessary adaptation of VI's
"persist on every successful call" pattern to a backfill engine whose failure-handling
semantics differ from VI's own (continue-past-failure vs. abort-on-failure) — mirroring the
pattern, not the literal condition, was the correct choice here.

No blockpack-side changes were needed: `cube.Registry.UpdateWatermarks` already existed and
was already tested (confirmed above, "the mechanism itself is not missing from blockpack");
this was purely tempo-side wiring, exactly as this note's own "Design consequence for VI"
section anticipated it would need to be. Regression-pinned by
`TestRunCubeBackfillCore_CallsUpdateWatermarksOnEachProgress`/
`_PersistFailureAbortsRun` (tempo's new `cube_backfill_watermark_test.go`), mutation-checked
(the persistence branch was forced off, both tests confirmed failing, then restored and
confirmed passing again — the same mutation-check discipline this project's own R7 adversarial
test used).

**Consequence: the gap between "backfill completed" and "the registry reflects that" no
longer exists for cube either.** A cube's L0-resolution watermark now advances per-minute
during the backfill pass itself (via `UpdateWatermarks` on every progress callback), not only
later via a subsequent compaction pass — the exact asymmetry this note originally identified
(compare NOTE-VIUSAGE-11's own "closing VI's own instance of this gap pattern" phrasing,
corrected below, now that BOTH sides — VI's own design AND cube's actual wiring — close it).
This was genuinely out of #496's original scope (R9's own text: "do NOT attempt to fix it as
part of #496") but was subsequently closed by direct user request rather than left open
indefinitely as a standalone follow-up issue — recorded here so a future reader does not
need to go hunting for whether the "standalone follow-up issue" this note originally
recommended was ever filed or acted on: it was acted on, directly, in this same effort.

---

## NOTE-VIUSAGE-6 — R10: this module reopens NOTE-VI-027's history — why the new model is different in kind

Date: 2026-07-10

#496 reintroduces column-indexing policy at the VI writer (`blockpack.ColumnPolicy`,
`valueindex/SPECS.md` SPEC-VI-11) after `internal/modules/valueindexconsumer`'s NOTE-VI-018
recorded that the ORIGINAL VI denylist (`DefaultValueIndexDenylist`) was removed in favor of
"index everything unconditionally, let the querier decide at read time" (referenced there as
"SUPERSEDED by NOTE-VI-027," issues #414/#415). Per the team-lead ruling (plan.md R10), this
reopening must be acknowledged explicitly, not silently reversed without comment — **the full
acknowledgment entry lives in `internal/modules/valueindexconsumer/NOTES.md`** (near
NOTE-VI-018, where the original denylist history already partially lives), not duplicated in
full here. This entry is a pointer, plus this module's own framing of why the new model
differs in kind.

**Why #496's policy is different in kind from the old denylist, not merely a reversal:**
1. **Usage-driven, not writer-imposed.** The old denylist statically declared 4 columns
   "useless as tag values" at the WRITER, unconditionally, for every tenant, forever. #496's
   policy is driven by OBSERVED QUERY USAGE (this module's registry) — a column becomes
   indexed because real queries repeatedly asked for it, not because someone decided ahead of
   time it was worth indexing.
2. **Opt-in-over-time, not a fixed a-priori set.** The old denylist (and its policy-free
   successor) offered no middle ground between "always index" and "never index." #496
   introduces a genuine THIRD state — "not yet indexed, but will become so automatically once
   usage crosses a threshold" — that the pre-#414/#415 world had no equivalent of.
3. **Caller-overridable, not a single global constant.** `DefaultDedicatedColumns`
   (`SPECS.md` SPEC-VIUSAGE-7) is an explicit, disclosed-as-provisional STARTING point a
   tenant may override (Part B's config wiring) — not a single hardcoded list applied
   identically everywhere with no override mechanism, as the old denylist was.
4. **The permanent hard-exclusion set is orthogonal, narrower, and structurally motivated,
   not a "these are useless" judgment.** `blockpack.HardExcludedColumns` (`valueindex/
   SPECS.md` SPEC-VI-11) permanently excludes exactly the 4 columns that are structurally
   unindexable as ordinary value-index entries for CORRECTNESS reasons tied to VI's own wire
   format (`span:id`/`span:parent_id`/`span:start` per NOTE-VI-027's history;
   `trace:id` per its dedicated `TraceGroup` format, `valueindexconsumer/SPECS.md` SPEC-VI-4)
   — not a value judgment about which columns are useful as tag values, which is what the
   original denylist was.

Back-refs: `internal/modules/valueindexconsumer/NOTES.md`'s R10 acknowledgment entry (the
full record — see that file), `internal/modules/valueindexconsumer/NOTES.md:NOTE-VI-018`
(the original denylist-removal history). See `SPECS.md` SPEC-VIUSAGE-6/7 and
`valueindex/SPECS.md` SPEC-VI-11.

---

## NOTE-VIUSAGE-7 — Where `ColumnPolicy` and `ColumnWatermark` actually ended up, and why (import-cycle constraints discovered during A1/A5, not anticipated by plan.md)

Date: 2026-07-10

`plan.md` Section 4.6 proposed `ColumnPolicy` living in `internal/modules/viusage/policy.go`;
Section 4.7 proposed `ColumnWatermark` being defined directly in the root `blockpack` package.
**Neither placement survived implementation, for the same underlying reason: Go's import
graph is a DAG, and both types sit at a point where the plan's proposed placement would have
created a cycle.**

**`ColumnPolicy` moved to the root package (`valueindex_policy.go`), not
`internal/modules/viusage`.** `internal/modules/viusage.BackfillEngine` (A4) needs to import
root `blockpack` for `*blockpack.Reader`/`blockpack.ObjectPutter`/
`blockpack.ExtractValueIndexEntriesForColumns`. Root `blockpack` was ALSO going to need to
import `internal/modules/viusage` for `ColumnPolicy` under the plan's original placement —
`blockpack → viusage → blockpack`, a cycle. Resolution: `ColumnPolicy`/`HardExcludedColumns`/
`Allowed`/`BuildColumnPolicy` all live in root `blockpack` (`valueindex_policy.go`) instead;
`viusage` now depends ONLY on root, one direction, no cycle. `viusage.DefaultDedicatedColumns`
(the actual dedicated-column LIST, distinct from the policy MECHANISM) correctly stayed in
`viusage` as planned — only the policy/enforcement type moved.

**`ColumnWatermark` lives in `internal/modules/vibuilder` (`watermark.go`), aliased from root
via `type ColumnWatermark = vibuilder.ColumnWatermark` (`valueindex_query.go`), not defined in
root directly.** `vibuilder.BuildSource` is the actual consumer of the R7 gate; root
`blockpack` already imports `vibuilder` (`BuildValueIndexSource` calls
`vibuilder.BuildSource`). If `ColumnWatermark` lived in root as the plan proposed, `vibuilder`
would need to import root to reference it — `blockpack → vibuilder → blockpack`, the same
cycle shape again. Root re-exports the type via a plain alias instead (matching the existing
`FileStore`/`ErrFileNotFound` re-export pattern already in `valueindex_query.go`), so external
callers never need to import an internal package directly, preserving the plan's intended
PUBLIC API shape even though the type's OWN package changed.

**Consequence: `BackfillState.CoversRange` (viusage) and `ColumnWatermark.CoversRange`
(vibuilder) are two independently-defined copies of the identical 3-field-struct,
3-branch-logic contract, not one shared type.** `viusage` cannot import `vibuilder` (root
already imports both, and `vibuilder` cannot import `viusage` either without recreating the
same cycle shape a third way), so there is no available common package for a single shared
type to live in without ALSO moving `BuildSource`'s own package boundary — out of scope for
#496. Both copies must be kept in sync by hand if this logic ever changes; there is no
single source of truth at the TYPE level, only at the SPECIFICATION level (this note +
`SPECS.md` SPEC-VIUSAGE-2 + `vibuilder/SPECS.md` SPEC-VB-4 together document that they must
agree).

**Recurring pattern worth naming once, so it is not rediscovered per-task:** any new type
that needs to be referenced by BOTH (a) a leaf/internal package `blockpack` already imports
(`vibuilder`, `valueindex`, etc.) and (b) a NEW package that itself needs to import root
`blockpack` for reader/writer primitives, cannot live in either (a) or the new package (b)
without a cycle — it must live in root itself (if (a) doesn't need it as a defining package)
or be duplicated (if (a) IS the actual consumer, as with `ColumnWatermark`/`vibuilder`). This
bit twice during #496 (A1's `ColumnPolicy`, A5's `ColumnWatermark`) with the same shape both
times; a future feature needing a similar cross-cutting type should check this constraint
FIRST, before writing a plan section that assumes a particular package placement.

Back-refs: `valueindex_policy.go`'s own doc comment (the `ColumnPolicy` move, written by
coder-1), `internal/modules/vibuilder/watermark.go`'s own doc comment (the `ColumnWatermark`
placement, same reasoning). See `valueindex/SPECS.md` SPEC-VI-11 and `vibuilder/SPECS.md`
SPEC-VB-4 for the resulting formal contracts, and `SPECS.md` SPEC-VIUSAGE-2's own
cross-reference to this same duplication.

**Addendum (2026-07-10, same day): this bit a THIRD time, for `BackfillEngine` itself, and
sharpened the constraint into a more precise statement.** `plan.md` Section 4.3 assumed
`BackfillEngine` would live in `internal/modules/viusage/backfill.go` alongside the rest of
this module — and it briefly did (A4), importing root `blockpack` directly for
`*Reader`/`ObjectPutter`/`ExtractValueIndexEntriesForColumns` (a one-directional import that,
by itself, is fine and was NOT the problem). The actual forcing constraint only surfaced once
`valueindex_usage.go` (root's re-export file for `viusage.Entry`/`Registry`/`Config`/
`TriggerConfig`/etc., built to let tempo's Part B call these without importing
`internal/modules/viusage` directly, which it cannot) needed to exist: **Go import cycles are
per-PACKAGE, not per-file.** It does not matter that `BackfillEngine`'s own file
(`backfill.go`) was the only `viusage` file importing root — the moment ANY file in the
`viusage` package imports root `blockpack`, the ENTIRE `viusage` package becomes ineligible to
be imported back by root, for ANY type, from ANY other file in that package. `BackfillEngine`
was therefore moved out of `viusage` entirely (to root's `valueindex_backfill.go`) — not
because it individually caused a two-step cycle, but because its mere presence in `viusage`
blocked root from importing `viusage` for the OTHER types `valueindex_usage.go` needed,
which was the actual, later-discovered requirement. This is a sharper, corrected version of
this note's "Recurring pattern" paragraph above: the constraint to check first is not just
"would this specific new type create a cycle," but "does ANY file in the target package
already import the other side, making the WHOLE package off-limits as an import target,
regardless of which type you actually want from it."


---

## NOTE-VIUSAGE-8 — Documentation finding: `NOTE-VI-027` is a collided, double-used ID across the shared VI-pipeline NOTES.md space (unrelated to #496, flagged during R10 work)

Date: 2026-07-10

While placing the R10 acknowledgment entry (NOTE-VIUSAGE-6 above), a numbering inconsistency
in the three EXISTING value-index modules' nominally-shared `NOTE-VI-N` space was found and
confirmed with the team lead: `NOTE-VI-027` is used for two (arguably three, but two of the
three are the same underlying decision — see below) genuinely unrelated things:

1. `internal/modules/valueindex/NOTES.md` has a real, fully-written `## NOTE-VI-027 — BlockRef:
   page-addressed block reference for v2 files (issue #417)`, dated 2026-06-28.
2. `internal/modules/valueindexconsumer/NOTES.md`'s `NOTE-VI-018` contains an annotation
   reading `"> SUPERSEDED by NOTE-VI-027 (issues #414/#415): DefaultValueIndexDenylist was
   removed..."` — but **no standalone `## NOTE-VI-027` heading for the denylist-removal
   decision actually exists anywhere** in any of the three modules' NOTES.md files. It is only
   ever referenced (also from root `valueindex_extract.go`'s own code comments, at least 3
   separate sites, all citing "NOTE-VI-027, issue #414" or "issue #415" for the SAME
   denylist-removal-plus-millisecond-truncation decision — confirmed by direct grep, not a
   third independent collision, just the same un-written decision referenced from multiple
   call sites).

This is a real, pre-existing (not introduced by #496) violation of the "IDs assigned in
ascending order and never reused" convention every SPECS.md in this repo states, AND the
underlying decision R10 asks to be "reopened" was never actually given its own dated NOTES.md
entry — it exists only as a reference/annotation. **Team lead's disposition (2026-07-10):** a
minor, pre-existing documentation-hygiene issue, not blocking for #496; may become a small,
separate follow-up cleanup ticket. No action taken on it here beyond this record — the R10
acknowledgment entry in `valueindexconsumer/NOTES.md` cites `NOTE-VI-018` itself as the real
historical record (not a nonexistent standalone `NOTE-VI-027`) and states this collision
explicitly rather than papering over it.

Back-refs: `internal/modules/valueindex/NOTES.md:187` (the real NOTE-VI-027, BlockRef),
`internal/modules/valueindexconsumer/NOTES.md:149` (the NOTE-VI-018 annotation referencing
the un-written denylist-removal decision), `valueindex_extract.go` (3+ code-comment
cross-references to the same un-written decision). See the R10 acknowledgment entry in
`valueindexconsumer/NOTES.md` for where this is cited in context.

---

## NOTE-VIUSAGE-9 — `valueindex_usage.go`: root-package re-export of viusage's Part-B-facing surface

Date: 2026-07-10

Root `blockpack` now has `valueindex_usage.go`, re-exporting exactly the `viusage` surface
tempo's Part B needs (`Entry`, `BackfillState`, `Config`/`DefaultConfig`, `TriggerConfig`,
`TriggerResult`, `ObjectStore`, `ErrConflict`, `Registry`/`NewRegistry`,
`RecordUseAndMaybeTrigger`, `MaybeRecordUseAndMaybeTrigger`, `DefaultDedicatedColumns`) via
plain type aliases and thin wrapper functions — mirroring `valueindex_query.go`'s existing
re-export style (`Lister`, `LookupStore`, `IndexFileCache`, `ColumnWatermark`). This exists
because tempo cannot import `internal/modules/viusage` directly (Go's internal-package rule
is enforced by import path; tempo's own package paths never share the
`github.com/grafana/blockpack/` prefix) — the same reason every other value-index submodule
(`valueindexcompactor`, `valueindexconsumer`) has its own public re-export subpackage
(`valueindexcompactor/valueindexcompactor.go`, NOTE-VI-017's "Public re-export (embedder
pattern)"). `viusage` has no equivalent standalone re-export subpackage of its own; instead,
root `blockpack` (which tempo already imports for everything else in this feature) carries
the re-export directly.

**Deliberately does NOT re-export `BackfillEngine`/`BackfillConfig`/`BlockFetcher`/
`BackfillProgress`** — those are root-NATIVE types already (`valueindex_backfill.go`, SPEC-
VIUSAGE-5's package-placement note), not `viusage` types needing re-export at all. This file's
own doc comment states this distinction explicitly, so a reader does not go looking for a
`BackfillEngine` alias here and wonder why it is missing.

Back-ref: `valueindex_usage.go` (its own doc comment makes this same argument). See `SPECS.md`
SPEC-VIUSAGE-1/3/4/6 (the aliased contracts) and SPEC-VIUSAGE-5 (the root-native types this
file deliberately excludes).

---

## NOTE-VIUSAGE-10 — `ErrNotFound`: fixing a real data-loss bug where `Registry.Load` conflated a genuine `ObjectStore.Get` error with "not found"

Date: 2026-07-10

**The bug (go-presubmit.md CRITICAL finding, holistic post-integration review):**
`Registry.Load` originally inferred "not found" purely from the SHAPE of `ObjectStore.Get`'s
return value — `err != nil && len(data) == 0 && etag == ""` was treated as an empty index,
with the `fmt.Errorf(...)` propagation line reachable only when that shape check failed. The
problem: every real `ObjectStore.Get` implementation this codebase actually uses (tempo's
`viUsageObjectStore.Get`, mirroring cube's `minioObjectStore.Get`) ALREADY signals a genuine
404 via `(nil, "", nil)` — a NIL error, not a non-nil one. This means the `err != nil` branch
in `Load` could only ever be reached by a REAL failure (permission denied, network timeout,
throttling, a corrupt SDK response) — and every one of those real failures also naturally has
`(data, etag) == (nil, "")` (an S3 client returning an error typically returns zero-value
results alongside it). So the old shape check fired for 100% of real errors, not 0%: every
transient `Get` failure was silently treated as "this tenant's registry is empty," which then
caused `updateEntryWithRetry` to persist a fresh single-entry index via an UNCONDITIONAL
`ConditionalPut` (`etag=""` on a store that treats empty-etag as create-if-not-exists,
already-exists-anyway ⇒ actual PUT with no `If-Match`), silently destroying every other
tracked column's usage/trigger/lease/watermark state for that tenant on a single flaky S3
call.

**The fix:** Added a typed `ErrNotFound` sentinel to `ObjectStore`'s contract.
`ObjectStore.Get` implementations MUST now return `ErrNotFound` (wrapped or bare) — never a
nil error with empty data — to signal a genuine miss; `Registry.Load` now checks
`errors.Is(err, ErrNotFound)` exclusively, and ANY other non-nil error propagates
unconditionally regardless of the accompanying `(data, etag)` shape. Root-package
`valueindex_usage.go` re-exports `ErrNotFound` alongside `ErrConflict` for external
`ObjectStore` implementations (tempo's `viUsageObjectStore`). Note that today's real
implementations (both viusage's tempo-side store and cube's own) already signal not-found
via a nil error, so this fix's practical effect is entirely on the previously-dead
`fmt.Errorf` branch: it is now genuinely reachable and correctly returns real errors instead
of swallowing them.

**Cross-reference, FIXED as a separate bonus fix (team-lead-approved, out of #496's own
scope but explicitly authorized):** the identical latent shape-based-inference bug
existed in `internal/modules/cube/registry.go`'s `Load` (R1 said copy cube's pattern, not
import it — the bug came along with the copy, unnoticed until this task's holistic
review). Cube's own `Load` had the exact same `err != nil && len(data) == 0 && etag == ""`
shape check, and cube's own real `Get` implementation (`minioObjectStore.Get`,
`cubemanager.go`) had the exact same nil-error-on-404 / non-nil-error-with-empty-shape-on-
real-failure behavior — meaning cube's registry carried the identical data-loss exposure.
Originally disclosed here as a standalone, NOT-fixed finding (mirroring NOTE-VIUSAGE-5's
R9 disposition), the team lead subsequently approved applying the identical fix to cube's
own `registry.go`/`ObjectStore` as a deliberate, small, well-understood bonus fix (mirrors
the #494 precedent for bonus-fixing adjacent code once a pattern is well-understood): added
`cube.ErrNotFound`, updated `cube.Registry.Load` to check `errors.Is(err, ErrNotFound)`
exclusively, updated tempo's `minioObjectStore.Get` (`cubemanager.go`) to return
`blockpack.CubeErrNotFound` explicitly on a genuine 404, re-exported `CubeErrNotFound`
at root (`cube_ingest.go`, alongside `CubeErrConflict`), and added the identical
regression-test pair (`TestRegistry_Load_RealErrorNotConflatedWithNotFound`/
`_ErrNotFoundTreatedAsEmpty`, `internal/modules/cube/registry_test.go`), mutation-tested
the same way. This is the ONLY change made to cube's code as part of this bonus fix — the
two OTHER standalone cube findings from this task (the R9 watermark-persistence gap,
Section 1; a protobuf-wiring gap noted elsewhere) remain untouched and out of scope.

Back-ref: `registry.go`'s `ObjectStore`/`ErrNotFound` doc comments,
`TestRegistry_Load_RealErrorNotConflatedWithNotFound`/`TestRegistry_Load_ErrNotFoundTreatedAsEmpty`
(`registry_test.go`), `valueindex_usage_test.go`'s `TestErrNotFound_ForwardsSameSentinel`,
and (for the cube bonus fix) `internal/modules/cube/registry.go`/`registry_test.go`,
`cube_ingest.go`'s `CubeErrNotFound` re-export, tempo's `cubemanager.go`.

---

## NOTE-VIUSAGE-11 — `BackfillEngine.processBlocks`: fixing an R7 correctness gap where an intermediate progress claim could overstate coverage a not-yet-processed block still had real data in

Date: 2026-07-10

**The bug (go-presubmit.md HIGH finding):** `processBlocks` (`valueindex_backfill.go`)
previously tracked a "running min" of each fetched block's REAL per-span content range
(`blockCoverageRangeSec`), lowering `BackfillProgress.WatermarkSec` after EVERY block and
reporting it via `progressFn` — which R9 requires the caller to persist via
`Registry.UpdateWatermark` on every call, specifically so a concurrent query mid-backfill
sees genuine, live progress. The bug: `refs` (from `BlockFetcher.ListBlocksInRange`) is
sorted newest-to-oldest by each block's NOMINAL time (tempo's `viBlockFetcher` sorts by
`BlockMeta.StartTime`) — NOT by real per-span content time. Late-arriving data, clock
skew, or multi-writer flush jitter can make a not-yet-processed ("nominally older") block
contain REAL span data that falls squarely inside a range an EARLIER `progressFn` call
already reported as covered. Concretely: process block A (real content down to 3h ago),
report `WatermarkSec = 3h-ago` — a concurrent query with `minSec` between "3h ago" and
"now" would see this watermark and conclude "covered," even though block B (not yet
processed, "nominally older" but with REAL content as recent as 1h ago) hasn't been
written to the index yet. This is exactly R7's "false complete answer assembled from
partial data" bug, just manifesting via an INTERMEDIATE persisted state rather than only
the final one — none of R7's existing adversarial tests (4.8, `valueindex_watermark_test.go`)
exercised this because they all inject a `ColumnWatermark`/`BackfillState` value directly
rather than deriving it from a realistic multi-block backfill run with out-of-nominal-order
real content.

**Why extending `BlockFetcher` with per-block nominal bounds does not fully close the
gap:** an earlier draft of this fix considered passing each candidate's nominal
`StartSec`/`EndSec` through the `BlockFetcher` interface so `processBlocks` could bound
how far it safely advances mid-run using an unprocessed block's own declared range as a
floor. This does not work: the review's own threat model (clock skew, late-arriving data)
is precisely a case where a block's NOMINAL metadata does not reliably bound its REAL
content — trusting nominal bounds as a safety floor would just relocate the same
metadata-trust assumption one level down, not eliminate it. The only way to make an
incremental claim provably safe without inspecting a block's real content is to inspect
EVERY block's real content first — at which point there is nothing left to be
"incremental" about for the coverage claim itself.

**The fix:** `processBlocks` no longer advances `WatermarkSec` below the window's own
newest edge (`maxSec`) on any call before the run's LAST block. `WatermarkSec = maxSec`
is the safe "nothing beyond the window's boundary is confirmed yet" sentinel —
`BackfillState.CoversRange`/`ColumnWatermark.CoversRange` require `minSec >=
WatermarkSec` to consider a range covered, which a `WatermarkSec` of `maxSec` never
satisfies for any real historical query, so every intermediate call correctly declines.
Only the FINAL call (after every listed block has genuinely been fetched and written)
advances `WatermarkSec` to `minSec` with `Done=true` — and `Done=true` independently
makes `CoversRange` always return true regardless of `WatermarkSec`'s specific value, so
the exact reported value on that final call is not itself safety-critical (kept at
`minSec` for observability/continuity with existing tests, not because `CoversRange`
depends on it once `Done` is set).

This still satisfies R9's own requirement ("the caller MUST actually call
`Registry.UpdateWatermark` from `progressFn`" — VI's own instance of the persistence-discipline
NOTE-VIUSAGE-5 required VI to close for itself, independent of and not to be confused with
cube's own SEPARATE, actual watermark-persistence gap, which NOTE-VIUSAGE-5's own later
addendum records as fixed too, in tempo's `cube_backfill.go`, task #127 — Section
1/NOTE-VIUSAGE-5) — the call happens on every block, unchanged — it just no longer lets an
intermediate call make a coverage claim this loop cannot yet prove safe. The accepted,
disclosed tradeoff: a long-running, many-block backfill no longer shows incrementally
advancing partial coverage to concurrent queries mid-run (R9's intended nice-to-have) —
every query against a Triggered-but-not-Done column declines until the ENTIRE backfill
window completes in one pass, then flips straight to fully covered. Given R7's own
explicit priority ("the single most important correctness gate in the whole #496 feature,"
"100% coverage, no mostly-tested outcome") over R9's visibility improvement, this is the
correct tradeoff, not a compromise — R9 was always framed as "better than cube's total
omission," never as a guarantee of granular mid-run visibility.

`blockCoverageRangeSec` (the per-block real-content inspection function this fix made
unused for the watermark decision) was removed rather than left as dead code.

Back-ref: `valueindex_backfill.go`'s `processBlocks` doc comment (the fullest technical
explanation), `TestBackfillEngine_OverlappingOutOfOrderBlocksNeverOverstatesCoverage`
(the adversarial regression pin — constructs exactly the late-arriving/out-of-nominal-order
scenario above and asserts the intermediate watermark never overstates coverage),
`TestBackfillEngine_WatermarkValuesAcrossMultiBlockRun` (updated to assert the new,
correct intermediate-sentinel behavior instead of the old, unsafe incremental-advance
behavior it previously pinned). Tempo's `viBlockFetcher.ListBlocksInRange`
(`vi_backfill.go`) doc comment updated to clarify its nominal sort is a
processing-preference/observability aid only, not a safety-relevant ordering `processBlocks`
depends on anymore.

---

## NOTE-VIUSAGE-12 — R4 reopened: repeated-use threshold removed for an unconditional first-use trigger; new file-catalog cursor (task #154)

Date: 2026-07-11

**Team-lead ruling (2026-07-11):** NOTE-VIUSAGE-3's original R4 rationale for the repeated-use
threshold — filtering one-off/exploratory queries out of triggering an expensive 48h
raw-block backfill — is reversed. Any query against a non-dedicated column is now considered
worth indexing immediately: the FIRST recorded use of a never-triggered column always fires a
backfill, with no distinct-use count or rolling time window evaluated at all. The accepted
tradeoff, disclosed explicitly rather than silently absorbed: a single one-off query now
does trigger a full 48h historical backfill, exactly the cost NOTE-VIUSAGE-3's threshold
existed to avoid — the team lead judged the added latency/complexity of tracking a
rolling-window use count was not worth that protection, given #496's usage registry is
itself the mechanism meant to surface real production usage patterns that could recalibrate
this later.

**What changed in code (task #154):**
- `TriggerConfig{Threshold int, WindowSeconds uint64, LeaseTTLSeconds uint64}` shrank to
  `TriggerConfig{LeaseTTLSeconds uint64}` — `Threshold`/`WindowSeconds` removed entirely; only
  the R8 lease-TTL bound remains.
- `Entry.UseTimestamps []uint64`, `MaxTrackedUses` (32), `Registry.recordUse`, and
  `pruneUseTimestamps` are all removed — once there is no count left to answer, there is
  nothing left for a bounded use-timestamp ring to support.
- `RecordUseAndMaybeTrigger`'s decision table collapsed the
  `!Triggered && len(prunedUseTimestamps) >= cfg.Threshold` branch into a bare
  not-yet-`Triggered` → always-trigger case (`default:` in the Go `switch`). The R8 lease
  acquire/renew/release/crash-self-heal lifecycle (NOTE-VIUSAGE-1's `BackfillState.Triggered`
  "never reverts" rule, and the whole of SPEC-VIUSAGE-3's lease mechanics) is completely
  unchanged — only "what causes a first-time trigger" changed, not what happens once
  triggered.
- **New, unrelated to the threshold removal but landed in the same task:**
  `BackfillState.LastCatalogRowID uint64` + `Registry.UpdateCatalogCursor(ctx, tenant,
  colHash, colType, rowID) error` — a monotonic cursor into tempo's Postgres-backed
  `file_catalog` table (Part 3 of this same multi-repo effort; task #169's
  `catalogBlockFetcher` is the intended caller). This lets a catalog-backed `BlockFetcher`
  implementation persist how far it has listed catalog rows for a column between backfill
  runs, so a re-run does not need to re-list already-processed rows. Deliberately independent
  of R7/R9's watermark/lease machinery (`WatermarkSec`/`Done`/the lease fields) — a catalog
  cursor tracks progress through a row-ID-ordered Postgres listing, not backfill time-window
  coverage, and nothing else in this package reads or writes it.

**Why the threshold's removal and the catalog cursor addition landed in the same task, despite
being unrelated concerns:** both are part of the same broader Postgres-backed
registry/backfill-state migration (this effort's `plan.md`, Part 0 and Part 3) — the threshold
removal is Part 0's own simplification pass, and the catalog cursor is new state Part 3's
catalog-based `BlockFetcher` needs; they happened to be implemented by the same task (#154)
because both touch `Entry`/`TriggerConfig`/`Registry`'s core shape, not because one caused the
other.

Back-refs: `internal/modules/viusage/entry.go:BackfillState.LastCatalogRowID`,
`internal/modules/viusage/trigger.go:TriggerConfig,RecordUseAndMaybeTrigger`,
`internal/modules/viusage/registry.go:Registry.UpdateCatalogCursor`. See `SPECS.md`
SPEC-VIUSAGE-8 (trigger contract) and SPEC-VIUSAGE-9 (catalog-cursor contract), and this
entry's own addendum to NOTE-VIUSAGE-3 above.

---

## NOTE-VIUSAGE-13 — `entryStore`: introducing a private storage abstraction so `Registry` can sit on Postgres, not just S3/Local/GCS/Azure blobs (tasks #157/#159)

Date: 2026-07-11

**Motivation:** the opt-in Postgres-backed registry work (tempo cross-repo task) needed
`Registry` to optionally persist through one Postgres row per `(tenant, colHash, colType)`
instead of the existing whole-tenant-blob `<tenant>/viusage/index.json` + conditional-PUT
pattern — without changing any of `Registry`'s existing public methods
(`Load`/`RenewLease`/`UpdateWatermark`/`UpdateCatalogCursor`/`RecordUseAndMaybeTrigger`),
since tempo's callers must not care which backend is active.

**Design:** `Registry.store` changed type from `ObjectStore` directly to a new private
`entryStore` interface (`load`/`upsertEntry`, `entry_store.go`) — `blobEntryStore` is the
one current implementation, wrapping `ObjectStore` with the EXACT SAME whole-tenant-blob +
conditional-PUT-retry body that used to live directly on `Registry`
(`Registry.updateEntryWithRetry`'s body moved verbatim into `blobEntryStore.upsertEntry`;
`Registry.Load`'s body moved into `blobEntryStore.loadWithETag`, with `Registry.Load`
becoming a one-line delegator that always returns `""` for the etag — confirmed zero
external callers ever consumed that return value meaningfully). This refactor is
explicitly BEHAVIOR-PRESERVING: the full pre-refactor test suite (24 tests) passes
UNMODIFIED in name and assertion after the refactor; only two test call sites needed a
MECHANICAL target change (`r.updateEntryWithRetry(...)` → `r.store.upsertEntry(...)`,
since the method moved and `registry_test.go` is in-package) — tracked and resolved as
task #176, not treated as a sign the refactor broke anything.

**The Go-visibility trap (why there are TWO interfaces, not one):** the natural next step
— exporting `entryStore` directly so tempo's Postgres implementation could satisfy it —
does not work: Go enforces unexported method names (`load`/`upsertEntry`) as
package-private, and no type outside this package can structurally implement an interface
whose method names are unexported, even via a `type EntryStore = entryStore` alias (the
alias does not rename the methods). The fix: a SECOND, exported interface,
`EntryStore{Load, UpsertEntry}` (capitalized method names), plus a tiny
`externalEntryStoreAdapter` that embeds an `EntryStore` and forwards its capitalized
methods to satisfy the unexported `entryStore` interface `Registry` actually holds.
`NewRegistryFromEntryStore(store EntryStore, tenant string) *Registry` constructs a
`Registry` over this adapter — mirrors exactly how `blobEntryStore` adapts `ObjectStore`,
both being "adapt an external, differently-shaped interface into the one `Registry` itself
depends on" instances of the same pattern.

**Ownership split (blockpack defines the interface, tempo brings pgx):** this mirrors how
`ObjectStore` already works — blockpack never imports a SQL driver; `EntryStore` is the
narrow contract, and tempo's `pgViUsageEntryStore` (pgx-backed, tempo repo) is the concrete
implementation. blockpack's `go.mod` gains zero new dependencies from this work.

**New public API surface (flagged for sign-off, not silently shipped):** `EntryStore`
(interface) and `NewRegistryFromEntryStore` (constructor), both re-exported at blockpack
root via `valueindex_usage.go`.

Back-ref: `entry_store.go:entryStore,EntryStore,externalEntryStoreAdapter,
NewRegistryFromEntryStore`; `registry.go:blobEntryStore` (SPEC-VIUSAGE-4's update);
`valueindex_usage.go` (root re-export). Tests:
`TestNewRegistryFromEntryStore_DelegatesToProvidedStore`,
`TestNewRegistryFromEntryStore_SameBehaviorAsObjectStoreBacked`
(`valueindex_usage_test.go`).

---
