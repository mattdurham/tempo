# viusage — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/viusage` package. It complements `NOTES.md` (design rationale) and
`TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VIUSAGE-N`. This is a
**distinct, independent numbering space** from the three existing value-index modules'
`SPEC-VI-N` (each of `valueindex/SPECS.md`, `valueindexcompactor/SPECS.md`,
`valueindexconsumer/SPECS.md` numbers its own `SPEC-VI-N` sequence from 1) and from
`NOTE-VI-N` (shared/global across those three modules' NOTES.md files by established
convention). `viusage` does not join either of those spaces: it is a genuinely separate
concern (usage tracking + repeated-use trigger + backfill orchestration) from the VI
wire-format/pipeline modules, not a fourth participant in their history — confirmed with the
team lead 2026-07-10, matching the precedent of `cube`/`valuecounts` each getting independent
prefixes rather than joining an existing module's numbering. IDs are assigned in ascending
order and never reused or renumbered; superseded entries are marked
`[SUPERSEDED by SPEC-VIUSAGE-N]` rather than deleted.

**Code-tag reconciliation note (2026-07-10):** the landed code's own `// NOTE:
SPEC-VIUSAGE-N` comments were written during A2-A6 before this file existed, and drifted
into a duplicate (`SPEC-VIUSAGE-002` used for both `registry.go` and `backfill.go`). This
file is the authoritative renumbering: `registry.go` keeps `002`; `backfill.go`'s tag should
be corrected to `004` and `config.go`'s to `005` in a follow-up code comment fix (flagged to
coder-1, not yet landed as of this writing — track until confirmed).

Next free ID: **SPEC-VIUSAGE-10**.

---

## SPEC-VIUSAGE-1: `Entry`/`BackfillState` schema — the tenant-level usage/backfill record
*Added: 2026-07-10*

**[PARTIALLY SUPERSEDED by SPEC-VIUSAGE-8, 2026-07-11]** — the `Entry.UseTimestamps`/
`MaxTrackedUses` paragraph immediately below is retained for history but no longer reflects
current code: both were removed (team-lead ruling 2026-07-11, R4's trigger is now
unconditional on first use — see SPEC-VIUSAGE-8). `BackfillState` also gained a new field,
`LastCatalogRowID` — see SPEC-VIUSAGE-9. The rest of this entry (the key schema, the
`BackfillState` fields other than the removed ones) is still accurate.

**Contract:** `Entry` is the full, stable description of one tracked `(Tenant, ColumnHash,
ColumnType)` usage/backfill record, stored as one element of the tenant-level
`<tenant>/viusage/index.json` object (`usageIndex{Version, Entries []Entry}`, `Version=1`).
Keyed by `(Tenant, ColumnHash, ColumnType)` — `ColumnHash = valueindex.ColHash(ColumnName)`,
`ColumnType = valueindex.ColTypeName(colType)` — the identical `(colHash, colType)`
file-sharding key `internal/modules/valueindex` already uses, so a usage entry's key
trivially maps onto the same on-disk column directory VI writes to. The same column name
observed under two distinct types tracks as two independent `Entry` records (mirrors
`valueindex_l0write.go`'s own `l0Group` keying).

**[REMOVED, SPEC-VIUSAGE-8]** `Entry.UseTimestamps []uint64` was a bounded, newest-appended
ring of recent distinct-use unix-second timestamps, truncated to the most recent
`MaxTrackedUses` (32) entries whenever appended to — both on ordinary append (the
since-removed `Registry.recordUse`) and after window-pruning (`RecordUseAndMaybeTrigger`).
Removed entirely once the repeated-use threshold concept it existed to serve was removed —
see SPEC-VIUSAGE-8.

`BackfillState` is one column's backfill lifecycle sub-record, embedded in `Entry.Backfill`.
Its zero value (`Triggered: false`) is the correct default for a freshly-created `Entry` that
exists only because a use was recorded — the threshold has not yet been crossed. Fields:
- `Triggered bool` — true once the repeated-use threshold has been crossed and a backfill has
  been (or is being) started. **Never reverts to false** (R5: no eviction in v1 — see
  `NOTES.md`'s R5 entry for the asymmetry reasoning).
- `BackfillInProgress bool` + `LeaseExpiresAt uint64` + `LeaseOwnerID string` — the R8 lease
  (SPEC-VIUSAGE-3's lifecycle). `LeaseOwnerID` is observability-only (which replica/job holds
  the lease); TTL expiry, not `LeaseOwnerID`, is what correctness depends on.
- `WatermarkSec uint64` — the R7 coverage watermark: the oldest wall-clock unix-second for
  which this column's backfill is confirmed COMPLETE, given a newest-to-oldest fill direction.
  Zero until the first unit of backfill work completes.
- `Done bool` — true once the full configured backfill window `[now-WindowSeconds, now]` is
  confirmed complete. Once `Done`, ordinary file-discovery-based coverage is trusted without
  any watermark gating (R5: same upkeep as any dedicated column from this point forward).
- `WindowStartSec`/`WindowEndSec uint64` — the backfill window this entry's watermark is
  scoped to (the config value in effect when the backfill was triggered), so a later config
  change to the default window does not retroactively reinterpret an already-`Done` entry's
  coverage.
- `LastCatalogRowID uint64` (added 2026-07-11) — see SPEC-VIUSAGE-9 for the full contract.

Back-refs: `internal/modules/viusage/entry.go:Entry,BackfillState`.

---

## SPEC-VIUSAGE-2: `BackfillState.CoversRange` — the R7 query-time coverage-check primitive
*Added: 2026-07-10*

**Contract:** `(bs BackfillState) CoversRange(minSec, maxSec uint64) bool` is pure, performs
no I/O, and is the ONE function every query-path coverage decision for a non-dedicated,
usage-tracked column must call before trusting a non-empty file-discovery result:

1. `bs.Done` → `true` unconditionally (full window confirmed complete; `maxSec` is not even
   consulted — a `Done` entry is complete for any range, not merely the range it was
   triggered for, mirroring dedicated-column semantics from this point forward).
2. `!bs.Triggered` → `false` unconditionally (never indexed — no coverage at all, matches
   today's "zero files discovered" case for a column with no VI data).
3. Otherwise (in-progress, newest-to-oldest fill: the covered range is
   `[WatermarkSec, now]`) → `minSec >= bs.WatermarkSec`. The query's window is covered ONLY if
   its OLDEST point (`minSec`) is not older than the watermark; any older sub-range is
   unconfirmed and must decline. `maxSec` is not consulted in this branch either — a
   newest-to-oldest backfill's covered range has no upper bound below "now," so only the
   lower bound can ever be the source of a coverage gap.

**Boundary condition (binding):** `minSec == WatermarkSec` MUST cover (`>=`, not `>`) — a
query whose oldest point lands exactly on the watermark is asking about data the backfill has
already confirmed complete up to and including that second.

**Duplicated, not shared, with `vibuilder.ColumnWatermark.CoversRange`:** the identical logic
also exists as `vibuilder.ColumnWatermark.CoversRange` (`internal/modules/vibuilder/
watermark.go`, cross-referenced by `vibuilder/SPECS.md` SPEC-VB-4) via a structurally
identical but independently-defined value type. This is a deliberate consequence of the
package-dependency-direction constraint documented in `NOTES.md`'s "Where ColumnPolicy and
ColumnWatermark actually ended up" entry — `viusage` cannot be imported by `vibuilder` (which
root `blockpack` already imports), so the two packages each carry their own copy of the same
three-field struct and the same three-branch decision. Both copies must be kept in sync by
hand if this logic ever changes; there is no single source of truth at the type level, only at
the specification level (this entry + SPEC-VB-4 together).

Back-ref: `internal/modules/viusage/entry.go:BackfillState.CoversRange`. Test:
`TESTS.md` TEST-VIUSAGE-1 through -4 (viusage's own copy);
`vibuilder/TESTS.md`'s watermark tests cover the vibuilder copy independently.

---

## SPEC-VIUSAGE-3: `RecordUseAndMaybeTrigger`/`TriggerConfig` — record+evaluate+lease-acquire in one pass; R8 lease lifecycle
*Added: 2026-07-10*

**[PARTIALLY SUPERSEDED by SPEC-VIUSAGE-8, 2026-07-11]** — the decision table's threshold row
below and the `TriggerConfig{Threshold, WindowSeconds}` fields it references no longer exist
in current code (team-lead ruling 2026-07-11: R4's trigger is now unconditional on first use).
The rest of this entry (the lease-lifecycle mechanics: acquire/renew/release/crash-self-heal,
and the `Done`/active-lease/expired-lease rows) is UNCHANGED and still accurate — see
SPEC-VIUSAGE-8 for the corrected decision table.

**Contract:** `RecordUseAndMaybeTrigger(ctx, registry, tenant, colName, colType, now,
cfg) (TriggerResult, error)` appends one usage timestamp for `(tenant, colName, colType)`
and, in the SAME conditional-write pass (via `Registry.store.upsertEntry`, SPEC-
VIUSAGE-4), evaluates the repeated-use threshold and, if crossed and no unexpired lease is
already held, acquires the backfill lease and marks `Triggered=true`. Combining
record+evaluate+lease-acquire into one PUT avoids a record-then-separately-check race between
concurrent callers and halves registry round-trips relative to a naive two-phase design.

**[REMOVED, SPEC-VIUSAGE-8]** `TriggerConfig{Threshold int, WindowSeconds uint64,
LeaseTTLSeconds uint64}` — R4's documented, unmeasured starting defaults (`Threshold=3`,
`WindowSeconds=3600`, `LeaseTTLSeconds=1800`) were wired in by the caller (A6/tempo's B3
config plumbing). `Threshold`/`WindowSeconds` are removed; only `LeaseTTLSeconds` remains —
see SPEC-VIUSAGE-8.

**[SUPERSEDED, SPEC-VIUSAGE-8] Decision table, per call (the `switch` inside the retry's
`mutate` closure), as it existed before 2026-07-11:**
| Current state | Action |
|---|---|
| `Backfill.Done` | no-op; `ShouldBackfill=false` (R5: fully backfilled, never re-trigger) |
| `Backfill.Triggered && BackfillInProgress && LeaseExpiresAt > now` | no-op; `ShouldBackfill=false` (another owner holds an active lease) |
| `Backfill.Triggered && (!BackfillInProgress \|\| LeaseExpiresAt <= now)` | re-acquire lease; `ShouldBackfill=true` (R8 crash self-heal — see below) |
| `!Backfill.Triggered && len(prunedUseTimestamps) >= cfg.Threshold` | set `Triggered=true`, acquire lease; `ShouldBackfill=true` (first-time crossing) |
| else | no-op; `ShouldBackfill=false` (below threshold) |

**[REMOVED, SPEC-VIUSAGE-8]** Usage timestamps were pruned to `[now-WindowSeconds, now]` (via
the since-removed `pruneUseTimestamps`) BEFORE the threshold count was taken, and further
bounded to `MaxTrackedUses` — both the window prune and the count-against-threshold happened
inside the SAME retry attempt as the append. This entire mechanism was removed alongside the
threshold concept — see SPEC-VIUSAGE-8.

**R8 lease lifecycle (explicit):**
1. **Acquire** (`acquireLease`): sets `BackfillInProgress=true`,
   `LeaseExpiresAt=now+LeaseTTLSeconds`, `LeaseOwnerID=<this process's hostname:pid>` — happens
   identically whether this is a first-time trigger or a crash-self-heal re-acquisition.
2. **Renew** (`Registry.RenewLease`, SPEC-VIUSAGE-4): a long-running backfill's caller
   periodically pushes `LeaseExpiresAt` forward via the same conditional-PUT retry discipline
   — required because a real 48h/multi-worker backfill run may legitimately outlast one
   `LeaseTTLSeconds` period even without crashing.
3. **Release**: NOT a separate step — `Registry.UpdateWatermark(..., done=true)`
   (SPEC-VIUSAGE-4; the caller's final `progressFn`-driven persistence call, Part B's
   launcher) sets `Done=true` AND `BackfillInProgress=false` in the SAME conditional-PUT,
   never a separate release step that could leave `Done=true` with the lease still held.
   `CoversRange`'s `Done`-first check (SPEC-VIUSAGE-2) means a stale `BackfillInProgress=true`
   alongside `Done=true` would be harmless for correctness even if this were somehow missed —
   but `UpdateWatermark`'s own contract guarantees it is not.
4. **Crash self-heal**: if the backfill process dies mid-run with no clean release, the
   lease's `LeaseExpiresAt` simply passes; the row in the decision table above
   (`Triggered && lease expired`) fires on the next call from ANY replica, re-acquiring the
   lease and returning `ShouldBackfill=true` again. No manual intervention, no separate reaper
   process, no additional state — self-heal is a property of the decision table, not a
   distinct mechanism.

**Idempotency:** a caller whose own call did not win the lease (a concurrent replica's call
did, within the same conditional-PUT race) gets `ShouldBackfill=false` with the same `Entry` —
exactly one caller's `ShouldBackfill` is `true` per crossing/re-acquisition, thanks to the
lease.

Back-refs: `internal/modules/viusage/trigger.go:RecordUseAndMaybeTrigger,TriggerConfig,
TriggerResult,acquireLease`. Tests: `TESTS.md` TEST-VIUSAGE-5 through -11 (lease-lifecycle
portions still accurate; threshold-specific assertions superseded — see TESTS.md's own
2026-07-11 update).

---

## SPEC-VIUSAGE-4: `Registry` — conditional-PUT-with-retry object storage contract
*Added: 2026-07-10*

**[UPDATED, 2026-07-11 — Part 1.3 `entryStore` refactor, non-behavioral]** `Registry` no
longer talks to `ObjectStore` directly: it holds a package-private `entryStore` interface
(`load`/`upsertEntry`), and `blobEntryStore` (the same file) is the ONLY current
implementation, wrapping `ObjectStore` exactly as described below — this entry's contract is
unchanged from the caller's perspective, only the internal structure moved
(`Registry.updateEntryWithRetry` no longer exists; its body is now `blobEntryStore.
upsertEntry`, and every mutator below calls `r.store.upsertEntry` instead). This refactor
exists so `Registry` can ALSO sit on top of a Postgres-backed `EntryStore` (exported,
2026-07-11) without any change to `Registry`'s own public methods — see the new
`entry_store.go` and `NOTES.md`'s corresponding entry for the "why."

**Contract:** `Registry` loads and persists one tenant's usage index
(`<tenant>/viusage/index.json`, `usageIndex{Version: 1, Entries []Entry}`) from an injected
`ObjectStore{Get, ConditionalPut}` interface. `ObjectStore` mirrors `internal/modules/
cube.ObjectStore`'s contract (`Get(ctx, path) (data []byte, etag string, err error)`;
`ConditionalPut(ctx, path, data, etag string) error`, where `ConditionalPut` signals an
ETag mismatch via `ErrConflict`) — **per R1, this is the design pattern copied, not shared
code**: `viusage.ObjectStore` is an independent, package-local type with zero import of
`internal/modules/cube` — **EXCEPT for one binding, deliberate divergence added 2026-07-10
(go-presubmit.md CRITICAL Fix C, see below): `Get` MUST signal a genuine miss by returning
the typed `ErrNotFound` sentinel (wrapped or bare) — NEVER by returning a nil error alongside
an empty `(data, etag)`.** `cube.ObjectStore`'s own contract has no equivalent requirement
(see `NOTES.md` NOTE-VIUSAGE-10's cross-reference for why this matters and why cube's own
copy of the pattern was NOT also fixed, out of #496's scope).

**`ErrNotFound` / `Registry.Load`'s corrected not-found contract (binding, CRITICAL fix,
2026-07-10):** `Registry.Load(ctx) ([]Entry, string, error)` returns an empty index
(`nil, "", nil`) IF AND ONLY IF `ObjectStore.Get` returns an error satisfying
`errors.Is(err, ErrNotFound)` — checked by ERROR IDENTITY, never inferred from the
accompanying `(data, etag)` value SHAPE. **This corrects a real, CRITICAL data-loss bug:**
`Load` previously inferred "not found" from `err != nil && len(data) == 0 && etag == ""` —
a value-shape check that cannot distinguish a genuine miss from a real transient failure
(permission denied, network timeout, throttling), since a real SDK error typically ALSO
returns zero-value `(data, etag)` alongside it. Every real `Get` failure was therefore
silently treated as "this tenant's registry is empty," which caused
`updateEntryWithRetry`'s subsequent `ConditionalPut` (with `etag=""`, an unconditional write on
a store that treats empty-etag as create-if-not-exists) to overwrite and destroy every OTHER
tracked column's usage/trigger/lease/watermark state for that tenant on a single flaky S3
call. Now: `errors.Is(err, ErrNotFound)` → empty index (`nil, "", nil`); ANY other non-nil
error → propagates wrapped (`fmt.Errorf("viusage registry: load: %w", err)`), regardless of
its own `(data, etag)` shape. Re-exported at root via `valueindex_usage.go` (`blockpack.
ErrNotFound = viusage.ErrNotFound`) for external `ObjectStore` implementations (tempo's
`viUsageObjectStore`) to return.

**`updateEntryWithRetry`'s retry discipline (binding, shared by every mutator in this
package):** load → locate entry by `(tenant, colHash, colType)`, creating it via
`createIfMissing` if absent (or erroring if `createIfMissing` is nil and no entry exists) →
invoke `mutate(*Entry)` to apply changes → re-encode the WHOLE index → `ConditionalPut` with
the just-loaded ETag → on `ErrConflict`, sleep (50ms, doubling per attempt) and retry, up to 5
attempts total, then return a typed "exceeded retries" error. **`mutate` is invoked once PER
RETRY ATTEMPT, against freshly-reloaded state each time** — a conflicting concurrent write
requires re-evaluating against current contents, so `mutate` closures (both `recordUse`'s and
`RecordUseAndMaybeTrigger`'s) MUST derive their decision from the `*Entry` parameter's current
field values on every invocation, never from closure-captured, pre-computed values from before
the retry loop started. This is what makes `RecordUseAndMaybeTrigger`'s threshold/lease
decision table (SPEC-VIUSAGE-3) safe under concurrent retries — SPEC-VIUSAGE-3's own text
depends on this.

`Registry.RenewLease(ctx, tenant, colHash, colType, newExpiresAt) error` pushes an existing
entry's `Backfill.LeaseExpiresAt` forward via the same retry discipline; errors if the entry
does not exist (mirrors cube's `UpdateWatermarks` "cube not found" behavior — renewing a lease
implies the entry was already created by a prior trigger/acquire call).

**`Registry.UpdateWatermark(ctx, tenant, colHash, colType, watermarkSec, windowStartSec, windowEndSec uint64, done bool) error`** (added after this entry was first written, closing
the R9 gap this module exists to avoid repeating — see `NOTES.md` NOTE-VIUSAGE-5's addendum)
persists one backfill-progress update via the SAME retry discipline. `WatermarkSec`/
`WindowStartSec`/`WindowEndSec` are written on every call (re-writing the window bounds on
every call within one run is a harmless no-op once they are first set — this is the ONLY
place these three fields are ever set; neither `RecordUseAndMaybeTrigger` nor `recordUse`
touches them). `done=true` ADDITIONALLY sets `Done=true` AND releases the lease
(`BackfillInProgress=false`) in the SAME conditional-PUT — per SPEC-VIUSAGE-3's Release step
(3), never a separate release step that could leave `Done=true` with the lease still held.
Errors if the entry does not exist (mirrors `RenewLease`'s own contract — a backfill run's
`progressFn` is only ever invoked after `RecordUseAndMaybeTrigger`'s `ShouldBackfill=true`
already created the entry). **This is the concrete method `BackfillEngine.Run`'s own
`progressFn` contract (SPEC-VIUSAGE-5) expects its caller to invoke on every progress call, not
just on `Done`** — the actual persistence mechanism R7/R9 require, now that it exists as a
named, tested method rather than only a textual "the caller's job" description.

Back-refs: `internal/modules/viusage/registry.go:Registry,ObjectStore,ErrConflict,ErrNotFound,
blobEntryStore,RenewLease,UpdateWatermark,UpdateCatalogCursor,indexVersion,usageIndex`;
`internal/modules/viusage/entry_store.go:entryStore,EntryStore,externalEntryStoreAdapter,
NewRegistryFromEntryStore`. Tests: `TESTS.md`
TEST-VIUSAGE-12 through -18 (Load/retry/RenewLease), TEST-VIUSAGE-35 through -37 (UpdateWatermark), TEST-VIUSAGE-38/39 (ErrNotFound/Load error-identity fix).

---

## SPEC-VIUSAGE-5: `BackfillEngine`/`BackfillConfig`/`Run` — raw-block-read backfill contract, existing-file-layout guarantee
*Added: 2026-07-10*

**Contract:** `BackfillEngine.Run(ctx, progressFn) error` processes one column's historical
backfill by reading RAW historical blocks (via the injected `BlockFetcher` interface), NOT
pre-extracted VI files — per R6, no pre-extracted data exists for a never-indexed column.

`BlockFetcher{ListBlocksInRange, FetchBlock}`: `ListBlocksInRange` returns block source-ref
keys overlapping `[minSec, maxSec]` **newest-first** (the ordering guarantee is the fetcher's
responsibility, not the engine's — mirrors cube's fill direction for the same "recent data
usable soonest" reason); `FetchBlock` opens a `*blockpack.Reader` via the existing
single-I/O-per-block read path (this repo's core I/O invariant, ARCH-002/003 — `Run` performs
no per-column I/O of its own, delegating entirely to the existing block-fetch path).

**Window resolution:** `maxSec = now` (via the injected `cfg.Now`, defaulting to `time.Now`);
`minSec = maxSec - cfg.WindowSeconds` (or `0` if that would underflow). Defaults applied by
`NewBackfillEngine` for any zero-valued `BackfillConfig` field: `WindowSeconds` → 48h
(`defaultBackfillWindowSeconds`, R4), `Workers` → 4 (`defaultBackfillWorkers`, mirrors cube's
tempo-side override default). **`Workers` is accepted as a config field but not yet consulted
by `Run`'s current implementation** — `processBlocks` runs strictly serially over `refs`, one
block at a time; parallelizing across `Workers` concurrent fetches is not implemented in this
pass (flagged here so a future reader does not assume the field is load-bearing yet — see
`NOTES.md` for whether/when this should change).

**Per-block processing:** for each fetched block, extraction is scoped to EXACTLY
`entry.ColumnName` via `ExtractValueIndexEntriesForColumns` (root package, SPEC-VI-11's
allowlist wrapper), then further filtered to entries whose extracted `ColType`'s
`valueindex.ColTypeName` matches `entry.ColumnType` exactly — a column observed as a
DIFFERENT type elsewhere in history belongs to a distinct `Entry`/key
`(Tenant, ColumnHash, ColumnType)` and must not be folded into this run. Surviving entries are
grouped by `ColType` (defensive; normally exactly one type survives the filter) and each group
is flushed via `blockpack.FlushAndPutValueIndexColumn` — **the EXACT SAME file-key convention
`WriteValueIndexL0`/`flushAndPutL0` uses**
(`<tenant>/<indexPrefix>/<colHash>/<typeName>/L0-<min>-<max>-<id>.blockpack`), so
`valueindexcompactor`'s normal column-scoped lap discovers these files with ZERO
special-casing (R6). This exact-key-match claim is the single most load-bearing assertion in
this module's test suite — see `TESTS.md` TEST-VIUSAGE-21's callout.

**`progressFn` contract:** called once per fetched block (never batched), reporting
`BackfillProgress{WatermarkSec, WindowStartSec, WindowEndSec, Done, LastError}`. `WatermarkSec`
tracks the minimum block-coverage `MinStart` seen so far across processed blocks (via
`blockCoverageRangeSec`, derived from every inner block's `MinStart`/`MaxStart`,
**independent of whether the target column has any values in that block** — an entirely-empty
block for the target column still correctly advances the watermark past it, since the
watermark tracks TIME coverage, not per-column data presence). On the LAST block
(`i == len(refs)-1`), `WatermarkSec` is forced to `minSec` (the window's own oldest bound) and
`Done=true` — closing any rounding gap between the last block's own `MinStart` and the
window's configured edge. **`Run` itself never persists anything** — per R7/R9 (see
`plan.md` Section 1's cube-watermark-gap finding), persisting `BackfillState.WatermarkSec`/
`Done` to the registry via `Registry.UpdateWatermark` (SPEC-VIUSAGE-4 — the concrete method
`updateEntryWithRetry`-based persistence uses) is the CALLER's job, invoked from inside
`progressFn`. This is a deliberate, explicit design choice made BECAUSE cube's own
`progressFn`-equivalent wiring was independently verified (plan.md Section 1) to never persist
a watermark at all — VI's own contract does not repeat that gap; `Registry.UpdateWatermark`
exists specifically to give the tempo-side caller (Part B, in progress as of this writing) a
single, concrete, tested method to call from `progressFn` on every progress update, not just
on `Done`, exactly what R9's finding says cube's own wiring omits.

**Empty-range case:** zero blocks in `[minSec, maxSec]` → `progressFn` is called exactly once
with `{WatermarkSec: minSec, WindowStartSec: minSec, WindowEndSec: maxSec, Done: true}` and
`Run` returns `nil` — no blocks fetched, nothing written, but the caller still receives a
terminal `Done` signal so it can mark the column fully backfilled (there was nothing to
backfill).

**Package-placement note (2026-07-10, updated after this entry was first written):**
`BackfillEngine`/`BackfillConfig`/`BlockFetcher`/`BackfillProgress` all live in the ROOT
`blockpack` package (`valueindex_backfill.go`), not `internal/modules/viusage` as this
entry originally documented — moved for the same reason `ColumnPolicy` (`SPEC-VI-11`)
already lives in root: `BackfillEngine` imports root directly (`*Reader`/`ObjectPutter`/
`ExtractValueIndexEntriesForColumns`/`FlushAndPutValueIndexColumn`). The forcing constraint
is sharper than a simple "avoid a two-file cycle" — **Go import cycles are per-PACKAGE, not
per-file**: as long as ANY file in `viusage` imported root `blockpack` (which
`BackfillEngine` must), root could never import `viusage` for ANYTHING, including the
OTHER types (`Entry`/`Registry`/`Config`/`TriggerConfig`/etc.) that `valueindex_usage.go`
needs to re-export back out to external callers (tempo's Part B, which cannot import
`internal/modules/viusage` directly). Moving `BackfillEngine` fully into root — not merely
having it import root, which was already true before this move and did not by itself force
anything — is what keeps `viusage` a true leaf package with respect to root, making the
`valueindex_usage.go` re-export file possible at all. See `NOTES.md` NOTE-VIUSAGE-7 for the
third instance of this same recurring pattern (`ColumnPolicy`, `ColumnWatermark`, and now
this).

**Cancellation:** `ctx` is checked before fetching each block AND again after each block's
`progressFn` call; `Run` returns `ctx.Err()` without processing further blocks. A pre-canceled
context is observed before the FIRST block fetch.

Back-refs: `valueindex_backfill.go:BackfillEngine,BackfillConfig,BlockFetcher,
BackfillProgress,Run,processBlocks,extractAndWriteBlock,blockCoverageRangeSec,
NewBackfillEngine` (root package -- see the package-placement note above). Tests: `TESTS.md` TEST-VIUSAGE-19 through -30.

---

## SPEC-VIUSAGE-6: `Config`/`MaybeRecordUseAndMaybeTrigger` — R12 safety valve, both sides gated by one boolean
*Added: 2026-07-10*

**Contract:** `Config{DedicatedColumnsEnabled bool}` (`DefaultConfig()` returns
`{DedicatedColumnsEnabled: true}` — R12's documented default: the feature is enabled by
default once complete). `MaybeRecordUseAndMaybeTrigger(ctx, cfg, registry, tenant, colName,
colType, now, triggerCfg) (TriggerResult, error)` is `RecordUseAndMaybeTrigger` gated by
`cfg.DedicatedColumnsEnabled`: when false, it returns a ZERO `TriggerResult` immediately with
NO registry I/O whatsoever (not merely "no trigger fires" — no `Load`, no `ConditionalPut`,
nothing touches the registry at all).

**Binding requirement (R12's full scope):** a caller (tempo's B1 usage-recording hook, Part B,
not yet landed as of this writing) MUST call `MaybeRecordUseAndMaybeTrigger`, not
`RecordUseAndMaybeTrigger` directly, so that a single `Config.DedicatedColumnsEnabled=false`
disables BOTH halves of #496 simultaneously: the forward write-path policy
(`blockpack.ColumnPolicy.Enabled`, SPEC-VI-11, valueindex's spec domain) AND this
usage-tracking/trigger path. R12's own text is explicit that disabling the feature must mean
"no usage-tracking/backfill machinery engaged at all," not merely "no new columns get
dedicated status" — a caller that bypasses this gate and calls
`RecordUseAndMaybeTrigger` directly would violate that contract even with
`ColumnPolicy.Enabled=false` elsewhere, since it is a SEPARATE boolean unless the caller wires
them together correctly. This function is the enforcement point that makes wiring them
together mechanical rather than a caller-remembered convention.

Back-refs: `internal/modules/viusage/config.go:Config,DefaultConfig,
MaybeRecordUseAndMaybeTrigger`. Tests: `TESTS.md` TEST-VIUSAGE-31 through -33.

---

## SPEC-VIUSAGE-7: `DefaultDedicatedColumns` — the R2 provisional bootstrap list
*Added: 2026-07-10*

**Contract:** `DefaultDedicatedColumns []string` is #496's provisional bootstrap dedicated-
column list (R2), sourced from Tempo's Parquet-14 `defaultDedicatedColumns`
(`tempo/tempodb/backend/block_meta.go:151-169`), translated into blockpack's scope-prefixed
column-name convention (`resource.`/`span.` prefixing, `internal/modules/blockio/writer/
config.go:15`). Includes the 4 "legacy" HTTP semconv aliases (`http.method`, `http.url`,
`http.route`, `http.status_code`) as `span.http.method` etc. — see `NOTES.md`'s A0 finding
entry for the evidence this decision rests on (Tempo's `block_meta.go` still returns all 4 as
current, non-deprecated defaults; vparquet4's `WellKnownColumnLookups` still statically maps 3
of the 4 to live query-execution code; no TraceQL attribute-name canonicalization layer exists
anywhere that would rewrite a legacy name to its current-semconv equivalent before a query
reaches VI).

**Explicitly provisional (binding, not a final answer):** this list is disclosed as an
unmeasured starting point pending real production usage-registry telemetry, per R2's own
text. It is NOT distinct from `HardExcludedColumns` (`blockpack.HardExcludedColumns`,
SPEC-VI-11, valueindex's spec domain) — the two lists are orthogonal: `HardExcludedColumns`
permanently excludes 4 structural columns (`span:id`, `span:parent_id`, `trace:id`,
`span:start`) regardless of ANY dedicated-list membership, while `DefaultDedicatedColumns` is
the tunable, provisional "index these unconditionally" allowlist a tenant may override.

Back-ref: `internal/modules/viusage/dedicated_columns.go:DefaultDedicatedColumns`. See
`NOTES.md`'s R2/A0 entry for the full evidence trail; `blockpack.HardExcludedColumns`
(`valueindex/SPECS.md` SPEC-VI-11) for the orthogonal permanent-exclusion set.

---

## SPEC-VIUSAGE-8: `RecordUseAndMaybeTrigger` — unconditional first-use trigger (supersedes the threshold-based decision table in SPEC-VIUSAGE-3)
*Added: 2026-07-11*

**Contract (team-lead ruling 2026-07-11):** R4's repeated-use trigger is unconditional — the
FIRST recorded use of a never-triggered, non-dedicated column always fires a backfill. There
is no longer a distinct-use count or rolling time window to cross; `TriggerConfig` retains
only `LeaseTTLSeconds uint64` (R8's lease-lifecycle bound). `Entry.UseTimestamps`,
`MaxTrackedUses`, `TriggerConfig.Threshold`/`WindowSeconds`, `pruneUseTimestamps`, and the
now-dead `Registry.recordUse` (whose only effect was appending to `UseTimestamps`) are all
removed — see SPEC-VIUSAGE-1's corresponding update.

**Decision table, per call (the `switch` inside the `upsertEntry` `mutate` closure), current:**
| Current state | Action |
|---|---|
| `Backfill.Done` | no-op; `ShouldBackfill=false` (R5: fully backfilled, never re-trigger — UNCHANGED from SPEC-VIUSAGE-3) |
| `Backfill.Triggered && BackfillInProgress && LeaseExpiresAt > now` | no-op; `ShouldBackfill=false` (another owner holds an active lease — UNCHANGED) |
| `Backfill.Triggered && (!BackfillInProgress \|\| LeaseExpiresAt <= now)` | re-acquire lease; `ShouldBackfill=true` (R8 crash self-heal — UNCHANGED) |
| `default` (not yet `Triggered`, and neither of the above two `Triggered` sub-cases apply — which is exactly "not yet triggered" since `Done`/`Triggered` are mutually exclusive with it) | set `Triggered=true`, acquire lease; `ShouldBackfill=true` (first-ever use, unconditional) |

The Go implementation expresses the last row as a bare `default:` case (not an explicit
condition) — SPEC-VIUSAGE-3's `Done`/`Triggered` cases already partition every other
possibility, so `default` is the precise, exhaustive "first-ever use" case with no
threshold-comparison logic needed at all.

**Mutation-tested regression guard:** the test proving this (`TestRecordUseAndMaybeTrigger_
FiresOnFirstUse_NoThresholdField`, `trigger_test.go`) was verified by temporarily reverting
the `default` case to a no-op (simulating a regressed "never fires on first use" bug) and
confirming 3 tests fail (`...FiresOnFirstUse...`, `...AlreadyTriggeredNeverReTriggers`,
`...ConcurrentCallersOnlyOneWinsLease`) before reverting back — not merely diff-read.

Back-refs: `internal/modules/viusage/trigger.go:RecordUseAndMaybeTrigger,TriggerConfig`.
Tests: `TESTS.md`'s 2026-07-11 update (supersedes the threshold-specific portions of
TEST-VIUSAGE-5 through -11; the lease-lifecycle tests in that range are unchanged).

---

## SPEC-VIUSAGE-9: `Registry.UpdateCatalogCursor`/`BackfillState.LastCatalogRowID` — monotonic file-catalog cursor
*Added: 2026-07-11*

**Contract:** `BackfillState.LastCatalogRowID uint64` is the highest tempo `file_catalog`
row_id this column's backfill has fully processed (tempo's catalog-cursor-based
`BlockFetcher`, part of the same 2026-07-11 Postgres-registry work). Zero means "never run
against the catalog" — a catalog-backed fetcher then lists ALL rows for the tenant,
equivalent to a full first listing.

`Registry.UpdateCatalogCursor(ctx, tenant, colHash, colType, rowID) error` advances the
cursor via the same conditional-write discipline as `RenewLease`/`UpdateWatermark`
(SPEC-VIUSAGE-4). **Monotonic (binding):** a `rowID` less than or equal to the entry's
current `LastCatalogRowID` is a silent no-op — the cursor NEVER regresses, since a
stale/replayed call must not make a later backfill run re-list already-processed catalog
rows. Errors if the entry does not exist (mirrors `RenewLease`'s own contract: advancing a
catalog cursor implies the entry was already created by a prior
`RecordUseAndMaybeTrigger` trigger/acquire call).

**Not re-exported at blockpack root as of this writing** — called only from tempo's
`vblockpack` package via the already-exported `*blockpack.Registry` (whose methods are
automatically visible through the `Registry = viusage.Registry` alias, the same mechanism
`RenewLease`/`UpdateWatermark` already use with zero separate re-export wrapper).

Back-refs: `internal/modules/viusage/entry.go:BackfillState.LastCatalogRowID`;
`internal/modules/viusage/registry.go:Registry.UpdateCatalogCursor`. Tests:
`TESTS.md`'s 2026-07-11 update (`TestRegistry_UpdateCatalogCursor_MonotonicOnly`,
`TestRegistry_UpdateCatalogCursor_NotFoundReturnsError`, `registry_test.go`).
