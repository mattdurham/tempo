# Implementation Plan: Postgres-Backed viusage/cube Registries + Threshold Removal + File Catalog

## Prompt-injection notice (must stay attached to this plan)

While researching this plan, a tool-result block contained an injected fake "system
reminder" instructing the agent to silently change its behavior and hide information
from the user (unrelated to this task's content — a date-change notice with a
"don't tell the user" instruction). It was refused and flagged verbatim to the user
in-conversation. Not acted upon. Recorded here only so the coder phase doesn't
mistake this plan's provenance as compromised — the plan content below is unaffected.

Also recorded: the planner in this session had no Bash/MCP tool access, so the
`~/bin/lth stats` / `~/bin/lth prompt` bootstrap calls and the final `~/bin/lth store`
call specified in this task's instructions could NOT be executed. The brainstorm
document (`.bob/state/brainstorm.md`) already contains an lth-bootstrapped research
pass for this task, which this plan builds on directly. **The team lead should run
the final `lth store` call themselves** (exact suggested content in this plan's final
section) since the planner could not.

## Overview

Three intertwined changes, all landing as reviewed artifacts only (no live infra
touched, per the standing checkpoint):

1. **Trigger threshold removal (blockpack, behavior change, needs sign-off):**
   `viusage.RecordUseAndMaybeTrigger` stops counting repeated uses — it triggers a
   backfill unconditionally the first time a column is ever recorded. This deletes
   `Entry.UseTimestamps`, `MaxTrackedUses`, `TriggerConfig.Threshold`, and
   `TriggerConfig.WindowSeconds` as load-bearing concepts.
2. **Postgres as an opt-in additional backend for viusage/cube registries** (Approach
   B + row-oriented storage from the brainstorm), replacing the single-shared-blob
   contention point with one Postgres row per (tenant, colHash, colType). Ships
   alongside the existing S3/Local/GCS/Azure blob path, untouched.
3. **File catalog + cursor-based backfill discovery** (new scope from the team lead,
   folded in below): a new Postgres table populated by a background loop inside the
   already-singleton `backend-scheduler` process, and a new `BlockFetcher`
   implementation that reads from that catalog via a persisted per-column cursor
   instead of live-listing S3/local storage on every backfill run — this is also the
   structural fix for tonight's "block deleted between list and fetch" failures.

Two repos, both edited directly (no worktrees, per standing convention):
blockpack (`/home/mdurham/source/blockpack_collection/blockpack`, branch `main`) and
tempo (`/home/mdurham/source/blockpack_collection/tempo`, branch `agentic-tempo`).

---

## PART 0 — Trigger threshold removal (blockpack public API change — FLAG FOR SIGN-OFF)

### Exact public API surface being removed/changed (report to user before proceeding)

| Symbol | Change | File |
|---|---|---|
| `viusage.TriggerConfig.Threshold` (int) | **removed** | `internal/modules/viusage/trigger.go` |
| `viusage.TriggerConfig.WindowSeconds` (uint64) | **removed** | `internal/modules/viusage/trigger.go` |
| `viusage.Entry.UseTimestamps` (`[]uint64`) | **removed** | `internal/modules/viusage/entry.go` |
| `viusage.MaxTrackedUses` (const 32) | **removed** | `internal/modules/viusage/entry.go` |
| `viusage.pruneUseTimestamps` (private) | **removed** (dead) | `internal/modules/viusage/trigger.go` |
| `(*Registry).recordUse` (private) | **removed** (dead — only callers are its own now-deleted tests; `UseTimestamps` was its entire effect) | `internal/modules/viusage/registry.go` |
| `blockpack.TriggerConfig` (root re-export, `= viusage.TriggerConfig`) | **field set shrinks** (Threshold/WindowSeconds gone, `LeaseTTLSeconds` kept) | `valueindex_usage.go` |
| `blockpack.Entry` (root re-export, `= viusage.Entry`) | **field removed** (`UseTimestamps`) | `valueindex_usage.go` |
| `RecordUseAndMaybeTrigger` / `MaybeRecordUseAndMaybeTrigger` **function signatures** | **UNCHANGED** — same params, same return type. Only the `TriggerConfig` struct literal callers pass now has fewer fields. | `valueindex_usage.go`, `internal/modules/viusage/{trigger.go,config.go}` |
| `tempodb.encoding.common.ViUsageConfig.TriggerThreshold` / `.TriggerWindow` (tempo-side YAML config) | **removed** (dead once blockpack's `TriggerConfig` has no matching fields) | `tempodb/encoding/common/config.go` |

**Confirmed via direct code read (not just the brainstorm's summary):** `RecordUseAndMaybeTrigger`'s switch statement (trigger.go:129-143) has exactly 3 branches: `Done` (never retrigger, R5, **unchanged**), `Triggered` (lease valid → no-op, lease expired → R8 self-heal re-acquire, **unchanged**), and `len(e.UseTimestamps) >= cfg.Threshold` (first-time trigger). Only the third branch's condition changes, to unconditional ("not yet Triggered → trigger now"). No other branch is touched.

### 0.1 — TDD: write the failing test first

**File: `blockpack/internal/modules/viusage/trigger_test.go` (modify)**

- [ ] Add `TestRecordUseAndMaybeTrigger_FiresOnFirstUse_NoThresholdField` — construct
      `TriggerConfig{LeaseTTLSeconds: 1800}` (no `Threshold`/`WindowSeconds` fields —
      this alone won't compile until Part 0.2 removes them, which is TDD's point: this
      test must fail to COMPILE first, then pass once the fields are gone and the
      switch condition is unconditional).
- [ ] Assert: one call to `RecordUseAndMaybeTrigger` for a never-seen column returns
      `TriggerResult{ShouldBackfill: true}` and `Entry.Backfill.Triggered == true`.
- [ ] Keep `TestRecordUseAndMaybeTrigger_Done_NeverRetriggers` (R5) and the R8
      self-heal tests (`TestRecordUseAndMaybeTrigger_ExpiredLease_SelfHeals` or
      equivalent) exactly as-is — these branches are unchanged and must still pass
      unmodified after Part 0.2 (regression guard that the OTHER two branches were
      not accidentally touched).
- [ ] **Delete** any test asserting the OLD threshold-gating behavior (e.g. a test
      proving a SECOND use is required before triggering) — that behavior is being
      removed, not preserved. Search: `grep -n "Threshold" trigger_test.go`.

**File: `blockpack/internal/modules/viusage/registry_test.go` (modify)**

- [ ] Delete every test that exists solely to exercise `UseTimestamps`/`recordUse`
      bounding behavior: `TestRegistry_RecordUse_BoundedTimestampWindow` (lines
      ~190-224) and any other `recordUse`-calling test whose assertions are about
      `UseTimestamps` contents/length (lines ~150-390 per this plan's own grep pass).
      Keep/adapt any test in that range that is actually about `FirstSeenSec`,
      `CreatedAt`, or entry-creation-on-first-use semantics unrelated to the ring —
      re-express those against `updateEntryWithRetry`'s `createIfMissing` path
      directly if `recordUse` itself is deleted (0.2 below).
- [ ] Add `TestRegistry_UpdateCatalogCursor_MonotonicOnly` (new, TDD for Part 3's new
      method — see Part 3.2): asserts calling `UpdateCatalogCursor` with a rowID
      LOWER than the currently-stored `LastCatalogRowID` is a no-op (cursor never
      regresses), and a HIGHER rowID advances it. Write this test now, confirm it
      fails to compile (method doesn't exist yet), before implementing in 0.2/3.2.

**File: `blockpack/internal/modules/viusage/config_test.go` (modify)**

- [ ] Update both `TriggerConfig{Threshold: 1, WindowSeconds: 3600, LeaseTTLSeconds:
      1800}` literals (lines 46, 66) to `TriggerConfig{LeaseTTLSeconds: 1800}`.

**File: `blockpack/valueindex_usage_test.go` (modify)**

- [ ] Update both `TriggerConfig{Threshold: 1, WindowSeconds: 3600, LeaseTTLSeconds:
      1800}` literals (lines 55, 73) to `TriggerConfig{LeaseTTLSeconds: 1800}`.

### 0.2 — Implementation

**File: `blockpack/internal/modules/viusage/entry.go` (modify)**

- [ ] Remove `UseTimestamps []uint64` field from `Entry` (lines 31-38).
- [ ] Remove `const MaxTrackedUses = 32` (lines 47-48).
- [ ] Add new field to `BackfillState` (Part 3's cursor, additive, see Part 3 for
      full justification): `LastCatalogRowID uint64 \`json:"last_catalog_row_id,omitempty"\``.
      Doc comment: "LastCatalogRowID is the highest file_catalog row_id this column's
      backfill has fully processed (tempo's catalog-cursor-based BlockFetcher,
      2026-07-11). Zero means 'never run against the catalog' — a catalog-backed
      fetcher then lists ALL rows for the tenant, equivalent to a full first listing.
      Only ever advances (monotonic) — see Registry.UpdateCatalogCursor."

**File: `blockpack/internal/modules/viusage/trigger.go` (modify)**

- [ ] `TriggerConfig`: remove `Threshold int` and `WindowSeconds uint64` fields and
      their doc comments. Keep `LeaseTTLSeconds uint64` untouched. Update the
      struct's own top doc comment (lines 19-25) to drop the
      "Threshold=1/WindowSeconds=3600" defaulting language — replace with: "R4's
      repeated-use trigger is now unconditional (team-lead ruling 2026-07-11): the
      first recorded use of a non-dedicated column always fires a backfill. Only
      LeaseTTLSeconds remains — it still bounds R8's lease lifecycle."
- [ ] Delete `pruneUseTimestamps` function (lines 59-79) entirely.
- [ ] In `RecordUseAndMaybeTrigger`'s mutate closure (lines 125-144): delete the line
      `e.UseTimestamps = pruneUseTimestamps(...)` (line 127). Change the switch's
      third case from `case len(e.UseTimestamps) >= cfg.Threshold:` to a bare
      `default:` (Go switch semantics: `Done` and `Triggered` are mutually exclusive
      with "not yet triggered," so `default` is the exact unconditional
      "not yet Triggered" case — no explicit condition needed). Body of that case
      (`e.Backfill.Triggered = true; acquireLease(...); shouldBackfill = true`) is
      **unchanged**.
- [ ] Update the function's own doc comment (lines 81-101) to remove references to
      "appends one usage timestamp" / "the repeated-use threshold" — replace with:
      "recording this use is the caller's job now only insofar as marking the entry
      seen; the FIRST recorded use for a never-triggered column always fires a
      backfill (no threshold left to cross)."

**File: `blockpack/internal/modules/viusage/registry.go` (modify)**

- [ ] Delete `recordUse` method (lines 164-193) — dead code once `UseTimestamps` is
      gone (its only effect was appending/truncating that field; no production
      caller exists today per this plan's own grep pass).
- [ ] Add new method `UpdateCatalogCursor` (Part 3.2 — TDD test written in 0.1 above):
      ```go
      // UpdateCatalogCursor advances (tenant, colHash, colType)'s persisted
      // file-catalog cursor to rowID via the same conditional-PUT retry discipline
      // as RenewLease/UpdateWatermark. Monotonic: a rowID lower than or equal to
      // the entry's current LastCatalogRowID is a silent no-op (never regresses
      // the cursor — a stale/replayed call must not make a later run re-list
      // already-processed catalog rows). Errors if the entry does not exist
      // (mirrors RenewLease's own contract).
      func (r *Registry) UpdateCatalogCursor(ctx context.Context, tenant, colHash, colType string, rowID uint64) error {
          _, err := r.updateEntryWithRetry(
              ctx, tenant, colHash, colType,
              nil,
              func(e *Entry) error {
                  if rowID > e.Backfill.LastCatalogRowID {
                      e.Backfill.LastCatalogRowID = rowID
                  }
                  return nil
              },
          )
          return err
      }
      ```

**File: `blockpack/valueindex_usage.go` (modify)**

- [ ] No code change needed beyond the type alias automatically reflecting
      `viusage.TriggerConfig`'s shrunk field set and `viusage.Entry`'s removed field
      (both are `type X = viusage.X` aliases — no separate copy to edit). Update the
      doc comment on `TriggerConfig` (lines 61-63) to match trigger.go's new comment
      (drop "distinct-use threshold" language).
- [ ] `UpdateCatalogCursor` is **not** re-exported here in this phase — it is
      called only from tempo's `vblockpack` package via the already-exported
      `*blockpack.Registry` (`Registry = viusage.Registry` alias, methods are
      automatically visible through the alias — confirmed via the SAME mechanism
      `RenewLease`/`UpdateWatermark` already use with zero separate re-export
      wrapper). No new line needed in this file.

### 0.3 — Tempo-side config surface cleanup (dependent on 0.2, same repo, do together)

**File: `tempo/tempodb/encoding/common/config.go` (modify)**

- [ ] Remove `TriggerThreshold int` field (lines 199-205) and `TriggerWindow
      time.Duration` field (lines 207-209) from `ViUsageConfig`.
- [ ] Remove their defaulting in `applyDefaults()` (lines 227-232).
- [ ] Keep `LeaseTTL`/`WatermarkCacheTTL` untouched.

**File: `tempo/tempodb/encoding/common/config_blockpack_test.go` (modify)**

- [ ] Remove the two now-dead assertions: `assert.Equal(t, 1,
      cfg.ViUsage.TriggerThreshold)` and `assert.Equal(t, time.Hour,
      cfg.ViUsage.TriggerWindow)` (lines 35-36).

**File: `tempo/tempodb/tempodb.go` (modify)**

- [ ] In the `triggerCfg := blockpack.TriggerConfig{...}` literal (lines 376-380),
      remove the `Threshold: vu.TriggerThreshold` and `WindowSeconds:
      uint64(vu.TriggerWindow.Seconds())` lines. Keep `LeaseTTLSeconds:
      uint64(vu.LeaseTTL.Seconds())`.

**Files: 6 test files with now-non-compiling `TriggerConfig{Threshold: ..., WindowSeconds: ...}` literals (modify, mechanical)**

- [ ] `tempo/tempodb/encoding/vblockpack/vi_watermark_cache_wiring_test.go:63`
- [ ] `tempo/tempodb/encoding/vblockpack/vi_usage_hook_test.go:591,627,658`
- [ ] `tempo/tempodb/encoding/vblockpack/vi_watermark_cache_test.go:62`
- [ ] `tempo/tempodb/encoding/vblockpack/vi_backfill_test.go:211`

  Each: change `blockpack.TriggerConfig{Threshold: 1, WindowSeconds: 3600,
  LeaseTTLSeconds: 1800}` → `blockpack.TriggerConfig{LeaseTTLSeconds: 1800}`.

### 0.4 — Cube investigated independently: NO equivalent change needed

Read `blockpack/internal/modules/cube/trigger.go` directly (`CreationTrigger.TryCreate`,
lines 68-153). Cube's creation trigger has **no repeated-use/threshold concept at
all** — it fires on the very first query matching a (tenant, dims, filters, aggAttrs)
pattern, gated only by the cardinality check (`CheckCardinality`) and the per-tenant
`maxCubes` limit. There is no `UseTimestamps`-equivalent, no `Threshold` field, and no
count-based gate anywhere in `cube.TriggerConfig` (which only has
`CardinalityLimits`/`MaxCubesPerTenant`). **Conclusion: cube needs zero changes for
this part of the task** — confirmed by direct read, not assumed from the brainstorm's
own suspicion.

---

## PART 1 — Postgres-backed row-oriented Registry storage (blockpack + tempo)

### 1.1 — Key design decision: blockpack defines the interface, tempo brings pgx (refines the brainstorm's own open question)

The brainstorm's sketch (`NewPostgresRegistry(pool, tenant)`) implied blockpack would
accept a raw `*pgxpool.Pool`, which would force blockpack to depend on pgx directly —
inconsistent with how `ObjectStore`/`CubeObjectStore` work today (blockpack defines a
narrow interface; **tempo** brings the concrete S3/GCS/Local/Azure implementation).
This plan uses the SAME ownership split for Postgres: **blockpack defines a new,
narrow, row-oriented storage interface; tempo implements it with pgx underneath.**
Net effect: **pgx is vendored into tempo's go.mod only — blockpack's go.mod gains
zero new dependencies.** This is a genuine improvement over the brainstorm's own
sketch, not a deviation requiring separate sign-off (it reduces blockpack's public
surface and dependency footprint versus the alternative).

### 1.2 — New internal interface (blockpack, private to each package — mirrors `ObjectStore`'s existing private-interface shape)

**File: `blockpack/internal/modules/viusage/entry_store.go` (new)**

```go
package viusage

import "context"

// entryStore is Registry's internal storage abstraction (introduced 2026-07-11 to
// let Registry sit on top of either a whole-blob ObjectStore (existing S3/Local/
// GCS/Azure path) or a row-oriented Postgres backend without changing Registry's
// own public methods at all). blobEntryStore (registry.go) wraps today's
// ObjectStore + conditional-PUT-retry loop, behavior-preserving. A second
// implementation is constructed from an externally-supplied EntryStore (see
// valueindex_usage.go's re-export) for the Postgres case.
type entryStore interface {
    // load returns every entry for tenant. The blob-backed implementation returns
    // them in on-disk order; a Postgres-backed implementation may return them in
    // any order (no caller today depends on Load's ordering — confirmed by reading
    // every external Load call site, all of which discard the etag and either
    // linear-scan or aggregate the result).
    load(ctx context.Context, tenant string) ([]Entry, error)
    // upsertEntry loads-or-creates the row keyed by (tenant, colHash, colType),
    // applies mutate to it, and persists the result — ONE atomic evaluate-and-
    // mutate step, mirroring updateEntryWithRetry's existing create+mutate+persist
    // contract. mutate may be invoked more than once (blob-backed: once per retry
    // attempt; Postgres-backed: exactly once under a row lock) — callers must
    // derive new state from the entry's CURRENT contents each call, never from
    // closure-captured pre-computed values (unchanged from today's contract).
    upsertEntry(
        ctx context.Context, tenant, colHash, colType string,
        createIfMissing func() Entry, mutate func(*Entry) error,
    ) (Entry, error)
}
```

### 1.3 — Refactor `Registry` to delegate through `entryStore` (behavior-preserving, TDD-verified)

**Step 1.3a — TDD: pin current behavior before refactoring**

- [ ] Before touching `registry.go`, run `go test ./internal/modules/viusage/...` and
      record the full passing test list (this is the regression baseline the refactor
      must not break). No new test needed yet — this step just confirms a known-green
      starting point.

**File: `blockpack/internal/modules/viusage/registry.go` (modify)**

- [ ] Change `Registry` struct:
      ```go
      type Registry struct {
          store  entryStore // was: store ObjectStore
          tenant string
      }
      ```
- [ ] `NewRegistry(store ObjectStore, tenant string) *Registry` **signature
      unchanged** (public contract preserved) — internally wraps: `return
      &Registry{store: &blobEntryStore{store: store}, tenant: tenant}`.
- [ ] New private type `blobEntryStore` (same file): wraps the existing `ObjectStore`
      + `indexPath()`/`usageIndex` JSON marshal + conditional-PUT-retry loop —
      literally the CURRENT `Load`/`updateEntryWithRetry` bodies, moved verbatim onto
      `blobEntryStore.load`/`blobEntryStore.upsertEntry`, with `r.store.Get`/
      `r.store.ConditionalPut` calls becoming `s.store.Get`/`s.store.ConditionalPut`
      (rename `r`→`s`, `store ObjectStore` field). `Registry.Load` becomes a one-line
      delegator: `return r.store.load(ctx, r.tenant)` (etag is dropped from the
      public signature? **NO — do not change Load's public signature.** See 1.3b.)
- [ ] `Registry.Load`'s **public signature stays `([]Entry, string, error)`**
      (confirmed via direct grep: every external caller in tempo discards the
      second return value with `_`; only the package's OWN retry loop ever used it
      meaningfully). Since `entryStore.load` returns just `([]Entry, error)` (no
      etag concept — meaningless for a Postgres row-per-entry backend), `Registry.
      Load` wraps it: `entries, err := r.store.load(ctx, r.tenant); return entries,
      "", err`. The blob-backed path's OWN internal retry loop (inside
      `blobEntryStore.upsertEntry`) still uses etags internally — only the PUBLIC
      `Load` method's second return value becomes always-empty-string, which is a
      behavior-invisible change given the confirmed-zero external consumers.
- [ ] `Registry.RenewLease`/`Registry.UpdateWatermark`/`Registry.UpdateCatalogCursor`
      (0.2's new method) and `RecordUseAndMaybeTrigger`'s call into
      `updateEntryWithRetry`: replace the direct `r.updateEntryWithRetry(...)` calls
      with `r.store.upsertEntry(ctx, tenant, colHash, colType, createIfMissing,
      mutate)`. Delete the now-unused `updateEntryWithRetry` method itself (its body
      has moved into `blobEntryStore.upsertEntry`).

**File: `blockpack/internal/modules/viusage/registry_test.go` (modify)**

- [ ] Re-run the FULL existing test suite (post-0.1's deletions) against the
      refactored `Registry` — every remaining test must pass UNMODIFIED (this
      refactor is explicitly behavior-preserving; a test needing a change here is a
      red flag, not an expected outcome). Any fake `ObjectStore` test doubles used
      today keep working unchanged since `NewRegistry(store ObjectStore, ...)`'s
      signature didn't change.

**Mirror identically for cube** (same steps, same file names, `cube` package):
- [ ] `blockpack/internal/modules/cube/entry_store.go` (new) — `entryStore` interface
      scoped to `RegistryEntry` instead of `Entry`, methods `load`/`addEntry` (cube's
      `Add` is create-only, not upsert-with-mutate — mirror `Add`/`Remove`/
      `UpdateWatermarks`'s existing 3-method shape rather than forcing cube into
      viusage's create-or-mutate shape; cube's actual usage pattern is genuinely
      different, R1 applies).
- [ ] `blockpack/internal/modules/cube/registry.go` (modify) — same
      blob-wrap-first, `Registry.store` becomes the new private interface,
      `NewRegistry(store ObjectStore, ...)` signature unchanged.

### 1.4 — New EXPORTED interface for external (Postgres) implementations (blockpack)

**File: `blockpack/valueindex_usage.go` (modify)** — add:

```go
// EntryStore is the row-oriented storage interface an alternative Registry backend
// implements (2026-07-11 Postgres support) — in place of ObjectStore's whole-blob
// conditional-PUT shape, this is one row per (tenant, colHash, colType). tempo's
// pgx-backed implementation lives entirely in tempo (this package never imports a
// SQL driver) — mirrors ObjectStore's own "interface owned here, concrete backend
// owned by the caller" split exactly.
type EntryStore = viusage.EntryStore // exported alias of the now-exported interface

// NewRegistryFromEntryStore constructs a Registry over an externally-supplied
// EntryStore (e.g. tempo's Postgres-backed implementation) instead of an
// ObjectStore. Registry's own public methods (Load/RenewLease/UpdateWatermark/
// UpdateCatalogCursor) are byte-identical regardless of which constructor built it.
func NewRegistryFromEntryStore(store EntryStore, tenant string) *Registry {
    return viusage.NewRegistryFromEntryStore(store, tenant)
}
```

**IMPORTANT Go-visibility trap, resolved below (read before implementing):** a naive
single-interface design (rename `entryStore` to `EntryStore` and export it directly)
does NOT work: `entryStore`'s methods (`load`/`upsertEntry`) are lowercase, and Go
enforces unexported method names as package-private — an external (tempo) type
CANNOT structurally implement an interface whose method names are unexported, even
via a `type EntryStore = entryStore` alias (the alias doesn't change the method
names). The correct shape needs **two interfaces plus a tiny adapter**:

**File: `blockpack/internal/modules/viusage/entry_store.go` (modify, continued
from 1.2)**

```go
// EntryStore is the EXPORTED counterpart of entryStore, with exported method names
// — what an external (tempo) Postgres implementation actually satisfies.
type EntryStore interface {
    Load(ctx context.Context, tenant string) ([]Entry, error)
    UpsertEntry(ctx context.Context, tenant, colHash, colType string, createIfMissing func() Entry, mutate func(*Entry) error) (Entry, error)
}

// externalEntryStoreAdapter adapts an external EntryStore to the internal,
// unexported entryStore Registry actually holds — mirrors how blobEntryStore
// adapts ObjectStore.
type externalEntryStoreAdapter struct{ EntryStore }

func (a *externalEntryStoreAdapter) load(ctx context.Context, tenant string) ([]Entry, error) {
    return a.EntryStore.Load(ctx, tenant)
}
func (a *externalEntryStoreAdapter) upsertEntry(ctx context.Context, tenant, colHash, colType string, createIfMissing func() Entry, mutate func(*Entry) error) (Entry, error) {
    return a.EntryStore.UpsertEntry(ctx, tenant, colHash, colType, createIfMissing, mutate)
}

func NewRegistryFromEntryStore(store EntryStore, tenant string) *Registry {
    return &Registry{store: &externalEntryStoreAdapter{store}, tenant: tenant}
}
```

- [ ] Implement exactly this two-interface + adapter shape.
- [ ] Mirror identically for `cube.EntryStore`/`cube.NewRegistryFromEntryStore`.

### 1.5 — TDD tests for the new public constructor (blockpack)

**File: `blockpack/valueindex_usage_test.go` (modify)**

- [ ] `TestNewRegistryFromEntryStore_DelegatesToProvidedStore` — a fake `EntryStore`
      (in-memory map, no real Postgres) proving `Registry.Load`/`RenewLease`/
      `UpdateWatermark`/`RecordUseAndMaybeTrigger` all route through the fake's
      `Load`/`UpsertEntry`, not through any `ObjectStore` path. Write this FIRST,
      confirm it fails to compile (constructor doesn't exist), then implement 1.4.
- [ ] `TestNewRegistryFromEntryStore_SameBehaviorAsObjectStoreBacked` — run the EXACT
      SAME `RecordUseAndMaybeTrigger` scenario (first-use unconditional trigger, from
      Part 0) against both a `NewRegistry(fakeObjectStore, ...)` and a
      `NewRegistryFromEntryStore(fakeEntryStore, ...)` Registry, asserting identical
      `TriggerResult` — the whole point of Approach B is that callers cannot tell
      the difference.

---

## PART 2 — Tempo-side pgx-backed `EntryStore` implementation

### 2.1 — Postgres client library: pgx v5 (confirmed, not overridden)

Brainstorm's pgx v5 recommendation stands — modern, actively maintained,
`pgxpool` gives native connection pooling with a small `MaxConns` (per the
brainstorm's own connection-budget risk: keep per-process pool size small,
single-digit, since every querier/frontend/backend-worker replica holds its own
pool and `max_connections` is a real fleet-wide budget). No lib/pq/database-sql
shim — pgx's native interface avoids an extra abstraction layer for no benefit here.

**File: `tempo/go.mod` (modify)** — add:
```
require github.com/jackc/pgx/v5 v5.7.6
```
(pin to whatever the latest v5.7.x is at implementation time — check
`https://pkg.go.dev/github.com/jackc/pgx/v5?tab=versions` for the actual current
release; this plan does not fabricate a precise patch version).

- [ ] `go mod tidy && go mod vendor` in tempo (standard vendoring, NOT the
      filesystem-`replace`-directive revendor discipline that applies to blockpack —
      pgx is a normal upstream dependency, no local replace involved).
- [ ] License check: pgx v5 is MIT-licensed — compatible.

### 2.2 — pgx-backed `EntryStore` implementation

**File: `tempo/tempodb/encoding/vblockpack/pg_entrystore.go` (new)**

```go
package vblockpack

// pg_entrystore.go — Postgres-backed blockpack.EntryStore/CubeEntryStore
// implementations (2026-07-11 opt-in Postgres backend). Row-oriented: one
// Postgres row per (tenant, col_hash, col_type), eliminating the single-shared-
// blob contention point the JSON-blob ObjectStore path has (every column's
// UpdateWatermark call today serializes against EVERY OTHER column's update for
// the same tenant via one shared index.json — see .bob/state/brainstorm.md's root-
// cause section). Uses a single transaction with SELECT ... FOR UPDATE to get the
// same atomicity RecordUseAndMaybeTrigger's evaluate-and-mutate step needs,
// without a retry loop (Postgres's row lock makes concurrent UpsertEntry calls
// for the SAME key queue rather than conflict-and-retry — a genuine improvement
// over the blob path's 5-attempt exponential backoff for the contended case).

import (
    "context"
    "fmt"

    blockpack "github.com/grafana/blockpack"
    "github.com/jackc/pgx/v5/pgxpool"
)

// pgViUsageEntryStore satisfies blockpack.EntryStore over a *pgxpool.Pool.
type pgViUsageEntryStore struct{ pool *pgxpool.Pool }

func newPgViUsageEntryStore(pool *pgxpool.Pool) *pgViUsageEntryStore {
    return &pgViUsageEntryStore{pool: pool}
}

func (s *pgViUsageEntryStore) Load(ctx context.Context, tenant string) ([]blockpack.Entry, error) {
    rows, err := s.pool.Query(ctx, viusageSelectAllSQL, tenant)
    if err != nil {
        return nil, fmt.Errorf("pg viusage entrystore: load: %w", err)
    }
    defer rows.Close()
    var out []blockpack.Entry
    for rows.Next() {
        e, scanErr := scanViUsageEntry(rows)
        if scanErr != nil {
            return nil, fmt.Errorf("pg viusage entrystore: scan: %w", scanErr)
        }
        out = append(out, e)
    }
    return out, rows.Err()
}

func (s *pgViUsageEntryStore) UpsertEntry(
    ctx context.Context, tenant, colHash, colType string,
    createIfMissing func() blockpack.Entry,
    mutate func(*blockpack.Entry) error,
) (blockpack.Entry, error) {
    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: begin: %w", err)
    }
    defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

    entry, found, err := loadEntryForUpdate(ctx, tx, tenant, colHash, colType)
    if err != nil {
        return blockpack.Entry{}, err
    }
    if !found {
        if createIfMissing == nil {
            return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: entry %s/%s/%s not found", tenant, colHash, colType)
        }
        entry = createIfMissing()
        if err := insertViUsageEntry(ctx, tx, entry); err != nil {
            return blockpack.Entry{}, err
        }
    }
    if err := mutate(&entry); err != nil {
        return blockpack.Entry{}, err
    }
    if err := updateViUsageEntry(ctx, tx, entry); err != nil {
        return blockpack.Entry{}, err
    }
    if err := tx.Commit(ctx); err != nil {
        return blockpack.Entry{}, fmt.Errorf("pg viusage entrystore: commit: %w", err)
    }
    return entry, nil
}
```

- [ ] `loadEntryForUpdate` issues `SELECT ... FROM viusage_entries WHERE tenant=$1
      AND col_hash=$2 AND col_type=$3 FOR UPDATE` inside the transaction — the row
      lock is held until `Commit`/`Rollback`, giving exactly the atomicity
      `RecordUseAndMaybeTrigger`'s switch statement needs, with `READ COMMITTED`
      (pgx's default) — **no `SERIALIZABLE` needed**, per the brainstorm's own
      isolation-level finding (confirmed correct: the atomicity boundary is the
      transaction wrapping ONE row's lock, not a multi-row invariant).
- [ ] `insertViUsageEntry` issues a plain `INSERT` (the row-lock semantics make an
      `ON CONFLICT` race window irrelevant here — the row didn't exist under
      `FOR UPDATE`'s own read, and no other transaction can have inserted the SAME
      primary key between that read and this insert without ALSO blocking on the
      unique index, which Postgres enforces natively — a concurrent insert attempt
      simply blocks until this transaction commits, then fails with a unique
      violation, which the caller (a second concurrent `RecordUseAndMaybeTrigger`)
      retries at the `Registry`/application level exactly as it would for a genuine
      first-time-trigger race today). **Write the concurrency test in Part 5 to
      prove this claim, do not just assert it.**
- [ ] `updateViUsageEntry` issues a plain `UPDATE ... WHERE tenant=$1 AND
      col_hash=$2 AND col_type=$3` (the row lock from `loadEntryForUpdate`/the
      insert above is already held for this transaction — no additional
      conditional-WHERE-on-old-value needed, unlike the blob path's etag check,
      because Postgres's row lock IS the concurrency control here).

- [ ] Mirror identically: `pgCubeEntryStore` (same file or a sibling
      `pg_entrystore_cube.go`) implementing `blockpack.CubeEntryStore` (cube's
      exported version of `EntryStore`, per 1.4's mirrored cube work) against
      `cube_entries` (schema in Part 4).

### 2.3 — Postgres connection config (mirrors `Memcached`/`Redis`'s existing shape)

**File: `tempo/modules/postgres/config.go` (new package, mirrors `modules/cache/redis`)**

```go
package postgres

import "time"

// Config configures the opt-in Postgres backend for viusage/cube registries and
// the file catalog (2026-07-11). Nil (the zero value of *Config on tempodb.Config)
// means "not configured" — every dispatch point in this plan treats a nil
// *postgres.Config identically to how a nil *redis.Config/*memcached.Config means
// "cache role disabled" today.
type Config struct {
    // DSN is a standard postgres:// connection string (libpq-compatible). Secrets
    // (password) MUST be redacted in any UI/log output per this project's
    // standing coding-philosophy rule — never log cfg.DSN directly; log only the
    // host/dbname portion via a redacting helper (implement alongside this file).
    DSN string `yaml:"dsn"`
    // MaxConns bounds this PROCESS's own pgxpool size. Kept deliberately small by
    // default (brainstorm risk: querier-replica-count × per-process-pool-size must
    // stay well under Postgres's max_connections fleet-wide). Default 4.
    MaxConns int32 `yaml:"max_conns"`
    // ConnectTimeout bounds initial connection establishment. Default 5s.
    ConnectTimeout time.Duration `yaml:"connect_timeout"`
}

func (c *Config) applyDefaults() {
    if c.MaxConns <= 0 {
        c.MaxConns = 4
    }
    if c.ConnectTimeout <= 0 {
        c.ConnectTimeout = 5 * time.Second
    }
}
```

**File: `tempo/modules/postgres/pool.go` (new)** — `NewPool(cfg *Config) (*pgxpool.Pool,
error)`: parses `cfg.DSN` via `pgxpool.ParseConfig`, applies `MaxConns`/
`ConnectTimeout`, returns a ready pool. Caller (tempodb.go) owns `Close()` on
shutdown — wire into the SAME module-service Stop path other components use (per
the brainstorm's own flagged risk: this is genuinely new lifecycle for tempo, don't
let the pool leak past process shutdown).

**File: `tempo/tempodb/config.go` (modify)** — mirror `Memcached`/`Redis`'s exact
shape (lines 75-76):
```go
Postgres *postgres.Config `yaml:"postgres"`
```
Add the corresponding import (`"github.com/grafana/tempo/modules/postgres"`).

### 2.4 — Dispatch: new branch in `ConfigureViUsage`/`ConfigureCubeManager`

**File: `tempo/tempodb/encoding/vblockpack/vi_usage_hook.go` (modify)**

- [ ] `ConfigureViUsage`'s signature grows by one param:
      `pgPool *pgxpool.Pool` (nil when Postgres is not configured — mirrors
      `s3cfg *s3backend.Config`'s existing nil-means-not-this-backend convention).
      **This IS a public-surface change to an exported tempo function** — tempo
      package functions aren't under blockpack's CLAUDE.md "ask before adding public
      API" rule (that rule is blockpack-specific), but flag it anyway since this
      session has been treating cross-repo signature growth consistently.
- [ ] Inside `ConfigureViUsage`, after building `store` via the EXISTING
      `newViUsageObjectStoreForBackend` dispatch (byte-identical, untouched): if
      `pgPool != nil`, **override** — construct `registry :=
      blockpack.NewRegistryFromEntryStore(newPgViUsageEntryStore(pgPool), tenant)`
      style per-tenant lazily (mirrors `realUsageRecorder.registryFor`'s existing
      per-tenant lazy-cache pattern EXACTLY — do not bake a single tenant in at
      construction, per this session's own standing "per-tenant singleton" finding).
      Concretely: `realUsageRecorder` gains a new field `pgPool *pgxpool.Pool` (nil
      when unset), and `registryFor(tenant)` branches: `if r.pgPool != nil { reg =
      blockpack.NewRegistryFromEntryStore(newPgViUsageEntryStore(r.pgPool), tenant)
      } else { reg = blockpack.NewRegistry(r.store, tenant) }` — the S3/Local/GCS/
      Azure `store` field construction above is UNTOUCHED either way (it's simply
      unused when `pgPool != nil`, costing nothing extra since it was already being
      built for the backfill deps regardless).

**File: `tempo/tempodb/encoding/vblockpack/cubemanager.go` (modify)**

- [ ] `ConfigureCubeManager`'s signature grows by one param: `pgPool *pgxpool.Pool`.
      Same override pattern: when non-nil, `cm.objStore` is never consulted;
      `cm.loadDefs`'s `blockpack.NewCubeRegistry(cm.objStore, cm.tenant)` call
      becomes conditional on `pgPool`, using
      `blockpack.NewCubeRegistryFromEntryStore(newPgCubeEntryStore(pgPool),
      cm.tenant)` instead. **Note the existing `cubeManagerOnce sync.Once` /
      single-tenant-baked-in `cubeManager` struct** (line 44: `tenant string` field,
      singleton constructed once) — this is a PRE-EXISTING single-tenant assumption
      in `cubemanager.go` (unlike `realUsageRecorder`'s already-multi-tenant
      `registryFor` map) that this plan does NOT change or fix (out of scope — flag
      it as a pre-existing limitation this task inherits, not introduces).

**File: `tempo/tempodb/tempodb.go` (modify)** — wire a `*pgxpool.Pool` (constructed
once via `modules/postgres.NewPool(cfg.Postgres)` when `cfg.Postgres != nil`) through
to both `ConfigureViUsage(..., pgPool)` and `ConfigureCubeManager(..., pgPool)` call
sites (near lines 340, 381).

### 2.5 — TDD tests for tempo-side pgx implementation (ephemeral Postgres — see Part 5 for the shared test-infra decision)

**File: `tempo/tempodb/encoding/vblockpack/pg_entrystore_test.go` (new)**

- [ ] `TestPgViUsageEntryStore_UpsertEntry_CreateThenMutate` — against a real
      ephemeral Postgres (testcontainers-go, Part 5), create a fresh entry via
      `UpsertEntry` with `createIfMissing` set, assert the row exists; call again
      with `createIfMissing: nil` and a `mutate` that flips `Triggered`, assert the
      persisted row reflects it.
- [ ] `TestPgViUsageEntryStore_UpsertEntry_MissingNoCreateIfMissing_Errors` — mirrors
      `RenewLease`'s existing "entry does not exist" error contract.
- [ ] `TestPgViUsageEntryStore_Load_ReturnsAllRowsForTenant_NotOtherTenants` —
      tenant-isolation regression pin (two tenants' rows, assert `Load` never
      cross-leaks).
- [ ] **The concurrency test is the big one — see Part 5.3.**

---

## PART 3 — File catalog + cursor-based backfill discovery (new scope, folded in properly)

### 3.1 — Schema

**File: `tempo/tempodb/encoding/vblockpack/schema/file_catalog.sql` (new, reviewed
artifact — NOT applied to any live Postgres per the checkpoint)**

```sql
CREATE TABLE IF NOT EXISTS file_catalog (
    row_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant      TEXT   NOT NULL,
    block_id    TEXT   NOT NULL,   -- uuid.UUID.String()
    block_ref   TEXT   NOT NULL,   -- "<tenant>/<block_id>/data.blockpack" (blockObjectKey format, reused verbatim)
    start_sec   BIGINT NOT NULL,
    end_sec     BIGINT NOT NULL,
    size_bytes  BIGINT NOT NULL DEFAULT 0,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ NULL,   -- soft delete; set by the lister's reconciliation pass
    UNIQUE (tenant, block_id)
);

-- Primary access pattern: cursor-based incremental discovery per tenant, live rows only.
CREATE INDEX IF NOT EXISTS idx_file_catalog_tenant_rowid_live
    ON file_catalog (tenant, row_id)
    WHERE deleted_at IS NULL;
```

- [ ] `row_id`'s **ordering property is load-bearing** (per the team lead's explicit
      framing) — `BIGINT GENERATED ALWAYS AS IDENTITY` (not `xmin`, not a
      timestamp) guarantees strictly-increasing, gap-tolerant-but-monotonic
      insertion order, which is exactly what "give me everything after cursor X" needs.
- [ ] `UNIQUE (tenant, block_id)` is the upsert key for the lister's `INSERT ... ON
      CONFLICT (tenant, block_id) DO UPDATE SET deleted_at = NULL` (handles the
      defensive "briefly vanished and reappeared" edge case, though in practice
      block IDs are immutable and never reappear once genuinely deleted).
- [ ] Partial index `WHERE deleted_at IS NULL` keeps the hot cursor-query path small
      regardless of how large the historical (soft-deleted) tail grows.

### 3.2 — Backfill-state cursor field (blockpack) — already specified in Part 0.2

`BackfillState.LastCatalogRowID` and `Registry.UpdateCatalogCursor` are specified in
Part 0.2 above (grouped there since both are additive `Entry`/`Registry` changes
alongside the threshold-removal edits to the SAME files — implement together, one
PR-sized change to `entry.go`/`registry.go`, not two separate passes).

### 3.3 — New `BlockFetcher` implementation: `catalogBlockFetcher` (tempo)

**Confirmed via direct read of `blockpack/valueindex_backfill.go`:** `BlockFetcher`'s
interface (`ListBlocksInRange(ctx, tenant, minSec, maxSec) ([]string, error)` +
`FetchBlock(ctx, sourceRef) (*Reader, error)`) needs **zero changes**. This is
exactly the "new implementation of an existing interface" pattern the team lead
suggested checking for, and it is the correct fit: the catalog only replaces
**listing**, never block-byte fetching (which stays on `backend.Reader`, unchanged).

**File: `tempo/tempodb/encoding/vblockpack/vi_backfill_catalog.go` (new)**

```go
package vblockpack

// vi_backfill_catalog.go — catalog-cursor-based blockpack.BlockFetcher (2026-07-11),
// an alternative to viBlockFetcher's live backend.Reader.Blocks()/BlockMeta()
// listing. Structurally fixes tonight's "block deleted between list and fetch"
// failure class: a catalog row only exists while the file_catalog lister
// (backend-scheduler, see Part 3.5) still sees the block in its own live,
// continuously-reconciled blocklist snapshot — see that file's doc comment for
// why this closes (not just narrows) the race in practice. FetchBlock is
// UNCHANGED from viBlockFetcher (delegates to the same backend.Reader) — only
// ListBlocksInRange's data source changes.
import (
    "context"
    "fmt"
    "sort"

    blockpack "github.com/grafana/blockpack"
    "github.com/grafana/tempo/tempodb/backend"
    "github.com/jackc/pgx/v5/pgxpool"
)

type catalogBlockFetcher struct {
    pool         *pgxpool.Pool
    reader       backend.Reader // same reader viBlockFetcher would have used
    cursorRowID  uint64         // entry.Backfill.LastCatalogRowID at construction
    maxRowIDSeen uint64         // set by ListBlocksInRange; read by the caller after Run() succeeds
}

func (f *catalogBlockFetcher) ListBlocksInRange(
    ctx context.Context, tenant string, minSec, maxSec uint64,
) ([]string, error) {
    rows, err := f.pool.Query(ctx, catalogListSQL, tenant, f.cursorRowID, minSec, maxSec)
    if err != nil {
        return nil, fmt.Errorf("catalogBlockFetcher: query: %w", err)
    }
    defer rows.Close()

    type candidate struct {
        ref      string
        startSec uint64
        rowID    uint64
    }
    var candidates []candidate
    for rows.Next() {
        var c candidate
        if scanErr := rows.Scan(&c.rowID, &c.ref, &c.startSec); scanErr != nil {
            return nil, fmt.Errorf("catalogBlockFetcher: scan: %w", scanErr)
        }
        candidates = append(candidates, c)
        if c.rowID > f.maxRowIDSeen {
            f.maxRowIDSeen = c.rowID
        }
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    // Newest-first, same contract as viBlockFetcher.ListBlocksInRange.
    sort.Slice(candidates, func(i, j int) bool { return candidates[i].startSec > candidates[j].startSec })
    refs := make([]string, len(candidates))
    for i, c := range candidates {
        refs[i] = c.ref
    }
    return refs, nil
}

func (f *catalogBlockFetcher) FetchBlock(ctx context.Context, sourceRef string) (*blockpack.Reader, error) {
    // Identical body to viBlockFetcher.FetchBlock — factor a shared helper
    // (fetchBlockViaReader(reader, sourceRef)) rather than duplicating, called
    // from both types.
    return fetchBlockViaReader(ctx, f.reader, sourceRef)
}

// MaxRowIDSeen reports the highest file_catalog row_id returned by any
// ListBlocksInRange call on this fetcher instance. NOT part of blockpack.
// BlockFetcher — a tempo-local accessor runViBackfillCore reads after eng.Run
// succeeds, to persist the new cursor via Registry.UpdateCatalogCursor. Mirrors
// this file's own atomicWriter-style "optional capability, checked via type
// assertion" convention already used in rawobjectstore.go.
func (f *catalogBlockFetcher) MaxRowIDSeen() uint64 { return f.maxRowIDSeen }
```

- [ ] `catalogListSQL`: `SELECT row_id, block_ref, start_sec FROM file_catalog WHERE
      tenant=$1 AND deleted_at IS NULL AND row_id > $2 AND end_sec >= $3 AND
      start_sec <= $4 ORDER BY row_id`.
- [ ] Extract `fetchBlockViaReader` as a small shared helper in `vi_backfill.go` (used
      by both `viBlockFetcher.FetchBlock` and `catalogBlockFetcher.FetchBlock`) —
      the ONE piece of genuine code-sharing between the two fetchers, since the
      fetch-by-ref logic (parse `sourceRef` → `tempoReaderProvider` →
      `blockpack.NewReaderFromProvider`) is identical either way.

### 3.4 — `runViBackfillCore` wiring for the cursor persist step

**File: `tempo/tempodb/encoding/vblockpack/vi_backfill.go` (modify)**

- [ ] After `eng.Run(ctx, ...)` returns `nil` (full success) inside
      `runViBackfillCore`, add:
      ```go
      if cf, ok := fetcher.(*catalogBlockFetcher); ok {
          if cursorErr := registry.UpdateCatalogCursor(
              ctx, entry.Tenant, entry.ColumnHash, entry.ColumnType, cf.MaxRowIDSeen(),
          ); cursorErr != nil {
              level.Warn(util_log.Logger).Log(
                  "msg", "vblockpack: VI backfill: catalog cursor persist failed",
                  "tenant", entry.Tenant, "column", entry.ColumnName, "err", cursorErr,
              )
              return cursorErr
          }
      }
      ```
      Only fires when a catalog-backed fetcher was used (type assertion — zero
      effect on the existing S3/Local/GCS/Azure `viBlockFetcher` path). A partial
      `eng.Run` failure never reaches this line, so the cursor never advances past
      an unconfirmed run (mirrors R7's existing "don't claim coverage until
      confirmed" philosophy, applied to catalog discovery too — the SAME
      candidate set is safely re-listed and re-processed on the next attempt,
      relying on `FlushAndPutValueIndexColumn`'s existing idempotent-overwrite
      behavior for any block that was in fact already written).
- [ ] `RunViBackfillDeps` gains no new field — the SAME `Fetcher blockpack.
      BlockFetcher` field just holds a `*catalogBlockFetcher` instead of a
      `*viBlockFetcher` when Postgres file-catalog mode is configured. New
      constructor: `NewViBackfillDepsCatalogOverride(deps RunViBackfillDeps, pool
      *pgxpool.Pool, entry blockpack.Entry, reader backend.Reader)
      RunViBackfillDeps` — takes an ALREADY-BUILT `deps` (from
      `NewViBackfillDepsS3`/`Raw`, unchanged) and returns a copy with `.Fetcher`
      swapped to a `*catalogBlockFetcher{pool: pool, reader: reader, cursorRowID:
      entry.Backfill.LastCatalogRowID}` — this is the "orthogonal override,
      independent of which object-store backend is otherwise active" shape
      described in this plan's design notes; called from `ConfigureViUsage`'s
      `onShouldBackfill` closure only when `pgPool != nil`.

### 3.5 — File catalog lister: piggybacks on the ALREADY-singleton backend-scheduler (answers the team lead's Q1 with a concrete "no new binary" answer)

**Confirmed via direct read of
`docs/sources/tempo/reference-tempo-architecture/components/compaction.md:45`:**
"The scheduler is a singleton: only one instance should run at a time. It
maintains the work cache... and **polls object storage to keep the blocklist up to
date**." Confirmed via direct read of `modules/backendscheduler/backendscheduler.go`
that `s.store.BlockMetas(tenant)` (lines 485, 812) already returns a live,
continuously-maintained in-memory `[]*backend.BlockMeta` snapshot per tenant,
updated as soon as a worker reports a compaction/retention job result back
(`applyJobsToBlocklist`). **Conclusion: no new binary or deployment target is
needed** — the lister is a new background loop INSIDE `BackendScheduler`, reusing
state it already maintains for free.

**File: `tempo/modules/backendscheduler/filecatalog/lister.go` (new package)**

```go
package filecatalog

// lister.go — periodic reconciliation of backend-scheduler's already-live,
// already-maintained per-tenant blocklist snapshot (s.store.BlockMetas) into the
// Postgres file_catalog table (2026-07-11). NOT a new S3 List() call — reuses
// state backend-scheduler already polls for on BlocklistPoll's existing cadence;
// this loop just reconciles that snapshot against Postgres on its OWN, faster
// tick (default 5m, see Config.ListInterval), independent of the (default 5m)
// BlocklistPoll interval it happens to currently match.
//
// Soft-delete reconciliation: any file_catalog row (for a tenant this pass
// covers) whose block_id is NOT present in the current BlockMetas snapshot gets
// deleted_at set. Since BlockMetas already excludes blocks a worker has reported
// as compacted/retained (applyJobsToBlocklist), this closes the "backfill's
// catalog query hands back a since-deleted block" race within one lister-tick's
// latency — bounded by ListInterval, with CompactedBlockRetention's default 1h
// (tempodb/config.go's own CompactorConfig default, tempodb.go:142) as the safety
// margin between "worker reports a block cleared" and "the object is actually
// gone from storage." An operator narrowing CompactedBlockRetention below
// roughly 2-3x ListInterval should be flagged as a real, load-bearing operational
// invariant this design relies on — document in this package's own doc comment
// (tempo has no SPECS.md convention today, unlike blockpack).
```

- [ ] `Lister` struct: `pool *pgxpool.Pool`, `blockMetas func(tenant string)
      []*backend.BlockMeta` (injected function, not a direct `storage.Store`
      dependency — keeps this package unit-testable without a real backend; tempo's
      `BackendScheduler` passes `s.store.BlockMetas` directly), `tenants func()
      []string` (injected similarly, from `s.store.Tenants()` or
      `s.blocklist.Tenants()` — confirm exact accessor name against
      `storage.Store`'s interface at implementation time).
- [ ] `func (l *Lister) RunOnce(ctx context.Context) error` — for each tenant: fetch
      current `BlockMetas`, `INSERT ... ON CONFLICT (tenant, block_id) DO UPDATE SET
      deleted_at = NULL` for every currently-live block, then `UPDATE file_catalog
      SET deleted_at = now() WHERE tenant=$1 AND deleted_at IS NULL AND block_id NOT
      IN (<currently-live set>)` for reconciliation. Batch the "currently-live set"
      comparison via a temp table or `= ANY($2::text[])` parameter, not N individual
      queries — this runs over potentially thousands of blocks per tenant.
- [ ] `func (l *Lister) Run(ctx context.Context, interval time.Duration)` — the
      ticker loop wrapper, mirrors `retentionLoop`'s exact shape
      (`tempodb/retention.go:20-36`) for consistency with this codebase's existing
      convention.

**File: `tempo/modules/backendscheduler/backendscheduler.go` (modify)**

- [ ] `BackendScheduler` struct gains one new field: `catalogLister
      *filecatalog.Lister` (nil when Postgres file-catalog is not configured —
      same nil-means-disabled convention as everywhere else in this plan).
- [ ] In `New(...)`: construct `catalogLister` from a new `cfg.Postgres
      *postgres.Config` field (added to `modules/backendscheduler/config.go`,
      mirroring `tempodb.Config.Postgres` from Part 2.3 — the SAME `*postgres.Config`
      type, reused, not a second copy) when non-nil.
- [ ] In `running()` (lines 205-234): add a THIRD ticker case, mirroring
      `maintenanceTicker`/`backendFlushTicker`'s exact existing shape:
      ```go
      catalogListTicker := time.NewTicker(s.cfg.CatalogListInterval) // default 5m
      defer catalogListTicker.Stop()
      // ... in the select:
      case <-catalogListTicker.C:
          if s.catalogLister != nil {
              if err := s.catalogLister.RunOnce(ctx); err != nil {
                  level.Warn(log.Logger).Log("msg", "file catalog list pass failed", "err", err)
              }
          }
      ```
      When `s.catalogLister == nil` (Postgres not configured), this ticker case is
      cheap no-op work (one nil check per tick) — zero risk to the existing
      singleton's other responsibilities.

### 3.6 — Retention/soft-delete: explicit, non-hand-wavy answer (per the team lead's direct ask)

**Does retention/compaction need to directly update the catalog?** **No — this
plan deliberately does NOT hook `tempodb/retention.go`'s `MarkBlockCompacted`/
`ClearBlock` calls directly.** Instead, the file-catalog lister's OWN periodic
reconciliation pass (3.5) achieves the same soft-delete effect indirectly, by
diffing against `s.store.BlockMetas(tenant)` — which is ALREADY updated the moment
a worker's retention job result is applied (`applyJobsToBlocklist`,
`backendscheduler.go:~806`), independent of this catalog work. This is a
deliberate design choice, not an oversight: hooking `retention.go` directly would
require `tempodb`'s own retention path (used by EVERY deployment, not just
Postgres-opted-in ones) to gain a Postgres dependency — violating the "opt-in
additional backend, zero risk to existing paths" principle this entire task is
built on. The reconciliation-based approach keeps 100% of the Postgres/catalog
logic inside the NEW, opt-in `filecatalog` package and `backendscheduler.go`'s own
already-conditional new ticker branch — retention.go, compactor.go, and every
other existing deletion path are **completely untouched**.

**Residual race window (stated explicitly, not hidden):** a block could
theoretically be compacted/cleared in the interval between two lister ticks. Given
`CompactedBlockRetention`'s default of 1 hour (the gap between `MarkBlockCompacted`
and the later `ClearBlock` that actually removes the object) and a proposed
`CatalogListInterval` default of 5 minutes, there is a >55-minute safety margin
before any physically-deleted object could still have a live (non-soft-deleted)
catalog row referencing it — comfortably closing the specific race that caused
tonight's failures (which happened within a single backfill run's list-to-fetch
window, on the order of seconds to low minutes, not 55+ minutes). This is a
probabilistic, interval-bounded guarantee, not an absolute one — flag explicitly
in this package's doc comment (as already specified in 3.5) so a future operator
narrowing `CompactedBlockRetention` understands the dependency.

### 3.7 — Cube's own backfill: explicitly NOT changed (investigated, not assumed)

Per the brainstorm's own R6 finding (confirmed, not re-derived): cube's backfill
(`cube_backfill.go`) reads **cheap, pre-extracted VI files** via a
`viBackfillSource`-shaped fetcher, not raw historical blocks via `backend.Reader.
Blocks()/BlockMeta()` — it never had the live-listing race this file-catalog work
fixes for VI's `viBlockFetcher`. **No changes to cube_backfill.go in this plan.** A
future follow-up COULD point cube's own file discovery at the same `file_catalog`
table if cube ever needs raw-block discovery, but that is explicitly out of scope
here (flagged, not silently assumed).

---

## PART 4 — SQL schema (complete, all 3 concerns)

**File: `tempo/tempodb/encoding/vblockpack/schema/registries.sql` (new, reviewed
artifact — NOT applied to any live Postgres)**

```sql
-- viusage: one row per (tenant, col_hash, col_type). Replaces the shared
-- <tenant>/viusage/index.json blob's per-tenant contention point.
CREATE TABLE IF NOT EXISTS viusage_entries (
    tenant              TEXT    NOT NULL,
    col_hash            TEXT    NOT NULL,
    col_type            TEXT    NOT NULL,
    column_name         TEXT    NOT NULL,
    first_seen_sec       BIGINT  NOT NULL DEFAULT 0,
    created_at           BIGINT  NOT NULL DEFAULT 0,
    lease_owner_id        TEXT    NOT NULL DEFAULT '',
    lease_expires_at       BIGINT  NOT NULL DEFAULT 0,
    watermark_sec         BIGINT  NOT NULL DEFAULT 0,
    window_start_sec       BIGINT  NOT NULL DEFAULT 0,
    window_end_sec         BIGINT  NOT NULL DEFAULT 0,
    triggered             BOOLEAN NOT NULL DEFAULT FALSE,
    backfill_in_progress    BOOLEAN NOT NULL DEFAULT FALSE,
    done                  BOOLEAN NOT NULL DEFAULT FALSE,
    last_catalog_row_id     BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant, col_hash, col_type)
);

-- viusage: the "list of queries" — genuinely unbounded, append-only, DECOUPLED
-- from the hot-path entries row above (resolves the brainstorm's Q2 in favor of
-- the "expanded reading": the user's own phrasing lists "backfill state AND the
-- list of queries" as two distinct things; with the repeated-use ring removed by
-- Part 0, there is no remaining hot-path reason to bound or co-locate this data).
-- Never read by RecordUseAndMaybeTrigger's trigger logic (which no longer needs
-- to count anything) — write-only from the application's perspective, an
-- observability/audit capability only. Postgres-backend-only: the blob-backed
-- (S3/Local/GCS/Azure) path gets NO equivalent (never had one; not a regression).
CREATE TABLE IF NOT EXISTS viusage_query_log (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant      TEXT NOT NULL,
    col_hash    TEXT NOT NULL,
    col_type    TEXT NOT NULL,
    queried_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_viusage_query_log_lookup
    ON viusage_query_log (tenant, col_hash, col_type, queried_at DESC);

-- cube: one row per active cube. dimensions/filters/agg_attrs/watermarks are
-- variable-shape/nested (Filters especially — ColumnFilter has its own sub-
-- structure) and bounded in practice (MaxCubesPerTenant=1000, small watermark
-- maps keyed by resolution level) — stored as JSONB rather than fully normalized
-- into child tables, a deliberate simplicity choice for this data's actual shape
-- and access pattern (loaded whole per cube, never queried by sub-field).
CREATE TABLE IF NOT EXISTS cube_entries (
    cube_id      TEXT    NOT NULL PRIMARY KEY,
    tenant       TEXT    NOT NULL,
    dimensions   JSONB   NOT NULL DEFAULT '[]',
    filters      JSONB   NOT NULL DEFAULT '[]',
    agg_attrs    JSONB   NOT NULL DEFAULT '[]',
    resolution   INT     NOT NULL DEFAULT 1,
    created_at   BIGINT  NOT NULL DEFAULT 0,
    watermarks   JSONB   NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_cube_entries_tenant ON cube_entries (tenant);
```

**File: `tempo/tempodb/encoding/vblockpack/schema/file_catalog.sql`** — see Part 3.1
(kept as a separate file since it's a genuinely distinct concern/lifecycle from the
two registry tables above).

- [ ] All three files are **reviewed, ready-to-apply artifacts only** — no
      migration tool wiring, no `CREATE TABLE` executed against any real Postgres
      instance as part of this plan. The concurrency/integration tests in Part 5
      apply this SQL against a testcontainers-go **ephemeral, throwaway** instance
      only (spun up and torn down per test run — not "a real Postgres instance
      outside the team's own tests," per the checkpoint's own wording).

---

## PART 5 — Test strategy

### 5.1 — Local/ephemeral Postgres: testcontainers-go, confirmed as the approach

No existing precedent in either repo (confirmed by the brainstorm's own grep pass,
re-confirmed here — no `testcontainers-go` in either `go.mod`). Proceeding with
testcontainers-go's Postgres module (`github.com/testcontainers/testcontainers-go/
modules/postgres`) as the brainstorm's own leaning — it is the standard, well-
maintained Go idiom for this exact need (spin up, migrate, test, tear down,
per-test-run, no persistent infra), and does not require any live infrastructure
beyond a local Docker/Podman daemon already available in this repo's standard Go
test sandbox (the SAME assumption every other container-based Go integration test
suite in this ecosystem already makes — flag for the coder phase to verify Docker
is actually available in the CI runner before relying on this, but no evidence
found that it would NOT be, and no cheaper realistic alternative exists for a
genuine multi-connection, real-row-lock concurrency test).

**File: `tempo/tempodb/encoding/vblockpack/pg_testutil_test.go` (new)**

- [ ] `newTestPostgresPool(t *testing.T) *pgxpool.Pool` — starts a
      `postgres.RunContainer` (testcontainers-go), applies `schema/registries.sql`
      + `schema/file_catalog.sql` verbatim (the SAME files Part 4 produces — this is
      the test that PROVES those schema files are actually valid, executable SQL,
      not just reviewed prose), registers `t.Cleanup` to terminate the container,
      returns a connected `*pgxpool.Pool`. Skip (`t.Skip`) with a clear message if
      Docker is unavailable in the environment (standard testcontainers-go
      pattern) — do not fail the whole suite in a sandbox without Docker.

### 5.2 — TDD ordering: the FIRST test to write for Part 2/3 (concrete, not abstract)

Before writing ANY of `pg_entrystore.go`'s implementation:

1. Write `pg_testutil_test.go`'s `newTestPostgresPool` helper first (infra-only, no
   assertions yet) — run it standalone, confirm a container actually starts and the
   schema applies without error. This is the "does the environment even support
   this" smoke test, done before any real logic exists.
2. Write `TestPgViUsageEntryStore_UpsertEntry_CreateThenMutate` (5.5 below, Part 2.5)
   against a NOT-YET-EXISTING `newPgViUsageEntryStore` — confirm it fails to
   compile.
3. Implement `pg_entrystore.go` until that one test passes.
4. Only then write the remaining Load/error-path tests (2.5's second/third bullets).
5. Only then write the concurrency test (5.3) — it depends on the basic
   create/mutate path already being correct, and mutation-testing a concurrency
   test against broken basic CRUD produces false confidence.

### 5.3 — Mutation-tested concurrency test (mirrors
`TestRawObjectStore_ConditionalPut_ConcurrentWritersNoLostUpdates`, per this
session's own standing convention)

**File: `tempo/tempodb/encoding/vblockpack/pg_entrystore_test.go` (continued)**

- [ ] `TestPgViUsageEntryStore_UpsertEntry_ConcurrentTriggersConvergeOnOneWinner` —
      the required regression guard: spin up N (≥20, matching the existing blob-path
      test's `n=20`) goroutines, all calling `UpsertEntry` for the SAME
      (tenant, colHash, colType) key with a `createIfMissing`/`mutate` pair
      structurally equivalent to `RecordUseAndMaybeTrigger`'s post-Part-0
      unconditional-trigger logic (first caller to actually create+trigger wins;
      every other concurrent caller observes `Triggered` already `true` and takes
      the no-op branch). Assert: **exactly one** of the N goroutines' resulting
      `Entry.Backfill.LeaseOwnerID` matches what ends up persisted (i.e., exactly
      one lease-acquire actually "won"), and the FINAL row's `Triggered==true`,
      `BackfillInProgress==true` — no double-trigger, no lost update.
- [ ] **Mutation-test this exact test before considering it done** (per this
      session's own standing convention, memory: "reintroduce the exact bug a test
      claims to catch, confirm it fails, then revert"): temporarily remove the
      `SELECT ... FOR UPDATE` row-lock clause from `loadEntryForUpdate` (making the
      read a plain, non-locking `SELECT`), re-run this test, confirm it now FAILS
      (proving the row lock is actually load-bearing for this guarantee, not
      coincidentally passing due to test timing) — then revert the removal and
      confirm the test passes again. Do this by hand during implementation review;
      do not skip it because the SQL "looks obviously correct."

### 5.4 — File-catalog lister tests (blockpack-free, tempo-only, no real Postgres needed for most of these)

**File: `tempo/modules/backendscheduler/filecatalog/lister_test.go` (new)**

- [ ] `TestLister_RunOnce_InsertsNewBlocks` — fake `blockMetas`/`tenants` funcs, real
      ephemeral Postgres (testcontainers-go, reuse 5.1's helper via a shared
      package or a small duplicate — tempo's module boundary between
      `modules/backendscheduler/filecatalog` and `tempodb/encoding/vblockpack`
      likely means this test infra helper needs its own small copy or a shared
      `internal/testutil`-style package; decide at implementation time based on
      import-cycle constraints, flag if one exists).
- [ ] `TestLister_RunOnce_SoftDeletesVanishedBlocks` — seed a catalog row, then call
      `RunOnce` with a `blockMetas` func that no longer returns that block ID,
      assert `deleted_at` is set (not a physical row delete).
- [ ] `TestLister_RunOnce_ReappearedBlock_ClearsDeletedAt` — defensive edge case
      (3.5's `ON CONFLICT ... DO UPDATE SET deleted_at = NULL`) — seed a
      soft-deleted row, call `RunOnce` with `blockMetas` returning that block ID
      again, assert `deleted_at` is cleared.

### 5.5 — Catalog-cursor `BlockFetcher` tests

**File: `tempo/tempodb/encoding/vblockpack/vi_backfill_catalog_test.go` (new)**

- [ ] `TestCatalogBlockFetcher_ListBlocksInRange_RespectsCursor` — seed catalog rows
      with known row_ids, construct a fetcher with `cursorRowID` set to skip the
      first half, assert only later rows are returned.
- [ ] `TestCatalogBlockFetcher_ListBlocksInRange_SkipsSoftDeletedRows` — seed a
      soft-deleted row inside the time window, assert it's excluded.
- [ ] `TestCatalogBlockFetcher_MaxRowIDSeen_TracksHighestReturned` — direct
      assertion on the accessor Part 3.4's cursor-persist step depends on.
- [ ] `TestRunViBackfillCore_CursorNotAdvancedOnPartialFailure` — inject a
      `BlockFetcher`/`ObjectPutter` combination where `FetchBlock` fails on the
      SECOND of 3 candidate blocks; assert `registry.UpdateCatalogCursor` is never
      called (via a fake `Registry`/`EntryStore` spy) — the explicit regression pin
      for Part 3.4's "never advance past an unconfirmed run" claim.

---

## PART 6 — CloudNativePG manifest (reviewed artifact, right-sized, NOT applied)

Per the checkpoint: this manifest is written and reviewed only. **No `kubectl
apply` in this plan or any step within it.**

**File: `tempo/.bob/artifacts/cnpg-cluster-viusage.yaml` (new, reviewed-only)**

Right-sizing rationale (per this plan's own explicit instruction to NOT copy
#467's sizing verbatim): #467's original use case is a file-metadata catalog for
EVERY value-index file across EVERY tenant (potentially millions of rows,
high-QPS range-scan queries backing live search). This task's data volume is
categorically smaller: `viusage_entries`/`cube_entries` are bounded by
(distinct non-dedicated columns per tenant) × (tenant count) — realistically
low thousands of rows fleet-wide, not millions; `viusage_query_log` is unbounded
but append-only, low-QPS-per-write (rate-limited to ~1 write per (tenant,column)
per 10s by tempo's existing `viUsageRateLimit`, confirmed in `vi_usage_hook.go`);
`file_catalog` is the largest table (one row per historical block, could reach
low millions fleet-wide over long retention windows) but is write-mostly from a
SINGLE lister process (backend-scheduler), not high-QPS from every querier.

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: tempo-viusage-postgres
  namespace: tempo-dev-test-03  # dev-test-03 ONLY per this task's scope — not fleet-wide
spec:
  instances: 3                  # unchanged from #467 — HA floor, not a sizing knob
  # Right-sized DOWN from #467's ~1-2 cores/2-4GB (that sizing was for a
  # file-metadata catalog backing live production search queries fleet-wide):
  resources:
    requests:
      cpu: "250m"
      memory: "512Mi"
    limits:
      cpu: "1"
      memory: "1Gi"
  storage:
    size: 10Gi                  # down from #467's implied larger footprint; file_catalog
                                 # is this plan's largest table and is still bounded by one
                                 # tenant-set's historical block count on ONE dev cluster
    storageClass: local-path    # local-PV, matching #467's own "durability comes from
                                 # WAL replicas, not the underlying disk" rationale
  affinity:
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            cnpg.io/cluster: tempo-viusage-postgres
  postgresql:
    parameters:
      max_connections: "100"    # small fleet (dev-test-03 only): a handful of
                                 # queriers/frontends/workers × MaxConns=4 default (2.3)
                                 # comfortably fits; revisit before any fleet-wide rollout
  walStorage:
    size: 5Gi
  backup:
    barmanObjectStore:
      destinationPath: "s3://tempo-dev-test-03-pg-backup/viusage"
      s3Credentials:
        accessKeyId:
          name: pg-backup-creds
          key: ACCESS_KEY_ID
        secretAccessKey:
          name: pg-backup-creds
          key: ACCESS_SECRET_KEY
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: tempo-viusage-postgres-pdb
  namespace: tempo-dev-test-03
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      cnpg.io/cluster: tempo-viusage-postgres
```

- [ ] Also required (separate, larger, explicitly flagged as its own prerequisite
      per the brainstorm's own risk finding): **installing the CloudNativePG
      operator itself** — confirmed absent from tempo-dev-test-03 and every other
      checked cluster. This plan does NOT attempt operator installation; it is a
      distinct infrastructure task requiring its own go/no-go, sequenced strictly
      BEFORE this Cluster manifest could ever be applied.

---

## PART 7 — One-time S3-to-Postgres importer (reviewed artifact, NOT run)

**File: `tempo/cmd/tempo-cli/cmd/importviusage/importviusage.go` (new, or wherever
`tempo-cli`'s existing subcommand convention lives — check `cmd/tempo-cli/`'s
actual structure at implementation time and match it, e.g. alongside existing
`tempo-cli` subcommands)**

- [ ] Reads `<tenant>/viusage/index.json`/`<tenant>/cubes/index.json` per tenant via
      the EXISTING `minioObjectStore`/`viUsageObjectStore` Get path (no new S3
      client code needed — reuse), decodes each `Entry`/`RegistryEntry`, and
      `INSERT ... ON CONFLICT (tenant, col_hash, col_type) DO NOTHING` (or
      `DO UPDATE`, decide based on whether re-running the importer idempotently
      should overwrite or skip — recommend `DO NOTHING`, since a live Postgres
      registry could already be ahead of a stale S3 snapshot by the time an
      operator runs this) into `viusage_entries`/`cube_entries` via a
      `*pgxpool.Pool` built from `modules/postgres.NewPool`.
- [ ] `UseTimestamps` (source JSON) is simply DROPPED during import — Part 0 removed
      the field; no destination column exists for it, and its semantic purpose
      (threshold evaluation) no longer exists either.
- [ ] `--dry-run` flag: decode and print what WOULD be imported, no writes — the
      safe default for any real future invocation; requires an explicit
      `--commit` flag to actually write.
- [ ] Explicitly documented in this file's own doc comment: **for
      tempo-dev-test-03's OWN cutover, this importer is NOT run** (per the
      brainstorm's Q3 finding: `CoversRange` already treats "never triggered" as
      "no coverage," so losing in-progress dev-test-03 backfill state is a
      wasted-work risk, not a data-loss risk — "start fresh in Postgres" is the
      accepted plan for THIS environment specifically). This importer exists as a
      reviewed, ready-to-use artifact for a hypothetical future production rollout
      where redundant backfill cost would actually matter.

---

## PART 8 — Execution order (dependency-ordered, parallelizable across 2 coders)

**Coder A (blockpack-focused):**
1. Part 0.1 (TDD tests) → 0.2 (implementation) → 0.4 (confirm cube untouched) —
   ~1-2 hours.
2. Part 1.2/1.3 (entryStore refactor, viusage then cube) — behavior-preserving,
   verify full existing suite still green.
3. Part 1.4/1.5 (exported EntryStore + NewRegistryFromEntryStore + tests).
4. `go mod vendor` inside blockpack is NOT touched by this task (no new blockpack
   deps) — but blockpack's own local checkout state needs to just be saved; tempo's
   `replace github.com/grafana/blockpack => ../blockpack` directive, confirmed
   present at `tempo/go.mod:475`, picks up local changes automatically — no
   publish step needed for this dev-only workflow, matching this session's
   existing revendor discipline.

**Coder B (tempo-focused, can start on config/schema work in parallel, but
`pg_entrystore.go`'s real implementation depends on Coder A's Part 1.4 landing
first):**
1. Part 2.1 (pgx vendoring) + 2.3 (postgres.Config/pool) — no dependency on
   blockpack changes, start immediately.
2. Part 4 (SQL schema files) — no code dependency, start immediately, write
   alongside Part 5.1's test-infra helper (which needs the schema files to exist).
3. Part 3.1 (file_catalog schema) — same, start immediately.
4. **Blocked on Coder A's Part 1.4:** Part 2.2 (`pg_entrystore.go`), 2.4 (dispatch
   wiring), 2.5 (tests).
5. Part 3.3/3.4 (catalogBlockFetcher, cursor persist) — depends on Part 0.2's
   `BackfillState.LastCatalogRowID`/`UpdateCatalogCursor` (Coder A) landing, but is
   otherwise independent of Part 2's EntryStore work — can proceed in parallel with
   step 4 above once Coder A's Part 0 is done (Part 0 lands FIRST, before Part 1,
   in Coder A's own sequence, so this unblocks early).
6. Part 3.5 (backend-scheduler lister) — depends only on Part 3.1's schema + Part
   2.3's `postgres.Config` (both early, no cross-repo blocker) — can start early.
7. Part 5 (all tests) — write alongside each corresponding implementation step per
   TDD, not batched at the end.
8. Part 6/7 (CNPG manifest, importer) — no code dependency on anything else, can
   be done ANY time, including first (pure artifact-writing).

**Suggested actual order for a single coder (if not parallelizing):** 0 → 1 → 4
(schema, cheap, unblocks everything else's tests) → 5.1 (test infra) → 2 → 3 → 6 → 7.

---

## PART 9 — Checkpoint (restated explicitly, final word of this plan)

**No task in this plan may run `kubectl apply` against any real cluster.**
**No task in this plan may connect to or provision any real Postgres instance
outside the team's own ephemeral testcontainers-go test infra (Part 5.1).**
All Postgres/CNPG/manifest/importer artifacts in Parts 4, 6, and 7 are written,
reviewed, and left un-applied/un-run.

**This plan's last step is: report back to the team lead for an explicit go/no-go
before anything past this point.** There is nothing past this point in this plan —
implementation and tests only, as instructed.

---

## Notes for the coder phase

- Confirm the actual current pgx v5 release version before pinning `go.mod` (this
  plan does not fabricate a precise patch version — check
  `pkg.go.dev/github.com/jackc/pgx/v5?tab=versions` at implementation time).
- The "Go visibility trap" worked through in Part 1.4 (private-method interface
  cannot be satisfied structurally from outside its own package) is real and
  easy to get wrong under time pressure — read that section's full reasoning
  before implementing, don't just copy the final code block without understanding
  why the naive single-interface approach fails.
- `backendscheduler`'s exact `storage.Store` accessor names
  (`Tenants()`/`BlockMetas()`) should be re-verified against the real interface
  definition at implementation time — this plan read call SITES, not the
  interface declaration itself, so confirm exact method names before writing
  `filecatalog.Lister`'s injected function signatures.
- Every new exported Go symbol introduced by this plan (blockpack:
  `EntryStore`, `NewRegistryFromEntryStore`, `Registry.UpdateCatalogCursor`,
  `BackfillState.LastCatalogRowID`, and cube's mirrored equivalents; tempo:
  `ConfigureViUsage`/`ConfigureCubeManager`'s new `pgPool` params) should be
  re-confirmed with the user/team lead before merging, per this session's
  standing "explicit sign-off before new/changed public API" discipline — Part 0's
  table at the top of this document is the single consolidated list to walk
  through for that conversation.

## Suggested `lth store` call (planner had no Bash access — team lead should run this)

```
~/bin/lth store --layer 4 --attr 'project=tempo' \
  --attr 'tags=planning,architecture,postgres,cnpg,viusage,filecatalog' \
  'Plan: Postgres-backed viusage/cube registries + trigger-threshold removal +
   file catalog. Approach: opt-in additional backend (Approach B), row-oriented
   EntryStore interface owned by blockpack with pgx implementation owned entirely
   by tempo (zero new blockpack deps). Key decisions: (1) TriggerConfig.Threshold/
   WindowSeconds and Entry.UseTimestamps deleted outright per team-lead ruling —
   first use always triggers, RecordUseAndMaybeTrigger/MaybeRecordUseAndMaybeTrigger
   signatures unchanged; (2) two-interface+adapter shape needed for EntryStore since
   Go forbids exporting an interface with unexported method names structurally;
   Postgres UpsertEntry uses SELECT...FOR UPDATE + one transaction, no retry loop,
   READ COMMITTED sufficient; (3) "list of queries" resolved as a SEPARATE unbounded
   viusage_query_log table, decoupled from the hot-path viusage_entries row; (4) file
   catalog lister piggybacks on backend-scheduler (confirmed already-singleton,
   already polls BlockMetas) — no new binary; soft-delete via periodic reconciliation
   against BlockMetas, not a retention.go hook, bounded by CompactedBlockRetention
   (1h) vs ListInterval (5m) safety margin; (5) BlockFetcher interface unchanged —
   catalogBlockFetcher is a new implementation querying file_catalog by cursor
   (BackfillState.LastCatalogRowID, new Registry.UpdateCatalogCursor method,
   monotonic-only), cursor only advances after eng.Run fully succeeds.'
```
