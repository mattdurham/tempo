# Implementation Plan: job-planner — centralized vi_backfill/cube_backfill chaining

## Provenance note

The planner in this session had no Bash/MCP tool access — the `~/bin/lth stats` /
`~/bin/lth prompt` bootstrap and the final `~/bin/lth store` call could not be run
directly. The team lead should run the `lth store` call themselves; suggested content
is at the end of this document. This plan is built entirely from direct code reads
(cited by file:line throughout) against the actual `job-planner-518` worktree, not
from the brainstorm's claims alone — several of the brainstorm's conclusions are
**corrected** below with concrete evidence (see "Corrections to the brainstorm").

This document fully replaces the previous (stale, unrelated #504/#507
Postgres-wiring) `plan.md`.

## Overview

Build a new standalone tempo module target, `-target=job-planner`, whose only job is
a Postgres poll loop that chain-enqueues the next bounded-window `vi_backfill`/
`cube_backfill` job for columns/cubes that have already been triggered at least once
and have more history left to backfill. Add a real bounded-window mechanism to both
backfill engines (which today do **not** have a working one — see corrections
below). Bundle `cube_backfill` into the same first PR. No changes to trace/span
compaction, no execution-engine algorithm changes, no proactive scanning of
never-queried columns.

---

## Corrections to the brainstorm (verified by direct code read — read this before implementing)

### Correction 1: cube_backfill's `WindowMinutes` field is dead. Bundling it is NOT "near-free."

The brainstorm claims cube_backfill "already accepts a `WindowMinutes` parameter and
already produces minute-grained output" and needs "zero execution-engine changes."
**The first half is true, the "near-free" framing is misleading.**

Confirmed via direct read:
- `jobstore.CubeBackfillDetail.WindowMinutes` (`tempodb/encoding/vblockpack/jobstore/jobstore.go:65`)
  is written at both job-creation call sites in `cubequerypath.go:121,181` — always as
  `math.MaxUint32` — but it is **never read**. `processCubeBackfillJobPostgres`
  (`modules/backendworker/backendworker.go:447-494`) unmarshals `detail` but only uses
  `detail.CubeID`; the actual window bound passed to `RunCubeBackfill` is
  `retentionMinutes`, computed independently from tenant config
  (`backendworker.go:486`, `cube_backfill.go:128 cubeBackfillWindowMinutes`). **The
  per-job `WindowMinutes` field is write-only dead JSONB today.**
- More importantly: `cube.Backfiller.Run(ctx, currentMinute, progressFn)`
  (`vendor/.../internal/modules/cube/backfill.go:91-112`) computes
  `startMinute = currentMinute-1` and `endMinute = currentMinute - WindowMinutes`,
  where `currentMinute` defaults to **`time.Now()`** whenever the caller passes `0`.
  **Every single call site in this codebase passes `currentMinute = 0`**
  (`cubequerypath.go:148`, `cube_backfill.go:104`) — meaning every cube_backfill run,
  no matter how many times chained, always processes the SAME "most recent N minutes"
  window relative to wall-clock now. **If job-planner simply shrinks `WindowMinutes`
  and enqueues repeated small jobs, every job reprocesses nearly the same recent
  slice and NEVER makes progress into older history.** This is not a hypothetical —
  it is the literal behavior of the code as it exists today.
- The fix is small but real: `Backfiller.Run`'s `currentMinute` parameter is already
  exactly the "anchor" mechanism needed — it is just never wired to anything but 0.
  `RegistryEntry.Watermarks[CubeRollupL0].MinMinute`
  (`vendor/.../internal/modules/cube/definition.go:61-66,82-83`) already holds the
  oldest minute successfully backfilled so far (`UpdateWatermarks` expands the range
  min-of-mins/max-of-maxes, `registry.go:115-125`) — exactly the resume point.
  job-planner must read this value and pass it as `currentMinute` when it enqueues
  the next chained job (see Part 2 below for exactly how this flows through
  `RunCubeBackfill`'s existing `currentMinute uint32` parameter,
  `vendor/.../cube_backfill_runner.go:255-271` — **no new blockpack-side parameter
  needed for cube**, just correct wiring of an existing one).

### Correction 2: VI backfill has the identical anchor gap, and needs a genuinely new field (unlike cube).

`BackfillEngine.Run` (`vendor/.../valueindex_backfill.go:137-142`) computes
`maxSec := e.cfg.Now().Unix()` unconditionally (`cfg.Now` defaults to `time.Now`,
`valueindex_backfill.go:87-89`) and `minSec := maxSec - WindowSeconds`. There is no
parameter analogous to cube's `currentMinute` — `Now` exists purely for test
determinism, not as a caller-supplied resume anchor. Tempo's wrapper
(`tempodb/encoding/vblockpack/vi_backfill.go:205-214`) always sets
`WindowSeconds: math.MaxUint64` — genuinely unbounded, matching the brainstorm's
finding, but shrinking `WindowSeconds` alone would hit the exact same "always
reprocess the most recent slice" bug as cube.

**Unlike cube, VI needs a real new field** (`BackfillConfig` has no existing
anchor-shaped parameter to reuse). Add `AnchorSec uint64` to `blockpack.BackfillConfig`
(0 means "use `cfg.Now()`", preserving today's default behavior exactly): change
`valueindex_backfill.go:138` from `maxSec := uint64(e.cfg.Now().Unix())` to
`maxSec := e.cfg.AnchorSec; if maxSec == 0 { maxSec = uint64(e.cfg.Now().Unix()) }`.
Tempo threads `entry.Backfill.WatermarkSec` (`vendor/.../internal/modules/viusage/entry.go:53`,
already present on every `Entry` passed into `runViBackfillCore`/`RunViBackfill`) as
`AnchorSec` — **read live from the entry at execution time, not stored in the job
detail** (the entry is already loaded before `RunViBackfill` runs; storing a second,
potentially-stale copy in `jobstore.ViBackfillDetail` would be redundant and could
drift if a job sits queued for a while — dedup_key already guarantees at most one
non-terminal job per column, so the watermark cannot move between enqueue and claim
either way, but reading live is simpler and has one fewer place to get wrong).

### Correction 3: backend-worker's completion handler already has NO chain-enqueue logic to remove.

The brainstorm (and the team lead's brief, following it) describes "backend-worker's
job-completion handler gets SIMPLER (no longer chain-enqueues)" as if removing
existing logic. **Confirmed false by direct read:** `reportPostgresJobOutcome`
(`modules/backendworker/backendworker.go:381-390`) only ever calls
`w.jobStore.Complete`/`w.jobStore.Fail` — it has never enqueued anything. The
brainstorm's Approach 2 (backend-worker chains on completion) was **considered but
never implemented** before the user's decision (d) superseded it. **There is no
backend-worker diff in this plan** — Part 5 below is a no-op confirmation step, not a
removal.

---

## Open questions — concrete calls

1. **Bundle `cube_backfill` into the same first PR: YES.** Per Correction 1, this is
   not literally "free," but the extra work (wiring `currentMinute` from
   `Watermarks[CubeRollupL0].MinMinute`) is small, self-contained, and touches the
   same files (`cube_backfill.go`, job-planner's poll loop) the VI change already
   requires touching for its own parallel structure. Splitting it into a second PR
   would mean re-deriving the exact same poll-loop skeleton twice.
2. **Relationship to #517: stays separate**, confirmed — no shared code path (trace
   compaction is ring/gRPC-dispatched, this is Postgres-poll-dispatched; the only
   shared table family is Postgres itself, and even `backend_jobs` is untouched by
   #517's scope per its own ticket).
3. **Window size / poll interval — concrete starting values, not literals to bikeshed
   further at execution time:**
   - **Poll interval: 60s.** Cheap indexed scan (see Part 3.3's query), no need for
     sub-minute latency given job-planner only continues already-in-progress
     backfills (not user-facing latency-sensitive).
   - **VI window size: 6 hours (21600s).** The existing default full-window constant
     is `defaultBackfillWindowSeconds = 48h` (`valueindex_backfill.go:37`) — 6h is a
     meaningful granularity reduction (8x smaller than the old effectively-unbounded
     runs) while keeping per-job overhead (one S3 block-listing call, N block
     fetches) low relative to job count. Configurable (`JobPlannerConfig.ViWindowSeconds`).
   - **Cube window size: 1440 minutes (24h).** cube_backfill reads cheap pre-extracted
     VI data (no raw block I/O), so it can afford a coarser default than "the issue's
     own literal 1-minute example" without meaningfully hurting lease safety — 24h of
     per-minute VI lookups is still well under the 30-minute lease
     (`jobstore.go:120-137`'s `lease_expires_at = now() + interval '30 minutes'`).
     Configurable (`JobPlannerConfig.CubeWindowMinutes`).
   - Both are op-tunable, not hardcoded — see Part 1's config struct.
4. **Naming: `job-planner`** (matches worktree/branch, no strong alternative surfaced
   in the brainstorm) — used as the final module target name, not just a working
   name, throughout this plan.

---

## PART 1 — Config plumbing

**File: `tempodb/encoding/common/config.go` (modify)**

- [ ] Add new struct, placed after `ValueCountCompactorConfig` (~line 428-...):
  ```go
  // JobPlannerConfig configures the job-planner Tempo target: a poll loop that
  // chain-enqueues the next bounded-window vi_backfill/cube_backfill job for
  // columns/cubes already triggered at least once with more history left to
  // backfill (issue #518). Mirrors ValueIndexCompactorConfig's YAML-decoding-only
  // shape (no direct blockpack import needed — job-planner has no blockpack
  // dependency at all, see Part 3).
  type JobPlannerConfig struct {
      Enabled           bool          `yaml:"enabled"`
      PollInterval      time.Duration `yaml:"poll_interval"`       // default 60s
      ViWindowSeconds   uint64        `yaml:"vi_window_seconds"`   // default 21600 (6h)
      CubeWindowMinutes uint32        `yaml:"cube_window_minutes"` // default 1440 (24h)
  }

  func (c *JobPlannerConfig) applyDefaults() {
      if c.PollInterval <= 0 {
          c.PollInterval = 60 * time.Second
      }
      if c.ViWindowSeconds == 0 {
          c.ViWindowSeconds = 6 * 3600
      }
      if c.CubeWindowMinutes == 0 {
          c.CubeWindowMinutes = 1440
      }
  }
  ```
- [ ] Add field to `BlockpackConfig` (~line 50-165, alongside `ValueIndexCompactor`/
      `CubeCompactorEnabled`): `JobPlanner JobPlannerConfig \`yaml:"job_planner"\``.
      Placed here (not a new top-level `StorageConfig` section) because job-planner
      is a blockpack-schema-driven pipeline component exactly like
      ValueIndexConsumer/ValueIndexCompactor — same config-ownership precedent.

**TDD:** `tempodb/encoding/common/config_blockpack_test.go` (modify) — add
`TestJobPlannerConfig_Defaults` asserting the three defaults above apply when
`Enabled: true` and the other fields are zero-valued. Write this first, confirm it
fails (struct doesn't exist), then implement.

---

## PART 2 — Bounded-window / anchor changes to the backfill engines

### 2.1 — blockpack: `BackfillConfig.AnchorSec` (new field, VI only)

**File: `vendor/github.com/grafana/blockpack/valueindex_backfill.go` (modify — vendor
patch, then upstream to blockpack's real repo per this project's revendor
discipline)**

- [ ] TDD first: add `TestBackfillEngine_Run_AnchorSecOverridesNow` to
      `valueindex_backfill_test.go` — construct a `BackfillEngine` with
      `AnchorSec: 1000, WindowSeconds: 100`, a fake `Now` that would return something
      else entirely if called, and a fake `BlockFetcher` recording the
      `(minSec, maxSec)` it was asked to list. Assert `ListBlocksInRange` was called
      with `maxSec=1000, minSec=900` — i.e. `Now()` was never consulted. Confirm this
      fails to compile (field doesn't exist) before implementing.
- [ ] Add `AnchorSec uint64` field to `BackfillConfig` (after `WindowSeconds`, same
      file, ~line 68) with a doc comment: "AnchorSec, if non-zero, is the window's
      newest edge (replacing `Now()`) — set by a caller resuming a chained backfill
      from a previously-persisted watermark. Zero means 'anchor to the current wall
      clock', preserving all pre-existing callers' behavior unchanged."
- [ ] `Run` (`valueindex_backfill.go:137-142`): change
      ```go
      maxSec := uint64(e.cfg.Now().Unix())
      ```
      to
      ```go
      maxSec := e.cfg.AnchorSec
      if maxSec == 0 {
          maxSec = uint64(e.cfg.Now().Unix())
      }
      ```
- [ ] Regression check: every existing test that does NOT set `AnchorSec` must pass
      unmodified (zero value preserves old behavior exactly) — run
      `go test ./... -run BackfillEngine` in blockpack's vendor copy (or the real
      blockpack repo, per this project's revendor workflow) before and after.

### 2.2 — tempo: thread `entry.Backfill.WatermarkSec` as `AnchorSec`

**File: `tempodb/encoding/vblockpack/vi_backfill.go` (modify)**

- [ ] TDD first: add `TestRunViBackfillCore_UsesEntryWatermarkAsAnchor` to
      `vi_backfill_test.go` — construct an `entry` with `Backfill.WatermarkSec` set to
      a known non-zero value, a fake fetcher recording its `ListBlocksInRange` call
      args, run `runViBackfillCore`, assert the fetcher was called with
      `maxSec == entry.Backfill.WatermarkSec` (not "now"). Confirm it fails before
      wiring the field through (2.1 must land first, or this test can be written and
      left red as a stacked-on-top-of-2.1 commit).
- [ ] In `runViBackfillCore` (`vi_backfill.go:205-214`), add
      `AnchorSec: entry.Backfill.WatermarkSec` to the `blockpack.BackfillConfig{...}`
      literal. `WindowSeconds` stays as the caller-supplied value (see 2.3 below for
      where that now comes from — no longer always `math.MaxUint64`).
- [ ] **Do not change this for the FIRST-ever backfill of a column** (WatermarkSec is
      genuinely 0 for a never-backfilled column, which is exactly what "anchor to
      now" should mean for a first pass — the zero-value fallback in 2.1 already
      handles this for free, no special-casing needed here).

### 2.3 — tempo: `ViBackfillDetail` gains a real bounded window; cubequerypath.go's reactive-trigger call sites use it

**File: `tempodb/encoding/vblockpack/jobstore/jobstore.go` (modify)**

- [ ] Add `WindowSeconds uint64 \`json:"window_seconds"\`` to `ViBackfillDetail`
      (mirrors `CubeBackfillDetail.WindowMinutes`'s existing shape exactly). Doc
      comment: "WindowSeconds bounds how far back from the column's current
      watermark this job processes. Zero means unbounded (full remaining history) —
      the reactive first-trigger path (`vi_usage_hook.go`) still passes zero here,
      preserving today's 'first backfill does everything' behavior; job-planner's
      chained continuation jobs (issue #518) pass a real bounded value."
- [ ] TDD: `jobstore_test.go` — extend `TestStore_InsertViBackfill_CreatesPendingRow`
      (or add a sibling test) asserting `WindowSeconds` round-trips through the JSONB
      `detail` column unchanged.

**File: `tempodb/encoding/vblockpack/vi_usage_hook.go` (modify)**

- [ ] At the `jobStore.InsertViBackfill(...)` call (~line 201-210), add
      `WindowSeconds: 0` explicitly (documents the "reactive first-trigger is
      unbounded" decision inline, matching decision (c) — this call site is
      unaffected by #518's scope, only made explicit).

**File: `tempodb/encoding/vblockpack/vi_backfill.go` (modify, continued from 2.2)**

- [ ] `processViBackfillJobPostgres` doesn't construct `BackfillConfig` directly
      today — that happens inside `runViBackfillCore`. Thread `detail.WindowSeconds`
      from `backendworker.go`'s `processViBackfillJobPostgres`
      (`modules/backendworker/backendworker.go:398-438`) through `RunViBackfill`
      into `runViBackfillCore`'s `WindowSeconds` field. Concretely: add a
      `windowSeconds uint64` parameter to `RunViBackfill`/`runViBackfillCore`
      (threaded from `RunViBackfillDeps` or as a new explicit param — prefer a new
      explicit param, since `RunViBackfillDeps` is backend-construction plumbing,
      not per-job data, and mixing them would blur that distinction), defaulting to
      `math.MaxUint64` when zero (preserves `vi_usage_hook.go`'s "0 means unbounded"
      contract exactly).
- [ ] `processViBackfillJobPostgres` passes `detail.WindowSeconds` (falling back to
      `math.MaxUint64` when zero, matching the contract above) into the new
      parameter.
- [ ] TDD: extend `backend_jobs_e2e_test.go`'s existing vi_backfill coverage with a
      case seeding `WindowSeconds: 3600` on a column whose entry already has a
      non-zero `WatermarkSec`, asserting the resulting block-listing window is
      `[WatermarkSec-3600, WatermarkSec]`, not `[now-3600, now]`.

### 2.4 — tempo: cube's `currentMinute` gets wired from `Watermarks[CubeRollupL0].MinMinute`

**File: `tempodb/encoding/vblockpack/cube_backfill.go` (modify)**

- [ ] `RunCubeBackfill`'s signature (`cube_backfill.go:85-88`) already threads to
      `blockpack.RunCubeBackfill(ctx, entry, viStore, store, pgPool, cfg, 0,
      defaultValueIndexPref)` — the `0` is the dead literal. Change the call site to
      pass `currentMinuteAnchor(entry)` instead of `0`, where:
      ```go
      // currentMinuteAnchor resolves the resume point for a chained cube backfill:
      // the oldest minute already confirmed backfilled (Watermarks[CubeRollupL0].
      // MinMinute), or 0 (meaning "anchor to wall-clock now", blockpack's own
      // zero-value convention, cube_backfill_runner.go/backfill.go) for a cube with
      // no L0 watermark yet -- i.e. its first backfill pass.
      func currentMinuteAnchor(entry blockpack.CubeRegistryEntry) uint32 {
          wm, ok := entry.Watermarks[blockpack.CubeRollupL0]
          if !ok {
              return 0
          }
          return wm.MinMinute
      }
      ```
- [ ] TDD first: `cube_backfill_watermark_test.go` (existing file, per Correction 1's
      grep hit) — add `TestCurrentMinuteAnchor_UsesExistingL0Watermark` and
      `TestCurrentMinuteAnchor_ZeroWhenNoWatermarkYet`, pure unit tests against the
      function above (no S3/Postgres needed — same pattern
      `cubeBackfillWindowMinutes` already uses in this file). Write first, confirm
      compile failure, implement.
- [ ] **Important correctness note for the coder:** `Backfiller.Run`'s window is
      `[currentMinute-WindowMinutes, currentMinute-1]` — passing
      `currentMinute = wm.MinMinute` means the NEXT window ends exactly at
      `wm.MinMinute - 1`, one minute older than the last confirmed minute, with no
      gap and no re-processing of an already-covered minute. Do not pass
      `wm.MinMinute - 1` (off-by-one; would redundantly reprocess `wm.MinMinute`
      itself, which `UpdateWatermarks`'s idempotent-overwrite tolerates but wastes
      one minute of work per chained job for no benefit).

### 2.5 — `CubeBackfillDetail.WindowMinutes` becomes real (currently dead per Correction 1)

**File: `modules/backendworker/backendworker.go` (modify)**

- [ ] `processCubeBackfillJobPostgres` (`backendworker.go:447-494`): change the
      `RunCubeBackfill` call (`backendworker.go:489`) to pass
      `detail.WindowMinutes` alongside `retentionMinutes` (both now flow into
      `RunCubeBackfill`) — `detail.WindowMinutes` becomes the PER-JOB bound (set by
      job-planner for chained jobs, or `math.MaxUint32` for the reactive-trigger's
      first job per `cubequerypath.go`'s existing literals, unchanged), while
      `retentionMinutes` remains an outer ceiling (a job's window can never exceed
      what tenant retention makes possible, regardless of what `WindowMinutes`
      requests — protects against a stale/misconfigured job-planner window value
      ever requesting more history than physically exists).
- [ ] `cubeBackfillWindowMinutes(retentionMinutes)` already does the "0 retention
      means unbounded" resolution (`cube_backfill.go:128-133`); `RunCubeBackfill`
      (`cube_backfill.go:85-121`) needs a new parameter for the per-job window
      (distinct from the retention ceiling it already takes), and its `cfg.WindowMinutes`
      assignment becomes `min(jobWindowMinutes, cubeBackfillWindowMinutes(retentionMinutes))`.
- [ ] TDD: extend `backend_jobs_e2e_test.go`'s cube_backfill coverage (per
      Correction 1's grep hits at lines 397, 495, 574, 632, 704, 757 — these already
      set `WindowMinutes: 60` but the field is currently ignored) with an assertion
      that a job seeded with a small `WindowMinutes` and a cube entry whose
      `Watermarks[CubeRollupL0].MinMinute` is already non-zero produces a
      **narrower, older** window than the retention ceiling alone would — i.e. these
      existing tests' `WindowMinutes: 60` literal finally becomes load-bearing
      instead of silently ignored. This is the single highest-value regression test
      in this whole plan: it directly pins Correction 1's bug fix.

---

## PART 3 — The `job-planner` tempo module

### 3.1 — Deployable shape

**File: `cmd/tempo/app/job_planner.go` (new)**

- [ ] `func (t *App) initJobPlanner() (services.Service, error)` — modeled on
      `initValueIndexCompactor`'s shape (`cmd/tempo/app/value_index.go:237-351`) for
      the enabled-check/services.NewIdleService pattern, but **deliberately does
      NOT depend on the `Store` module** (see design note below) and constructs its
      OWN `*pgxpool.Pool` directly via `postgres.NewPool(ctx, pgCfg)`
      (`modules/postgres/pool.go`, same call `initValueIndexCompactor` makes at
      `value_index.go:277`), closed in the `services.NewIdleService` stop func.
  - Hard-fails at init (mirrors `initValueIndexConsumer`'s `errors.New(...)` pattern,
    `value_index.go:177-179`) if `t.cfg.StorageConfig.Trace.Postgres == nil` — there
    is no fallback mode for job-planner (it has no non-Postgres purpose at all,
    unlike VI compactor which can run VI-only without Postgres).
  - No S3/GCS/Azure client construction anywhere in this file — job-planner never
    reads/writes blockpack files, only Postgres rows.

- [ ] **Design deviation from the team lead's brief, flagged explicitly:** the brief
      suggested mirroring value-index-compactor's `{Store, Server}` module
      dependency. **Do not do this.** Confirmed via direct read
      (`cmd/tempo/app/modules.go:544-559`, `initStore`) that the `Store` module
      constructs the FULL tempodb storage stack via `tempo_storage.NewStore`,
      including the S3/GCS/Azure backend reader/writer/compactor —
      `initValueIndexCompactor` depends on `Store` but never actually uses `t.store`
      (confirmed by grep: no `t.store` reference in that function). Depending on
      `Store` would silently construct object-storage clients job-planner never
      needs, directly contradicting this plan's own "materially smaller dependency
      footprint... no S3/GCS/Azure client at all" design goal. **job-planner's
      module deps should be `{Server}` only** — `Server` for pprof/metrics
      endpoints, nothing else. Flag this to the team lead/user as a deliberate,
      reasoned deviation from the brief, not an oversight.

**File: `cmd/tempo/app/modules.go` (modify)**

- [ ] Add constant (~line 899-902, alongside `ValueIndexConsumer`/`ValueIndexCompactor`):
      `JobPlanner string = "job-planner"`.
- [ ] Register: `mm.RegisterModule(JobPlanner, t.initJobPlanner)` (~line 777-779,
      alongside the other value-index targets).
- [ ] Add dependency entry (~line 821-822): `JobPlanner: {Server},` — per the
      deviation above, NOT `{Store, Server}`.

### 3.2 — Poll loop

**File: `modules/jobplanner/service.go` (new package)**

- [ ] `Config` — reuse `common.JobPlannerConfig` directly (no separate blockpack-style
      re-export needed; job-planner has zero blockpack dependency, unlike
      value-index-compactor's `toVICCompactorCfg` conversion, because it never calls
      into a blockpack `Config` struct at all — it only issues raw SQL and calls
      `jobstore.InsertViBackfill`/`InsertCubeBackfill`, both already tempo-side).
- [ ] `Service` struct: `pool *pgxpool.Pool`, `jobStore *jobstore.Store`,
      `cfg common.JobPlannerConfig`.
- [ ] `func (s *Service) Run(ctx context.Context) error` — simple ticker loop
      (mirrors `backendscheduler`'s ticker-based blocklist-poll shape more closely
      than `valueindexcompactor.Service.Run`'s errgroup/work-list machinery, which
      is overkill here: job-planner's per-tick work is "one or two cheap SQL
      queries, N small INSERTs," not "coordinate M concurrent expensive merges" —
      no need for the concurrency-limited work-list pattern at all):
      ```go
      func (s *Service) Run(ctx context.Context) error {
          if !s.cfg.Enabled {
              <-ctx.Done()
              return ctx.Err()
          }
          ticker := time.NewTicker(s.cfg.PollInterval)
          defer ticker.Stop()
          for {
              select {
              case <-ctx.Done():
                  return ctx.Err()
              case <-ticker.C:
                  s.pollOnce(ctx)
              }
          }
      }
      ```
      `pollOnce` logs and continues on error (never aborts the loop — mirrors
      `backendscheduler`'s "one bad tick doesn't kill the poller" posture); metrics
      counters for `job_planner_columns_planned_total` /
      `job_planner_cubes_planned_total` / `job_planner_poll_errors_total`
      (prometheus, registered on `prometheus.DefaultRegisterer` exactly like
      `vccCfg.Registerer` in `value_index.go:301`).

- [ ] TDD: `service_test.go` — `TestService_Run_StopsOnContextCancel`,
      `TestService_PollOnce_ErrorDoesNotAbortLoop` (fake pollOnce-equivalent that
      errors once then succeeds, assert Run keeps ticking) — pure unit tests, no
      Postgres needed for these two.

### 3.3 — The actual planning queries

**File: `modules/jobplanner/plan_vi.go` (new)**

- [ ] Query against `viusage_entries` (schema:
      `vendor/github.com/grafana/blockpack/internal/modules/viusage/schema.sql:10-27`):
      ```sql
      SELECT tenant, col_hash, col_type, column_name, watermark_sec
      FROM viusage_entries
      WHERE triggered = TRUE
        AND done = FALSE
        AND backfill_in_progress = FALSE
      ```
      `triggered = TRUE` is exactly "already triggered at least once" (decision c —
      job-planner never looks at untriggered columns). `done = FALSE` excludes
      columns that have already reached full historical coverage (no more work).
      `backfill_in_progress` is checked here as a cheap pre-filter to shrink the
      candidate set, but is NOT the actual correctness guard — the dedup_key
      partial unique index on `backend_jobs` is (a stale/crashed
      `backfill_in_progress=true` row with no real in-flight job must not
      permanently block chaining; `dedup_key` is what actually prevents
      double-insertion, this WHERE clause is just a cheap reduction of candidate
      rows before the INSERT is attempted).
- [ ] For each row: `jobStore.InsertViBackfill(ctx, tenant, jobstore.ViBackfillDetail{
      ColumnHash: col_hash, ColumnName: column_name, ColumnType: col_type,
      WindowSeconds: cfg.ViWindowSeconds})`. Rely entirely on `InsertViBackfill`'s
      existing dedup_key no-op-on-conflict behavior (`jobstore.go:73-99`) — no
      separate "is there already a pending job" check needed before calling it (this
      is precisely why `dedup_key` exists; duplicating that check here would be a
      redundant, racy TOCTOU check the INSERT's own conflict handling already makes
      unnecessary).
- [ ] **Do not attempt to determine "no more history left" here.** Per decision (c)
      and this plan's Part 2, job-planner's only job is "chain if not done" — the VI
      engine's own `Done` flag (persisted by `runViBackfillCore`'s progressFn call
      into `Registry.UpdateWatermark`, `vi_backfill.go:216-229`) is the sole source
      of truth for "no more history exists"; job-planner just stops seeing a row
      once `done=TRUE`, no separate retention-boundary computation needed on the
      planner side (unlike cube backfill, which needs its own retention clamp per
      2.5, because the VI engine has no analogous internal retention awareness at
      all — it stops naturally when `ListBlocksInRange` returns no more blocks).

**File: `modules/jobplanner/plan_cube.go` (new)**

- [ ] Query against `cube_entries` (schema:
      `vendor/github.com/grafana/blockpack/internal/modules/cube/schema.sql:10-19`):
      ```sql
      SELECT cube_id, tenant, watermarks
      FROM cube_entries
      WHERE watermarks -> '1' IS NOT NULL  -- has an L0 watermark (RollupL0 == 1, cube_ingest.go:420)
      ```
      (cube has no `triggered`/`done` boolean columns — "has an L0 watermark at all"
      is the existing `hasL0` heuristic already used at `cubequerypath.go:174`,
      reused here for consistency rather than inventing a second heuristic).
- [ ] Decode `watermarks` JSONB into `map[uint32]blockpack.ResolutionWatermark`,
      read `wm := watermarks[blockpack.CubeRollupL0]`. If `wm.MinMinute == 0`,
      **skip** — matches Correction 1's cube "zero means still-first-pass, or
      already exhausted down to minute 0" ambiguity; distinguishing these two cases
      exactly is out of scope for a first cut (a cube whose real history has
      genuinely reached minute 0 is a years-old, near-`retention`-boundary case, and
      re-enqueuing a harmless no-op job for it is an acceptable, cheap
      false-negative — NOT a correctness bug, just a very rare wasted poll cycle.
      Flag this as an accepted limitation, not fixed in this PR).
- [ ] For each remaining row: `jobStore.InsertCubeBackfill(ctx, tenant,
      jobstore.CubeBackfillDetail{CubeID: cube_id, WindowMinutes:
      cfg.CubeWindowMinutes})`.
- [ ] TDD (both files): unit tests against a fake `jobStore`-shaped interface (small,
      2-method: `InsertViBackfill`/`InsertCubeBackfill`) proving the query-result-row
      → `Insert*` call mapping is correct, plus the integration tests in Part 4.

---

## PART 4 — Test plan

### 4.1 — Unit tests (no Postgres)
- `blockpack/valueindex_backfill_test.go`: `AnchorSec` override (2.1).
- `tempodb/encoding/vblockpack/vi_backfill_test.go`: anchor threading (2.2).
- `tempodb/encoding/vblockpack/cube_backfill_watermark_test.go`: `currentMinuteAnchor` (2.4).
- `modules/jobplanner/service_test.go`: loop lifecycle (3.2).
- `modules/jobplanner/plan_vi_test.go` / `plan_cube_test.go`: query-row → Insert
  mapping against fakes (3.3).

### 4.2 — Integration/e2e (real Postgres testcontainer — reuse this project's
established helper, confirmed present at `tempodb/encoding/vblockpack/pg_testutil_test.go`
and `modules/backendscheduler/filecatalog/pg_testutil_test.go` per #513/#515/#516's
established pattern)

- [ ] **New file: `modules/jobplanner/jobplanner_e2e_test.go`** —
      `TestJobPlanner_PollOnce_ChainsViBackfillWhenNotDone`: seed a `viusage_entries`
      row with `triggered=true, done=false, watermark_sec=X`, run one `pollOnce`,
      assert a new `backend_jobs` row exists with `dedup_key` matching, `detail`
      containing the configured `WindowSeconds`.
- [ ] `TestJobPlanner_PollOnce_SkipsWhenDone` — same setup with `done=true`, assert
      no new row.
- [ ] `TestJobPlanner_PollOnce_SkipsWhenNonTerminalJobAlreadyExists` — pre-insert a
      `pending` `vi_backfill` row for the same dedup_key, assert `pollOnce`'s INSERT
      attempt is a silent no-op (proves the dedup_key reliance from 3.3, not just
      asserts it).
- [ ] `TestJobPlanner_PollOnce_ChainsCubeBackfillFromExistingWatermark` — seed
      `cube_entries.watermarks` with a real `CubeRollupL0` entry, assert the enqueued
      job's `WindowMinutes` matches config.
- [ ] **The single most important test in this plan** (regression-pins Correction 1
      end-to-end): `TestJobPlanner_ChainedCubeJobsMakeGenuineBackwardProgress` — seed
      a cube entry, run backend-worker's real `processCubeBackfillJobPostgres`
      against synthetic VI data spanning several days, chain 2-3 job-planner-enqueued
      jobs end to end (poll → insert → claim → execute → poll again), and assert
      `Watermarks[CubeRollupL0].MinMinute` after the SECOND chained job is strictly
      LOWER (older) than after the first — i.e. actually prove backward progress, not
      just "a job ran." Without Correction 1's fix, this test would fail by showing
      the SAME `MinMinute` after both jobs (each one reprocessing the same recent
      slice). This is exactly the kind of test this project's own "flaky test rigor"
      and "mutation-test regression guard" conventions call for — write it, then
      temporarily revert 2.4's `currentMinute` wiring to confirm this test actually
      fails without the fix, then re-apply and confirm it passes.
- [ ] Mirror the above for VI: `TestJobPlanner_ChainedViJobsMakeGenuineBackwardProgress`
      against a fake `BlockFetcher` recording every `(minSec,maxSec)` window it was
      asked to list across 2-3 chained jobs, asserting each window's `maxSec` is
      strictly older than the previous window's `minSec`.

### 4.3 — Existing lease-expiry double-claim bug (flagged, NOT fixed in this PR)

Confirmed via direct read (`jobstore.go:120-137`): no lease renewal/heartbeat exists
anywhere in `jobstore.Store` — a claimed job's lease is set once
(`lease_expires_at = now() + interval '30 minutes'`) and never extended. Shrinking
job windows (this PR's whole point) makes this bug **structurally less likely to
matter in practice** (a 6h-window VI job or 24h-window cube job that used to run for
hours now typically finishes in minutes, well under the 30-minute lease), but does
**not fix the underlying gap** — a sufficiently slow/throttled/large-tenant job can
still exceed 30 minutes and trigger the double-claim race. **Recommendation: file a
separate follow-up ticket for lease renewal/heartbeat (out of scope for #518) rather
than silently relying on "windows are usually short now" as a substitute fix** — flag
this explicitly to the team lead/user rather than letting the granularity change be
mistaken for a fix.

### 4.4 — Quality gates
- [ ] `go build ./...` (tempo)
- [ ] `go test ./...` (tempo, includes new packages)
- [ ] `go test -race ./modules/jobplanner/... ./tempodb/encoding/vblockpack/...`
- [ ] blockpack side (vendor patch in 2.1, upstreamed to the real blockpack repo per
      this project's revendor discipline): `make precommit` in blockpack per its own
      CLAUDE.md (gofumpt, golangci-lint, nilaway, betteralign, tests, deadcode,
      staticcheck) — **do not skip this even though the vendor copy is edited
      directly during development**; the real fix must land in blockpack's own repo
      and pass its own gates before revendoring into tempo.
- [ ] This project's standard mutation-testing gate: reintroduce Correction 1's exact
      bug (revert 2.4's `currentMinuteAnchor` wiring back to a literal `0`), confirm
      `TestJobPlanner_ChainedCubeJobsMakeGenuineBackwardProgress` fails, then
      re-apply the fix and confirm it passes again (per this project's standing
      "mutation-test regression guard before approving" convention).

---

## PART 5 — backend-worker (no-op confirmation, per Correction 3)

**No production code changes beyond 2.5's `WindowMinutes` wiring above.** Add one
confirming comment to `reportPostgresJobOutcome`
(`modules/backendworker/backendworker.go:381`) noting that job-planner (not
backend-worker) owns all chain-enqueue responsibility, and that this function's
`Complete`/`Fail`-only shape is intentional and unchanged by #518 — purely
documentation, to prevent a future reader from assuming enqueue logic was ever
removed from here (it never existed here).

---

## PART 6 — k8s / deploy

**No existing `.k8s/configs/value-index-compactor.yaml`-equivalent manifest file is
present in this worktree** (confirmed: `.k8s/` does not exist as a tracked directory
here; `deploy-blockpack.sh`'s references to `.k8s/configs/*` are to files that must
live outside this particular checkout, e.g. locally on the deploying machine). This
plan therefore follows `deploy-blockpack.sh`'s OWN precedent for a component with no
pre-existing manifest: the inline-heredoc `kubectl apply` pattern already used for
`postgres-viusage` (`deploy-blockpack.sh:59-101`), rather than assuming a manifest
file that may not exist.

- [ ] Add a new section to `deploy-blockpack.sh`, modeled on the `postgres-viusage`
      heredoc block: `kind: Deployment` (NOT StatefulSet — no sharding, per the
      brainstorm's own reasoning, unchanged), `replicas: 2` (HA per decision, not
      partition-based scale-out), container `args: [-target=job-planner,
      -config.file=/etc/tempo/tempo.yaml]`, `profiles.grafana.com/*` pprof scrape
      annotations mirroring value-index-compactor's, `prom-metrics` port. Idempotent
      `kubectl apply`, matching the existing postgres-viusage block's own idempotency
      note.
- [ ] Add a `kubectl set image deployment/job-planner ...` + `kubectl rollout
      restart deployment/job-planner` roll step, and a `kubectl rollout status
      deployment/job-planner --timeout=120s` readiness wait (matching querier's
      Deployment-shaped wait, not a StatefulSet pod-0-only wait — no ordinal pods to
      special-case, and per this project's standing "verify all replicas after
      deploy" rule, `rollout status` on a Deployment already waits for every
      replica, unlike the StatefulSet pattern's pod-0-only shortcut).
- [ ] **Flag to team lead/user before applying to a live cluster**: this is the
      first time this project creates a brand-new Deployment via
      `deploy-blockpack.sh`'s heredoc pattern rather than rolling an
      already-existing resource — confirm the namespace/image/replica count before
      the first real `kubectl apply` against `tempo-dev-test-03`.

---

## Sign-off flags (read before executing)

1. **blockpack public API change** (Part 2.1): adding `BackfillConfig.AnchorSec` is
   additive/non-breaking (zero value preserves all existing behavior), but it is
   still new public surface on a package whose CLAUDE.md says "Do not add new public
   API surface without explicit user permission." Flag explicitly before
   implementing, even though the change is small and safe.
2. **First-ever live `kubectl apply` of a brand-new Deployment** via
   `deploy-blockpack.sh` (Part 6) — confirm before running against
   `tempo-dev-test-03`.
3. **job-planner's module-dependency deviation from the brief** (`{Server}` instead
   of `{Store, Server}`, Part 3.1) — reasoned and documented above, but flagging
   since it diverges from the literal brief.
4. **Everything else (Parts 1-5, all test-writing, all vendor-local blockpack edits,
   all tempo-side wiring) is routine, TDD-covered, additive-only work** — no other
   step needs sign-off beyond normal code review; this is a net-new component with
   no removal of existing behavior anywhere except the two dead-field corrections
   (which make previously-ignored fields load-bearing, not remove anything).

---

## Notes for the coder phase

- Confirm `cube.RegistryEntry.Watermarks`'s map key type/value shape
  (`map[uint32]ResolutionWatermark`, `definition.go:61-83`) against the actual code
  at implementation time — this plan read the struct directly, but re-verify field
  names (`MinMinute`/`MaxMinute`) haven't drifted before wiring 2.4.
- `viusage_entries.watermark_sec`'s exact semantics ("[WatermarkSec, now] is
  covered", `entry.go:48-58,94-97`) are asymmetric with cube's `[MinMinute,MaxMinute]`
  range — VI only tracks a single low-water mark (assumes coverage up to "now" is
  implicit), cube tracks an explicit closed range. Do not conflate the two anchor
  mechanisms' semantics when implementing 2.2 vs 2.4 side by side.
- Every new exported Go symbol introduced by this plan (blockpack:
  `BackfillConfig.AnchorSec`; tempo: `RunViBackfill`/`runViBackfillCore`'s new
  `windowSeconds` parameter, `RunCubeBackfill`'s new per-job-window parameter,
  `JobPlannerConfig`, the `job-planner` module target itself) should be re-confirmed
  with the user/team lead before merging, per this session's standing "explicit
  sign-off before new/changed public API" discipline — the Sign-off flags section
  above is the consolidated list to walk through for that conversation.

## Suggested `lth store` content (team lead to run — planner had no Bash access)

```
~/bin/lth store --layer 4 --attr 'project=tempo' --attr 'tags=planning,architecture,job-planning' \
  'job-planner (#518): both vi_backfill and cube_backfill backfill engines anchor their window to wall-clock now on every call, with zero resume-from-watermark mechanism today -- shrinking WindowSeconds/WindowMinutes alone (as the brainstorm assumed was near-free) would make chained jobs reprocess the same recent slice forever, never progressing into older history. Fix: cube already has an unused currentMinute anchor parameter (Backfiller.Run) -- wire it from Watermarks[CubeRollupL0].MinMinute. VI has no such parameter -- added new BackfillConfig.AnchorSec, threaded from entry.Backfill.WatermarkSec. Also found CubeBackfillDetail.WindowMinutes was already being written by cubequerypath.go job-creation call sites but never read by backendworker.go execution -- dead field, now wired up. backend-worker has NO existing chain-enqueue-on-completion logic to remove (brainstorm assumed there was); job-planner is pure net-new. job-planner module deps should be {Server} only, NOT {Store, Server} like value-index-compactor -- Store transitively builds the full S3/GCS/Azure trace storage stack job-planner never needs.'
```
