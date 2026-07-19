# Brainstorm — grafana/blockpack #518: Centralize backfill/compaction job planning; shrink backfill granularity

## 2026-07-18 - Task Received

Issue: grafana/blockpack#518. User direction (verbatim): "I want to remove any backfill or
compaction from components and make a new component that will look at the postgres list of
files and backfills, then create lots of smaller jobs. For instance backfill should handle 1m
jobs and let compaction process naturally join them."

Related: #517 (migrate compaction/retention/redaction dispatch off the ring, onto Postgres —
filed same session, narrower/different axis).

This is a cross-repo (tempo + blockpack), architecture-level, genuinely open-ended request.
Deliverable is a recommended minimally-risky FIRST STEP, not a full end-state design.

## 2026-07-18 - Research Findings

### Current state of job creation/scheduling (verified against code, not just the prompt's claims)

**`backend_jobs` Postgres table** — `tempodb/encoding/vblockpack/migrate/backend_jobs.sql`
(tempo worktree). Columns: `id, job_type, tenant, status, detail JSONB, dedup_key, created_at,
claimed_at, claimed_by, lease_expires_at, started_at, finished_at, retries, last_error,
next_retry_at`. Key constraint: partial unique index on `dedup_key` WHERE
`status IN ('pending','claimed','running')` — enforces at most one non-terminal job per
dedup_key. Claim SQL (`jobstore.go:120-137`) is `UPDATE ... FOR UPDATE SKIP LOCKED`,
lease = 30 minutes fixed at claim time.

**Confirmed correctness gap, independent of #518: no lease renewal/heartbeat exists.**
Grepped `jobstore.go` and `backendworker.go` for lease-extension logic — none found. The lease
is set once (`lease_expires_at = now() + interval '30 minutes'`) and never extended while a job
is actively being worked. Any job that legitimately runs longer than 30 minutes (which today's
whole-history `vi_backfill` / whole-cube `cube_backfill` jobs can easily do — jobs were
"observed running/claimed for hours") becomes reclaimable by a second worker while the first
worker is still actively processing it — a real double-processing race that exists in
production *today*, independent of this ticket, and shrinking job granularity fixes it as a
side effect.

**`vi_backfill`**: one job per `(tenant, col_hash, col_type)` (dedup_key has no time component).
Created reactively in `vi_usage_hook.go:199-210`'s `onShouldBackfill` callback — fired by real
query traffic hitting a column that needs backfill, not by a proactive scan. Window is
`math.MaxUint64` (`vi_backfill.go:213`) — genuinely unbounded; no start/end time parameter
exists in `BackfillConfig` today (`vendor/.../valueindex_backfill.go:68`).

Execution (`BackfillEngine.Run`, `vendor/.../valueindex_backfill.go:137-177`): lists all matching
blocks once, then processes them **serially, block-by-block** (line 218), calling `progressFn`
after each block. Tempo's wrapper persists the watermark on **every** `progressFn` callback
(`vi_backfill.go:216`), not just at the end — so a killed/retried job does resume from its last
completed block rather than restarting from scratch. One VI L0 output file is written per block
processed. **Conclusion: VI already advances incrementally and writes many small files per run —
the only thing genuinely missing is a bounded time-window parameter.** Adding one is a moderate,
contained change (new field on `BackfillConfig`/`ViBackfillDetail`, threaded into the block-listing
call), not a rewrite of the execution engine.

**`cube_backfill`**: one job per `(tenant, cube_id)`. Created in `cubequerypath.go` on cube
creation (line 119) or on retry when a cube exists but has no L0 data yet (line 179). As of a
very recent commit (`d1cd5c6b1`, 2026-07-17) the window is already bounded by tenant retention
(`retentionMinutes`, `backendworker.go:486`) rather than truly unbounded — narrower than the
original brainstorm prompt assumed; this was verified against current code, not just the prompt.

Execution (`internal/modules/cube/backfill.go:91-112`) is **already minute-granular internally**:
it iterates minute-by-minute, calls `progressFn` and persists the watermark after each minute
(`cube_backfill_runner.go:225-227`), and writes one output file per minute via `flushMinute`
(`backfill.go:272`) — sparse minutes write nothing. It even builds several minutes concurrently
(up to `cfg.Workers`) while flushing serially in strict newest→oldest order for watermark
contiguity. **Conclusion: cube_backfill needs zero execution-engine changes to support
fine-grained jobs — it already accepts a `WindowMinutes` parameter and already produces
minute-grained output. Only the job-creation/orchestration layer would need to change.** This
makes a "narrow-window cube_backfill job" essentially free to build once the VI pattern below
exists, and arguably even lower-risk than the VI change — worth bundling into the same first PR,
even though the issue's own example and the brainstorm prompt named VI only.

**Trace/span compaction** (`modules/backendscheduler/provider/compaction.go`): confirmed still
100% Postgres-independent. Uses ring ownership (`Owns()`), a `tenantselector.PriorityQueue`
(line 67), and a live in-memory `BlockMeta` scan (`blockselector.CompactionBlockSelector`,
line 69) to pick input blocks, dispatched via the legacy gRPC `Next()`/`UpdateJob()` RPCs
(`backendworker.go:296`, `:529`). No architectural change here from what the prompt described.

**VI/VCNT/cube *compaction* (execution side)** — investigated whether many small backfill
outputs would strain the existing compactors:
- VI (`valueindexcompactor/service.go:357`): merge triggers on `len(files) >=
  CompactThresholdFiles` (default 2), grouped by structural level, not by time or column
  identity. `NOTE-VI-101` in that package's NOTES.md explicitly states backfilled L0 files from
  usage-triggered columns require **zero special-casing** — "discovered by the same generic walk
  with no code change." Disk-streaming merge keeps peak memory roughly flat as file count grows
  (`BENCH-VI-1`: ~1.72x memory growth from K=5→K=50 input files).
- VCNT (`valuecountscompactor/service.go:271-276`, `cluster.go:15-57`): merge triggers on a
  file-count threshold *within* a time-clustered bucket (default `MaxTimeSpanPerMerge` = 24h).
  Clustering is pre-decode (filename-based), specifically designed (`NOTE-VC-018`) to handle many
  small files cheaply.
- Cube (`internal/modules/cube/compactor.go:100-142`): `L0MergeThreshold=60` files/hour,
  `L1MergeThreshold=24` files/day — these thresholds are *already* tuned assuming one L0 file per
  minute (live ingestion already produces exactly this cadence, `NOTE-CUBE-007`). A 1-minute-
  granularity cube backfill output is not just "handled," it's the exact shape the thresholds
  were designed around.

**All three compactors are structurally ready for a large increase in small-file count with no
code changes.** This is strong, concrete evidence — not speculation — for resolving open
question (a) below.

### Postgres state available to a planner

- `file_catalog` (tenant, block_id, block_ref, start_sec, end_sec, size_bytes, deleted_at) —
  cursor-based (`row_id`), per-tenant block inventory. Gives time-range/existence info but *not*
  per-column presence within a block (that's derived from `column_manifest_blobs` /
  block-internal manifests, read inside `BackfillEngine` itself today).
- `viusage_entries` — **one row per `(tenant, col_hash, col_type)`**, single scalar
  `watermark_sec` + `last_catalog_row_id` cursor, plus `done` and `backfill_in_progress` flags.
  This is the exact table the new standalone planner component (see Recommendation, below) polls
  to find chainable columns. This single-scalar-per-column watermark is also the key constraint
  on job parallelism (see Risk section below).
- `cube_entries` (blockpack-owned schema, migrated out of tempo's own tables per #504/#506) —
  per-cube JSONB `watermarks` map, no single "done" flag; completion is inferred. The equivalent
  table the planner polls if `cube_backfill` chaining is bundled into the same component.
- `column_manifest_blobs` — per-(tenant, col_hash, col_type) manifest blob, tracks which blocks
  have been processed for a column already (used internally by backfill to skip re-processing).

None of this requires new tables to support *bounded-window* jobs for a single column processed
serially. It would require schema/model changes to support genuinely *parallel*, out-of-order
windows for the same column (see below).

## 2026-07-18 - Open Questions Resolution

### Question (a): planning-only, or does execution move too?

**Resolved with high confidence: planning-only.** The evidence above (NOTE-VI-101, VCNT's
time-clustering design intent, cube's thresholds already tuned to minute-granularity) shows the
three compaction executors are *already* built to consume many small, frequently-arriving files
without modification — this is exactly the mechanism the user is pointing at with "let compaction
process naturally join them." The new component's job is **only** to decide *when and how much*
backfill/compaction work exists (planning) and enqueue it; the merge algorithms stay exactly
where they are (`valueindexcompactor`, `valuecountscompactor`, `cube/compactor.go`). No execution
code needs to move.

### Question (b): does trace/span compaction get folded in?

**Decided by the user (2026-07-18): NO — out of scope for #518.** #518 stays scoped to
`vi_backfill`/`cube_backfill` planning/granularity; #517 stays separate, scoped to the
ring-removal migration for trace compaction/retention/redaction dispatch. This matches the
brainstorm's recommendation: trace compaction is the highest-volume, most correctness-sensitive
path in the system, is architecturally furthest from Postgres today (zero Postgres involvement,
ring-owned, gRPC-dispatched), and is already the explicit subject of a separate ticket.

### Question (c) — proactive vs. reactive triggering — **Decided by the user (2026-07-18):
stay REACTIVE.** Keep today's lazy, cost-controlled, query-triggered model (`onShouldBackfill` /
`OnCreateAttempt`) exactly as today for deciding *whether a column/cube gets backfilled at all*.
The new component (see below) does not proactively scan `file_catalog` for never-queried
columns — it only *plans/chains* the narrow-window jobs for columns that have already been
triggered at least once. Eager, demand-independent backfill of all data remains a separate,
later product decision, not adopted here.

### Question (d) — should the new component be built now as a real standalone deployable —
**Decided by the user (2026-07-18): YES.** This reverses the brainstorm's original
recommendation (which had suggested keeping the chaining logic inside `backendworker.go`'s
job-completion handling for the first step, deferring an actual new service to a later phase).
The user wants a real new component to exist from day one, even though its first real
responsibility is narrowly scoped to `vi_backfill`/`cube_backfill` chaining. See "New component
shape" under Recommendation, below, for the concrete design given this decision.

## 2026-07-18 - Approaches Considered

(Recorded as originally brainstormed, before the user's decision on question (d) above — kept
for the historical record of the tradeoff that was weighed. The **chosen design is Approach 2's
granularity/execution/trigger conclusions, combined with Approach 1's "real standalone
component" shape** — see Recommendation.)

### Approach 1: Build a new standalone proactive planner service now (full "centralize" vision)
**Description:** New deployable/loop that scans `file_catalog` + `viusage_entries` + `cube_entries`
independently of query traffic, decides all outstanding backfill work, and enqueues fine-grained
jobs for both vi_backfill and cube_backfill.
**Pros:** Directly matches the ticket title's "new component" framing; one place owns all backfill
planning going forward.
**Cons:** Bundles the granularity change with the proactive-vs-reactive product decision (question
c) and with genuinely new schema needs (per-window completion tracking, not just a scalar
watermark) in one PR. Highest blast radius of the options considered.
**Resolution:** the user took the "real standalone component" half of this approach (question d)
but explicitly rejected the "proactive scanning" half (question c stays reactive) — a hybrid of
Approach 1's deployment shape with Approach 2's narrow scope.

### Approach 2: Granularity-only, chained small jobs, keep reactive trigger, logic inside backend-worker (ORIGINALLY RECOMMENDED, PARTIALLY SUPERSEDED)
**Description:** Keep `vi_backfill`'s existing reactive trigger site (`onShouldBackfill`) and its
existing per-column dedup_key/single-active-job invariant exactly as today. Add a bounded window
parameter to `BackfillConfig`/`ViBackfillDetail` (currently missing). Originally proposed: when a
bounded-window job completes and the column's watermark hasn't caught up to "now," have
*backend-worker's own completion handler* enqueue the next bounded-window job for that column.
**Superseded by the user's decision (d):** the chaining/enqueue decision now lives in the new
standalone component's own poll loop instead of backend-worker's completion handler — see
Recommendation. The granularity, execution-untouched, and reactive-trigger conclusions from this
approach are all still correct and adopted as-is.

### Approach 3: Parallel small jobs across windows of the same column (rejected for first step)
**Description:** Let multiple bounded-window jobs for the same column run concurrently across
workers instead of strictly serially.
**Pros:** Would realize the full parallelism benefit of fine granularity, not just the
lease-safety benefit.
**Cons:** `viusage_entries.watermark_sec` is a single scalar per column — out-of-order completion
across concurrent windows would require replacing it with a per-window completion model (e.g., a
range/bitmap table), a genuine schema change with real correctness stakes. Still rejected for the
first step regardless of the new-component decision — the new component's poll loop still
enqueues at most one outstanding chained job per column at a time, matching today's
single-active-job-per-dedup_key invariant.
**Fits existing patterns:** No — would need a new completion-tracking primitive.

## 2026-07-18 - Recommendation

### Chosen design: real standalone "job-planner" component, scoped to vi_backfill/cube_backfill granularity-chaining only

All three of the user's decisions are now incorporated: reactive triggering stays (c), trace
compaction stays out of scope (b), and the chaining/enqueue logic is built as a genuine new
deployable from day one (d), not deferred into backend-worker.

### New component shape (per the user's decision on question (d))

**Concrete mirror pattern used below**: this codebase already has exactly one precedent for "a
new standalone Postgres/S3-driven backend component," `value-index-compactor`
(`blockpack/cmd/value-index-compactor/main.go`, `blockpack/valueindexcompactor/` thin re-export,
`tempo/cmd/tempo/app/value_index.go`'s `initValueIndexCompactor`, `tempo/.k8s/configs/
value-index-compactor.yaml`'s 20-replica `StatefulSet`). The new "job-planner" component should
follow the **tempo-module-target** half of this pattern (not blockpack's standalone-binary half —
the planner needs no blockpack import at all, since it never touches object storage or the
block-format execution engine, only Postgres):

1. **Deployable shape**: a new tempo module target, e.g. `-target=job-planner`.
   - New file `cmd/tempo/app/job_planner.go` (mirrors `value_index.go`'s
     `initValueIndexCompactor`), registering a `services.Service` that wraps a poll loop.
   - New module constant in `cmd/tempo/app/modules.go` (mirrors `ValueIndexCompactor string =
     "value-index-compactor"` at line 901 and its `mm.RegisterModule(...)` at line 778, deps
     `{Store, Server}` at line 822) — the planner needs `Store` (for Postgres access) and
     `Server` (for pprof/metrics), but crucially **does not need an S3/GCS/Azure client at all** —
     a materially smaller dependency footprint than `value-index-compactor`, which needs both
     Postgres and S3.
   - New k8s manifest mirroring `.k8s/configs/value-index-compactor.yaml`'s shape (container
     image, `args: [-target=job-planner, -config.file=...]`, `profiles.grafana.com/*` pprof
     scrape annotations, `prom-metrics` port) but as a small **`Deployment`**, not a
     `StatefulSet` — no ordinal-based sharding is needed. `value-index-compactor`'s 20-replica
     `StatefulSet` shards by `POD_NAME` ordinal because it does heavy, partitionable S3 merge
     I/O; the planner's work (deciding "is there a chainable column, if so INSERT one row") is
     cheap, low-volume, and already made safe for concurrent/redundant execution by the existing
     `dedup_key` partial-unique-index (any number of planner replicas polling and attempting to
     enqueue the same next-window job will have all but one INSERT no-op/conflict harmlessly).
     Recommend 1-2 replicas for HA, not partition-based scale-out.

2. **Its own poll loop, replacing the "backend-worker chain-enqueues on completion" idea**:
   - On a configurable interval (plan phase to size, likely 30s-2min — cheap enough that even a
     fairly tight interval is fine given the query is a simple indexed scan, not a heavy job),
     query `viusage_entries` for rows where `done = false` (or `watermark_sec` has not reached
     "now"/the tenant's retention boundary) **and** there is no current non-terminal
     `backend_jobs` row for that column's dedup_key. This is exactly "columns that have been
     triggered before (question c: reactive triggering already inserted at least one job for
     them at some point) but are currently idle and have more history left to backfill."
   - For each such column, compute the next bounded window
     `[watermark_sec, min(watermark_sec + windowSize, now/retention))` and call
     `jobStore.InsertViBackfill` with the windowed `ViBackfillDetail` (the same bounded-window
     field added for the granularity change, independent of which component calls it).
   - Do the identical poll-and-enqueue over `cube_entries` for `cube_backfill`, if bundled into
     the same first PR (see open question 1, below — the recommendation to bundle stands).
   - This poll loop is the **entire** scope of the new component in its first-step form. It does
     **not** execute any backfill work itself, does **not** touch object storage, and does
     **not** decide whether a never-before-queried column gets its very *first* job — that
     initial reactive trigger stays exactly where it is today, embedded in `onShouldBackfill`/
     `OnCreateAttempt` in the query path (per decision (c)). The new component only owns
     *continuing* an already-started backfill in small steps.

3. **Interaction with backend-worker — unchanged on the execution side, simplified on the
   completion side**: backend-worker still claims (`SELECT ... FOR UPDATE SKIP LOCKED`) and
   executes (`RunViBackfill`/`RunCubeBackfill`) jobs exactly as today; it has no awareness that
   jobs are narrower or that job-planner (rather than the query-path trigger) created them. The
   one concrete change *to backend-worker itself*: its job-completion handler (`Complete`/`Fail`
   in `jobstore.go`, called from `reportPostgresJobOutcome`) goes back to being **purely**
   "mark done/fail, persist the final watermark" — no enqueue side effect at all. That
   responsibility now belongs entirely to job-planner's poll loop. This is actually *simpler*
   than the original Approach 2 sketch (which would have added enqueue logic to backend-worker's
   completion path) — the user's decision (d) doesn't just add a new service, it also removes a
   responsibility from an existing one, keeping backend-worker a pure executor.

4. **Consequence of polling vs. instant in-process chaining**: there is a small latency gap (up
   to one poll interval) between one window finishing and the next starting, versus the original
   sketch's instant same-process re-enqueue. This is an explicit, acceptable tradeoff of
   "genuinely centralized planning" (the user's stated goal) vs. minimum latency — not a
   correctness regression: `dedup_key` still prevents any double-creation, and backend-worker's
   existing lease-expiry self-heal sweep is untouched.

**Implementation strategy (high-level, plan phase to detail):**
1. Add a bounded time window to `ViBackfillDetail`/`BackfillConfig` (currently only supports
   "everything since epoch"). Thread it into `BackfillEngine.Run`'s block-listing call. (Same as
   originally scoped — independent of which component calls it.)
2. Build the new `job-planner` tempo module target: `cmd/tempo/app/job_planner.go`,
   `modules.go` registration, k8s `Deployment` manifest, poll loop against `viusage_entries`
   (and `cube_entries` if bundled) as described above.
3. Remove any enqueue-on-completion responsibility from backend-worker's Postgres job-completion
   path — it should only ever mark terminal state, never create new rows.
4. Pick a concrete window size and poll interval for the plan phase to validate — the issue's own
   example says 1 minute for the window; confirm that's not needlessly small given per-job
   overhead vs. lease-safety needs (this is a tuning question, not an architecture question).
5. Leave `CompactionProvider` (trace compaction) and the VI/VCNT/cube compactors entirely
   unchanged.

**Key decisions:**
- Execution stays put everywhere (resolves question a).
- Trace compaction stays out of scope (resolves question b — user-decided).
- Reactive (usage-triggered) trigger model stays as-is for the *first* job of a column/cube
  (resolves question c — user-decided); the new component only chains *subsequent* windows.
- The new component is a real standalone tempo module target/deployable from day one (resolves
  question d — user-decided), mirroring `value-index-compactor`'s tempo-module-target pattern but
  as a lightweight `Deployment` (1-2 replicas, no sharding) rather than a sharded `StatefulSet`,
  since its work is cheap and already idempotency-protected by `dedup_key`.

**Risks identified:**
- Risk: picking too small a window (literally 1 minute) multiplies `backend_jobs` row
  churn/volume for high-traffic columns without a corresponding benefit once the lease-safety
  problem is already fixed by a coarser window. Mitigation: treat window size as a config knob the
  plan phase tunes with real numbers, not a fixed literal 1 minute.
- Risk: poll-interval latency (item 4 above) between chained windows — acceptable per the user's
  explicit choice of centralization over minimum latency, but should be sized and documented.
- Risk: running job-planner with >1 replica means multiple replicas will race to plan the same
  column on the same poll tick. Mitigation: this is already safe (not just "probably safe") —
  the `dedup_key` partial unique index makes redundant `InsertViBackfill` calls a harmless
  no-op/conflict, exactly the same protection `onShouldBackfill`'s existing multi-replica callers
  already rely on today.

**Open questions still needing team lead/user input before planning proceeds:**
1. Bundle `cube_backfill`'s equivalent chaining into the same first PR/component (near-free per
   the research above, since its execution engine is already minute-granular and already
   retention-bounded), or defer it to keep the first PR narrower, per the issue's own
   "vi_backfill only" suggestion? (Not yet explicitly decided by the user — recommend bundling,
   but flagging since it wasn't one of the 3 questions explicitly resolved.)
2. Relationship to #517: recommend keeping separate (different axis — ring removal vs. planning
   centralization) — consistent with decision (b) above, but worth an explicit confirmation.
3. Exact window size and poll interval values — tuning parameters for the plan phase, not
   architecture decisions.
4. Naming for the new component (`job-planner` used as a working name in this document, matching
   the worktree/branch name) — plan phase or user to confirm the real name before it appears in
   module constants, k8s manifest names, and Dockerfile COPY lines.

## 2026-07-18 - BRAINSTORM COMPLETE
**Status:** Complete — all 3 originally-open questions have been decided by the user (reactive
triggering kept, trace compaction excluded, new component built as a real standalone deployable
from day one). Recommendation updated accordingly.
**Recommendation:** A new tempo module target (`-target=job-planner`, working name), deployed as
a small 1-2 replica `Deployment` (mirroring `value-index-compactor`'s tempo-module-target
integration pattern but without its S3 dependency or StatefulSet sharding), whose only
responsibility is a poll loop against `viusage_entries`/`cube_entries` that chain-enqueues the
next bounded-window `vi_backfill`/`cube_backfill` job for columns/cubes that have already been
triggered at least once and have more history left to backfill. The very first trigger for a
never-before-queried column/cube stays exactly where it is today (reactive, query-path-embedded).
Backend-worker's execution and claim logic is completely unchanged; its completion handler is
simplified (loses its provisional enqueue-on-completion responsibility, which now belongs to the
new component). No execution-engine changes to any of VI/VCNT/cube compaction. Trace/span
compaction and #517 stay out of scope and separate.
**Next Phase:** PLAN — handing off to the planner with the 4 remaining open questions listed
above (bundle cube_backfill, #517 relationship, window/poll-interval tuning, component naming).
