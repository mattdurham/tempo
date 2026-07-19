Task: Implement grafana/blockpack issue #518 — centralize backfill/compaction job planning into a new component; shrink backfill job granularity.

Repo: /home/mdurham/source/blockpack_collection/tempo-worktrees/job-planner-518 (branch job-planner-518, off tempo's agentic-tempo).

This is a CROSS-REPO, exploratory, architecture-level brainstorm — bigger in scope than #513/#515/#516. The other repo (blockpack) is at /home/mdurham/source/blockpack_collection/blockpack (branch main) — read it directly, no worktree needed there yet since it's unclear if blockpack-side code even needs to change.

Fetch the full, authoritative issue text: `gh issue view 518 --repo grafana/blockpack`

## User's stated direction (verbatim intent, from conversation)
"I want to remove any backfill or compaction from components and make a new component that will look at the postgres list of files and backfills, then create lots of smaller jobs. For instance backfill should handle 1m jobs and let compaction process naturally join them."

## What's already established this session (do not re-derive, verify against current code instead)

**Job creation/scheduling today (3 independent, inconsistent mechanisms):**
1. `vi_backfill`/`cube_backfill`: event-driven (triggered by real query traffic hitting a novel pattern), queued in Postgres `backend_jobs` table, claimed via SQL by `backend-worker` (tempo/modules/backendworker/backendworker.go). Each job's window is HUGE — cube_backfill's WindowMinutes = math.MaxUint32 or full tenant retention; vi_backfill lists+processes every overlapping block for a (tenant,column) pair in ONE job invocation. Only ~11 total jobs ever existed in the dev cluster because triggering is sparse and each job's scope is enormous.
2. Trace/span compaction: fully self-driven continuous priority-queue loop inside backendscheduler (modules/backendscheduler/provider/compaction.go's CompactionProvider) — NO Postgres involvement, dispatched via legacy gRPC backendScheduler.Next().
3. VI/VCNT/cube compaction: fully self-driven continuous polling loops inside the dedicated value-index-compactor StatefulSet (blockpack/internal/modules/valueindexcompactor, valuecountscompactor, cube packages + tempo/tempodb/encoding/vblockpack/cube_scheduler.go) — each decides its own work independently, no shared planning layer.

**Existing compaction machinery (this is the "let compaction naturally join them" mechanism the user is referring to — confirm it can actually absorb many small backfill outputs without modification):**
- VI: multi-level L0->L1->L2..., disk-streaming merge (bounded memory), dedupes by identity, drops entries from retention-deleted source blocks. blockpack/internal/modules/valueindexcompactor/service.go.
- VCNT: same multi-level idea but in-memory decode+merge with a record-count admission gate, TIME-CLUSTERING (groups candidate files by wall-clock range before merging — issue #494), sums counts by key. blockpack/internal/modules/valuecountscompactor/service.go.
- Cube: three-level rollup ladder (L0 minute -> L1 hourly -> L2 daily), boundary-gated (only fully-elapsed hour/day boundaries), mathematically exact aggregation. blockpack/internal/modules/cube/compactor.go + rollup.go, tempo/tempodb/encoding/vblockpack/cube_scheduler.go.

**Postgres tables available (confirm exact schema, don't assume):** backend_jobs, column_manifest_blobs, cube_entries, file_catalog, viusage_entries, viusage_query_log — in the `postgres-viusage` StatefulSet's default `postgres` database (not a separate "viusage" db, confirmed this session).

**Related, already-filed ticket:** #517 ("Long-term: migrate compaction/retention/redaction dispatch off the ring, onto the Postgres job queue") — tracks removing ring/gossip dependency from compaction dispatch specifically. This ticket's centralized-planner direction would likely satisfy #517 as a side effect (a Postgres-state-driven planner has no reason to need ring/gossip), but is broader — #517 is narrowly about the ring dependency, #518 is about the planning/granularity architecture itself. Your brainstorm should clarify the relationship and recommend whether #517 should be closed/merged into #518's scope, or stay separate.

## Your job

1. Read .bob/state/context.md for pre-loaded lth memory context.
2. Read the actual code for all the mechanisms listed above — verify the claims against current code (things may have drifted), don't just trust this prompt.
3. Read Postgres schema for real: check tempo/tempodb/encoding/vblockpack/jobstore/ and any migration/schema files for file_catalog, column_manifest_blobs, viusage_entries, cube_entries — understand what state is ALREADY tracked there that a new planner component could read from, versus what (if anything) would need to be added.
4. Resolve the two open questions #518's issue body explicitly raises:
   a. Does "remove compaction from components" mean removing the PLANNING/decision logic only (leaving merge execution algorithms in their current executor components), or does execution move too? The user's own example ("let compaction process naturally join them") suggests planning-only, execution-stays — confirm or challenge this reading.
   b. Does trace/span compaction (backendscheduler's CompactionProvider, the highest-volume, most correctness-sensitive path) get folded into this new centralized planner too, or is #518 scoped to VI/VCNT/cube/backfill only? Do NOT assume either way — this materially changes blast radius and risk.
5. Investigate the CONCRETE first step: vi_backfill at 1-minute granularity, exactly as the user's example specifies. What would a "1-minute vi_backfill job" actually look like given the current per-(tenant,column) job model? Does the (tenant,column) scoping stay the same with just a narrower time window per job, or does granularity change some other dimension too? Would VI's existing L0->L1 compaction genuinely absorb many small 1-minute output files without any changes to the compactor itself, or does something (file naming, level assignment, minimum-file-count thresholds like CompactThresholdFiles=3) need adjusting to make this work well? Trace through the actual merge-decision code (compactColumn in valueindexcompactor/service.go) to check whether many-small-files-arriving-frequently is already handled gracefully or would cause new problems (e.g. thrashing, excessive small-file accumulation before threshold is hit, S3 API call volume from many tiny files).
6. Think about correctness risks specific to a centralized planner: race conditions between the planner creating new small jobs and executors processing old ones, idempotency/dedup (the existing backend_jobs table already has a dedup_key + unique index for pending/claimed/running — would 1-minute granularity multiply job-row volume dramatically, and is that fine or a scaling concern), and how a planner would know a given 1-minute window has genuinely never been backfilled yet (query the file_catalog/watermark state correctly) vs. re-doing work already covered.
7. Given the scale, DO NOT try to design the full end-state architecture. Recommend a concrete, minimally-risky FIRST STEP (the plan phase will detail it) — almost certainly "convert vi_backfill's job granularity to fine-grained windows, keep everything else (cube_backfill, trace compaction, VI/VCNT/cube compaction execution) unchanged for now" given the issue's own recommendation to scope tightly.
8. Write findings to .bob/state/brainstorm.md (fully overwrite the stale, unrelated content already there — read it first per the Write tool's requirement).

AFTER writing — store key findings back to lth:
  ~/bin/lth store --layer 4 --attr 'project=tempo' --attr 'tags=brainstorm,architecture,job-planning' '<key decision or insight>'

Report back to the team lead (me) with your recommended first-step scope and any open questions needing my/the user's input before planning proceeds. This one may legitimately need a real back-and-forth with the user on scope given how open-ended the request is — don't force a fake resolution to the two explicit open questions if you genuinely can't determine the right answer from the code alone; flag them clearly instead.