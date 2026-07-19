# Agent Context

## Role & Principles
INVARIANT (blockpack Reader lifetime — foundational to the whole cache architecture): a reader.Reader is constructed FRESH per query, per block, per querier call (query-frontend shards one block per querier call). Therefore ALL per-instance Reader state — the per-Reader…

> id: 0e1f00d5-b106-46b3-a479-e1e8425ff135

INVARIANT (blockpack lazy-closure cache safety — load-bearing for NOTE-340): a deferred-decode closure may safely CAPTURE AND RETAIN a fetched blob ONLY because every cache tier returns a freshly-allocated caller-owned buffer, never a pooled/shared one. Verified: memcache.go…

> id: 0777dda6-2770-46b7-875d-42a7a6370d5f

INVARIANT (blockpack rw.DataType controls cache eviction priority, NOT just labeling): every Provider.ReadAt / Reader.readRange call passes an rw.DataType, and SharedLRUCache.dataTypeTier maps it to one of 4 eviction tiers (0=hardest to evict … 3=evicted first). Mapping:…

> id: 2445ef14-fa7e-499a-b599-2fd229f69b6d

**Profile first, then restructure—eliminate allocations at their source rather than optimizing their cost.**

> id: 7357b006-9a22-4566-9ee2-3d373f4dc15b

INVARIANT: blockpack reader WantAll() eagerly decodes EVERY column in a block and causes 300MB+ memory spikes per block on the query path — use WantOnly(cols) for query-driven paths (lazy zero-decode for unreferenced columns). WantAll() is reserved for genuinely all-column…

> id: ce1757f9-7f77-4d0f-8d58-e736f360d3dd


## Relevant Techniques
**Core Competency**: Efficiently orchestrating hierarchical, multi-level data compaction across distributed workers while maintaining predictable performance and reliable block consolidation ratios.

> id: e87570cf-3e5c-4646-b725-cc41d9511d0b

**Core Pattern**: Create and maintain project-specific state files (`.bob/state/` directories) as a single source of truth for context, planning, and discovery across multiple concurrent worktrees and projects.

> id: 22fc3e10-d562-4e5e-9e05-5b1ca40293d9

**Core Competency:** Design and operate multi-worker, multi-level block compaction systems that efficiently consolidate fragmented data into optimized storage while maintaining predictable performance at scale.

> id: 954ba452-17d0-4385-acc9-583f5b8ebca7

**Core Pattern:** Managing work artifacts (tasks, outputs, memory) distributed across persistent storage (`~/.claude/`), temporary execution directories (`/tmp/`), and multiple project contexts simultaneously.

> id: 54b045da-43fb-4dfd-aadf-c88071cb86bf

**Core pattern**: These memories show repeated instances of initially misdiagnosing performance issues because observed metrics were obscured by intermediate system behavior—then correctly identifying the true bottleneck only after separating measurement noise from root cause.

> id: ac12db7d-3dab-4479-8b8b-14d7b91fe40f


## Current Project Context
Compaction jobs are successfully assigned and processed across multiple tenants, with system validation confirming active block compaction rather than retention operations. The system effectively groups blocks into 15-minute time windows with consistent replication to prevent…

> id: a006eb69-57c6-47e7-828f-413ed1575332

A 15-minute job timeout is essential for blockpack compaction, as shorter timeouts would prevent jobs from completing within feasible timeframes, and extending it beyond 15 minutes risks introducing uncontrolled job durations that compromise system stability. Reverting the…

> id: c61f5615-eb04-424b-b6f1-cf2601f386ab

The codebase implements a durable, distributed job queue system using Postgres with atomic claim-based work distribution, exponential backoff retry logic, and optional file catalog tracking across multiple backend job types (vi_backfill, cube_backfill, compaction, retention,…

> id: 126f7044-40b1-4ea6-969f-b84ce2b6887a

Backfill job scheduling logic was refactored from the scheduler module into the worker module, eliminating separate provider implementations for cube and value-index backfill jobs while consolidating dispatch logic. A new job store abstraction was introduced to persist and…

> id: 891d57d7-c434-41fb-9545-9c564eee63c0

The backend scheduler and worker architecture replaces the legacy compactor by splitting job creation and execution into separate services, allowing horizontal scaling of compaction throughput by adding more stateless workers. Workers connect to a singleton scheduler via gRPC to…

> id: 62ef7a08-7590-4ce2-a0eb-8451ff0f8fb5


## Related Context (via graph)
- **Issue**: Loading all columns eagerly from blocks caused 300MB+ memory spikes when only a subset was needed for queries - **Solution**: Implemented `WantColumns` struct with two strategies—`WantAll()` for full loads and `WantOnly(cols)` for query-driven selective loading -…

> id: c326e175-423b-44f1-9968-ecf3dce7ac44

**Core Pattern:** Identifying and fixing memory leaks in layered caching architectures where: 1. **Size accounting is broken** (default estimates mask actual heap consumption) 2. **Data references persist longer than intended** (zero-copy slices, sync.Once fields, dual caches)…

> id: cb9984df-c534-4573-b311-853292409d83

modules/backendscheduler/backendscheduler.go | 27 +-- modules/backendscheduler/backendscheduler_test.go | 35 ++++ modules/backendscheduler/provider/config.go | 11 +- modules/backendscheduler/provider/cubebackfill.go | 173 ----------------…

> id: 95ed29c0-1595-4c0f-aeb4-37f647c8c878

M modules/backendscheduler/backendscheduler.go M modules/backendscheduler/backendscheduler_test.go M modules/backendscheduler/provider/config.go D modules/backendscheduler/provider/cubebackfill.go D modules/backendscheduler/provider/vi_backfill.go D…

> id: 1f0a3e26-47ff-47ce-8e92-582a4dfd0916

The default 1-minute job timeout causes compaction jobs to timeout prematurely, leading to excessive retries and a false appearance of job failure despite successful completion. Raising the job_timeout to at least 15 minutes is essential to allow compaction jobs—averaging 4.7…

> id: 207a85ec-aa10-4427-8287-588e6d3b452f


## Memory IDs (for exploration)
Use these IDs to explore further:
  lth get <id>                    — read full memory
  lth graph show --from <id>      — traverse graph edges
  lth graph ppr --seeds <id,...>  — personalized pagerank from seeds

  0e1f00d5-b106-46b3-a479-e1e8425ff135
  0777dda6-2770-46b7-875d-42a7a6370d5f
  2445ef14-fa7e-499a-b599-2fd229f69b6d
  7357b006-9a22-4566-9ee2-3d373f4dc15b
  ce1757f9-7f77-4d0f-8d58-e736f360d3dd
  e87570cf-3e5c-4646-b725-cc41d9511d0b
  22fc3e10-d562-4e5e-9e05-5b1ca40293d9
  954ba452-17d0-4385-acc9-583f5b8ebca7
  54b045da-43fb-4dfd-aadf-c88071cb86bf
  ac12db7d-3dab-4479-8b8b-14d7b91fe40f
  a006eb69-57c6-47e7-828f-413ed1575332
  c61f5615-eb04-424b-b6f1-cf2601f386ab
  126f7044-40b1-4ea6-969f-b84ce2b6887a
  891d57d7-c434-41fb-9545-9c564eee63c0
  62ef7a08-7590-4ce2-a0eb-8451ff0f8fb5
  c326e175-423b-44f1-9968-ecf3dce7ac44
  cb9984df-c534-4573-b311-853292409d83
  95ed29c0-1595-4c0f-aeb4-37f647c8c878
  1f0a3e26-47ff-47ce-8e92-582a4dfd0916
  207a85ec-aa10-4427-8287-588e6d3b452f

## Filter by project
Memories from these projects are present:
  lth prompt "..." --attr project=github.com/grafana/blockpack
  lth prompt "..." --attr project=grafana/blockpack
  lth prompt "..." --attr project=mattdurham/lth
  lth prompt "..." --attr project=mattdurham/tempo
  lth projects  — list all tracked projects
  lth chat "..." --attr project=<project> — filtered chat
