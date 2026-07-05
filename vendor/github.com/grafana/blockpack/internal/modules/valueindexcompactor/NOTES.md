## NOTE-VI-017 — Value index compactor service (issue #399)

Date: 2026-06-25

The compactor is the third and final stage of the value-index pipeline (publisher NOTE-VI-015
# 397, consumer NOTE-VI-016 #398, compactor #399). It periodically merges the many small L0
value index files the consumer writes into fewer, larger L1/L2 files per (tenant, column), and
drops entries whose originating blockpack file has been deleted by retention.

### Single pass = one level per column

`compactColumn` groups a column directory's files by parsed compaction level and compacts only
the **lowest** level that meets `CompactThresholdFiles`. Only one level is merged per pass; a
higher level that has since crossed the threshold (because a prior pass produced more L1 files)
is picked up on a later pass. This keeps each `CompactFiles` call same-level (required by
VI-012 — output level = input level + 1) without trying to cascade L0→L1→L2 in a single run,
which would complicate crash safety.

### Retention via source-existence, not delete messages

There are deliberately no delete events on the queue (#397 publishes create-only). Instead each
entry carries its `SourceRef` (the originating blockpack path), and during compaction every
unique source is probed via `SourceExister.Exists` (an S3 HEAD in production). Entries from a
deleted source are dropped rather than propagated, so dead entries are reclaimed organically as
retention removes the source blocks. `cachingRefChecker` (store.go) caches existence per source
path for the duration of one compaction job so each unique source is probed at most once,
regardless of how many entries reference it — the issue's "batch HEAD by unique source path"
cost optimisation. `exister` may be nil to skip the check entirely (all entries propagated).

### Write-then-delete crash safety; stateless / multi-instance safe

`mergeLevel` writes the merged output file(s) with **fresh** output IDs (`valueindex.NewID()`)
*before* deleting any input. A crash after Put but before Delete leaves both the new output and
the old inputs in place; the next pass recompacts and the duplicate entries are removed by the
merge's dedup. Because outputs use fresh IDs and inputs are only deleted after a successful
write, multiple compactor instances are safe — at worst they produce redundant outputs that the
next pass dedups. No persistent state is kept between passes.

### Two-level directory walk (NOTE-VI-017-b)

`compactTenant` previously issued a single recursive `List` of the entire tenant prefix, which
loads all file keys into memory in one shot. With 1.7 M+ VI files per tenant this call takes
many minutes and effectively blocks the compactor indefinitely.

The fix replaces that one giant list with a **three-step walk**:

1. `ListDirs(indexes/<tenant>/)` — non-recursive, returns only col-hash subdirs (O(1000) entries).
2. For each col-hash: `ListDirs(indexes/<tenant>/<hash>/)` — returns type subdirs (e.g. `string/`, `int64/`).
3. For each type dir: `List(indexes/<tenant>/<hash>/<type>/)` — recursive within that single leaf
   directory (~2000 files at most), passed directly to `compactColumn`.

Each individual S3 call is cheap; the compactor starts producing L1 files within seconds of
startup instead of hanging for minutes. `IndexStore` gains a `ListDirs` method (non-recursive,
returns only directory prefixes ending in `/`); both the in-memory test store and the
production minio-backed store implement it.

### Per-(tenant, column) isolation, best-effort pass

`RunOnce` iterates tenants, `compactTenant` iterates that tenant's column directories. Both
record only the first error and continue so one bad column/tenant does not stall the rest; the
first error is returned for the caller to log. `Run` ignores per-pass errors (logs are the
caller's job) and keeps ticking. Tenants are either an explicit list or `"*"` (discovered by
listing the index prefix and taking the first path segment after it).

### IndexStore abstraction

Object storage is abstracted behind `IndexStore` (List/Get/Put/Delete, all `context`-aware) so
the orchestration is unit-testable without a real object store. Mirroring the consumer's
`ObjectPutter` decision (NOTE-VI-016), the production S3-backed `IndexStore` and `SourceExister`
are supplied by **tempo**, not blockpack — blockpack stays object-store-agnostic. `List` returns
full keys (not leaf names) so the caller can Get/Delete directly; the compactor groups by
`path.Dir`.

### Output key

`<index_prefix>/<tenant>/<col_hash>/L<level>-<xid>.blockpack` (via
`valueindex.FormatFilename(level, valueindex.NewID())`). Same column → same `<col_hash>`
directory, which both the consumer writes into and the compactor lists by.

### Public re-export (embedder pattern)

`/valueindexcompactor/valueindexcompactor.go` re-exports the minimal API (type aliases + ctor
wrapper), mirroring NOTE-VI-015 / NOTE-VI-016 / NOTE-370, because tempo cannot import
`internal/*`.

Back-refs:
`internal/modules/valueindexcompactor/service.go`,
`internal/modules/valueindexcompactor/store.go`,
`internal/modules/valueindexcompactor/config.go`,
`valueindexcompactor/valueindexcompactor.go`

## NOTE-VI-023 — Prometheus metrics for the compactor (issue #407)

Date: 2026-06-28

The compactor exposes Prometheus metrics via a `Registerer prometheus.Registerer` field on
`Config` (`yaml:"-"`, injected by tempo as `prometheus.DefaultRegisterer`), nil = total no-op.
See the consumer's NOTE-VI-023 for the shared design (nil-receiver no-ops, `registerOrReuse`
AlreadyRegisteredError tolerance, pre-resolved native-histogram observers). The collectors live
in `metrics.go`.

### CompactFiles now returns CompactStats

`valueindex.CompactFiles` previously returned only `error`; it now returns
`(CompactStats, error)` where `CompactStats{Retained, Dropped int}` counts entries kept vs
dropped by the retention `Checker`. These are **pre-dedup** counts (each input occurrence is
counted once), so `entries_retained_total` reflects the live entries flowing into the merge, not
the deduped output cardinality. This is the only exported-signature change; all callers (the
compactor `mergeLevel`, the valueindex compaction tests) were updated in the same commit. The
compactor feeds `stats.Retained`/`stats.Dropped` into the
`blockpack_value_index_compactor_entries_{retained,dropped}_total` counters.

### Per-merge file counts

`mergeLevel` records `files_read` (= input count), `files_written` (incremented in the
`CompactFiles` output callback, so it counts split outputs correctly), and `files_deleted`
(incremented per successful input delete, so a partial delete failure undercounts deletions
rather than overcounting). `merge_duration_seconds` wraps the whole get→merge→put→delete cycle.

**Addendum (2026-07-02):** Since NOTE-VI-046's streaming rewrite, `files_written_total` can
legitimately be 0 for a completed merge whose inputs were entirely retention-collapsed (every
source dead) — the merge still proceeds to delete all N inputs, so `written=0, deleted>0` in
the same pass is an expected outcome, not an anomaly, and should not be misread as such during
rollout.

**Addendum (2026-07-03):** `backlogL0Files` (registered here but left unpopulated at the time —
see the original registration comment this addendum corrects) is now genuinely populated; see
NOTE-VI-059 and `SPECS.md` SPEC-VI-6 for the current contract.

### Per-pass run counters

`RunOnce` wraps the whole pass with `run_duration_seconds` and increments `runs_total{success}`
or `runs_total{error}` based on the first-error result. List/get/put/delete failures also bump
`errors_total{op}` at their call sites.

Back-refs:
`internal/modules/valueindexcompactor/metrics.go`,
`internal/modules/valueindexcompactor/service.go`,
`internal/modules/valueindexcompactor/config.go`,
`internal/modules/valueindex/compaction.go`

## NOTE-VI-024 — Type-bucketed paths require no compactor change (issue #409)

Date: 2026-06-28

The consumer now writes index files under `<tenant>/indexes/<col_hash>/<type>/`
(NOTE-VI-024 in `valueindexconsumer/NOTES.md`). The compactor needed no logic
change: `compactTenant` groups keys by `path.Dir(key)`, which under the deeper
layout is `<tenant>/indexes/<col_hash>/<type>` — exactly the per-(hash, type)
grouping required so a merge never combines files of different types under one
single-typed writer. `List` is prefix-based (the extra nesting is returned
naturally) and `mergeLevel` joins the output filename onto that `colDir`,
preserving the `<type>` segment in the L1+ output key.

Back-refs: `internal/modules/valueindexcompactor/service.go`
(`compactTenant`/`mergeLevel`, unchanged; covered by `TestRunOnce_TypeBucketsMergedSeparately`).

## NOTE-VI-046 — mergeLevel streams input files instead of buffering them all upfront (OOM fix)

Date: 2026-07-02

`Service.mergeLevel` previously downloaded every input level file's full bytes into a
`fileBytes [][]byte` slice before calling `valueindex.CompactBucketFiles`, which in turn
decoded every file fully and materialized the entire merged group set (via
`MergeBucketFiles`'s map-of-maps) before cutting any output block — peak memory scaled with
the sum of all input file sizes for a merge level.

`mergeLevel` now decodes/filters one input file at a time via `DecodeFilteredBucketFile`
immediately after each `store.Get` call, and passes the resulting per-file
`valueindex.BucketFileIterator`s to `valueindex.StreamCompactBucketFiles`, which performs a
heap-based k-way merge and emits output blocks incrementally rather than materializing the
full merged result. See `valueindex/NOTES.md` NOTE-VI-046 for the full design rationale
(map-of-maps as the actual OOM driver, not K-open-files; the file-vs-block granularity
decision; the follow-up needed for block-granularity) and `valueindex/SPECS.md` SPEC-VI-1/
SPEC-VI-2 for the formal invariants this depends on.

An "all refs dead" edge case (every `BucketBlockRef` in every input file dropped by the
retention `RefChecker` before the merge starts) is **not** pre-filtered by `mergeLevel`
itself — `DecodeFilteredBucketFile`/`filterDeadRefs` always returns a non-nil, zero-block
`*BucketFile` in this case, so `mergeLevel` still appends an (already-exhausted)
`BucketFileIterator` to `iterators` for every such input. It naturally degrades to the
empty-input case one layer down, inside `StreamCompactBucketFiles`'s heap-seeding loop:
an iterator whose `Peek()` is immediately `(nil, false)` is simply never pushed onto the
heap, so a merge where every input degrades this way ends with an empty heap and no output
callback invocation (no output file written).

**Addendum (2026-07-03):** This entry's own claim that `mergeLevel` "never holds all inputs'
raw bytes in memory simultaneously" was correct only about the transient `store.Get` `[]byte`
— the decoded `*valueindex.BucketFile` behind each `BucketFileIterator` stayed resident for
every input for the whole merge, so peak decoded memory still scaled with the sum of all N
inputs' decoded sizes. See NOTE-VI-052 for the disk-streaming redesign that actually bounds
per-merge memory independent of input count, and the corrected `SPECS.md` SPEC-VI-1.

Back-refs: `internal/modules/valueindexcompactor/service.go:mergeLevel`,
`internal/modules/valueindex/stream_compaction.go`.

## NOTE-VI-047 — Cross-block global ordering is an emergent guarantee, not a designed one

Date: 2026-07-02

Blocks within a single `BucketFile` are globally ordered by `(TimeSec ASC, CanonicalValue
ASC)` end-to-end — not just internally sorted within each block. `SplitIntoBlocks`
(`internal/modules/valueindex/bucketmerge.go:145-176`) is the only function that ever
partitions groups into multiple blocks: it flattens every existing block's `Groups` into one
slice, sorts that slice **globally** by `(TimeSec, CanonicalValue)`, then cuts it into
fixed-size blocks sequentially, so block N's last group is always `<=` block N+1's first
group by construction. Every multi-block `BucketFile` this compactor reads or produces goes
through `SplitIntoBlocks` — the consumer's write path and `mergeLevel`'s own compaction
output both route through it.

This is the property `BucketFileIterator` (NOTE-VI-046) relies on to walk each input file
sequentially, block by block, without opening all of a file's blocks up front or re-sorting
across blocks. It was discovered by reading `SplitIntoBlocks`'s implementation while
validating the streaming design, not previously documented anywhere. The formal statement
lives in `valueindex/SPECS.md` SPEC-VI-1.

**Do not confuse this with the trace blockpack format's block ordering (a separate,
unrelated file format used elsewhere in `blockio/`), which has no such guarantee** — its
block-cutting sort key `(service.name, MinHashSig, TraceID)` has no timestamp component at
all, so trace blocks carry no inherent cross-block time/value ordering and require a separate
TS index for time-range pruning. The BucketGroup VI format needs no equivalent because its
sort key already covers both dimensions (`time_sec`, `value`) the format is queried by.

Back-refs: `internal/modules/valueindex/bucketmerge.go:SplitIntoBlocks`,
`internal/modules/valueindex/stream_compaction.go:BucketFileIterator`.

## NOTE-VI-052 — Disk-backed streaming merge: mergeLevel's input side (peak-memory-bound fix)

Date: 2026-07-03

### Why

NOTE-VI-046 (2026-07-02) fixed `MergeBucketFiles`'s map-of-maps full-materialization but left
every input file's **decoded** representation resident for the duration of one merge (K
decoded `*valueindex.BucketFile`s, all live simultaneously via `BucketFileIterator`s in the
`iterators` slice). At the time, K (input files per merge level) was assumed small/bounded, so
this was not treated as a problem. That assumption no longer holds at current batch sizes
(`DefaultCompactBatchBytes` = 1 GiB, K plausibly 40-150 files per merge) — this repeats the
same OOM-risk shape NOTE-VI-046 fixed, just at file-decode granularity instead of
merged-result granularity. Bounding this is also a prerequisite (not itself implemented here)
for a future change to parallelize `Run()`'s work-item loop across multiple concurrent merges
per shard — running several unbounded merges concurrently would multiply this exact problem.

### What changed

`mergeLevel` still calls `s.store.Get` exactly once per input file (unchanged call count/
signature — no `IndexStore` interface change). Instead of decoding the fetched bytes directly
into an in-memory `*valueindex.BucketFile`, it writes them to a local temp file
(`diskstage.go:writeLocalTempInput`, `os.CreateTemp(dir, "vi-merge-in-*.tmp")`) and constructs
a `valueindex.GroupIterator` against that file via `valueindex.NewDiskBucketFileIterator`,
which decodes only bounded metadata (header magic, footer, string table, block directory)
eagerly and decodes one block's groups at a time, lazily, as `Advance(ctx)` crosses a block
boundary — discarding the previous block once exhausted. Peak decoded memory per merge is now
bounded by (number of concurrently-open iterators × one block) instead of the sum of all
inputs' full decoded sizes. See `valueindex/NOTES.md` NOTE-VI-053 for the `valueindex`-side
half of this redesign (the new `GroupIterator` interface, the disk-backed iterator itself, and
the output-side disk staging) — this entry covers only the `mergeLevel`/input-staging half.

**Measured effect:** see `BENCHMARKS.md` BENCH-VI-1 for the actual before/after peak-memory
numbers (`TestMergeLevel_MemoryScalesWithInputCount_PreRedesign` vs.
`TestMergeLevel_MemoryBoundedRegardlessOfInputCount`).

### New local-disk dependency and its deployment prerequisite (flagged, not addressed here)

This redesign introduces `mergeLevel`'s first local-disk usage: each input file's raw bytes
are staged under `os.TempDir()` (prefix `vi-merge-in-*.tmp`; the output side, in `valueindex`,
uses `vi-merge-out-*.tmp` — see NOTE-VI-053) for the duration of one merge. Local disk usage
during a merge scales with the sum of the N input files' raw sizes plus one output file — this
is the exact quantity intentionally moved out of memory and onto disk.

**Flagged, external, not-yet-addressed:** the `value-index-compactor` Kubernetes StatefulSet
has no `emptyDir`/`ephemeral-storage` configured today — its container root filesystem overlay
is writable by default (so `os.CreateTemp` calls already succeed), but this is an unmanaged,
unsized, unmonitored resource, not a supported one. This must get a dedicated `emptyDir`
volume and matching `ephemeral-storage` request/limit before this redesign lands in
production, especially once/if the `Run()`-parallelization follow-up this task unblocks
multiplies concurrent local-disk usage by however many merges run at once. This is a
`tempo-mrd`/k8s-config change, outside this repo — recorded here so it is not a surprise at
rollout, not to be actioned by this repo's own tests or CI.

### Corruption-handling behavior change (deliberate, safer — not a regression)

Before this change, `DecodeFilteredBucketFile`'s eager whole-file decode treated **every**
decode failure past the header-magic check (footer corruption, string-table corruption,
block-index corruption, or an individual block's decode/snappy failure) identically to a
legacy-format mismatch: silently skip the file, do not abort the merge. The disk-backed
iterator distinguishes these cases, because it now can:

- **Header magic mismatch only** → still treated as "legacy pre-v2 file, skip this file, do
  not abort the merge" (`NewDiskBucketFileIterator` returns `(nil, nil)`, an explicit untyped
  nil `GroupIterator` — never a typed-nil pointer leaking through the interface, verified by
  `TestNewDiskBucketFileIterator_LegacyFileReturnsTrueNilInterface`).
- **Any other decode failure** — footer/string-table/block-index corruption (discovered
  eagerly, at construction) or an individual block's corruption (discovered lazily, at that
  block's `Advance`) — is now a **real error** that aborts the whole merge (`mergeLevel`
  returns the error; S3 inputs remain untouched for retry, per the write-then-delete
  crash-safety invariant, NOTE-VI-017). Verified by
  `TestNewDiskBucketFileIterator_CorruptBlockAborts`.

**This is an intentional, small, deliberate behavior change, not a regression:** it converts a
previously-silent, low-severity data-loss path (a corrupt file's data permanently and silently
dropped) into a visible, retried error (the merge fails loud and retries the whole batch later
until an operator addresses the corrupt file). This trades a small availability cost (a
genuinely corrupt file in production could now cause repeated merge failures for its batch
until intervention) for a strictly safer default (no more silent, permanent data loss on
corruption discovered past the header check). Flagging prominently here per this repo's
convention for documenting deliberate behavior divergences.

### Ownership/cleanup contract (per-merge temp files)

Every per-merge local temp file (N inputs + 1 output) is cleaned up via `defer` on every exit
path of the function that created it:

| Outcome | Who closes the fd | Who removes the input's local temp file |
|---|---|---|
| Header magic mismatch (legacy skip) | `NewDiskBucketFileIterator` | caller (`mergeLevel`, immediately) |
| Any other `NewDiskBucketFileIterator` construction error | `NewDiskBucketFileIterator` | caller (`mergeLevel`, immediately) |
| Success (live iterator returned) | the iterator's own `Close()` | the iterator's own `Close()` (via `mergeLevel`'s `defer`) |

A crash between writing a local temp file and process exit could leak it across restarts
(local disk, once an `emptyDir` is added, persists across container restarts of the same pod).
`valueindex.SweepOrphanedMergeTempFiles()` — called once, best-effort, from
`valueindexcompactor.NewService` — globs and removes any leftover `vi-merge-*.tmp` files at
process start to catch this. A sweep failure is logged/metriced but never fails service
construction (a leftover orphaned file is a disk-hygiene concern, not a correctness blocker).
See `valueindex/NOTES.md` NOTE-VI-053 for the sweep's own implementation and its deliberate
naming distinctness from `runspill.go`'s `vi-run-*.tmp` files.

Back-refs: `internal/modules/valueindexcompactor/service.go:mergeLevel`,
`internal/modules/valueindexcompactor/diskstage.go:writeLocalTempInput`,
`internal/modules/valueindex/disk_iterator.go:NewDiskBucketFileIterator`,
`internal/modules/valueindex/temp_cleanup.go:SweepOrphanedMergeTempFiles`.

## NOTE-VI-054 — CompactConcurrency and CompactMaxInputFiles: different default-safety philosophies

Date: 2026-07-03

This is the config-plumbing half of parallelizing `Run()`'s column-merge loop (see NOTE-VI-052's
own "Why" section, which flagged bounding per-merge memory as a prerequisite for exactly this
follow-up). Two new `Config` fields land together but default very differently on purpose:

- **`CompactConcurrency`** defaults to `1` (`DefaultCompactConcurrency`), which is byte-for-byte
  today's existing fully-sequential dispatch order. This is a **zero-behavior-change default**:
  no deployment is affected unless it explicitly opts in by raising the value. Concurrency is a
  genuinely new capability being introduced cautiously — there is no pre-existing bug it fixes on
  its own, only new throughput it can unlock once an operator chooses to enable it.
- **`CompactMaxInputFiles`** defaults to `150` (`DefaultCompactMaxInputFiles`) **unconditionally**
  — this default changes `compactColumn`'s batch-selection behavior for every deployment
  immediately upon this code shipping, independent of whether `CompactConcurrency` is ever
  touched. This is deliberate: an unbounded per-merge file count is a real, pre-existing
  fd/local-disk-exhaustion latent bug (a single sequential merge today can already open
  hundreds-to-thousands of local temp files with no scratch volume provisioned — see NOTE-VI-052's
  flagged `emptyDir`/`ephemeral-storage` gap), so the fix is applied unconditionally rather than
  gated behind the concurrency opt-in. `150` is an explicit, conservative, provisional
  placeholder (chosen without live cluster data on average L0 file size at planning time) —
  trivially tunable post-deploy via the `compact_max_input_files` YAML key without a code change.

Both fields use the same mechanical `<= 0 → default` normalization in `withDefaults()`, but
`0`/negative never means "unlimited" for either one (see SPEC-VI-2) — this is a departure from
`CompactBatchBytes`'s existing "0 = no cap" convention, made deliberately because an accidentally
unbounded file-count or concurrency value is exactly the failure mode these two fields exist to
prevent.

Back-refs: `internal/modules/valueindexcompactor/config.go:Config.CompactConcurrency`,
`config.go:Config.CompactMaxInputFiles`, `config.go:withDefaults`.

## NOTE-VI-055 — Concurrency rollout gradualness is a deploy-time decision, not encoded in code

Date: 2026-07-03

`CompactConcurrency` is a plain per-`Config` field with no `ShardIndex`-conditional logic in
code. Rolling concurrency out gradually (e.g. enabling `compact_concurrency: 8` on one shard
first, watching `errors_total{op=~"list|get|put|delete|panic"}` and the in-flight/configured-
concurrency/backlog gauges NOTE-VI-059 describes, then widening to the rest) is entirely a
deploy-time / ConfigMap-per-shard decision: different shards can simply be given different
`compact_concurrency` values in their own YAML today, with zero code support needed beyond the
field existing.

`ShardIndex`-conditional concurrency in code was considered and rejected — it would require a
code change to express something the existing per-shard-ConfigMap mechanism already provides for
free, adding complexity without adding capability. This keeps `Run()`'s own logic
shard-agnostic: it always dispatches at whatever `CompactConcurrency` its own `Config` carries,
and the operational judgment of how fast to roll out stays with whoever manages the deploy.

Back-refs: `internal/modules/valueindexcompactor/config.go:Config.CompactConcurrency`,
`service.go:Run`.

## NOTE-VI-056 — effectiveBatchBytes (Candidate A) chosen over streaming IndexStore.Put (Candidate B)

Date: 2026-07-03

Raising `CompactConcurrency` means multiple merges' output buffers can be resident at once,
multiplying the aggregate worst-case output-buffer memory that was previously bounded by a
single `CompactBatchBytes` at a time. Two mitigations were considered:

- **Candidate A (chosen):** scale `CompactBatchBytes` down by `CompactConcurrency` via a new
  `effectiveBatchBytes()` helper (SPEC-VI-3), so aggregate worst-case output-buffer memory across
  all concurrently in-flight merges stays bounded near the single configured `CompactBatchBytes`
  regardless of concurrency. This requires no interface changes and composes cleanly with the
  existing per-merge batch-selection loop.
- **Candidate B (rejected for this task, deferred):** make `IndexStore.Put` accept a streaming
  `io.Reader` instead of a full `[]byte`, so output buffering itself never needs to hold a whole
  batch's merged bytes in memory regardless of concurrency. This was explicitly ruled out of
  scope here because `IndexStore` is an exported interface (`store.go`'s own doc comment: "exported
  so external callers [tempo] can supply their object store") — widening it is a breaking,
  cross-repo (blockpack + tempo) change requiring its own dedicated brainstorm and rollout, not a
  incidental addition to a `Run()`-concurrency task. Candidate B remains a legitimate, separate,
  future follow-up if `effectiveBatchBytes` scaling alone proves insufficient at high concurrency
  in practice.

Back-refs: `internal/modules/valueindexcompactor/service.go:effectiveBatchBytes`,
`store.go:IndexStore`.

## NOTE-VI-057 — errgroup.SetLimit chosen over a hand-rolled worker pool

Date: 2026-07-03

`Run()`'s concurrency restructuring uses `golang.org/x/sync/errgroup`'s `WithContext` +
`SetLimit(n)` + `Go(...)`, the same idiom already used identically in
`internal/modules/vibuilder/builder.go` (precedent read directly during planning). A hand-rolled
worker pool (a fixed number of long-lived goroutines pulling from a shared channel of work items)
was considered and rejected: it would need its own separate in-flight-tracking structure (to know
when a "lap" — one full pass through the current work list — has fully drained) purely to
reconstruct the exact guarantee `errgroup`'s `Wait()` already provides for free at each lap
boundary (SPEC-VI-4). A hand-rolled pool's cross-lap same-`colDir` exclusivity would require
either that same manually-built drain-tracking or a per-`colDir` lock/lease scheme — meaningful
extra complexity for no real throughput benefit, since lap durations are already small relative
to `CompactInterval` and `errgroup.SetLimit` already bounds concurrency exactly as a pool would.
`g.Wait()`-before-rebuild is simpler to state, simpler to verify (one ordering constraint,
directly callable out in `Run()`'s own doc comment), and reuses a dependency and idiom already
established elsewhere in this codebase.

Back-refs: `internal/modules/valueindexcompactor/service.go:Run`,
`internal/modules/vibuilder/builder.go` (the precedent this idiom matches).

## NOTE-VI-058 — Global per-shard concurrency cap chosen over a per-level (L0→L1 vs L1→L2) split

Date: 2026-07-03

`CompactConcurrency` is one flat limit applied to `Run()`'s entire work-item dispatch loop,
regardless of which compaction level a given `columnWork` item will end up merging at (that is
decided later, inside `compactColumn`, once the column's files are actually listed and grouped by
level). A per-level split (e.g. a separate, independently-tunable concurrency limit for L0→L1
merges versus L1→L2 merges) was considered and rejected for this iteration:

- `Run()`'s work-list granularity is per-`(tenant, colDir)`, not per-level — a `columnWork` item
  does not know in advance which level it will compact, so splitting concurrency by level would
  require restructuring `buildWorkList`/dispatch around level-aware work items, a materially
  larger change than this task's scope.
- The current production backlog is observed to be ~100% L0-bound (per the brainstorm's own
  finding) — a per-level split today would either starve the one level that actually has backlog
  (if the two limits summed to the same total concurrency) or sit almost entirely unused on the
  L1→L2 side (if given its own separate budget), neither of which meaningfully improves on a
  single flat limit given today's actual workload shape.

If a future workload shifts meaningfully away from "almost entirely L0-bound," revisit this
decision — the `columnsCompacted{level}` counter and the newly-lit `backlogL0Files` gauge
(NOTE-VI-059) together provide the observability needed to notice that shift when/if it happens.

Back-refs: `internal/modules/valueindexcompactor/service.go:Run`, `service.go:buildWorkList`,
`service.go:compactColumn`.

## NOTE-VI-059 — No backlog-size-aware prioritization added this iteration

Date: 2026-07-03

`Run()`'s work-item dispatch order is unchanged by this task: `buildWorkList` produces a flat,
unordered list of `(tenant, colDir)` pairs each lap, and `Run()` dispatches them in that order
(now with up to `CompactConcurrency` running at once, but no reordering by backlog size,
tenant priority, or any other weighting). A backlog-size-aware scheduler (e.g. dispatching
columns with the largest L0 backlog first) was explicitly not built this iteration — concurrency
alone is expected to provide most of the needed throughput multiplier for the currently-observed
L0-bound backlog shape, and adding prioritization logic without first observing whether plain
concurrency is sufficient would be speculative complexity.

This is measurable, not just assumed: `backlogL0Files` (SPEC-VI-6), previously registered but
left permanently at 0 (NOTE-VI-023's original registration comment — a dedicated directory walk
to populate it doubled pass latency and caused OOMKills at scale), is now genuinely populated
from data `compactColumn` already has in hand (zero extra I/O), and combines with the new
`merges_in_flight` and `configured_concurrency` gauges (Phase 3) to give the dashboards needed to
watch whether concurrency alone is closing the backlog fast enough post-deploy. Revisit
backlog-aware prioritization only if that observability shows plain concurrency is insufficient
in practice.

Back-refs: `internal/modules/valueindexcompactor/service.go:Run`, `service.go:buildWorkList`,
`service.go:compactColumn` (the `s.metrics.setBacklogL0` call site), `metrics.go`.

## NOTE-VI-065 — Trace-index format-dispatch runs before the `vbg2Magic` purge, not only inside the merge function (issue #428 wiring, Stage 3)

Date: 2026-07-04

`compactColumn`'s existing `vbg2Magic` Peek/purge loop predates this change and exists to
detect and delete legacy pre-v2 (VIMT/VINX) files it encounters mixed in with current
`BucketGroup` files. `TraceGroup` files (`valueindex.EncodeTraceGroups`) were never designed
to carry that magic at all — they have no outer `BucketFile`-style ToC/footer framing, just a
bare `version[1] + string_table + groups` payload (snappy-compressed). Had the trace-index
dispatch decision been made only inside a merge function (i.e., after the purge loop already
ran), every `TraceGroup` L0 file `valueindexconsumer` ever flushed would have been
misidentified as legacy junk and deleted on the compactor's very first pass over that colDir —
a real, silent data-loss bug, not a hypothetical one, since the purge loop's whole job is to
delete anything that doesn't start with the expected magic bytes.

The fix: `isTraceIndexColDir(colDir)` is checked in `compactColumn` immediately after the
existing level-grouping/batch-selection logic but **before** the `vbg2Magic` loop, and a true
match skips that loop entirely (routing to `mergeTraceLevel` instead of `mergeLevel`). This is
why the dispatch condition lives in its own small file
(`traceindex_dispatch.go`) rather than as a branch buried inside `mergeLevel`'s own body — the
decision has to happen at a point in `compactColumn`'s control flow that didn't structurally
exist as a natural "merge function" boundary before this change.

Back-refs: `internal/modules/valueindexcompactor/service.go:compactColumn`,
`internal/modules/valueindexcompactor/traceindex_dispatch.go:isTraceIndexColDir`. Regression
guard: `TestCompactColumn_TraceIndexColDirDispatchesToTraceMerge`'s `orderGuardStore` fails the
test if `Delete` is ever called before any `Get` for a trace-index colDir. See `SPECS.md`
SPEC-VI-7.

## NOTE-VI-066 — A corrupt trace-index input is skipped and left in place, not deleted (issue #428 wiring, Stage 3)

Date: 2026-07-04

`mergeTraceLevel` treats a `valueindex.DecodeTraceGroups` failure on one input file as
recoverable at the merge level (skip that file, proceed with the rest) but NOT as license to
delete the unreadable file. This is a deliberate asymmetry with the normal case (a
successfully-decoded file is always deleted once incorporated into a merge, even if every span
it carried ends up dropped by retention — SPEC-VI-7 point 4/5): a corrupt file might represent
a genuine producer-side bug worth investigating, or might be recoverable by some future fix;
deleting it destroys that option irreversibly, while leaving it in place costs only a small,
bounded amount of storage and one wasted decode attempt per future compaction pass (it will
keep failing to decode and keep being skipped until either manually removed or a producer-side
fix makes it decodable — it does not block the rest of that colDir's compaction, and does not
grow unboundedly since it does not multiply, it just sits there).

This mirrors the same "corruption degrades to safe, non-destructive skip" posture Stage 4's
read side takes for a corrupt index file (root `SPEC.md` SPEC-ROOT-018: "a discovered file
fails to decode... skip this file, try the next candidate") — corruption anywhere in this
pipeline is handled by skipping and preserving, never by silently deleting.

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch.go:mergeTraceLevel`. Tested
by `TestMergeTraceLevel_CorruptInputSkippedNotAborted`. See `SPECS.md` SPEC-VI-7.
