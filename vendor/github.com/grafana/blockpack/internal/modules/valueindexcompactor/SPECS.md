# valueindexcompactor — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/valueindexcompactor` package. It complements `NOTES.md` (design rationale)
and `TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, which is shared/global
across the whole value-index pipeline's NOTES.md files by established convention, and distinct
from `internal/modules/valueindex/SPECS.md`'s own independent `SPEC-VI-N` sequence — each
module's SPECS.md numbers from 1). IDs are assigned in ascending order and never reused or
renumbered; superseded entries are marked `[SUPERSEDED by SPEC-VI-N]` rather than deleted.

Next free ID: **SPEC-VI-8**.

---

## SPEC-VI-1: mergeLevel streaming-wiring contract
*Added: 2026-07-02*
*Corrected: 2026-07-03 (see below)*

**Contract (corrected 2026-07-03):** inputs are staged to local disk one at a time as they
are fetched; each input's decoded representation is bounded to one block at a time via a
disk-backed lazy iterator (`valueindex.NewDiskBucketFileIterator`) — peak decoded memory is
bounded by (number of concurrently-open iterators × one block), not by the number or total
size of input files.

**Correction note:** the original 2026-07-02 wording claimed "`mergeLevel` decodes and
retention-filters one input file at a time via `valueindex.DecodeFilteredBucketFile`... It
never holds all inputs' raw bytes in memory simultaneously." That claim was true only of the
transient `[]byte` returned by `s.store.Get` — the **decoded** `*valueindex.BucketFile` held
inside each `*valueindex.BucketFileIterator` was never freed, so all N inputs' fully-decoded
representations stayed resident simultaneously for the whole merge, and peak memory actually
scaled with the sum of all input files' decoded sizes (proven by
`TestMergeLevel_MemoryScalesWithInputCount_PreRedesign`, see `BENCHMARKS.md` BENCH-VI-1). The
corrected contract above reflects the disk-streaming redesign (NOTE-VI-052) that makes the
"one block at a time" bound actually true, verified by
`TestMergeLevel_MemoryBoundedRegardlessOfInputCount`. See `valueindex/NOTES.md` NOTE-VI-046
(original OOM fix, file-granularity) and NOTE-VI-053 (this redesign, block-granularity
follow-up) for the full rationale, and `valueindex/SPECS.md` SPEC-VI-1/SPEC-VI-2/SPEC-VI-5 for
the formal invariants this depends on.

**Crash-safety property preserved:** input keys are deleted only after the merged output's
`Put` has succeeded — the streaming rewrite does not change this ordering. A `Put` failure
leaves all inputs in place for the next compaction pass to retry (write-then-delete, per
NOTE-VI-017). Local temp files (`vi-merge-in-*.tmp`/`vi-merge-out-*.tmp`) introduced by this
redesign are pure, disposable caches of already-durable S3 state — their cleanup is
orthogonal to this S3-level ordering guarantee (see NOTE-VI-052 for the local-disk
crash-safety/cleanup contract).

Back-ref: `internal/modules/valueindexcompactor/service.go:mergeLevel`,
`internal/modules/valueindexcompactor/diskstage.go:writeLocalTempInput`,
`internal/modules/valueindex/disk_iterator.go:NewDiskBucketFileIterator`.

---

## SPEC-VI-2: CompactConcurrency / CompactMaxInputFiles config contract
*Added: 2026-07-03*

**Contract:** `Config.CompactConcurrency` (yaml `compact_concurrency`) sets the number of
concurrent column merges `Run()` dispatches per lap; `Config.CompactMaxInputFiles` (yaml
`compact_max_input_files`) caps the number of files a single merge job may consume,
independent of and applied before `CompactBatchBytes`/`effectiveBatchBytes()`. Both fields
default via a uniform `<= 0 → default` rule in `withDefaults()`:
`DefaultCompactConcurrency = 1` (exactly today's sequential dispatch order — a zero-behavior-
change default for any deployment that does not explicitly opt in) and
`DefaultCompactMaxInputFiles = 150`.

**Explicitly not "0 = unlimited":** unlike `CompactBatchBytes` (where `0` means "no byte cap,"
honored by `effectiveBatchBytes()` returning `0` unchanged — see SPEC-VI-3), neither
`CompactConcurrency` nor `CompactMaxInputFiles` has an "unbounded" escape hatch. `<= 0` for
either field always normalizes to its default, never to "no limit" — an unbounded file-count
cap is exactly the fd/local-disk-exhaustion bug `CompactMaxInputFiles` exists to prevent
(NOTE-VI-054), so allowing `0` to mean "uncapped" here would silently reintroduce it.

**`CompactMaxInputFiles`'s floor of 2:** `compactColumn`'s batch-selection loop clamps an
effective cap below 2 up to 2, mirroring the existing "always include at least 2 files to make
progress" floor already used for the byte cap — a pathological `compact_max_input_files: 1`
must not stall the merge on a 1-file no-op.

Back-ref: `internal/modules/valueindexcompactor/config.go:Config.CompactConcurrency`,
`config.go:Config.CompactMaxInputFiles`, `config.go:withDefaults`,
`service.go:compactColumn` (file-count cap block).

---

## SPEC-VI-3: effectiveBatchBytes — concurrency-scaled output-buffer memory bound
*Added: 2026-07-03*

**Invariant:** raising `CompactConcurrency` scales down `effectiveBatchBytes()` proportionally
(`CompactBatchBytes / CompactConcurrency`), so aggregate worst-case output-buffer memory across
all concurrently in-flight merges stays bounded near the single configured `CompactBatchBytes`
regardless of how many merges run at once — turning up concurrency turns up how many batches
run at once, not peak aggregate memory.

**Exact formula and guards** (`effectiveBatchBytes()`):
- `CompactBatchBytes <= 0` (explicit "no cap") → returns `0` unchanged; scaling never manufactures
  a cap out of "no cap," regardless of `CompactConcurrency`.
- Otherwise → `CompactBatchBytes / max(CompactConcurrency, 1)`, floored to `1` if integer
  division would otherwise truncate a real, nonzero cap to `0` (e.g. `CompactBatchBytes: 3`,
  `CompactConcurrency: 10` → `1`, never silently reinterpreted as uncapped).
- `CompactConcurrency < 1` (e.g. called against a not-yet-`withDefaults()`-normalized `Config`)
  is defensively clamped to `1` inside the helper itself — the helper must not divide by zero
  or misbehave if ever called before defaulting.

Back-ref: `internal/modules/valueindexcompactor/service.go:effectiveBatchBytes`. Tested by
`concurrency_test.go:TestEffectiveBatchBytes_Table` (all four cases: exact no-op at
concurrency 1, proportional scaling, defensive concurrency-0 clamp, 0-stays-0, and the
floor-to-1 truncation guard).

---

## SPEC-VI-4: colDir exclusivity across a lap boundary
*Added: 2026-07-03*

**Invariant:** no two goroutines dispatched by `Run()` ever call `List`/`compactColumn` for the
same `colDir` concurrently, including across a lap boundary. `Run()`'s `g.Wait()` call — invoked
both at the `ctx.Err() != nil` early-return branch and at the `idx >= len(work)` lap-rebuild
branch, always *before* the work list is rebuilt or a new `errgroup.Group` is constructed — is
the sole mechanism that guarantees this: it fully drains every in-flight goroutine from the
just-finished lap before any goroutine from the next lap can be dispatched. `buildWorkList`
already guarantees no duplicate `colDir` *within* one lap by construction (each `colDir` appears
at most once per call), so exclusivity within a lap is structural; exclusivity *across* a lap
boundary depends entirely on this `g.Wait()`-before-rebuild ordering and would break immediately
if a plain unbounded worker-pool-with-continuous-dispatch design were substituted instead (the
alternative the brainstorm considered and rejected for exactly this reason).

**Do not reorder the `g.Wait()` calls relative to work-list rebuild** — this is called out
directly in `Run()`'s own doc comment as a correctness-critical ordering constraint, not a style
preference.

Back-ref: `internal/modules/valueindexcompactor/service.go:Run` (both `g.Wait()` call sites).
Tested by `run_concurrency_test.go:TestRun_NeverProcessesSameColDirConcurrentlyAcrossLapBoundary`
(TEST-VI-10).

---

## SPEC-VI-5: Panic isolation — a panicking merge must not crash the process or affect other concurrent merges
*Added: 2026-07-03*

**Invariant:** a panic occurring anywhere inside one column's dispatched merge (the `List` call,
`compactColumn`, or anything `compactColumn` transitively calls, including `mergeLevel`) must not
crash the process and must not prevent any other concurrently in-flight column's merge from
completing normally. This is this module's own instance of the codebase-wide `SPEC-ROOT-001`
panic-safety invariant.

**Mechanism:** every `g.Go` closure dispatched by `Run()` wraps its body in a `defer func() {
if rec := recover(); rec != nil { ... } }()`. On recovery it logs via `slog.Error` (including
`colDir`, `tenant`, and a captured `debug.Stack()`) and increments
`errors_total{op="panic"}` (`compactorOpPanic`).

**Critical detail — the recover() branch returns `nil`, not a synthesized error:** this is a
deliberate correction to an earlier draft design that had the recovered branch return a non-nil
`error` from the `g.Go` closure. `errgroup.WithContext`'s `gctx` is canceled the *first* time any
`g.Go` closure returns a non-nil error — returning an error from the panic branch would cancel
`gctx` for the entire lap the moment any one column panicked, which every other concurrently
in-flight closure observes via its own `ctx.Err()` checks (inside `mergeLevel`'s per-file loop),
aborting every other in-flight merge in that lap. That is exactly the cross-column blast radius
this invariant exists to prevent. The panic branch therefore returns `nil`, identically to the
ordinary `lerr`/`cerr` error branches in the same closure — only a genuine `recover()` triggers
the panic-specific logging/metric side effects; the closure's *return value* to `errgroup` is
uniformly `nil` on every path, panic or not. This is what makes `gctx` actually never canceled
mid-lap true in practice, not just in intent.

**Consequence for `Run()`'s own return value:** because every closure returns `nil` regardless
of outcome, `g.Wait()`'s own return value is never non-nil from panics or ordinary per-item
errors, so `Run()` itself only ever returns `ctx.Err()` (from cancellation) — a panic never
alters `Run`'s control-flow return value.

Back-ref: `internal/modules/valueindexcompactor/service.go:Run` (the `g.Go` closure's
`recover()` defer). Tested by
`run_concurrency_test.go:TestRun_PanicInOneMergeDoesNotCrashOthersOrProcess` (TEST-VI-9).

---

## SPEC-VI-6: backlogL0Files gauge contract
*Added: 2026-07-03*

**Contract:** `blockpack_value_index_compactor_backlog_l0_files{tenant}` is set to
`len(byLevel[0])` — the number of level-0 files observed for one column directory — on every
`compactColumn` call, immediately after that column's files have been grouped by level and
before batch-selection/merge logic runs. The gauge reflects a snapshot from the *most recent*
`compactColumn` call for whichever `(tenant, colDir)` last reported, not a running total across
all columns; a tenant with many columns will have this gauge repeatedly overwritten as
`compactColumn` cycles through that tenant's columns, so it approximates "how backlogged was the
last column I looked at for this tenant," useful for dashboard trend-watching rather than an
exact point-in-time total.

**No extra I/O:** this reuses `byLevel` data already fetched by `compactColumn`'s existing
level-grouping pass (the same `List` result already retrieved to decide whether to merge) — it
does not add any new `List`/`Get` call.

**History:** previously (see NOTE-VI-023) this gauge was registered but intentionally left
unpopulated (`Help` text: "currently not populated — snapshot was removed due to OOM") because a
*dedicated* directory walk to count L0 files doubled pass latency and caused OOMKills at scale.
This entry supersedes that "always 0" contract: the gauge is now genuinely populated, but via
data already in hand rather than a new dedicated walk, so the original OOM concern does not
reapply.

Back-ref: `internal/modules/valueindexcompactor/service.go:compactColumn` (the
`s.metrics.setBacklogL0` call site), `metrics.go:setBacklogL0`. Tested by
`metrics_test.go:TestCompactColumn_PopulatesBacklogL0Files` (TEST-VI-11).

---

## SPEC-VI-7: Trace-index format-dispatch — colDir detection, magic-purge skip, `mergeTraceLevel` crash/retention contract
*Added: 2026-07-04*

**Dispatch condition:** `compactColumn` (`service.go`) branches on `isTraceIndexColDir(colDir)`
(`traceindex_dispatch.go`) — a hash-equality check, `path.Base(path.Dir(colDir)) ==
traceIDColHash`, where `traceIDColHash = valueindex.ColHash("trace:id")` is precomputed once at
package init. This check runs **before** the existing `vbg2Magic` Peek/purge loop, not only
inside the merge function. This ordering is a real correctness fix, not a stylistic choice:
`TraceGroup` files (`valueindex.EncodeTraceGroups`) carry no `vbg2Magic` (`0x56424732`) framing
at all — if the dispatch check ran only inside a merge function (after the purge loop already
executed), every `TraceGroup` L0 file would be Peek-validated against a magic number it was
never designed to have, silently deleted as legacy junk on the compactor's very first pass over
that colDir, before `mergeTraceLevel` ever got a chance to run.

**On a true dispatch match:** the `vbg2Magic` Peek/purge loop is skipped **entirely** for that
colDir (not run and ignored — genuinely not executed), and `mergeTraceLevel` is called instead
of `mergeLevel`. The existing upstream batch-selection logic (level-grouping, `effectiveBatchBytes`
byte cap, `CompactMaxInputFiles` file-count cap) runs identically for trace-index colDirs as for
standard ones — dispatch happens strictly after that capping, so a trace-index colDir's merge
batch is capped by the same rules as any other column's.

**Collision note (documented, not defended against in code):** dispatch is by hash equality
against a 128-bit `ColHash` output (`ColHash` truncates `SHA-256(colName)` to its first 16
bytes before hex-encoding — `valueindex/hash.go`'s own doc comment: "32-char lower-hex column
hash... SHA-256(col_name)[:16]" — so the effective hash space is 128 bits, not the full 256-bit
SHA-256 digest), not a reserved/special-cased literal column-name string. A
different column name colliding with `"trace:id"`'s hash is a cryptographic hash collision —
not a realistic concern, no runtime guard exists for it, and none is added by this entry.

**`mergeTraceLevel` crash-safety contract (mirrors `mergeLevel`'s write-then-delete ordering,
SPEC-VI-1):**
1. Sort inputs by key for deterministic ordering; `outputLevel = files[0].level + 1`.
2. Fetch and `valueindex.DecodeTraceGroups` each input. **A decode failure on one input file is
   NOT fatal to the merge** — that one file is skipped (counted, logged via
   `slog.Warn`, `compactorOpDecode` error-metric incremented) and excluded from both the merge
   input set and the post-merge delete set; the merge proceeds with every other, successfully
   decoded file. This differs deliberately from a hard `Get` failure (network/store error),
   which DOES abort the whole merge immediately (`TestMergeTraceLevel_GetErrorAbortsMerge`) —
   the distinction is "this specific payload is unreadable" (skip it, keep the data around for
   investigation) vs. "the store itself is not answering" (nothing productive can be salvaged
   from partial success).
3. `valueindex.MergeTraceGroups(ctx, checker, allGroups...)` merges every successfully-decoded
   file's groups (SPEC-VI-6).
4. **Zero-output guard:** if the merge result is empty (every span across every input was
   retention-dropped), no output file is written at all — mirrors the `BucketGroup` path's
   "no spurious empty output file" behavior (NOTE-VI-046).
5. **Write-then-delete:** the merged output (if any) is `Put` before any input is deleted. Only
   inputs that were successfully decoded and incorporated into the merge are deleted afterward
   — a corrupt input (step 2) is deliberately left in place, never deleted, since deleting it
   cannot be undone and a retry can't fix a genuinely malformed payload, while leaving it in
   place preserves the option to investigate a possible producer-side bug. A `Put` failure
   leaves every input untouched (`TestMergeTraceLevel_PutFailureKeepsInputs`), identical
   ordering guarantee to `mergeLevel`.
6. The merged output's filename embeds `[wallMinSec, wallMaxSec]` computed directly from the
   in-memory merged `[]TraceGroup` (`traceGroupWallRange`) — `TraceGroup` files carry no footer
   to decode a range back out of (unlike `BucketGroup`'s `DecodeBucketFooter`), mirroring
   `valueindexconsumer`'s identical approach at flush time
   (`valueindexconsumer/SPECS.md` SPEC-VI-2).

Back-refs: `internal/modules/valueindexcompactor/service.go:compactColumn` (dispatch site),
`internal/modules/valueindexcompactor/traceindex_dispatch.go:isTraceIndexColDir`,
`:mergeTraceLevel`, `:traceGroupWallRange`. See `valueindex/SPECS.md` SPEC-VI-6 (the widened
`MergeTraceGroups` this function calls) and root `SPEC.md` SPEC-ROOT-018 (the read-side
counterpart this write-side work makes possible).
