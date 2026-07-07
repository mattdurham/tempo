# vibuilder — value-index source orchestration

## NOTE-VI-036: BuildSource — discover + download + predicate-filter into a ValueIndexSource (issue #461)

*Added: 2026-06-30*

The index-driven search (`QueryTraceQLFromIndex`, NOTE-VI-035) and metrics
(`ExecuteMetricsTraceQL` with `TraceMetricOptions.ValueIndex`, NOTE-VI-033) paths
both consume a *pre-populated* `executor.ValueIndexSource`. Until this package the
"querier discovers + downloads + applies the per-leaf predicate" step lived only as
prose in the executor NOTES. `vibuilder.BuildSource` implements it so tempo's
`blockpackBlock` can wire the index path with one call.

**Flow.** Walk `vm.Program.Predicates`:

- Each **leaf** RangeNode naming a column → build a `valueindex.Predicate`
  (`buildPredicate`): single-value equality, one/two-sided range (Min/Max →
  Range/Between), or regex `Pattern`. ColHash + ColTypeName → discover overlapping
  files via the `FileDiscoverer` (an `IndexFileCache`, issue #462) → download via
  `FileStore` → `valueindex.QueryFiles(pred, ...)` → map `LookupResult` →
  `executor.VILookupResult` → `src.Add`.
- **Match-all with a column list** (no Nodes, Columns populated — the shape of
  `{} | rate()`): probe every type bucket per column with a **nil predicate**
  (`Reader.Lookup` treats nil as match-all) to enumerate the span universe for
  `AllResults`.

**Coverage contract (mirrors NOTE-VI-033 exactly).** A column that is indexed but
has no matching spans (or no files at all) is `Add`ed with an **empty slice** —
that is *coverage*, not fallback. `LookupResults` then returns `(nil, true)`. A
column the builder could not express a predicate for (vector, present-only, or an
unsupported value type) is **never Added**, so the executor sees no coverage and
falls back to a full block scan for that leaf. `BuildSource` returns
`(nil, false, nil)` only when *no* column resolved at all (e.g. bare `{}`, which
has neither nodes nor columns and so cannot enumerate a universe partitioned
per column) — the caller then skips the index path entirely.

**Fail-safe on error.** A discovery or download error is returned to the caller,
which must fall back to a full scan rather than fail the query. A per-file download
error aborts the whole build (returning the error) rather than silently dropping a
file, which would under-count results — **except** a not-found file, which is
skipped (see NOTE-VI-041).

**Why a separate package.** The orchestration needs `valueindex` (predicates,
QueryFiles, IndexFileCache), `executor` (VILookupResult, SliceValueIndexSource), and
`vm` (RangeNode). `executor` and `valueindex` do not import each other; vibuilder is
the single place that depends on all three, avoiding an import cycle. The public
`blockpack.BuildValueIndexSource` / `blockpack.NewIndexFileCache` /
`blockpack.ValueIndexFileStore` are thin wrappers in `valueindex_query.go`.

Back-ref: `internal/modules/vibuilder/builder.go`, `valueindex_query.go`.
Tests: `builder_test.go`, `api_test.go:TestBuildValueIndexSource_*`.

## NOTE-VI-039: BuildSource records download I/O on the source for querier observability (tempo issue #465)

*Added: 2026-06-30*

`SliceValueIndexSource` now carries a `ValueIndexBuildStats` (FilesRead, BytesRead,
Hits) populated as `BuildSource` downloads each column's files. `lookupColumn` /
`lookupColumnAll` / `downloadAll` return the per-call file count + byte total;
`BuildSource` folds them into the source via `src.RecordFileIO`. `Stats()` derives
`Hits` from the stored result slices at read time (so it stays correct regardless of
`Add` ordering) and returns the accumulated file/byte counts.

**Why on the source, not a new return value.** `BuildSource` and its public wrappers
(`BuildValueIndexSource`, `BuildValueIndexSourceForMetrics`) already return
`(source, ok, err)` and are called from tempo + the deadcode anchor. Attaching the
stats to the returned source keeps that signature stable (no tempo callsite churn)
and the I/O is intrinsic to "what did it cost to build this source" — the natural
owner. tempo's `blockpackBlock.tryIndexFetch` reads `src.Stats()` and stamps the
counts onto its OTel span even when the index later declines (those bytes were spent).

**Covered-but-empty still counts bytes.** A column the predicate matched zero spans
for still downloaded files to decide that; those bytes/files count toward the stats
(observability reflects real I/O, not just hits).

Back-ref: `builder.go` (lookupColumn/lookupColumnAll/downloadAll/BuildSource),
`internal/modules/executor/metrics_trace.go` (ValueIndexBuildStats, Stats,
RecordFileIO), `api.go` (ValueIndexBuildStats alias).
Tests: `builder_test.go:TestBuildSource_Stats*`,
`metrics_trace_vi_test.go:TestSliceValueIndexSource_StatsAccumulate`.

**Addendum (2026-07-07, B-5, issue #488) — `BytesRead`'s meaning survives the ranged-read
rewire unchanged, but now measures something bigger than what actually crosses the wire.**
`downloadAll` no longer exists (see NOTE-VI-041/049's own addenda below), but this note's core
claim — `BytesRead` reflects "what did it cost to decide this column's coverage," accumulated
per consulted file — is unchanged and re-pinned by `TestBuildSource_StatsRecordFileIO`. What
changed underneath: `queryKeysRanged` (the `downloadAll` replacement) reports each surviving
key's **full cached object size** (`storeRangedSource.Size()`), not the (typically much smaller)
number of bytes `valueindex.QueryBucketFileRanged`'s `ReadAt` calls actually transferred. See
`SPECS.md` SPEC-VB-2 for the full rationale and the recorded candidate follow-up (a separate
ranged-wire-bytes stat, not attempted in #488).

## NOTE-VI-041: a not-found value-index file is a skipped miss, not a build failure (issue #399 point 5)

*Added: 2026-06-30*

`ErrFileNotFound` is the sentinel a `FileStore` returns (wrapped or bare) when the
requested value-index object does not exist — an S3 404 / NoSuchKey. `downloadAll`
recognizes it via `errors.Is` and **skips** that file instead of aborting the whole
build; every other read error still aborts (the caller falls back to a correct full
scan).

**Why skip is safe — and only for not-found.** The value-index compactor (issue
#399) writes its merged output then deletes the inputs; the querier's file-listing
cache (issue #462) can still name a key the compactor has just deleted. A genuinely
absent file holds zero postings, so dropping it cannot under-count results — it is
the last line of defense after the compactor cleans up stale source refs. A
transient error (network, auth, 5xx) is the opposite: the file may well hold
matching spans, so silently dropping it *would* under-count. The distinction is the
whole point — classify narrowly (404 only), abort on everything else.

**Wiring.** `blockpack.ErrValueIndexFileNotFound` re-exports the sentinel for tempo;
tempo's `minioVIStore.Size`/`ReadAt` map a minio `NoSuchKey`/`404` to it via
`mapNotFound` (minio's `GetObject` is lazy, so the 404 surfaces on the first read,
not the call — both paths run through `mapNotFound`). `io.EOF` and non-404 errors
pass through unchanged.

Back-ref: `builder.go` (ErrFileNotFound, downloadAll), `valueindex_query.go`
(ErrValueIndexFileNotFound), tempo `value_index_query.go` (mapNotFound).
Tests: `builder_test.go:TestBuildSource_NotFoundFileIsSkippedNotFailed`,
`TestBuildSource_AllFilesNotFoundIsCoveredEmpty`,
`TestBuildSource_NonNotFoundDownloadErrorStillFails`; tempo
`value_index_query_test.go:TestMapNotFound`.

**Addendum (2026-07-07, B-5, issue #488) — corrects stale back-ref, `downloadAll` deleted.**
`downloadAll` (named throughout the entry above) was deleted in B-5's ranged-read rewire — it
has zero remaining callers, confirmed via grep. The 404-skip classification this note documents
is unchanged in substance but now lives in `queryKeysRanged`: a per-key `ErrFileNotFound`,
surfaced through `valueindex.QueryBucketFileRanged`'s wrapped error chain (from either
`storeRangedSource.Size` or `.ReadAt`) and recognized via the same `errors.Is` check, is skipped
exactly as `downloadAll`'s was. **Current back-ref:** `builder.go:queryKeysRanged,
storeRangedSource` (replaces `downloadAll` above); see `vibuilder/SPECS.md` SPEC-VB-2 for the
up-to-date contract text. The `builder_test.go:TestBuildSource_*` tests listed above are
untouched by this rewire (they exercise `BuildSource`'s own leaf-loop behavior, which did not
change shape). `NOTE-VI-049`'s own addendum below covers the corresponding
`TestDownloadAll_*`→`TestQueryKeysRanged_*` test-name mapping for the lower-level tests.

## NOTE-VI-049: Bounded-concurrency downloads — downloadAll, BuildSource leaves, lookupColumnAll buckets (tempo timeout incident, issue #465)

*Added: 2026-07-02*

**Root cause.** `downloadAll`'s fully serial per-key loop issued 2 round trips per key
(`Size` then `ReadAt`), and the loops that call it (`BuildSource`'s per-leaf loop,
`lookupColumnAll`'s per-type-bucket loop) looped over columns/buckets on top of that — at
files=465 this produced ~930 sequential S3 calls for one query. At typical S3/minio per-call
latency this was confirmed as the root cause of 33s-2m38s TraceQL search timeouts on
the dev test cluster.

**Fix — bounded concurrency at three call sites**, all via `golang.org/x/sync/errgroup` with
`SetLimit`, mirroring the existing errgroup+SetLimit+pre-sized-slice pattern already used at
`reader.go:476-503` (NOTE-291) rather than inventing a new concurrency idiom:

- `downloadAll` (per-column file download): `errgroup.WithContext(context.Background())`,
  `g.SetLimit(downloadConcurrency)`, **`downloadConcurrency = 24`** — chosen conservatively
  within a brainstormed 16-32 range to bound peak S3/minio connections per column while
  still cutting the serial round-trip cost by roughly an order of magnitude. Each slot uses
  an explicit `keep bool` marker (`dlSlot{data, keep}`), not a nil-means-skip convention:
  `readWhole` legitimately returns `(nil, nil)` for a zero-byte object, so a nil-sentinel
  would have conflated a real empty file with a 404-skip and silently dropped a legitimate
  (if empty) file's presence from the result set. `keep` defaults false (zero value) and is
  only set true on a successful read, so a 404-skip and a real empty file are both
  representable and distinguishable in `slots`.
- `BuildSource`'s leaf-predicate loop: `errgroup.WithContext(ctx)`,
  `g.SetLimit(leafConcurrency)`, **`leafConcurrency = 4`** — kept smaller than
  `downloadConcurrency` because each leaf's own `downloadAll` can itself fan out up to
  `downloadConcurrency` downloads; worst-case simultaneous connections for one multi-leaf
  query is `leafConcurrency * downloadConcurrency` (96) — flagged as a value to revisit
  after load-testing, not made configurable in this fix. The loop passes the errgroup's own
  derived `gctx` (not the outer `ctx`) into `lookupColumn`, so `FileDiscoverer.FilesForTimeRange`
  observes cancellation from a sibling leaf's failure, not just from the caller's original
  context.
- `lookupColumnAll`'s per-type-bucket loop (match-all path): `errgroup.WithContext(ctx)`,
  `g.SetLimit(len(buckets))` where `buckets := allTypeBuckets()` is a **fixed 7-entry set**
  (String, Int64, Uint64, Float64, Bool, Bytes, UUID) — no separate concurrency constant
  needed since the loop's own cardinality is already small and fixed.

**Result ordering preserved despite concurrency.** `lookupColumnAll` writes each goroutine's
result into a pre-sized `slots[i]` (indexed by loop position, not completion order) and then
walks `slots` in original bucket order to pick `firstType` — "the first bucket in
`allTypeBuckets()` order with results" — so the returned `firstType` matches the original
serial implementation's semantics exactly, not whichever goroutine happened to finish first.
Pinned by `TestLookupColumnAll_PreservesFirstTypeOrdering`, run with `-count=20` to confirm
the ordering is stable across repeated runs rather than accidentally correct once.

**NOTE-VI-036/039/041 contracts confirmed unchanged.** Concurrency only changes I/O
*scheduling*, not the decision logic any of these three notes govern:

- **NOTE-VI-036 (coverage contract)** — a column's Add-with-empty-slice-is-coverage vs
  never-Added-is-fallback outcome depends only on whether a predicate was buildable and
  what `valueindex.QueryFiles`/`QueryBucketFiles` returned for that leaf, not on which
  goroutine ran when. Each leaf's `Add` call still carries exactly the leaf's own results,
  independent of the other leaves running concurrently.
- **NOTE-VI-039 (stats accumulation)** — `src.RecordFileIO(filesRead, bytesRead)` and
  `src.Add(...)` are called once per leaf goroutine with that leaf's own totals; `Stats()`'s
  Hits-derived-at-read-time design already tolerates any `Add` ordering, now safely so under
  true concurrency (see executor **NOTE-VI-048**, this fix's hard prerequisite —
  `SliceValueIndexSource` had to be made concurrency-safe (`sync.Mutex` guarding
  `Add`/`RecordFileIO`/`Stats`/`LookupResults`/`AllResults`) before this call site could
  safely run leaves in parallel; without it, concurrent `Add`/`RecordFileIO` calls would race
  on the unsynchronized map/counters).
- **NOTE-VI-041 (404-skip vs abort-on-real-error)** — `downloadAll`'s per-key classification
  (`errors.Is(err, ErrFileNotFound)` → skip; anything else → abort) is unchanged and runs
  identically inside each goroutine. The only new behavior is that a real error from one
  goroutine sets `gctx.Err()`, which not-yet-started sibling goroutines check and skip
  (`return nil //nolint:nilerr`) without themselves reporting a spurious second error — the
  goroutine that hit the real error is the one whose error `g.Wait()` returns.

**Cancellation is exact for not-yet-scheduled work, best-effort for in-flight work, and does
not reach `downloadAll` from the outer query context at all.** Two independent limitations
compound here, worth stating precisely rather than glossing as one "best-effort" caveat:

1. `FileStore.Size(key string) (int64, error)` and `ReadAt(key string, p []byte, off int64)
   (int, error)` take no `context.Context` parameter (confirmed by direct read of the
   interface, `builder.go`) — this is Approach 2 from the brainstorm and was explicitly kept
   out of scope for this fix. An in-flight `Size`/`ReadAt` call cannot itself be interrupted
   once started — only goroutines that haven't yet called `readWhole` observe `gctx.Err()`
   and skip. This affects `downloadAll`'s *own internal* first-error early-stop.
2. `downloadAll(store FileStore, keys []string) ([][]byte, int64, error)` takes **no
   `context.Context` parameter at all** and roots its own errgroup in
   `errgroup.WithContext(context.Background())` — confirmed by direct read; its callers
   (`lookupColumn`, `lookupColumnAll`) each hold a real `ctx`/`gctx` but do not pass it
   through to `downloadAll`. This means an *outer* cancellation (query timeout, client
   disconnect propagated via the top-level `ctx` into `BuildSource`) does not reach
   `downloadAll`'s download loop at all — only a real download error occurring inside
   `downloadAll` itself triggers its internal early-stop. Threading the outer context into
   `downloadAll` (and from there into a context-aware `FileStore`) would require changing the
   `FileStore` interface, a wider blast radius than the timeout this fix addresses.
   Flag this explicitly to future readers: an outer-cancelled query still runs every already
   in-flight and already-dispatched-but-not-yet-hit-an-error `downloadAll` download to
   completion before `BuildSource` can return.

Back-ref: `internal/modules/vibuilder/builder.go:downloadAll,BuildSource,lookupColumn,lookupColumnAll,allTypeBuckets`,
`reader.go:parseMatchingBlocks` (lines 476-503, NOTE-291 — the root-package `GetTraceByID`
errgroup+SetLimit+pre-sized-slice pattern this fix mirrors, not to be confused with the
similarly-named `internal/modules/blockio/reader` package)
[**Addendum (2026-07-04):** `reader.go:parseMatchingBlocks` was renamed to `parseBlocksWithWant`
during the trace-by-ID index wiring effort (root `SPEC.md` SPEC-ROOT-018). **Second correction
(2026-07-04, same day):** the renamed function's result-collection shape also changed since the
first addendum was written — it now returns a `map[int]*modules_reader.BlockWithBytes` guarded
by a `sync.Mutex` on each write, not a pre-sized result slice. The bounded-errgroup-fan-out part
of the pattern being referenced is still accurate; only the "pre-sized result slice" detail is
now stale],
`internal/modules/executor/metrics_trace.go:SliceValueIndexSource` (NOTE-VI-048, the
concurrency-safety prerequisite).
Tests: `builder_test.go:TestDownloadAll_BoundedConcurrencyScaling` (timing regression guard —
concurrent download wall-time stays bounded regardless of file count, unlike the old serial
cost), `TestDownloadAll_PreservesNotFoundSkipSemantics`,
`TestDownloadAll_NonNotFoundErrorAbortsAndReturnsError`,
`TestDownloadAll_LegitimatelyEmptyFileIsKeptDistinctFromSkippedFile` (the `keep`-bool
real-empty-vs-404-skip distinction above), `TestLookupColumnAll_PreservesFirstTypeOrdering`
(bucket-loop ordering invariant above, run at `-count=20`),
`TestBuildSource_SingleEqualityLeafResolves`, `TestBuildSource_ANDTwoLeavesBothResolved`
(leaf-loop correctness under concurrency), plus the full `TestBuildSource_*` suite re-run
under `-race` and `make precommit` (gocyclo <20 for all three rewritten functions) as part of
task #48's verification pass.

**Addendum (2026-07-07, B-5, issue #488) — corrects stale back-refs; regression-test coverage
was PORTED, not dropped.** `downloadAll` and `readWhole` (named throughout the entry above) were
deleted in B-5's ranged-read rewire — `queryKeysRanged` (calling
`valueindex.QueryBucketFileRanged` per key through a `storeRangedSource` adapter, `vibuilder/SPECS.md`
SPEC-VB-2) is the current mechanism this note's bounded-concurrency design describes.
`downloadConcurrency`/`leafConcurrency` and their numeric values (4/2, per NOTE-VI-050 below) are
completely unchanged by the rewire — only what each unit of bounded-concurrent work *does* per
key changed (a `ReadAt`-pruned partial fetch instead of a whole-file download).

The four `TestDownloadAll_*` regression tests this note's Tests: list names above were **ported
to their `queryKeysRanged` equivalents, not dropped** — coverage is preserved:

| Old (deleted) | New (current) |
|---|---|
| `TestDownloadAll_BoundedConcurrencyScaling` | `TestQueryKeysRanged_BoundedConcurrencyScaling` |
| `TestDownloadAll_PreservesNotFoundSkipSemantics` | `TestQueryKeysRanged_PreservesNotFoundSkipSemantics` |
| `TestDownloadAll_NonNotFoundErrorAbortsAndReturnsError` | `TestQueryKeysRanged_NonNotFoundErrorAbortsAndReturnsError` |
| `TestDownloadAll_LegitimatelyEmptyFileIsKeptDistinctFromSkippedFile` | `TestQueryKeysRanged_LegitimatelyEmptyFileIsKeptDistinctFromSkippedFile` |

`TestLookupColumnAll_PreservesFirstTypeOrdering` and the `TestBuildSource_*` tests this note also
names are untouched by B-5 — they exercise `lookupColumnAll`'s own bucket-loop ordering and
`BuildSource`'s leaf-loop behavior, neither of which changed shape, only what they call
internally. **Current back-ref:** `builder.go:queryKeysRanged,storeRangedSource,
downloadConcurrency,leafConcurrency` (replaces the `downloadAll`/`readWhole` mentions above).

## NOTE-VI-050: downloadConcurrency/leafConcurrency lowered from 24/4 to 4/2 — the bound is CPU-bound, not just connection-count-bound (follow-up to NOTE-VI-049)

*Added: 2026-07-02*

**What changed.** After NOTE-VI-049 shipped and was deployed to the dev test cluster, TraceQL
search queries stopped timing out on the serial-download bottleneck but continued to time out
on `context deadline exceeded`. A live CPU profile of the querier during a retest (Pyroscope,
`process_cpu`, 20-minute window covering the retest) showed **56% of all sampled CPU time
(166s of 294.5s total) inside `errgroup.(*Group).Go.func1`** — i.e. inside the goroutines this
fix's concurrency introduced — with the hot path being
`vibuilder.BuildSource`/`lookupColumn` → `valueindex.QueryBucketFiles` →
`valueindex.DecodeBucketFile`/`decodeBucketBlock` (snappy decompression, ~50s) →
`executor.viMatchSpans`/`viEvalNodes`/`viSortDedup` (sorting/dedup, ~80-90s across several
`slices.pdqsortCmpFunc`/`SortFunc` frames).

**Why this invalidates NOTE-VI-049's original sizing rationale.** `downloadConcurrency`/
`leafConcurrency` were chosen (16-32 / conservatively-smaller) purely to bound *S3/minio
connection pressure*, on the implicit assumption that `downloadAll`'s per-key work was I/O-wait
bound end to end. It isn't: `readWhole`'s two round trips are I/O-wait, but the caller
immediately decodes and sorts the returned bytes in the *same* goroutine before returning —
real, non-trivial CPU work. The querier runs under a **5-core CPU limit**
(the dev test cluster's `querier` Deployment). The original bound's worst case
(`leafConcurrency * downloadConcurrency` = 96 concurrent goroutines, most doing CPU work, not
waiting on network) oversubscribed 5 cores by roughly 19x. Individual per-block query-stats
logs showed sub-millisecond `total`/`plan_dur` even while the outer HTTP request timed out at
33-39s — consistent with each unit of work being fast in isolation but queued behind heavy Go
scheduler contention for 5 physical cores, not consistent with any single slow operation.

**Fix.** Lowered `downloadConcurrency` 24→4 and `leafConcurrency` 4→2 (worst case 8, roughly
1.6x the core count — small deliberate headroom for the genuinely I/O-wait portion of each
call, not 19x). `lookupColumnAll`'s bucket loop, which previously used
`g.SetLimit(len(buckets))` (7, unbounded relative to CPU work) now shares `leafConcurrency`
instead, since each bucket goroutine calls `downloadAll` internally and has the identical
`leafConcurrency * downloadConcurrency` worst-case shape as `BuildSource`'s leaf loop.

**Existing tests still hold without modification.**
`TestDownloadAll_BoundedConcurrencyScaling`'s `elapsed < 400ms` bound (N=30 keys, 15ms
simulated per-call latency) was derived from the *serial* baseline (~900ms) and a
generously-loose upper bound, not from `downloadConcurrency`'s specific value — at
`downloadConcurrency = 4`, expected wall time is `ceil(30/4) * 30ms = 240ms`, still comfortably
under 400ms. No test needed to change; only the two `const` values and their doc comments.

**Not yet resolved / explicitly out of scope for this note:** the correct fix long-term is
likely separating the genuinely-I/O-bound fetch (`readWhole`) from the CPU-bound
decode+sort+match step into two independently-sized worker pools — e.g. fetch at
connection-pool-appropriate concurrency (16-32), decode/sort at core-count-appropriate
concurrency (4-5) — rather than coupling both under one bound as this fix and NOTE-VI-049 both
do. That is a larger change (touches `downloadAll`'s and `lookupColumn`'s control flow, not
just constants) and was deliberately deferred in favor of this smaller, immediately-deployable
constant change. Flagged for a future NOTE-VI entry if pursued.

**Addendum (2026-07-07, B-5, issue #488):** this note's constants (`downloadConcurrency=4`,
`leafConcurrency=2`) and their rationale are unaffected by the ranged-read rewire — see
NOTE-VI-049's own B-5 addendum above for the corresponding `downloadAll`→`queryKeysRanged`
back-ref correction, which applies equally to this note's `readWhole`/`downloadAll` mentions.

Back-ref: `internal/modules/vibuilder/builder.go:downloadConcurrency,leafConcurrency,lookupColumnAll`.
No new tests required (see above); existing suite re-verified with
`go test -race ./internal/modules/vibuilder/... ./internal/modules/executor/...` after the
constant change.

## NOTE-VI-084: `lookupColumn`/`lookupColumnAll` rewired onto `QueryBucketFileRanged`; `storeRangedSource` adapter (issue #488, B-5)

*Added: 2026-07-07*

**What changed.** Both `lookupColumn` (single-column path) and `lookupColumnAll` (match-all path
— rewired alongside `lookupColumn` per a scope correction: the original issue text named only
`lookupColumn`, but `lookupColumnAll` also called `downloadAll` internally in its per-bucket
goroutine, and leaving it on the old whole-file path would have reintroduced exactly the I/O cost
#488 exists to remove, just for a different caller) no longer call `downloadAll` +
`valueindex.QueryBucketFiles` against fully-downloaded file bytes. Both now call a new shared
helper, `queryKeysRanged`, which runs `valueindex.QueryBucketFileRanged` per key through a new
`storeRangedSource{store, key}` adapter — a per-key wrapper satisfying `valueindex.RangedSource`
(`valueindex/SPECS.md` SPEC-VI-7) over vibuilder's existing `FileStore`. `downloadAll` and
`readWhole` are now fully orphaned (zero remaining callers, confirmed via grep) and have been
deleted, along with their dedicated tests (ported to `TestQueryKeysRanged_*` — see NOTE-VI-049's
addendum for the full mapping).

**Why a new adapter type instead of widening `FileStore` itself.** `valueindex.RangedSource` is
keyless (`Size()`/`ReadAt(p, off)`) because a single `BucketGroup` file's read operations don't
need to know which key they're reading — the file has already been identified by the time
`QueryBucketFileRanged` is called. `vibuilder.FileStore` is keyed (`Size(key)`/`ReadAt(key, p,
off)`) because it addresses a whole store, not one file. `storeRangedSource` bridges the two by
binding a single `(store, key)` pair at construction, so `valueindex` never needs to know
`vibuilder.FileStore` exists — preserving the same import-direction discipline NOTE-VI-036
established for why `vibuilder` exists as its own package in the first place (the one place
depending on `executor`, `valueindex`, and `vm` together, so neither of the other two needs to
depend on the third).

**Size-caching is load-bearing, not an optimization nicety.** `storeRangedSource.Size()` caches
its first successful result (`valueindex/SPECS.md` SPEC-VB-1) because both
`QueryBucketFileRanged`'s own footer-locating read and `queryKeysRanged`'s separate
`bytesRead` observability bookkeeping (NOTE-VI-039) call `Size()` on the same adapter instance;
without caching, an object-storage backend's `Size` (typically its own network round trip, e.g.
an S3 `HEAD`) would be paid twice per file, silently eating into the exact I/O savings this
rewire is meant to produce.

**What is unchanged, deliberately.** The 404-skip-not-abort classification (NOTE-VI-041), the
bounded-concurrency model and its constants (`downloadConcurrency=4`, `leafConcurrency=2`,
NOTE-VI-049/050 — `queryKeysRanged`'s fan-out bound and `lookupColumnAll`'s per-bucket loop bound
are numerically identical to `downloadAll`'s and its callers' before this rewire, just wrapping a
different unit of per-key work), and the coverage contract (NOTE-VI-036, unaffected since it
depends only on whether a predicate was buildable and what the query call returned, not on which
I/O path produced that return value). **`bytesRead`'s meaning is also deliberately unchanged
(SPEC-VB-2, NOTE-VI-039's own addendum) — it remains "total object size of every consulted
file," not "bytes actually transferred," so existing observability consumers (tempo's
`blockpackBlock.tryIndexFetch` span stamping, NOTE-VI-039) see no behavior change in what gets
reported. This will surprise anyone trying to measure #488's I/O-reduction win by watching this
counter — it will not move, even though the real wire-byte reduction is real and significant. The
actual reduction shows up only in store-side transfer metrics (e.g. S3/minio request/byte
counters), not in this application-level stat. A separate ranged-wire-bytes stat is a recorded
candidate follow-up, not attempted in #488 — see the issue's closing comment.**

**Verification.** `TestLookupColumn_ParityWithRangedPath` (`vibuilder/TESTS.md` TEST-VB-1) pins
`lookupColumn`'s exact output against a golden value recorded before this rewire (task B-1) and
re-passes unmodified after it (task B-6) — the strongest available evidence that this is a pure
I/O-path change, not a behavior change. Two new I/O-counting regression tests
(`vibuilder/TESTS.md` TEST-VB-2/3) assert the exact `ReadAt` ranges issued for a time-excluded
and a value-excluded-by-all-blocks file, respectively, proving the I/O reduction is real and not
just theoretically implied by calling a differently-named function.

Back-ref: `internal/modules/vibuilder/builder.go:lookupColumn,lookupColumnAll,queryKeysRanged,storeRangedSource`.
See `SPECS.md` SPEC-VB-1/2, `valueindex/NOTES.md` NOTE-VI-081 (the corresponding `valueindex`-side
design rationale for the ranged-read path this rewire consumes) and NOTE-VI-084's own forward
reference from there.
