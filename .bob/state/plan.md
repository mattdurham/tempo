# Implementation Plan: Bounded-Concurrency Fix for `vibuilder` Value-Index Downloads

*Created: 2026-07-02*
*Based on: brainstorm.md, section "Investigation: TraceQL search timeouts on
tempo-dev-test-03 — 'whole file vs. internal block' fetch hypothesis"*
*Repo: `/home/mdurham/source/blockpack_collection/blockpack` (source of truth).
Work happens directly on `main`. No new branches. No push. No PR without being
asked.*
*Vendor sync into tempo-mrd (`/home/mdurham/source/blockpack_collection/tempo`,
branch `agentic-tempo`) is a separate, later step — not part of this plan's
task list, since the constraints call for planning the blockpack-side fix
first.*

## Overview

`vibuilder.downloadAll` (`internal/modules/vibuilder/builder.go:412-440`)
downloads value-index files in a fully serial loop, each file costing two
sequential round trips (`Size` then `ReadAt`). With `files=465` typical per
query this produces up to ~930 sequential S3 calls and is the confirmed root
cause of 33s-2m38s TraceQL search timeouts on tempo-dev-test-03. This plan
replaces the serial loop with a bounded-concurrency fan-out
(`golang.org/x/sync/errgroup`, already a dependency), applies the same
treatment to `BuildSource`'s leaf loop and `lookupColumnAll`'s type-bucket
loop, and adds a mutex to `executor.SliceValueIndexSource` — which this
investigation confirms is **not** currently safe for concurrent writers —
as a required prerequisite.

## Investigation Finding (Step 1.5 / Open Question Resolution)

**Is `SliceValueIndexSource` safe for concurrent writers today? NO — confirmed by code read.**

Read `internal/modules/executor/metrics_trace.go:1207-1298`:

```go
type SliceValueIndexSource struct {
    data map[string]map[modules_shared.ColumnType][]VILookupResult
    stats ValueIndexBuildStats
}

func (s *SliceValueIndexSource) RecordFileIO(filesRead int, bytesRead int64) {
    s.stats.FilesRead += filesRead   // unsynchronized read-modify-write
    s.stats.BytesRead += bytesRead   // unsynchronized read-modify-write
}

func (s *SliceValueIndexSource) Add(colName string, colType modules_shared.ColumnType, results []VILookupResult) {
    byType, ok := s.data[colName]    // unsynchronized map read
    if !ok {
        byType = make(map[modules_shared.ColumnType][]VILookupResult)
        s.data[colName] = byType     // unsynchronized map write
    }
    byType[colType] = append(byType[colType], results...)
}
```

Both `data` (a plain Go map) and `stats` (plain int/int64 fields with `+=`)
have zero synchronization. Concurrent `Add`/`RecordFileIO` calls from
parallel leaf goroutines would race on the map (a real `fatal error:
concurrent map writes` risk, not just a benign data race) and silently lose
counter updates. **A mutex must be added to `SliceValueIndexSource` before
any leaf-loop parallelization lands** (Task 2 below is a hard prerequisite
for Tasks 4-5).

`executor/` is a **mature spec-driven module** — it has `SPECS.md`,
`NOTES.md`, `TESTS.md`, and `BENCHMARKS.md` (confirmed via
`internal/modules/executor/*.md`). The mutex fix must go through the full
spec-doc workflow for this module (see "Spec-Driven Module Updates" below),
not just a NOTES.md entry.

## Spec-Driven Modules in Scope

| Module | Docs present | Treatment |
|---|---|---|
| `internal/modules/executor/` | `SPECS.md`, `NOTES.md`, `TESTS.md`, `BENCHMARKS.md` (mature) | Full spec-doc update required for the `SliceValueIndexSource` mutex change |
| `internal/modules/vibuilder/` | `NOTES.md` only (brand-new, added 2026-06-30) | NOTES.md dated entry only — see "Open Question: vibuilder doc maturity" below |

**Do not read SPECS.md/NOTES.md/TESTS.md/BENCHMARKS.md directly** — per this
repo's CLAUDE.md, spawn/consult the persistent `spec-oracle` agent (or use
`blockpack_search_modules` / `blockpack_lookup_requirement`) for all
spec/note/test/benchmark reads and writes during execution. This plan's IDs
(SPEC-/NOTE-/TEST- numbers) below are **placeholders based on the highest
existing IDs found in this planning pass** (`NOTE-VI-047` was the highest
global `NOTE-VI-*` ID found repo-wide at plan time) — the coder MUST re-verify
the next free ID via the spec-oracle/MCP tool immediately before writing any
spec entry, since other concurrent work may have advanced the sequence.

## Open Question: vibuilder doc maturity (flagged, not resolved)

The task brief asks whether `vibuilder`'s sibling modules' convention implies
it should grow a full `SPECS.md`/`TESTS.md`/`BENCHMARKS.md` suite. Findings:

- `internal/modules/valueindexcompactor/` and
  `internal/modules/valuecountscompactor/` (standalone, independently
  deployable compactor services) both have the **full four-doc suite**.
- `internal/modules/valueindex/` — vibuilder's closest architectural sibling
  and direct dependency (predicates, `QueryBucketFiles`, `IndexFileCache`) —
  has **only `NOTES.md`**, same as vibuilder. `internal/modules/valueindex/`
  does NOT have SPECS.md/TESTS.md/BENCHMARKS.md either (confirmed via glob).

This is a mixed convention: standalone compactor *services* get the full
suite; internal orchestration/library packages consumed by the querier
(`valueindex`, `vibuilder`) currently get NOTES.md only. Given `vibuilder`'s
closest sibling (`valueindex`) has not graduated to a full suite, **this plan
does not add SPECS.md/TESTS.md/BENCHMARKS.md to vibuilder** — it adds a dated
NOTES.md entry only, consistent with `valueindex`'s current maturity level.
**This remains an open question for a human/maintainer decision, not an
assumption to build on long-term** — if `valueindex` graduates to a full
suite in the future, `vibuilder` likely should too.

## Files to Modify

1. `internal/modules/executor/metrics_trace.go` — add `sync.Mutex` to
   `SliceValueIndexSource`; guard `Add`, `RecordFileIO`, `Stats`,
   `LookupResults`, `AllResults`.
2. `internal/modules/executor/metrics_trace_vi_test.go` — add a
   `-race`-covered concurrent-access test.
3. `internal/modules/executor/SPECS.md`, `NOTES.md`, `TESTS.md` — spec-doc
   updates for the concurrency-safety invariant (via spec-oracle).
4. `internal/modules/vibuilder/builder.go` — bounded-concurrency rewrite of
   `downloadAll`, `BuildSource`'s leaf loop, `lookupColumnAll`'s bucket loop;
   new concurrency-bound constants; new imports
   (`golang.org/x/sync/errgroup`, `time` only if needed for tests).
5. `internal/modules/vibuilder/builder_test.go` — regression/scaling test
   (fake latency store), concurrency-preservation tests for leaf loop and
   bucket loop ordering, updated/added `-race` coverage.
6. `internal/modules/vibuilder/NOTES.md` — new dated entry documenting the
   concurrency fix and chosen bounds.

No new files are required.

## Implementation Steps

### Phase 1: Tests First (TDD)

**Step 1.1: Read current `SliceValueIndexSource` and `builder_test.go` fakes (already done in planning; coder re-confirms)**
- [ ] Re-open `internal/modules/executor/metrics_trace.go:1207-1298` and
  `internal/modules/vibuilder/builder_test.go` to confirm line numbers
  haven't shifted since this plan was written.

**Step 1.2: Write the concurrency-safety race test for `SliceValueIndexSource` FIRST**
- [ ] In `internal/modules/executor/metrics_trace_vi_test.go`, add
  `TestSliceValueIndexSource_ConcurrentAddAndRecordFileIO_NoRace`:
  spawn ~50 goroutines, each calling `src.Add(col, colType, results)` and
  `src.RecordFileIO(1, 100)` on a shared `*SliceValueIndexSource` (mix of a
  few distinct column names so map-write contention is exercised, not just
  counter contention), then call `src.Stats()` and `src.AllResults()` after
  `wg.Wait()` and assert the totals equal `numGoroutines * perGoroutineAdds`.
- [ ] Run `go test -race -run TestSliceValueIndexSource_ConcurrentAddAndRecordFileIO_NoRace ./internal/modules/executor/...`
  and confirm it **fails** (`fatal error: concurrent map writes` or a race
  report) against the current unmodified code. This is the required
  "verify tests fail first" step — if it doesn't fail, the test isn't
  actually exercising concurrent access; fix the test before proceeding.

**Step 1.3: Write the `downloadAll` serial-timing regression test FIRST**
- [ ] In `internal/modules/vibuilder/builder_test.go`, extend `fakeStore`
  with an injected-latency wrapper (do not modify `fakeStore` itself if
  other tests share it — add a new `latencyStore` type wrapping a
  `*fakeStore`):
  ```go
  type latencyStore struct {
      inner   *fakeStore
      latency time.Duration
  }
  func (s *latencyStore) Size(key string) (int64, error) {
      time.Sleep(s.latency)
      return s.inner.Size(key)
  }
  func (s *latencyStore) ReadAt(key string, p []byte, off int64) (int, error) {
      time.Sleep(s.latency)
      return s.inner.ReadAt(key, p, off)
  }
  ```
- [ ] Add `TestDownloadAll_BoundedConcurrencyScaling`: build N=30 small
  value-index-shaped byte slices (content doesn't need to parse — `downloadAll`
  only reads bytes, it doesn't call `valueindex.QueryBucketFiles`), latency
  15ms per call (2 calls/key = 30ms/key serial). Call `downloadAll(store,
  keys)`, measure wall-clock via `time.Now()`/`time.Since`. Assert:
  - `elapsed < 400ms` (generous upper bound: with `downloadConcurrency=24`,
    expected ~2 batches × 30ms = 60ms; 400ms gives large CI slack while still
    being far below the ~900ms serial cost).
  - Also assert `err == nil`, `len(files) == 30`, `totalBytes` matches
    expected sum — this doubles as a correctness check, not just timing.
- [ ] Run this test against the **current, unmodified** `downloadAll`.
  Confirm it **fails** (serial cost ≈ 30 × 30ms = 900ms, exceeding the 400ms
  bound). Record the actual observed serial duration in the PR description
  later as evidence.

**Step 1.4: Write ordering/correctness tests for the concurrent rewrites FIRST**

These assert behavior that only matters once the loops are parallel — write
them now so Phase 2 has a clear target, and confirm they compile (they will
fail to compile or fail at runtime against the pre-fix serial code only in
the sense that the *behavior* they check doesn't yet risk breaking — these
tests mostly need to pass both before and after the fix since they test
*semantics*, not timing; write them now so no invariant gets lost during the
rewrite):

- [ ] `TestDownloadAll_PreservesNotFoundSkipSemantics` (if not already
  covered by existing `TestBuildSource_NotFoundFileIsSkippedNotFailed` /
  `TestBuildSource_AllFilesNotFoundIsCoveredEmpty` at the `BuildSource`
  level — check first; add a `downloadAll`-level unit test only if those
  don't already pin the behavior at this lower layer). Verify a 404 among
  several keys is dropped from the result while non-404 keys' bytes/order
  are otherwise preserved.
- [ ] `TestDownloadAll_NonNotFoundErrorAbortsAndReturnsError` — same as
  `TestBuildSource_NonNotFoundDownloadErrorStillFails` but exercised
  directly against `downloadAll` with several keys where one (not
  necessarily the first) errors with a non-404 error; assert `downloadAll`
  returns a non-nil error and does not partially apply.
- [ ] `TestDownloadAll_LegitimatelyEmptyFileIsKeptDistinctFromSkippedFile` —
  **important edge case found during planning**: `readWhole` returns
  `(nil, nil)` both for a zero-byte object (`store.Size(key) <= 0`) AND
  conceptually could be confused with a "skipped" slot if the concurrent
  rewrite uses a naive `[]byte` sentinel. Build a `fakeStore` with one
  legitimately-zero-length file and one 404 file among several keys; assert
  `len(files)` returned by `downloadAll` equals `(number of keys) - (number
  of 404s)`, i.e., the empty-but-present file counts as a slot and the 404
  does not. This test only passes if the rewrite uses an explicit
  `keep bool` (or equivalent) per-slot marker rather than a `nil`-means-skip
  convention (see Task 4 design below).
- [ ] `TestLookupColumnAll_PreservesFirstTypeOrdering` — construct a column
  with files present under two different type buckets (e.g. both
  `ColumnTypeString` and `ColumnTypeInt64`) such that, under the *original*
  serial iteration order (`allTypeBuckets()`: String, Int64, Uint64,
  Float64, Bool, Bytes, UUID), `ColumnTypeString` is the first bucket with
  results. Assert the parallel rewrite still returns `firstType ==
  ColumnTypeString` deterministically (run the test with `-count=20` in CI
  or locally to catch nondeterminism from a naive "first goroutine to
  finish wins" implementation).

**Step 1.5: Run `go test ./internal/modules/vibuilder/... ./internal/modules/executor/...` and confirm the new tests fail/compile-fail as expected, old tests still pass**
- [ ] `go test ./internal/modules/vibuilder/...` — new timing test fails,
  everything else passes.
- [ ] `go test -race ./internal/modules/executor/...` — new race test fails
  (or panics), everything else passes.

### Phase 2: Implementation

**Step 2.1: Add mutex to `SliceValueIndexSource` (prerequisite for Tasks 4-5)**
- [ ] In `internal/modules/executor/metrics_trace.go`, add a `sync.Mutex`
  field to `SliceValueIndexSource` (the `sync` package is already imported
  in this file for `compositeKeyScratchPool`, so no new import needed):
  ```go
  type SliceValueIndexSource struct {
      mu   sync.Mutex
      data map[string]map[modules_shared.ColumnType][]VILookupResult
      stats ValueIndexBuildStats
  }
  ```
- [ ] Guard `RecordFileIO` with `s.mu.Lock(); defer s.mu.Unlock()`.
- [ ] Guard `Add` with `s.mu.Lock(); defer s.mu.Unlock()`.
- [ ] Guard `Stats` (reads `s.stats` and iterates `s.data`) with
  `s.mu.Lock(); defer s.mu.Unlock()`.
- [ ] Guard `LookupResults` (reads `s.data`) with the same pattern.
- [ ] Guard `AllResults` (reads `s.data`) with the same pattern.
- [ ] Keep `NewSliceValueIndexSource` unchanged (zero-value mutex is ready
  to use).
- [ ] Run `go vet ./internal/modules/executor/...` to confirm no
  copy-of-mutex issues (the type is always used via pointer receiver
  already, per the existing method set — confirm no code anywhere copies a
  `SliceValueIndexSource` by value; `grep -rn "SliceValueIndexSource{" .`
  outside the constructor).

**Step 2.2: Verify the race test now passes**
- [ ] `go test -race -run TestSliceValueIndexSource_ConcurrentAddAndRecordFileIO_NoRace ./internal/modules/executor/...`
  — must pass cleanly with no race report.
- [ ] `go test -race ./internal/modules/executor/...` — full package race
  run, confirm no new races introduced elsewhere.

**Step 2.3: Add concurrency-bound constants to `builder.go`**
- [ ] Near the top of `internal/modules/vibuilder/builder.go` (after the
  `ErrFileNotFound` var block), add:
  ```go
  // downloadConcurrency bounds how many value-index files downloadAll fetches
  // in parallel for one column's file set (issue #<TBD — file/find the
  // tracking issue number before landing>). Chosen conservatively within the
  // brainstormed 16-32 range to bound peak S3/minio connections per column
  // download while still cutting the ~465-serial-round-trip cost by an order
  // of magnitude.
  const downloadConcurrency = 24

  // leafConcurrency bounds how many predicate leaf columns BuildSource
  // downloads in parallel. Kept smaller than downloadConcurrency because each
  // leaf's own downloadAll can itself fan out up to downloadConcurrency
  // downloads — worst case simultaneous connections for one query is
  // leafConcurrency * downloadConcurrency, so this stays conservative
  // (multi-leaf queries are the less common case; 1-2 leaves is typical).
  const leafConcurrency = 4
  ```
  (`lookupColumnAll`'s bucket loop has a small, fixed 7-entry bucket list —
  bound it with `len(buckets)`, i.e. effectively unbounded within that small
  set; no separate constant needed, see Step 2.6.)
- [ ] Add the import: `"golang.org/x/sync/errgroup"` (already a module
  dependency per `go.mod:22`, no `go mod tidy` needed).

**Step 2.4: Rewrite `downloadAll` with bounded concurrency, preserving order and skip/abort semantics**
- [ ] Replace the body of `downloadAll` (`builder.go:412-440`) with:
  ```go
  func downloadAll(store FileStore, keys []string) ([][]byte, int64, error) {
      if len(keys) == 0 {
          return nil, 0, nil
      }
      type dlSlot struct {
          data []byte
          keep bool // false for a skipped (404) key; zero-value default
      }
      slots := make([]dlSlot, len(keys))
      g, gctx := errgroup.WithContext(context.Background())
      g.SetLimit(downloadConcurrency)
      for i, key := range keys {
          i, key := i, key
          g.Go(func() error {
              if gctx.Err() != nil {
                  // Another key already hit a real (non-404) error; do not
                  // start new work, but do not report a spurious error either
                  // — the goroutine that found the real error reports it.
                  return nil
              }
              data, err := readWhole(store, key)
              if err != nil {
                  if errors.Is(err, ErrFileNotFound) {
                      return nil // skip: slots[i] stays keep=false
                  }
                  return fmt.Errorf("vibuilder: download %s: %w", key, err)
              }
              slots[i] = dlSlot{data: data, keep: true}
              return nil
          })
      }
      if err := g.Wait(); err != nil {
          return nil, 0, err
      }
      files := make([][]byte, 0, len(keys))
      var totalBytes int64
      for _, s := range slots {
          if !s.keep {
              continue
          }
          files = append(files, s.data)
          totalBytes += int64(len(s.data))
      }
      return files, totalBytes, nil
  }
  ```
  **Design notes for the coder:**
  - `dlSlot.keep` (not `data != nil`) is the skip/keep discriminator — this
    is required because `readWhole` legitimately returns `(nil, nil)` for a
    zero-byte object (`builder.go:450-452`), which must still be counted as
    a present (empty) file, distinct from a 404-skipped key. Using `data !=
    nil` as the sentinel would silently drop legitimately-empty files —
    this was caught during planning (see Step 1.4's dedicated test) and
    must not regress.
  - `errgroup.WithContext` gives best-effort early-exit: once a real error
    is found, goroutines not yet scheduled by `SetLimit`'s semaphore skip
    their work via the `gctx.Err() != nil` check. Goroutines already
    in-flight when the error occurs still complete their (wasted) I/O —
    this is an acceptable, bounded cost (at most `downloadConcurrency`
    extra in-flight calls), not a correctness issue, since `FileStore.Size`/
    `ReadAt` have no context parameter to cancel the underlying I/O itself
    (changing that interface is explicitly out of scope — see "Out of
    Scope" below).
  - The reduction loop after `g.Wait()` runs single-threaded (no goroutines
    active), so it is race-free by construction — no atomics needed for
    `totalBytes`.

**Step 2.5: Rewrite `BuildSource`'s leaf loop with bounded concurrency**
- [ ] Replace the leaf loop in `BuildSource` (`builder.go:115-137`). Keep
  the fast, non-I/O `buildPredicate` call synchronous (it's pure CPU, no
  benefit to parallelizing, and keeping it serial simplifies the "which
  leaves are unindexable" bookkeeping):
  ```go
  leaves := collectLeaves(preds.Nodes)
  type leafWork struct {
      col     string
      colType modules_shared.ColumnType
      pred    valueindex.Predicate
  }
  var work []leafWork
  for i := range leaves {
      pred, colType, ok := buildPredicate(&leaves[i])
      if !ok {
          continue
      }
      work = append(work, leafWork{col: leaves[i].col, colType: colType, pred: pred})
  }
  if len(work) > 0 {
      g, gctx := errgroup.WithContext(ctx)
      g.SetLimit(leafConcurrency)
      for _, w := range work {
          w := w
          g.Go(func() error {
              results, filesRead, bytesRead, err := lookupColumn(gctx, disc, store, w.col, w.colType, w.pred, timeRange)
              if err != nil {
                  return err
              }
              // Record the download I/O for this leaf so the querier can report
              // it on its OTel span (issue #465); a covered-but-empty column
              // still counts the bytes of any files we read deciding it was
              // empty. src is now mutex-protected (Task 2.1) — safe from
              // concurrent leaves.
              src.RecordFileIO(filesRead, bytesRead)
              // Add even when empty: a covered-but-empty column is coverage,
              // not fallback (NOTE-VI-033).
              src.Add(w.col, w.colType, results)
              return nil
          })
      }
      if err := g.Wait(); err != nil {
          return nil, false, err
      }
      added = true
  }
  ```
  - Note `lookupColumn` is called with `gctx` (the errgroup's derived
    context) instead of the original `ctx` — this lets `disc.FilesForTimeRange`
    (which already accepts a `context.Context`) observe cancellation once a
    sibling leaf fails, giving real (not just best-effort) early-exit for
    the discovery half of a not-yet-started leaf. This is a strict
    improvement over `downloadAll`'s best-effort-only cancellation, since
    `FileDiscoverer.FilesForTimeRange` already takes a context (no interface
    change needed here).
  - Delete the old sequential `for i := range leaves { ... }` block and the
    standalone `added := false` / `added = true` line that preceded it if
    now redundant — keep `added` declared once before this block, defaulting
    `false`, exactly as today, just set from the new block.

**Step 2.6: Rewrite `lookupColumnAll`'s bucket loop with bounded concurrency, preserving `firstType` determinism**
- [ ] Replace the body of `lookupColumnAll` (`builder.go:356-395`):
  ```go
  func lookupColumnAll(
      ctx context.Context,
      disc FileDiscoverer,
      store FileStore,
      col string,
      timeRange *[2]uint64,
  ) ([]modules_executor.VILookupResult, modules_shared.ColumnType, int, int64, error) {
      colHash := valueindex.ColHash(col)
      buckets := allTypeBuckets()
      type bucketResult struct {
          results   []modules_executor.VILookupResult
          filesRead int
          bytesRead int64
          hasResults bool
      }
      slots := make([]bucketResult, len(buckets))
      g, gctx := errgroup.WithContext(ctx)
      g.SetLimit(len(buckets)) // small fixed set (7); no separate constant needed
      for i, colType := range buckets {
          i, colType := i, colType
          g.Go(func() error {
              colTypeName := valueindex.ColTypeName(colType)
              keys, err := disc.FilesForTimeRange(gctx, colHash, colTypeName, timeRange[0], timeRange[1])
              if err != nil {
                  return fmt.Errorf("vibuilder: discover-all %s: %w", col, err)
              }
              if len(keys) == 0 {
                  return nil
              }
              files, bytesRead, err := downloadAll(store, keys)
              if err != nil {
                  return err
              }
              // A nil predicate matches every entry (Reader.Lookup treats nil as
              // match-all), so the universe of indexed spans for this column is
              // returned.
              lrs, err := valueindex.QueryBucketFiles(nil, timeRange, files...)
              if err != nil {
                  return fmt.Errorf("vibuilder: query-all %s: %w", col, err)
              }
              slots[i] = bucketResult{
                  results:    toVILookupResults(lrs),
                  filesRead:  len(files),
                  bytesRead:  bytesRead,
                  hasResults: len(lrs) > 0,
              }
              return nil
          })
      }
      if err := g.Wait(); err != nil {
          return nil, 0, 0, 0, err
      }
      var all []modules_executor.VILookupResult
      var firstType modules_shared.ColumnType
      var totalFiles int
      var totalBytes int64
      firstSet := false
      for i, s := range slots {
          totalFiles += s.filesRead
          totalBytes += s.bytesRead
          if !s.hasResults {
              continue
          }
          if !firstSet {
              firstType = buckets[i] // deterministic: first bucket in allTypeBuckets() order with results, matching original serial semantics exactly
              firstSet = true
          }
          all = append(all, s.results...)
      }
      return all, firstType, totalFiles, totalBytes, nil
  }
  ```
  - **Critical semantic preserved:** `firstType` selection is based on
    `buckets` iteration order (`i` index into the pre-sized `slots` array),
    NOT on goroutine completion order — this is what
    `TestLookupColumnAll_PreservesFirstTypeOrdering` (Step 1.4) pins.

**Step 2.7: Update `lookupColumn`'s call sites for the new `gctx` threading (if any signature mismatch)**
- [ ] Confirm `lookupColumn`'s signature (`builder.go:323-331`) already
  accepts `ctx context.Context` as its first parameter — it does; no
  signature change needed, only the caller (Step 2.5) now passes `gctx`
  instead of the outer `ctx`.

### Phase 3: Verification

**Step 3.1: Run all new and existing tests**
- [ ] `go test ./internal/modules/vibuilder/...` — all tests pass, including
  the timing test (Step 1.3, now within bound) and ordering tests (Step 1.4).
- [ ] `go test -race ./internal/modules/vibuilder/... ./internal/modules/executor/...`
  — no race reports.
- [ ] `go test -race -count=20 -run TestLookupColumnAll_PreservesFirstTypeOrdering ./internal/modules/vibuilder/...`
  — repeat run to catch nondeterminism from the reduction logic.
- [ ] Full existing `vibuilder` suite (`builder_test.go`'s pre-existing
  tests: `TestBuildSource_SingleEqualityLeafResolves`,
  `TestBuildSource_ANDTwoLeavesBothResolved`,
  `TestBuildSource_NotFoundFileIsSkippedNotFailed`,
  `TestBuildSource_NonNotFoundDownloadErrorStillFails`,
  `TestBuildSource_StatsRecordFileIO`,
  `TestBuildSource_StatsCountFilesEvenWhenEmpty`, etc.) must all still pass
  unmodified — these pin the exact coverage/abort/skip contract this plan
  must not regress.

**Step 3.2: Code quality**
- [ ] `make precommit` (from `blockpack/` root) — runs gofumpt, golines,
  golangci-lint (gocritic, gocyclo, staticcheck, revive), betteralign,
  nilaway, build, tests, deadcode, staticcheck. Must pass with zero
  tolerance per this repo's CLAUDE.md.
- [ ] Confirm cyclomatic complexity stays `< 30` for the rewritten
  `downloadAll`, `BuildSource`, and `lookupColumnAll` (gocyclo is part of
  `make precommit`; the errgroup rewrites are flatter than the original
  serial loops, so complexity should stay the same or drop, but verify).
- [ ] `blockpack_precommit_checklist` MCP tool as a secondary confirmation.

**Step 3.3: Manual/log-level sanity check (optional, not blocking)**
- [ ] If a local minio/S3-backed integration environment is available,
  re-run the `builder_test.go` fake-store scaling test with a higher
  latency (e.g. 80ms, matching the brainstorm's observed S3 round-trip
  latency) and N=465 to sanity-check the projected wall-clock improvement
  (~930 round trips × 80ms serial ≈ 74s → ~40 batches at
  `downloadConcurrency=24` ≈ 3.2s). This is illustrative, not a required
  CI test (465 × 80ms would make the *serial* baseline assertion too slow
  for routine CI; keep the checked-in test at the smaller N=30/15ms scale
  from Step 1.3).

## Spec-Driven Verification Tests

### Module: `internal/modules/executor/`

**Source:** Code-read invariant (Investigation Finding above) — `SliceValueIndexSource` must be safe under concurrent `Add`/`RecordFileIO` for the vibuilder leaf-loop parallelization to be correct.

| Invariant | Test to Verify | Test File |
|---|---|---|
| `Add`/`RecordFileIO` are safe for concurrent callers | `TestSliceValueIndexSource_ConcurrentAddAndRecordFileIO_NoRace` | `internal/modules/executor/metrics_trace_vi_test.go` |
| `Stats()`/`AllResults()`/`LookupResults()` return correct totals after concurrent writes | Same test, assertions after `wg.Wait()` | same file |

### Module: `internal/modules/vibuilder/`

**Source:** NOTES.md invariants NOTE-VI-036 (coverage contract), NOTE-VI-039 (stats accumulate regardless of Add ordering), NOTE-VI-041 (404-skip vs abort-on-real-error).

| Invariant | Test to Verify | Test File |
|---|---|---|
| A non-404 download error still aborts the whole build | `TestDownloadAll_NonNotFoundErrorAbortsAndReturnsError` (new, `downloadAll`-level) + existing `TestBuildSource_NonNotFoundDownloadErrorStillFails` (must still pass unmodified) | `builder_test.go` |
| A 404 file is skipped, not a build failure | `TestDownloadAll_PreservesNotFoundSkipSemantics` (new, if not already pinned at this layer) + existing `TestBuildSource_NotFoundFileIsSkippedNotFailed` (must still pass unmodified) | `builder_test.go` |
| A legitimately-empty (zero-byte) file is kept, not confused with a skipped 404 | `TestDownloadAll_LegitimatelyEmptyFileIsKeptDistinctFromSkippedFile` (new) | `builder_test.go` |
| `lookupColumnAll`'s `firstType` matches the deterministic bucket-order semantics of the original serial loop | `TestLookupColumnAll_PreservesFirstTypeOrdering` (new) | `builder_test.go` |
| Stats accumulate correctly regardless of leaf completion order (NOTE-VI-039) | Existing `TestBuildSource_StatsRecordFileIO`, `TestBuildSource_StatsCountFilesEvenWhenEmpty` (must still pass unmodified after leaf-loop parallelization) | `builder_test.go` |

These tests MUST be written first (Phase 1) and MUST pass after implementation (Phase 2/3).

## Spec-Driven Module Updates

### Module: `internal/modules/executor/` (full suite: SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md)

**Required updates (route through spec-oracle / MCP tools, do not hand-edit blind):**
- [ ] Add a `SPECS.md` entry documenting the new invariant: "`SliceValueIndexSource`'s `Add`, `RecordFileIO`, `Stats`, `LookupResults`, and `AllResults` are safe for concurrent callers (mutex-protected)." Assign the next free `SPEC-` ID via `blockpack_lookup_requirement`/spec-oracle — do not invent one (highest ID observed in this planning pass in this file was `SPEC-005`, but re-verify).
- [ ] Add a dated `NOTES.md` entry (next free `NOTE-VI-` ID, `NOTE-VI-048` as a placeholder pending re-verification — highest ID found repo-wide at plan time was `NOTE-VI-047`) explaining *why*: vibuilder's leaf-loop parallelization (this fix) requires the source to tolerate concurrent writers; document that the mutex adds negligible overhead relative to the I/O it guards.
- [ ] Update `TESTS.md` with the new test's scenario/setup/assertions:
  `TestSliceValueIndexSource_ConcurrentAddAndRecordFileIO_NoRace` — goroutine
  count, what's asserted, why `-race` is required to catch a regression.
- [ ] `BENCHMARKS.md`: no update needed — this is a correctness fix, not a
  performance-sensitive hot path change at the `executor` layer (the mutex
  is held only briefly per `Add`/`RecordFileIO` call, not per-span).

### Module: `internal/modules/vibuilder/` (NOTES.md only, per the Open Question above)

**Required updates:**
- [ ] Add a new dated `NOTES.md` entry (`NOTE-VI-04X`, next free ID after the
  executor one above — verify via spec-oracle so the two new entries don't
  collide) titled something like "Bounded-concurrency downloads: `downloadAll`,
  `BuildSource` leaves, `lookupColumnAll` buckets (tempo timeout incident)".
  Content must cover:
  - The root cause (serial downloads, ~930 round trips at `files=465`).
  - The chosen bounds: `downloadConcurrency=24`, `leafConcurrency=4`, bucket
    loop bounded by its fixed 7-entry set.
  - Explicit confirmation that NOTE-VI-036/039/041's contracts (coverage
    semantics, stats accumulation, 404-skip-vs-abort) are unchanged —
    concurrency only changes I/O scheduling, not decision logic.
  - The `SliceValueIndexSource` mutex prerequisite and a back-reference to
    the `executor/NOTES.md` entry above.
  - A note that `FileStore.Size`/`ReadAt` still lack context parameters
    (Approach 2, out of scope here) so cancellation on error is best-effort
    for in-flight downloads, exact for not-yet-scheduled ones.

## Edge Cases to Handle

### Edge Case 1: Legitimately empty (zero-byte) value-index file vs. a skipped 404
**Scenario:** `readWhole` returns `(nil, nil)` for a real zero-byte object,
which is bitwise indistinguishable from a naive "skip" sentinel.
**Expected:** The empty file counts as a present, empty slot in `downloadAll`'s
output; a 404 does not appear at all.
**Test:** `TestDownloadAll_LegitimatelyEmptyFileIsKeptDistinctFromSkippedFile` (Step 1.4).

### Edge Case 2: Two leaves resolving the same (column, type) pair concurrently
**Scenario:** A query with duplicate leaf predicates on the same column
(per `collectLeaves`'s comment, this is possible and intentional — each
leaf keeps its own predicate). Under the parallel leaf loop, two goroutines
could call `src.Add(sameCol, sameType, ...)` concurrently.
**Expected:** Both calls succeed without data loss (mutex serializes the
`append`); result *order* across the two leaves' appended slices may differ
run-to-run, which is acceptable because downstream consumers treat this as
an unordered coverage set, not an ordered list (confirmed by `AllResults`'s
map-based dedup and `LookupResults`'s flatten-all-types behavior, neither of
which depends on append order for correctness).
**Test:** Covered incidentally by `TestBuildSource_ANDTwoLeavesBothResolved`
continuing to pass; add an explicit duplicate-column case only if that
existing test doesn't already exercise it — check before adding a new test
to avoid duplication.

### Edge Case 3: All leaves fail to build a predicate (nothing to parallelize)
**Scenario:** Every leaf's `buildPredicate` returns `ok=false` (e.g. all
vector predicates).
**Expected:** `work` stays empty, the `errgroup` block is skipped entirely
(`len(work) > 0` guard), `added` stays `false`, behavior identical to today.
**Test:** Existing `TestBuildSource_VectorPredicateLeavesColumnUncovered` —
must still pass unmodified.

### Edge Case 4: `keys` slice is empty for `downloadAll`
**Scenario:** A column's discovery returns zero files.
**Expected:** Early return `(nil, 0, nil)` before spawning any errgroup —
unchanged from the current code's first two lines.
**Test:** Existing `TestBuildSource_NoFilesStillCovered` — must still pass.

### Edge Case 5: Context cancellation from the caller (querier-side deadline)
**Scenario:** The outer `ctx` passed into `BuildSource` is canceled/expires
mid-flight (this is, after all, the exact production scenario — context
deadline exceeded).
**Expected:** `disc.FilesForTimeRange` calls (which do accept `ctx`) will
observe cancellation and return promptly; `downloadAll`'s `FileStore.Size`/
`ReadAt` calls do NOT observe the outer `ctx` (interface has no context
param — unchanged, out of scope). This plan does not change this behavior;
it only reduces total round-trip count so the deadline is far less likely
to be hit in practice.
**Test:** Not newly tested here (would require an integration-level test
with a real slow store); flagged as expected residual risk, not a gap this
plan must close (Approach 2's `FileStore` context-plumbing would be needed
for full cancellation, and is explicitly out of scope).

## Risks/Concerns

### Risk 1: Peak connection pressure against S3/minio backend
**Risk:** `leafConcurrency(4) * downloadConcurrency(24) = 96` worst-case
simultaneous connections for one multi-leaf query, multiplied further across
concurrently-sharded blocks in a real search request.
**Impact:** Could exhaust minio/S3 client connection pool or trigger
backend-side throttling under load.
**Mitigation:** Bounds chosen conservatively within the brainstormed 16-32
range for `downloadConcurrency`; `leafConcurrency` kept small (4) since
multi-leaf queries are less common than single-leaf. Flagged in the
`vibuilder/NOTES.md` entry as a value to reconsider after load-testing
against tempo-dev-test-03's actual backend (out of scope for this plan —
this is Approach 1 only; connection-pool tuning is a follow-up if
production metrics show pressure after rollout).

### Risk 2: Losing the exact abort-on-error ordering
**Risk:** With concurrent downloads, the "first" error returned by
`errgroup.Wait()` may not be the same key that would have errored first
under the old strictly-sequential loop (order is now a race).
**Impact:** Error messages in logs may cite a different key than before for
the same underlying failure (e.g. "connection reset on key X" vs "on key
Y") — cosmetic, not a correctness issue, since ALL non-404 errors abort the
whole build either way and the caller falls back to a full scan regardless
of which specific key's error message is surfaced.
**Mitigation:** Document this explicitly in the `vibuilder/NOTES.md` entry
so it isn't mistaken for a regression during future debugging. No code
mitigation needed — `NOTE-VI-041`'s contract is "any non-404 error aborts",
not "the first-encountered non-404 error in key order is reported".

### Risk 3: `betteralign`/struct-size lint on `SliceValueIndexSource`
**Risk:** Adding a `sync.Mutex` field changes the struct's memory layout;
`betteralign` (part of `make precommit`) checks field alignment/ordering.
**Impact:** Possible lint failure if the mutex isn't placed per betteralign's
preferred ordering.
**Mitigation:** Run `make precommit` (Step 3.2) before considering the task
done; reorder fields if betteralign flags it (conventionally, put `sync.Mutex`
first, which is also idiomatic Go and matches the plan's example above).

### Risk 4: `nilaway` false positives on errgroup closures
**Risk:** `nilaway` (part of `make precommit`) sometimes flags closures
capturing loop variables or interface values in ways it can't prove
non-nil.
**Impact:** Possible CI-only failure not caught by a quick local `go build`.
**Mitigation:** Run the full `make precommit` locally before considering
the task done, not just `go build`/`go test`.

## Dependencies

### Internal Dependencies
- `internal/modules/executor` — `SliceValueIndexSource`, `VILookupResult`,
  `ValueIndexBuildStats` (existing, being modified in Task 2.1).
- `internal/modules/valueindex` — `QueryBucketFiles`, `ColHash`,
  `ColTypeName`, predicates (existing, unmodified).
- `internal/vm` — `Program`, `RangeNode` (existing, unmodified).

### External Dependencies
- `golang.org/x/sync/errgroup` — already a `go.mod` dependency
  (`golang.org/x/sync v0.20.0`, confirmed via `go.mod:22`), already used
  elsewhere in this repo (`reader.go:482-483`, per the `NOTE-291` pattern
  this plan's `downloadAll`/leaf-loop rewrites deliberately mirror: bounded
  `errgroup.Group` + `SetLimit` + pre-sized index-addressed result slice).
  No new dependency, no license check needed.

### No New Dependencies

## Out of Scope (explicitly, per brainstorm.md's recommendation)

- **Approach 2** (merging `Size()`+`ReadAt()` into a single `GetObject`
  call) — touches the `FileStore`/`ValueIndexFileStore` public API surface
  (aliased in `valueindex_query.go`); flagged as a fast-follow requiring
  explicit user permission before implementing, per this repo's "no new
  public API surface without explicit permission" rule. Not a task in this
  plan.
- **Approach 3** (investigating whether the 465-files/query figure reflects
  a value-index-compactor L0→L1/L2 backlog) — a separate investigation
  track, not a code change. Not a task in this plan.
- **Vendor bump into tempo-mrd** — happens after this fix lands and passes
  `make precommit` in the blockpack repo; not part of this plan's task list
  (per the task's own framing: "target the blockpack repo's
  `internal/modules/vibuilder/` package as the primary fix location").
- **Threading `context.Context` through `FileStore.Size`/`ReadAt`** — would
  enable true cancellation of in-flight downloads on error/deadline, but is
  an interface change bundled conceptually with Approach 2; not done here.

## Complexity Analysis

### `downloadAll` (rewritten)
**Estimated complexity:** ~6 (single loop, one nested error check, one
reduction loop) — lower than the original serial version's effective
complexity once the reduction is separated from the download logic.

### `BuildSource` (leaf-loop section only, rewritten)
**Estimated complexity:** ~5 for the new leaf block, added to the existing
function's complexity elsewhere (match-all branch untouched by this plan).
Overall function complexity should stay well under the repo's `< 30` gocyclo
limit — verify with `gocyclo -over 30 internal/modules/vibuilder/` as part
of Step 3.2.

### `lookupColumnAll` (rewritten)
**Estimated complexity:** ~7 (loop + 3 early-return error checks inside the
goroutine + reduction loop) — comparable to the original.

None of these approach the repo's complexity limit; no further decomposition
needed.

## Test Coverage Goals

- New/modified code in `vibuilder/builder.go`: **100%** of the new
  concurrency branches (skip-on-404, abort-on-error, empty-vs-skip
  distinction, firstType ordering) — these are exactly the invariants this
  plan's tests are designed to pin.
- `executor/metrics_trace.go`'s mutex-guarded methods: covered by the
  existing non-concurrent tests (unchanged behavior) plus the new
  `-race` concurrent test.
- Package-wide: must stay above this repo's enforced **>70%** coverage
  threshold (`internal/modules/vibuilder/` and
  `internal/modules/executor/`) — `make precommit`/CI enforces this.

## Success Criteria

- [ ] All new tests (Phase 1) fail against the pre-fix code, confirming
  they exercise real behavior.
- [ ] All new tests pass after the fix (Phase 2/3).
- [ ] All pre-existing `vibuilder` and `executor` tests continue to pass
  unmodified — no regression to `NOTE-VI-036/039/041`'s documented
  contracts.
- [ ] `go test -race ./internal/modules/vibuilder/... ./internal/modules/executor/...`
  passes with zero race reports.
- [ ] `make precommit` passes cleanly (gofumpt, golines, golangci-lint incl.
  gocyclo `<30`, betteralign, nilaway, build, tests, deadcode, staticcheck).
- [ ] `internal/modules/executor/SPECS.md`, `NOTES.md`, `TESTS.md` updated
  (via spec-oracle/MCP, correct next-free IDs verified at implementation
  time, not assumed from this plan's placeholders).
- [ ] `internal/modules/vibuilder/NOTES.md` updated with a dated entry.
- [ ] No changes to `FileStore`/`ValueIndexFileStore` public API surface.
- [ ] No changes on the `blockio`/trace-block read path (untouched,
  confirmed out of scope by the brainstorm).
- [ ] Work stays on `main` in the blockpack repo; no branch created, no
  push, no PR opened without being explicitly asked.

## Notes

- This plan deliberately mirrors the existing `errgroup.Group` + `SetLimit`
  + pre-sized-slice-by-index pattern already used in this repo at
  `reader.go:476-503` (`parseMatchingBlocks`, documented under `NOTE-291`)
  for consistency with an established, precommit-clean concurrency idiom in
  this codebase, rather than introducing a new pattern.
- The concurrency bounds (`downloadConcurrency=24`, `leafConcurrency=4`) are
  starting values based on the brainstorm's 16-32 recommendation and a
  conservative multiplicative-pressure estimate; they are not required to be
  configurable/tunable-from-tempo for this fix to land — that can be a
  follow-up if load testing shows the fixed constants need adjustment per
  environment.

## Questions/Uncertainties

1. **vibuilder doc maturity** (see dedicated section above) — should
   `vibuilder` eventually get a full `SPECS.md`/`TESTS.md`/`BENCHMARKS.md`
   suite? Not resolved here; deferred as an open question since its closest
   sibling (`valueindex`) hasn't graduated either.
2. **Exact next-free `SPEC-`/`NOTE-VI-`/`TEST-` IDs** — this plan's IDs
   (`SPEC-006`?, `NOTE-VI-048`, `NOTE-VI-049`) are placeholders based on the
   highest IDs found during this planning pass; the coder MUST re-verify via
   spec-oracle/MCP tooling immediately before writing, since concurrent work
   elsewhere in the repo may have advanced the sequence.
3. **Whether `leafConcurrency=4` is the right value** — chosen conservatively
   to bound `leafConcurrency * downloadConcurrency` connection pressure;
   revisit after production rollout metrics/load testing (Risk 1) if
   multi-leaf queries turn out to be more common than assumed here.
4. **Tracking issue number** for the `downloadConcurrency` constant's doc
   comment (placeholder "`issue #<TBD>`" in Step 2.3) — fill in the real
   tempo/blockpack issue number if one exists for this incident before
   landing, consistent with this codebase's convention of citing issue
   numbers in comments (e.g. `issue #461`, `issue #465`, `issue #399`).
