# Implementation Plan: #495 — tempo-side VCNT listing-time pruning

## Bootstrap note

This planner's toolset in this session is Read/Write/Glob/Grep only — no Bash/shell tool is
available, so the `~/bin/lth stats` / `~/bin/lth prompt ...` bootstrap commands specified in the
task instructions could not be executed directly from this agent. The brainstorm document
(`.bob/state/495-vcnt-pruning/brainstorm.md`) already ran that same bootstrap during its own
phase and recorded the result: "`lth prompt` returned prior project context already captured in
`.bob/state/context.md`, including the exact NOTE-VI-030 `IsInTimeRange` boundary formula this
task must reuse. No new information beyond what context.md already surfaced." This plan proceeds
on that basis. The post-plan `lth store` call the task instructions specify also could not be run
from here — the exact command and content to run is provided at the end of this document for
whichever agent/session has shell access next.

## Overview

Add a single pure, exported filename-decision helper `VCNTFileOverlapsRange` to a new file
`tempodb/encoding/vblockpack/vcnt_prune.go`, and call it from both tempo-side VCNT listing paths
— `buildVCNTSection` (`cube_backfill.go`) and `fetchVCNTSection` (`modules/frontend/vcnt_fetch.go`)
— to skip GET/Read of `.vcnt` files whose v2-embedded time range provably does not overlap the
query window, before any object-store I/O. A parse failure (v1-shaped or malformed filename)
unconditionally means "always fetch" — never a config/flag toggle. No blockpack changes. No
caching added. This is a small, contained, tempo-only change on the existing `agentic-tempo`
branch (no worktree).

## Current-code verification (re-read directly, do not trust brainstorm's line numbers blindly)

Re-verified by direct `Read` of both files in this session (2026-07-10):

**`tempodb/encoding/vblockpack/cube_backfill.go`**
- Lines 74-88: `fetchVCNTSection`'s doc comment, including the stale claim at lines 81-84:
  `"VCNT filenames (L<N>-<id>.vcnt) carry no embedded time range ... minTS/maxTS are accepted
  here only to document that scoping contract; they are not used to prune the file list."`
  — must be rewritten.
- Lines 89-105: `(cqp *cubeQueryPath) fetchVCNTSection(ctx, tenant string, dims []string, minTS,
  maxTS uint64) ([]byte, []blockpack.VCNTChunkDirEntry)` — already has real `minTS, maxTS`
  params, already threads them into `buildVCNTSection(...)` at line 104. **No change needed to
  this function itself** beyond the doc comment above it.
- Lines 111-151: `buildVCNTSection(ctx context.Context, store valueIndexStore, tenant string,
  dims []string, _, _ uint64) ([]byte, []blockpack.VCNTChunkDirEntry)` — the two ignored params
  are at line 116 (`_, _ uint64`).
- Line 126: `for _, k := range keys {`
- Lines 127-129: `if path.Ext(k) != ".vcnt" { continue }`
- Line 130: `data, getErr := store.Get(ctx, k)` — the GET call to insert the prune check before.

**`modules/frontend/vcnt_fetch.go`**
- Lines 73-75: current signature —
  `func fetchVCNTSection(ctx context.Context, rawR backend.RawReader, tenant, indexPrefix
  string, dims []string) (data []byte, dir []blockpack.VCNTChunkDirEntry, filesCount int,
  bytesRead int64)` — **no `minTS`/`maxTS` params today; signature change required.**
- Line 94: `for _, k := range keys {`
- Line 95: `keypath, name := splitObjectKey(k)` — `name` is the bare leaf filename
  `VCNTParseFilenameV2` needs.
- Line 96: `rc, _, err := rawR.Read(ctx, name, keypath, nil)` — the Read call to insert the
  prune check before.
- Line 255 (inside `buildQueryPlanFromProgram`, which starts at line 218 and already has
  `minTS, maxTS uint64` in its own param list at line 220): `data, dir, filesCount, bytesRead :=
  fetchVCNTSection(vcntCtx, rawR, tenant, indexPrefix, dims)` — the one real call site to update.

**`modules/frontend/vcnt_fetch_test.go`** — direct calls to `fetchVCNTSection` (the only call
sites that need an added argument; all other tests in this file call `buildQueryPlan`/
`buildMetricsQueryPlan`, which already have `minTS, maxTS` in their own signatures today and
will automatically thread real values down through `buildQueryPlanFromProgram` once its line-255
call site is updated — those tests need NO edits):
- Line 68: `TestFetchVCNTSection_NilRawReaderReturnsNil`
- Line 77: `TestFetchVCNTSection_NoDimsReturnsNil`
- Line 98: `TestFetchVCNTSection_MergesObjectsForRequestedDims`
- Line 119: `TestFetchVCNTSection_MissingDimYieldsNoCoverageNotError`

(The brainstorm's count of "7 call sites" for this file over-counted by including indirect
`buildQueryPlan`/`buildMetricsQueryPlan`-based tests that don't need edits. Re-verified directly:
exactly 4 direct calls need a new argument.)

**Existing fixture-writer helpers — confirmed IMPORTANT finding not fully called out in the
brainstorm:** both test files' existing object-writing helpers produce **v1-shaped** filenames,
not v2:
- `vcnt_fetch_test.go:53`, `writeVCNTObject`: `blockpack.VCNTFormatFilename(0,
  blockpack.VCNTNewID())` — v1, no embedded range.
- `cube_vcnt_fetch_test.go:69`, `vcntObjKey`: `blockpack.VCNTObjectKey(tenant,
  defaultValueIndexPref, column, id)` — v1, no embedded range.

This means **every existing test at both call sites currently exercises the "always fetch" v1
fallback path already**, regardless of whatever `minTS`/`maxTS` values they pass — none of them
will be affected in outcome by adding the pruning check (this directly satisfies R7(d)'s audit
requirement: confirmed no existing test's placeholder window value becomes load-bearing, because
none of the existing fixtures are v2-shaped in the first place). New v2-shaped fixture helpers
must be added specifically to exercise real pruning behavior — see Phase 1 below.

**`memVCNTStore` (cube_vcnt_fetch_test.go:21-64)** implements `valueIndexStore` (`List`, `Get`,
`Size`, `ReadAt`) as a plain struct — no existing counting/recording wrapper for it (unlike
`vcnt_fetch_test.go`'s `countingRawReader`). One must be added for the R7(a) assertion at this
call site.

## Files to Create

1. `tempodb/encoding/vblockpack/vcnt_prune.go` — the shared pure filename-decision helper.
2. `tempodb/encoding/vblockpack/vcnt_prune_test.go` — unit tests for the helper in isolation
   (pure string/uint64 inputs, no store fakes).

## Files to Modify

1. `tempodb/encoding/vblockpack/cube_backfill.go`
   - Rewrite the stale doc comment at lines 74-88 (specifically the false "carry no embedded
     time range ... not used to prune the file list" claim at 81-84).
   - Rename `buildVCNTSection`'s ignored `_, _ uint64` params (line 116) to `minSec, maxSec
     uint64` and use them.
   - Insert the prune check inside the `for _, k := range keys` loop (line 126), after the
     `.vcnt` extension check (lines 127-129), before `store.Get` (line 130).

2. `tempodb/encoding/vblockpack/cube_vcnt_fetch_test.go`
   - Add a v2-filename fixture-writer helper (`vcntObjKeyV2`) alongside the existing v1
     `vcntObjKey`.
   - Add a `countingVCNTStore` wrapper around `memVCNTStore` that records every key passed to
     `Get`, for the "never fetched" assertion (R7a).
   - Add new tests per the Spec-Driven/R7 test list below.
   - No changes needed to the 3 existing tests (`TestBuildVCNTSection_MergesPerDim`,
     `TestBuildVCNTSection_IgnoresNonVCNTKeys`, `TestBuildVCNTSection_NoCoverageReturnsNil`) —
     confirmed all their fixtures are v1-shaped, unaffected by pruning.

3. `modules/frontend/vcnt_fetch.go`
   - Add `minTS, maxTS uint64` parameters to `fetchVCNTSection`'s signature (lines 73-75).
   - Insert the prune check inside the `for _, k := range keys` loop (line 94), right after
     `keypath, name := splitObjectKey(k)` (line 95), before `rawR.Read` (line 96).
   - Update the one real call site inside `buildQueryPlanFromProgram` (line 255) to pass
     `minTS, maxTS` (already in scope as that function's own params, line 220).

4. `modules/frontend/vcnt_fetch_test.go`
   - Add `minTS, maxTS` arguments to the 4 direct `fetchVCNTSection` calls (lines 68, 77, 98,
     119), using a real, meaningful window at each (not a value that happens to include
     everything by accident with no reasoning given).
   - Add a v2-filename fixture-writer helper (`writeVCNTObjectV2`) alongside the existing v1
     `writeVCNTObject`.
   - Add new tests per the Spec-Driven/R7 test list below, reusing the existing
     `countingRawReader` wrapper (already defined at line 171) for the "never Read" assertion.

## Implementation Steps

### Phase 0: Confirm no drift since brainstorm (2-3 min)

- [ ] Re-run `Grep` for `VCNTFormatFilename\b|VCNTObjectKey\b` (v1, no "V2" suffix) across
      `tempodb/` and `modules/frontend/` to confirm no other VCNT write call site exists beyond
      `vcntwriter.go`'s `flush` (which already uses `VCNTObjectKeyV2`) — this re-confirms R8/
      Requirement 2's "no blockpack changes, no other write path to worry about" finding still
      holds. If a new v1 write call site is found, flag it — do not silently work around it.

### Phase 1: Tests (TDD) — write all tests below FIRST, confirm they fail to compile/fail

**Step 1.1: Helper unit tests — `tempodb/encoding/vblockpack/vcnt_prune_test.go`**

- [ ] Create the file with package `vblockpack`, importing `blockpack
      "github.com/grafana/blockpack"`, `"testing"`.
- [ ] `TestVCNTFileOverlapsRange_V2InRangeReturnsTrue` — build a name via
      `blockpack.VCNTFormatFilenameV2(0, 100, 200, "id1")`; call
      `VCNTFileOverlapsRange(name, 150, 160)`; assert `true`.
- [ ] `TestVCNTFileOverlapsRange_V2OutOfRangeReturnsFalse` — name via
      `VCNTFormatFilenameV2(0, 100, 200, "id1")`; call `VCNTFileOverlapsRange(name, 300, 400)`;
      assert `false`.
- [ ] `TestVCNTFileOverlapsRange_V1ShapedAlwaysTrue` — name via
      `blockpack.VCNTFormatFilename(0, "id1")` (v1, no range); call
      `VCNTFileOverlapsRange(name, 300, 400)` — a window with NO relation to anything embedded
      in a v1 name; assert `true` ("unknown range, always fetch").
- [ ] `TestVCNTFileOverlapsRange_MalformedNameAlwaysTrue` — call
      `VCNTFileOverlapsRange("not-a-filename-at-all", 0, 100)` and
      `VCNTFileOverlapsRange("L0-abc.vcnt", 0, 100)` (missing `.vcnt`-suffix-shaped but
      wrong-field-count v1 form is already covered above; use a genuinely garbage string here,
      e.g. `""` and `"junk.tmp"`); assert `true` for each.
- [ ] `TestVCNTFileOverlapsRange_BoundaryWallMaxEqualsQueryMin` — name via
      `VCNTFormatFilenameV2(0, 50, 100, "id1")`; call `VCNTFileOverlapsRange(name, 100, 200)`
      (query window starts exactly at the file's `WallMaxSec`); assert `true` (inclusive-both-
      ends formula: `WallMaxSec >= queryMinSec`).
- [ ] `TestVCNTFileOverlapsRange_BoundaryWallMinEqualsQueryMax` — name via
      `VCNTFormatFilenameV2(0, 100, 150, "id1")`; call `VCNTFileOverlapsRange(name, 0, 100)`
      (query window ends exactly at the file's `WallMinSec`); assert `true`
      (`WallMinSec <= queryMaxSec`).
- [ ] `TestVCNTFileOverlapsRange_JustOutsideBoundaryReturnsFalse` — name via
      `VCNTFormatFilenameV2(0, 100, 150, "id1")`; call `VCNTFileOverlapsRange(name, 0, 99)`
      (query window ends ONE SECOND before the file starts); assert `false` — pins the strict
      "just past the edge" case immediately adjacent to the inclusive-boundary tests above, so a
      future off-by-one in the helper is caught by a test that currently would NOT be caught by
      the boundary-inclusive tests alone.

**Step 1.2: `buildVCNTSection` integration tests — `cube_vcnt_fetch_test.go`**

- [ ] Add helper:
  ```go
  // vcntObjKeyV2 builds the S3 key a v2-format .vcnt object would live under, embedding an
  // explicit wall-clock range for pruning tests.
  func vcntObjKeyV2(tenant, column, id string, wallMinSec, wallMaxSec uint64) string {
      colHash := blockpack.VCNTColHash(column)
      filename := blockpack.VCNTFormatFilenameV2(0, wallMinSec, wallMaxSec, id)
      return path.Join(tenant, defaultValueIndexPref, "unique_values", colHash, filename)
  }
  ```
- [ ] Add a recording wrapper:
  ```go
  // countingVCNTStore wraps memVCNTStore and records every key passed to Get, so a test can
  // assert a file was never fetched (not merely "absent from the output section" — a bug that
  // filters post-fetch instead of pre-fetch could satisfy that weaker assertion by accident).
  type countingVCNTStore struct {
      *memVCNTStore
      gotKeys []string
  }

  func (c *countingVCNTStore) Get(ctx context.Context, key string) ([]byte, error) {
      c.gotKeys = append(c.gotKeys, key)
      return c.memVCNTStore.Get(ctx, key)
  }
  ```
- [ ] `TestBuildVCNTSection_OutOfRangeV2FileNeverFetched` — one in-store v2 object at
      `vcntObjKeyV2(tenant, "span:kind", "id1", 500, 600)` with real vcnt bytes (reuse existing
      `vcntObj` helper), wrapped in `countingVCNTStore`; call `buildVCNTSection(ctx, store,
      tenant, []string{"span:kind"}, 0, 100)` (query window entirely before the file's range);
      assert `data == nil && dir == nil` AND assert the out-of-range key is NOT present in
      `store.gotKeys` — the mechanism assertion R7(a) requires, not just the outcome.
- [ ] `TestBuildVCNTSection_InRangeV2FileStillFetched` — same shape, but query window
      `100, 700` overlaps `[500, 600]`; assert the key IS present in `store.gotKeys` and the
      section is non-nil with the expected record content (reuse the
      `VCNTSelectivityInRange`-based content assertion pattern from
      `TestBuildVCNTSection_MergesPerDim`).
- [ ] `TestBuildVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow` — write only a v1-shaped
      object via the EXISTING `vcntObjKey`/`vcntObj` helpers, call `buildVCNTSection` with a
      window (e.g. `900, 1000`) that shares nothing with anything a v2 file might have declared;
      assert the key IS present in `store.gotKeys` (the file was fetched despite the "unrelated"
      window) and the section reflects its content — pins R7(b).
- [ ] `TestBuildVCNTSection_BoundaryTouchingWindowsIncluded` — one v2 object at
      `vcntObjKeyV2(tenant, "span:kind", "id1", 100, 200)`; run it twice: once with query window
      `200, 300` (`WallMaxSec == queryMinSec`) and once with `0, 100` (`WallMinSec ==
      queryMaxSec`); assert both are fetched (key present in `gotKeys`, non-nil section) — pins
      R7(c) at the integration level, not just the helper's own unit test.

**Step 1.3: `fetchVCNTSection` integration tests — `vcnt_fetch_test.go`**

- [ ] Add helper:
  ```go
  // writeVCNTObjectV2 writes data using the v2 filename format with an explicit wall-clock
  // range, for pruning tests (writeVCNTObject above stays v1-shaped on purpose, exercising the
  // "unknown range, always fetch" fallback path).
  func writeVCNTObjectV2(t *testing.T, rawW backend.RawWriter, column string, data []byte, wallMinSec, wallMaxSec uint64) {
      t.Helper()
      colHash := blockpack.VCNTColHash(column)
      name := blockpack.VCNTFormatFilenameV2(0, wallMinSec, wallMaxSec, blockpack.VCNTNewID())
      keypath := backend.KeyPath{"tenant-a", testIndexPrefix, "unique_values", colHash}
      require.NoError(t, rawW.Write(t.Context(), name, keypath, bytes.NewReader(data), int64(len(data)), nil))
  }
  ```
- [ ] Update the 4 direct `fetchVCNTSection` call sites (lines 68, 77, 98, 119) to pass a real
      `minTS, maxTS` — for the 2 nil/no-dims/no-coverage tests any concrete window is fine (e.g.
      `0, 200`, matching the window used elsewhere in this file, with a one-line comment noting
      it's a real, deliberately chosen window, not a leftover placeholder); for
      `TestFetchVCNTSection_MergesObjectsForRequestedDims` also use `0, 200` and add a one-line
      comment confirming this test's fixtures are v1-shaped (`writeVCNTObject`) so the window is
      inert here by design — the NEW tests below are what actually exercises pruning.
- [ ] `TestFetchVCNTSection_OutOfRangeV2FileNeverFetched` — `writeVCNTObjectV2` one object for
      `"span.http.method"` with range `500, 600`; wrap `rawR` in the existing `countingRawReader`
      (line 171); call `fetchVCNTSection(ctx, counting, tenant, testIndexPrefix,
      []string{"span.http.method"}, 0, 100)` (window before the file's range); assert
      `data == nil && dir == nil && filesCount == 0` AND `counting.readCalls == 0` — the R7(a)
      mechanism assertion (Find still runs — it must, to discover the file exists at all — only
      `Read` must be zero).
- [ ] `TestFetchVCNTSection_InRangeV2FileStillFetched` — same shape, window `100, 700` overlaps
      `[500, 600]`; assert `counting.readCalls == 1`, `filesCount == 1`, non-nil section.
- [ ] `TestFetchVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow` — use the EXISTING
      `writeVCNTObject` (v1) with a window (e.g. `900, 1000`) sharing nothing with any v2 range
      used elsewhere in this test file; assert `counting.readCalls == 1` and non-nil section —
      pins R7(b).
- [ ] `TestFetchVCNTSection_BoundaryTouchingWindowsIncluded` — one v2 object with range
      `100, 200`; run with window `200, 300` and separately `0, 100`; assert both are fetched
      (`counting.readCalls == 1` each run, non-nil section) — pins R7(c) at the integration
      level.

**Step 1.4: Verify all new/updated tests fail before implementation**

- [ ] Run `go build ./tempodb/encoding/vblockpack/... ./modules/frontend/...` — expect compile
      failure (`VCNTFileOverlapsRange` undefined, `fetchVCNTSection` called with wrong arg
      count) — this is the expected, correct failure proving the tests reference code that
      doesn't exist yet.
- [ ] Note: the compile failure itself IS the "tests fail" signal here (Go doesn't let you run
      partially-compiling packages) — no separate "run and observe red" step is meaningful until
      Phase 2's signature changes land; at that point re-run to confirm the NEW test assertions
      (not just compilation) fail without the prune-check insertion, then add the insertion and
      confirm green. Concretely: implement the signature/param-rename changes (Phase 2, Step
      2.1) WITHOUT yet inserting the `VCNTFileOverlapsRange` check, run the new
      "OutOfRange...NeverFetched" tests, confirm they FAIL (file still fetched) — this proves the
      test actually asserts something — then insert the check (Step 2.2) and confirm they PASS.

### Phase 2: Implementation

**Step 2.1: Add the helper (2 min)**

- [ ] Create `tempodb/encoding/vblockpack/vcnt_prune.go`:
  ```go
  package vblockpack

  // vcnt_prune.go — shared, pure filename-decision helper for tempo-side VCNT listing-time
  // pruning (issue #495, follow-up to #494's v2-ranged VCNT filenames). Called from both
  // cube_backfill.go:buildVCNTSection (querier/cube-backfill path) and
  // modules/frontend/vcnt_fetch.go:fetchVCNTSection (frontend plan-time path) — see each
  // call site's own doc comment for why their surrounding fetch loops stay unmerged; only this
  // small, store-independent predicate is shared between them.

  import (
      blockpack "github.com/grafana/blockpack"
  )

  // VCNTFileOverlapsRange reports whether the .vcnt file named name should be fetched to answer
  // a query over [minSec, maxSec] (argument order matches blockpack.VCNTFileMeta.IsInTimeRange's
  // own (queryMinSec, queryMaxSec) order — do not swap).
  //
  // A v1-shaped or otherwise unparseable name (blockpack.VCNTParseFilenameV2 returns an error)
  // ALWAYS returns true: unknown range means always fetch, never drop. This is an unconditional,
  // hard-coded safety rule, not a tunable — it deliberately avoids repeating blockpack's own
  // valueindex/discovery.go mistake (NOTE-VI-030) of treating "I don't know this file's range"
  // as "skip it," which silently and permanently drops pre-v2-format files from ever being
  // considered.
  func VCNTFileOverlapsRange(name string, minSec, maxSec uint64) bool {
      meta, err := blockpack.VCNTParseFilenameV2(name)
      if err != nil {
          return true
      }
      return meta.IsInTimeRange(minSec, maxSec)
  }
  ```

**Step 2.2: `cube_backfill.go` — rename params, insert check, fix doc comment (3-5 min)**

- [ ] Rename `buildVCNTSection`'s params (current line 116) from `_, _ uint64` to `minSec,
      maxSec uint64`.
- [ ] Insert immediately after the `.vcnt` extension check (current lines 127-129), before
      `store.Get` (current line 130):
  ```go
  if !VCNTFileOverlapsRange(path.Base(k), minSec, maxSec) {
      continue
  }
  ```
- [ ] Rewrite the doc comment (current lines 81-84). Replace:
  > "VCNT filenames (L<N>-<id>.vcnt) carry no embedded time range — unlike VI files — so all of a
  > column's .vcnt files are fetched and the query window is applied at the record level by the
  > gate's ValuesInRange decode. minTS/maxTS are accepted here only to document that scoping
  > contract; they are not used to prune the file list."

  with something like:
  > "VCNT filenames written since issue #494 embed a wall-clock time range (v2 format,
  > L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt); VCNTFileOverlapsRange (issue #495) uses that
  > range to skip GET-ing files that provably cannot overlap [minTS, maxTS], before any S3 read.
  > A v1-shaped or unparseable filename is always fetched (unknown range, never dropped). The
  > query window is still additionally applied at the record level by the gate's ValuesInRange
  > decode after fetch — this file-level check only avoids unnecessary GETs, it does not replace
  > that record-level filtering."

**Step 2.3: `vcnt_fetch.go` — signature change, insert check, update call site (3-5 min)**

- [ ] Change `fetchVCNTSection`'s signature (current lines 73-75) to add `minTS, maxTS uint64`:
  ```go
  func fetchVCNTSection(
      ctx context.Context, rawR backend.RawReader, tenant, indexPrefix string, dims []string,
      minTS, maxTS uint64,
  ) (data []byte, dir []blockpack.VCNTChunkDirEntry, filesCount int, bytesRead int64) {
  ```
- [ ] Insert immediately after `keypath, name := splitObjectKey(k)` (current line 95), before
      `rawR.Read` (current line 96):
  ```go
  if !vblockpack.VCNTFileOverlapsRange(name, minTS, maxTS) {
      continue
  }
  ```
- [ ] Update the call site inside `buildQueryPlanFromProgram` (current line 255):
  ```go
  data, dir, filesCount, bytesRead := fetchVCNTSection(vcntCtx, rawR, tenant, indexPrefix, dims, minTS, maxTS)
  ```
- [ ] Update this function's own doc comment (lines 48-72) if it makes any "fetches everything"
      claim — re-read it during implementation; at time of planning it does not explicitly claim
      "no pruning" (unlike `cube_backfill.go`'s comment), but confirm no such claim needs fixing
      once the change lands.

**Step 2.4: Update existing test call sites (2-3 min)**

- [ ] `vcnt_fetch_test.go` lines 68, 77, 98, 119: add `, 0, 200` (or the specific window chosen
      per Step 1.3) as trailing args to each direct `fetchVCNTSection(...)` call.
- [ ] Confirm no other direct callers of `fetchVCNTSection` exist elsewhere in the tree (`Grep`
      for `fetchVCNTSection(` across `modules/frontend/`).

### Phase 3: Verification

**Step 3.1: Run tests**

- [ ] `go test ./tempodb/encoding/vblockpack/... ./modules/frontend/...` — all should pass,
      including every new test from Phase 1.
- [ ] `go test -race ./tempodb/encoding/vblockpack/... ./modules/frontend/...`
- [ ] `go test -cover ./tempodb/encoding/vblockpack/... ./modules/frontend/...`

**Step 3.2: Regression-prove the mutation the task most cares about**

- [ ] Temporarily revert the `err != nil` branch in `VCNTFileOverlapsRange` to `return false`
      (the wrong, NOTE-VI-030-style polarity) and confirm
      `TestVCNTFileOverlapsRange_V1ShapedAlwaysTrue`,
      `TestBuildVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow`, and
      `TestFetchVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow` all FAIL. Revert back to
      `return true` and confirm all three pass again. This is the single most important
      regression guard in this task (R6/R7's central concern) — do not skip it.
- [ ] Temporarily comment out the prune-check insertion at ONE call site only (e.g.
      `cube_backfill.go`'s) and confirm that call site's
      `TestBuildVCNTSection_OutOfRangeV2FileNeverFetched` fails while the sibling
      `TestFetchVCNTSection_OutOfRangeV2FileNeverFetched` still passes — proves the two call
      sites are independently tested and a regression at one would not be masked by the other.
      Restore the check afterward.

**Step 3.3: Code quality**

- [ ] `go fmt ./tempodb/encoding/vblockpack/... ./modules/frontend/...`
- [ ] `go vet ./tempodb/encoding/vblockpack/... ./modules/frontend/...`
- [ ] Confirm `VCNTFileOverlapsRange` and its two call-site insertions stay well under any
      complexity threshold — this is a 4-line pure function and 3-line call-site insertions;
      no complexity risk expected.

## Spec-Driven Verification Tests

No spec-driven modules (SPECS.md/NOTES.md/TESTS.md/BENCHMARKS.md or the `// NOTE:` invariant
comment) exist in tempo's `modules/frontend/` or `tempodb/encoding/vblockpack/` — confirmed by
the brainstorm's own Spec-Driven Modules section and re-confirmed here (`Glob` for
`SPECS.md`/`NOTES.md`/`TESTS.md`/`BENCHMARKS.md` under both directories returns nothing). The
blockpack-side invariants this task depends on (`valuecounts.ParseFilenameV2`'s no-v1-fallback
behavior, `FileMeta.IsInTimeRange`'s inclusive-both-ends formula) are read directly above as
load-bearing constraints, but this task makes no blockpack-side edits, so no blockpack
SPECS.md/NOTES.md update is in scope. This section is otherwise N/A per the template.

## R7 Test List Summary (both call sites — mandatory, not optional)

| Requirement | `buildVCNTSection` test | `fetchVCNTSection` test |
|---|---|---|
| (a) out-of-range v2 file never fetched (mechanism, not outcome) | `TestBuildVCNTSection_OutOfRangeV2FileNeverFetched` | `TestFetchVCNTSection_OutOfRangeV2FileNeverFetched` |
| in-range v2 file still fetched (sibling positive case) | `TestBuildVCNTSection_InRangeV2FileStillFetched` | `TestFetchVCNTSection_InRangeV2FileStillFetched` |
| (b) v1/garbage filename always fetched | `TestBuildVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow` | `TestFetchVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow` |
| (c) boundary-touching windows included | `TestBuildVCNTSection_BoundaryTouchingWindowsIncluded` | `TestFetchVCNTSection_BoundaryTouchingWindowsIncluded` |
| (d) audit existing placeholder windows | Confirmed above: all 3 existing tests use v1 fixtures, unaffected — no action needed beyond the confirmation already documented in this plan | Confirmed above: all 4 pre-existing direct calls use v1 fixtures or no-dims/nil short-circuits, unaffected — no action needed beyond the confirmation already documented in this plan |

Plus the helper's own pure unit tests in `vcnt_prune_test.go` (7 tests, Step 1.1) covering
in-range, out-of-range, v1-shaped, malformed, both boundary-inclusive edges, and one
just-outside-boundary case.

## Edge Cases to Handle

### Edge Case 1: Zero-width file range (`WallMinSec == WallMaxSec`)
**Scenario:** A freshly-flushed L0 VCNT file that only ever touched one minute bucket.
**Expected:** Handled correctly by the existing two-sided comparison with no special case —
confirmed in the brainstorm's Requirement 6 analysis; no dedicated test needed beyond the
boundary tests already planned (a zero-width range is just the boundary case where
`WallMinSec == WallMaxSec`, already implicitly covered by the boundary tests' construction if a
future editor sets both to the same value — not adding a redundant test for this specifically).

### Edge Case 2: `rawR`/`store` returns an empty listing for a dim
**Scenario:** No `.vcnt` files exist yet for a column.
**Expected:** Unchanged — both functions already handle this (`continue` on empty `keys`); the
new prune check never executes since the per-file loop body never runs. Already covered by
existing `TestBuildVCNTSection_NoCoverageReturnsNil` / `TestFetchVCNTSection_MissingDimYieldsNoCoverageNotError`.

### Edge Case 3: A parse error that is NOT "v1-shaped" but something else malformed (e.g. empty
string, wrong suffix, corrupted numeric field)
**Scenario:** Any of `valuecounts.ParseFilenameV2`'s several distinct error returns (missing
`.vcnt` suffix, missing `L` prefix, wrong segment count, non-integer level/minSec/maxSec, empty
id, `minSec > maxSec`).
**Expected:** All are caught by the same single `err != nil` check — `VCNTFileOverlapsRange` does
not need to distinguish between error causes, only error-vs-no-error. Covered by
`TestVCNTFileOverlapsRange_MalformedNameAlwaysTrue`.

## Risks/Concerns

### Risk 1: Signature change to `fetchVCNTSection` missed at a call site
**Risk:** A caller of `fetchVCNTSection` elsewhere in the tree not updated, causing a compile
failure (best case) or, if somehow it compiled via a different overload, wrong behavior (not
possible in Go without generics — a missed call site will simply fail to compile).
**Mitigation:** `Grep -n "fetchVCNTSection("` across `modules/frontend/` before considering Phase
2 complete; the compiler itself enforces this.

### Risk 2: Argument-order mixup between `VCNTFileOverlapsRange(name, minSec, maxSec)` and
`IsInTimeRange(queryMinSec, queryMaxSec)`
**Risk:** A future edit at either call site swaps `minSec`/`maxSec` when calling the helper,
silently inverting which end of the window is checked (R2's explicit concern).
**Mitigation:** The helper's signature keeps the exact same argument name/order convention as
`IsInTimeRange` (`minSec, maxSec` = `(queryMinSec, queryMaxSec)`), documented in the helper's own
doc comment ("do not swap"); the boundary tests (both unit and integration level) would catch an
order-swap immediately since `TestVCNTFileOverlapsRange_BoundaryWallMaxEqualsQueryMin` and
`_BoundaryWallMinEqualsQueryMax` are NOT symmetric under argument swap (they use different
window/file combinations specifically chosen so a swap changes the outcome).

### Risk 3: `meta.IsInTimeRange` called on a value receiver
**Risk:** `VCNTFileMeta.IsInTimeRange` has a pointer receiver (`func (m *FileMeta)
IsInTimeRange(...)`); `blockpack.VCNTParseFilenameV2` returns a value, not a pointer.
**Mitigation:** Not actually a risk — `meta` is a local, addressable variable, so
`meta.IsInTimeRange(...)` compiles and works via Go's automatic `&meta` — confirmed by reading
`internal/modules/valuecounts/filename.go:114-117` directly (pointer receiver) and
`blockpack.VCNTParseFilenameV2`'s own signature (returns `VCNTFileMeta` by value) side by side;
no special handling needed in the helper, just noting this was checked, not assumed.

### Risk 4: Doc-comment update at `cube_backfill.go` drifts from the actual final behavior if
implementation changes mid-flight
**Risk:** Someone tweaks the insertion point or fallback logic during implementation without
updating the comment rewritten in Step 2.2.
**Mitigation:** Step 2.2 pairs the code change and comment rewrite in the same step; Phase 3's
review should re-read the comment against the final code before considering the task done (per
this task's own R5/Work-Item-3 requirement — this doc fix is required regardless of anything
else).

## Dependencies

### Internal Dependencies
- `tempodb/encoding/vblockpack` (already the home package of `cube_backfill.go`).
- `modules/frontend` already imports `github.com/grafana/tempo/tempodb/encoding/vblockpack`
  (for `CheckIndexCoverage`) — the new `vblockpack.VCNTFileOverlapsRange` call needs no new
  import statement, just a new symbol reference from an already-imported package.

### External Dependencies
- `github.com/grafana/blockpack` — already vendored, already imported by both target files
  (`cube_backfill.go` as `blockpack "github.com/grafana/blockpack"`, `vcnt_fetch.go` as
  `"github.com/grafana/blockpack"`). No revendor needed — confirmed
  `vendor/github.com/grafana/blockpack/vcnt.go` and
  `vendor/github.com/grafana/blockpack/internal/modules/valuecounts/filename.go` already contain
  `VCNTParseFilenameV2`/`FileMeta.IsInTimeRange`/`VCNTFormatFilenameV2` exactly as needed, read
  directly in this session.

### New Dependencies
None.

## Complexity Analysis

`VCNTFileOverlapsRange` is a 2-branch, 4-line pure function — trivial complexity. Both call-site
insertions are single `if !X { continue }` guards inside an already-existing loop — no
measurable complexity increase to either `buildVCNTSection` or `fetchVCNTSection`.

## Test Coverage Goals

- `vcnt_prune.go`: 100% (every branch — err path and both true/false outcomes of
  `IsInTimeRange` — exercised by the 7 unit tests in Step 1.1).
- Both call sites' modified loops: 100% of the new branch (in-range/out-of-range/v1-shaped) via
  the integration tests in Steps 1.2/1.3.

## Success Criteria

- [ ] All tests pass, including every new test listed above.
- [ ] `go vet`/`go fmt` clean on all touched/created files.
- [ ] No signature-change call site missed (`fetchVCNTSection(` grep across `modules/frontend/`
      shows only the 5 expected call sites: 1 production + 4 test, all updated).
- [ ] `cube_backfill.go`'s stale doc comment is corrected (Work Item 3, R5 — required
      regardless of anything else).
- [ ] The err-polarity regression check in Step 3.2 was actually performed (introduce the wrong
      polarity, confirm 3 tests fail, revert, confirm they pass again) — not merely read from the
      diff.
- [ ] No blockpack-side files touched (confirm via `git status`/diff scope — this task is
      tempo-only, R8/R9).
- [ ] No caching introduced anywhere (R3) — confirm neither call site gained a cache field/TTL/
      memoization as a side effect of this change.

## Notes

- This plan found one detail not fully surfaced in the brainstorm: both test files' EXISTING
  fixture-writer helpers (`writeVCNTObject`, `vcntObjKey`) produce v1-shaped filenames, not v2 —
  meaning the brainstorm's stated risk ("existing tests currently don't exercise any meaningful
  time window... after this change those placeholder values become load-bearing") turns out, on
  direct re-verification, to be a non-issue for the CURRENTLY-existing tests (none of their
  fixtures are v2-shaped, so none are affected by pruning regardless of window value) — but this
  makes the NEW v2-fixture tests in Phase 1 the ONLY tests that actually exercise pruning at all,
  raising their importance rather than lowering it. The R7(d) "audit" step is satisfied by this
  finding itself, documented here rather than requiring a separate audit pass during coding.
- The brainstorm's line-number citations for both target files were re-verified directly in this
  planning session (2026-07-10) via `Read` and found to match within a line or two (off by at
  most one from doc-comment line counting) — no drift from other work found. The exact current
  numbers cited in this plan's "Current-code verification" section are authoritative for the
  coder; re-read the files once more immediately before editing in case anything changed between
  planning and coding.

## Questions/Uncertainties

- None blocking. The one open question the brainstorm left for the planner (exported vs.
  unexported naming: `VCNTFileOverlapsRange`) is resolved by the team-lead ruling R1, which
  explicitly names and exports this function — no further decision needed.

---

## For the next agent with shell access — pending `lth store` call

Run (this planner had no Bash tool available to execute it directly):

```
~/bin/lth store --layer 4 --attr 'project=tempo' --attr 'tags=planning,architecture,vcnt,pruning' \
  'Plan #495 (tempo VCNT listing-time pruning): shared pure helper VCNTFileOverlapsRange(name, minSec, maxSec) added to tempodb/encoding/vblockpack/vcnt_prune.go, wrapping blockpack.VCNTParseFilenameV2+FileMeta.IsInTimeRange with an unconditional err!=nil->true (always fetch) fallback, called from both cube_backfill.go:buildVCNTSection (param rename only, no signature change) and modules/frontend/vcnt_fetch.go:fetchVCNTSection (real signature change: +minTS,maxTS, threaded from buildQueryPlanFromProgram). Key decisions: (1) fallback polarity lives in exactly one tested function, never gated behind config, to avoid repeating blockpack NOTE-VI-030s discovery.go mistake; (2) discovered during planning that BOTH existing test files fixtures (writeVCNTObject/vcntObjKey) are v1-shaped, so none of the pre-existing tests actually exercise pruning -- new v2-shaped fixture helpers (writeVCNTObjectV2/vcntObjKeyV2) were required and are the only tests proving pruning works; (3) mandatory R7 test discipline at both call sites via counting/recording wrappers on the real Get/Read call (countingVCNTStore, existing countingRawReader), not outcome-only assertions, per NOTE-VI-095 precedent; (4) no caching, no blockpack changes.'
```
