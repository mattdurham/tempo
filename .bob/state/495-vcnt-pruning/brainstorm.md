# Brainstorm — #495 tempo-side VCNT listing-time pruning

## 2026-07-10 21:05:00 - Task Received

grafana/blockpack #495 — tempo-side VCNT listing-time pruning. Follow-up to #494 (VCNT v2
filenames + time-cluster-based compaction). Two tempo-side consumers list/fetch VCNT files
unconditionally and apply the query window only after full decode:

1. `modules/frontend/vcnt_fetch.go`'s `fetchVCNTSection` (lists via `backend.RawReader.Find()`).
2. `tempodb/encoding/vblockpack/cube_backfill.go`'s `buildVCNTSection` (lists via
   `valueIndexStore.List()`; comment explicitly says VCNT filenames carry no embedded range —
   stale as of #494).

Work required: add filename parsing to both listing paths, skip files whose declared
[WallMinSec, WallMaxSec] doesn't overlap the query window before any S3 GET, update the stale
comment. Critical constraint: a v1-shaped filename or a parse failure MUST be treated as
"unknown range, always fetch, never skip" — never as an error, never dropped. This mirrors VI's
own `FilesForTimeRange`/`IsInTimeRange` pattern but must NOT repeat blockpack's own
`valueindex/discovery.go` mistake (NOTE-VI-030) of fully removing its v1 fallback, which silently
drops old files from discovery forever.

Six investigation requirements: (1) confirm blockpack's current v2 parsing API, (2) confirm
tempo's VCNT L0 writer already emits v2-ranged filenames, (3) find the exact pruning insertion
point at both call sites, (4) check whether VI's IsInTimeRange boundary semantics transfer
directly to VCNT, (5) shared helper vs. duplicated inline logic, (6) false-negative risk analysis.

Starting brainstorm process...

(lth bootstrap ran: `lth stats` — 59593 memories / 448046 edges; `lth prompt` returned prior
project context already captured in `.bob/state/context.md`, including the exact NOTE-VI-030
"IsInTimeRange" boundary formula this task must reuse. No new information beyond what
context.md already surfaced. Navigator MCP tool is not available in this agent's toolset — skipped
per instructions.)

## 2026-07-10 21:20:00 - Research Findings

### Requirement 1 — Current blockpack v2 VCNT parsing API (confirmed against blockpack@67c49cf1, main, clean tree)

Confirmed via direct read of `/home/mdurham/source/blockpack_collection/blockpack/vcnt.go` and
`internal/modules/valuecounts/filename.go`. The issue text's guess (`VCNTParseFilenameV2`) is
exactly right and did NOT change across #494/#496 — this is the live, current, public API:

- `blockpack.VCNTFormatFilenameV2(level int, wallMinSec, wallMaxSec uint64, id string) string`
  — `vcnt.go:105`
- `blockpack.VCNTParseFilenameV2(name string) (VCNTFileMeta, error)` — `vcnt.go:113`, thin
  re-export of `valuecounts.ParseFilenameV2` (`internal/modules/valuecounts/filename.go:76`)
- `blockpack.VCNTFileMeta = valuecounts.FileMeta` — `vcnt.go:110`, struct: `{Filename, ID string;
  Level int; WallMinSec, WallMaxSec uint64}`
- `(*VCNTFileMeta).IsInTimeRange(queryMinSec, queryMaxSec uint64) bool` — method on
  `valuecounts.FileMeta`, `internal/modules/valuecounts/filename.go:115`:
  `return m.WallMaxSec >= queryMinSec && m.WallMinSec <= queryMaxSec`
- `blockpack.VCNTObjectKeyV2(tenant, indexPrefix, colName, id string, wallMinSec, wallMaxSec uint64) string`
  — `vcnt.go:127`, already used by tempo's own writer.

Tempo's vendored copy already has all of these (`vendor/github.com/grafana/blockpack/vcnt.go`
matches head-for-head) — **no blockpack change and no revendor is needed** for this task. This
satisfies the "no blockpack changes needed unless a genuine gap is found" constraint: there is
no gap.

**Important, issue-relevant asymmetry vs. VI:** `valuecounts.ParseFilenameV2` (VCNT) returns a
hard error for a v1-shaped name — "no v1 fallback ... a v1-shaped name here is treated
identically to any other unparseable input" (`internal/modules/valuecounts/filename.go:72-75`).
This is the SAME posture as `valueindex.ParseFilenameV2` (VI), which ALSO now hard-errors on a
v1-shaped name (`internal/modules/valueindex/filename.go:69-71`, "the v1 ... fallback was removed
once every value-index file writer moved to v2 — see NOTE-VI-030"). The difference that matters
for #495 is not in the parser — both parsers behave identically (error on non-v2) — it's in what
the **caller** does with that error. VI's own discovery path
(`internal/modules/valueindex/discovery.go:32-33`, `50`) treats a parse error as "skip like any
other unparseable key," which is exactly the silent-permanent-drop behavior #495 says NOT to
replicate for VCNT. See Requirement 6 below — this is the single most important thing to get
right in the tempo-side implementation.

### Requirement 2 — Is tempo's VCNT L0 writer already emitting v2-ranged filenames? YES, confirmed live.

`tempodb/encoding/vblockpack/vcntwriter.go` (`flush`, lines 158-187):
```go
minSec, maxSec := blockpack.VCNTRecordTimeRange(records)
...
key := blockpack.VCNTObjectKeyV2(tenant, indexPrefix, colName, id, minSec, maxSec)
```
This is the ONLY VCNT writer found in tempo (no other `.vcnt` PUT call site exists —
`grep -rn "VCNTObjectKey\b" .` (v1, no "V2" suffix) returns zero hits outside old test fixtures).
So every VCNT L0 file tempo writes today is already v2-ranged. Combined with the
`valuecountscompactor` merge path (blockpack's compactor, confirmed at
`blockpack/internal/modules/valuecountscompactor/service.go:236`: `minSec: meta.WallMinSec, maxSec:
meta.WallMaxSec`) which also preserves/derives v2 ranges on merge, this means v1-shaped
stragglers are a **shrinking, bounded population from before #494 shipped**, not an ongoing
write path — exactly what the brainstorm-prompt's Requirement 2 asked to verify rather than
assume. This affects effort allocation: the "unknown range, always fetch" fallback path is a
correctness-critical safety net for a genuinely temporary population, not a permanent steady-state
concern, so it deserves to be dead simple (one `if err != nil { return true }`) and well-tested,
not an elaborate secondary code path.

### Requirement 3 — Exact pruning insertion point at both call sites

**`modules/frontend/vcnt_fetch.go:fetchVCNTSection`** (lines 73-115):
- Signature TODAY: `fetchVCNTSection(ctx, rawR backend.RawReader, tenant, indexPrefix string, dims
  []string) (data []byte, dir []VCNTChunkDirEntry, filesCount int, bytesRead int64)` — **no
  minTS/maxTS parameters at all**. Its only caller, `buildQueryPlanFromProgram` (line 255), already
  has `minTS, maxTS uint64` in scope (they're its own params) but does not thread them through.
  **This requires a signature change** (add `minTS, maxTS uint64` params) plus updating the one
  call site (`vcnt_fetch.go:255`) and every test call site in `vcnt_fetch_test.go` (7 call sites
  found: lines 77, 98, 119, 134, 141, 155-ish via `buildQueryPlan`, 209, 230).
- Insertion point: inside the `for _, k := range keys` loop (line 94), where `keypath, name :=
  splitObjectKey(k)` already computes the bare leaf filename (line 95) — the exact string
  `VCNTParseFilenameV2` needs. Filter there, BEFORE the `rawR.Read(ctx, name, keypath, nil)` call
  on line 96 — i.e. before any GET.

**`tempodb/encoding/vblockpack/cube_backfill.go:buildVCNTSection`** (lines 111-151):
- Signature TODAY already has minTS/maxTS: `buildVCNTSection(ctx, store valueIndexStore, tenant
  string, dims []string, _, _ uint64)` — the params exist but are literally named `_, _` (i.e.
  the compiler enforces their disuse) with the doc comment explicitly declaring "not used to
  prune the file list" (lines 81-84). **No signature change needed here** — just rename `_, _` to
  `minTS, maxTS` and use them.
- Insertion point: inside the `for _, k := range keys` loop (line 126), immediately after the
  existing `if path.Ext(k) != ".vcnt" { continue }` check (line 127) and BEFORE `data, getErr :=
  store.Get(ctx, k)` (line 130) — i.e. before any GET. `path.Base(k)` gives the leaf filename
  `VCNTParseFilenameV2` needs (keys here are full store keys, same shape as `fetchVCNTSection`'s
  `k`).

Both call sites confirm the window and the file listing are available together, before any GET,
at exactly the point the issue's Requirement 3 asked to verify — no restructuring beyond the
signature/rename above is needed.

### Requirement 4 — Do VI's IsInTimeRange semantics transfer directly, or does VCNT need different boundary semantics?

**They transfer directly, byte-for-byte identical formula, verified by reading both
implementations side by side:**

- VI (`internal/modules/valueindex/filename.go:112-116`):
  `return m.WallMaxSec >= queryMinSec && m.WallMinSec <= queryMaxSec`
- VCNT (`internal/modules/valuecounts/filename.go:114-117`):
  `return m.WallMaxSec >= queryMinSec && m.WallMinSec <= queryMaxSec`

Both are inclusive-on-both-ends interval overlap tests: `[WallMinSec, WallMaxSec] ∩
[queryMinSec, queryMaxSec] ≠ ∅`. This exactly matches the plan.md-established formula the
brainstorm-prompt quotes for #494 (`WallMaxSec >= queryMinSec && WallMinSec <= queryMaxSec`).

The brainstorm-prompt raises a legitimate concern worth confirming rather than assuming — VCNT
records are aggregates over a per-minute bucket window, not point-in-time spans like VI's
per-span records — but this does NOT change the FILE-level overlap check's boundary semantics.
Investigating why: a VCNT file's `[WallMinSec, WallMaxSec]` is computed by
`blockpack.VCNTRecordTimeRange` (`vcnt.go:119`, re-exporting `valuecounts.TimeRange`) which scans
every record's `TimeStart`/`TimeEnd` and returns `(min(TimeStart), max(TimeEnd))` across the whole
file — i.e. the file's declared range is already the union of all its constituent aggregate
windows, collapsed to one interval. Once collapsed to a single `[min,max]` interval, "does this
file possibly contain relevant records" is identical in shape to VI's "does this file possibly
contain relevant spans" — same interval-overlap question, same formula, regardless of what's
inside (points vs. per-minute buckets). The internal aggregate-window structure only matters for
the RECORD-level pruning that already happens post-fetch, decoded, inside
`valuecounts.SelectivityInRange`/`ColumnTotalInRange` (`blockpack/internal/modules/queryplan/
vcnt_cost.go:25,52`) — a separate, complementary, already-shipped layer this task does not touch.
**Conclusion: reuse `VCNTFileMeta.IsInTimeRange` exactly as VI uses its own `FileMeta.IsInTimeRange`
— no adjustment needed.**

### Requirement 5 — Shared helper vs. duplicated inline logic

Both call sites conceptually do "list → parse-and-filter → fetch," but their surrounding list/get
abstractions differ in shape (`backend.RawReader.Find`+callback+`Read(name, keypath)` vs.
`valueIndexStore.List`+`Get(fullKey)`), and `vcnt_fetch.go`'s own doc comment (lines 1-12)
explicitly, deliberately keeps `fetchVCNTSection` unmerged with `cube_backfill.go`'s
`buildVCNTSection` for a real, load-bearing reason: "different lifecycle: querier-side is a
long-lived per-process cache serving many concurrent block-jobs; this runs once per query at plan
time." Unifying the FULL fetch loops into one shared function would fight that documented,
deliberate decision.

However, the filename-decision itself — "given this leaf filename and the query window, should I
fetch this file?" — is a small, pure, store-independent function with no lifecycle coupling at
all. It is exactly the kind of correctness-critical logic (never produce a false negative) that
benefits from living in ONE place, unit-tested once, rather than being hand-copied at two call
sites where a future edit could silently invert the error-handling polarity at only one of them
(this codebase has direct precedent for that exact failure mode: NOTE-VI-095 was a live
data-loss incident caused by a check that existed in one sibling function but was missing/wrong
in the other).

Both call sites already import (or can trivially import) a common package that can host this
helper: `modules/frontend/vcnt_fetch.go` already imports both `github.com/grafana/blockpack`
directly (line 25) AND `github.com/grafana/tempo/tempodb/encoding/vblockpack` (line 27, for
`CheckIndexCoverage`) — so `vblockpack` is already an acceptable import for `modules/frontend`,
and `cube_backfill.go` already lives inside `vblockpack`. **A tiny exported helper added to
`vblockpack` (its own new small file, e.g. `vcnt_prune.go`) is importable from both sites without
adding a new dependency edge or fighting the deliberate non-unification of the two full fetch
loops.**

### Requirement 6 — False-negative risk analysis (the critical correctness question)

**Primary risk identified: replicating blockpack's own VI-side mistake (NOTE-VI-030) on the VCNT
side.** `internal/modules/valueindex/discovery.go:32-33` treats any `ParseFilenameV2` error
(including a plain v1-shaped legitimate old filename) as "skip like any other unparseable key" —
this is blockpack's OWN internal S3-object discovery, and it silently, permanently drops
pre-#494-generation files from ever being considered, forever, unless a merge happens to touch
them. The brainstorm-prompt is explicit that #495 must NOT do this. Confirmed directly by reading
`valuecounts.ParseFilenameV2`'s doc comment (identical "no v1 fallback" wording to VI's) — the
VCNT parser is just as ready to be misused this way as VI's was; the discipline has to live in
the TEMPO-SIDE CALLER, not in the parser (the parser correctly signals "I don't know" via an
error either way, and I don't know is not evidence of absence).

**The exact wrong code (do not write this):**
```go
meta, err := blockpack.VCNTParseFilenameV2(name)
if err != nil {
    continue // WRONG — drops v1/malformed files exactly like NOTE-VI-030
}
if !meta.IsInTimeRange(minSec, maxSec) {
    continue
}
```

**The correct code:**
```go
meta, err := blockpack.VCNTParseFilenameV2(name)
if err == nil && !meta.IsInTimeRange(minSec, maxSec) {
    continue // only skip when we POSITIVELY know the file doesn't overlap
}
// err != nil (v1-shaped or malformed) falls through and is always fetched
```

**Boundary-condition risks checked against the confirmed formula
(`WallMaxSec >= queryMinSec && WallMinSec <= queryMaxSec`):**
- Single-point query window (`minSec == maxSec`, e.g. an exact-timestamp lookup): a file whose
  range exactly touches that point at either edge (`WallMaxSec == queryMinSec` or `WallMinSec ==
  queryMaxSec`) is correctly INCLUDED — both comparisons are `>=`/`<=`, not strict — matching VI's
  existing, already-shipped behavior.
- Zero-width file range (a file whose single flush only ever touched one minute bucket,
  `WallMinSec == WallMaxSec`): still a valid degenerate interval, no special-case needed — the
  same two-sided comparison handles it correctly (confirmed: this is the common case for a
  freshly-flushed L0 VCNT file per `vcntwriter.go`'s per-block-flush cadence).
- Integer overflow / malformed numeric fields: `ParseFilenameV2` already parses via
  `strconv.ParseUint(..., 64)` and separately rejects `minSec > maxSec` as a hard error
  (`internal/modules/valuecounts/filename.go:106-109`) — any such malformed file already falls
  into the `err != nil` "always fetch" branch, never silently misparsed into a wrong range.
- **Secondary, adjacent risk (do not need to fix, but should verify doesn't already exist here):**
  NOTE-VI-095 (`internal/modules/valueindex/NOTES.md:1730`) documents a live incident where
  `strings.TrimSuffix` silently no-ops on a wrong-extension name, letting a `.vcnt` file parse as
  a `.blockpack` VI filename. Checked: `valuecounts.ParseFilenameV2` (the VCNT parser this task
  will call) already has the equivalent `if base == name { return err }` guard
  (`internal/modules/valuecounts/filename.go:78-80`), fixed as part of that same incident. No gap
  here — nothing to fix, just confirming the fix already covers the function #495 depends on.
- **No false-negative risk from staleness of the listing itself**: neither call site caches its
  listing today (frontend's `Find` and `buildVCNTSection`'s `List` are both live, uncached calls
  each invocation) — pruning only changes which of the LISTED files get GET'd, it cannot cause a
  file to be missing from the listing in the first place. (VI's `IndexFileCache` add/remove/TTL
  staleness concerns from `filecache.go` are a different, unrelated caching layer not present on
  either VCNT call site — out of scope for this task, and should not be conflated with it.)

### Architecture Observations

- The frontend and querier VCNT-fetch paths are intentionally kept separate (documented,
  deliberate — see Requirement 5); this pruning task should respect and reinforce that boundary,
  not erode it.
- `vcnt_cost.go`'s record-level VCNT pruning (post-fetch, decoded) and this task's file-level
  pruning (pre-fetch, filename-only) are two independent, complementary layers — the latter is
  strictly about avoiding unnecessary GETs, the former about avoiding unnecessary work on data
  already in hand. Neither subsumes the other; both stay after this change.
- Tempo already vendors the exact blockpack commit this task needs; no go.mod/vendor changes are
  required.

### Dependencies

No new dependencies. Everything needed (`blockpack.VCNTParseFilenameV2`, `blockpack.VCNTFileMeta`,
`(*VCNTFileMeta).IsInTimeRange`) is already vendored and already imported by at least one of the
two target files (`vcnt_fetch.go` already imports `github.com/grafana/blockpack`;
`cube_backfill.go` already imports it as `blockpack "github.com/grafana/blockpack"`).

### Test Patterns

Both call sites have existing table/scenario-style unit tests (`modules/frontend/
vcnt_fetch_test.go`, `tempodb/encoding/vblockpack/cube_vcnt_fetch_test.go`) built against
in-memory fakes (`newLocalRawReadWriter`, `memVCNTStore`) — no live S3/minio needed. Existing
tests already assert "ignores non-.vcnt keys" (`TestBuildVCNTSection_IgnoresNonVCNTKeys`) and
"no coverage returns nil" (`TestBuildVCNTSection_NoCoverageReturnsNil`) — the natural place to add
sibling tests: "v1-shaped/malformed filename is still fetched" and "out-of-range v2 filename is
skipped, in-range v2 filename is fetched." `fetchVCNTSection`'s test file also has a
`countingRawReader` wrapper (line ~160) already built for asserting zero-I/O in a different
scenario — the same pattern is directly reusable to assert pruned files are never `Read` (not just
"absent from the returned section," which a bug could satisfy accidentally by filtering
post-fetch instead of pre-fetch).

### Spec-Driven Modules in Scope

Checked `modules/frontend/` and `tempodb/encoding/vblockpack/` (tempo repo) for SPECS.md/
NOTES.md/TESTS.md/BENCHMARKS.md and for the `// NOTE: Any changes to this file must be reflected
in the corresponding specs.md or NOTES.md.` invariant comment in any `.go` file — none found.
Tempo itself is not spec-driven in this convention; that discipline lives entirely on the
blockpack side (`internal/modules/valuecounts/`, `internal/modules/valueindex/`, both of which DO
carry it, e.g. `internal/modules/valuecounts/filename.go:3` and
`internal/modules/valueindex/filename.go:3`). Those blockpack-side invariants (no-v1-fallback
parsing, hard `.vcnt`/`.blockpack` suffix checks) were read directly above as load-bearing
constraints on this task's design, but this task itself makes no blockpack-side edits, so no
blockpack SPECS.md/NOTES.md update is anticipated as part of #495's own scope.

## 2026-07-10 21:35:00 - Approaches Considered

### Approach 1: Shared pure filename-decision helper in `vblockpack`, called from both sites

**Description:** Add one small, unexported-or-exported function (e.g.
`vblockpack.vcntShouldFetch(name string, minSec, maxSec uint64) bool` or exported
`VCNTFileOverlapsRange` if a future third caller might need it) in a new small file in
`tempodb/encoding/vblockpack/`. It wraps `blockpack.VCNTParseFilenameV2` +
`meta.IsInTimeRange`, with the "err != nil → always include" safety rule baked in once. Both
`cube_backfill.go:buildVCNTSection` (same package, calls it directly) and
`modules/frontend/vcnt_fetch.go:fetchVCNTSection` (already imports `vblockpack` for
`CheckIndexCoverage`, calls the exported form) use it at their respective per-file loop points
identified in Requirement 3.

**Pros:**
- The single most correctness-sensitive line (`err != nil → don't skip`) is written and tested
  exactly once — directly addresses the NOTE-VI-095-style risk of the same check silently
  diverging between two hand-copied call sites over time.
- No new package, no new dependency edge (`modules/frontend` already imports `vblockpack`).
- Small, easily unit-tested in isolation with pure string/uint64 inputs — no store fakes needed
  for the decision logic itself (though the call-site integration tests still need the existing
  fakes).
- Does not touch or weaken the deliberate frontend/querier fetch-loop separation documented in
  `vcnt_fetch.go`'s own header comment — only the tiny filename-decision predicate is shared, not
  the loop, the store abstraction, or the lifecycle.

**Cons:**
- One more small file/symbol in `vblockpack`'s public-ish surface (if exported) — minor surface
  growth, though within tempo's own codebase, not blockpack's public API (no blockpack-side
  approval concern).
- Slightly less locality: a reader of `vcnt_fetch.go` has to follow one more jump to see the full
  skip logic, vs. reading it inline.

**Fits existing patterns:** Yes — this mirrors how VI's own `IsInTimeRange`/`FilesForTimeRange`
already centralizes its decision logic in `filecache.go`/`filename.go` rather than re-deriving it
at each VI call site, and mirrors this same codebase's own precedent of pulling a check into one
place after NOTE-VI-095 showed the cost of not doing so.

### Approach 2: Duplicate the ~4-line filename-decision inline at each call site

**Description:** Write the identical `err == nil && !meta.IsInTimeRange(...)` skip check directly
inside both `buildVCNTSection`'s and `fetchVCNTSection`'s existing per-file loops, with no new
shared symbol. Each file already imports `blockpack` directly, so no new import is even needed.

**Pros:**
- Maximum locality — the entire fetch/skip decision for a given call site is visible in one
  function, no jump required.
- Zero new files, minimal diff footprint.
- Consistent with `vcnt_fetch.go`'s own stated philosophy of deliberately NOT sharing code with
  `cube_backfill.go` due to differing lifecycles — this extends that philosophy uniformly rather
  than carving out one shared exception.

**Cons:**
- Directly recreates the exact class of risk NOTE-VI-095 documents: two copies of a
  correctness-critical "treat error as inclusive, not exclusive" check, with no mechanism
  preventing them from silently drifting apart on a future edit (e.g. someone "cleans up" one
  call site's error handling without realizing the polarity matters).
- Marginally larger overall diff (duplicated logic + duplicated tests, since the two behaviors
  would need independent test coverage anyway to catch drift).

**Fits existing patterns:** Partially — consistent with the lifecycle-separation philosophy for
the FETCH LOOPS, but inconsistent with how VI itself already centralizes its OWN identical
decision logic (`FileMeta.IsInTimeRange`) rather than re-deriving the overlap formula at each VI
call site.

### Approach 3: Push the decision into blockpack itself as a new exported helper

**Description:** Ask blockpack to add something like `blockpack.VCNTFileShouldInclude(name
string, minSec, maxSec uint64) bool` (err-swallowing, always-safe-default built in) as a new
public API function, so tempo never has to reason about the parse-error case at all.

**Pros:**
- Centralizes the safety rule at the true source of truth (blockpack owns the filename format and
  every other consumer of it).
- Slightly terser call sites.

**Cons:**
- Violates this task's explicit constraint: "no blockpack changes should be needed unless
  investigation reveals a genuine gap ... flag it explicitly rather than silently deciding to add
  blockpack code." Investigation (Requirement 1) found NO gap — `VCNTParseFilenameV2` +
  `IsInTimeRange` are already sufficient and already exported; there is nothing missing that
  would justify growing blockpack's public API (which also requires explicit user permission per
  blockpack's own CLAUDE.md: "Do not add new public API surface without explicit user
  permission").
- Blurs the responsibility boundary: "should we drop this file" is a caller policy decision (VI's
  own callers, e.g. `discovery.go`, make the OPPOSITE choice on this exact question — drop on
  error), not something the filename format's own parser should decide for every consumer.

**Fits existing patterns:** No — blockpack's existing `IsInTimeRange` is deliberately a pure,
error-free predicate on an already-successfully-parsed `FileMeta`; the parse-error handling
policy is left to each caller by design (VI's own two callers — `discovery.go` skip-on-error vs.
`filecache.go`'s `AddFile`/`listColumn` also skip-on-error, i.e. VI is internally consistent, just
consistently making the choice #495 says not to make for VCNT). Rejected — also directly
forbidden by this task's own constraints.

## 2026-07-10 21:40:00 - Recommendation

### Chosen Approach: Approach 1 — shared pure filename-decision helper in `vblockpack`

**Rationale:**
- Directly serves the single most important correctness requirement (Requirement 6): one
  canonical, unit-tested implementation of "err != nil → never skip" removes the chance of the
  two call sites silently diverging on this exact point in a future edit — this codebase has a
  concrete precedent (NOTE-VI-095) for how costly that divergence can be when it happens to a
  correctness-critical parse/skip check duplicated across sibling call sites.
- Zero new dependencies: `modules/frontend/vcnt_fetch.go` already imports `vblockpack` for
  `CheckIndexCoverage`, so adding one more imported symbol from the same package costs nothing
  structurally.
- Respects the existing, deliberate, documented separation between the frontend's and querier's
  full fetch LOOPS (Requirement 5) — only the tiny store-independent decision predicate is
  shared, which was never part of the reason those loops are kept separate in the first place
  (their separation is about lifecycle/caching, not about this filename check).
- No blockpack changes, consistent with the task's explicit tempo-only constraint and
  Requirement 1's finding of no gap.

**Implementation Strategy:**
1. Add a new small file in `tempodb/encoding/vblockpack/` (e.g. `vcnt_prune.go`) with an exported
   function, something like:
   ```go
   // VCNTFileOverlapsRange reports whether the .vcnt file named name should be fetched for the
   // query window [minSec, maxSec]. A v1-shaped or otherwise unparseable name returns true
   // (unknown range — always fetch, never drop; see NOTE-VI-030 for the mistake this avoids).
   func VCNTFileOverlapsRange(name string, minSec, maxSec uint64) bool {
       meta, err := blockpack.VCNTParseFilenameV2(name)
       if err != nil {
           return true
       }
       return meta.IsInTimeRange(minSec, maxSec)
   }
   ```
2. In `cube_backfill.go:buildVCNTSection`: rename the ignored `_, _ uint64` params to `minSec,
   maxSec uint64`; insert `if !VCNTFileOverlapsRange(path.Base(k), minSec, maxSec) { continue }`
   right after the existing `.vcnt` extension check, before `store.Get`. Update the stale doc
   comment (lines 81-84) to describe the new pruning behavior and the unknown-range fallback,
   replacing the now-false "carry no embedded time range ... all of a column's .vcnt files are
   fetched" claim.
3. In `modules/frontend/vcnt_fetch.go:fetchVCNTSection`: add `minTS, maxTS uint64` parameters;
   thread them from the one real call site (`buildQueryPlanFromProgram`, which already has both
   in scope). Insert `if !vblockpack.VCNTFileOverlapsRange(name, minTS, maxTS) { continue }` right
   after `keypath, name := splitObjectKey(k)`, before `rawR.Read`.
4. Update every existing test call site for both functions to pass a time window (use a window
   that exercises both the "in range" and "v1/malformed → always included" cases, not just
   pass-through unchanged values that would mask a bug).
5. Add new regression tests per call site: (a) a v2 filename outside the window is not fetched
   (assert via a counting wrapper that `Read`/`Get` was never called for it, not just that it's
   absent from the output — this is the same "assert on the mechanism, not just the outcome"
   discipline noted in this project's own microbench/allocation-testing conventions), (b) a
   v1-shaped or garbage filename IS still fetched regardless of the window, (c) boundary-touching
   windows (`WallMaxSec == queryMinSec`, `WallMinSec == queryMaxSec`) are still included.
6. Update `cube_backfill.go`'s stale comment (required regardless of the rest, per the issue's own
   Work Item 3).

**Key Decisions:**
- **Fallback polarity is unconditional and lives in exactly one function** — `err != nil` always
  returns `true` (fetch), never gated behind any config/flag/environment check. This is a hard
  correctness rule, not a tunable.
- **No caching added.** Neither call site caches its listing today, and this task does not
  introduce one — consistent with `vcnt_fetch.go`'s own documented team-lead ruling that a cache
  is a measurement-driven follow-up, not something to bundle into an unrelated correctness fix.
- **`fetchVCNTSection` gets a signature change (new params)**; `buildVCNTSection` does not (its
  params already exist, just unused) — asymmetric effort between the two call sites, correctly
  reflecting their asymmetric starting state.

**Risks Identified:**
- **False negative from a flipped `err` check** (the core risk, Requirement 6): mitigated by
  centralizing the check in one tested function (this recommendation's whole point) plus adding
  an explicit "v1/malformed filename is still fetched" regression test at BOTH call sites'
  integration-test level, not just the helper's own unit test — so a future edit to either call
  site that bypasses the helper (e.g. someone inlines the logic "for a small optimization" and
  gets the polarity backward) is still caught.
- **Test-fixture drift**: existing tests for both call sites currently don't exercise any
  meaningful time window (some pass `0, 200` or similar placeholder values) — after this change
  those placeholder values become load-bearing. Mitigation: audit every existing call site
  (`grep -n "buildVCNTSection\|fetchVCNTSection"`) during implementation to confirm no existing
  test's placeholder window accidentally excludes fixture data it currently expects to see.
- **Silent breakage if the helper is added to `vblockpack` but `modules/frontend` forgets to
  import/use it correctly** (e.g. passes the wrong argument order) — mitigated by keeping the
  function signature's argument order identical to `IsInTimeRange`'s own established
  `(queryMinSec, queryMaxSec)` order, and by the boundary regression tests in item 5 above.

**Open Questions:**
- Whether `VCNTFileOverlapsRange` should be exported (capitalized) or whether `modules/frontend`
  should instead get its own private copy that calls the same underlying `blockpack` primitives
  directly (skipping the `vblockpack` hop entirely). Assumption made here: exporting it from
  `vblockpack` is preferable since it is the one package both call sites already have access to
  and it keeps the safety-critical logic in exactly one place — but this is a naming/placement
  detail for the planner to confirm, not a design fork that changes correctness.
- Exact final name (`VCNTFileOverlapsRange` vs. something else) — cosmetic, left to the planner.

## 2026-07-10 21:42:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Recommendation:** Approach 1 — shared pure filename-decision helper (`VCNTFileOverlapsRange`)
added to `tempodb/encoding/vblockpack`, called from both `cube_backfill.go:buildVCNTSection` and
`modules/frontend/vcnt_fetch.go:fetchVCNTSection`, with an unconditional "parse error → always
fetch" safety rule and boundary/regression test coverage at both call sites.
**Next Phase:** PLAN

Ready for workflow-planner agent to create detailed implementation plan.
