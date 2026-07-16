# Cube Module Design Notes

## NOTE-CUBE-002: Dictionary is per-file scope, not global

**Date:** 2026-06-29
**Decision:** Each cube file carries its own self-contained `Dictionary` (string↔uint16 per
dimension); dictionaries are never shared across files or persisted externally.

**Rationale:**

- A cube file must be independently readable without a side-channel lookup service — per-file
  scope means `OpenReader`/`Reader.GetCell` never need anything beyond the file's own bytes.
- `uint16` IDs keep the `Cell` record at a fixed 12 bytes (`SPEC-CUBE-001`); a global dictionary
  would need either a wider ID (larger cells) or an out-of-band mapping step per read.
- Real dictionaries stay small in practice: `CardinalityGate`'s default `MaxDistinctPerDim`
  (1000) is enforced at cube-creation time (`SPEC-CUBE-013`), far below the `uint16` ceiling of
  65535, so rebuilding the reverse maps at decode time (`DecodeDictionary`) is cheap.
- Rollup/compaction (`SPEC-CUBE-016`) merges dictionaries from multiple input files by resolving
  each input's local IDs back to strings and re-interning into the output file's own fresh
  dictionary — per-file scope makes this merge straightforward (no ID-collision resolution
  needed across inputs, since every input's IDs are only ever compared as strings).

**Back-ref:** `internal/modules/cube/dict.go:Dictionary`; cross-referenced from
`NOTE-CUBE-007`'s accumulator-side double-intern discussion.

---

## NOTE-CUBE-004: Snappy Chunking at 2048 Cells

**Date:** 2026-06-29  
**Decision:** Chunk size set to 2048 cells nominal (24 KB raw → ~8-12 KB compressed).

**Rationale:**

- Matches value index precedent (`internal/modules/valueindex/entries.go` uses 2048 entries/chunk)
- VCNT uses 4096 records/chunk (precedent for snappy-chunked sections)
- 2048 cells × 12 bytes = 24 KB raw fits comfortably in L2 cache (~256 KB)
- Snappy decompression of 8-12 KB chunks is fast (~50-100 μs, proven in benchmarks)
- Balances decompression overhead (too small → many decompress ops) vs memory footprint (too large → high RSS)

**Alternative considered:** 512, 1024, 4096 cells. 512 too small (excessive chunk directory overhead), 4096 too large (memory pressure for sparse queries).

**Measurement plan:** BENCH-CUBE-002 will benchmark varying chunk sizes (512, 1024, 2048, 4096) to validate choice.

---

## NOTE-CUBE-005: Binary Search (O(log n)) vs Arithmetic (O(1))

**Date:** 2026-06-29  
**Decision:** Use binary search on sorted cells (O(log n)) for random access, not arithmetic on dense grid (O(1)).

**Rationale:**

- **Sparse storage dominates:** At realistic 1-10% fill, sparse 12B cells save 85-98% storage vs dense 8B cells (see brainstorm storage calculations).
- **O(log n) acceptable latency:** Binary search over 10k cells = ~14 comparisons = 10-50 μs. Query latency dominated by S3 GET (50-100ms), so binary search is <0.1% overhead.
- **Chunk directory amortizes:** Range queries (`GetCellsInRange`) scan multiple adjacent cells after one binary search on the directory.
- **Proven pattern:** SpanTree (`internal/modules/blockio/shared/constants.go:140-155`) uses fixed-stride records + binary search successfully at large scale.

**Alternative considered:** Dense 8-byte position-derived cells (ticket #442 original proposal). Rejected because:

1. Contradicts lth prior decision (memory `6cff0daf`: "sparse data with gaps break position-based indexing")
2. Wastes 85-98% storage at realistic sparse fill
3. Ticket's 6.9 GB/30d storage number implies high cardinality (not 100×20 dense), reinforcing sparse is correct

**Measurement plan:** BENCH-CUBE-003 will measure GetCell latency (target: <50 μs for 10k cells, cold cache).

---

## NOTE-CUBE-006: Sort Order (Minute First, Then Dimensions)

**Date:** 2026-06-29  
**Decision:** Sort cells by `(Minute ASC, Dim1ID ASC, Dim2ID ASC)`.

**Rationale:**

- **Time-first for chunk pruning:** Chunk directory stores `min_minute` per chunk. Sorting by minute first enables range queries to skip entire chunks where `min_minute > max_query_minute` (O(log chunks) scan, not O(chunks) linear scan).
- **Secondary sort on dimensions for merge:** During compaction (L0 → L1 → L2), merging multiple files with the same sort order is a standard multi-way merge-sort (proven in VCNT `internal/modules/valuecounts/compaction.go:15-85`).
- **Matches VCNT precedent:** VCNT sorts by `(ColumnName, TimeStart, Value, Count)` — time is the primary key for range-based pruning.

**Alternative considered:** `(Dim1ID, Dim2ID, Minute)` (dimension-first). Rejected because chunk pruning by minute is the dominant query pattern (time-bounded metrics queries like `rate_over_time[5m]`).

**Query implications:** Queries like "all cells for service=auth, status=200, time=[T1..T2]" benefit from sorted-by-minute order:

1. Binary search chunk directory for chunks in [T1, T2] range
2. Decompress only those chunks (not whole file)
3. Within-chunk binary search for (minute, dim1_id, dim2_id) tuples

---

## NOTE-CUBE-007: Accumulator is single-minute; the caller rotates buckets

_Added: 2026-06-29 (issue #443)_

The accumulator is deliberately scoped to **one** minute bucket, fixed at construction. The
ingest loop (out of scope for #443) owns one accumulator per active cube and, on each
wall-clock minute boundary (and on shutdown), calls `FlushTo` — which writes the file and
`Reset`s the accumulator to the next minute. Keeping the minute out of the per-span hot path
means `Add` does zero time math; the bucket is implicit.

`Add` interns dimension strings into the accumulator's own `Dictionary` and counts into a
`map[cellKey]uint32` keyed by the interned `(dim1_id, dim2_id)`. At `Encode`, IDs are resolved
back to strings and fed to a fresh `cube.Writer`, which re-interns them into the per-file
dictionary. The double-intern is intentional: in-memory IDs only need to be internally
consistent for counting, and re-interning once per minute (not per span) is negligible while
keeping the file's dictionary self-contained (NOTE-CUBE-002, per-file scope).

`cube.Writer` gained `Encode() ([]byte, error)` so the accumulator can produce bytes for the
object store without a temp file; `Flush(path)` now calls `Encode` then writes atomically.

**Back-ref:** `internal/modules/cube/accumulator.go:Add,Encode,FlushTo,Reset`

---

## NOTE-CUBE-009: Registry conditional-PUT retry — 5 attempts, 50ms base, doubling backoff

**Date:** 2026-06-29 (issue #444)
**Decision:** `Registry.Add`/`Registry.Remove` retry a conditional-PUT conflict (`ErrConflict`,
S3 412-equivalent) up to 5 times, with backoff starting at 50ms and doubling each attempt
(50ms, 100ms, 200ms, 400ms — 5th attempt returns the exhausted-retries error without a further
wait).

**Rationale:**

- Bounded retry count (SPEC-ROOT-001: no unbounded loops) — a persistently-contended tenant
  index fails loudly (a wrapped error) rather than retrying forever.
- Re-reading the index (`Load`) on every attempt (not just retrying the same stale write) is
  required for correctness: a conflict means another writer's version must be re-fetched before
  a meaningful re-check of "is my entry already present" / "is the tenant limit still open" can
  happen — a blind retry of the original payload could silently drop a concurrent writer's cube.
- Doubling backoff spreads out concurrent creators competing for the same tenant's index after a
  conflict, reducing the odds of a repeated collision on the very next attempt, without needing
  jitter for this scale (per-tenant cube registration is a low-frequency operation, not a hot
  path).
- The same 5-attempt/50ms-doubling shape is used identically by `Remove`, keeping the two
  mutation paths' retry behavior symmetric and easy to reason about together.

**Back-ref:** `internal/modules/cube/registry.go:Registry.Add,Registry.Remove`

**Addendum (2026-07-11, task #158): this retry loop now lives in `blobEntryStore.addEntry`/
`removeEntry`/`updateWatermarksEntry` (`entry_store.go`), moved verbatim out of
`Registry.Add`/`Remove`/`UpdateWatermarks`.** The 5-attempt/50ms-doubling shape described
above is UNCHANGED — see NOTE-CUBE-025 for why the move happened and SPEC-CUBE-014's own
`[UPDATED]` annotation for the resulting contract.

---

## NOTE-CUBE-010: Filter is part of the cube identity — root exports for external routing

_Added: 2026-07-06 (issue #480)_

`ComputeCubeID` has always hashed `tenant + sorted(dims) + sorted(filters)`, so the routing
key already differentiates two cubes that share group-by dims but differ in their originating
query filter (`{span.kind = server}` vs `{status = error}` vs `{}`). `RegistryEntry.Filters`
is persisted in `index.json`, and both `QueryRouter.Route` and `CreationTrigger.TryCreate`
match on the filter-aware `CubeID`. The correctness gap in #480 was **entirely on the tempo
side**: tempo passed `nil` filters to both `Route` and `TryCreate` because it only parsed the
`by(...)` group-by clause and never the `{...}` predicate — so a filtered query could silently
route to (or overwrite) an unfiltered cube.

The blockpack change is purely additive API surface so an external consumer (tempo) can
construct the filter half of the routing key:

- `CubeDefFilterOp` (alias of `cube.DefFilterOp`) + the `CubeDefFilterOp{GT,GTE,LT,LTE,EQ}`
  constants — needed to set `CubeColumnFilter.Op`, which was otherwise an unconstructible
  internal string type from outside the module.
- `CubeComputeID(tenant, dims, filters)` — exposes the routing-key computation so callers can
  precompute/verify a cube identity directly.

**Decision (issue #480, point 2):** "no filter" and "some filter" are intentionally *distinct*
cube identities. An unfiltered `{}` query produces an empty filter set (its own identity),
never a filter-independent base cube that queries post-filter against (that would be a much
larger design and is explicitly out of scope). Tempo's `extractFilters` therefore treats an
empty predicate as a valid zero-filter identity, and refuses (falls back to full scan) for any
predicate a cube cannot faithfully bake in (`!=`, regex, OR) rather than risk a filter mismatch.

**Back-ref:** `cube_ingest.go:CubeDefFilterOp,CubeComputeID`; `internal/modules/cube/definition.go:ComputeCubeID`.

---

## NOTE-CUBE-011: Log2Bucketize's ceiling bucketing must not be confused with executor's pow2Floor (issue #491)

**Date:** 2026-07-08
**Decision:** `cube.Log2Bucketize` (ceiling: smallest power of two ≥ v, excluding v<2 entirely)
and `internal/modules/executor/intrinsic_helpers.go`'s `pow2Floor` (floor: largest power of two
≤ v, defined for all v including ≤0) are two independent, un-unified histogram-bucketing
implementations in this codebase.

**Rationale:**

- `cube.Log2Bucketize` is a deliberate byte-for-byte port of tempo's own histogram algorithm
  (`pkg/traceql.Log2Bucketize`), chosen so cube-backed quantile queries produce numerically
  identical results to tempo's existing scan-based histogram path — any deviation would be a
  silent cross-backend inconsistency.
- `pow2Floor` predates this port and serves a different existing consumer (`metrics_trace.go`'s
  own histogram accumulation, NOTE-181) with its own floor-based bucketing convention and its own
  value-domain handling (an already-converted-to-seconds input, versus `Log2Bucketize`'s
  raw-value-then-ceiling).
- **This divergence is a recorded, PRE-EXISTING inconsistency between blockpack's two histogram
  implementations — not a defect introduced or being fixed by #491.** Team-lead confirmed via
  `plan-e.md` ruling 1 that unifying the two is out of scope; `cube`'s package-boundary constraint
  (must not import `tempo`, and stays independent of `executor` too, to remain a generic,
  dependency-free module) makes sharing one function impractical without a separate, larger
  package-restructuring decision outside this phase's scope.
- Anyone touching either implementation in the future must not assume they can be merged or that
  one is simply a bug-fixed version of the other — they intentionally solve different problems
  (tempo-parity vs. executor's own pre-existing histogram path) and are not on a migration path
  toward unification as far as any current spec text establishes.

**Back-ref:** `internal/modules/cube/bucket.go:Log2Bucketize`; `internal/modules/executor/intrinsic_helpers.go:pow2Floor`

---

## NOTE-CUBE-012: Cube identity — aggAttrs joins unconditionally; vetoed byte-identity/hash-preservation shim (issue #491)

**Date:** 2026-07-08
**Decision:** `ComputeCubeID`'s new fourth (`aggAttrs`) hash segment applies unconditionally, with
no special case for `len(aggAttrs)==0`. `RegistryEntry.AggAttrs` is never legitimately empty
(`duration` is mandatory in every cube's attribute set, ruling 3).

**Rationale:**

- The attribute SET changes cube identity — two cubes with the same dims/filters but different
  materialized attribute sets are genuinely different files with different content. The
  aggregation FUNCTION set does not change identity (every attribute in the set always gets
  count+sum+min+max+buckets computed uniformly), so only the attribute-NAME set, not the function
  list, needs to join identity.
- `computeDimsFiltersKey` was factored out as the single source of truth for the pre-existing
  3-segment hash so `ComputeCubeID` and the router's superset-matching grouping key (E-6b) can
  never independently drift apart — one shared helper, not two hand-synchronized copies.

**Alternative considered and REJECTED (vetoed by team-lead, second-round correction):** an earlier
plan draft special-cased `len(aggAttrs)==0` to reproduce the exact pre-#491 3-segment hash
byte-for-byte, framed as preserving existing pure-count cubes' `CubeID` across the upgrade.
Rejected for two independent reasons:

1. This project's standing no-backward-compat rule (reaffirmed by #490's flat, no-fallback
   removal of the old trace-by-id format) forbids designing compat shims for old data at all —
   there is no live legacy cube data whose identity needs preserving.
2. Even independent of that rule, the input the shim was meant to trigger on (`aggAttrs` empty)
   can never legitimately occur in the finished system — `duration` is mandatory in every cube's
   attribute set. A special case guarding an input that cannot occur is a compat shim with
   nothing left to be compatible with.

**Consequence:** `ComputeCubeID`'s hash is simply the 4-segment computation, always — no
degenerate/legacy code path exists or should be reintroduced. Any future reader tempted to
"restore" byte-identical CubeIDs for old count-only cubes should read this entry first: that
design was considered and explicitly rejected, not merely unconsidered.

**Back-ref:** `internal/modules/cube/definition.go:ComputeCubeID,computeDimsFiltersKey`

---

## NOTE-CUBE-013: DecodeHeader closes a latent version-validation gap (issue #491, E-3)

**Date:** 2026-07-08
**Decision:** `DecodeHeader` now rejects any file whose `Version` byte does not equal
`VersionCube`, returning a typed error rather than proceeding to parse the rest of the header.

**Rationale:**

- **This was a real, pre-existing latent gap, not something wire-format v2 introduced.**
  `DecodeHeader` previously validated ONLY the magic number, never the version byte. A v1 file's
  byte 5 (reserved, always zero in v1) would have been silently misread as a `NumAggAttrs` count
  once byte 5 was repurposed for that field in v2 — a v1 file happened to decode "successfully"
  with `NumAggAttrs=0` purely by coincidence (v1's reserved byte was always zero), but the
  validation itself was always absent; this task made the absence visible and fixed it, rather
  than the absence being a v2-only bug.
- Follows this project's `SPEC-ROOT-013` precedent exactly: a typed error for an old-format file,
  no dual reader — the project's standing no-backward-compat rule means there is no task in this
  phase (or planned) that adds a v1 decode path.
- Fixed opportunistically because this task already touches `file.go` for the `NumAggAttrs` field
  — not scope creep, but also not something a future task should assume was already true before
  E-3 landed.

**Back-ref:** `internal/modules/cube/file.go:DecodeHeader`

---

## NOTE-CUBE-014: AggCell is the sole cell type — Cell was deleted, not deprecated-and-kept (issue #491, task #40, "APPENDIX 2 compliance")

**Date:** 2026-07-08 (revised same-day; supersedes two earlier drafts describing since-abandoned
designs — a fully-parallel plain/full-fidelity codec pair, then a nested-`.Cell`-field
generalization — neither of which landed)

**Decision:** `Cell`/`EncodeCell`/`DecodeCell`/`CompareCell` were DELETED outright.
`AggCell` (with base fields flattened directly onto the struct) is the sole cell type used
everywhere — the earlier E-3 design, which nested a nominal `Cell` inside `AggCell` and kept a
fully separate plain-`[]Cell` codec pair alongside an `Agg`-suffixed full-fidelity pair, was
reversed by a team-lead binding ruling before it landed.

**Rationale:**

- Two independently-callable codec paths reading/writing the identical underlying bytes is a
  duplicated-maintenance-point risk.
- This is this project's standing no-backward-compat/one-format-version policy (`SPEC-ROOT-013`
  precedent) applied at the Go-type level: there is exactly one current cell representation, not
  a legacy-shaped one kept alongside a new one "just in case."
- `Writer.AddCell`/`Writer.AddAggCell` remain as two convenience METHODS over the one internal
  `AggCell`-based buffer — this preserves the ergonomic plain-count call site for callers that
  only ever need `numAggAttrs==0`, without reintroducing a second TYPE. The vetoed thing was type
  duality, not convenience-method duality.
- `Reader.GetCellsInRange` and `Reader.GetAggCellsInRange` are kept as two NAMES for the same
  reason — avoiding a purely mechanical rename ripple into an unrelated in-flight task's call
  site — not two implementations.

**Back-ref:** `internal/modules/cube/cell.go:AggCell`, `internal/modules/cube/writer.go:AddCell,AddAggCell`,
`internal/modules/cube/reader.go:GetCellsInRange,GetAggCellsInRange`

---

## NOTE-CUBE-015: SpanValues.Float64, the Min/Max sentinel, the mandatory-duration enforcement point, and the DurationColumn spelling contract (issue #491, E-4)

**Date:** 2026-07-08

**(a) Breaking interface change: `SpanValues.Float64`.** `SpanValues` gains `Float64(column
string) (float64, bool)`, the single numeric-extraction method every aggAttr computation uses.
Both production implementers in THIS repo were updated in the same change: `valueIndexSpanValues`
(`backfill.go`) currently returns `(0, false)` unconditionally — a deliberate stub, since real
numeric extraction from the value index is E-8's job, not E-4's. Tempo's own OTLP-span adapter
(`tempoSpanValues`) needed this method added on the tempo side too (done as part of subsequent
tempo-side wiring, E-10/E-11a).

**(b) Min/Max "no valid samples" sentinel convention.** `AggAttrValues.Min`/`Max` are
sentinel-initialized to `math.MaxFloat64`/`-math.MaxFloat64` (`newCellAggState`) — the same
convention `executor/metrics_trace.go`'s scan-path histogram already uses — so the first real
sample always wins the initial compare regardless of its sign or magnitude. `SampleCount==0` is
the signal downstream code (E-5's rollup merge, E-11a's response mapping) must check before
trusting `Min`/`Max`, to avoid leaking the sentinel values into output as if they were real data.

**(c) `duration`-always-in-`AggAttrs`: `validateDefinition` is the SOLE enforcement point.**
Per ruling 5(d) (the third-round clamp), `validateDefinition` (`accumulator.go`) is deliberately
the ONLY place this invariant is checked — called by `NewAccumulator` (this task) and by
`CreationTrigger.TryCreate` (`trigger.go`, E-7). E-3's own appendix explicitly REMOVED a redundant
wire-decode-time check that an earlier design had proposed, in favor of this single enforcement
point — two independent checks for the same invariant risk drifting out of sync if one is updated
without the other. Returns `*DefinitionError` (`Reason`+`Suggestion`), deliberately structured
identically to `CardinalityError` (`cardinality.go`) per this phase's error-family-consistency
rule — never a bare `fmt.Errorf` for a condition callers may want to `errors.As` against.

**(d) `DurationColumn` spelling contract: sourced from the shared constant, not re-invented.**
`DurationColumn = modules_shared.SpanDurationColumnName` (`internal/modules/blockio/shared/
constants.go`, value `"span:duration"`) — independently verified twice (once before
implementation, once during implementation) that this matches tempo's own
`tempoSpanValues.Int64` case and `pkg/traceql`'s intrinsic name for duration. Worth recording
explicitly: a bare re-invented literal `"duration"` (missing the `span:` prefix, or any other
plausible-looking spelling) would have been a SILENT-ZERO-ACCUMULATION bug — `Float64("duration")`
would simply always return `(_, false)` against real spans carrying the column as `"span:duration"`,
so every cube's duration aggregate would silently stay empty (`SampleCount` always 0) with no
error anywhere to surface the mismatch. This is exactly the kind of defect class Phase D's
TEST-VI-22/EX-36 real-write-path testing policy exists to catch.

**Back-ref:** `internal/modules/cube/accumulator.go:SpanValues,DurationColumn,validateDefinition,DefinitionError,newCellAggState`

---

## NOTE-CUBE-016: Rollup merge reuses AggAttrValues directly; associativity verified via genuine 3-input grouping, not a 2-input pass-through (issue #491, E-5)

**Date:** 2026-07-08

**Decision:** `MergedCell.AggAttrs` is typed `[]AggAttrValues` — the SAME type `cell.go`'s
`AggCell.Aggs` and `Writer.AddAggCell` already use — rather than a separate
"`AggAttrMergedValues`" type an earlier plan draft proposed.

**Rationale:**

- `AggAttrValues`'s shape (`SampleCount`/`Sum`/`Min`/`Max`/`Buckets`) already matches exactly what
  a merge needs to produce, and `Writer.AddAggCell` already consumes exactly this type — a second,
  parallel type would be a second, redundant maintenance point for the identical five fields
  (`SPEC-ROOT-009`'s single-source-of-truth principle, ruling 5, applied here the same way it was
  applied to `computeDimsFiltersKey`, E-6).
- The cross-resolution associativity claim (`L0→L1→L2` == `L0→L2` directly) is recorded as a
  documented invariant (`SPEC-CUBE-016`'s addendum), not just a proven-once test fact, because a
  future change to the merge function could silently violate it without a spec-level statement to
  check against.
- **Verification methodology is worth recording explicitly:** the associativity test
  (`TestRollup_L0ToL1ToL2_MatchesL0ToL2Direct`) deliberately uses a genuine 3-input grouping (two
  inputs pre-merged into an intermediate L1 file, combined with a third raw L0 input at L2, versus
  all three merged directly at L2 in one pass) rather than a 2-input pass-through, which would not
  actually exercise grouping-order independence. Mutation-verification confirmed this distinction
  matters: an "overwrite instead of accumulate" mutation was initially caught only after the
  fixture's input ORDER was redesigned between the two paths — an earlier fixture draft
  accidentally processed the same input last in both paths, letting the mutation slip through
  silently despite superficially testing "the same thing."
- `Reader.GetCellsRange`'s return type changed to `[]AggCell` as a direct, mechanical consequence
  of task #40's type unification (`NOTE-CUBE-014`) — not an E-5 design decision of its own; noted
  here only so a reader of `rollup.go` doesn't mistake the type change for something E-5 chose
  independently.

**Back-ref:** `internal/modules/cube/rollup.go:MergedCell,mergeAggAttrInto,finalizeAggAttrs,Rollup`,
`internal/modules/cube/rollup_test.go:TestRollup_L0ToL1ToL2_MatchesL0ToL2Direct`

---

## NOTE-CUBE-017: Backfill's numeric aggAttr extraction — Float64-absent convention, type-inference default, and the extended VI join (issue #491, E-8)

**Date:** 2026-07-08

**Decision 1 — `Float64` treats any parse failure as absent, never an error.**
`valueIndexSpanValues.Float64` parses the VI's string-typed `SourceRef` via `strconv.ParseFloat`;
a missing column or a non-numeric string both return `(0, false)`. Backfill has no way to
distinguish "not present" from "present but malformed" once a value is already stored as an
opaque VI string, and the accumulator already treats `Float64`-not-ok as "no sample for that
attribute" (E-4) — the correct behavior here too, not a special case.

**Decision 2 — `aggAttrDefsFor` defaults every non-duration column to `AggAttrTypeFloat64`.** A
bare registry column name (`RegistryEntry.AggAttrs []string`) carries no type tag — the
`AggAttrDef.Type` distinction (Int64/Duration vs Float64, ruling 1) only exists once a `Definition`
is actually constructed. `DurationColumn` is hardcoded to `AggAttrTypeInt64` (it's always present,
E-4/E-6); every other column defaults conservatively to `AggAttrTypeFloat64`. Consequence: a
non-duration numeric aggAttr backfilled this way gets `Sum`/`Min`/`Max`/`SampleCount` correctly
but never populates `Buckets[]` (no histogram/quantile support for that specific attribute via
backfill). This is NOT a new capability gap — ruling 1 already scopes bucketing to Int64/
Duration-typed attributes only, so a bare Float64-typed attribute would never get Buckets[]
regardless of how it was typed.

**Decision 3 — `lookupAggAttrValues` extends the existing 2-dimension join pattern to N aggAttr
columns**, not a new join mechanism. One `LookupColumn` call per aggAttr column not already
covered by a dimension; results joined back by `(TraceID, SpanID)` via `viEntryKey` — the exact
key shape the pre-existing dim1/dim2 code already used for its own cross-column join. This join
mechanism has a genuine real-write-path regression test
(`TestBackfill_RealWriteReadPath_JoinsByTraceAndSpanID_NotJustTrace`, `backfill_test.go`, fix
#46) — using the real `valueindex.Writer.AddEntryV4`→`Flush`→`OpenReader`→`Reader.Lookup` path,
not a hand-built fake, proving two spans sharing a trace with distinct `SpanID`s each get their
own attribute value without cross-contamination.

**Back-ref:** `internal/modules/cube/backfill.go:aggAttrDefsFor,lookupAggAttrValues,viEntryKey,valueIndexSpanValues.Float64`

---

## NOTE-CUBE-018: Cardinality byte-cost is a sizing heuristic, not a compat mechanism; TryCreate is validateDefinition's second, delegating enforcement point (issue #491, E-7)

**Date:** 2026-07-08

**Decision 1 — `MaxCombinedCellBytes`'s default is a sizing heuristic, explicitly not a
compatibility figure.** The default (`50_000 * 12 = 600,000` bytes) is derived arithmetically from
the pre-#491 `MaxCombinedCells` limit times the pre-#491 fixed cell size (12 bytes) — but this is
purely a convenient, familiar starting number for the NEW byte-cost check, not a preserved wire
constraint. No file format, hash, or byte layout from any prior version is being kept compatible
by this specific number; a future task could freely retune it without touching any format
concern.

**Decision 2 — `TryCreate` is `validateDefinition`'s SECOND call site, delegating rather than
duplicating (ruling 5(d), completing the enforcement-point plan E-4/`NOTE-CUBE-015` first
described).** `TryCreate(ctx, tenant, dims, filters, aggAttrs, ...)` calls
`validateDefinition(Definition{AggAttrs: aggAttrs})` as its literal first step — before any
registry I/O, before `ComputeCubeID`, before the cardinality gate. This is deliberately a pure
structural precondition check independent of registry state: an `aggAttrs` missing `duration` is
rejected the same way regardless of whether the tenant's registry is reachable, near its limit, or
empty. `RegistryEntry.AggAttrs` is populated from the validated `aggAttrs` at creation time —
previously always empty from this code path (no `aggAttrs` parameter existed at all before this
task).

**Back-ref:** `internal/modules/cube/cardinality.go:CardinalityLimits,CheckCardinality`,
`internal/modules/cube/trigger.go:TryCreate`

---

## NOTE-CUBE-019: Retention-decoupled L0 deletion — the plan's own deletion-condition text was backwards, corrected via mid-implementation checkpoint (issue #491, E-12a, ruling 4(a))

**Date:** 2026-07-08

**Decision:** `Compactor.Execute` defers deletion of a compaction's input files (via
`EvictAgedL0`, later) if and only if `plan.Level == RollupL1` — an L0→L1 rollup. Every other
`Execute` call (`RollupL0` pure-merge, `RollupL2` day-rollup) deletes its inputs immediately,
unchanged from pre-#491 behavior.

**Rationale:**

- Retention-decoupling exists so recent data stays queryable at MINUTE resolution even after
  being summarized into L1 — this only matters for the specific operation that elevates L0 data
  to a coarser resolution (the L0→L1 rollup). A pure L0-to-L0 merge doesn't change resolution at
  all (the merged output already serves identical minute-resolution needs), and an L1→L2 rollup
  is one level removed from L0 entirely — retention was never meant to extend that far.
- **The plan's own literal text for this condition was backwards** (`plan.Level != RollupL0`,
  which would have deferred deletion for BOTH the L0→L1 AND L1→L2 cases, incorrectly extending
  retention-decoupling to L1→L2 rollups where it doesn't belong). Caught and corrected via a
  mid-implementation checkpoint with planner-e, not discovered independently by the coder — worth
  recording explicitly so a future reader of the plan history isn't confused by the discrepancy
  between the plan's original wording and what actually shipped.
- `EvictAgedL0`'s dual condition (age AND watermark coverage) is deliberately conjunctive, not
  either-or: an L0 file that's old enough but not yet reflected in the L1 watermark must NOT be
  deleted (its data would become permanently unqueryable at any resolution until the rollup
  catches up), and a file that's already rolled up but still young must also survive (recent-data
  minute-resolution queries still need it).

**Back-ref:** `internal/modules/cube/compactor.go:Execute,EvictAgedL0,CompactorConfig`,
`internal/modules/cube/registry.go:UpdateWatermarks`

---

## NOTE-CUBE-020: Route's superset tie-break and resolution-completeness decline — why smallest-not-any, why decline-whole-not-partial (issue #491, E-6b)

**Date:** 2026-07-08

**Decision 1 — smallest covering superset, not any covering superset.** Preferring the SMALLEST
`AggAttrs` set that still covers `neededAttr` (rather than, say, the first match or the largest)
keeps routing predictable and keeps a query from paying for a wider cube's cardinality/byte
footprint than it needs — a cube with `{duration}` and a cube with `{duration, status_code}` both
cover a `neededAttr=""` count query, but the narrower one is the cheaper, more natural choice.
Ties break by newest `CreatedAt` (not oldest) so a query naturally migrates onto a freshly-created,
presumably-better-tuned cube once one exists, rather than staying pinned to an older definition.

**Decision 2 — decline the WHOLE query on incomplete watermark coverage, never a partial answer.**
Mirrors this project's structural-query precedent (Phase D's coverage-gap-vs-legitimate-miss
distinction, `NOTE-VI-072`): a query the cube cannot FULLY answer at the chosen resolution must
surface as a decline the caller can fall back from (to the value index), not a silently-partial
or mixed-resolution result set that looks complete but isn't. This is why the check compares
against the FULL `[watermarkMinute, queryMaxMinute]` window rather than accepting any overlap.

**Back-ref:** `internal/modules/cube/router.go:Route,smallestSupersetNewest`

---

## NOTE-CUBE-021: Registry-vs-file consistency is a distinct failure class from validateDefinition — cross-referenced explicitly (issue #491, E-6b, APPENDIX 3)

**Date:** 2026-07-08

**Decision:** `ValidateFileMatchesRegistry` is kept as its own function/error type, deliberately
NOT folded into or confused with E-4's `validateDefinition` (`NOTE-CUBE-015`), even though both
ultimately concern "does this cube's `AggAttrs` make sense."

**Rationale:**

- `validateDefinition` answers a BEFORE-the-fact question: is a proposed `Definition` legal to
  create a NEW cube from (does it include `duration`)? It has no file to inspect — none exists
  yet.
- `ValidateFileMatchesRegistry` answers an AFTER-the-fact question: does an ALREADY-WRITTEN file's
  own header agree with its OWN registry entry? This can only be asked once both exist, and it
  catches a genuinely different failure mode — corruption, a buggy writer, or a stale/incorrect
  registry entry — not a definition-time policy violation.
- Modeled on this codebase's existing `T1b`/`NOTE-VI-078` index-vs-data-inconsistency error
  family: a real mismatch between two things that are supposed to agree fails loudly (a typed,
  `errors.As`-comparable error), never gets silently parsed around or ignored.
- Scoped to a count-only check because the wire format has no per-file attribute-name
  self-description to compare identities against (`SPEC-CUBE-024`) — this is an intentional,
  documented limitation, not an oversight.

**Back-ref:** `internal/modules/cube/definition.go:ValidateFileMatchesRegistry`,
`internal/modules/cube/accumulator.go:validateDefinition`

---

## NOTE-CUBE-022: E-4's absent-attribute-skip convention is now cross-repo-verified against the real tempo engine (issue #491, ENGINE-FIX-NAN, tempo-side fix)

**Date:** 2026-07-08

**Decision/finding:** E-4's `Accumulator.Add`/`addAggAttrs` convention — a span missing an
aggAttr's value simply does not update that attribute's `Sum`/`Min`/`Max`/`SampleCount` for this
call (`NOTE-CUBE-015`) — was always correct on the blockpack/cube side, but was NOT actually
verified against tempo's real `sum_over_time` engine implementation until this fix, despite the
parity golden test suite (`tempo/tempodb/encoding/vblockpack/cube_metrics_parity_test.go`)
appearing to pass all along.

**What was actually wrong (tempo-side, not blockpack):** `pkg/traceql/engine_metrics_functions.go`'s
`sumOverTime()` unconditionally Kahan-added a missing attribute's `NaN` into the running sum
instead of skipping it (unlike `avgOverTimeSpanAggregator`, which already had the correct
skip-NaN guard, and unlike min/max, which were incidentally NaN-safe via comparison semantics).
This poisoned the result ORDER-DEPENDENTLY: `[120.5, 80.25, missing]` produced `NaN`, while the
same three values in reverse order produced `200.75` — a nondeterministic production answer for
identical input differing only in span iteration order. The parity golden's own
`sumByServiceFloatAttr` hand-rolled oracle helper matched blockpack's own "skip absent" assumption,
but had never actually been checked against the real, then-buggy engine for the specific span
order the fixture happened to exercise — the test was internally consistent, not an end-to-end
proof.

**Fixed (tempo repo, `engine_metrics_functions.go`):** `sumOverTime()` now skips a `NaN` input the
same way `avgOverTimeSpanAggregator` already does. `sum_over_time` is now genuinely
order-independent and matches `sumByServiceFloatAttr`'s formula exactly, the same as min/max/avg
always did.

**Consequence for future cube work:** do not assume a hand-rolled "skip absent attribute" parity
oracle is automatically correct for a NEW aggregation kind without first confirming the REAL
`pkg/traceql` implementation treats a missing attribute the same way — this exact assumption
silently failed for `sum_over_time` until this fix, and could recur for a future aggregation if
the same verification step is skipped. This convention also governs the cube-path's own
`cellValueForFunction` (tempo `cubequerypath.go`): a never-sampled attribute must surface as
`NaN`, matching this fixed engine semantics, never as a silent `0`.

**Back-ref:** `tempo/pkg/traceql/engine_metrics_functions.go:sumOverTime` (tempo repo, not
blockpack); `internal/modules/cube/accumulator.go:addAggAttrs` (the blockpack-side convention this
finding verifies).

---

## NOTE-CUBE-023: Type-aliased root exports inherit new methods automatically — do not write a redundant wrapper (issue #491, E-9)

**Date:** 2026-07-08

**Finding, worth remembering for future root re-export work:** when `cube_ingest.go` re-exports a
cube-package type as a plain Go type alias (e.g. `type CubeReader = cube.Reader`, `type
CubeQueryRouter = cube.QueryRouter`, `type CubeRegistry = cube.Registry`), any method the
UNDERLYING type gains LATER — even long after the alias itself was written — is automatically
reachable through the alias, with zero additional code. E-9 confirmed this directly: `Route`'s new
`neededAttr` parameter (E-6b), `Reader.GetAggCell`/`GetAggCellsInRange`/`NumAggAttrs` (E-3/E-6b),
and `Registry.UpdateWatermarks` (E-12a) all needed no new wrapper function or type — they were
already fully exposed the moment `CubeQueryRouter`/`CubeReader`/`CubeRegistry` were first aliased,
regardless of how much the underlying type grew afterward.

**Consequence:** before writing a new wrapper function for a method addition on an
ALREADY-ALIASED cube type, check whether the alias already exposes it for free — only a NEW type
(not yet aliased) or a genuinely NEW package-level function (not a method) needs an explicit new
re-export line in `cube_ingest.go`.

**Back-ref:** `cube_ingest.go:CubeReader,CubeQueryRouter,CubeRegistry` (the pre-existing aliases
that inherited E-3/E-6b/E-12a's new methods for free).

---

## NOTE-CUBE-024: Registry-entry-to-runtime-definition conversion is one seam, not two — a holistic pass caught what per-task review structurally could not (issue #491 Phase E fix pass, review.md Issues 1-3)

**Date:** 2026-07-08

**Finding:** `CubeRegistryEntryToDefinition` (cube_ingest.go, written for #480's filter-identity
work) predates E-7's `RegistryEntry.AggAttrs` field and was never revisited when E-6/E-7 added it —
so it silently never copied `AggAttrs` into the `Definition` it built. Since
`tempo/tempodb/encoding/vblockpack/cubemanager.go`'s `loadDefs` is the ONLY real production caller
of `LoadCubeDefinitions`/`CubeRegistryEntryToDefinition`, and `NewCubeAccumulator`'s
`validateDefinition` requires `DurationColumn` present in `AggAttrs` (SPEC-CUBE-025), this meant
`filterValidCubeDefs` silently dropped EVERY well-formed registered cube — forward ingest never
activated any cube in production, despite `cubemanager_test.go`/`cube_ingest_publicapi_test.go`
both having decent-looking coverage of the surrounding functions. The reason no test caught it:
every existing test constructed `CubeDefinition` BY HAND with `AggAttrs` set explicitly — none of
them ever called `CubeRegistryEntryToDefinition` itself, so this specific conversion path had zero
test coverage anywhere in either repo.

The SAME root cause (a registry-entry-to-runtime-definition conversion the individual per-task
reviews never actually exercised end-to-end) also explains why `CubeRegistryEntryToDefinition`'s
filter-conversion loop was a no-op in production (`cubemanager.go` always called
`LoadCubeDefinitions` with `filterFn == nil`, Issue 2) and why `Backfiller.processMinute` never
read `entry.Filters` at all (Issue 3, a separate standalone omission in a different function
that happened to have the identical shape of bug — a registry field simply never wired into the
runtime type that ingest/backfill actually consume).

**Fix:** `AggAttrDefsFor` (backfill.go, exported) and the new `ColumnFilterToFilter`
(definition.go) are now the SINGLE conversion functions used by all three call sites
(`CubeRegistryEntryToDefinition`, tempo's `cubemanager.go` `filterFn`, and
`Backfiller.processMinute`) — see SPEC-CUBE-027. Each of the three real-write-path tests added for
Issues 1-3 goes through the actual production conversion function (`LoadCubeDefinitions`,
`ColumnFilterToFilter`, `Backfiller.Run`) rather than a hand-built `Definition`, specifically to
close the "never actually called this function" coverage gap this note documents.

**Lesson for future spec-driven module work in this codebase:** when a registry/persisted-entry
type and a runtime/in-memory type both exist for the same concept, the CONVERSION function between
them is a first-class seam that needs its OWN direct test — a test that only ever constructs the
runtime type by hand, however thorough, provides zero evidence the conversion function itself
works, and this class of gap is exactly what individual per-task review structurally cannot catch
(each task landed cleanly against its own remit; the seam between AggAttrs — E-6/E-7 — and the
conversion function — pre-dating E-6/E-7 — was nobody's individual task).

**Back-ref:** `cube_ingest.go:CubeRegistryEntryToDefinition,LoadCubeDefinitions`, `internal/modules/cube/definition.go:ColumnFilterToFilter`, `internal/modules/cube/backfill.go:AggAttrDefsFor,Backfiller.processMinute`

---

## NOTE-CUBE-025: entryStore refactor — why cube's storage abstraction has 4 narrow methods instead of viusage's 1 generic one (task #158/#160)

**Date:** 2026-07-11

**Decision:** `Registry` now holds a package-private `entryStore` interface
(`load`/`addEntry`/`removeEntry`/`updateWatermarksEntry`) instead of talking to `ObjectStore`
directly. `blobEntryStore` (`entry_store.go`) wraps today's `ObjectStore` + conditional-PUT-retry
loops, behavior-preserving — every method body is `registry.go`'s pre-refactor `Add`/`Remove`/
`UpdateWatermarks`/`Load` moved verbatim. An exported `EntryStore` counterpart (same 4 operations,
exported method names) plus `externalEntryStoreAdapter` and `NewRegistryFromEntryStore` let an
external caller (tempo) construct a `Registry` over a Postgres-backed implementation instead of
an `ObjectStore`, with zero change to `Registry`'s own public methods.

**Why now:** this is the cube-side mirror of `internal/modules/viusage`'s identical refactor
(`viusage/NOTES.md`'s entryStore entries, `viusage/entry_store.go`) — both modules are migrating
to a Postgres-backed registry backend as part of the same broader effort (this effort's `plan.md`
Part 1/Part 2), and both needed the identical "let `Registry` sit on top of either backend without
changing its own public surface" seam.

**Why cube's `entryStore` has 4 narrow methods instead of viusage's 1 generic `upsertEntry`:**
viusage's every registry write goes through the identical shape — load an entry by key, create it
if missing, mutate it, persist — so one generic `upsertEntry(createIfMissing, mutate)` primitive
covers `RecordUseAndMaybeTrigger`/`RenewLease`/`UpdateWatermark`/`UpdateCatalogCursor` all at once.
Cube's three pre-existing `Registry` operations have genuinely different list-mutation semantics
instead: `Add` appends-with-a-limit-check (and is a no-op if the entry already exists — no mutate
step at all), `Remove` filters an entry out of the list, and `UpdateWatermarks` mutates one
existing entry's map field. Forcing these three shapes through a single generic
create-or-mutate-one-entry primitive would not actually simplify anything — R1's
"mirror the pattern, don't force a shared abstraction that doesn't fit" precedent (already applied
once for viusage vs. cube's own original registry, `NOTE-CUBE-009`'s cross-module relationship)
applies again here, one level down: mirror the SHAPE of viusage's refactor (an `entryStore` seam
`Registry` delegates through), not the literal generic-primitive DESIGN, since cube's own existing
write operations don't share viusage's one shape to generalize over.

**Behavior-preservation, verified:** every `blobEntryStore` method is `registry.go`'s pre-refactor
method body moved verbatim (rename `Registry` receiver `r` → `blobEntryStore` receiver `s`,
`r.store` → `s.store`) — no logic changed, only which type owns the code. `Registry`'s own public
methods (`Load`/`Add`/`Remove`/`UpdateWatermarks`/`IsActive`) are unchanged one-line delegations to
`r.store`'s corresponding method.

**Back-ref:** `internal/modules/cube/entry_store.go:entryStore,blobEntryStore,EntryStore,
externalEntryStoreAdapter,NewRegistryFromEntryStore`; `internal/modules/cube/registry.go:Registry`.
See `SPECS.md` SPEC-CUBE-014's `[UPDATED]` annotation and `NOTE-CUBE-009`'s addendum above.

## NOTE-CUBE-026: ruling 4(b) revisit rationale — why "decline the whole query" was the actual bug (issue #217)

*Added: 2026-07-13*

**Why the original ruling 4(b) (`SPEC-CUBE-023`/E-6b) was revisited.** The original design's own
stated rationale — "verify complete coverage or decline, never serve a partial/mixed-resolution
answer" — was defensible in isolation, but issue #217's governing principle
("never decline a query we have coverage for") identified this as the SAME class of bug already
fixed for the value-index path (Phase 2, tempo's `CheckIndexCoverage` simplification): a cube with
full backfill for 90% of a requested window used to decline the WHOLE query rather than serve the
90% it genuinely has. `Route`'s caller (tempo's `cubequerypath.go`) already had an existing,
non-terminal fallback path (`ErrCubeWarming` → VI/scan) for the `Found=false` case — the only
change needed was giving `Route` a way to say "found, but only this sub-range" instead of a
binary `Found bool`, so the caller could serve the covered part directly instead of discarding it
entirely and falling back to a (slower, VI-based) answer for the WHOLE window.

**Why this is safe without inventing a mixed-resolution merge engine.** The revisit is
deliberately scoped to a SINGLE resolution level's own edge-truncated coverage — Decision 1
(smallest-covering-superset tie-break across candidate cubes) and the "no mixed-resolution
answers" boundary are both untouched. `CoveredMinMinute`/`CoveredMaxMinute` describe one
contiguous overlap of one chosen level's own watermark with the query window; nothing about
picking a DIFFERENT resolution level or stitching two levels together changed.

**Why a genuine interior gap remains out of scope.** `ResolutionWatermark` is a single
`{MinMinute, MaxMinute}` pair by construction (`SPEC-CUBE-...`'s own definition) — it can only
ever describe ONE contiguous covered range. A backfill that somehow produced two disjoint covered
segments with a hole between them cannot be expressed by this struct at all; `Route` has no way to
even detect such a case, let alone serve around it. This remains a genuine, acknowledged gap
(carried forward from the original ruling), not something this revisit resolves — a richer
watermark representation (e.g. a list of covered ranges) would be required, and is explicitly
deferred as a future follow-up if it turns out to matter in practice.

**Consumer-side implication (tempo, out of this repo's scope but recorded for cross-reference):**
`cubequerypath.go`'s `tryQueryFromCube` narrows its actual cell read to
`CoveredMinMinute`/`CoveredMaxMinute` (never reads cells outside the confirmed-covered range) and
tags the resulting `QueryRangeResponse` `PartialStatus_PARTIAL` with a message identifying the
uncovered edge. It deliberately does NOT additionally merge a second, VI-sourced answer for the
uncovered edge within the same call — that merge was assessed and found to require a
metrics-function-specific re-aggregation strategy (sum-like functions merge safely; `rate()`/
`quantile_over_time()` do not merge correctly via simple concatenation) that no existing code path
in either repo implements today; shipping it without being able to verify correctness for every
supported function was judged riskier than the honest partial-coverage answer actually shipped.

Back-ref: `internal/modules/cube/router.go:RoutingResult,Route` (`SPEC-CUBE-028`).
tempo-side: `tempodb/encoding/vblockpack/cubequerypath.go:tryQueryFromCube,cubeCoveredWindow,cubePartialCoverageMessage`.
Issue #217.

## NOTE-CUBE-027: post-#217 review fix pass — consumer-side short-circuit correction, and the boundary-step under-counting limitation (2026-07-13 follow-up)

*Added: 2026-07-13, follow-up to `NOTE-CUBE-026`/`SPEC-CUBE-028` (does not silently rewrite either — see below).*

A consolidated review of #217 (`tempo/.bob/state/217-review.md`) found one CRITICAL and one HIGH
finding, both about `NOTE-CUBE-026`'s own "consumer-side implication" paragraph (the last
paragraph above). This entry records the fix pass; it supersedes nothing in `NOTE-CUBE-026`/
`SPEC-CUBE-028` themselves — `Route`'s own contract is unchanged and still correctly described
there.

**CRITICAL, fixed (tempo-side): `tryQueryFromCube`'s PARTIAL answer was returned unconditionally
by its caller.** `NOTE-CUBE-026` correctly describes `tryQueryFromCube` narrowing to
`CoveredMinMinute`/`CoveredMaxMinute` and tagging the result `PartialStatus_PARTIAL` — but it did
not call out that `backend_block.go`'s `QueryRange` (the caller) returned THAT partial answer
unconditionally on `ok=true`, without ever attempting the VI/scan path below it, even though that
path is a zero-new-code, always-available fallback that (pre-#217) was ALWAYS tried whenever the
cube path declined (`ok=false`). This meant a cube backfill lag could force a needlessly truncated
answer even when VI/scan could have answered the full window — the exact class of bug #217 exists
to prevent, reintroduced one call site up from the fix. Corrected: `QueryRange` now defers a
PARTIAL cube answer (mirroring the existing `cubeWarming` "only surfaced if the path below ALSO
declines" pattern) and only falls back to it once VI/scan itself declines for that window.
**Independently verified while fixing this:** every cube-answerable query has a group-by clause
(`tryQueryFromCube` declines immediately otherwise), and blockpack's VI-only metrics engine
(`vm.MetricsShapeIsVIAnswerable`, since issue #481 removed the scan-based engine outright) can
never answer a group-by query — so "cube partial AND VI/scan has full coverage" cannot currently
be constructed by any real query; the only reachable outcome today is "cube partial AND VI/scan
declines," which correctly falls back to cube's own partial answer. The fix is still correct and
intentionally kept — it is architecturally right per #217's own principle and future-proofs
against any later relaxation of VI's group-by restriction — but it is a no-op for every query
reachable in the current codebase. Tempo-side test:
`tempodb/encoding/vblockpack/cubequerypath_partial_fallback_test.go`'s
`TestQueryRange_CubePartialCoverage_DefersToVIScan_FallsBackWhenVIDeclines` (real end-to-end
`QueryRange` call, real cube registry entry + real cube L0 file served through a real
`*minio.Client`/local fake-S3 endpoint, real VI-backed block).

**HIGH, documented (not fixed): a step wider than one minute straddling the covered/uncovered
boundary can be computed from fewer minutes than its full width, with no per-step signal.**
`buildCubeQueryResponse` emits one sample per per-minute cube cell regardless of `req.Step`;
`CoveredMinMinute`/`CoveredMaxMinute` are watermark-derived with no relationship to `req.Step`'s
own alignment. If a query's step is wider than one minute and the covered/uncovered boundary falls
mid-step, that one step is computed from fewer minutes than its full width for
`rate()`/`sum_over_time()`/`quantile_over_time()`-style functions, indistinguishable from a
fully-covered step except via the response-level `PartialStatus_PARTIAL` flag (which does not
localize WHICH step is affected). This is judged an acceptable, explicitly acknowledged limitation
for now — analogous to the genuine-interior-gap limitation `NOTE-CUBE-026` already documents
honestly rather than silently — because a correct fix (rounding `CoveredMinMinute`/
`CoveredMaxMinute` inward to the nearest `req.Step`-aligned boundary before narrowing, dropping the
partial step entirely) touches the same narrowing logic the CRITICAL fix above already changed in
the same review pass, and the risk of introducing a NEW boundary-arithmetic bug while fixing a
narrower, harder-to-trigger issue (a wide-step query whose window happens to straddle a backfill
boundary) was judged not worth taking in the same pass as the CRITICAL fix. Left as a real,
tracked follow-up, not a silent gap: a future fix should round `CoveredMinMinute` up and
`CoveredMaxMinute` down to the nearest multiple of the query's step width (measured from the
query's own `reqMinMinute` origin, matching how steps are actually aligned) before narrowing in
`cubeCoveredWindow`, dropping the boundary-spanning step entirely rather than serving it truncated.

Back-ref: `internal/modules/cube/router.go:RoutingResult,Route` (unchanged).
tempo-side: `tempodb/encoding/vblockpack/backend_block.go:QueryRange` (the CRITICAL fix),
`tempodb/encoding/vblockpack/cubequerypath.go:buildCubeQueryResponse,cubeCoveredWindow` (the HIGH
finding, unchanged — documented only). Issue #217, review `tempo/.bob/state/217-review.md`.

---

## NOTE-CUBE-028: Log2Bucketize's 1<<64 overflow — a deliberate, intentional divergence from tempo's byte-for-byte port (bounds-safety fix)

**Date:** 2026-07-13
**Decision:** `Log2Bucketize` now returns the `-1` sentinel for `v>=2^63+1`, in addition to its
pre-existing `v<2` exclusion.

**Rationale:**

- For a pathologically large `v` (`>=2^63+1`), the ceiling power-of-two boundary is `2^64`, which
  does not fit in `uint64`. The original code computed `1 << (64 - bits.LeadingZeros64(v-1))`
  unconditionally; for this input the shift count is exactly `64`, and Go's defined shift
  semantics for a shift count `>=` the operand's bit width silently wrap the result to `0` rather
  than raising any error. Zero is itself a plausible-looking (but wrong) boundary — it slipped
  past `accumulator.go`'s own `if boundary != -1` guard, and `BucketIndex(0) ==
  bits.TrailingZeros64(0) == 64` is one past `Buckets`' valid `[0,63]` array range, producing an
  out-of-bounds write that panics on write (`accumulator.go:addAggAttrs`).
- `v>=2^63+1` is unreachable via any real span duration or count (`2^63` nanoseconds is ~292
  years) — SPEC-CUBE-019 already documents this package's "port of tempo's algorithm" contract as
  applying to the realistic domain, not to adversarial/corrupted input. This fix is narrowly scoped
  to closing that adversarial gap without touching any in-range behavior: every value in
  `[2, 2^63]` still ceilings identically to before (verified by
  `TestLog2Bucketize_MatchesTempoForKnownValues`, unchanged).
- Chose the existing `-1` sentinel (matching the `v<2` convention already in this function) over
  clamping to bucket 63, because clamping would silently misrepresent a value strictly greater than
  `2^63` as if it were `<=2^63` — the same "wrong-but-plausible-looking value" problem this fix
  removes, just moved from `0` to `63`.
- **This is a deliberate, intentional divergence from tempo's own `pkg/traceql.Log2Bucketize`**,
  which contains the same unconditional shift and does not guard this case — SPEC-CUBE-019's
  "byte-for-byte port" invariant is retained for the realistic input domain but no longer literally
  true for this one out-of-domain input. Not filing upstream against tempo (this project's
  convention: findings in other repos are recorded here, not filed as external tickets).

**Back-ref:** `internal/modules/cube/bucket.go:Log2Bucketize`. Test:
`TestLog2Bucketize_PathologicallyLargeValueExcludedNotOOB` (bucket_test.go),
`TestAccumulator_Add_PathologicallyLargeDuration_ExcludedNotOOB` (accumulator_test.go, exercises
the real `Add`/`addAggAttrs` path and was mutation-verified to reproduce the exact pre-fix
`index out of range [64] with length 64` panic when the fix is reverted).

---

## NOTE-CUBE-029: per-tenant active-cube limit removed entirely, not deprecated-and-kept (issue #497)

*Added: 2026-07-14*

**Decision:** The per-tenant active-cube cardinality gate is removed. `MaxCubesPerTenant`
(the exported default-1000 constant, `registry.go`), `Registry.maxCubes` (the field it seeded),
`TriggerConfig.MaxCubesPerTenant` (the override, `trigger.go`), and `ErrLimitReached` (the typed
error, `registry.go`) are all DELETED outright — not left behind as unused/dead fields, and not
kept as a deprecated-but-inert compat shim. Active cube count per tenant is now unbounded.

**Where the gate lived (now removed):** the `len(cubes) >= maxCubes` check + `ErrLimitReached`
return existed at two call sites — `blobEntryStore.addEntry` (`entry_store.go`, reached via
`Registry.Add`) and `CreationTrigger.TryCreate` (`trigger.go`, checked before the cardinality
gate to avoid paying VCNT I/O cost when the tenant was already "full"). Both checks are gone;
`TryCreate`'s cardinality gate (`SPEC-CUBE-013`) is now the only gate a new cube must pass.

**Signature propagation:** `maxCubes int` is now unused everywhere it appeared, so it was removed
from the parameter list rather than left as a dead parameter — `entryStore.addEntry`,
`blobEntryStore.addEntry`, the exported `EntryStore.AddEntry` interface method, and
`externalEntryStoreAdapter.addEntry` all drop the parameter together (one signature change
propagated to every implementer/caller, including the root package's `cube_ingest.go`
re-exports and its `pubAPICubeEntryStore` test fake in `cube_ingest_publicapi_test.go`). Root
package's `CubeErrLimitReached` alias (`cube_ingest.go`) and its `cmd/deadcode/main.go` anchor
reference are removed for the same reason — there is no longer an underlying type to alias.

**Accepted cost-model change (holistic-review finding, not just a count ceiling):**
`MaxCubesPerTenant` was not only a cardinality ceiling — it was also an implicit bound on the
COST of every `Add`/`TryCreate` call. `blobEntryStore.addEntry` does a full
Get+unmarshal+linear-scan+marshal+`ConditionalPut` of the entire per-tenant `index.json` blob on
every call (O(n) per write, O(n²) to populate a tenant from scratch); `CreationTrigger.TryCreate`
performs several of these full round trips synchronously on the query path for the first query
matching a new pattern. With no ceiling, both costs are now unbounded in the number of active
cubes for a tenant — measured at ~3.3s for 1500 sequential `Add` calls and ~5.7s for 1100
sequential `TryCreate` calls against an in-memory store double (pure CPU, not I/O latency). This
tradeoff is accepted as part of #497's explicit intent (remove limits, no compat, no reduced
functionality to preserve a bound) — it is not an oversight, but it is a real, load-bearing
consequence worth a deployment ever running into pathological per-tenant cube growth should
watch for (elevated cube-registration latency, not just unbounded storage), not merely "the count
ceiling is gone."

**Rationale:**

- This project's standing no-backward-compat rule (reaffirmed for #481's scan-fallback removal,
  #490's trace-by-id format removal, #491's `Cell`-type deletion) applies identically here: once a
  limit is deleted, nothing referencing it survives as a dead-but-compiling artifact "just in
  case." A field or error type with no remaining caller is exactly the kind of maintenance-risk
  dead code this rule exists to prevent.
- `ErrLimitReached` was checked via `grep` before removal to confirm nothing UNRELATED depended on
  it — its only real production consumers were the two enforcement sites this change deletes, plus
  the mechanical root-package alias/test-fake mirror of the same contract. No other caller in
  either this repo depended on it for an unrelated reason.
- Cross-referenced context: issue #182 (prior cube-cardinality-control discussion) and issue #497
  (this removal's tracking issue) both concern the same per-tenant cube-count ceiling; #497 is the
  authoritative decision to remove it outright rather than raise or reconfigure the default.

**Test changes:** `TestRegistry_LimitReached` (registry_test.go, TEST-CUBE-036) and
`TestTrigger_LimitEnforced` (trigger_test.go, TEST-CUBE-042) — both of which asserted
`ErrLimitReached` once a small `maxCubes` was hit — are replaced, not merely deleted, by
`TestRegistry_NoLimitEnforced_UnboundedCubeCount` and
`TestTrigger_NoLimitEnforced_UnboundedCubeCount`, which each register well beyond the old default
(1500 and 1100 cubes respectively, for one tenant) via the real `Registry.Add`/`CreationTrigger.
TryCreate` call sites and assert no error and a matching final registry count — proving the NEW
unbounded behavior at both former enforcement points, not just the absence of the old test.

**Back-ref:** `internal/modules/cube/registry.go:Registry`,
`internal/modules/cube/entry_store.go:entryStore,blobEntryStore,EntryStore,
externalEntryStoreAdapter`, `internal/modules/cube/trigger.go:TriggerConfig,CreationTrigger.
TryCreate`; `cube_ingest.go:CubeTriggerConfig`; `cmd/deadcode/main.go`. Tests:
`internal/modules/cube/registry_test.go:TestRegistry_NoLimitEnforced_UnboundedCubeCount`,
`internal/modules/cube/trigger_test.go:TestTrigger_NoLimitEnforced_UnboundedCubeCount`. Issues
#182, #497.

## NOTE-CUBE-030: Native Postgres EntryStore implementation added (issue #506)

Date: 2026-07-15

**Decision:** `pg_entry_store.go` adds `PgEntryStore{pool}` / `NewPgEntryStore(pool)`, a native
Postgres-backed implementation of `EntryStore` (SPEC-CUBE-014), ported verbatim from tempo's
`tempodb/encoding/vblockpack/pg_entrystore_cube.go` (2026-07-11). blockpack now depends on
`github.com/jackc/pgx/v5` directly for this file only; `EntryStore`'s own contract, `NewRegistry`,
and the blob-backed path are unchanged and remain fully supported — this is purely additive.
`NewPgRegistry(pool, tenant)` (`registry.go`) is a one-call convenience wrapper equivalent to
`NewRegistryFromEntryStore(NewPgEntryStore(pool), tenant)`. `ApplySchema(ctx, pool)` (`//go:embed
schema.sql`) applies `cube_entries`'s schema; never called automatically by any constructor,
mirroring tempo's own `migrate.Apply` precedent — the embedding application calls it once at its
own startup.

**Rationale:** see spec-oracle-506's own review (`.bob/state/spec-oracle-506-review.md`) for the
full analysis of why this mirrors, rather than shares code with, viusage's/colhashmanifest's own
native Postgres implementations — cube's `entryStore` interface has 4 narrow methods (not
viusage's 1 generic `upsertEntry`), a distinction this port preserves exactly (`AddEntry`/
`RemoveEntry`/`UpdateWatermarksEntry` as 3 separate methods, matching NOTE-CUBE-025's own R1
ruling).

**Load-bearing details preserved verbatim from the port, not reimplemented or "cleaned up":**
- Advisory-lock-before-exists-check ordering in `AddEntry` (`pg_advisory_xact_lock(hashtext(tenant))`
  acquired FIRST, before the idempotency check) — the first cube for a tenant has no row to
  `SELECT ... FOR UPDATE`, so this ordering is the only thing preventing two concurrent
  first-registrations of the same new CubeID from both passing the "not present" check before
  either INSERT commits.
- The JSONB-null-map watermarks re-init guard in `UpdateWatermarksEntry` (`json.Marshal(nil map)`
  → `"null"` → unmarshal resets to nil) — carried over with its original explanatory comment
  citing "a real Postgres run" as the evidence for the bug class.

**Mutation-tested per this project's standing rigor bar:** the advisory lock was temporarily
removed from `AddEntry` for `TestPgEntryStore_AddEntry_ConcurrentFirstCubeRegistration_
NoDuplicateOrRace` — plain removal alone did not reliably reproduce a failure against a fast
local Postgres round-trip, so a temporary 50ms sleep was added between the exists-check and the
INSERT to widen the race window (mirroring tempo's own documented precedent for the analogous
viusage test). With both changes in place, the test correctly failed with a real duplicate-key
violation (SQLSTATE 23505); both were reverted and the full package suite (including `-race`)
confirmed green again.

**Back-refs:** `internal/modules/cube/pg_entry_store.go:PgEntryStore,NewPgEntryStore,ApplySchema`,
`internal/modules/cube/registry.go:NewPgRegistry`, `internal/modules/cube/schema.sql`,
`cube_ingest.go:NewPgCubeEntryStore,NewPgCubeRegistry,ApplyCubeSchema`. Tests:
`pg_entry_store_test.go`, `pg_blob_differential_test.go`. See SPEC-CUBE-031. Issue #506.

### Addendum (2026-07-15): schema-drift risk between `schema.sql` and tempo's `registries.sql`

`schema.sql` was ported verbatim from tempo's own
`tempodb/encoding/vblockpack/schema/registries.sql` (`cube_entries` section) and is, as of this
writing, byte-identical to it (confirmed by direct diff during the #506 holistic review). This is
kept in sync **by convention only — there is no automated check, CI diff, or shared source file
between the two repos.** If tempo's copy of `registries.sql` is ever modified (e.g. a new column
or index added to `cube_entries`), blockpack's `schema.sql` must be updated to match by hand, and
vice versa; nothing today would catch a silent divergence, and whichever `ApplySchema`/
`migrate.Apply` call happens to run first against a fresh database "wins" the initial table shape.
This is not a live bug — both `CREATE TABLE IF NOT EXISTS`/`CREATE INDEX IF NOT EXISTS` statements
are idempotent, so calling both against the same database today is a safe no-op — but it is a real,
currently-undocumented gap for the future. Whoever eventually migrates tempo's 4 cube-related call
sites (`cube_scheduler.go`, `cube_backfill.go` x2, `cubequerypath.go`) onto blockpack's
`NewPgCubeEntryStore`/`ApplyCubeSchema` (the stated next step after #506) should, at that point,
retire tempo's own copy of the `cube_entries` section of `registries.sql` and treat blockpack's
`schema.sql` as the sole source of truth — this closes the gap permanently. Until that happens,
treat any change to either file as requiring a manual check of the other. See go-presubmit.md /
review.md Issue 2 (#506 holistic review) for the finding that prompted this addendum.

## NOTE-CUBE-031: Query-path orchestration moved wholesale from tempo to blockpack root; no blockpack-side singleton (issue #508)

Date: 2026-07-16. ID-numbering caveat: confirmed via direct inspection of this file's own highest
existing ID (`NOTE-CUBE-030`) rather than the `blockpack_search_modules` MCP tool or a spec-oracle
agent scoped to this worktree — neither was reachable in this session.

**Decision:** `cubequerypath.go`'s registry cache, `Router.Route` + partial-coverage narrowing,
fan-out file fetch + rollup, and `cube_backfill.go`'s `viBackfillSource`/`buildVCNTSection`/
backfill-runner/`LoadCubeEntry` all moved from tempo (`tempodb/encoding/vblockpack/`) into
blockpack root (`cube_query_path.go`, `cube_backfill_runner.go`) as this issue's continuation of
#506's own stated next step (see this file's schema-drift addendum above). TraceQL string
parsing (`extractGroupByDims`/`extractFilters`/`extractAggAttr`) and `tempopb` response
construction stay tempo-side (hard type dependency on tempo's own protobuf types) — everything
else moved.

**Why no blockpack-side singleton, despite `NewCubeQueryPath` looking superficially similar to a
constructor tempo would normally wrap in its own `sync.Once`:** an earlier planning pass's
brainstorm cited `ConfigureValueIndexQuery`'s `sync.Once` wrapper (`value_index_query.go`) as
precedent for blockpack owning its own singleton. Direct reading corrected this:
`ConfigureValueIndexQuery` is a **tempo-side** `sync.Once`, not a blockpack-side one — blockpack
has never owned a package-level singleton for any of its query-path types, and this move does not
introduce the first one. `NewCubeQueryPath` returns a plain value; tempo's existing
`cubeQueryPathOnce`/`getCubeQueryPath` wrapper is unchanged, now just constructing blockpack's
type inside its `Do` closure exactly as it already constructed the pre-move tempo-local struct.

**Why `OnCreateAttempt` is a callback, not an auto-spawned goroutine inside blockpack:** blockpack
has zero logging/metrics dependencies by design (matches `loadEntries`'s pre-existing "declines
are silent by design" convention, ported unchanged) and must never import tempo's `jobstore`
package directly. `RunCubeBackfill` launching itself automatically from inside
`CubeQueryPath.QueryRange` would force blockpack to either duplicate tempo's `jobstore.
InsertCubeBackfill` durable-queue insert logic or silently drop it — the callback keeps that
decision, and all its logging/metrics side effects, entirely on tempo's side of the boundary.

**Back-ref:** `cube_query_path.go`, `cube_backfill_runner.go`. See `SPEC-CUBE-032`. Issue #508.

## NOTE-CUBE-032: `decodeCanonicalVI` numeric-truncation bug fixed during the port, not ported verbatim (issue #508)

Date: 2026-07-16

**The bug:** tempo's original `decodeCanonicalVI` (`cube_backfill.go`, pre-move) byte-sniffed a VI
column's raw value bytes to guess numeric-vs-string (`for _, c := range b { if c < 0x20 { goto
numeric } }`), then decoded a guessed-numeric value's 8 bytes as `int64` and took `v % 10` — a
single decimal digit, discarding the rest of the value. The same heuristic additionally
mis-decoded a genuine `float64` column's IEEE-754 bit pattern as if it were an `int64`. Both are
real, untested, pre-existing value-corruption bugs, not introduced by this port — confirmed via
direct reading of the pre-move source, not a guess.

**Why fixed instead of ported line-for-line (a deliberate, flagged scope decision — this function
changes signature, unlike the fan-out/panic-recover logic in `QueryRange`, which WAS ported
verbatim per this issue's own plan):** the VI column's actual type is already available for free
at the only call site — `NewCubeVIBackfillSource.LookupColumn`'s own `for _, typeName := range
[]string{"string","int64","uint64","bool","float64"}` list-prefix loop. Threading that type
through instead of byte-sniffing eliminates BOTH bugs at once, with no additional plumbing beyond
one new parameter.

**Regression test, written first, real-write-path (not hand-built bytes):**
`TestNewCubeVIBackfillSource_LookupColumn_DecodesNumericTypesCorrectly`
(`cube_backfill_runner_test.go`) writes a real VI L0 file via the production `valueindex.Writer`
for a known `int64` value ≥ 10 (42) and a known `float64` value (3.14), reads both back through
`LookupColumn`, and asserts the decoded strings are exact (`"42"`, a value that round-trips via
`strconv.ParseFloat` to `3.14`) — confirmed to fail against a reintroduced `v % 10` variant before
the type-threaded fix, per this repo's mutation-test rigor bar.

**Back-ref:** `cube_backfill_runner.go:decodeCanonicalVI,cubeVIBackfillSource.LookupColumn`. See
`SPEC-CUBE-032`. Issue #508.

## NOTE-CUBE-033: Zero-dimension `Dim1Column` sentinel fix, and the backfill dim2-sentinel-mismatch bug it also fixed (issue #508)

Date: 2026-07-16

**Bug 1 (forward-ingest, zero-dim cubes could never accumulate anything):**
`CubeRegistryEntryToDefinition` (`cube_ingest.go`) already had an `else` branch defaulting
`Dim2Column` to `"__all__"` for a single-dimension entry, but had **no equivalent `else` branch
for `Dim1Column`** — a `Dimensions: []` entry produced `Dim1Column: ""`, and
`Accumulator.Add`'s `SpanValues.String("")` call falls through every real `SpanValues`
implementer's branches to `return "", false`, rejecting every span. A zero-dimension cube would
have silently never accumulated anything, forward-ingest included, until this fix.

**Bug 2 (backfill/forward-ingest sentinel mismatch, latent since before this issue, affecting
EVERY real single-dimension cube):** `internal/modules/cube/backfill.go`'s `processMinute` used a
hardcoded `"_"` literal for its own dim2 placeholder (`dim2Col := "_"`, `dim2 := "_"`), while
`CubeRegistryEntryToDefinition`'s forward-ingest path used `"__all__"` for the identical logical
slot. `CubeRollup` merges series by literal dictionary-encoded dim1/dim2 STRING value — two
different placeholder strings for "no second dimension" meant a single-dimension cube's
backfilled and forward-ingested files were NEVER merged into one series by `CubeRollup`, for
every real single-dimension cube with both backfilled and forward-ingested data (i.e.,
essentially all of them). Found and fixed in the same pass as Bug 1 because both are instances of
the same missing single-source-of-truth constant.

**Fix:** added `AllDimSentinel = "__all__"` (`definition.go`), root-aliased as
`CubeAllDimSentinel` (`cube_ingest.go`) — see `SPEC-CUBE-033`. `CubeRegistryEntryToDefinition` now
sets `Dim1Column = CubeAllDimSentinel` when `len(entry.Dimensions) == 0`, and both of
`backfill.go:processMinute`'s dim2 locals use the same constant instead of `"_"`.

**Regression tests, written first, confirmed to fail against the pre-fix code before
implementing:**
- `TestCubeRegistryEntryToDefinition_ZeroDimension` (root, `cube_zerodim_publicapi_test.go`) —
  Bug 1: converts a `Dimensions: nil` entry, asserts `Dim1Column == Dim2Column ==
  CubeAllDimSentinel`, then drives a real `Accumulator.Add` call through it and asserts
  `CellCount() == 1`.
- `TestBackfill_SingleDimDim2SentinelMatchesForwardIngest` (`backfill_test.go`) — Bug 2: builds
  one cube file via the forward-ingest-style `Definition{Dim2Column: AllDimSentinel}` path and
  one via the real `Backfiller`/`processMinute` path for the SAME single-dimension
  `RegistryEntry`, feeds both into `Rollup`, and asserts exactly ONE merged series results (the
  pre-fix `"_"` literal produces two, confirmed before implementing the fix).

**Zero-dimension backfill remains explicitly, permanently unsupported** — not something this fix
extends to. See `SPEC-CUBE-033`'s own "documented, PERMANENT limitation" section for the full
rationale (no VI "enumerate every span" query exists). `backfill.go`'s pre-existing
`len(entry.Dimensions) == 0` guard is kept, with its doc comment revised to state this precisely
rather than merely "unsafe to index."

**Back-ref:** `definition.go:AllDimSentinel`, `cube_ingest.go:CubeAllDimSentinel,
CubeRegistryEntryToDefinition`, `backfill.go:Backfiller.processMinute`. See `SPEC-CUBE-033`.
Issue #508.

## NOTE-CUBE-034: Cube-file S3 directory-name zero-padding mismatch — found by Phase 8's own end-to-end test (issue #508)

Date: 2026-07-16

**The bug:** `Accumulator.FlushTo` → `Filename(tenant, cubeID [16]byte)` always
`hex.EncodeToString`s the FULL 16-byte `Definition.ID`. Every real production path that builds
that `[16]byte` (`CubeRegistryEntryToDefinition` for forward-ingest, `processMinute` for backfill)
derives it via `IDFromBytes(entry.CubeID)`, which decodes the registry's 8-byte/16-hex-char
`CubeID` into the FIRST 8 bytes of a `[16]byte` and leaves the remaining 8 bytes zero. The
resulting S3 directory name is therefore always 32 hex characters: the registry's 16-hex-char
`CubeID` followed by 16 literal `"0"` characters.

The newly-moved `QueryRange` (ported from tempo's `tryQueryFromCube` verbatim, per this issue's
own "port line-for-line" instruction for this exact code region) built its file-discovery `Lister`
prefix from `result.Entry.CubeID` **directly, unpadded** — a `path.Join(tenant, "cubes",
result.Entry.CubeID) + "/"` prefix that structurally can never match any real file's actual
32-character directory. This is the exact same convention `cube_compactor.go`'s
`cubeFileStore.List` already comments on and pads for ("Registry stores 16-hex-char IDs (8
bytes); S3 dirs use 32-hex-char (16 bytes, zero-padded)") — meaning the pre-move tempo query path
this was ported from carried this same latent bug, silently and permanently falling back to
VI/scan for every cube-answerable query (this codebase's own "declines are silent by design"
convention means a silent zero-file listing looks identical to "no coverage yet," not an error —
nothing would have surfaced this without an end-to-end test through the real write path).

**Why this had gone unnoticed:** every EARLIER unit test in this issue's own Phase 4 (fan-out/
rollup logic) used a fake `Lister` that ignores its `prefix` argument entirely and returns a
fixed key set regardless — those tests correctly proved the fan-out/panic-recover/ordering logic
itself, but could not have caught a prefix-construction bug by design. Only Phase 8's end-to-end
test, whose fake `Lister` filters by REAL prefix-matching against a shared `CubeFileStore`'s
underlying map (`fakeCubeListerFromStore`, `cube_query_path_publicapi_test.go`), exercises the
actual string comparison and caught it — the exact "individually-tested functions, untested seam"
failure mode `NOTE-CUBE-024` already documents for a different pair of functions.

**Fix:** `QueryRange` now zero-pads `result.Entry.CubeID` to 32 hex characters (mirroring
`cubeFileStore.List`'s own string-padding, not a byte-level `IDFromBytes` round-trip, to avoid a
new error path for what is always a well-formed registry `CubeID` in practice) before building
its `Lister` prefix.

**Back-ref:** `cube_query_path.go:QueryRange`. Confirmed end-to-end by
`TestCubeQueryPath_GroupedQuery_TriggersCreationAndBackfill`
(`cube_query_path_publicapi_test.go`) — this test failed with the unpadded prefix, passed after
the fix, with no other change. See `SPEC-CUBE-032`. Issue #508.
