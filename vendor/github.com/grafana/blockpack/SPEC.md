# Blockpack — Root Engineering Principles

This file defines codebase-wide engineering invariants that apply to all packages.
Module-specific contracts live in each package's `SPECS.md`.

---

## SPEC-ROOT-001: No Panics

**The code should never panic if at all possible.**

Panics crash the goroutine handling the current request and are never acceptable on
query or I/O paths. Every recoverable error must be returned to the caller as a Go error.

**Rules:**

- Never use bare (non-ok) type assertions on values that cross package or API boundaries.
  Always use the two-value form: `v, ok := x.(T)`. If `!ok`, return an error.
- Never index a slice without a bounds check when the index comes from external input,
  a parsed value, or any value not statically provable to be in range.
- Never call `panic()` on any path that can be reached at runtime during normal operation.
  Panics are only acceptable in `init()` functions for programming errors detected at startup
  (e.g., registering duplicate codec IDs).
- Nil-deref prevention: always check pointers returned from lookups (map lookups, interface
  unwraps, optional results) before dereferencing.

**Rationale:**

Blockpack is used as an embedded library inside query-serving processes. A single unrecovered
panic crashes the entire host process and drops all in-flight queries. Errors must propagate
so callers can handle, log, and continue serving other requests.

**Enforcement:**

- `go vet` and `nilaway` are run in CI.
- New code reviewed for bare type assertions, unguarded index operations, and explicit panics.

---

## SPEC-ROOT-002: Go File Layout

**Every `.go` file must follow a consistent internal layout.**

**Declaration order within a file:**

1. `import` block
2. `const` declarations
3. `var` declarations
4. Standalone functions (not methods), exported before unexported, each group alphabetical
5. Each struct type followed immediately by its methods, exported methods before unexported, each group alphabetical

**File-per-struct rule:**

If a struct has more than 4 methods, it must live in its own file named after the struct
(e.g. `block_label_set.go` for `blockLabelSet`). Shared helpers used only by that struct
go in the same file.

**Rationale:**

Consistent layout makes it predictable where to find any given function or method.
The file-per-struct rule prevents large files that mix multiple types and their methods,
making navigation and code review easier.

**Enforcement:**

Reviewed during code review. No automated tooling enforces this today.

---

## SPEC-ROOT-003: Defer Byte Parsing Until Needed

**Never parse or decode bytes before confirming the result will be used.**

Parsing is expensive. Block bytes, column data, and encoded fields must not be decoded
until a predicate or caller has confirmed the data is needed for the current query.

**Rules:**

- Apply all cheap filters (bloom, min/max, time range) before decoding block bytes.
- Inside a block, apply column predicates on already-decoded columns before decoding
  remaining columns.
- Never decode a column that is not in `wantColumns` or required by a predicate.
- Lazy decode patterns (decode-on-first-access) are preferred over eager decode in
  all hot paths.

**Rationale:**

Decoding unused bytes wastes CPU and memory. On query paths with high block counts,
skipping unnecessary decodes is the single largest source of throughput improvement.

**Enforcement:**

Reviewed during code review. Benchmark regressions (bytes_decoded/op) are a signal of violation.

---

## SPEC-ROOT-004: Preallocate Slices and Maps — Minimize append

**Always preallocate slices and maps when the size or a reasonable upper bound is known.
Calling `append` on a nil or under-sized slice in a hot path is a bug to fix, not an
acceptable default.**

**Rules:**

- Use `make([]T, 0, n)` when appending up to `n` elements; use `make([]T, n)` when
  filling by index. Never start from `var s []T` or `[]T{}` inside a hot loop.
- Use `make(map[K]V, n)` with a capacity hint whenever the number of entries is
  predictable. An unhinted map that will hold hundreds of entries is a bug.
- **Trace up the call stack before declaring a size "unknown".**  If a function cannot
  determine the capacity itself, check whether its callers know — the hint can be
  threaded in as a parameter. Declaring something unbounded without checking ancestors
  is not acceptable.
- For maps keyed on a sparse domain (e.g. block indices from a large ref list), cap the
  hint at a domain-appropriate maximum (e.g. `min(len(refs), 64)`) to avoid
  pathological over-allocation.
- For per-block or per-row scratch slices reused across iterations, hoist the allocation
  outside the inner loop and use `clear()` to reset between iterations rather than
  re-allocating each time (see NOTE-054 for the safe-reuse pattern with `strings.Join`).
- Prefer pooled buffers (see SPEC-ROOT-005) for scratch objects with unbounded or
  highly variable sizes.

**Rationale:**

Unbounded `append` growth causes repeated reallocations (growslice) and GC pressure.
In the executor hot path, eliminating 1 alloc/row across 1800 spans/block removes
~1800 allocations per query per block. Preallocating also improves cache locality by
keeping data in contiguous memory from the first write.

The "trace up the call stack" rule exists because function authors often declare a size
unknown when the caller — or the caller's caller — already holds the bound. Threading a
`hint int` parameter costs nothing and eliminates allocs.

**Enforcement:**

`make precommit` runs benchmarks; `allocs/op` and `B/op` regressions surface violations.
New allocations on hot paths that have a statically derivable upper bound will be flagged
in code review.

---

## SPEC-ROOT-005: Aggressive Pool and Cache Reuse

**Use `sync.Pool`, the object cache, and pre-allocated buffers aggressively on all hot paths.**

**Rules:**

- Any per-request or per-block scratch object (buffers, row sets, predicate slices,
  column maps) that does not escape to the caller must be pool-managed.
- The object cache (`internal/modules/objectcache`) is the preferred mechanism for
  caching deserialized block structures across requests; prefer it over ad-hoc caching.
- Pool `Get` must always be paired with a deferred `Put` before the object escapes scope.
- Do not pool objects that escape to callers — incorrect pool reuse causes data races.
- Prefer resetting and reusing an existing allocation over allocating a new one.

**Rationale:**

The query hot path processes thousands of blocks per second. Without pooling, each block
triggers O(columns) allocations. Aggressive reuse is the primary lever for reducing
allocs/op and GC pause frequency.

**Enforcement:**

`make precommit` includes benchmarks; allocs/op and B/op regressions are blocking.

---

## SPEC-ROOT-006: Condition Complexity Limit

**An `if` or `else if` condition may contain at most 3 boolean operands; beyond that, extract a named predicate.**

**Rules:**

- A condition with more than 3 operands (`&&`, `||`, `!` applied to sub-expressions)
  must be extracted into a named boolean variable or helper function before the `if`.
- The name of the extracted variable or function must describe the intent, not the
  mechanics (e.g. `blockIsEmpty` not `lenZeroAndNilMap`).
- Nested ternary-style chains using multiple `||`/`&&` in a single expression are
  subject to the same limit.

**Example — non-compliant:**

```go
if a != nil && b > 0 && c.enabled && d != "" {
```

**Example — compliant:**

```go
hasValidInput := a != nil && b > 0 && c.enabled && d != ""
if hasValidInput {
```

**Rationale:**

Complex boolean conditions are a leading source of logic bugs and test gaps. Extracting
named predicates makes each condition independently readable, testable, and reviewable.

**Enforcement:**

Reviewed during code review. `golangci-lint` (gocognit/gocritic) surfaces some violations.

---

## SPEC-ROOT-007: Prefer Functional Code Over Struct-Based Code

**Prefer standalone functions and function-typed parameters over structs with methods when state is not required.**

**Rules:**

- If a "component" holds no mutable state and requires no lifecycle (no `Close`, no
  background goroutine), implement it as a package-level function, not a struct with
  a single method.
- Transformation and filtering logic (predicates, mappers, reducers) must be expressed
  as plain functions or function values, not single-method structs or interfaces.
- Structs are appropriate when: (a) they hold configuration or cached state shared
  across calls, (b) they implement a multi-method interface required by callers, or
  (c) they manage a resource with an explicit lifecycle.
- Avoid wrapping a function in a struct solely to satisfy a one-method interface;
  prefer a function type (`type PredicateFn func(...) bool`) instead.

**Rationale:**

Unnecessary structs add indirection, inflate API surface, and make call graphs harder
to follow. Functions are cheaper to construct, easier to test in isolation, and compose
naturally without requiring interface machinery.

**Enforcement:**

Reviewed during code review. Single-method structs with no state are flagged for refactoring.

---

## SPEC-ROOT-008: Test-Driven Development

**New behaviour must be written test-first wherever practical.**

**Rules:**

- Write the test before writing the implementation. The test defines the contract;
  the implementation satisfies it.
- Every new exported function, method, or module entry point must have at least one
  table-driven unit test covering the happy path and at least one error/edge case.
- Tests must be in the same package (`_test` suffix) unless white-box access is
  genuinely required.
- Do not write implementation code to pass a test that does not yet exist.
- When fixing a bug, write a failing test that reproduces the bug first, then fix
  the implementation to make it pass.
- Integration tests (hitting real storage, real parsers) are preferred over mocks;
  see SPEC-ROOT-001 for the no-mock-database rationale carried over from prior
  incident history.

**Rationale:**

TDD forces precise specification of behaviour before implementation, catches regressions
at the moment they are introduced, and produces a living executable specification.
Tests written after the fact tend to test the implementation rather than the contract.

**Enforcement:**

CI enforces > 70% coverage. New code submitted without tests will be rejected in review.
Bug fixes without a reproducing test case will be returned for a test before merge.

---

## SPEC-ROOT-009: Spec File Authoring and Maintenance

**Every module under `internal/modules/<name>/` must maintain four living spec files.
These files are the source of truth for the module; code is their implementation.**

### The four files

| File | Answers | Contains |
|---|---|---|
| `SPECS.md` | *What* does this module do? | Public contracts, input/output semantics, invariants, entry-point signatures, error conditions. When code conflicts with `SPECS.md`, `SPECS.md` wins. |
| `NOTES.md` | *Why* was it built this way? | Dated design decisions, rejected alternatives, non-obvious constraints, and the reasoning behind each. Required reading before modifying a package. |
| `TESTS.md` | *How* is correctness verified? | Test plan, coverage goals, named test cases with expected inputs/outputs, edge cases, and known gaps. |
| `BENCHMARKS.md` | *How fast* is it? | Benchmark cases, I/O metrics, baseline numbers (serve as regression thresholds), and notes on measurement conditions. |

### Entry ID format

Every entry in each file must carry a sequential, file-scoped ID:

| Prefix | File | Example |
|---|---|---|
| `SPEC-` | `SPECS.md` | `SPEC-007` |
| `NOTE-` | `NOTES.md` | `NOTE-003` |
| `TEST-` | `TESTS.md` | `TEST-012` |
| `BENCH-` | `BENCHMARKS.md` | `BENCH-002` |
| `REQ-` | `REQUIREMENTS.md` | `REQ-008` |

IDs are assigned in ascending order and never reused or renumbered. If an entry is
superseded, mark it `[SUPERSEDED by SPEC-NNN]` rather than deleting it.

### Two-way linking

Every spec entry that corresponds to code must include a back-reference to the
implementing file and function:

```markdown
## SPEC-007
**Single I/O invariant** — blocks are always fetched in a single read.
Back-ref: `internal/blockio/reader/reader.go:GetBlockWithBytes`
```

Every non-trivial implementation that directly satisfies or is governed by a spec entry
must carry the ID in a comment:

```go
// SPEC-007: single I/O per block — never issue per-column reads
func (r *Reader) GetBlockWithBytes(ctx context.Context, id ulid.ULID) ([]byte, error) {
```

### NOTES.md entry format

Each `NOTES.md` entry must include:

- The date the decision was made (`YYYY-MM-DD`)
- The decision itself (one sentence)
- The rationale (why this choice over the alternatives)
- Alternatives considered and why they were rejected (if applicable)

### BENCHMARKS.md entry format

Each `BENCHMARKS.md` entry must include:

- The benchmark function name and file
- Baseline numbers: `ns/op`, `B/op`, `allocs/op`, and `io_ops` where applicable
- The Go version and hardware note used to establish the baseline
- A regression threshold (e.g. "flag if ns/op increases > 10%")

### When to update spec files

| Event | Required update |
|---|---|
| New exported function or type | Add entry to `SPECS.md`; add test cases to `TESTS.md` |
| Design decision made | Add dated entry to `NOTES.md` |
| Bug fixed | Add or update test case in `TESTS.md`; note the fix in `NOTES.md` if non-obvious |
| Performance change | Update baseline in `BENCHMARKS.md` |
| Behaviour removed or changed | Mark old entry superseded; add new entry |

Spec files must be updated in the same commit as the code change. Stale specs are
treated as bugs.

### Rules for agents

- Do not read spec files directly from disk on large modules — use `blockpack_search_modules`
  and `blockpack_package_docs` to query them efficiently.
- Writing and updating spec files is always permitted and expected as part of any code change.
- When adding a new spec entry, assign the next sequential ID by reading the highest
  existing ID in that file. Do not invent IDs or skip numbers.

**Rationale:**

Without living spec files, design intent is lost in git history, invariants drift silently,
and new contributors (human or agent) have no authoritative reference. The four-file
structure separates *what*, *why*, *verification*, and *performance* so each concern
can be maintained independently.

---

## SPEC-ROOT-010: Never Swallow Errors

**Invariant:** Errors must never be silently discarded. Every `err != nil` check must either:

1. **Propagate** the error to the caller (`return ..., err` or `return ..., fmt.Errorf("context: %w", err)`)
2. **Log** the error with `slog.Warn` or `slog.Error` and a clear explanation of why the error is non-fatal
3. **Annotate** with `// SPEC-ROOT-010 exception: <reason>` when the error is intentionally ignored (e.g., best-effort cleanup)

The following pattern is **prohibited**:

```go
result, err := doSomething()
if err != nil {
    return nil  // ← error silently swallowed
}
```

The following patterns are **acceptable**:

```go
// Propagate
result, err := doSomething()
if err != nil {
    return nil, fmt.Errorf("doSomething: %w", err)
}

// Log + degrade
result, err := doSomething()
if err != nil {
    slog.Warn("doSomething failed, falling back", "err", err)
    return fallbackResult, nil
}

// Intentional ignore (annotated)
_ = f.Close() // SPEC-ROOT-010 exception: best-effort file close on error path
```

**Rationale:**

Silent error swallowing causes invisible performance degradation and correctness issues.
In this codebase, a swallowed `DecodePageTOC: unknown version` error caused the intrinsic
fast path to silently fall back to a 50x slower block scan path for every query — with no
error in logs, no metric, and correct results. The bug persisted undetected across multiple
benchmark sessions.

**Enforcement:**

- `errcheck` linter catches unchecked error returns (already enabled)
- Code review must verify that every `if err != nil` block propagates, logs, or annotates
- No existing linter catches "checked but swallowed" — this is a manual review requirement

---

## SPEC-ROOT-011: Bounded Goroutine Fan-Out

**Invariant:** Executor block-group fetching must use `errgroup.SetLimit(runtime.NumCPU())`
or equivalent to cap parallel goroutines. Unbounded fan-out causes OOM under large result sets.

Back-ref: `internal/modules/executor/stream.go:forEachBlockInGroups`

---

## SPEC-ROOT-012: Per-Column Decompression Bomb Guard

**Invariant:** Every `snappy.Decode` call on a V14 column blob must check `uncompressedLen`
against `shared.MaxBlockSize` before allocating. A malformed snappy header can claim an
enormous decoded size.

Back-ref: `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## SPEC-ROOT-013: V14-Only enc_version

**Invariant:** V14 block columns use `enc_version=3` (VersionBlockEncV3). V12 columns with
`enc_version=2` are not readable by the V14 decoder and must be rejected at the block header
level.

Back-ref: `internal/modules/blockio/reader/column.go:readColumnEncoding`

---

## SPEC-ROOT-014: Single-Tier Block TOC

**Invariant:** V14 blocks use a unified single-tier column TOC (`column_count[4]` +
`reserved2[8]` at header offsets 12–23), identical to the V12 header layout. A two-tier
intrinsic/attribute split was evaluated and deferred pending further profiling.

Back-ref: `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## SPEC-ROOT-015: Raw Block Bytes Must Route Through Cache

**Invariant:** All raw block byte fetches (`ReadGroup`, `ReadBlocks`) must route through
`r.cache` using the key `fileID+"/block/"+blockIndex`. Direct calls to `ReadCoalescedBlocks`
or `provider.ReadAt` from `ReadGroup`/`ReadBlocks` are forbidden — they bypass the multi-tier
cache and cause redundant S3 reads on every query for the same block.

**Exception:** When `r.fileID == ""` no stable cache key can be formed; `ReadGroup` falls
through to a direct `ReadCoalescedBlocks` call. Any code path that constructs a `Reader`
without a fileID opts out of block-level caching and must document that trade-off.

**Rationale:** S3 read latency (~50–100 ms) dominates query cost. Without block-level caching,
repeated queries over the same time window re-fetch the same raw bytes on every call even when
all decoded section data (intrinsic columns, trace index, bloom) is already cache-warm.
Profiling showed `ReadCoalescedBlocks` allocating >294 MB per benchmark window — entirely from
avoidable S3 re-reads. Routing through `r.cache` eliminates these re-reads for hot blocks.

**Cache contract:**

- On a full cache hit (all `cr.BlockIDs` present): `ReadGroup` returns without any S3 I/O.
- On any cache miss: `ReadGroup` fetches the full group (coalesced), stores every block via
  `r.cache.Put`, then returns. Re-fetching already-cached blocks in the same group is
  deliberate — it avoids re-coalescing a partial group at the cost of re-reading a few
  extra bytes from a pooled buffer (nanoseconds vs the 75 ms S3 round-trip).
- `r.cache.Put` errors are silently discarded — eviction or size limits are not fatal.

Back-ref: `internal/modules/blockio/reader/reader.go:Reader.ReadGroup`,
`internal/modules/blockio/reader/reader.go:Reader.ReadBlocks`

---

## SPEC-ROOT-016: Single-Pass Processing — Only Load What You Use

**Process data in a single pass. Never load data into an intermediate collection just to iterate it again.**

**Rules:**

- **One pass per data source.** When scanning a column or index, accumulate results immediately
  as each entry is visited. Do not collect values into a slice and process them in a second loop.
- **Pull only matching data.** Only fetch, decode, or allocate data that is actually needed for
  the current query. If a dimension is absent (no group-by, no predicate, nil column), skip the
  allocation entirely — do not allocate a zero-filled placeholder to satisfy a generic code path.
- **Prefer dense arrays over hash maps for hot lookup paths.** For per-span lookups keyed by a
  bounded integer, a pre-allocated array gives O(1) cache-friendly access. Hash maps have O(1)
  amortized cost but cause cache misses at high element counts that dominate CPU. Use hash maps
  only when the key space is unbounded or sparse.
- **Allocate proportional to the actual problem size.** Size arrays to the number of distinct
  values present in the data, not to a theoretical maximum.

**Rationale:**

Multi-pass designs that collect intermediate state compound allocation cost, increase GC pressure,
and destroy CPU cache locality. In production at 150 M spans per query window, a single extra
`[]BlockRef` allocation added 1.2 GB of heap; hash map probing in the accumulation hot path
consumed 54% of query CPU. Single-pass dense-array designs eliminated both.

**Enforcement:**

Code review. When adding any new data-processing path, verify no intermediate collection is built
solely to be iterated a second time.

---

## SPEC-ROOT-017: Column Threading Invariant — Propagate wantColumns End-to-End

**Invariant:** The set of columns needed by a query must be threaded from compilation through
every decode step. No subsystem may decode, load, or scan column data for columns absent from
the query's needed set.

**Rules:**

- `ProgramWantColumns(program)` is the authoritative source of predicate column names at
  compile time. It must be threaded into `ParseBlockFromBytes`, `SpanFieldsAdapter`, and
  `loadIntrinsicCache`.
- `secondPassCols` (computed in `computeColumnFilters`) augments `wantColumns` with output
  columns (searchMetaCols, traceIntrinsicColumns, SelectColumns). It is the correct filter
  for second-pass block decodes and for `loadIntrinsicCache` in result-materialization paths.
- `loadIntrinsicCache` must skip any intrinsic column not in its `wantCols` parameter.
  When `wantCols` is nil, all columns are loaded (match-all and GetTraceByID paths).
- Per-block `isDualStorage` detection must be computed once per block, not once per
  `IterateFields` call. The result is passed to the adapter constructor.
- Stream paths (`streamFilterProgram`) must pass
  `wantCols=ComputeSecondPassCols(program, selectColumns)`, computed once before the row loop.
  The result may be nil for programs with no predicates and no SelectColumns; nil is valid and
  means load-all.
- Paths that need all fields (GetTraceByID) must pass `wantCols=nil`. The structural result
  path passes `wantCols=ComputeSecondPassCols(nil, opts.SelectColumns)`, which is nil when
  SelectColumns is empty (load-all) and non-nil when SelectColumns is provided. `nil` wantCols
  is explicitly valid and means "load all intrinsic columns" — it is not a missing-value bug.
- Callers passing non-nil wantCols for span:end synthesis must include both span:start and
  span:duration in the set; omitting either suppresses span:end synthesis (the synthesis check
  requires both keys present in intrinsicCache).

**Addendum (2026-07-04):** The blanket claim above that "Paths that need all fields
(GetTraceByID) must pass `wantCols=nil`" no longer describes `GetTraceByID`'s decode strategy
as a whole — it describes only its *materialization* sub-step. As of the trace-by-ID index
wiring effort (SPEC-ROOT-018), `GetTraceByID`'s no-index scan path (`scanTraceByID`,
`reader.go`) first runs a cheap `WantOnly({"trace:id"})` match phase over every block
(`scopeMatchingBlocks`) and only re-decodes blocks that actually contain a matching row with
`WantAll()` (`parseBlocksWithWant`). The index path (`getTraceByIDViaIndex` →
`materializeTraceGroup`) never enumerates every block at all — it decodes only the exact
blocks an index hit names, always with `WantAll()` since full-field materialization is
required at that point regardless of path. **The invariant this rule protects (materialization
of a matched span must see every field, since `SpanFieldsAdapter` needs an open-ended,
non-fixed column set — see SPEC-ROOT-018) still holds; only the "every block, every column,
unconditionally" framing is now stale.** See SPEC-ROOT-018 and
`internal/modules/blockio/reader/SPECS.md` SPEC-012 for the corrected, complete statement.

**Rationale:**

Decoding intrinsic columns not needed by a query wastes CPU and memory on every matched span.
For queries against old-format files (pre-dual-storage), this is especially expensive because
`loadIntrinsicCache` performs an O(N_refs) linear scan per column. The per-span dual-storage
detection loop is also a hidden O(N_intrinsics) cost per span per `IterateFields` call.

**Enforcement:**

Code review. Any new `NewSpanFieldsAdapterWithReader` call site must document its `wantCols`
and `isDualStorage` choice. `nil` wantCols is acceptable only for paths that need all fields.

Back-ref: `internal/modules/blockio/span_fields.go:loadIntrinsicCache`,
`internal/modules/blockio/span_fields.go:IterateFields`,
`internal/modules/blockio/span_fields.go:NewSpanFieldsAdapterWithReader`,
`internal/modules/executor/stream.go:computeColumnFilters`,
`internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/modules/executor/predicates.go:ComputeSecondPassCols`,
`query_traceql.go:streamFilterProgram`,
`api.go:QueryTraceQL`,`
`reader.go:GetTraceByID`

---

## SPEC-ROOT-018: GetTraceByID — Breaking Signature Change, Index-Is-Authoritative Contract
*Added: 2026-07-04*
*Revised: 2026-07-06 (issue #473, NOTE-VI-071): the trace-by-ID index is now AUTHORITATIVE,
not a hint-with-fallback. `getTraceByIDFullScan` (the unconditional index fallback) is gone;
the scan survives ONLY as the no-index path (`scanTraceByID`) for callers that pass no lister.
The original "index is a hint" contract is preserved below in strikethrough form for history;
the authoritative contract that supersedes it follows.*

**This is a breaking change to an exported root-package API function**, authorized explicitly
by the user (not a unilateral agent decision) as part of fixing a production incident: a live
heap profile on the dev test cluster showed `GetTraceByID`'s unconditional full-file block scan
driving a 64Gi-limited querier to 40.5GB/95% heap during a single in-flight `/api/traces/{id}`
request. Full history in `.bob/state/brainstorm.md` and `.bob/state/plan.md`.

**Old signature (pre-2026-07-04):** `GetTraceByID(r *Reader, traceIDHex string) (results
[]SpanMatch, err error)`.

**New signature:** `GetTraceByID(ctx context.Context, r *Reader, traceIDHex string, lister
valueindex.LookupStore, tenant, indexPrefix string, queryMinSec, queryMaxSec uint64) (results
[]SpanMatch, err error)`.

**Original contract (2026-07-04, ~~superseded 2026-07-06~~):** ~~the index was a hint, never
authoritative for absence; any index miss / decode failure / index-data skew fell back to
`getTraceByIDFullScan` and returned exactly what a full scan would have. That fallback is now
gone (issue #473); the authoritative contract below replaces it.~~

**Authoritative contract (2026-07-06, NOTE-VI-071):** the trace-by-ID index is authoritative
when consulted, exactly like the search/metrics index (SPEC-ROOT-019). The two paths are now
mutually exclusive, selected by whether the caller supplies a lister:

1. **Index path — `lister != nil && tenant != ""`.** `GetTraceByID` consults the trace-by-ID
   value index (`internal/modules/valueindex/traceindex.go`, `TraceGroup`/`SpanEntry`) via
   `getTraceByIDViaIndex`: it discovers candidate index files scoped to `[queryMinSec,
   queryMaxSec]`, decodes them, and on finding the trace, resolves every `SpanEntry`'s exact
   `BlockRef`+`RowIdx` directly — touching only the blocks the index names, never enumerating
   the whole file. Three outcomes, and **no fallback to a scan**:
   - **Hit** → the covered spans (complete for this file).
   - **Miss** → an empty result, no error. This includes "no candidate index file covers the
     window" (`DiscoverIndexFiles` returns zero keys) and "a readable candidate holds no entry
     for the trace." The index is authoritative for absence: absence means not found.
   - **Error** → any of: a discovery-time failure (e.g. object-store `List` error), a
     fetch/decode failure on a candidate file (corrupt index), an index-named block that does
     not resolve in `r` (`BlockIndexForPage` `!ok` — also how a cross-file span manifests, see
     item 3), a block the reader returns no bytes for, a block that fails to parse, or a
     resolved row whose own `trace:id` column does not match (`rowMatchesTraceID`; defensive
     re-verify). These are index/data inconsistency: the index and the data file are out of
     sync, surfaced so it is observable rather than masked by a silent scan. Contrast the old
     contract, where every one of these was a routine silent fallback. **No indeterminacy of
     any kind may produce a wrong or partial result** — it produces an error instead.
2. **No-index path — `lister == nil || tenant == ""`.** There is no index to consult (WAL
   blocks, which are freshly-ingested and never indexed — see tempo `vblockpack/wal_block.go`
   which permanently passes `nil`; or a backend block when the value-index query feature is
   disabled). `scanTraceByID` performs an exact, complete scan of `r`. This is the sole correct
   path when no index exists — it is **not** a fallback from a failed index attempt.
3. **v1 scope decision — no cross-file trace assembly; sourceRef scoping (NOTE-VI-076, issue
   #479).** `GetTraceByID` operates on exactly one `*Reader` for one file. A compacted
   `TraceGroup` index file commonly spans many source blocks, so `DiscoverIndexFiles` returns
   the SAME wide file as a candidate for EVERY block whose window overlaps it; the resolved
   group therefore carries `SpanEntry`s belonging to sibling blocks, not to `r`. `GetTraceByID`
   takes a `sourceRef` — the object key of the block `r` was opened against, matching what the
   write path stamps on each `SpanEntry.SourceRef` (tempo `blockObjectKey`). When non-empty,
   `materializeTraceGroup` drops any span whose `SourceRef != sourceRef` BEFORE resolving it
   against `r`, so a sibling entry is never mistaken for skew (that sibling block resolves it in
   its own parallel call). If no span survives the filter, that is an authoritative "not found
   in THIS block" `(nil, nil)`, not an error. Only an entry that matches this `sourceRef` yet
   still fails to resolve (`BlockIndexForPage` `!ok`, or a `trace:id` re-verify mismatch)
   remains genuine index/data skew (item 1). An empty `sourceRef` disables the filter (v1
   back-compat). Genuine cross-file trace assembly (opening additional readers for other files
   an index names) is explicitly out of scope for v1.
4. **`queryMinSec`/`queryMaxSec`** scope the index discovery window. Pass `(0,
   math.MaxUint64)` when no tighter hint is available — this widens the candidate set. Under
   the authoritative contract a too-narrow window that excludes the covering file reads as a
   miss (not found), so callers must pass a window that actually contains the trace (backend
   blocks use their own wall-clock range).

**Accepted, known coverage gap (issue #473):** any trace written before the trace-by-ID index
began building coverage (NOTE-VI-070, commit `fd2726f0`) has no index entry and, on the index
path, reads as "not found." There is no backfill and none is planned; retention ages out the
uncovered window. This is a deliberate, accepted consequence of making the index authoritative,
per the issue's maintainer direction ("the data-coverage gap ... is an accepted, known
consequence for this environment, not a blocker to shipping the removal").

**Known-affected external consumer (breaking-change blast radius):** the `tempo` checkout at
`/home/mdurham/source/blockpack_collection/tempo`'s `vblockpack` package has two call sites,
both confirmed and updated as part of this same change (Stage 6). **Correction (2026-07-05):**
this back-reference originally cited `/home/mdurham/source/tempo-mrd` — a separate checkout of
the same `mattdurham/tempo` repo that exists alongside the correct one — as the location where
Stage 6's work landed; that was a checkout-naming mixup discovered after the fact. The actual
Stage 6 changes are in `/home/mdurham/source/blockpack_collection/tempo`. `tempodb/encoding/
vblockpack/backend_block.go` (`(*blockpackBlock).FindTraceByID`) now wires a real
`LookupStore`/`indexPrefix` from the querier's configured value-index query reader
(`getValueIndexQueryReader()`, the same singleton the search/metrics index path uses) when
`value_index_query.enabled`, using the block's own `BlockMeta.StartTime`/`EndTime` as the time
hint; the store is `nil` (behaviour-neutral full scan) when the path is disabled. The
LookupStore interface is re-exported at the root as `blockpack.LookupStore` /
`blockpack.TraceIndexGetter` (NOTE-ROOT-021, issue #468) precisely because tempo cannot import
blockpack's internal `valueindex` package. **This completes steps 1–3 of issue #468; steps 4–5
(prove index-hit == full-scan against real dev/prod data, then remove `getTraceByIDFullScan`)
remain deliberately out of scope — the fallback stays until proven redundant.** `tempodb/encoding/vblockpack/wal_block.go` (`(*walBlock).FindTraceByID`)
permanently passes `nil`/`""` since WAL data can never have index coverage. Any other external
caller of `GetTraceByID` not enumerated here will fail to compile against the new signature —
loudly, at
build time, not silently at runtime.

**Full-field materialization requirement (relates to SPEC-ROOT-017):** every span actually
returned by `GetTraceByID` — via either path — is always decoded with `WantAll()` at the point
of materialization, never `WantOnly()`. `NewSpanFieldsAdapterWithReader` exposes whatever
columns are present in the decoded block, an open-ended, per-span attribute set that a fixed
column list would silently truncate (`internal/modules/blockio/reader/SPECS.md` SPEC-012). The
two-phase `WantOnly`/`WantAll` split in the no-index scan narrows which *blocks* get the
expensive `WantAll()` decode; it never narrows which *columns* a materialized span exposes.

Back-refs: `reader.go:GetTraceByID`, `:getTraceByIDViaIndex`, `:scanTraceByID`,
`:materializeTraceGroup`, `:findTraceGroupInCandidates`, `:scopeMatchingBlocks`,
`internal/modules/valueindex/lookupstore.go:LookupStore`,
`gettracebyid_index_test.go` (authoritative index-path tests: hit / authoritative-miss /
discovery-error / corrupt-candidate-error / cross-file-skew-error / stale-entry-error),
`gettracebyid_test.go` (no-index scan path), `traceindex_pipeline_test.go`
(end-to-end multi-L0-candidate merge on the authoritative path), `api_test.go`.

---

## SPEC-ROOT-019: Search/Metrics Value Index — Authoritative for Covered Columns
*Added: 2026-07-06 (issue #474, NOTE-VI-047)*

The value index is **authoritative** for the search (`QueryTraceQLFromIndex`) and metrics
(`ExecuteMetricsTraceQL` with `TraceMetricOptions.ValueIndex`) query paths — **not** a
hint-with-speculative-fallback. When every leaf predicate the query needs resolves against the
index, the index-produced result is the complete, correct answer for that file. This is
deliberately the **opposite** posture to SPEC-ROOT-018's trace-by-ID "index is a hint" contract:
the search/metrics span-attribute index has been written and matured over a long window (the
block writer treats it as *"the authoritative source for pruning"*), whereas the trace-by-ID
`TraceGroup` index only began building coverage on 2026-07-06 (NOTE-VI-070) and therefore keeps
its fallback until coverage is proven (issue #473). The two indexes are at different maturity
points; do not conflate their contracts.

**What "authoritative" removed:** the `maxIndexHits` speculative fallback in
`executor.QueryTraceQLFromIndex`. Previously a *correctly-answered* query whose result set
exceeded `DefaultMaxIndexHits` (100k) was discarded and re-run as a full scan "because a scan is
cheaper" — a scan that could only reproduce the identical result at higher cost. That heuristic,
and the `maxIndexHits` parameter on both the executor and the exported root
`QueryTraceQLFromIndex`, are gone. Size no longer influences correctness or path selection.

**Fallback is now reserved for genuine "the index cannot answer this":**

1. **No coverage for a leaf.** A negation (`!=`, `!~`) or otherwise unindexable predicate has no
   fast VI path; the querier does not `Add` that column, `viMatchSpans` returns `ok=false`, and
   the caller falls back to a full scan. This is a **documented, accepted exception**, not a
   regression — negation-heavy queries scan as before. `(nil, false, nil)`.
2. **Wrong query type.** Structural/pipeline queries are not filter expressions and use their own
   execution paths. `(nil, false, nil)`.
3. **Metrics shape unsupported by the VI path.** Group-by, or a non-`count`/`rate` function, or a
   file predating per-span timestamps (`TimeSec == 0`), falls back. `(nil, false, nil)`.

**Index/data inconsistency is now an error, not a silent scan.** When a matched span names a
block/page absent from the data file (`BlockIndexForPage` `!ok`, or a block the reader returns no
bytes for), the index and the file are out of sync. Because the index is authoritative, this is
surfaced as an error `(nil, false, err)` — the querier logs it and falls back to a correct full
scan, but the inconsistency is observable rather than masked. Contrast SPEC-ROOT-018, where the
same skew is a routine, silent fallback because that index is only a hint.

**Breaking signature change (authorized by issue #474):** the exported
`blockpack.QueryTraceQLFromIndex` lost its trailing `maxIndexHits int` parameter, as did the
internal `executor.QueryTraceQLFromIndex`. The single external consumer — tempo's
`vblockpack` package (`tempodb/encoding/vblockpack/value_index_query.go:tryIndexFetch`) — is
updated in the same commit cycle: it drops the argument and now logs+falls-back on a non-nil
error (index/data inconsistency) rather than treating error identically to a routine miss.

Back-refs: `internal/modules/executor/search_trace_vi.go:QueryTraceQLFromIndex`,
`api.go:QueryTraceQLFromIndex`, `api.go:ExecuteMetricsTraceQL`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetricsFromVI`,
`search_trace_vi_test.go` (AND/OR/authoritative-large-result/inconsistency-is-error tests),
`valueindex_query.go` (querier-flow doc). External: tempo
`tempodb/encoding/vblockpack/value_index_query.go`, `backend_block.go`.

**Addendum 2026-07-06 (issue #481, NOTE-VI-078) — the inconsistency error is now ENFORCED
end-to-end.** The blockpack executor already returned `(nil, false, err)` for an index/data
inconsistency (above). The tempo consumer, however, still *masked* it: `tryIndexFetch` logged the
error and fell through to a full block scan, so index corruption was silently worked around
rather than surfaced. As of NOTE-VI-078 the tempo search path (`tryIndexFetch` → `Fetch`)
PROPAGATES that error and FAILS the query — no scan. This makes SPEC-ROOT-019's "inconsistency is
an error" clause true at the querier boundary, not only inside the executor, matching how
SPEC-ROOT-018's trace-by-ID path (NOTE-VI-071) stopped masking the same skew behind a scan.

**Still in force after NOTE-VI-078:** the three ROUTINE DECLINE categories above (no-coverage
negation/unindexable leaf; non-filter query; unsupported metrics shape) STILL fall back to a
correct full scan — they are "the index cannot answer this query SHAPE," not corruption.
Removing *those* fallbacks (issue #481's full directive) additionally requires the
selectivity-aware execution strategy of #481 part 2 (recognizing low-selectivity predicates like
`kind=server` where index pruning cannot help), which is an open design question, not yet
scoped into its own issue. NOTE-VI-078 deliberately closes only the inconsistency-masking gap,
which is safe to flip in isolation because it never changes behaviour for a query the index could
legitimately answer or legitimately decline — only for one it answered against data that no
longer exists.

---

## SPEC-FORMAT-001: All Metadata Sections Must Be ToC-Driven for Selective Decoding

**Invariant:** Every metadata section that contains per-column data MUST be stored as an
individual entry in the unified Table of Contents (ToC), addressable by a typed
`(Type, SubType, Name)` key. Monolithic blobs that pack all columns together are
forbidden for new file format versions.

**Rationale:** A monolithic section blob requires:

1. Full S3/object-storage fetch of all column data (regardless of query selectivity)
2. Full snappy decompression of all column data on every access
3. GC pressure from large allocations that are mostly unused

For a query touching 2 columns out of 200, a monolithic range index requires decoding
~200x more data than necessary.

**Wire format — Footer V8:**

Footer V8 is 18 bytes (identical layout to Footer V7; distinguished by `version=8`):

```
magic[4]=0xC011FEA1 · version[2]=8 · toc_offset[8] · toc_length[4]
```

The footer points to a snappy-compressed Table of Contents blob.

**Wire format — ToC blob (after snappy decompression):**

```
entry_count[4 LE] · signal_type[1] · reserved[3] · ToCEntry[entry_count]
```

**Wire format — ToCEntry (variable length):**

```
type[4 LE] · subtype[4 LE] · name_len[2 LE] · name[name_len] · offset[8 LE] · length[4 LE]
```

Minimum entry size (name=""): 22 bytes. Maximum name length: `MaxNameLen` (1024 bytes).

**Type and SubType constants:**

```
ToCTypeMetadata = 1   // file-level metadata sections
ToCTypeIndex    = 2   // file-level index structures
ToCTypeBlock    = 3   // raw block data blobs (reserved; not used in V8)

// SubTypes for ToCTypeMetadata (Type=1)
ToCSubTypeRange     = 1   // per-column range index
ToCSubTypeSketch    = 2   // per-column KLL/sketch
ToCSubTypeBloom     = 3   // file-level bloom filter
ToCSubTypeIntrinsic = 4   // per-column intrinsic blob
ToCSubTypeTrace     = 5   // compact trace index
ToCSubTypeTS        = 6   // timestamp index

// SubTypes for ToCTypeIndex (Type=2)
ToCSubTypeBlockIndex = 7  // block offset table
```

**Lookup key:** readers build `map[ToCKey]{Offset, Length}` where
`ToCKey = struct{ Type, SubType uint32; Name string }`.

**Example entries:**

```
(1, 1, "resource.service.name") → per-column range index blob
(1, 2, "resource.service.name") → per-column KLL/sketch blob
(1, 3, "")                      → file-level bloom filter blob
(1, 4, "resource.service.name") → per-column intrinsic blob
(1, 5, "")                      → compact trace index blob
(1, 6, "")                      → timestamp index blob
(2, 7, "")                      → block index blob
```

**Sections to convert:** `SectionRangeIndex` (0x02) and `SectionSketchIndex` (0x05)
are currently monolithic blobs in V14 (Footer V7) files and MUST be represented as
individual per-column ToCEntry records in V8 files.

**Version support:** V8 is the only supported 18-byte magic footer version. The reader
rejects any file whose magic matches but whose version field is not 8 (including V7).
Legacy V3/V4/V5/V6 files (non-18-byte footers) remain readable via the legacy path.
Old readers that pre-date V8 will fail on V8 files; readers must be upgraded before
V8 writers are deployed.

**Enforcement:** Any new multi-column metadata section added without individual ToCEntry
records will be rejected in code review. Existing sections (`SectionRangeIndex`,
`SectionSketchIndex`) must be per-column in V8 files.

Back-ref: `internal/modules/blockio/shared/constants.go` (FooterV8Version, ToCType*,
ToCSubType* constants to be added)
Back-ref: `internal/modules/blockio/shared/types.go` (ToCEntry, ToCKey structs to be
added)
Back-ref: `internal/modules/blockio/writer/writer.go:writeV14Sections` (replaced by
`writeV8Sections` emitting `[]ToCEntry` and Footer V8)
Back-ref: `internal/modules/blockio/writer/metadata.go:writeFooterV7` (parallel
`writeFooterV8` function to be added)
Back-ref: `internal/modules/blockio/reader/parser.go:tryReadFooterV7` (parallel
`tryReadFooterV8` to be added)
Back-ref: `internal/modules/blockio/reader/parser.go:parseSectionsLazyV14` (parallel
`parseSectionsV8` using ToC map)
Back-ref: `internal/modules/blockio/reader/parser.go:scanRangeIndexOffsets` (replaced
by direct ToC key lookup)
Back-ref: `internal/modules/blockio/reader/sketch_index.go:parseSketchIndexSection`
(replaced by per-column ToC-keyed lazy load)

---

## SPEC-OBS-001: Context Propagation — All Query Entry Points Must Accept ctx

*Added: 2026-06-19*

Every public blockpack query entry point (`QueryTraceQL`, `QueryTraceQLWithProgram`,
`ExecuteMetricsTraceQL`) and every internal executor
entry point (`Collect`) MUST accept `context.Context` as their first
parameter. Nil is normalized to `context.Background()` at the entry point; downstream code
may trust ctx is non-nil.

**Rationale:** Without a propagated context, blockpack spans cannot attach to the distributed
trace from the Tempo caller (issue #368). A nil-normalization guard is required for callers
that pass nil defensively (e.g. tests, migration helpers).

**Verification:** `TestCollect_BackgroundContext` and `TestCollect_ContextCancellation`
(context_propagation_test.go). All NOTE-058 TODO comments must be absent.

Back-ref: `api.go:QueryTraceQL,QueryTraceQLWithProgram,ExecuteMetricsTraceQL`,
`internal/modules/executor/stream.go:Collect`.

---

## SPEC-OBS-002: Mandatory Spans Per Query Type

*Added: 2026-06-19*

Every call to `Collect` MUST produce the following OTel span hierarchy when a TracerProvider
is configured:

```
blockpack.query          — one per Collect call (wraps full execution)
  blockpack.planner      — one per planBlocks call, carries pruning counts
  blockpack.block (×N)   — one per dispatched block in the block-scan path
```

Spans are no-ops when no TracerProvider is configured (OTel global noop). The hierarchy is
only produced on the `planBlocks → scanBlocks` execution path; intrinsic fast-path and
structural paths are exempt.

**Attribute contract:**

- `blockpack.query`: no attributes required (span name is sufficient)
- `blockpack.planner`: MUST set `blockpack.planner.total_blocks`, `selected_blocks`,
  `pruned_by_time`, `pruned_by_index`, `pruned_by_bloom`, `pruned_by_colstats`,
  `pruned_by_intrinsic_toc`, `explain`
- `blockpack.block`: MUST set `blockpack.block.index`

**Verification:** `TestQueryEmitsSpans` (otel_integration_test.go).

Back-ref: `internal/modules/executor/stream.go:Collect,scanBlocks`,
`internal/modules/executor/otel_spans.go:emitPlannerSpan,startBlockSpan`.

---

## SPEC-OBS-003: IsRecording() Guard — No Attribute Allocations on Unsampled Queries

*Added: 2026-06-19*

Every `span.SetAttributes(...)` call that runs on the hot query path MUST be wrapped in
`if span.IsRecording() { ... }`. The `attribute.Int()` and `attribute.String()` functions
allocate `attribute.KeyValue` structs; calling them on a noop span wastes allocations on
every unsampled query (the production majority).

The noop tracer's `Start` call is ~2ns and creates a noop span that always returns false for
`IsRecording()`. The guard ensures zero new allocations per query when no TracerProvider is
active.

**Enforcement:** Code review checklist. Every `SetAttributes` in `otel_spans.go` and
`tracer.go` must have the guard. Any future span attribute added to the hot path MUST follow
this pattern.

**Verification:** `TestAttachCacheStats_IsRecording` (otel_spans_test.go) verifies attributes
are set on a recording span. The noop path is implicitly verified by
`TestQueryNoTracerProvider_NoSpans`.

Back-ref: `internal/modules/executor/otel_spans.go:attachCacheStats,emitPlannerSpan,startBlockSpan`.

---

## SPEC-OBS-004: Block Span Cache Attributes — Aggregate Per-Block, Not Per-Fetch

*Added: 2026-06-19*

Cache observability for `blockpack.block` spans MUST be implemented as aggregate hit/miss
counts (not per-fetch child spans). Per-fetch spans in tieredcache would add ~10–100 ns per
`GetOrFetch` call × 8 section fetches × hundreds of blocks = significant overhead on
unsampled queries.

The `CacheStats` struct (`internal/modules/blockio/reader/cache_stats.go`) is a stack-
allocated `[CacheStatsCount]int32` pair (hits, misses) per section index. It is passed as a
nil-safe `*CacheStats` pointer into `readBlockColumnarWithCache`. Attributes MUST only be
set on the span when `cs != nil && span.IsRecording()`.

Section-to-attribute key mapping:

- `CacheStatsSectionToc` (0) → `blockpack.cache.toc.hits`, `blockpack.cache.toc.misses`
- `CacheStatsSectionCol` (1) → `blockpack.cache.col.hits`, `blockpack.cache.col.misses`

**Verification:** `TestAttachCacheStats_IsRecording`, `TestAttachCacheStats_ZeroStats`,
`TestAttachCacheStats_NilStats` (otel_spans_test.go).

Back-ref: `internal/modules/blockio/reader/cache_stats.go:CacheStats`,
`internal/modules/blockio/reader/columnar_read.go:readBlockColumnarWithCache`,
`internal/modules/executor/otel_spans.go:attachCacheStats`.

---

## SPEC-OBS-005: Per-Section Cache Stat Collection via Fetched-Flag Pattern

*Added: 2026-06-19*

Cache hit vs miss detection in `readBlockColumnarWithCache` MUST use a fetched-flag wrapper
rather than inspecting return values or adding state to the tieredcache. The pattern:

```go
var fetched bool
result, err = cache.GetOrFetchV8Section(fileID, section, 0, key, func() ([]byte, error) {
    fetched = true
    return innerFetch()
})
if err == nil && cs != nil {
    if fetched { cs.Misses[sectionIdx]++ } else { cs.Hits[sectionIdx]++ }
}
```

This is zero-cost when `cs == nil` (the guard prevents the branch), and requires no changes
to the tieredcache API. The tieredcache Prometheus metrics (`blockpack_typed_cache_requests_total`)
remain the authoritative aggregate-level signal; OTel attributes serve per-trace debugging.

For the combined ToC+columns batch fetch path (`sectionMixedFetcher`): if `toc` is non-nil
after the batch (cache hit), record a ToC hit. For column-level: `len(keepCols)` are cold
misses; the rest of `wantColumns` are warm hits.

Back-ref: `internal/modules/blockio/reader/columnar_read.go:readBlockColumnarWithCache`.

---

## SPEC-ROOT-020: QueryStructuralFromIndex / CompileStructuralLegs — Root Public API for Index-Driven Structural Queries
*Added: 2026-07-08 (issue #489, plan-d.md D5/D5b)*

**What this is.** `QueryStructuralFromIndex` (`structural.go`) is the root-package public entry point for the index-driven structural-query path, mirroring `QueryTraceQLFromIndex`'s existing shape and doc-comment conventions (`api.go`): a thin wrapper with no heavy lifting of its own — `modules_executor.ExecuteStructuralFromIndex` (SPEC-STRUCT-9) does the actual work. Lives in its own new file (`structural.go`) rather than `vcnt.go`/`api.go` per the Rulings log (plan-d.md, ruling 2), deliberately independent of the concurrent #481 session's uncommitted `vcnt.go` re-exports.

**Signature and parameter contract:**
```go
func QueryStructuralFromIndex(
    ctx context.Context,
    traceqlQuery string,
    leftSource, rightSource ValueIndexSource,
    vcntData []byte, vcntDir []VCNTChunkDirEntry,
    traceGroupStore LookupStore,
    tenant, indexPrefix string,
    readerFor StructuralReaderProvider,
    minTS, maxTS uint64,
    indexOnly bool,
    opts QueryOptions,
) (results []SpanMatch, ok bool, err error)
```
- `leftSource` is REQUIRED (never nil): L is unconditionally the structural walk's anchor (SPEC-STRUCT-9/NOTE-VI-092) — a nil `leftSource` is a routine decline (`ok=false, err=nil`), identical in spirit to `QueryTraceQLFromIndex`'s own "no index coverage supplied" decline.
- `rightSource` MAY be nil (e.g. a match-all right leg, or a caller declining to pre-resolve it) — it is consulted ONLY for the optional cost-based intersection prefilter (plan-d.md D4 step 4), which is automatically and silently disabled whenever `rightSource` or `vcntData` is nil, never attempted with a possibly-nil dereference. This nil-guard independently avoids the same `rightSource.AllResults()` panic risk that task #10 fixed one layer down inside `ExecuteStructuralFromIndex` itself — belt-and-suspenders, not a coincidence.
- `vcntData`/`vcntDir` is one decoded VCNT section scoring BOTH legs over the SAME window (mirrors `queryplan.ClassifyProgramVCNT`'s single-window composition); nil/empty always skips the cost-based intersection prefilter — the query still executes correctly, just without that optimization.
- `traceGroupStore` and `tenant` are REQUIRED (NOTE-VI-073) — there is no scan fallback for a caller that omits them; this mirrors `GetTraceByID`'s own authoritative-index contract (SPEC-ROOT-018).
- `readerFor` (`StructuralReaderProvider`) resolves each candidate trace's `SpanEntry.SourceRef` to an open `*Reader` for Option A's multi-file materialization (SPEC-VIS-3) — the caller owns reader caching/pooling, exactly as `MaterializeTraceGroupMultiFile`'s own contract states.
- `indexOnly` mirrors issue #487's `IndexOnly`/`ErrSliceIndexCoverageGap` pattern (SPEC-VIS-2): when true, a coverage gap that would otherwise be a routine decline instead returns `ErrStructuralIndexCoverageGap` — a time-sliced structural job has no safe scan fallback across slice boundaries.

**Scope (v1): 2-node structural queries only.** A chain flattening to other than exactly 2 filter nodes, or a negated operator (`!>>`, `!>`, `!~` — routed to `modules_executor.ExecuteNegatedStructuralFromIndex`, SPEC-STRUCT-10, not this function), is a routine decline (`ok=false, err=nil`); the caller falls back to `QueryTraceQL` (the scan path), which already handles the general case and is not removed by this function's existence.

**`CompileStructuralLegs(traceqlQuery string) (leftProg, rightProg *Program, op StructuralOp, ok bool, err error)`** is the root-level public re-export of `modules_executor.CompileStructuralLegs` (itself a zero-logic passthrough to `compileStructuralPair`, NOTE-VI-087) — exposed (D5b, task #11) so an external caller (tempo's DT1 dispatch) can classify a structural query's operator and build BOTH `ValueIndexSource`s (left AND right) BEFORE calling `QueryStructuralFromIndex`. Without this, a permanently-nil `rightSource` would silently disable D4's intersection prefilter and the entire discovery-seed cost mechanism (D4-SEED ruling) on the actual production dispatch path — `CompileStructuralLegs` exists specifically so the caller has the operator/leg information needed to decide whether building a right-side VI source is worth the cost, before ever calling `QueryStructuralFromIndex`.

**Public re-exports (D5b, task #11), all zero-cost type aliases / passthroughs, not adapters:**
- `ErrStructuralIndexCoverageGap` = `modules_executor.ErrStructuralIndexCoverageGap` (mirrors the `ErrValueIndexFileNotFound` re-export convention, `valueindex_query.go`) — without this, callers cannot `errors.Is()` against the typed coverage-gap error this path returns.
- `StructuralOp` = `traceqlparser.StructuralOp` (type alias), plus the 8 operator constants (`OpDescendant`, `OpChild`, `OpSibling`, `OpAncestor`, `OpParent`, `OpNotSibling`, `OpNotDescendant`, `OpNotChild`) — re-exported so a caller can classify a `StructuralOp` (e.g. deciding whether a query is negated, to route to D4 vs D6) without importing `internal/traceqlparser` directly.
- `StructuralReaderProvider` = `modules_executor.StructuralReaderProvider` — verified a genuinely zero-cost alias, not an adapter: `Reader` is itself a plain alias for `modules_reader.Reader` (`reader.go:44`), so root callers pass their own `*blockpack.Reader`-returning functions here unchanged.
- `StructuralSelectivityClassifier` = `modules_executor.StructuralSelectivityClassifier` — exported only so advanced callers/tests can construct their own classifier; `QueryStructuralFromIndex` builds one internally over `queryplan.ClassifyProgramVCNT` in the normal case.

**Result conversion.** Each `SpanMatch.Fields` returned is materialized (via `NewSpanFieldsAdapterWithReader` + `.Clone()`, then the adapter released — NOTE-ALLOC-4) and safe to retain after return, the same conversion pattern `QueryTraceQLFromIndex` and `QueryTraceQL`'s own structural case use. This depends on D4's `SpanMatch.Block` fix (`materializeConfirmedSpanBlocks`) always producing a real, non-nil parsed `Block`.

**Panic safety.** `QueryStructuralFromIndex` wraps its body in a `recover()` that converts any panic into a returned error (`internal error in QueryStructuralFromIndex: %v`) — SPEC-ROOT-001 compliant, matching `QueryTraceQL`'s own existing top-level recover convention.

Back-ref: `structural.go:QueryStructuralFromIndex, CompileStructuralLegs, ErrStructuralIndexCoverageGap, StructuralOp, StructuralReaderProvider, StructuralSelectivityClassifier`. Tests: `structural_test.go`, `search_trace_vi_realvi_test.go`. Issue #489.

## SPEC-ROOT-021: QueryNegatedStructuralFromIndex — Root Public API for Index-Driven NEGATED Structural Queries
*Added: 2026-07-08 (issue #489, Phase D holistic review fix pass — closes the SPEC-ROOT-020 gap where D6 was described as routed-to but had no actual root entry point)*

**What this is.** `QueryNegatedStructuralFromIndex` (`structural.go`) is the root-package public entry point for the index-driven NEGATED structural-query path (`!>>`, `!>`, `!~`), mirroring `QueryStructuralFromIndex`'s (SPEC-ROOT-020) shape, decline conventions, and panic-safety wrapper exactly — `modules_executor.ExecuteNegatedStructuralFromIndex` (SPEC-STRUCT-10) does the actual work. Before this function existed, D6's engine was reachable only from tests inside `internal/modules/executor` (Go's `internal/` visibility rule made it a permanent dead end for any external caller), despite SPEC-ROOT-020's own text describing routing to it as though a root entry point already existed.

**Signature and parameter contract:**
```go
func QueryNegatedStructuralFromIndex(
    ctx context.Context,
    traceqlQuery string,
    rightSource ValueIndexSource,
    traceGroupStore LookupStore,
    tenant, indexPrefix string,
    readerFor StructuralReaderProvider,
    minTS, maxTS uint64,
    indexOnly bool,
    opts QueryOptions,
) (results []SpanMatch, ok bool, err error)
```
- `rightSource` is REQUIRED (never nil): unlike SPEC-ROOT-020's `leftSource`/`rightSource` split, D6 has no optional side — `rightSource` is the ONLY VI-resolved operand (NOTE-VI-093); a nil `rightSource` is a routine decline (`ok=false, err=nil`), mirroring `QueryStructuralFromIndex`'s own `leftSource == nil` decline.
- There is no `vcntData`/`vcntDir`/selectivity-classifier parameter: D6 has no cost-based intersection-prefilter equivalent to D4's step 4 (NOTE-VI-093's own cost-model note — D6's per-candidate cost is proportional to trace size, not to VI selectivity).
- `traceGroupStore`/`tenant`/`indexPrefix`/`readerFor`/`minTS`/`maxTS`/`indexOnly`/`opts` carry the identical contract SPEC-ROOT-020 documents for `QueryStructuralFromIndex` — not repeated here.

**Scope (v1): 2-node NEGATED structural queries only.** A chain flattening to other than exactly 2 filter nodes, or a positive operator (routed to `QueryStructuralFromIndex`, SPEC-ROOT-020, instead), is a routine decline (`ok=false, err=nil`).

**Result conversion and panic safety** are identical to `QueryStructuralFromIndex`'s (SPEC-ROOT-020) — same `NewSpanFieldsAdapterWithReader`/`.Clone()`/`ReleaseSpanFieldsAdapter` sequence, same top-level `recover()`-to-error wrapper.

Back-ref: `structural.go:QueryNegatedStructuralFromIndex`. Tests: `structural_index_realvi_test.go:TestQueryNegatedStructuralFromIndex_RealWriteValueIndexL0_EndToEnd`. See NOTE-VI-095 (`internal/modules/executor/NOTES.md`) for the fix-pass context. Issue #489.
