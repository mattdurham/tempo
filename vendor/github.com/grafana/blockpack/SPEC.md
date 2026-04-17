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

## SPEC-ROOT-016: Maximum file size — 20 GB per blockpack file

**Invariant:** A single blockpack file (the unit produced by one writer.Flush cycle)
can be up to 20 GB on disk. Implementations that size buffers, caches, or working
memory per-file MUST tolerate this upper bound without unbounded growth.

**Consequences:**
- Span count per file: up to ~100M (at ~200 B/span compressed, typical mix of
  attributes). Extreme high-cardinality workloads may reach ~200M spans per file
  at ~100 B/span.
- Working memory per file for set-like structures (bitmaps, row-masks, offset
  arrays) must be sized against this span count, not against a small "typical"
  file assumption.
- Pool caps and allocation budgets that silently bypass the pool for large files
  create hidden GC pressure at the 20 GB boundary. Pool cap guards MUST either
  (a) accommodate files at this size, or (b) explicitly document the fallback
  path (fresh per-call alloc) as acceptable for the large-file edge case.

**Where this binds:**
- `internal/modules/executor/`: intersectBitmapPool cap (NOTE-074) — sized for
  ≤134M spans (16 MB bitmap).
- `internal/modules/blockio/reader/`: block index, range index, sketch caches
  — must not precompute O(spans) structures without a bounded working set.
- `internal/modules/blockio/writer/`: block builder — MaxBlockSpans enforces
  per-block upper bound, but total spans per file are not similarly capped.

**How to apply:** When introducing any new per-file data structure, document
its size against the 100M-span / 20 GB upper bound. If it exceeds a reasonable
working-set budget (~50 MB per file per query), propose a tier (pool vs direct
alloc vs compressed representation) rather than a single fixed allocation.
