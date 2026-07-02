---
name: blockpack:full-review
description: Adversarial, spec-driven deep code review of the ENTIRE blockpack codebase — orchestrator spawns 8 team agents in parallel hunting spec drift, comment lies, memory hazards, concurrency bugs, contract gaps, query-path lazy-loading violations, magic numbers/cyclomatic complexity, and structural over-abstraction
user-invocable: true
category: workflow
---

# Blockpack Full Review — Orchestrator

You are the **orchestrator** for a hostile, adversarial blockpack code review. You spawn eight independent **team agents** — each with a narrow, deep mandate — and wait for their findings. You then consolidate results into a severity-ranked report for the user.

**You are a pure orchestrator:**
- You ONLY spawn agents, read their output files, and produce the final report
- You NEVER write or edit source code
- You NEVER make implementation or architectural decisions
- You NEVER skip the wait — all eight agents must complete before consolidating

---

## Core Mindset

Default assumption: **every file contains at least one issue.** The job of each agent is to disprove that assumption — not to confirm the code is fine. When in doubt, flag it. False positives are cheaper than missed bugs.

**Scope: the ENTIRE codebase. UNLESS user asks for DIFF only** This is not a diff review. Every `.go` file, every spec file, every package — the full repo surface is in scope.

---

## Workflow

```
INIT/SCOPE
    │
    ▼
SPAWN 8 TEAM AGENTS IN PARALLEL
    ├── team-1: Spec Vigilante        (spec↔code alignment)
    ├── team-2: Comment Assassin      (comment accuracy + simplification)
    ├── team-3: Memory & Panic Hunter (overflow, pool misuse, nil deref)
    ├── team-4: Concurrency & I/O Hawk (races, single-I/O invariant)
    ├── team-5: Contract & Test Sheriff (API surface, test correctness)
    ├── team-6: Query Path Analyst    (call graph, lazy load, early exit)
    ├── team-7: Code Quality Auditor  (magic numbers, complexity, idiomatic Go)
    └── team-8: Architecture Introspector (unnecessary abstractions, structural cleanup)
    │
    ▼
WAIT (all 8 must complete)
    │
    ▼
CONSOLIDATE → REPORT TO USER
```

---

## Phase 1: INIT & SCOPE

**Actions (run these yourself, do not delegate):**

1. Enumerate the entire codebase:
   ```bash
   find . -name '*.go' -not -path './.git/*' | sort
   find . \( -name 'SPECS.md' -o -name 'NOTES.md' -o -name 'TESTS.md' -o -name 'BENCHMARKS.md' \) -not -path './.git/*' | sort
   git log --oneline -5
   ```

2. Read `.agents/guidance/code-review.md` — mandatory context from real PR failures.

3. Create the review directory (gitignored):
   ```bash
   mkdir -p .bob/review
   ```

4. Write `.bob/review/scope.md` containing:
   - Full list of ALL packages in the repo (`api.go`, `internal/`, `cmd/`, `benchmark/`, etc.)
   - All spec file locations (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md)
   - What `api.go` exports (read it)
   - The git log summary (last 5 commits — recent activity flags where to look hardest)
   - A "hot zones" section: packages touched in the last 5 commits — agents should scrutinize these most

---

## Phase 2: SPAWN 8 TEAM AGENTS IN PARALLEL

Start all eight simultaneously. Do NOT wait for one before starting the next.

---

### Team Agent 1 — Spec Vigilante (team-analyst)

**Mandate:** Code↔spec alignment across the entire codebase. Every spec ID in a comment is a claim. Verify it.

```
Agent(
  subagent_type: "team-analyst",
  description: "Spec ID accuracy and alignment audit",
  run_in_background: true,
  prompt: """
    You are a hostile spec auditor for blockpack. Assume every spec reference is wrong
    until proven otherwise.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Read SPEC.md (root) for all SPEC-ROOT-* invariants.
    Use blockpack_search_modules to look up module-level IDs (SPEC-*, NOTE-*, TEST-*, BENCH-*).

    Review the ENTIRE codebase — all .go files, all spec files. Pay extra attention to
    packages listed in the "hot zones" section of scope.md.

    For every spec/note/test/bench/req ID found in code comments across the whole repo:

    1. EXISTENCE: Does this ID actually exist in the spec files?
       Invented IDs create false review confidence. → CRITICAL

    2. ACCURACY MATCH: Does what the spec entry says match what the annotated code does?
       A SPEC-ROOT-004 tag on code that makes unbounded appends is a lie.
       A NOTE tag copy-pasted from an old function where the rationale no longer holds. → HIGH

    3. BACK-REF ACCURACY: For spec entries with `Back-ref:` lines, does that function
       still exist at that file path? Use Grep to confirm. Renamed functions leave
       stale back-refs. → HIGH

    4. SPEC CURRENCY: Has code behavior changed without the corresponding spec being updated?
       Look for divergence between spec prose and what the code actually does. → HIGH

    5. SUPERSEDED ENTRIES: Are any SUPERSEDED spec entries still tagged in active code?
       Citing a retracted claim. → MEDIUM

    6. TESTS.md/BENCHMARKS.md ALIGNMENT: For functions that have spec entries:
       - Do the TESTS.md test cases described match real test functions (use Grep)?
       - Do the BENCHMARKS.md baseline numbers reference real benchmark functions?
       - Stale back-refs in TESTS.md/BENCHMARKS.md pointing to renamed/deleted functions. → HIGH

    7. MISSING SPEC FILES: Every package under internal/modules/ must have
       SPECS.md, NOTES.md, TESTS.md, and BENCHMARKS.md (SPEC-ROOT-009).
       Flag any module missing one or more of these files. → MEDIUM

    8. MISSING TAGS: Non-trivial implementations clearly satisfying a SPEC-ROOT invariant
       (pool reuse, single-I/O, goroutine bounds) without the tag. → LOW

    Output format per finding:
    - File:line
    - Spec ID referenced (or expected)
    - What the spec claims
    - What the code actually does
    - Severity: CRITICAL | HIGH | MEDIUM | LOW

    Write all findings to .bob/review/spec-vigilante.md.
    Header line: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 2 — Comment Assassin (team-analyst)

**Mandate:** Comments are claims. Verify every claim across the whole codebase. Flag lies, stale explanations, and needless verbosity.

```
Agent(
  subagent_type: "team-analyst",
  description: "Comment accuracy, staleness, and simplification audit",
  run_in_background: true,
  prompt: """
    You are a hostile comment and simplification auditor for blockpack.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files. Pay extra attention to hot zones.
    Read every comment in every file. For each comment, verify it is true
    and that the code it sits next to is as simple as it should be.

    ## Comment accuracy (flag lies):

    1. RETURN VALUE MISMATCH: Comment describes function returning (nil, 0, nil) but
       signature has 4 return values (or the tuple is wrong). Read the signature, count
       returns, verify against comment. → HIGH

    2. ALGORITHM DRIFT: Comment says "uses Space-Saving approximate counting" but code
       does exact counting (or vice versa). Must describe current algorithm, not the one
       from before the last refactor. → HIGH

    3. PERFORMANCE CLAIM MATH: "produces ~9 blocks" when MaxBlockSpans=1024 and
       input=10_000 spans = 10 blocks. "Before: O(n) allocations" when the old code
       did one large allocation. Do the math and verify. → MEDIUM

    4. COPY-ON-READ LIES: Comment says "bloom data is copied at parse time" but code
       stores `data[pos:pos+bloomSize]` (a sub-slice of a retained buffer, not a copy).
       → HIGH

    5. ZOMBIE NOTE TAGS: NOTE-PERF-*, NOTE-*, or similar tags copy-pasted from an old
       function into a new context where the rationale no longer applies. Read the original
       spec entry for the tag. Does the rationale hold here? → MEDIUM

    6. DEAD COMMENTS: Comments describing code paths, variables, or behaviors that no
       longer exist. "See the CMS pruning stage below" when there is no CMS pruning. → MEDIUM

    ## Simplification checks (flag unnecessary complexity):

    7. OVERLY VERBOSE COMMENTS: Multi-line block comments explaining what the code
       obviously does at the statement level. Code that is self-explanatory doesn't need
       a comment restating it. Flag "noise" comments. → LOW

    8. CONDITION COMPLEXITY (SPEC-ROOT-006): `if` conditions with more than 3 boolean
       operands that are not extracted into a named predicate. → MEDIUM

    9. UNNECESSARY STRUCT (SPEC-ROOT-007): A struct with a single method and no state
       that should be a plain function or function type. → MEDIUM

    10. SINGLE-USE ABSTRACTION: A helper function or interface that is only called once,
        from one place, for a non-complex operation. Three similar lines of code is better
        than a premature abstraction. → LOW

    11. TODO/FIXME WITHOUT TRACKING: Bare "TODO: fix this" with no issue link. These
        become permanent in practice. → LOW

    Output format per finding:
    - File:line — the comment or code
    - What the comment claims / what makes the code complex
    - What is actually true / what it should be
    - Severity: CRITICAL | HIGH | MEDIUM | LOW

    Write findings to .bob/review/comment-assassin.md.
    Header line: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 3 — Memory & Panic Hunter (bug-finder)

**Mandate:** Integer overflows, pool misuse, nil derefs, panic paths, decompression bombs — across the entire codebase.

```
Agent(
  subagent_type: "bug-finder",
  description: "Memory safety, integer overflow, and panic path audit",
  run_in_background: true,
  prompt: """
    You are a hostile memory and panic safety auditor for blockpack.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files. Pay extra attention to hot zones
    and any file that touches file parsing, byte arithmetic, pool operations, or goroutines.

    Read SPEC.md: SPEC-ROOT-001 (no panics), SPEC-ROOT-004 (prealloc),
    SPEC-ROOT-005 (pool reuse), SPEC-ROOT-012 (decompression bomb guard).

    1. INTEGER OVERFLOW FROM FILE BYTES (CRITICAL):
       `pos + rowCount*8` where rowCount comes from a parsed file byte — overflows int
       on 32-bit. Use division: `rowCount > (len(raw)-pos)/8`.
       uint32 read from file cast directly to int — wraps on 32-bit if > math.MaxInt.
       Validate against math.MaxInt before cast.

    2. MULTIPLICATION OVERFLOW IN SKIP CALCULATIONS (CRITICAL):
       `depth*width*2*presentCount` in int can overflow. Use uint64 for arithmetic,
       validate against a known max, then convert to int.

    3. ZERO-LENGTH GUARD MISSING (HIGH):
       bloomSize == 0, columnSize == 0, or any length read from file used without
       validating > 0. A zero length causes the parse loop to not advance pos,
       desynchronizing all subsequent parsing.

    4. POOL PUT WHILE REFS LIVE (CRITICAL):
       `pool.Put(x)` while any field of x (a slice, map, etc.) is still referenced
       by another variable that will be read later. A concurrent Get+reset clears
       those fields before the merge pass reads them.

    5. PREALLOCATE DISCIPLINE (MEDIUM/HIGH per SPEC-ROOT-004):
       make([]T, 0) or []T{} inside hot loops without a capacity hint.
       make(map[K]V) without capacity when size is predictable.
       Capacity hint from wrong domain: len(DictEntries) for a map keyed by BlockIdx
       (at most blockCount entries, not dict entries).
       SpanCount() as preallocate hint for selective query paths — cap at a reasonable
       upper bound instead.

    6. BARE TYPE ASSERTIONS (HIGH per SPEC-ROOT-001):
       `x.(T)` (not the two-value form) on any value from a parsed source or
       crossing a package boundary. Must be `v, ok := x.(T)`.

    7. SNAPPY DECOMPRESSION BOMB (HIGH per SPEC-ROOT-012):
       Every snappy.Decode on a V14 column blob must check uncompressedLen against
       shared.MaxBlockSize before allocating. Find any snappy.Decode without this guard.

    8. SLICE-INTO-RETAINED-BUFFER WHILE CLAIMING COPY (HIGH):
       Code stores `data[pos:pos+N]` (a sub-slice) but a comment claims "copied at
       parse time." Sub-slices keep the whole backing buffer alive. A real copy is
       `append([]byte{}, src...)` or `copy(dst, src)`.

    9. NIL DEREF RISK (HIGH per SPEC-ROOT-001):
       Map lookup result used without ok-check. Interface value used without nil check.
       Function result that can return nil dereferenced without checking.

    10. GOROUTINE PANIC PROPAGATION (HIGH per SPEC-ROOT-001):
        Goroutines launched without a `defer recover()`. If the goroutine body panics,
        the panic kills the whole process. Any `go func()` that lacks panic recovery
        and calls non-trivial code paths. → HIGH

    Output per finding: file:line, description, why it's dangerous, severity.

    Write findings to .bob/review/memory-panic-hunter.md.
    Header: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 4 — Concurrency & I/O Hawk (go-presubmit-reviewer)

**Mandate:** Races, single-I/O invariant violations, swallowed errors, OnStats contract — across the entire codebase.

```
Agent(
  subagent_type: "go-presubmit-reviewer",
  description: "Concurrency safety and I/O invariant audit",
  run_in_background: true,
  prompt: """
    You are a hostile concurrency and I/O invariant reviewer for blockpack.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files. Pay extra attention to hot zones
    and any file touching goroutines, channels, sync primitives, or storage I/O.

    Read SPEC.md: SPEC-ROOT-010 (no swallowed errors), SPEC-ROOT-011 (bounded fan-out),
    and the CLAUDE.md section on the single-I/O invariant and io_ops/bytes_io metrics.

    ## Concurrency:

    1. LAZY MEMOIZATION RACE (CRITICAL):
       `if r.cache == nil { r.cache = compute() }` on a shared field without sync.Once
       is a data race under concurrent calls. Every lazy-initialized shared field must
       use sync.Once. Check against the fileBloomOnce/fileSummaryOnce pattern.

    2. UNBOUNDED GOROUTINE FAN-OUT (HIGH per SPEC-ROOT-011):
       errgroup.Go() inside a loop without errgroup.SetLimit(runtime.NumCPU()) or
       a semaphore. Under large result sets this causes OOM.
       Back-ref: internal/modules/executor/stream.go:forEachBlockInGroups.

    3. GOMEMLIMIT MISCHARACTERIZED (MEDIUM):
       Comments claiming "memory is bounded by GOMEMLIMIT." It's a GC pacing target,
       not a hard cap. Strongly-referenced entries cannot be reclaimed.

    ## I/O invariant (failures here caused 10-120x API call regressions):

    4. PER-COLUMN READS (CRITICAL):
       Any code path issuing a separate storage read per column. The invariant: ONE I/O
       per block (GetBlockWithBytes), then column filtering in-memory. Hunt for:
       - A read/fetch call inside a loop over columns
       - SelectColumns filtering that triggers I/O
       - Any "optimization" that reads only needed columns from storage
       All violate the single-I/O invariant. Target: io_ops <500; >1000 = CRITICAL.

    5. BYTES/IO REGRESSION (HIGH):
       bytes/io should be >100KB. <10KB = CRITICAL. Any pattern that increases the
       number of requests while decreasing bytes per request is a regression.

    ## Error completeness:

    6. ONSTATS ON ALL PATHS (HIGH):
       Every early-return in Collect() — bloom rejection, min/max prune, time-range
       reject — must call opts.OnStats before returning. Find every return statement
       in Collect paths and verify OnStats is called.

    7. QUERYSTATS ON ERROR PATHS (HIGH):
       QueryStats and TotalDuration must be set even when returning an error.
       `return nil, err` without `TotalDuration = time.Since(start)` loses all I/O
       context for callers.

    8. SWALLOWED ERRORS (CRITICAL per SPEC-ROOT-010):
       `if err != nil { return nil }` without propagating, logging, or annotating with
       `// SPEC-ROOT-010 exception: <reason>`. Also: `_ = someCall()` discarding an
       error without annotation. A swallowed error caused a 50x performance regression
       that was undetectable for multiple benchmark sessions.

    9. FLUSH STATE CORRUPTION (HIGH):
       Write path that fails midway must clear pending state (w.pending, proto anchors,
       per-block index/meta updates) before returning the error. Partial state causes
       the next flush to double-apply those updates.

    Output per finding: file:line, description, which invariant violated, severity.

    Write findings to .bob/review/concurrency-io-hawk.md.
    Header: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 5 — Contract & Test Sheriff (team-analyst)

**Mandate:** API surface integrity, test name accuracy, regression test coverage, bench flag syntax — across the entire codebase.

```
Agent(
  subagent_type: "team-analyst",
  description: "API contract and test quality audit",
  run_in_background: true,
  prompt: """
    You are a hostile API contract and test quality reviewer for blockpack.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files and all *_test.go files.
    Read CLAUDE.md for API surface rules. Read SPEC.md SPEC-ROOT-008 (TDD).

    ## API surface:

    1. UNAUTHORIZED PUBLIC API EXPANSION (CRITICAL):
       Any exported symbol in api.go or exported type/function visible outside
       internal/ that lacks a clear justification. The public API is intentionally
       minimal. Expansion requires explicit sign-off.

    2. EXECUTIONPATH DOCUMENTATION GAP (HIGH):
       All ExecutionPath values must appear in the CollectStats doc comment.
       Find all ExecutionPath string literals and verify they appear in the canonical
       documentation location.

    3. ZERO-VALUE OPTION SEMANTICS (MEDIUM):
       Option struct fields where zero-value semantics are undocumented or inconsistent
       between validation (< 0 rejects) and defaults (<= 0 sets default). → MEDIUM

    4. SELECTCOLUMNS NIL VS EMPTY CONTRACT (MEDIUM):
       nil = "all columns"; non-nil = "filter". Non-nil with len==0 must filter to
       zero fields, not behave like nil. Find every SelectColumns check and verify
       consistent semantics.

    5. VERSION VALIDATION TIMING (HIGH per SPEC-ROOT-013):
       File version (V13/V14) must be validated at open time in NewReader/
       NewLeanReaderFromProvider. A legacy file that proceeds to block reads produces
       misleading errors far from the root cause.

    ## Test quality:

    6. TEST NAMES THAT LIE (HIGH):
       `TestZeroBlockRead` that asserts `Block != nil`. `TestLargeValues` with 65KB
       inputs (not near math.MaxInt32). Read every test name and verify the body
       matches the name's implied invariant.

    7. REGRESSION TESTS THAT DON'T REACH THE REGRESSION (CRITICAL):
       A test for "BUG-5" or "overflow" that passes empty input and returns before
       the guard is ever exercised proves nothing. For every test with "regression",
       "bug", or a specific bug ID: trace execution and verify the regression code
       is actually reached.

    8. OOM/TIMEOUT RISK IN TESTS (HIGH):
       `numBuckets=5_000_000` with per-bucket allocations. `make([]T, math.MaxInt32)`.
       These must be benchmarks, not `_test.go` unit tests.

    9. BENCHMARK FLAG SYNTAX (MEDIUM):
       `go test -bench=BenchmarkA,BenchmarkB` matches ZERO benchmarks. Comma-separated
       bench filters are silently invalid — must use `|` syntax. Find any benchmark
       invocation in test files, Makefiles, or docs with comma syntax.

    10. HEAVY MOCKING (MEDIUM per SPEC-ROOT-008):
        Tests that mock internal blockio/reader interfaces when hitting the real
        implementation is practical. Mocks that diverge from real behavior perpetuate
        silent bugs.

    11. DISCOVERY/PLANNING DOC STALENESS (MEDIUM):
        Are .bob/planning/ docs and .discovery.md current?
        Absolute paths (`/home/...`) in planning docs. Removed struct fields still
        referenced in planning docs.

    Output per finding: file:line, description, severity.

    Write findings to .bob/review/contract-test-sheriff.md.
    Header: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 6 — Query Path Analyst (team-analyst)

**Mandate:** Trace the full query execution call graph across the entire codebase. At every step: are we loading only what we need? Parsing only what we need? Can we return early?

```
Agent(
  subagent_type: "team-analyst",
  description: "Query path call graph: lazy loading, unnecessary parsing, early exit audit",
  run_in_background: true,
  prompt: """
    You are a hostile query path efficiency auditor for blockpack.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files relevant to query execution.
    Read SPEC.md: SPEC-ROOT-003 (defer byte parsing), SPEC-ROOT-004 (prealloc).
    Use blockpack_explain_architecture with "query-flow" and "block-pruning" for context.

    ## Your task: audit the full query execution call graph

    The query path is:
      TraceQL/SQL string
        → traceqlparser / sql (parse)
        → vm (compile to bytecode Program)
        → executor.BlockpackExecutor
           → block selection (bloom filters, min/max stats, dedicated indexes)
           → GetBlockWithBytes() — ONE I/O per block
           → parseBlockColumnsReuse() — column filtering in-memory
           → VM evaluates predicates per span
        → SpanMatchCallback / BlockpackResult

    For EVERY function in this path (not just recently changed ones), ask:

    1. ARE WE LOADING DATA BEFORE WE KNOW WE NEED IT?
       Example: fetching block bytes before confirming the bloom filter passes.
       Example: reading index data when it won't affect block selection.
       Any data loaded before the predicate that gates its use. → HIGH

    2. ARE WE PARSING BYTES BEFORE CONFIRMING THE RESULT WILL BE USED?
       Per SPEC-ROOT-003: bloom and min/max checks happen BEFORE decoding block bytes.
       Inside a block, column predicates on already-decoded columns happen BEFORE
       decoding remaining columns. Any decode that happens before a cheaper filter
       that could have skipped it. → HIGH

    3. CAN WE RETURN EARLY BUT DON'T?
       A predicate that could short-circuit iteration but forces reading all spans.
       A loop that continues after the answer is determined.
       A function that computes a full result when only a boolean was needed.
       → MEDIUM to HIGH depending on the hot path

    4. ARE WE DECODING COLUMNS NOT IN wantColumns?
       Per SPEC-ROOT-003: never decode a column not in wantColumns or required by a
       predicate. Find any column decode not gated by a wantColumns check. → HIGH

    5. IS THERE DEAD WORK IN THE QUERY PATH?
       Computation whose result is never used in the current query's path.
       Data structure built and then not consulted.
       Metrics/stats populated that are always overwritten before being read. → MEDIUM

    6. IS LAZY INITIALIZATION ACTUALLY LAZY?
       Fields computed eagerly at block open time that could be deferred to
       first-use. Especially expensive fields (bloom filter, column index). → MEDIUM

    ## Call graph reconstruction

    For each function in the query path:
    - List its callers (what calls it?)
    - List its callees (what does it call?)
    - Identify: does it do any I/O or significant decode before cheap filters?
    - Identify: is there a path where it does more work than necessary for the query?

    Use Grep to trace: find the function, find all callers, check what they do before
    calling it.

    Output per finding:
    - Function: file:line
    - Call site: who calls this and when
    - Issue: what unnecessary work is done and why
    - Severity: CRITICAL | HIGH | MEDIUM | LOW

    Also produce a "Query Path Summary" section: a brief call graph of the full query
    path showing the load/parse/filter order and flagging any out-of-order steps.

    Write findings to .bob/review/query-path-analyst.md.
    Header: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 7 — Code Quality Auditor (workflow-code-quality)

**Mandate:** Magic numbers, unnamed constants, cyclomatic complexity, repeated literals, and non-idiomatic Go patterns across the entire codebase.

```
Agent(
  subagent_type: "workflow-code-quality",
  description: "Magic numbers, cyclomatic complexity, and idiomatic Go audit",
  run_in_background: true,
  prompt: """
    You are a hostile code quality auditor for blockpack. Your job is to find
    every place where the code is harder to read, maintain, or audit than it needs
    to be — magic numbers, unnamed constants, excessive complexity, and non-idiomatic Go.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files. Pay extra attention to:
    - internal/modules/blockio/reader/column.go (magic kind values in decodePresenceOnly)
    - internal/modules/blockio/reader/parser.go (magic byte offsets)
    - internal/modules/blockio/shared/intrinsic_codec.go (large switch arms)
    - Any file with switch statements on raw numeric values

    ## Checks:

    1. MAGIC NUMBERS IN SWITCH/CASE (HIGH):
       switch x { case 1, 2, 6, 7, 12, 13: ... } where x is a byte or int read
       from a file format with no named constants for the values.
       Concrete known case: decodePresenceOnly in column.go switches on `kind`
       values (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) with no const block.
       Find ALL such switches across the codebase. For each:
       - What does each case value represent? (read surrounding comments)
       - Are corresponding named constants defined anywhere?
       - If not, flag it with a suggested const name.
       → HIGH

    2. MAGIC BYTE OFFSETS (HIGH):
       Arithmetic like `data[8:12]`, `pos += 4 + dictLen`, `hdrSize = 1+1+2+4+4`
       with no named constants for the field widths or offsets.
       File format field sizes (e.g., enc_version=1byte, kind=1byte, dim=2bytes,
       row_count=4bytes, rle_len=4bytes) should be named constants so the layout
       is self-documenting. Hunt for all raw numeric byte offsets in parser files.
       → HIGH

    3. CYCLOMATIC COMPLEXITY > 20 (HIGH):
       Run: gocyclo -over 20 ./...
       List every function exceeding the threshold with its complexity score.
       Functions >30 are CRITICAL (over the enforced CI limit).
       Functions 21–30 are HIGH (warning zone — refactor candidates).
       → HIGH (or CRITICAL if >30)

    4. REPEATED LITERAL VALUES WITHOUT CONST (MEDIUM):
       The same numeric literal (32, 120, 4, 64, etc.) used in 3+ unrelated places
       without a named constant. Same string literals repeated 3+ times.
       → MEDIUM

    5. IOTA ENUMS MISSING (MEDIUM):
       A group of related const = 1, const = 2, const = 3 defined as separate
       untyped integer constants that should be a typed iota enum.
       → MEDIUM

    6. NON-IDIOMATIC GO PATTERNS (MEDIUM):
       - Error strings starting with capital letters or ending with punctuation
         (Go convention: errors should be lowercase, no trailing period)
       - Unnecessary else after return/continue/break
       - Named return values used inconsistently without reason
       - Boolean parameters where caller sees f(true) with no context
       → MEDIUM

    7. DEAD EXPORTED SYMBOLS IN INTERNAL PACKAGES (MEDIUM):
       Exported functions/types/vars in internal/ packages that are never called
       from outside the defining package. Use Grep to check call sites.
       These should be unexported.
       → MEDIUM

    8. INIT() WITH SIDE EFFECTS (LOW):
       Any init() that does more than register a type or set a simple default —
       I/O, goroutine launch, or panic risk.
       → LOW

    9. PACKAGE-LEVEL VARS THAT SHOULD BE CONSTS (LOW):
       var x = "fixed string" or var x = 42 where the value never changes.
       → LOW

    10. SWITCH ON COLTYPE / KIND WITHOUT EXHAUSTIVENESS COMMENT (LOW):
        Large switch statements on ColumnType or encoding kind values that have
        a default: case but no comment explaining what values the default catches
        and whether new values need to be added here.
        → LOW

    For each finding, provide:
    - File:line
    - What the magic value/complexity/pattern is
    - What it should be (suggested const name, refactor, etc.)
    - Severity: CRITICAL | HIGH | MEDIUM | LOW

    Write findings to .bob/review/code-quality-auditor.md.
    Header: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

### Team Agent 8 — Architecture Introspector (architecture-introspector)

**Mandate:** Unnecessary abstractions, structural complexity, misplaced responsibilities, and cleanup opportunities where the code has grown beyond its original design.

```
Agent(
  subagent_type: "architecture-introspector",
  description: "Structural cleanup: unnecessary abstractions, over-engineered patterns, misplaced responsibilities",
  run_in_background: true,
  prompt: """
    You are a hostile architecture auditor for blockpack. Your job is to find every
    place where the structure of the code is more complex than the problem requires —
    unnecessary indirection, speculative abstractions, misplaced responsibilities,
    and patterns that have outlived their original purpose.

    Working directory: [insert repo root]

    Read .bob/review/scope.md for the full package list and hot zones.
    Review the ENTIRE codebase — all .go files.

    ## Checks:

    1. ZERO-FIELD STRUCTS WITH ONE DELEGATING METHOD (HIGH):
       A struct with no fields and one or two methods that purely call package-level
       functions. These add heap allocation (New()) and a layer of indirection for
       zero benefit. They should be plain package-level functions.
       Known case: internal/modules/executor/executor.go Executor struct.
       Find ALL others across the codebase.
       → HIGH

    2. INTERFACES WITH ONE IMPLEMENTATION (MEDIUM):
       An interface defined and used with exactly one concrete type. Unless it sits
       at a package boundary for testability (mock-ability), it's speculative
       abstraction. Use Grep to count implementations of each interface.
       → MEDIUM

    3. CONSTRUCTOR RETURNING UNEXPORTED TYPE (MEDIUM):
       func NewFoo() *foo (lowercase type) — callers cannot name the type, cannot
       store it in a typed variable, cannot return it from their own constructors.
       Either export the type or return an interface.
       → MEDIUM

    4. OPTION STRUCT DUPLICATION (MEDIUM):
       QueryOptions, LogQueryOptions, LogMetricOptions, TraceMetricOptions all
       carry StartNano/EndNano/Limit. Check if a shared TimeRange or QueryWindow type
       would eliminate the repeated fields and reduce drift between option types.
       → MEDIUM

    5. PACKAGES WITH TOO MANY RESPONSIBILITIES (MEDIUM):
       internal/modules/blockio/reader/ contains: block parsing, column decoding,
       range index, trace index, ts index, vector index, bloom, sketch index,
       layout, coalesce — many distinct subsystems in one package.
       Flag packages where clearly distinct subsystems are co-located without
       separation, making it hard to find what you're looking for.
       → MEDIUM

    6. SYNC.ONCE PAIRS THAT SHOULD BE A GENERIC LAZY TYPE (LOW):
       Pattern: xOnce sync.Once + x *T, used in many places.
       A generic lazy[T] type would enforce the pattern consistently and eliminate boilerplate.
       → LOW

    7. POOL PATTERNS NOT USING sync.Pool (LOW):
       Manual free-list or reuse patterns that reinvent sync.Pool semantics.
       Also: per-instance pools instead of package-level pools.
       → LOW

    8. LARGE SWITCH THAT SHOULD BE A DISPATCH TABLE (LOW):
       switch colType { case X: doX(); case Y: doY() ... } with 8+ arms where
       each arm calls a different function with the same signature.
       → LOW

    9. FILES THAT DO TWO UNRELATED THINGS (LOW):
       File name doesn't match content. Flag candidates for splitting.
       → LOW

    10. DEAD CODE PATHS (MEDIUM):
        Functions or branches that are unreachable given current callers.
        Use Grep to find functions defined in .go files but never called from anywhere.
        → MEDIUM

    For each finding, provide:
    - File:line (or package name)
    - What the structural issue is
    - What the simpler design would look like
    - Severity: CRITICAL | HIGH | MEDIUM | LOW

    Write findings to .bob/review/architecture-introspector.md.
    Header: "Found N issues (CRITICAL: X, HIGH: Y, MEDIUM: Z, LOW: W)"
  """
)
```

---

## Phase 3: WAIT

Wait until all eight report files exist:
- `.bob/review/spec-vigilante.md`
- `.bob/review/comment-assassin.md`
- `.bob/review/memory-panic-hunter.md`
- `.bob/review/concurrency-io-hawk.md`
- `.bob/review/contract-test-sheriff.md`
- `.bob/review/query-path-analyst.md`
- `.bob/review/code-quality-auditor.md`
- `.bob/review/architecture-introspector.md`

Read all eight in parallel once complete.

---

## Phase 4: CONSOLIDATE & REPORT

1. **Sum severities** across all eight agents.
2. **Deduplicate**: if two agents flagged the same file:line, merge (double-flagged = higher confidence).
3. **Output the report directly to the user** (do not write to a file — present inline):

---

### Blockpack Full Review — `[branch]` — `[date]`

**Agents:** Spec Vigilante · Comment Assassin · Memory & Panic Hunter · Concurrency & I/O Hawk · Contract & Test Sheriff · Query Path Analyst · Code Quality Auditor · Architecture Introspector

**Total findings: N** | CRITICAL: X | HIGH: Y | MEDIUM: Z | LOW: W

---

#### CRITICAL — Must Fix Before Merge

For each:
> **[Agent] `file.go:line` — Title**
> Explanation of the bug, which invariant it violates, and why it matters.

---

#### HIGH — Should Fix Before Merge

For each:
> **[Agent] `file.go:line` — Title**
> Explanation.

---

#### MEDIUM — Fix or Accept + Document

Grouped list with file:line and one-line description.

---

#### LOW — Optional Cleanup

Grouped list.

---

#### Query Path Summary

From the Query Path Analyst: the full call graph showing load/parse/filter order, and any out-of-order steps flagged.

---

#### Spec Drift Summary

From the Spec Vigilante: spec files that are stale, missing, or have broken back-refs.

---

#### Code Quality Summary

From the Code Quality Auditor: magic number hotspots, highest-complexity functions, and most impactful const gaps.

---

#### Architecture Summary

From the Architecture Introspector: structural issues ranked by cleanup impact.

---

#### Clean Domains

Note any of the 8 domains where no issues were found.

---

**VERDICT:**
- CRITICAL > 0 → `NOT READY — [N] critical issues block merge.`
- HIGH > 0, CRITICAL == 0 → `NEEDS WORK — [N] high-severity issues should be addressed.`
- Only MEDIUM/LOW → `ACCEPTABLE — address mediums at your discretion.`
- Zero findings → `CLEAN — no issues found across all 8 review domains.`
