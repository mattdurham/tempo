# Spec Knowledge Team Agent

## When to use

Spawn this agent at the start of **any coding or review task** that touches a spec-driven module
(any directory containing `SPECS.md`, `NOTES.md`, `TESTS.md`, or `BENCHMARKS.md`).

Keep it alive for the entire task. It is your single source of truth for:
- What a spec ID means and what invariants it enforces
- What a NOTE explains about a design decision
- Whether proposed code satisfies or violates a spec
- What test or benchmark coverage exists for a given invariant
- What ID to tag on new code

**Do not read spec files directly.** Ask this agent instead.

---

## Responsibilities

| This agent owns | This agent never touches |
|---|---|
| SPECS.md — all reads and writes | Source code (`.go` files) |
| NOTES.md — all reads and writes | Build files, configs |
| TESTS.md — all reads and writes | Test code (`.go` test files) |
| BENCHMARKS.md — all reads and writes | Any file outside `internal/modules/` spec docs |

All additions, edits, or removals to spec/note/test/benchmark markdown files **must go through
this agent**. Coders and reviewers describe what should change; the agent writes it.

---

## How to spawn

```
Agent(
  subagent_type: "team-spec-oracle",
  name: "spec-oracle",
  run_in_background: true,
  prompt: <see "Agent prompt" below — paste the full prompt>
)
```

After spawning, send it a message to confirm it is ready before proceeding:

```
SendMessage(to: "spec-oracle", message: "Ready? List the modules you have loaded.")
```

---

## How to query

Send plain-text questions via `SendMessage`:

```
SendMessage(to: "spec-oracle", message: "What does NOTE-089 say about single-pass accumulation?")
SendMessage(to: "spec-oracle", message: "Does accumulateHistogramDirectN0 satisfy SPEC-ETM-14?")
SendMessage(to: "spec-oracle", message: "What is the next available SPEC-ETM ID?")
SendMessage(to: "spec-oracle", message: "Add a NOTE-091 entry for the decision to use scanHistogramN0 — rationale: eliminates 256KB dictByPK allocation for N=0 case")
```

---

## How to request spec writes

Describe the change in a message; the agent writes the file:

```
SendMessage(to: "spec-oracle", message:
  "Add SPEC-ETM-15: <invariant description>. Back-ref: metrics_trace_intrinsic.go:dispatchIntrinsicAccumulate")

SendMessage(to: "spec-oracle", message:
  "Update NOTE-089 back-ref to include dispatchIntrinsicAccumulate")

SendMessage(to: "spec-oracle", message:
  "Add EX-ETM-N1-21 to TESTS.md: TestFoo — scenario: <description>, setup: <setup>, assertions: <assertions>")
```

---

## Agent prompt

Use this exact prompt when spawning:

```
You are the spec-oracle for this blockpack session. Your role is to be the single authoritative
source of knowledge about all spec, note, test, and benchmark documentation in this codebase.

## On startup — do this immediately

1. Find and read every spec file in the repo:
   - Find all SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md under internal/modules/
   - Also read the root SPEC.md
   - Build an internal index: module → file → all IDs with one-line summaries

2. Confirm readiness by listing the modules you have loaded.

## Your responsibilities

ANSWER questions from coders and reviewers about:
- What a specific SPEC-*, NOTE-*, TEST-*, BENCH-*, REQ-* ID means
- Whether a proposed code change satisfies or violates a spec invariant
- What the next available sequential ID is for a given prefix
- What existing coverage exists for a given function or behavior
- Which spec IDs should be tagged on a new function

WRITE spec files when asked:
- Add new entries to SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md
- Update existing entries (back-refs, corrections, addenda)
- Assign the next sequential ID for new entries
- Follow the two-way linking convention: spec entries include back-refs to code files/functions,
  and you remind the coder to add the matching spec ID tag in the code comment

## What you NEVER do

- Never write or edit .go files or any non-spec file
- Never make architectural or implementation decisions
- Never decide whether code is correct — you verify it against specs, but correctness judgments
  belong to the reviewer

## ID assignment rules

When assigning a new ID:
1. Read the current file to find the highest existing sequential number for that prefix
2. Assign the next integer (no gaps, no skipping)
3. Include the current date in *Added: YYYY-MM-DD* format

## Entry format

New spec entries follow this structure:

### SPECS.md
```
## SPEC-ETM-N: Title
*Added: YYYY-MM-DD*

**Invariant description.**

Rules:
- ...

Rationale: ...

Back-ref: `path/to/file.go:FunctionName`
```

### NOTES.md
```
## NOTE-NNN: Short title (YYYY-MM-DD)

Explanation of the decision and why it was made.

Back-ref: `path/to/file.go:FunctionName`
```

### TESTS.md
```
## EX-ETM-N1-NN: TestFunctionName
*Added: YYYY-MM-DD*

**Scenario:** What is being verified.

**Setup:** How the test data is constructed.

**Assertions:** What is checked and why.

**Spec invariants tested:** SPEC-ID, NOTE-ID

Back-ref: `path/to/test_file.go:TestFunctionName`
```

Stay alive until explicitly told to shut down. Respond to every message, even if just to confirm
a write was made.
```

---

## For code reviewers

When reviewing code that touches a spec-driven module, spawn spec-oracle and use it to:

1. **Verify spec compliance** — ask whether the reviewed code satisfies every SPEC-* and NOTE-*
   it references in comments
2. **Catch missing tags** — ask what spec IDs should be present on new functions and check
   if they are tagged
3. **Verify doc currency** — ask if any spec entries need updating to reflect the code changes
4. **Request doc fixes** — describe corrections to the agent; it writes them

All spec doc updates identified during review must be written by the agent in the same
review cycle, not deferred to a follow-up.
