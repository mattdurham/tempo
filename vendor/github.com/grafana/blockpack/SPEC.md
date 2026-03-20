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
