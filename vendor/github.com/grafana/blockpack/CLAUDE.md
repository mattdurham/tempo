# CLAUDE.md

This file provides guidance to AI agents working with this repository.
This content is mirrored in `CLAUDE.md` and `AGENTS.md`; keep them in sync.

---

## Project Context

Blockpack is a high-performance columnar storage format for OpenTelemetry trace data, designed for:
- 10-50x compression ratios
- Fast TraceQL/SQL query execution
- Tempo/Grafana integration
- Block-level pruning and multi-level indexing

The public API (`api.go`) is minimal and intentionally small — all implementation details belong in `internal/`. **Do not add new public API surface without explicit user permission.**

---

## This Is an Agentic Codebase

This codebase is designed for agentic workflows. Agents are expected to:
- Use the MCP server tools (see below) **before** reading or modifying any package
- Follow the SPECS workflow (see below) when building or extending modules
- Consult spec files via the MCP server rather than reading them directly

### Bob Workflow (Recommended)

[github.com/mattdurham/bob](https://github.com/mattdurham/bob) is the recommended agentic workflow tool for structured, multi-step work in this repo. It provides planning, worktree isolation, brainstorming, and execution phases tuned for this codebase. Using bob is **recommended but not required** — agents may use their own workflow tooling.

---

## SPECS Workflow

Each major component under `internal/modules/<name>/` has a set of living spec files:

| File | Purpose |
|---|---|
| `SPECS.md` | Public contracts, input/output semantics, invariants. The **"what"**. This is the source of truth for correct behavior — when code conflicts with SPECS.md, SPECS.md wins. |
| `NOTES.md` | Design decisions, rationale, deliberate choices. The **"why"**. Each entry is dated and explains the reasoning behind non-obvious decisions. Critical reading before modifying a package. |
| `TESTS.md` | Test plan, coverage goals, test cases. Describes what is tested and why. |
| `BENCHMARKS.md` | Performance baselines, benchmark cases, I/O metrics. Baseline numbers serve as regression thresholds. |

### Go File Invariant

By convention, `.go` files within a module should carry a `// NOTE` comment near the top declaring the module's key invariant. When present, read it before editing any file in the module.

### Spec ID Tagging (Two-Way Linking)

Spec files (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md) use structured IDs to identify entries:

| Prefix | Source file | Example |
|---|---|---|
| `SPEC-` | SPECS.md | `SPEC-007` |
| `NOTE-` | NOTES.md | `NOTE-003` |
| `TEST-` | TESTS.md | `TEST-012` |
| `BENCH-` | BENCHMARKS.md | `BENCH-002` |
| `REQ-` | REQUIREMENTS.md | `REQ-008` |

**When writing or modifying code that implements, satisfies, or is constrained by a spec entry, tag the code with the relevant ID in a comment:**

```go
// SPEC-007: single I/O per block — never issue per-column reads
func (r *Reader) GetBlockWithBytes(ctx context.Context, id ulid.ULID) ([]byte, error) {
```

```go
// NOTE-003: bloom filter must be checked before fetching block bytes
// REQ-008: block selection must use min/max pruning
if !r.bloomFilter.Test(key) {
```

**When writing or modifying spec entries that correspond to existing code, include a back-reference to the file and function:**

```markdown
## SPEC-007
**Single I/O invariant** — blocks are always fetched in a single read.
Back-ref: `internal/blockio/reader/reader.go:GetBlockWithBytes`
```

**Rules:**
- Tag every non-trivial implementation that directly satisfies or is governed by a spec ID.
- Use `blockpack_search_modules` or `blockpack_lookup_requirement` to find the relevant ID before writing the comment — do not invent IDs.
- If you have questions about a piece of code, look up the IDs tagged in its comments first; those spec entries are the authoritative explanation.
- If no ID exists yet for a new design decision, add an entry to the appropriate spec file and assign the next sequential ID before tagging code.

### Rules for Agents

**DO NOT read SPECS.md, NOTES.md, TESTS.md, or BENCHMARKS.md directly.**
These files can be large. Use the MCP server instead:

- `blockpack_search_modules` — full-text search across all spec files (preferred entry point)
- `blockpack_package_docs` — read README.md for architecture overview of a package

**Writing and updating spec files is always permitted.** These are living documents and must be kept current as code evolves.

---

## MCP Server (USE BEFORE MODIFYING CODE)

This repo ships a project-specific MCP server pre-configured in `.mcp.json`. **Before modifying any package, use these tools to understand its design decisions, invariants, and extension patterns.**

### AI Assistant Tools

| Tool | When to use |
|---|---|
| `blockpack_search_modules` | Search SPECS/NOTES/TESTS/BENCHMARKS for a term — start here |
| `blockpack_package_docs` | List all documented packages, or read a specific package's README.md |
| `blockpack_explain_architecture` | Explain component interactions: `reader`, `writer`, `executor`, `vm`, `query-flow`, `block-pruning`, etc. |
| `blockpack_lookup_requirement` | Look up REQ-IDs or keyword-search `.bob/planning/REQUIREMENTS.md` |
| `blockpack_precommit_checklist` | Run quality checks and report status |

### Developer Tooling

| Tool | When to use |
|---|---|
| `blockpack_analyze_file` | Inspect a `.blockpack` file's metadata and statistics |
| `blockpack_run_benchmark` | Run benchmark queries and report I/O metrics |
| `blockpack_validate_file` | Validate blockpack file format compliance |
| `blockpack_file_layout` | Generate byte-level layout of a blockpack file (JSON or HTML) |
| `blockpack_describe_file` / `blockpack_describe_directory` | JSON descriptions of blockpack files |
| `blockpack_scan_directory` | Scan a directory for blockpack and parquet blocks |
| `blockpack_convert_proto` / `blockpack_convert_parquet` | Convert OTLP/Parquet files to blockpack |
| `blockpack_query_tempo` | Run TraceQL queries against Tempo endpoints |

---

## Behavioral Rules (Always Enforced)

- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
  - **Exception:** spec files under `internal/modules/` (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md) must be kept current — creating or updating them is always permitted
- NEVER save working files, text/mds, or tests to the root folder
- ALWAYS read a file before editing it
- NEVER commit secrets, credentials, or .env files

### internal/ packages

`internal/` packages may be edited as needed when explicitly requested. Each major package has a `README.md` documenting its design decisions, invariants, and extension patterns — read it via `blockpack_package_docs` before touching any code.

---

## Code Quality & Pre-Commit Checks (STRICT ENFORCEMENT)

- **ALWAYS** run `make precommit` before committing changes
- This runs all quality checks: gofumpt, golines, golangci-lint, betteralign, gocyclo, tests, coverage
- **NEVER** commit if `make precommit` fails
- CI will enforce these same checks — precommit ensures you catch issues locally first
- All checks are blocking — zero tolerance for violations

### Quality Standards

- **Cyclomatic complexity:** < 40 (enforced by gocyclo and golangci-lint)
- **Test coverage:** > 70% (enforced in CI)
- **Formatting:** gofumpt (stricter than gofmt)
- **Line length:** 120 characters (enforced by golines)
- **Nil safety:** nilaway checks for nil pointer dereferences (GHA only)
- **Memory efficiency:** betteralign checks for struct field alignment
- **Linting:** 40+ linters via golangci-lint (.golangci.yml)

---

## Build & Test

```bash
# Build all binaries to ./bin/
make build

# Run unit tests (with -race and checkptr)
make test

# Run a single test
go test -run TestFoo ./internal/blockio/reader/

# Run all pre-commit checks (REQUIRED before committing)
make precommit

# Run full CI pipeline locally
make ci

# Auto-fix all formatting issues (gofumpt, golines, betteralign)
make format-all

# Individual checks
make lint              # golangci-lint (40+ linters)
make nilaway           # Nil safety checks
make betteralign       # Struct alignment checks
make gofumpt-check     # Format check
make golines-check     # Line length check
```

### Tool Installation

```bash
make install-tools
# Or individually:
go install go.uber.org/nilaway/cmd/nilaway@latest
go install github.com/dkorunic/betteralign/cmd/betteralign@latest
go install mvdan.cc/gofumpt@latest
go install github.com/segmentio/golines@latest
go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
```

---

## Architecture Overview

### Query execution pipeline

```
TraceQL / SQL string
  → traceqlparser / sql (parse)
  → vm (compile to bytecode Program)
  → executor.BlockpackExecutor (run Program against Reader)
     → block selection (bloom filters, min/max stats, dedicated indexes)
     → GetBlockWithBytes() — ONE I/O per block (full block read, always)
     → parseBlockColumnsReuse() — column filtering happens in-memory, NOT at I/O
     → VM evaluates predicates per span
  → SpanMatchCallback / BlockpackResult
```

### Key design invariant — object storage I/O

**Always read entire blocks in a single I/O operation. Never add per-column I/O.**
This system targets object storage (S3/GCS/Azure) where request latency (50-100ms) dominates cost, not bytes transferred. Per-column selective reads caused 10-120x more API calls in prior versions and were removed.

Monitor these metrics when touching I/O code:
- `io_ops` — good: <500; warning: 500-1000; critical: >1000
- `bytes/io` — good: >100KB; warning: 10-100KB; critical: <10KB

### Package map

| Package | Role |
|---|---|
| `api.go` | Minimal public API — thin wrappers only |
| `internal/blockio/writer/` | Columnar write path; adaptive encodings per column type |
| `internal/blockio/reader/` | Columnar read path; block cache; block index lookup |
| `internal/blockio/shared/` | Shared provider interfaces, LRU/shard caches, RLE index |
| `internal/executor/` | Query executor; block selection; predicate extraction |
| `internal/vm/` | Stack-based bytecode VM; TraceQL/metrics compiler |
| `internal/traceqlparser/` | TraceQL parser (filter + metrics queries) |
| `internal/tempoapi/` | Tempo-compatible API: FindTraceByID, SearchTags, GetBlockMeta |
| `internal/blockio/compaction/` | Multi-file compaction with size-bounded output |
| `internal/modules/` | Spec-driven modules with SPECS/NOTES/TESTS/BENCHMARKS docs |
| `cmd/mcp-server/` | Project MCP server (architecture, search, requirements, precommit tools) |
| `benchmark/` | Real-world and format comparison benchmarks |

---

## Concurrency Best Practices

- All operations should be concurrent/parallel when possible
- Batch related operations in a single message
- Batch all file reads/writes/edits together
- Batch all Bash commands together when they don't depend on each other
