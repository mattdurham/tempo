# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

Blockpack is a high-performance columnar storage format for OpenTelemetry trace data, designed for:
- 10-50x compression ratios
- Fast TraceQL/SQL query execution
- Tempo/Grafana integration
- Block-level pruning and multi-level indexing

The public API (`api.go`) is minimal and intentionally small — all implementation details belong in `internal/`. **Do not add new public API surface without explicit user permission.**

## Behavioral Rules (Always Enforced)

- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
- NEVER save working files, text/mds, or tests to the root folder
- ALWAYS read a file before editing it
- NEVER commit secrets, credentials, or .env files

## internal/ packages — modification rules

All other `internal/` packages may be edited as needed.

## Code Quality & Pre-Commit Checks (STRICT ENFORCEMENT)

- **ALWAYS** run `make precommit` before committing changes
- This runs all quality checks: gofumpt, golines, golangci-lint, betteralign, gocyclo, tests, coverage
- **NEVER** commit if `make precommit` fails
- CI will enforce these same checks - precommit ensures you catch issues locally first
- All checks are blocking - zero tolerance for violations

## Quality Standards

- **Cyclomatic complexity:** < 40 (enforced by gocyclo and golangci-lint)
- **Test coverage:** > 70% (enforced in CI)
- **Formatting:** gofumpt (stricter than gofmt)
- **Line length:** 120 characters (enforced by golines)
- **Nil safety:** nilaway checks for nil pointer dereferences (GHA only)
- **Memory efficiency:** betteralign checks for struct field alignment
- **Linting:** 40+ linters via golangci-lint (.golangci.yml)

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

## Tool Installation

```bash
make install-tools
# Or individually:
go install go.uber.org/nilaway/cmd/nilaway@latest
go install github.com/dkorunic/betteralign/cmd/betteralign@latest
go install mvdan.cc/gofumpt@latest
go install github.com/segmentio/golines@latest
go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
```

## MCP Server (USE BEFORE MODIFYING CODE)

This repo ships a project-specific MCP server pre-configured in `.claude/mcp.json`. **Before modifying any package, use these tools to understand its design decisions, invariants, and extension patterns.** Each major package has a README.md with Key Types, Data Flow, Invariants (what NOT to break), and Extending (how to safely add features).

**Start here:**
- `blockpack_package_docs` — call with no args for a system overview and ranked list of most important components; call with a package path (e.g. `internal/executor`) for full documentation
- `blockpack_explain_architecture` — explain component interactions: `reader`, `writer`, `blockio`, `executor`, `query-flow`, `block-pruning`, `arena`, `vm`, `encodings`, `tempoapi`, `traceqlparser`

**Other tools:**
- `blockpack_lookup_requirement` — look up REQ-IDs or keyword-search `.bob/planning/REQUIREMENTS.md`
- `blockpack_precommit_checklist` — run quality checks and report status
- `blockpack_analyze_file` — inspect a `.blockpack` file's metadata and statistics
- `blockpack_run_benchmark` — run benchmark queries and report I/O metrics
- `blockpack_validate_file` — validate blockpack file format compliance

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

**Always read entire blocks in a single I/O operation. Never add per-column I/O.** See `internal/blockio/AGENTS.md` for the full rationale. The short version: this system targets object storage (S3/GCS/Azure) where request latency (50-100ms) dominates cost, not bytes transferred. Per-column selective reads caused 10-120x more API calls in prior versions and were removed.

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
| `cmd/mcp-server/` | Project MCP server (architecture, requirements, precommit tools) |
| `benchmark/` | Real-world and format comparison benchmarks |

## Concurrency Best Practices

- All operations should be concurrent/parallel when possible
- Batch related operations in a single message
- Batch all file reads/writes/edits together
- Batch all Bash commands together when they don't depend on each other
