# mcp-server

MCP (Model Context Protocol) server that provides AI coding agents with project-aware tools for the blockpack codebase.

## Purpose

The MCP server gives AI agents (Claude Code, Copilot, etc.) structured access to blockpack's architecture, requirements, quality checks, and package documentation. Instead of agents reading raw source files, they can query semantic tools that return curated, contextual information. The server runs via stdio transport and is pre-configured in `.claude/mcp.json`.

## Tools

| Tool | Purpose |
|---|---|
| `blockpack_package_docs` | List all documented packages or read a specific package's README.md |
| `blockpack_explain_architecture` | Explain component interactions (reader, writer, executor, vm, query-flow, block-pruning, etc.) |
| `blockpack_lookup_requirement` | Look up requirements by REQ-ID or keyword from `.bob/planning/REQUIREMENTS.md` |
| `blockpack_precommit_checklist` | Run quality checks (gofumpt, golines, tests, go mod tidy) and report status |
| `blockpack_analyze_file` | Inspect a `.blockpack` file's metadata, block count, span count, time range |
| `blockpack_run_benchmark` | Execute benchmarks matching a pattern and return parsed results |
| `blockpack_validate_file` | Verify blockpack file format compliance and integrity |

## How Documentation Discovery Works

The `blockpack_package_docs` tool dynamically scans for `README.md` files under `internal/` and `cmd/` on first use (lazily cached for the session). Any package that has a README.md is automatically discoverable. The `blockpack_explain_architecture` tool also pulls README.md content when explaining components, so keeping READMEs up-to-date directly improves the architecture tool's responses.

To add documentation for a new package: create a `README.md` in the package directory. It will be picked up automatically.

## Running

```bash
# Via MCP config (automatic in Claude Code):
go run ./cmd/mcp-server

# The server communicates over stdio (JSON-RPC)
```

## Package Structure

- `main.go` -- Server initialization and tool registration
- `server/storage.go` -- File-backed key-value storage with in-memory cache (TTL: 1 hour)
- `tools/docs.go` -- Dynamic README.md discovery and serving
- `tools/architecture.go` -- Component architecture explanations (README-backed)
- `tools/requirements.go` -- Requirements lookup from `.bob/planning/REQUIREMENTS.md`
- `tools/workflow.go` -- Pre-commit checklist runner
- `tools/analysis.go` -- Blockpack file analysis
- `tools/benchmark.go` -- Benchmark execution and parsing
- `tools/validation.go` -- File format validation
- `tools/testing.go` -- Test utilities (mock storage)

## Storage

The MCP server stores data in `.blockpack-mcp/` (gitignored):

- **cache/** -- Cached docs and analysis (1-hour TTL)
- **state/** -- Session state and history
- **config/** -- User settings (editable JSON)
- **workspace/** -- Temporary analysis files

## Adding a New Tool

1. Create `tools/newtool.go` with `NewTool()` and handler function
2. Register in `main.go`: `mcpServer.AddTool(tools.NewTool(storage), tools.NewHandler(storage))`
3. Add test in `tools/newtool_test.go` using `newMockStorage()`
4. Update tool count in `main.go` log message

## Testing

```bash
go test ./cmd/mcp-server/...
```

## Package Dependencies

- **Imports**: `github.com/mark3labs/mcp-go` (MCP protocol), `github.com/grafana/blockpack` (public API for file analysis/validation)
- **Used by**: `.claude/mcp.json` (auto-started by Claude Code)
