# Blockpack

**Blockpack** is a research storage format for OpenTelemetry trace data exploring two ideas: automatically building a range index over every column using KLL quantile sketches, and clustering spans by attribute similarity using Jaccard (MinHash) signatures before block assignment. The goal is to make arbitrary range and attribute predicates efficiently prunable without any schema declaration at write time.

> Built for object storage. Designed for Tempo and Grafana.

## Primary Goals

Blockpack was designed around two core principles:

### 1. Index every column via KLL

Every column — numeric (duration, timestamp, status code, latency) or string (service name, HTTP route, environment) or bytes — is automatically indexed using a [KLL quantile sketch](https://arxiv.org/abs/1603.05346). This means any range predicate (`duration > 5ms`, `service.name > "m"`) can be evaluated against the index to prune blocks before any I/O is issued. There is no need to declare which columns should be indexed at write time; the system indexes everything.

### 2. Tight compaction through structured grouping

Spans are clustered before being assigned to blocks using a three-level sort key:

1. **`service.name`** — keeps spans from the same service together, maximising dictionary compression on resource attributes
2. **`timestamp`** — ensures temporal locality within a service, making time-range pruning highly effective
3. **Jaccard (MinHash) signature** — groups spans with similar attribute sets using a 32-byte MinHash fingerprint derived from their key/value pairs. Spans that share the same attributes (same HTTP route, same error code, same environment) land in the same block, dramatically improving columnar compression ratios

This combination means blocks are internally homogeneous: similar spans grouped by service and attribute similarity, sorted by time. The result is that spans with common attributes land together in the same block, creating better compression opportunities for columnar encodings and making block-level pruning effective for arbitrary attribute queries.

## Overview

Blockpack is a specialized file format designed for storing and querying distributed traces. It combines columnar storage, adaptive compression, and multi-level indexing to provide:

- **Significant compression** over raw OTLP traces via columnar encoding and span clustering
- **Fast queries** via TraceQL (filter, metrics, and structural queries)
- **Efficient trace lookups** with O(log N) performance
- **Block-level pruning** using bloom filters and statistics
- **Tempo compatibility** for seamless Grafana integration

## AI Workflow

Blockpack uses a SPECS/NOTES/BENCHMARKS/TESTS workflow, when an agent sees this please sync the code and details as appropriate.

Examples from the blockio module:
- [SPECS.md](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/SPECS.md)
- [NOTES.md](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/NOTES.md)
- [BENCHMARKS.md](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/BENCHMARKS.md)
- [TESTS.md](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/TESTS.md)

## Features

### Storage Efficiency

- **Columnar layout**: Each span attribute stored separately with type-specific encoding
- **Block organization**: ~2000 spans per block with independent compression
- **Adaptive encoding**: 11 encoding schemes automatically selected per column
- **Zstd compression**: Dictionary-based compression achieving 3-5x size reduction
- **MinHash clustering**: Spans sorted by service and similarity for optimal block locality

### Query Performance

- **Multi-level indexing**: Bloom filters, min/max statistics, and dedicated indexes
- **Block pruning**: Skip entire blocks using time/trace ranges (10-100x speedup)
- **Dedicated indexes**: Global value→block mapping for high-cardinality columns
- **Trace index**: Fast trace-by-ID lookups
- **Value statistics**: Per-block attribute statistics for advanced pruning
- **Coalesced I/O**: Adjacent blocks merged into single reads for object storage

### Query Languages

- **TraceQL filter**: `{ span.http.status_code = 500 && resource.service.name = "frontend" }`
- **TraceQL metrics**: `{ span.http.status_code = 500 } | rate() by (resource.service.name)`
- **Structural TraceQL**: `{ .parent } >> { .child }` (descendant, child, sibling operators)

## Quick Start

### Writing Traces

Create a `Writer` with `NewWriter`, add OTLP trace data with `AddTracesData`, then call `Flush` to finalize the file.

### Finding Traces by ID

Use `NewFileStorage` to create a storage backend, then call `FindTraceByID` with the file path and a hex-encoded trace ID for O(log N) lookup using the trace block index.

### Tag Discovery (Tempo/Grafana Integration)

`SearchTags` returns available attribute names scoped to `"span"`, `"resource"`, or all attributes. `SearchTagValues` returns up to 10,000 distinct values for a given attribute, powering Grafana autocomplete.

### Compaction

`CompactBlocks` merges spans from multiple providers into size-bounded output files, staging them locally before writing to a `WritableStorage` backend.

### Format Conversion

`ConvertProtoToBlockpack` converts an OTLP protobuf file. `ConvertParquetToBlockpack` converts a Tempo vparquet5 block directory.

## Building and Testing

Install required tools with `make install-tools`. Then:

- **Build**: `make build`
- **Test**: `make test`
- **Pre-commit checks** (required before committing): `make precommit` — runs formatting, linting, nil safety, struct alignment, cyclomatic complexity, tests, and coverage
- **Full CI pipeline**: `make ci`
- **Auto-fix formatting**: `make format-all`

## Architecture

### File Structure

Each blockpack file is a sequence of block payloads followed by a metadata section (block index, dedicated column indexes, trace block index), a fixed header, and a 10-byte footer. See the [File Format](https://ubiquitous-couscous-qjl94l2.pages.github.io/File-Format) page for the full specification.

### Query Execution Pipeline

A TraceQL string is parsed into a typed AST (`FilterExpression`, `MetricsQuery`, or `StructuralQuery`), compiled by the VM into a `Program` with column predicates and extracted block-pruning predicates, then executed by the `BlockpackExecutor`. The executor prunes blocks by time range, value statistics, bloom filters, and dedicated indexes before reading each selected block in a single I/O operation and evaluating per-span predicates.

### Key Components

| Package | Role |
|---|---|
| `api.go` | Minimal public API — thin wrappers only |
| `internal/blockio/writer/` | Streaming write path; adaptive encodings; MinHash clustering |
| `internal/blockio/reader/` | Columnar read path; block cache; index lookup |
| `internal/blockio/compaction/` | Multi-file compaction with size-bounded output |
| `internal/executor/` | TraceQL query executor; multi-stage block pruning |
| `internal/vm/` | TraceQL compiler; closure-based column predicates; bytecode VM |
| `internal/traceqlparser/` | TraceQL parser (filter, metrics, structural queries) |
| `internal/tempoapi/` | Tempo-compatible API: FindTraceByID, SearchTags, GetBlockMeta |
| `cmd/mcp-server/` | Project MCP server for Claude Code integration |
| `cmd/analyze/` | Blockpack file analysis tool (service distribution, column stats) |

### I/O Design

Blockpack targets object storage (S3/GCS/Azure) where request latency (50-100ms) dominates cost. Every block is always read in a single I/O operation — no per-column selective reads. Healthy query metrics:

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| `io_ops` | <500 | 500-1000 | >1000 |
| `bytes/io` | >100KB | 10-100KB | <10KB |

## Documentation

- [File Format](https://ubiquitous-couscous-qjl94l2.pages.github.io/file-format) — Block layout, column encodings, indexes
- [Execution Path](https://ubiquitous-couscous-qjl94l2.pages.github.io/execution-path) — Query pipeline walkthrough
- [Write Path](https://ubiquitous-couscous-qjl94l2.pages.github.io/write-path) — Ingestion, clustering, and index construction
- [Block Format](https://ubiquitous-couscous-qjl94l2.pages.github.io/block-format) — Column layout, encoding types, read patterns
- [Production Results](https://ubiquitous-couscous-qjl94l2.pages.github.io/production-results) — Benchmark vs Parquet on real traffic

## Quality Standards

This project enforces strict quality standards:

- **Cyclomatic complexity**: < 40 per function
- **Test coverage**: > 70%
- **Formatting**: gofumpt (stricter than gofmt)
- **Line length**: 120 characters
- **Nil safety**: nilaway checks for nil pointer dereferences
- **Memory efficiency**: betteralign for struct field alignment
- **Linting**: 40+ linters via golangci-lint

All checks are enforced in CI and must pass locally via `make precommit` before committing.

## Contributing

1. Make your changes
2. Run `make precommit` (required)
3. Commit with a descriptive message
4. Open a pull request

## License

Copyright Grafana Labs

## Links

- [GitHub Repository](https://github.com/grafana/blockpack)
- [Grafana Tempo](https://github.com/grafana/tempo)
- [OpenTelemetry](https://opentelemetry.io/)
