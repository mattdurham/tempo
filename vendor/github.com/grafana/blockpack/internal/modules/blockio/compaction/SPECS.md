# compaction — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/blockio/compaction` package. It complements NOTES.md (design rationale)
and TESTS.md (test plan).

---

## 1. Responsibility Boundary

The compaction package **merges and deduplicates multiple blockpack files** into one or more
output files. It does not perform query evaluation, block selection, or I/O coalescing.

| Concern | Owner |
|---------|-------|
| Merging spans from multiple files | **compaction** |
| Deduplicating spans by (trace:id, span:id) | **compaction** |
| Splitting output by size limit | **compaction** |
| Writing blocks (columnar format) | `blockio.Writer` |
| Reading input blocks | `blockio/reader.Reader` |

---

## 2. CompactBlocks Contract

```go
func CompactBlocks(
    ctx context.Context,
    providers []modules_rw.ReaderProvider,
    cfg Config,
    outputStorage OutputStorage,
) (outputPaths []string, droppedSpans int64, error)
```

### 2.1 Inputs

- `providers`: ordered list of input blockpack files as `ReaderProvider` implementations.
  Providers are processed in order. Each provider that implements `io.Closer` is closed
  after processing.
- `cfg`: configuration for output file sizing and block granularity.
- `outputStorage`: destination for output files. Each file is pushed via `Put(path, data)`.

### 2.2 Outputs

- `outputPaths`: relative paths of output files written to `outputStorage`, in creation order.
  Each path is a filename of the form `"compacted-NNNNN.blockpack"`.
- `droppedSpans`: count of spans silently dropped due to missing or invalid `trace:id` or
  `span:id` columns (see §4).
- `error`: non-nil on I/O errors, context cancellation, or output write failures.

### 2.3 Invariants

- **No false negatives:** every span present in any input file with valid (trace:id, span:id)
  appears in exactly one output file — deduplicated, not omitted.
- **Deduplication:** if the same (trace:id, span:id) pair appears in multiple input files,
  only the first occurrence (in provider order) is written to output.
- **Column preservation:** all column values for a span are preserved verbatim via the
  native columnar copy path (`Writer.AddRow`).
- **Empty input:** when `providers` is empty or nil, returns `(nil, 0, nil)`.
- **Nil outputStorage:** returns an error immediately.

---

## 3. Config

```go
type Config struct {
    StagingDir        string // local directory for staging; defaults to os.TempDir()
    MaxOutputFileSize int64  // max estimated output file size in bytes; 0 = no limit
    MaxSpansPerBlock  int    // max spans per block; defaults to 2000 when zero
}
```

### 3.1 MaxOutputFileSize

When non-zero, `CompactBlocks` calls `Writer.CurrentSize()` after each span write.
When the estimate exceeds `MaxOutputFileSize`, the current output file is flushed and a
new one is started. This produces multiple output files.

**Invariant:** Output file sizes are estimates. The actual byte size may exceed
`MaxOutputFileSize` by one block's worth of spans.

### 3.2 MaxSpansPerBlock

Controls the `MaxBlockSpans` parameter passed to `NewWriterWithConfig`. Defaults to 2000.
This is independent of `MaxOutputFileSize` — both limits can trigger output file rotation.

---

## 4. Span Dropping Semantics

Spans with missing or invalid `trace:id` or `span:id` columns are **silently dropped**.
The count of dropped spans is returned as `droppedSpans`. A span is dropped when:

- The `trace:id` column is absent from the block.
- The `trace:id` value is not present for this row (`IsPresent` returns false).
- The `trace:id` value is not 16 bytes long.
- The `span:id` column is absent from the block.
- The `span:id` value is not present for this row.
- The `span:id` value is not 8 bytes long.

**Rationale:** Spans without valid identifiers cannot be deduplicated. Silently dropping
them preserves the output's structural validity.

Back-ref: `internal/modules/blockio/compaction/compaction.go:dedupeKey`

---

## 5. OutputStorage Interface

```go
type OutputStorage interface {
    Put(path string, data []byte) error
}
```

`Put` receives the relative filename (e.g. `"compacted-00000.blockpack"`) and the complete
file contents. The implementation is responsible for durability. If `Put` returns an error,
`CompactBlocks` returns that error immediately without writing further files.

---

## 6. Context Cancellation

`CompactBlocks` checks `ctx.Err()`:
1. Before processing begins (returns immediately if already canceled).
2. Before each input provider is processed.

When context is canceled mid-compaction, the function returns the context error along with
the `droppedSpans` count accumulated so far. Partially-written output files are **not** pushed
to `outputStorage`; the staging directory is cleaned up by deferred removal.

---

## 7. Staging Directory Lifecycle

Output files are written to a temporary staging directory before being pushed to
`outputStorage`. The staging directory is always cleaned up (removed) when `CompactBlocks`
returns, regardless of success or failure. This prevents staging-dir accumulation even on
errors.

**Invariant:** After `CompactBlocks` returns, the staging directory no longer exists.

Back-ref: `internal/modules/blockio/compaction/compaction.go:prepareStagingDir`,
`internal/modules/blockio/compaction/compaction.go:CompactBlocks` (deferred `cleanup()`)
