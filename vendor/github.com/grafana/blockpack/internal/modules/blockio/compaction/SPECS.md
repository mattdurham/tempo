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
- `droppedSpans`: count of spans silently dropped due to genuine `(trace:id, span:id)`
  duplication across inputs — i.e. the normal, expected dedupe case (see §4). It does
  **not** count legacy/malformed-identity input; that now returns an error instead (holistic
  review Fix 2/NOTE-104).
- `error`: non-nil on I/O errors, context cancellation, output write failures, or a source
  block whose `trace:id`/`span:id` identity is missing or malformed (see §4).

This return value is also surfaced by the public `blockpack.CompactBlocks`/
`blockpack.CompactBlocksStreaming` wrappers in the root package (holistic review Fix 3;
previously discarded there).

### 2.3 Invariants

- **No false negatives:** every span present in any input file with valid (trace:id, span:id)
  appears in exactly one output file — deduplicated, not omitted.
- **Deduplication:** if the same (trace:id, span:id) pair appears in multiple input files,
  only the first occurrence (in provider order) is written to output; later occurrences are
  silently dropped and counted in `droppedSpans`.
- **Column preservation:** all column values for a span are preserved verbatim via the
  native columnar copy path (`Writer.AddRow`).
- **Empty input:** when `providers` is empty or nil, returns `(nil, 0, nil)`.
- **Nil outputStorage:** returns an error immediately.
- **Legacy/malformed identity input:** a source block that entirely lacks the `trace:id`
  block column (the NOTE-469 #389-#420 intrinsic-only legacy shape), or that carries the
  column but has a malformed/absent value for a specific row, aborts compaction with a
  typed error rather than being silently dropped — see §4.

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

## 4. Span Dropping and Identity-Error Semantics

**NOTE-104 (holistic-review Fix 2, 2026-07-07):** `dedupeKey` mirrors
`writer.go:AddRowFromReader`'s treatment of the NOTE-469 legacy-identity condition
exactly, distinguishing two error cases from the one legitimate silent-drop case:

1. **Block-level identity absent (typed error, NOTE-469 family):** the `trace:id` block
   column is entirely absent from the source block — the #389-#420 intrinsic-only legacy
   shape. `dedupeKey` returns the shared greppable error
   `"...legacy intrinsic-only source block unsupported (missing trace:id block column, see
   NOTES.md NOTE-469) — re-compact"` (prefixed `"compaction: dedupeKey:"`), which propagates
   through `addSpanFromBlock`/`processBlock`/`processProvider` and aborts `CompactBlocks`/
   `CompactBlocksStreaming` with that error. This block can no longer be compacted; it must
   be re-compacted through an older code path or otherwise regenerated first.
2. **Row-level identity malformed (typed error, distinct from case 1):** the `trace:id` or
   `span:id` column is present on the block, but this specific row's value is absent or
   malformed (`trace:id` not 16 bytes, `span:id` not 8 bytes, or `IsPresent` false for
   either). `dedupeKey` returns a distinct, row-scoped error (e.g. `"trace:id missing or
   malformed at row %d"` / `"span:id missing or malformed at row %d"`), which likewise
   aborts compaction. This is not expected in practice — the production writer always
   populates both columns together when present — but is checked explicitly rather than
   assumed, matching `AddRowFromReader`.
3. **Genuine duplicate (silent drop, normal operation):** both IDs are present and
   well-formed, but the exact `(trace:id, span:id)` pair was already written earlier in
   this compaction (from an earlier provider or an earlier row). This is the only case
   counted in `droppedSpans`.

**Rationale:** Cases 1 and 2 represent an unsupported or corrupt source block — silently
dropping those spans would be indistinguishable from ordinary deduplication and would lose
data with no operator-visible signal, violating the "typed errors for still-reachable
legacy inputs" invariant. Case 3 is expected, high-volume, and harmless, so it remains a
cheap counter rather than an error.

Back-ref: `internal/modules/blockio/compaction/compaction.go:dedupeKey`,
`compaction.go:addSpanFromBlock`

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

---

## 8. Log File Compaction (CompactLogFile / CompactLogFileBytes)

### Signatures

```go
func CompactLogFile(input modules_rw.ReaderProvider, output io.Writer, cfg Config) error
func CompactLogFileBytes(input []byte, cfg Config) ([]byte, error)
```

### Purpose

`CompactLogFile` reads a log-signal blockpack file, globally re-sorts all rows by
`(minHash[0..3], timestamp)`, and writes a new compacted file to `output`.

The MinHash signature is computed over `"key=value"` attribute pairs for each log record.
Re-sorting by minHash clusters log records with similar label sets into contiguous blocks,
producing tight per-block label-value boundaries that the range index can prune effectively
for label-based queries.

`CompactLogFileBytes` is a convenience wrapper over `CompactLogFile` that accepts and
returns byte slices. Intended for tests and single-file compaction workflows.

### Invariants

- **Signal type guard**: `CompactLogFile` returns an error if the input file's signal type
  is not `SignalTypeLog`. Trace files must use `CompactBlocks`.
- **Global sort**: all rows from all input blocks are collected into memory, sorted by
  `(minHash[0..3], timestamp)`, then re-written as a new file. This is an O(N) memory
  operation and is intended for moderate-size files.
- **Config reuse**: `cfg` is passed through to the underlying writer; `MaxBlockSpans`
  controls the output block size.

Back-ref: `internal/modules/blockio/compaction/log_compaction.go:CompactLogFile`,
`internal/modules/blockio/compaction/log_compaction.go:CompactLogFileBytes`

### NOTE-37 (Design Rationale)

See NOTE-37 in `NOTES.md`: global re-sort by `(minHash, timestamp)` is the mechanism
that makes label-based range index pruning effective for log files.
