# Brainstorm: Three Open Problems

## Problem 1: Value-index L0 write silently stopped

### Root cause analysis

The code in `create.go:173` does:

```go
if r, rerr := blockpack.NewReaderFromProvider(&fileReaderProvider{f: tmp}); rerr == nil {
    // ... WriteValueIndexL0 ...
}
// rerr != nil → silently dropped
```

The writer (`vendor/.../blockio/writer/v8_sections.go`) unconditionally writes
`FooterV9Version = 9`. The reader (`vendor/.../blockio/reader/parser.go`) in
`tryReadFooterMagic18()` accepts **only** `FooterV9Version`:

```go
if ver != shared.FooterV9Version {
    return false, &UnsupportedFormatVersionError{Version: ver}
}
```

Any prior block format (V3–V8, i.e. version byte ≠ 9) causes
`NewReaderFromProvider` to return a non-nil `rerr` of type
`*UnsupportedFormatVersionError` — which is silently swallowed by the
`rerr == nil` guard in `create.go`.

**But the actual freshly-written temp file always carries FooterV9** (the writer
emits it unconditionally since commit `76b7477b`). The writer and the vendored
reader are identical format versions, so `NewReaderFromProvider` on a
freshly-written temp file should succeed.

The more likely explanation for the July 07:15 UTC halt is one of:

1. **`tmp.Seek(0, io.SeekStart)` failure is silently swallowed** — the outer
   `if _, serr := tmp.Seek(0, io.SeekStart); serr == nil` guard also eats its
   error without logging. If OS-level `seek` on a temp file fails (e.g. bad fd,
   OOM, disk full), the value-index write is skipped with no log line.

2. **`getValueIndexSink()` returned `nil`** — if the S3 client initialization
   in `ConfigureValueIndex` failed silently (its `sync.Once` swallowed the
   error via `slog.Warn`) and `valueIndexSink` was never set. In that case the
   outer `if store, prefix := getValueIndexSink(); store != nil` is false and
   the block proceeds without touching the index path — again no log line.

3. **An `ErrUnsupportedFormatVersion` could appear** only if the vendor
   (`tempo/vendor/github.com/grafana/blockpack`) and the writer diverged —
   e.g. if blockpack was updated to emit V10+ but tempo's vendor still carries
   the V9 reader. Not currently the case (both are V9).

### Fix

Two concrete changes needed:

**a) Log the silent `rerr != nil` case** — add a `level.Warn` for
`NewReaderFromProvider` failures so the skip is visible in logs:

```go
if r, rerr := blockpack.NewReaderFromProvider(&fileReaderProvider{f: tmp}); rerr == nil {
    // ...
} else {
    level.Warn(util_log.Logger).Log(
        "msg", "vblockpack: value-index L0 skipped (could not open temp file)",
        "block", blockObjectKey(meta.TenantID, blockUUID.String()),
        "err", rerr,
    )
}
```

**b) Log the silent `serr != nil` seek-failure case** similarly.

**c) Separately investigate** whether `ConfigureValueIndex`'s `slog.Warn` on
minio-client construction failure is being hidden. Add a metric or a startup
error log that is visible in block-builder startup logs.

---

## Problem 2: VCNT not wired

### What exists in blockpack

The `internal/modules/valuecounts` package provides:

- **`Record`** — one row: `(ColumnName, Value, TimeStart, TimeEnd, Count int64)`.
- **`EncodeRecords(records []Record, perChunk int) ([]byte, []ChunkDirEntry)`** — encodes
  a pre-sorted slice into snappy-chunked wire bytes.
- **`Sort(records []Record)`** — sorts in canonical `(ColumnName, TimeStart, Value,
  Count)` order required before `EncodeRecords`.
- **`Compact(records []Record) []Record`** — sums by `(col, timeStart, timeEnd, value)`,
  drops keys ≤ 0. Used during compaction.
- **`ValuesInRange`, `TopNInRange`, `CardinalityInRange`** — read-path queries over
  encoded data.
- Section is stamped `ToCSubTypeValueCounts = 16` in the ToC.

### What is missing (no public API yet)

There is **no** public `WriteVCNT` or `AccumulateVCNT` function in the blockpack
root package. The `valuecounts` package lives under `internal/modules/` and is
not re-exported at the blockpack root (no wiring in `blockpack/api.go` or any
`blockpack/*.go` file). The blockpack writer (`Writer.Flush()`) does not emit a
VCNT section.

The only place `ToCSubTypeValueCounts` is referenced is in the reader test and
the shared constants — confirming the section is defined but not yet emitted by
any production path.

### What tempo's block-builder needs to do

To produce VCNT data, the block-builder must:

1. After writing each span via `writer.AddTempoTrace`, accumulate per-column
   value counts into an in-memory `map[string]map[string]int64` (column →
   value → count) with the block's minute-bucket time window.
2. After `writer.Flush()`, encode the accumulated records:

   ```go
   recs := // build []valuecounts.Record from accumulator
   valuecounts.Sort(recs)
   data, dir := valuecounts.EncodeRecords(recs, 0)
   ```

3. Write the VCNT file to object storage under the standard key:
   `<tenant>/indexes/unique_values/<colHash>/L0-<id>.vcnt`

**However** — to add the VCNT section to the blockpack file itself (as
`ToCSubTypeValueCounts`), blockpack's writer would need a new API, e.g.
`writer.SetVCNTRecords([]valuecounts.Record)` before `Flush()`. That API does
**not exist yet** — it needs to be added to blockpack first, then vendored into
tempo.

Alternatively, the VCNT data can be written as a **separate object** in S3 (a
`.vcnt` file), bypassing the blockpack file format entirely, similar to how the
value-index L0 files work today. The query path in `valuecounts.ValuesInRange`
operates on raw bytes, not on a blockpack ToC — so a standalone `.vcnt` file
would work with no blockpack changes.

### Recommended path

Option A (simpler, no blockpack changes): accumulate value counts in tempo
during span ingest, write a standalone `.vcnt` file to S3 after each block flush
using `EncodeRecords`. A separate querier-side `.vcnt` reader reads these files
for tag autocomplete.

Option B (cleaner, requires blockpack change): add a `WriterConfig.VCNTRecords
[]valuecounts.Record` field (or a `writer.AddVCNTRecord(...)` method) so the
VCNT section is embedded in the blockpack file's ToC. This requires a blockpack
change + vendor update.

---

## Problem 3: Cubes not wired

### What exists in blockpack

The `internal/modules/cube` package provides:

- **`SpanValues` interface** (in `accumulator.go`):

  ```go
  type SpanValues interface {
      String(column string) (string, bool)
      Int64(column string) (int64, bool)
  }
  ```

- **`Accumulator`** — per-minute, per-cube in-memory counter:

  ```go
  acc := cube.NewAccumulator(def, minute)
  counted, err := acc.Add(spanValues)  // call once per span
  key, err := acc.FlushTo(store, tenant) // write L0 cube file
  acc.Reset(nextMinute)                // rotate to next minute
  ```

- **`Definition`** — what the accumulator needs from the cube config:
  `{Dim1Column, Dim2Column, Filters, ID [16]byte, Resolution uint32}`.
- **`RegistryEntry`** — stable JSON-serialized cube description in `index.json`.
- **`Registry`** — conditional-PUT S3 index at `<tenant>/cubes/index.json`.
- **`CreationTrigger`** — first-query cube registration after cardinality gate.
- **`Filename(tenant, cubeID) string`** — returns the L0 object key
  `<tenant>/cubes/<hex_id>/L0-<xid>.cube`.
- **`ObjectPutter`** interface (same shape as value-index's `ObjectPutter`):

  ```go
  type ObjectPutter interface {
      Put(path string, data []byte) error
  }
  ```

### What is missing

There is **no public wiring** in blockpack's root package for the cube path.
The `cube` package exists under `internal/modules/cube/` and is not exported
in `blockpack/api.go`. The blockpack writer does not call any cube accumulation.

Tempo has no `cube.Accumulator`, no `cube.Registry`, and no `ObjectPutter` for
cube L0 files.

### What the block-builder needs

1. **Load cube definitions** at startup from `<tenant>/cubes/index.json` via
   `cube.Registry.Load()`. Convert each `RegistryEntry` to a `cube.Definition`
   (mapping `Dimensions[0]` → `Dim1Column`, `Dimensions[1]` → `Dim2Column`).

2. **Create one `cube.Accumulator` per active cube per minute**:

   ```go
   acc := cube.NewAccumulator(def, currentMinute)
   ```

3. **For each span ingested**, call `acc.Add(spanValues)` where `spanValues`
   implements `cube.SpanValues`. Tempo's span representation would need a thin
   adapter that exposes `String(col)` / `Int64(col)` from the OTLP attributes.

4. **On minute rotation (or block flush)**, call `acc.FlushTo(store, tenant)` for
   each accumulator. This writes the encoded cube file to S3 and resets the
   accumulator.

5. **Register new cubes** on first query via `cube.CreationTrigger.TryCreate()`
   in the query path — this is not a block-builder concern but a querier concern.

### Key constraint

The `cube.SpanValues` interface is the bridge point. Tempo's current ingest path
(OTLP protobuf → `writer.AddTempoTrace`) does not surface individual span fields
for cube accumulation. A new adapter layer is needed:

```go
type protoSpanValues struct {
    span     *v1.Span
    resource *resource.Resource
}
func (s *protoSpanValues) String(col string) (string, bool) { /* lookup attribute */ }
func (s *protoSpanValues) Int64(col string) (int64, bool)   { /* lookup attribute */ }
```

The block-builder's ingest loop would need to be modified to call
`acc.Add(adapter)` per span before handing the span to the blockpack writer.

---

## Summary Table

| Problem | Root cause | Fix complexity | Blockpack changes needed? |
|---------|-----------|---------------|--------------------------|
| 1. L0 write silently stopped | `rerr != nil` swallowed silently; likely `getValueIndexSink()==nil` at startup or seek error | Low — add 2 log lines | No |
| 2. VCNT not wired | No public API in blockpack root; `valuecounts` is internal | Medium — either standalone .vcnt file write (no blockpack change) or embed in ToC (needs blockpack change) | Yes for Option B, No for Option A |
| 3. Cubes not wired | No public API in blockpack root; `cube` is internal; no tempo-side adapter | High — requires blockpack API export + SpanValues adapter + per-minute rotate logic in block-builder | Yes (export cube package) |

## Immediate next steps

1. **Problem 1 (high priority, low effort):** Add `level.Warn` logging for
   `rerr != nil` and `serr != nil` in `create.go`. Check `ConfigureValueIndex`
   startup for silent minio failures. Ship this now.

2. **Problem 2 (medium):** Decide Option A vs B. Option A (standalone `.vcnt`
   files) can be done entirely in tempo using the already-vendored internal
   packages. Option B needs a blockpack PR first.

3. **Problem 3 (high effort):** Requires a blockpack PR to export the `cube`
   package surface plus significant tempo-side work. Should be planned as a
   separate spike.
