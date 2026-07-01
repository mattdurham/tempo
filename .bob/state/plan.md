# Implementation Plan: Value-Index Logging, VCNT, and Cubes

*Created: 2026-07-01*
*Based on: brainstorm.md*
*Priority order: Task 1 → Task 2 → Task 3*

---

## Task 1 (High Priority, Low Effort): Fix Silent Value-Index Write Failures

### Background

`tempodb/encoding/vblockpack/create.go` lines 171–178 contain two silent-skip
patterns that make value-index L0 write failures invisible in logs:

```go
if store, prefix := getValueIndexSink(); store != nil {
    if _, serr := tmp.Seek(0, io.SeekStart); serr == nil {   // ← silent skip
        if r, rerr := blockpack.NewReaderFromProvider(&fileReaderProvider{f: tmp}); rerr == nil {  // ← silent skip
            sourceRef := blockObjectKey(meta.TenantID, blockUUID.String())
            if werr := blockpack.WriteValueIndexL0(r, store, sourceRef, meta.TenantID, prefix); werr != nil {
                level.Warn(util_log.Logger).Log(...)  // ← only this path logs
            }
        }
    }
}
```

The same `NewReaderFromProvider` silent-skip exists in `compactor.go`'s
`tempoOutputStorage.Put` (lines ~295–302).

`ConfigureValueIndex` in `valueindex.go` calls `slog.Warn` when the minio client
fails — but `slog.Warn` writes to the default Go `slog` handler, **not** to Tempo's
`go-kit/log` structured logger. On a block-builder startup this produces a log line
that may not be captured or correlated with the block-builder's own logs.

### Exact Changes

#### 1a. `tempodb/encoding/vblockpack/create.go`

**Add logging for the two silent-skip guards** (lines 171–178). The seek failure
and reader-open failure both need `level.Warn` log lines.

Current code (lines 171–178):

```go
if store, prefix := getValueIndexSink(); store != nil {
    if _, serr := tmp.Seek(0, io.SeekStart); serr == nil {
        if r, rerr := blockpack.NewReaderFromProvider(&fileReaderProvider{f: tmp}); rerr == nil {
            sourceRef := blockObjectKey(meta.TenantID, blockUUID.String())
            if werr := blockpack.WriteValueIndexL0(r, store, sourceRef, meta.TenantID, prefix); werr != nil {
                level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 write failed", "block", sourceRef, "err", werr)
            }
        }
    }
}
```

Replacement:

```go
if store, prefix := getValueIndexSink(); store != nil {
    blockKey := blockObjectKey(meta.TenantID, blockUUID.String())
    if _, serr := tmp.Seek(0, io.SeekStart); serr != nil {
        level.Warn(util_log.Logger).Log(
            "msg", "vblockpack: value-index L0 skipped (seek failed)",
            "block", blockKey,
            "err", serr,
        )
    } else if r, rerr := blockpack.NewReaderFromProvider(&fileReaderProvider{f: tmp}); rerr != nil {
        level.Warn(util_log.Logger).Log(
            "msg", "vblockpack: value-index L0 skipped (could not open block reader)",
            "block", blockKey,
            "err", rerr,
        )
    } else if werr := blockpack.WriteValueIndexL0(r, store, blockKey, meta.TenantID, prefix); werr != nil {
        level.Warn(util_log.Logger).Log(
            "msg", "vblockpack: value-index L0 write failed",
            "block", blockKey,
            "err", werr,
        )
    }
}
```

This also eliminates one extra `blockObjectKey` allocation (was being called
redundantly only in the inner success path).

#### 1b. `tempodb/encoding/vblockpack/compactor.go`

The same silent-skip exists in `tempoOutputStorage.Put` (lines ~295–302):

```go
if store, prefix := getValueIndexSink(); store != nil {
    if r, rerr := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data}); rerr == nil {
        sourceRef := blockObjectKey(s.tenantID, uuid.UUID(newID).String())
        if werr := blockpack.WriteValueIndexL0(r, store, sourceRef, s.tenantID, prefix); werr != nil {
            level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 write failed (compaction)", "block", sourceRef, "err", werr)
        }
    }
}
```

Replacement:

```go
if store, prefix := getValueIndexSink(); store != nil {
    blockKey := blockObjectKey(s.tenantID, uuid.UUID(newID).String())
    if r, rerr := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data}); rerr != nil {
        level.Warn(util_log.Logger).Log(
            "msg", "vblockpack: value-index L0 skipped (could not open block reader, compaction)",
            "block", blockKey,
            "err", rerr,
        )
    } else if werr := blockpack.WriteValueIndexL0(r, store, blockKey, s.tenantID, prefix); werr != nil {
        level.Warn(util_log.Logger).Log(
            "msg", "vblockpack: value-index L0 write failed (compaction)",
            "block", blockKey,
            "err", werr,
        )
    }
}
```

#### 1c. `tempodb/encoding/vblockpack/valueindex.go`

`ConfigureValueIndex`'s startup failure currently uses `slog.Warn` (Go stdlib):

```go
valueIndexConfigOnce.Do(func() {
    client, err := newMinioForValueIndex(s3cfg)
    if err != nil {
        slog.Warn("vblockpack: value-index write path disabled", "err", err)
        return
    }
    ...
})
```

Replace `slog.Warn` with `level.Warn(util_log.Logger)` so the startup failure
appears in Tempo's structured log output alongside other block-builder startup
messages.

Required import addition: `"github.com/go-kit/log/level"` and
`util_log "github.com/grafana/tempo/pkg/util/log"` (check if already present;
`valueindex.go` currently imports only `log/slog` for the one Warn call).

After the change:

```go
valueIndexConfigOnce.Do(func() {
    client, err := newMinioForValueIndex(s3cfg)
    if err != nil {
        level.Warn(util_log.Logger).Log(
            "msg", "vblockpack: value-index write path disabled (S3 client init failed)",
            "err", err,
        )
        return
    }
    ...
    level.Info(util_log.Logger).Log(
        "msg", "vblockpack: value-index write path enabled",
        "bucket", s3cfg.Bucket,
        "prefix", indexPrefix,
    )
    ...
})
```

The `Info` line at the end confirms successful initialisation (useful on startup).

### Tests

Add unit tests in `tempodb/encoding/vblockpack/valueindex_test.go` (already exists):

- `TestConfigureValueIndex_LogsOnBadEndpoint` — call `ConfigureValueIndex` with
  a nil or garbage S3 config and assert no panic. (Actual log line capture is not
  required — just confirm no panic and the sink stays nil.)

Add unit tests in `tempodb/encoding/vblockpack/create_test.go` (already exists):

- `TestCreateBlock_ValueIndexSeekFailure` — not practical to simulate kernel seek
  failure in unit test; note in code comment that the seek-fail path is covered by
  the log-visible error message.

No new test files required — the existing `*_test.go` files cover the surrounding
paths; this change is logging-only.

### Acceptance

- `go build ./tempodb/encoding/vblockpack/...` passes
- `go test ./tempodb/encoding/vblockpack/...` passes
- `grep -n "slog\." tempodb/encoding/vblockpack/valueindex.go` returns no hits
  (stdlib slog fully replaced)
- `grep -n "level.Warn" tempodb/encoding/vblockpack/create.go` returns 2+ hits

---

## Task 2 (Medium Priority): VCNT Standalone `.vcnt` Files

### Decision: Option A (no blockpack changes)

**Rationale:** The `valuecounts` package lives at
`vendor/github.com/grafana/blockpack/internal/modules/valuecounts/` and is in
the vendored tree (`modules.txt` does **not** list it as explicitly imported by
tempo — confirmed by grep). However, because this is a local replace directive
(`replace github.com/grafana/blockpack => ../blockpack`), Go's `internal`
visibility rule is scoped to the module boundary: an `internal/` package in
module `github.com/grafana/blockpack` is **not importable** from
`github.com/grafana/tempo`.

This means tempo **cannot** directly import
`github.com/grafana/blockpack/internal/modules/valuecounts`.

**Available paths:**

1. **Add a public re-export in blockpack** — add a thin public wrapper file in
   `../blockpack/` that re-exports the `valuecounts` types/functions needed:
   `Record`, `Sort`, `EncodeRecords`, `ColHash`, `FormatFilename`, `NewID`.
   This is a blockpack change but extremely small (one new file, no API redesign).
   Blockpack's `api.go` already explicitly restricts scope, but the brainstorm
   confirms this restriction is about the *query* API — a separate
   `valuecount.go` wrapper is consistent with `valueindex_extract.go`,
   `valueindex_l0write.go`, and `valueindex_query.go` already living in the
   blockpack root as thin wrapper files.

2. **Copy the minimal code into tempo** — copy just the encoding functions into
   `tempodb/encoding/vblockpack/vcntwriter.go`. This produces a fork that must be
   kept in sync with blockpack.

**Recommended: Option A.1 — add a public re-export file to blockpack.**

This is the correct long-term approach (no fork, single source of truth) and
requires only a few lines of wrapper code. The brainstorm's "Option A (simpler,
no blockpack changes)" description was slightly wrong — blockpack *does* need a
tiny change to export the types, but it is not a new API, only a visibility lift.

### Exact Changes

#### 2a. `../blockpack/vcnt.go` (new file in blockpack root)

```go
// Package blockpack — vcnt.go exposes the valuecounts types and encoding
// functions for tempo's block-builder, which writes standalone .vcnt files
// to S3 after each block flush.
package blockpack

import "github.com/grafana/blockpack/internal/modules/valuecounts"

// VCNTRecord is one value-count row: a (column, value) pair observed in Count
// spans within the [TimeStart, TimeEnd] unix-seconds window.
type VCNTRecord = valuecounts.Record

// VCNTChunkDirEntry is one chunk directory entry returned by EncodeVCNTRecords.
type VCNTChunkDirEntry = valuecounts.ChunkDirEntry

// SortVCNTRecords sorts records into the canonical VCNT order required by
// EncodeVCNTRecords: (ColumnName ASC, TimeStart ASC, Value ASC, Count ASC).
func SortVCNTRecords(records []VCNTRecord) {
    valuecounts.Sort(records)
}

// EncodeVCNTRecords encodes a pre-sorted slice of VCNTRecords into snappy-
// compressed chunk bytes. records MUST be sorted by SortVCNTRecords first.
// perChunk <= 0 uses the package default (~512). Returns the raw chunk bytes
// and the chunk directory; both are needed to write and later query the file.
func EncodeVCNTRecords(records []VCNTRecord, perChunk int) ([]byte, []VCNTChunkDirEntry) {
    return valuecounts.EncodeRecords(records, perChunk)
}

// VCNTColHash returns the per-column directory hash for the .vcnt object key:
//   <prefix>/<tenant>/unique_values/<col_hash>/L0-<id>.vcnt
func VCNTColHash(colName string) string {
    return valuecounts.ColHash(colName)
}

// VCNTFilename returns a .vcnt filename for the given compaction level and ID.
// Use level=0 for freshly-written L0 files. Use valuecounts.NewID() for the id.
func VCNTFilename(level int, id string) string {
    return valuecounts.FormatFilename(level, id)
}

// VCNTNewID returns a new unique ID suitable for use in VCNTFilename.
func VCNTNewID() string {
    return valuecounts.NewID()
}
```

After adding this file, run `go mod vendor` from the tempo root to sync it into
`vendor/github.com/grafana/blockpack/vcnt.go` and add the package to
`vendor/modules.txt`.

#### 2b. `tempodb/encoding/vblockpack/vcntwriter.go` (new file in tempo)

This file accumulates per-column value counts during block ingest and writes
the resulting `.vcnt` file to S3 after `writer.Flush()`.

```go
package vblockpack

// vcntwriter.go — per-block value-count accumulator and writer (standalone .vcnt files).
//
// During ingest (CreateBlock), a vcntAccumulator collects the distinct string values
// seen for each column across all traces in the block, within the block's time window.
// After writer.Flush(), the accumulated records are encoded and written as a standalone
// .vcnt file to object storage under:
//   <prefix>/<tenant>/unique_values/<col_hash>/L0-<id>.vcnt

import (
    "context"
    "fmt"
    "strings"

    "github.com/grafana/blockpack"
    "github.com/grafana/tempo/tempodb/backend"
)

// vcntSink is process-level — same singleton pattern as valueIndexSink.
// Set by ConfigureVCNT (not yet wired); guarded by valueIndexSinkMu (re-use).
// For now this is a placeholder; the actual singleton wiring is Task 2c.

// vcntAccumulator accumulates (column, value, timeStart, timeEnd, count) tuples
// across all spans in one block. Columns are limited to string-valued span and
// resource attributes; identity columns (trace:id, span:id, span:parent_id)
// are excluded (high cardinality, no tag-autocomplete value).
type vcntAccumulator struct {
    // counts maps column → value → count.
    counts map[string]map[string]int64
    // timeStart and timeEnd are unix seconds for the block's time window.
    // They are set from the block meta on first use.
    timeStart uint64
    timeEnd   uint64
}

func newVCNTAccumulator(timeStartSec, timeEndSec uint64) *vcntAccumulator {
    return &vcntAccumulator{
        counts:    make(map[string]map[string]int64),
        timeStart: timeStartSec,
        timeEnd:   timeEndSec,
    }
}

// Add records a (column, value) observation. value must be the string
// representation of the attribute value. column is the blockpack column name
// (e.g. "span.http.method", "resource.service.name").
//
// Identity and high-cardinality columns are filtered here:
//   - "trace:id", "span:id", "span:parent_id", "span:parent_span_id" — excluded
//   - "span:start", "span:duration", "__embedding__" — excluded (numeric/binary)
//
// Value is truncated at 256 bytes to avoid unbounded memory use.
func (a *vcntAccumulator) Add(column, value string) {
    if isExcludedVCNTColumn(column) {
        return
    }
    if len(value) > 256 {
        value = value[:256]
    }
    if a.counts[column] == nil {
        a.counts[column] = make(map[string]int64)
    }
    a.counts[column][value]++
}

// Records converts the accumulator into a sorted []blockpack.VCNTRecord slice,
// ready for EncodeVCNTRecords. The caller must not use the accumulator after
// calling Records.
func (a *vcntAccumulator) Records() []blockpack.VCNTRecord {
    total := 0
    for _, vals := range a.counts {
        total += len(vals)
    }
    if total == 0 {
        return nil
    }
    recs := make([]blockpack.VCNTRecord, 0, total)
    for col, vals := range a.counts {
        for val, cnt := range vals {
            recs = append(recs, blockpack.VCNTRecord{
                ColumnName: col,
                Value:      []byte(val),
                TimeStart:  a.timeStart,
                TimeEnd:    a.timeEnd,
                Count:      cnt,
            })
        }
    }
    blockpack.SortVCNTRecords(recs)
    return recs
}

// isExcludedVCNTColumn returns true for columns that should NOT be indexed in
// the value-count file: identity, numeric/binary, and embedding columns.
func isExcludedVCNTColumn(col string) bool {
    switch col {
    case "trace:id", "span:id", "span:parent_id", "span:parent_span_id",
        "span:start", "span:duration", "__embedding__",
        "span:status", "span:kind": // numeric
        return true
    }
    // Exclude any column that starts with "__" (internal blockpack columns).
    return strings.HasPrefix(col, "__")
}

// writeVCNTFile encodes the accumulator's records and writes one .vcnt file per
// column group to object storage via the provided ObjectPutter.
// Returns the number of columns written and any write error.
// Best-effort: errors are returned but do not fail the block write.
func writeVCNTFile(
    ctx context.Context,
    acc *vcntAccumulator,
    store backend.Writer,
    tenantID string,
    prefix string,
) (int, error) {
    _ = ctx // reserved for future cancellation
    recs := acc.Records()
    if len(recs) == 0 {
        return 0, nil
    }

    // Group records by column; each column gets its own .vcnt file.
    // This mirrors how blockpack's WriteValueIndexL0 writes per-column files.
    colRecs := groupVCNTRecordsByColumn(recs)
    id := blockpack.VCNTNewID()
    var firstErr error
    written := 0
    for col, colSlice := range colRecs {
        data, _ := blockpack.EncodeVCNTRecords(colSlice, 0)
        if len(data) == 0 {
            continue
        }
        colHash := blockpack.VCNTColHash(col)
        filename := blockpack.VCNTFilename(0, id)
        // Object key: <prefix>/<tenant>/unique_values/<col_hash>/L0-<id>.vcnt
        key := fmt.Sprintf("%s/%s/unique_values/%s/%s", prefix, tenantID, colHash, filename)
        if err := store.Append(ctx, key, []byte(tenantID), data); err != nil {
            if firstErr == nil {
                firstErr = fmt.Errorf("vcnt write %s: %w", col, err)
            }
            continue
        }
        written++
    }
    return written, firstErr
}

// groupVCNTRecordsByColumn returns a map from column name to records for that column.
// Input must already be sorted in canonical VCNT order (ColumnName ascending).
func groupVCNTRecordsByColumn(recs []blockpack.VCNTRecord) map[string][]blockpack.VCNTRecord {
    out := make(map[string][]blockpack.VCNTRecord)
    for i := range recs {
        col := recs[i].ColumnName
        out[col] = append(out[col], recs[i])
    }
    return out
}
```

**Note:** The `writeVCNTFile` function uses `store backend.Writer` as a placeholder.
The actual storage mechanism (direct S3 minio put, same as `s3ObjectPutter`) needs
to be wired in a follow-up step once the singleton pattern is decided. The key
object-path format is fully determined above.

#### 2c. Wiring VCNT accumulation in `create.go`

The block-builder's `CreateBlock` function iterates over traces and calls
`writer.AddTempoTrace(tr)`. To accumulate value counts, the loop needs to extract
span attributes from each `tempopb.Trace` and call `acc.Add(col, val)`.

**Problem:** `writer.AddTempoTrace(tr)` consumes a `*tempopb.Trace`. Tempo's
`tempopb.Trace` is a protobuf type with `ResourceSpans[].Resource.Attributes` and
`ResourceSpans[].ScopeSpans[].Spans[].Attributes`. These are accessible before
the trace is handed off to the blockpack writer.

Changes to `CreateBlock` loop body:

```go
// After: id, tr, nextErr := i.Next(ctx)
// Before: writer.AddTempoTrace(tr)

if tr != nil && acc != nil {
    accumulateVCNTFromTrace(acc, tr)
}
```

New helper function `accumulateVCNTFromTrace` in `vcntwriter.go`:

```go
// accumulateVCNTFromTrace adds all string-valued span and resource attributes
// from a tempopb.Trace to the VCNT accumulator.
func accumulateVCNTFromTrace(acc *vcntAccumulator, tr *tempopb.Trace) {
    for _, rs := range tr.ResourceSpans {
        // Resource attributes → "resource.<key>" columns.
        if rs.Resource != nil {
            for _, kv := range rs.Resource.Attributes {
                if kv.Value != nil {
                    if sv, ok := kv.Value.Value.(*v1_common.AnyValue_StringValue); ok {
                        acc.Add("resource."+kv.Key, sv.StringValue)
                    }
                }
            }
        }
        for _, ss := range rs.ScopeSpans {
            for _, span := range ss.Spans {
                // Span attributes → "span.<key>" columns.
                for _, kv := range span.Attributes {
                    if kv.Value != nil {
                        if sv, ok := kv.Value.Value.(*v1_common.AnyValue_StringValue); ok {
                            acc.Add("span."+kv.Key, sv.StringValue)
                        }
                    }
                }
                // Intrinsic string columns — add selectively.
                if span.Name != "" {
                    acc.Add("span:name", span.Name)
                }
            }
        }
    }
}
```

Required imports in `create.go` or `vcntwriter.go`:

- `tempopb "github.com/grafana/tempo/pkg/tempopb/trace/v1"` — check exact import path
- `v1_common "go.opentelemetry.io/proto/otlp/common/v1"` — for `AnyValue_StringValue`

#### 2d. `vendor/modules.txt` update

After adding `vcnt.go` to `../blockpack`, run:

```bash
cd /home/mdurham/source/blockpack_collection/tempo && go mod vendor
```

This adds `github.com/grafana/blockpack` (with the new `VCNTRecord`, `VCNTColHash`,
etc. symbols) to the vendor tree and updates `modules.txt`.

### What is NOT in scope for Task 2

- **Querier-side `.vcnt` reader** — reading `.vcnt` files for tag autocomplete is a
  separate feature. The writer side (accumulate + write) is Task 2's deliverable.
- **VCNT compaction** — merging L0 `.vcnt` files into L1/L2 is also separate.
- **Wiring the S3 singleton for VCNT** — `writeVCNTFile` has a backend.Writer
  placeholder. The actual singleton (`ConfigureVCNT`) parallels `ConfigureValueIndex`
  and can be added in the same PR or a follow-up. Task 2 is complete when the
  accumulation logic and encoding are wired and the write call is in place.

### Tests for Task 2

New file: `tempodb/encoding/vblockpack/vcntwriter_test.go`

```go
func TestVCNTAccumulator_Basic(t *testing.T) {
    // Add known (column, value) pairs, call Records(), assert sorted output.
}

func TestVCNTAccumulator_ExcludesIdentityColumns(t *testing.T) {
    // Add "trace:id", "span:id" — Records() should return nil.
}

func TestVCNTAccumulator_TruncatesLongValues(t *testing.T) {
    // Add a 300-byte value — assert stored length is 256.
}

func TestGroupVCNTRecordsByColumn(t *testing.T) {
    // Three records for two columns — assert correct grouping.
}

func TestIsExcludedVCNTColumn(t *testing.T) {
    // Table-driven: known-excluded and known-included columns.
}
```

### Acceptance

- `go build ./tempodb/encoding/vblockpack/...` passes
- `go test ./tempodb/encoding/vblockpack/...` passes
- `go build github.com/grafana/blockpack` (vendor) passes (new `vcnt.go` compiles)
- `vcntwriter_test.go` passes with `go test -v -run TestVCNT`

---

## Task 3 (Low Priority): Cubes

### Decision: File a GitHub issue, skip implementation

**Rationale:**

The `cube` package lives at
`blockpack/internal/modules/cube/` and is **not** exported in blockpack's public
API. To use it from tempo, blockpack would need to:

1. Export `Accumulator`, `Definition`, `SpanValues`, `ObjectPutter`, and
   `FlushTo` in a new `cube.go` root-level wrapper file (analogous to Task 2a's
   `vcnt.go`).
2. Tempo needs a `SpanValues` adapter over `*tempopb.Span` + `*resource.Resource`.
3. Tempo needs per-minute accumulator rotation in `CreateBlock`'s ingest loop.
4. Tempo needs a `cube.Registry` startup load and per-tenant cube definition cache.
5. The querier needs `cube.CreationTrigger.TryCreate()` wired into the search path.

This is 5 independent changes across blockpack and tempo, each requiring test
coverage. Total estimated effort: 3–5 days.

### Action

File a GitHub issue in the blockpack repo titled:
**"Export cube package for tempo block-builder integration"**

Issue body should contain:

- Link to `internal/modules/cube/accumulator.go` — the `SpanValues` interface
- The accumulator usage pattern from brainstorm.md Problem 3
- Required public API surface: `Accumulator`, `Definition`, `SpanValues`,
  `ObjectPutter`, `Registry`, `RegistryEntry`, `CreationTrigger`
- Estimated work: small blockpack change (export wrapper) + medium tempo work
- Dependency: VCNT (Task 2) should be shipped first as the simpler precedent

**No code changes for Task 3.**

---

## File Change Summary

| File | Change | Task |
|------|--------|------|
| `tempodb/encoding/vblockpack/create.go` | Add warn logging for seek/reader errors | 1a |
| `tempodb/encoding/vblockpack/compactor.go` | Add warn logging for reader error (compaction) | 1b |
| `tempodb/encoding/vblockpack/valueindex.go` | Replace `slog.Warn` with `level.Warn`; add info log | 1c |
| `../blockpack/vcnt.go` | New file: public re-export of valuecounts types | 2a |
| `tempodb/encoding/vblockpack/vcntwriter.go` | New file: accumulator + write functions | 2b |
| `tempodb/encoding/vblockpack/create.go` | Wire VCNT accumulation in ingest loop | 2c |
| `tempodb/encoding/vblockpack/vcntwriter_test.go` | New file: unit tests | 2 |
| `vendor/github.com/grafana/blockpack/vcnt.go` | `go mod vendor` syncs this automatically | 2a |

---

## Execution Order

```
Task 1a → Task 1b → Task 1c → build+test
Task 2a (blockpack vcnt.go) → go mod vendor → Task 2b (vcntwriter.go) → Task 2c (wire in create.go) → build+test
Task 3: file GitHub issue only
```

Task 1 and Task 2 are independent — Task 2 does not depend on Task 1.

---

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| `internal` visibility prevents `valuecounts` import | High (blocks Task 2) | Task 2a adds blockpack re-export wrapper; this is the approved approach |
| `go mod vendor` picks up unrelated blockpack changes | Low | Review diff after vendor; only `vcnt.go` should appear |
| VCNT accumulation adds latency to `CreateBlock` hot path | Medium | Accumulation is O(spans × attrs); add `go test -bench=BenchmarkCreateBlock` before landing |
| VCNT accumulation memory use for large blocks | Medium | 256-byte value truncation + column exclusion list bounds it; document expected memory |
| `slog.Warn` → `level.Warn` changes log format | None (improvement) | Confirmed Tempo uses go-kit/log throughout; `util_log.Logger` is the right target |
| `writeVCNTFile` uses placeholder backend.Writer | Medium | Mark with TODO; do not wire the actual S3 call until the singleton is ready |

---

## Open Questions

1. **VCNT object key format** — the brainstorm states
   `<tenant>/indexes/unique_values/<colHash>/L0-<id>.vcnt`. Confirm with the
   blockpack `valuecounts/filename.go` `FormatFilename` + the value-index prefix
   convention. The plan above uses
   `<prefix>/<tenant>/unique_values/<colHash>/L0-<id>.vcnt` — verify this matches
   the querier's expected key format before wiring the write call.

2. **Protobuf import path for span attributes** — `tempopb.Trace` vs
   `v1.ResourceSpans` path needs to be confirmed against the actual import in
   `create.go`. Do not assume; read the imports before writing `accumulateVCNTFromTrace`.

3. **VCNT write sink** — should `writeVCNTFile` use the same `s3ObjectPutter` as
   `valueIndexSink`, or should it reuse the `backend.Writer` passed to
   `CreateBlock`? The latter avoids a second S3 client but requires confirming the
   backend.Writer interface supports the required object key format. Resolve before
   landing Task 2c.
