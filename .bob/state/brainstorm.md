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

---

# Investigation: TraceQL search timeouts on tempo-dev-test-03 — "whole file vs. internal block" fetch hypothesis

## 2026-07-02 17:04:48 - Task Received

Investigate and root-cause the "whole file download instead of block" performance
problem on tempo-dev-test-03's blockpack query path. TraceQL search queries against
tenant 11638 (window now-3h to now-1h) are timing out (~30-33s up to 2-2.5min,
hitting context deadline exceeded). Value-index coverage was already confirmed
working (querier logs show "coverage found", `index.used`/`index.hits` populated).
Working hypothesis handed in: the querier's fetch path pulls entire compacted
`.blockpack` FILES (60-65MB observed) rather than the specific internal BLOCK(s)
needed, because a single internal block "should be" at most a few MB.

Five implicated block IDs (tenant 11638): 93b44dd6-09cc-5088-bce8-1e1f675bf534,
925a3b53-c371-519b-96c0-6655c0d4511f, b6478933-c598-5a12-b601-3ea6b651d013,
882b9288-eaa1-5280-927f-f5edf2f2745e, dc40aa41-8b83-5e90-8eac-c8c8cb84656c.

Starting brainstorm process...

## 2026-07-02 17:10:00 - Research Findings

### Ground truth on file/block sizes (blockpack-explorer + analyze-block)

Port-forwarded `blockpack-explorer` (`kubectl port-forward svc/blockpack-explorer
-n tempo-dev-test-03 8090:8090`) and queried it directly (its `/query` endpoint
takes a raw SQL string as the POST body, not JSON — `{"error":"only SELECT
queries are permitted"}` otherwise).

`SELECT * FROM files WHERE path LIKE '%<blockID>%'` for all 5 implicated blocks:

| blockID | filesize (bytes) | spans |
|---|---|---|
| 93b44dd6-... | 63,613,103 | 1,286,218 |
| 925a3b53-... | 64,670,196 | 1,312,392 |
| b6478933-... | 65,321,763 | 1,334,998 |
| 882b9288-... | 64,473,610 | 1,313,356 |
| dc40aa41-... | 63,268,990 | 1,278,934 |

All 5 confirmed 63-65MB, matching the "60-65MB" figure from the shared background.
**However** — critically — `toc_entries` for these files shows only ONE named
section type (`type2.sub7.` = BlockIndex), totaling ~100KB. blockpack's ToC does
NOT enumerate per-internal-block sections; the bulk of the file is raw block
payload addressed only via the file's internal block-index (offset/length pairs),
invisible to `blockpack.ToCEntries()`.

To get real per-block sizes, downloaded one full file (93b44dd6, 63,613,103 bytes,
via `blockpack-explorer`'s `GET /file?path=...`, which itself took **24.2s** over
the port-forward — consistent with the reported query latencies, corroborating
that a 60MB single-object fetch really would be slow) and ran `blockpack/cmd/
analyze-block -file <path>` (built from `/home/mdurham/source/blockpack_collection/
blockpack` via `go build ./cmd/analyze-block`) against it:

- **644 inner blocks** in the file, 1,286,218 total spans (~2000 spans/block avg).
- **713 unique columns** — extreme attribute-key sprawl (many distinct
  `span.<app-specific>` attributes) is what inflates file size, not oversized
  blocks: `trace:id`, `span:id`, `span:parent_id` alone account for 28.08MB
  (49% of column bytes, 46% of the file) purely from ID-column overhead across
  644 blocks × ~2000 spans.
- Total inner-block column bytes: 57.18MB (94.3% of the 60.67MB file);
  file-level TOC sections: 0.01MB.
- **Average per-block payload ≈ 57.18MB / 644 ≈ 91KB** — much smaller than a
  "few MB", not larger. The premise that blocks are "at most a few MB" undersold
  it; real blocks in this tenant are two orders of magnitude smaller than that.

This already casts doubt on "blocks are secretly huge, so GetBlockWithBytes reads
gigabytes": inner blocks are tiny (~91KB avg). A file-wide scan touching all 644
blocks, even done correctly per-block, would still add up to ~57-60MB of total
I/O for this file — which is very close to the "whole file" size by coincidence
of block density, not because of a whole-file-read bug.

### Ground truth on what the querier actually requests (code path)

`GetBlockWithBytes` (blockpack `internal/modules/blockio/reader/reader.go:642`)
is **not called anywhere in tempo-mrd's own code** (`grep -rn GetBlockWithBytes
tempo/ | grep -v vendor` → zero hits outside vendor and stale `.bob/state/`
notes). It exists only inside the vendored blockpack package itself (used by
`blocksimilarity.go`, `valueindex_extract.go`, `compaction.go`, `writer_block.go`
compaction paths) — **not** on tempo's query path. The "GetBlockWithBytes = full
file" framing from the 2026-06-04 lth memory is stale/inaccurate for the current
code: `GetBlockWithBytes(blockIdx, wantColumns)` reads exactly one **internal**
block (`ReadBlockRaw` → `r.readRange(meta.Offset, meta.Length, ...)`,
`reader.go:269-275`), not the whole S3 object — the "block" in its name has always
meant the internal block, matching the ~91KB avg block size we measured, not a
whole file.

Tempo's actual per-block-file I/O:
- `tempoReaderProvider.ReadAt` (`tempodb/encoding/vblockpack/backend_block.go:67-80`)
  calls `p.reader.ReadRange(ctx, DataFileName, blockID, tenantID, off, buf, nil)` —
  a **ranged** read, buffer-length-bounded.
- S3 backend `readRange` (`tempodb/backend/s3/s3.go:673-696`) issues
  `options.SetRange(offset, offset+len(buffer))` then `GetObject` — a real HTTP
  Range GET, sized to exactly the requested buffer, not the whole object.
- `readAll`/`Read` (whole-object, `s3.go:643-654`) is a **separate** method, used
  only by non-search paths (see below).
- `Reader.ReadBlocks`/`ReadGroup`/`CoalescedGroups` (`blockio/reader/reader.go:
  293-330`) coalesce adjacent block ranges via `shared.AggressiveCoalesceConfig`
  (`blockio/shared/coalesce.go:12-17`): `MaxGapBytes=4MB`, **`MaxReadBytes=8MB`
  cap per coalesced request**. So even a full-file scan of all 644 densely-packed
  blocks would legitimately fan out into ~7-8 separate 8MB-capped ranged GETs
  totaling ~57MB — never one single 60MB GetObject call.
- Two genuine **whole-object** `StreamReader` + `io.ReadFull` reads do exist,
  bounded by `maxBlobSize = 512MB` (`backend_block.go:30-32`): `FetchTagNames`
  with no conditions (line 1167-1226) and `Validate` (line 1239-1263). Neither is
  on the TraceQL search (`Fetch`) hot path that is actually timing out — they
  serve `/api/search/tags` (no-filter case) and block-open validation
  respectively. Flagged as a secondary, unrelated risk (see Open Questions).
- The **`size=NNNNN`** parameter visible in the query-frontend → querier HTTP
  request logs (e.g. `size=63268990` for dc40aa41) is **not** a measurement of
  bytes fetched — it is `BlockMeta.Size_` (the block's on-disk file size),
  unconditionally echoed by the frontend's sharding request builder
  (`pkg/api/http.go:463,752`, `urlParamSize`) on every single per-block request,
  win or lose, fetch-heavy or fetch-free. **This is almost certainly what the
  session's "60-65MB fetches observed in logs" finding actually was** — a
  misread of a metadata echo parameter, not evidence of an actual 60MB
  `GetObject`. This significantly undercuts the original working hypothesis.

### Correlating with real querier logs (the actual root cause)

Pulled `querier-7576cbd764-r2gkz` logs (`kubectl logs -n tempo-dev-test-03
querier-... --since=3h`) filtered to the 5 implicated block IDs. Every single
failing request follows this pattern:

```
level=info ... caller=value_index_query.go:139 msg="vblockpack: index fetch: coverage found" block=dc40aa41-... tenant=11638 files=465
level=warn ... caller=server.go:2286 ... msg="GET /querier/tempo/api/search?...blockID=dc40aa41-...&size=63268990..." (500) 33.317131983s Response: "vblockpack Fetch: context deadline exceeded"
```

- `files=465` is `ValueIndexBuildStats.FilesRead` — the count of **value-index**
  files (small per-column/per-time-bucket postings files under
  `<tenant>/indexes/...`) downloaded while building the index source for this
  query's predicate, **not** trace-data blocks. The "coverage found" log line is
  emitted only *after* `vibuilder.BuildSource` fully returns — meaning the whole
  33s-2m38s request duration is being spent inside `BuildSource`/`downloadAll`,
  not in trace-block I/O at all.
- A second predicate variant (`span.http.response.status_code = 200 || 201`)
  repeatedly fails even earlier, at **discovery**, not download:
  `msg="vblockpack: index fetch: build source error" ... err="vibuilder: discover
  span.http.response.status_code: context deadline exceeded"` — a cold-cache S3
  LIST for that column's index directory itself blew the deadline.
- Durations observed across repeated attempts on the same 5 blocks: 33.3s,
  33.4s, 33.8s, 52-53s, 1m34s-1m44s, 2m11s-2m38s (escalating with load/GC/other
  contention, but consistently far past any reasonable per-block budget).

**Root cause, confirmed via code read:** `vibuilder.downloadAll` (vendored at
`tempo/vendor/github.com/grafana/blockpack/internal/modules/vibuilder/
builder.go:420-440`) downloads value-index files with a **fully serial, 
unbatched `for` loop, zero concurrency**:

```go
func downloadAll(store FileStore, keys []string) ([][]byte, int64, error) {
    ...
    for _, key := range keys {
        data, err := readWhole(store, key)   // no goroutines, no errgroup
        ...
    }
}
```

`readWhole` (`builder.go:442-459`) does **two sequential HTTP round trips per
file**: `store.Size(key)` (a `StatObject` call, `minioVIStore.Size`,
`value_index_query.go:213-219`) then `store.ReadAt(key, buf, 0)` (a ranged
`GetObject`, `value_index_query.go:225-239`) — never combined into one call.
With `files=465` and 2 round trips each, that is up to **930 sequential S3 API
calls** at typical 50-100ms latency = 46-93 seconds, just for one leaf column's
files — and `BuildSource`'s own leaf loop (`builder.go:116-137`,
`for i := range leaves { ... lookupColumn(...) ... }`) processes each predicate
leaf **serially** too, so a 2-condition query (e.g. `resource.service.name = X
&& span.http.request.method = Y`) pays this cost twice, back to back. This
fully and precisely explains both the magnitude and the escalating pattern of
the observed timeouts, and matches the exact log sequence (all time spent before
"coverage found" logs, immediately followed by context-deadline failure).

`vibuilder` is a brand-new package (`NOTES.md`, `NOTE-VI-036`, added
2026-06-30 — three days before this incident). Its `NOTES.md` documents the
discover/download/predicate-filter flow, the coverage contract, and the
not-found-file skip policy in detail, but **never mentions concurrency or
rate-limiting as a deliberate design choice** — this reads as an oversight in a
very recently landed feature, not an intentional serialization for backend
protection.

The file-*discovery* half (`valueindex/filecache.go`, `IndexFileCache`) is
correctly cached (30s TTL, background refresh, O(1) warm lookups) — this is NOT
part of the bug for warm columns. The one observed discovery timeout
(`span.http.response.status_code`) is a **cold-cache** miss: the first
`FilesForTimeRange` call for a not-yet-cached column does a synchronous S3
`List` (`filecache.go:266-287`) with no explicit timeout/pagination bound of its
own — if that column's index directory has accumulated many objects (e.g. an L0
compaction backlog), the first query pays for a full listing under whatever
context deadline remains, which by that point had already been mostly consumed
by the first leaf's serial download loop.

### Spec-Driven Modules in Scope

**`tempo/vendor/github.com/grafana/blockpack/internal/modules/vibuilder/`** —
spec-driven (has `NOTES.md`; no `SPECS.md`/`TESTS.md`/`BENCHMARKS.md` present
yet for this new package).
- Key documented invariants (NOTE-VI-036/039/041): per-leaf coverage semantics
  (`Add` even when empty = coverage, not fallback), a real download/discovery
  error must abort the whole build and fall back to a full scan (except 404 =
  skip), and `RecordFileIO` must keep accumulating stats regardless of `Add`
  ordering. **None of these invariants say anything about serial vs. concurrent
  downloads** — parallelizing `downloadAll` and the leaf loop does not conflict
  with any stated contract, as long as the "any non-404 error aborts the whole
  build" semantics and the accumulated `FilesRead`/`BytesRead` stats are
  preserved under concurrent execution (needs a mutex or atomic instead of the
  current unsynchronized local accumulation, since `errgroup`-style parallel
  fetch would need thread-safe stat aggregation).
- `tempo/vendor/github.com/grafana/blockpack/internal/modules/blockio/` (has
  full `SPECS.md`/`NOTES.md`/`TESTS.md`/`BENCHMARKS.md`) is NOT implicated —
  its "single I/O per block, no per-column reads" invariant is about the
  trace-data block read path (`GetBlockWithBytes`/`ReadGroup`), which this
  investigation found is already ranged/coalesced correctly and is not the
  cause of the timeouts.
- No fix has been written yet (diagnosis-only phase); if/when `vibuilder`
  gets a concurrency fix, its `NOTES.md` should get a new dated entry
  documenting the change and the chosen concurrency bound.

## 2026-07-02 17:20:00 - Approaches Considered

### Approach 1: Parallelize `vibuilder.downloadAll` with bounded concurrency

**Description:** Replace the serial `for _, key := range keys { readWhole(...) }`
loop in `builder.go:420-440` with a bounded-concurrency fan-out (e.g.
`golang.org/x/sync/errgroup` with `SetLimit(N)`, N in the 16-32 range to match
typical S3/minio client connection pool sizing), collecting `[][]byte` results
in key order and aggregating `totalBytes` atomically. Apply the same bounded
concurrency to the **leaf loop** in `BuildSource` (`builder.go:116-137`) so
independent predicate columns download in parallel too, not just files within
one column.

**Pros:**
- Directly targets the measured, confirmed bottleneck (465 serial round trips).
- Small, contained change — one file (`builder.go`), no format/API changes, no
  ToC/wire format impact, no touch to the `blockio` package's I/O invariants.
- Preserves all documented `NOTES.md` contracts (abort-on-real-error, 404-skip,
  coverage-vs-fallback semantics) — concurrency doesn't change the *decision*
  logic, only the *scheduling* of independent I/O.
- Expected impact: ~465 serial round trips → ~465/N parallel batches; with
  N=32 that's roughly a 30x reduction in wall-clock I/O time for the download
  phase alone, likely bringing a currently 33s-2m38s query well under typical
  query timeouts.

**Cons:**
- Needs care with error aggregation under concurrency (first real error should
  still abort/fall back exactly as today — a canceled context for in-flight
  goroutines on first error, matching `errgroup`'s default behavior).
- Increases peak concurrent connections to the object store per in-flight query;
  needs to be bounded (not "unlimited fan-out") to avoid conflict with any
  connection-pool sizing on the S3/minio client, and needs load-testing across
  concurrently-running queries (many blocks in flight during a sharded search).
- Does not fix the separate cold-cache-LIST-times-out failure mode
  (`vibuilder: discover ...: context deadline exceeded`) — a second, smaller fix
  is still needed there (Approach 2).

### Approach 2: Merge `Size()` + `ReadAt()` into a single GetObject call

**Description:** `readWhole` currently does `store.Size(key)` (a `StatObject`)
then `store.ReadAt(key, buf, 0)` (a ranged `GetObject`) — two round trips to
learn a size we could get from the *same* GetObject response headers (Content-
Length) in one call. Change `ValueIndexFileStore`/`FileStore`'s contract (or add
a single `ReadWhole(key) ([]byte, error)` method) so `minioVIStore` does one
unranged `GetObject` and reads until EOF, using the response's advertised size
only for buffer pre-allocation.

**Pros:**
- Independently halves the round-trip count regardless of concurrency —
  compounds with Approach 1 (930 round trips → 465 with this change alone, or
  ~465/N with both combined).
- Value-index files are described as "small" in existing code comments
  (`value_index_query.go:222-224`, "one column directory's merged postings") —
  reading them whole without first stat'ing is low-risk.

**Cons:**
- Touches the `FileStore`/`ValueIndexFileStore` interface shape — a public API
  surface in `valueindex_query.go` (`blockpack.ValueIndexFileStore = vibuilder.
  FileStore`), so this is a small blockpack API change requiring a vendor bump,
  not purely tempo-local. Needs the "do not add new public API surface without
  explicit permission" rule in blockpack's CLAUDE.md to be respected (ask before
  implementing).
- Smaller impact alone than Approach 1; best treated as a complementary
  follow-up rather than the primary fix.

### Approach 3: Cap/bound total value-index file count per query and/or investigate the "465 files" figure itself

**Description:** Independent of the concurrency bug, ask whether 465 files for
a single (2-hour-window, 2-leaf-column) query is itself abnormally high — e.g.
a value-index compaction backlog (L0 files not being merged into L1/L2 promptly
by the `value-index-compactor-*` StatefulSet, 10 replicas observed running in
tempo-dev-test-03) inflating the per-query file count well past what a healthy
compaction cadence would produce. If so, add a query-time cap (skip/sample
oldest-first beyond N files with a documented approximation) as a safety valve,
and/or prioritize compactor throughput as a separate workstream.

**Pros:**
- Addresses a potential root cause *behind* the root cause — even a parallelized
  downloader degrades linearly with file count; if the count keeps growing
  (backlog), Approaches 1-2 only buy headroom, not a permanent fix.
- A hard cap is a cheap safety net against pathological cases (e.g. a
  misconfigured retention window or a stalled compactor) turning into query
  failures instead of graceful degradation.

**Cons:**
- Requires further investigation not completed in this pass (did not inspect
  `value-index-compactor-*` pod logs/backlog metrics — flagged as an open
  question below due to time budget).
- A hard cap trades correctness (could silently under-cover the query) for
  latency — needs careful design to remain "coverage or explicit fallback,
  never silent under-count" per `NOTE-VI-041`'s existing fail-safe philosophy.
- Does not fix today's incident by itself if 465 is actually a normal/expected
  count for this cardinality — Approach 1 is required regardless.

## 2026-07-02 17:22:00 - Recommendation

### Chosen Approach: Approach 1 (parallelize `vibuilder.downloadAll` + the leaf
loop) as the immediate fix, with Approach 2 as a fast-follow and Approach 3 as a
parallel investigation track — NOT a redesign of the trace-block fetch path.

**Rationale:**

The original working hypothesis ("querier fetches whole 60-65MB `.blockpack`
FILES instead of internal BLOCKs") is **not supported by the evidence** and
should be retired:
- Internal blocks are small (~91KB avg, confirmed via `analyze-block` against a
  real downloaded 60MB file: 644 blocks, 57.18MB of column payload).
- The trace-block fetch path (`ReadBlockRaw`/`ReadGroup`/`CoalescedGroups`) is
  already correctly using ranged S3 GETs, capped at 8MB per coalesced request
  (`AggressiveCoalesceConfig`) — there is no single-GetObject-for-the-whole-file
  bug on this path.
- The "60-65MB fetch" observed in the background note is best explained by the
  `size=NNNNN` URL parameter (`BlockMeta.Size_`, echoed on every request
  regardless of actual I/O), not a real transfer measurement.
- The real, log-confirmed, code-confirmed bottleneck is the value-index
  build phase: a brand-new (`2026-06-30`), currently fully serial download loop
  (`vibuilder.downloadAll`) issuing up to ~930 sequential round trips per query,
  matching both the magnitude (33s-2m38s) and shape (all time spent before
  "coverage found" logs) of the observed timeouts.

Approach 1 is the smallest, most targeted, most spec-compliant fix: it changes
only *how* already-decided, independent I/O is scheduled, not *what* is
fetched or *when* the index path falls back — so it carries very low
regression risk relative to `vibuilder`'s existing documented invariants.

**Implementation Strategy (for the later PLAN/EXECUTE phase, not done here):**
1. In `vibuilder/builder.go`, replace `downloadAll`'s serial loop with a bounded
   `errgroup.Group` (or a simple worker-pool) fan-out over `keys`, preserving
   result order (write into a pre-sized `[][]byte` by index, not via append) and
   the exact abort-on-real-error / skip-on-404 semantics.
2. Apply the same bounded concurrency to `BuildSource`'s leaf loop over
   `leaves` (`builder.go:116-137`), and to `lookupColumnAll`'s per-type-bucket
   loop (`builder.go:356-395`), guarding `src.Add`/`src.RecordFileIO` calls
   appropriately if `SliceValueIndexSource` is not already safe for concurrent
   writers (needs to be checked — the module's `NOTES.md` does not currently
   document any synchronization; may need a mutex added there too).
3. Pick a concurrency bound conservatively (e.g. a package-level constant or a
   parameter threaded from tempo's config, defaulting to something like 16-32)
   rather than unbounded `go func()` per key, to avoid connection-pool
   exhaustion against the S3/minio backend during concurrent multi-block
   sharded searches.
4. Add a benchmark/test asserting wall-clock scaling (e.g. N files at simulated
   latency L should take roughly L × ceil(N/concurrency) rather than L × N),
   per blockpack's `make precommit` and `BENCHMARKS.md` conventions for this
   module once it graduates to having one.
5. Separately (Approach 3), pull `value-index-compactor-*` pod logs/metrics to
   determine whether 465 files/query is a symptom of an L0→L1 compaction
   backlog; if so, treat compactor throughput as a related but distinct
   workstream.

**Key Decisions:**
- Do not touch the `blockio`/trace-block read path (`GetBlockWithBytes`,
  `ReadGroup`, `CoalesceBlocks`) — it is not implicated and already carries a
  fully-documented `SPECS.md`/`NOTES.md`/`BENCHMARKS.md` contract ("single I/O
  per block, ranged, coalesced ≤8MB") that this investigation confirmed is
  intact and correct.
- Treat the two whole-object `StreamReader` reads (`FetchTagNames` no-condition
  path, `Validate`) as out of scope for this fix — they're real but on a
  different, non-search code path; worth a follow-up ticket, not blocking this
  fix.
- `maxBlobSize` (512MB) sanity-check requested in the original prompt: it is
  **orthogonal** to this incident — it only bounds the two whole-object
  `StreamReader` paths above, not the search/Fetch hot path, and is not
  implicated in the observed timeouts. No evidence it needs to change.

**Risks Identified:**
- **Concurrent stat aggregation correctness**: `SliceValueIndexSource.RecordFileIO`
  and per-leaf `src.Add` calls must be verified thread-safe (or made so) before
  parallelizing the leaf loop — this needs a code read of `executor/
  metrics_trace.go` (`ValueIndexBuildStats`) not yet done in this pass.
- **Backend connection pressure**: parallelizing 465 downloads × many
  concurrently-sharded blocks across a search request could multiply peak
  concurrent S3 connections significantly; needs a bound and ideally a load
  test against tempo-dev-test-03's actual minio/S3 backend before rollout.
- **The 465-files number itself is unverified as "expected"**: if it reflects a
  compaction backlog rather than steady-state, Approach 1 buys time but does
  not fix the underlying growth; needs the Approach 3 follow-up.
- **Vendor lag**: any fix lands in the `blockpack` repo first, then needs a
  vendor bump into `tempo-mrd` (per this session's constraints: blockpack main
  branch only, no push without being asked; tempo-mrd stays on `agentic-tempo`).

**Open Questions:**
- Is `SliceValueIndexSource` (holding `Add`/`RecordFileIO`) safe for concurrent
  writers today? Not checked in this pass — must be answered before
  implementing Approach 1's leaf-loop parallelization.
- Are `value-index-compactor-*` pods keeping up, or is there an L0→L1/L2
  backlog inflating the per-query file count in `<tenant>/indexes/...`? Not
  checked in this pass (10 compactor replicas were observed running healthy —
  no restarts — but backlog size/lag was not measured).
- Does the querier's per-request context deadline (whatever value tempo-dev-
  test-03's query-frontend/querier is configured with) leave any reasonable
  budget once index-file download is fixed, or is there a second bottleneck
  waiting behind this one (e.g. the actual `QueryTraceQLFromIndex` block-fetch
  stage, never reached in the failing traces pulled here since the timeout
  occurred earlier, during `BuildSource`)? Should be re-measured once Approach
  1 lands.
- Was the earlier querier OOMKill (16:24:31-16:26:40 EDT, mentioned in the
  original background) actually caused by this same code path (e.g. many
  concurrent in-flight `[][]byte` value-index file buffers accumulating across
  many simultaneously-timing-out block Fetches, each retrying), or is it
  unrelated? Not correlated against pod memory metrics in this pass.

## 2026-07-02 17:23:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Root cause confirmed:** Sequential, unbatched, 2-round-trip-per-file value-index
download loop in `vibuilder.downloadAll`/`readWhole` (blockpack, vendored into
tempo-mrd), NOT a whole-file-vs-block trace-data fetch bug. The original working
hypothesis is refuted by direct measurement (real per-block sizes ~91KB avg,
ranged/coalesced S3 reads already correctly scoped and capped at 8MB) and by log
correlation (all timeout duration occurs before "coverage found", inside
`BuildSource`, matching the serial-download math almost exactly).
**Recommendation:** Approach 1 (bounded-concurrency fan-out in
`vibuilder.downloadAll` + `BuildSource`'s leaf loop), Approach 2 as a fast-follow,
Approach 3 as a parallel investigation into whether the 465-file count itself
reflects a compaction backlog.
**Next Phase:** PLAN

Ready for workflow-planner agent to create detailed implementation plan (in
blockpack first, then vendor-bump into tempo-mrd per this session's repo rules).
