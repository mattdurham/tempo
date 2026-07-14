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

---

# Task: Make VI/cube usage-recording/backfill machinery backend-agnostic (not S3-only)

## 2026-07-11 10:45:00 - Task Received

Two repos, both worked on directly (no worktrees, per standing convention):
- blockpack: `/home/mdurham/source/blockpack_collection/blockpack` (branch main)
- tempo: `/home/mdurham/source/blockpack_collection/tempo` (branch agentic-tempo)

**Requirement (explicit user ruling, verbatim):** "It should work on any
backend." / "Local backend should also work just fine, in fact it should run on
local backend." / "Ideally we should be able to integration test this locally."

**Confirmed-this-session root cause handed in:** `tempo/tempodb/tempodb.go`
gates the entire VI/usage/cube feature behind `if cfg.Backend == backend.S3`
(two gates: write-path and read-path), and 11 vblockpack functions take
`*s3backend.Config` directly and construct raw `*minio.Client`s. blockpack's
registry types need conditional-PUT/ETag semantics that tempo's own
`backend.RawReader`/`RawWriter` do not provide. Required design: a
backend-agnostic conditional-write primitive (content-hash-as-ETag + mutex +
atomic rename for local; native primitives for GCS/Azure), replacing the
S3-specific minio adapters as the default path, plus a real local-backend
integration test through the frontend → RoundTrip → RecordUsageIfNoIndexCoverage
→ trigger → backfill chain.

Starting brainstorm process (verifying every cited file:line against current
source, then researching blockpack's exact interfaces, tempo's raw.go/local.go/
gcs.go/azure.go/s3.go, and any existing conditional-write precedent in tempo).

## 2026-07-11 11:00:00 - Research Findings

### 1. The two gates — verified, line numbers shifted slightly

`tempo/tempodb/tempodb.go`:
- **Write-path gate**, line 297: `if cfg.Block.Blockpack.ValueIndexEnabled &&
  cfg.Backend == backend.S3 {` — calls `vblockpack.ConfigureValueIndex`,
  `ConfigureCubeManager` (per `CubeTenants`), `ConfigureCubeQueryPath`,
  `ConfigureViWatermarkCache`.
- **Read-path gate**, line 326: `if cfg.Backend == backend.S3 {` — calls
  `newMinioForValueIndex` + `ConfigureValueIndexQuery`, `ConfigureViUsage`,
  `ConfigureViWatermarkCache` (again, for the querier-side watermark gate).
- Both gates carry a **2026-07-11-dated comment** (lines 289-296, "No
  cfg.Block/cfg.S3 nil-checks here (2026-07-11 cleanup)") confirming this is
  the same-day precedent the prompt referenced — the write-gate's comment is
  the exact model to follow when rewriting the S3-only condition itself, not
  just the nil-check removal it documents.
- All 11 function signatures in the prompt's list were re-verified verbatim
  against current source (`grep -n "^func Configure\|^func Run\|^func launch\|^func newMinio\|^func newViBackfillMinioClient\|^func LoadCubeEntry"` across `valueindex.go`,
  `vi_usage_hook.go`, `vi_watermark_cache.go`, `vi_backfill.go`, `cubemanager.go`,
  `cubequerypath.go`, `cube_backfill.go`) — every signature and line number in
  the prompt is still accurate; nothing has moved since the prompt was written.

### 2. `New()`'s local variables — where rawR/rawW live (answers the prompt's open question precisely)

`tempodb.go:202-223`:
```go
func New(cfg *Config, cacheProvider cache.Provider, logger gkLog.Logger) (Reader, Writer, Compactor, error) {
    var rawR backend.RawReader
    var rawW backend.RawWriter
    var c backend.Compactor
    ...
    switch cfg.Backend {
    case backend.Local:  rawR, rawW, c, err = local.New(cfg.Local)
    case backend.GCS:    rawR, rawW, c, err = gcs.New(cfg.GCS)
    case backend.S3:     rawR, rawW, c, err = s3.New(cfg.S3)
    case backend.Azure:  rawR, rawW, c, err = azure.New(cfg.Azure)
    }
    ...
    if cacheProvider != nil {
        ...
        rawR, rawW, err = backend_cache.NewCache(&cfg.BloomCacheCfg, rawR, rawW, cacheProvider, logger)  // line 244
    }
    r := backend.NewReader(rawR)   // line 250
    w := backend.NewWriter(rawW)   // line 251
    rw := &readerWriter{ rawR: rawR, ... }   // rawW is NOT retained on the struct
    ...
    // <-- VI/cube gates (lines 297, 326) execute HERE, still inside New()'s
    //     function body, with rawR/rawW as live local variables.
    return rw, rw, rw, nil   // line 367
}
```
Both `rawR` (`backend.RawReader`) and `rawW` (`backend.RawWriter`) are **plain
local variables**, populated once by the 4-way backend switch and potentially
re-wrapped by `backend_cache.NewCache` (line 244) if a cache provider is
configured — and both are still directly in scope, unwrapped-or-cached
consistently, at the exact point (lines 297/326) where the new backend-agnostic
gates need to construct an ObjectStore/ObjectPutter adapter. No struct-field
plumbing is needed for this — the new `vblockpack.Configure*` call sites can
take `rawR`/`rawW` (or a small wrapper built from them) as direct arguments,
exactly parallel to how `cfg.S3` is threaded today.

**Cache-safety note:** `backend.RawReader.Read`/`ReadRange` only attempt
caching when a non-nil `*backend.CacheInfo` is passed (confirmed:
`tempodb/backend/cache/cache.go:195`, `if cacheInfo == nil { ... skip cache }`).
Every call site in the new design that reads registry files must pass `nil`
for `cacheInfo` (matching all existing non-block-data `RawReader` calls in this
codebase) — caching a mutable registry/index.json read would be actively
dangerous (a stale cached read defeats the whole point of ETag/version-based
optimistic-concurrency detection). This is a correctness-critical convention
the planner must state explicitly, not just an incidental default.

### 3. blockpack's real interfaces — narrower than the prompt's sketch (important scope correction)

Grepped every `ObjectStore`/`ObjectPutter`/`CubeObjectStore` definition in
blockpack. There are **three genuinely different storage surfaces in play**,
not one:

**(a) `ObjectPutter` — simple, unconditional PUT.** Used for L0 file writes
(value-index L0, cube L0). No ETag, no conditional semantics at all:
```go
// blockpack/valueindex_l0write.go:41, blockpack/internal/modules/valueindexconsumer/service.go:28,
// blockpack/internal/modules/cube/accumulator.go:158 (three near-identical copies)
type ObjectPutter interface {
    Put(path string, data []byte) error
}
```

**(b) `ObjectStore` (aka `CubeObjectStore`) — Get + ConditionalPut with ETag.**
Used **only** by the two tiny JSON registry files (viusage's usage index and
cube's `index.json`), NOT by any bulk data write:
```go
// blockpack/internal/modules/viusage/registry.go:37-38, blockpack/internal/modules/cube/registry.go:35-38
// (blockpack root re-exports both as type aliases: ObjectStore = viusage.ObjectStore
//  (valueindex_usage.go:71), CubeObjectStore = cube.ObjectStore (cube_ingest.go:62))
type ObjectStore interface {
    Get(ctx context.Context, path string) (data []byte, etag string, err error)
    ConditionalPut(ctx context.Context, path string, data []byte, etag string) error
}
```
`ErrConflict`/`CubeErrConflict` signal a 412-equivalent; `ErrNotFound`/
`CubeErrNotFound` signal a genuine miss (must NOT be inferred from an
empty-shaped `(nil, "", nil)` return — both registries' doc comments call this
out explicitly as a previously-real bug, NOTE-VIUSAGE-10).

**(c) A read-only "file store" surface — List/Get/Size/ReadAt, no writes at
all.** Used by the value-index *query-time* postings lookup
(`vibuilder.FileStore` / `blockpack.ValueIndexFileStore`, backing
`minioVIStore` in `value_index_query.go:460-534`) and separately by
`cubequerypath.go`'s own inline `listObjects`/`getObject` helpers
(`cubequerypath.go:368-390`) for reading cube L0/L1 files. Neither of these
needs conditional-write semantics — they are pure reads.

**This materially narrows the task.** "Conditional-write ETag emulation" is
only a real design problem for **(b)** — two small JSON index files per
tenant (`<tenant>/cubes/index.json`, and viusage's per-tenant usage index).
Everything else — L0 writes (a), and VI-postings/cube-file reads (c) — needs
only a thin, non-conditional wrapper over `backend.RawWriter.Write` /
`backend.RawReader.Read`+`List`, which is comparatively trivial and carries no
concurrency-correctness risk.

### 4. The exact current S3 ConditionalPut implementation — confirms the prompt's ETag claim

`cubemanager.go:54-101` (`minioObjectStore`) and `vi_backfill.go:39-89`
(`viUsageObjectStore`) are near-identical minio adapters:
```go
func (s *minioObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
    obj, err := s.client.GetObject(ctx, s.bucket, path, minio.GetObjectOptions{})
    // ... 404 -> blockpack.CubeErrNotFound ...
    data, err := io.ReadAll(obj)
    info, err := s.client.StatObject(ctx, s.bucket, path, minio.StatObjectOptions{})  // 2nd round trip for ETag
    return data, info.ETag, nil
}
func (s *minioObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
    opts := minio.PutObjectOptions{ContentType: "application/json"}
    if etag != "" {
        opts.SetMatchETag(etag)   // real S3 conditional PUT via If-Match header
    }
    _, err := s.client.PutObject(ctx, s.bucket, path, bytes.NewReader(data), int64(len(data)), opts)
    // 412 -> blockpack.CubeErrConflict
}
```
Confirms: S3's ETag really is used as a literal opaque conditional-write token
(minio's `SetMatchETag` → real HTTP `If-Match`), exactly as the prompt claimed.
**One pre-existing, non-regressing gap worth flagging to the planner:** when
`etag == ""` (the "object doesn't exist yet" case from a fresh `Load()`),
`ConditionalPut` does **not** call `SetMatchETag` at all — it's an
unconditional PUT, not a true create-if-not-exists (`If-None-Match`). This is
existing S3 behavior today (a latent lost-update window on first-create races
between two tenants' first cube/usage registration), not something the
backend-agnostic redesign introduces — but any generic replacement should
either preserve this exact (already-shipped) semantic or explicitly improve it
as a deliberate, called-out decision, not an accidental behavior change.

The consumer side (`blockpack/internal/modules/cube/registry.go:115-153`,
`Add`/`Remove`/`UpdateWatermarks`) is a clean **Load → mutate → ConditionalPut,
retry on ErrConflict, up to 5 attempts with 50ms-doubling backoff** loop —
`viusage/registry.go` mirrors this exactly. This retry loop is
backend-agnostic already (it only calls the `ObjectStore` interface); it needs
no changes regardless of which adapter backs it.

### 5. Existing backend-agnostic conditional-write precedent in tempo — a major find

`tempo/tempodb/backend/versioned.go` defines exactly the kind of primitive the
task needs, **already used in production** by
`modules/overrides/userconfigurable/client/` (tenant override limits):
```go
type VersionedReaderWriter interface {
    RawReader
    WriteVersioned(ctx context.Context, name string, keypath KeyPath, data io.Reader, size int64, version Version) (Version, error)
    DeleteVersioned(ctx context.Context, name string, keypath KeyPath, version Version) error
    ReadVersioned(ctx context.Context, name string, keypath KeyPath) (io.ReadCloser, Version, error)
}
```
All 4 backends implement `NewVersionedReaderWriter`/`NewNoConfirm` equivalents,
but **correctness varies sharply by backend** (verified by reading every
implementation):
- **GCS** (`gcs.go:397-452`) — genuinely atomic: `WriteVersioned` uses
  `storage.Conditions` (`createPreconditions(version)`) passed to the GCS
  object writer natively; `Version` is the object's real `Generation` number.
  This is a correct, race-free conditional write.
- **S3** (`s3.go:582-610`) and **Azure** (`azure.go:341-365`) — both are
  **racy emulations**, explicitly documented as such: `// Note there is a
  potential data race here because S3 does not support conditional headers.`
  (s3.go:583) and `// TODO use conditional if-match API` (azure.go:342). Both
  do a plain `ReadVersioned` → compare `Version` in Go code → unconditional
  `Write` — a genuine read-then-write race window, not a real CAS.
  **This S3 comment is factually stale/wrong** — blockpack's own
  `minioObjectStore.ConditionalPut` (finding #4 above) proves S3 *does*
  support real conditional PUT via `If-Match`/ETag; tempo's own
  `VersionedReaderWriter.WriteVersioned` for S3 simply never adopted it.
- **Local** (via `backend.NewFakeVersionedReaderWriter`, `versioned.go:40-63`,
  wired in `client.go:114-126`) — **no correctness at all**: `WriteVersioned`
  ignores the passed `version` argument entirely and calls plain `Write`
  unconditionally, always returning `VersionNew`. `client.go:139-144` logs an
  explicit runtime warning for Local/S3/Azure: `"versioned backend requests
  are best-effort ... concurrent requests ... might cause data races"`.

**Conclusion for the planner:** tempo's own most-similar existing precedent
(`VersionedReaderWriter`) is **not safe to reuse as-is** for the VI/cube
registries — its Local path has zero correctness guarantees and its S3/Azure
paths have a real, acknowledged race window that blockpack's existing
`minioObjectStore` (S3-only, today) does NOT have. A new implementation must
be strictly better than `VersionedReaderWriter`'s Local/S3/Azure paths, not a
thin wrapper around them — at minimum for Local, since that's this task's
priority backend.

### 6. Local backend already has a real atomic-write primitive to build on

`tempo/tempodb/backend/local/local.go:76-112`:
```go
// WriteAtomic writes via a temp file + os.Rename so concurrent writers to
// the same path never leave a partial or interleaved file on disk.
func (rw *Backend) WriteAtomic(ctx context.Context, name string, keypath backend.KeyPath, data io.Reader, _ int64) error {
    ...
    tmp, err := os.CreateTemp(blockFolder, "."+filepath.Base(name)+".tmp-*")
    ...
    if err := os.Rename(tmpName, finalName); err != nil { return err }
    ...
}
```
This is **not** part of the `backend.RawWriter` interface (it's an extra
method on the concrete `*local.Backend` type) but is already real,
already-shipped, already used in production by
`modules/livestore/instance_search.go:946`. It is the correct primitive to
build the local conditional-write path on — far better than
`FakeVersionedReaderWriter`'s unconditional plain `Write`, and matches the
prompt's own suggested design (atomic write via temp+rename) almost exactly,
with zero new code needed for the atomicity half of the problem — only the
"verify hash still matches" + mutex half needs to be added.

`local.Backend.Write` (the plain `RawWriter.Write`, line 51-74) is, by
contrast, **not** atomic — `os.Create` + `io.Copy` directly to the final path,
so a concurrent reader could observe a partially-written file. Confirms the
prompt's claim exactly: today's plain local `Write` is non-atomic; the fix
must route through `WriteAtomic` (already present) rather than adding new
atomic-write logic from scratch.

### 7. List() semantics — a genuine, confirmed backend-agnostic-ification gap

`backend.RawReader.List(ctx, keypath)` is documented as "returns all objects
one level beneath the provided keypath" (`raw.go:64`). Verified both
implementations:
- **Local** (`local.go:174-194`): `os.ReadDir` filtered to `f.IsDir()` only —
  returns child **directory names**, not files.
- **S3** (`s3.go:365-394`): `ListObjects(bucket, prefix, marker, "/", 0)` with
  `delimiter="/"` — returns `res.CommonPrefixes` only, i.e. also
  directory-like prefixes, not individual object keys.

These two are **mutually consistent** (both are "one level of prefixes/dirs"),
so `backend.RawReader.List` is fine as-is for anything that only needs
one-level enumeration.

**However**, the VI/cube code that would need to be backend-agnostic-ified
does NOT use this semantic today — it uses **recursive, flat key listing**:
- `value_index_query.go:466-476` (`minioVIStore.List`):
  `minio.ListObjectsOptions{Prefix: prefix, Recursive: true}` — returns every
  object key under a prefix, at any depth, in one call.
- `cubequerypath.go:368-378` (`listObjects`): identical
  `Recursive: true` pattern.

`backend.RawReader.List` **cannot express this** — it would take repeated
one-level calls (a manual recursive walk) to reproduce flat recursive listing
against Local/GCS/Azure. This is a real, load-bearing design gap the planner
must resolve, not a detail: value-index postings discovery and cube file
discovery are both latency-sensitive (this same session's earlier
investigation found a *different* VI code path, `vibuilder.downloadAll`,
degrading from 33s to 2m38s purely from serial-round-trip count — this
codebase is demonstrably sensitive to "one extra round trip per listing
level" style regressions). A naive recursive walk-of-one-level-List calls adds
one round trip per directory depth per discovery call; whether that's
acceptable depends on how deep/wide the `<tenant>/indexes/...` and
`<tenant>/cubes/...` key layouts actually are in practice (needs checking
against `valueindex`'s actual file-layout convention — not done in this pass,
flagged as an open question).

### 8. `RecordUsageIfNoIndexCoverage` (the frontend call site) — verified clean

`modules/frontend/vcnt_fetch.go:244` calls `vblockpack.RecordUsageIfNoIndexCoverage(ctx, tenant, prog, dedicated, minTS, maxTS, time.Now())`.
The function itself (`vi_usage_hook.go:355-392`) takes **no S3-specific
parameter at all** — it reads through `getViUsageRecorder()` and
`getValueIndexQueryReader()` process-level singletons, both of which are
populated by the gates in `tempodb.go`. **No signature change needed here** —
confirms the prompt's "verify" ask came back clean; this call site does not
thread anything S3-specific and needs no edits.

### Navigator Knowledge

Navigator MCP tool was not available in this environment (no
`mcp__navigator__consult` tool exposed to this agent); skipped per the
brainstormer's fallback instruction. lth (the local memory/prompt tool) served
the equivalent role via `~/bin/lth prompt` — its returned techniques
("atomic read-modify-write using ETags/version markers + automatic retry",
"decompose complex systems into stateless composable units") both align with
and are already reflected in blockpack's existing `Registry.Add/Remove/
UpdateWatermarks` retry-loop design (finding #4) — no new guidance beyond
what direct source-reading already surfaced.

### Spec-Driven Modules in Scope

- **blockpack: `internal/modules/viusage/`** — spec-driven (per this
  session's context, consulted via blockpack's MCP tools per its CLAUDE.md
  convention rather than reading NOTES.md directly). Key invariant confirmed
  independently via the exported `ObjectStore`/`ErrConflict`/`ErrNotFound`
  doc comments (`registry.go:22-47` equivalent structure to cube's, quoted in
  finding #3): `ErrNotFound` must never be inferred from an empty-shaped
  return; only a real sentinel error counts.
- **blockpack: `internal/modules/cube/`** — spec-driven, same pattern; direct
  quote from `registry.go:3-6`: "SPEC-CUBE-014 — Registry maintains the
  tenant-level index of active cubes ... Concurrent writes use S3 conditional
  PUT (If-Match ETag) with exponential-backoff retry (up to 5 attempts)."
  **This spec text is itself S3-specific wording** — if/when the backend is
  generalized, this SPEC-CUBE-014 line (and viusage's equivalent) should be
  updated to describe the abstraction backend-agnostically (e.g. "conditional
  PUT via the configured ObjectStore, ETag-equivalent per backend") rather
  than hardcoding "S3" in the spec text — a documentation follow-up for
  whichever agent implements this, not a blocker.
- **blockpack: `internal/modules/valueindex/`** — file layout/hashing, not
  directly touched by this design (the L0 write path only needs `ObjectPutter`,
  finding #3a) but its on-disk key-layout convention (`<tenant>/indexes/
  <colHash>/...`) determines how deep the recursive-listing gap (finding #7)
  actually bites — not fully explored this pass.
- **tempo: `tempodb/encoding/vblockpack/`** — no `SPECS.md`, but extensive
  inline `NOTE-VI-*`/`R1-R13` doc-comment rulings (finding #4's R1, R6, R9,
  R12 references seen in `vi_backfill.go`'s header comment and elsewhere)
  encode real prior design rulings. None of the rulings read this pass
  conflict with backend-agnostic-ifying the *construction* of the
  `ObjectStore`/`ObjectPutter` adapters — they govern watermark/backfill
  *behavior*, which is orthogonal to *which backend* supplies the bytes.

## 2026-07-11 11:20:00 - Approaches Considered

### Approach 1: New generic `ObjectStore`/`ObjectPutter` adapter wrapping `backend.RawReader`/`RawWriter`, content-hash-as-ETag for Local (and reused generically for GCS/Azure), S3 kept on its existing native minio path

**Description:** Add a new file (e.g. `tempo/tempodb/encoding/vblockpack/
objectstore_backend.go`) defining a type — call it `rawBackendObjectStore` —
that implements blockpack's `ObjectStore`/`CubeObjectStore` (`Get`+
`ConditionalPut`) and `ObjectPutter` (`Put`) by wrapping a `backend.RawReader`
+ `backend.RawWriter` pair (exactly the `rawR`/`rawW` locals from finding #2).
For the read-only file-store surface (finding #3c), add a second, much
simpler adapter wrapping `RawReader.List`/`Read` (accepting the recursive-list
gap from finding #7 via a manual walk, or deferring it — see Approach 3).

`ConditionalPut`'s emulation: `Get` reads current bytes via `RawReader.Read`
and computes `etag = sha256(bytes)` (hex-encoded) as the "ETag" — a faithful
generic analog since S3's own ETag literally is a content hash (MD5) for
non-multipart PUTs (confirmed by finding #4, `minioObjectStore.Get` reads
`info.ETag` straight off the object). `ConditionalPut` then: acquires a
**path-scoped, in-process `sync.Mutex`** (a small sharded map, e.g.
`map[string]*sync.Mutex` keyed by the object's full key, guarded by its own
top-level mutex for map access), re-reads current bytes+hash under that lock,
compares to the caller-supplied etag (mismatch or "object now exists when
caller expected empty" → `ErrConflict`/`CubeErrConflict`), and only then calls
`local.Backend.WriteAtomic` (finding #6 — reused verbatim, not reinvented) for
Local, or `RawWriter.Write` for GCS/Azure (see below on native primitives).

**S3 stays on its existing path** — do NOT migrate S3 to the new generic
adapter. The existing `minioObjectStore`/`viUsageObjectStore` (finding #4) are
already correct (real HTTP `If-Match`), already proven in production, and
migrating them to a content-hash emulation would be a strict correctness
downgrade (losing atomicity: S3's real conditional PUT has no read-then-write
race at all, whereas even a mutex-guarded local emulation only protects
same-process writers, not genuinely concurrent processes/replicas sharing one
S3 bucket from *outside* this process — though for Local that caveat is moot,
per Approach reasoning below).

**GCS**: use its own native `WriteVersioned`/generation-match precondition
(finding #5 — genuinely atomic already) rather than the generic content-hash
emulation, via a thin shim converting blockpack's `string` etag ↔
`backend.Version` generation string. **Azure**: no genuinely atomic native
primitive is wired today (finding #5, Azure's own `WriteVersioned` is racy) —
use the generic content-hash+mutex emulation for Azure too (same as Local),
accepting the same-caveat-as-Local limitation (protects only same-process
concurrent writers) since Azure's real `If-Match` support (mentioned in its
own `// TODO` comment) is out of scope to newly wire up in this pass.

**Pros:**
- Directly reuses two already-correct, already-shipped primitives:
  `minioObjectStore`'s S3 path (finding #4, untouched, zero regression risk)
  and `local.Backend.WriteAtomic` (finding #6, zero new atomic-write code).
- Narrow blast radius: new file(s) in `vblockpack`, S3 production behavior is
  provably unchanged (same code path, same struct, same minio calls).
  Directly answers the "is S3 provably unchanged" question the prompt's
  constraints require before any deploy recommendation: **yes**, because this
  approach never touches the S3 adapters at all.
- Content-hash-as-ETag is honest about what it emulates (finding #4 confirms
  S3's own ETag genuinely is a content hash for the common case) — no
  semantic surprise for callers expecting "ETag-shaped" values.
- Local's mutex+atomic-rename gives **genuine same-process correctness** for
  the priority integration-test scenario (a single test process driving
  frontend → trigger → backfill against `backend.Local`), which is exactly
  what "local backend is the priority proof case" (prompt) needs — no
  multi-process local deployment is claimed to be a supported/tested
  configuration, so same-process-only correctness is a deliberate, sufficient
  scope cut, not a shortcut.

**Cons:**
- Genuine signature-change surface: all 11 functions listed in finding #1
  need their `*s3backend.Config` parameter replaced by something backend-
  agnostic (the new adapter, or `rawR`/`rawW` directly) — real, mechanical,
  but touches many call sites (`tempodb.go`'s two gates,
  `vi_backfill.go`'s `launchViBackfill`/`RunViBackfill` mutual callers,
  `cube_backfill.go`'s equivalents). Needs careful PLAN-phase sequencing to
  avoid a half-migrated state.
- Multi-process Local correctness is explicitly NOT solved (in-process mutex
  only) — must be documented as a known, deliberate limitation (mirroring
  `client.go:139-144`'s existing warning-log pattern for its own best-effort
  backends) rather than silently claimed as "safe."
- Azure gets the weaker (mutex-emulation) path in this pass rather than a
  real `If-Match` wire-up — acceptable per the prompt's stated priority
  ordering ("local priority, GCS/Azure designed-for-but-can-be-cut"), but the
  planner should size whether Azure's real fix is cheap enough to include
  anyway (Azure SDK does support conditional headers per its own code's
  `// TODO` — unexplored in this pass).

### Approach 2: One unified generic path for ALL FOUR backends (including S3), migrating S3 off `minioObjectStore` onto the same content-hash+mutex/native-precondition adapter

**Description:** Same adapter as Approach 1, but also point S3 at it —
replacing `minioObjectStore`/`viUsageObjectStore`'s real `SetMatchETag`
If-Match usage with either (a) the generic content-hash emulation (a
regression: loses the real atomicity S3 already has today), or (b) a
S3-specific native branch inside the same adapter type that still calls
`SetMatchETag` under the hood (no regression, but then it's not really "one
code path," just one Go type with a backend-specific branch — structurally
Approach 1 with extra indirection).

**Pros:**
- Single call site shape everywhere; less special-casing in `tempodb.go`'s
  gates (one adapter constructor regardless of `cfg.Backend`).
- If done as (b), no correctness loss — but see Cons on why (b) isn't really
  simpler than Approach 1.

**Cons:**
- Doing it as (a) is a straight-up regression on the one backend
  (S3) that is live in production today — violates the prompt's explicit
  constraint ("S3 must not regress") and the "state clearly whether S3's
  behavior is provably unchanged before recommending deploy" requirement;
  provably unchanged becomes false.
- Doing it as (b) requires touching and re-testing the exact
  `minioObjectStore`/`viUsageObjectStore` code that is currently correct and
  unexercised by this change under Approach 1 — pure risk with no
  corresponding benefit, since "one code path" doesn't actually simplify
  anything when the path still branches by backend internally.
- Higher review/test surface for a task whose explicit priority is "local
  backend correct, S3 unregressed" — this approach spends effort in the wrong
  place relative to the stated priority ordering.

### Approach 3: Backend-conditional factory — keep S3 on its native path (as in Approach 1), but for the recursive-listing gap (finding #7), add a small helper that does a manual recursive walk over `RawReader.List` rather than changing the `RawReader`/`RawWriter` interfaces themselves

**Description:** Rather than adding a new method to `backend.RawReader`
(interface surface change affecting all 4 backend packages plus any mocks/
fakes), add a small package-local helper in `vblockpack` —
`walkPrefix(ctx, rawR, keypath) ([]string, error)` — that recursively calls
`RawReader.List` one level at a time, accumulating full keys, to reproduce
the flat-recursive-listing semantics `minioVIStore.List`/`cubequerypath.
listObjects` currently get from `Recursive: true`. Bound recursion depth to
whatever the value-index/cube key layout actually uses (needs the open
question from finding #7 answered first: how many levels deep is
`<tenant>/indexes/<colHash>/...` in practice — likely 2-3 levels, i.e. a small,
bounded number of extra round trips, not unbounded).

**Pros:**
- Zero interface changes to `backend.RawReader`/`RawWriter` — no ripple into
  the other 3 backend packages, no mock/fake updates needed elsewhere in
  tempo (a smaller, safer change than modifying `raw.go`'s core interfaces).
- Composes cleanly with Approach 1's adapter — the same file can hold both the
  `ObjectStore`/`ObjectPutter` CAS adapter and this listing helper.

**Cons:**
- More round trips than S3's single recursive `ListObjects` call for Local/
  GCS/Azure — a real, if likely small (bounded by directory depth, not file
  count), latency cost. Needs to be sized against the actual key-layout depth
  before committing (open question, not resolved this pass) — if the layout
  is genuinely shallow (2-3 levels) this is a non-issue; if it turns out to
  fan out combinatorially (e.g. one directory per column-hash × one per time
  bucket), it could reproduce this session's earlier
  serial-round-trip-blowup failure mode (Problem/Investigation section above)
  in a new place. This is the single highest-value thing for the PLAN phase
  to measure before locking in the walk approach.
- Does not benefit S3 at all (S3 keeps its own recursive listing — this
  helper is Local/GCS/Azure-only), so it's an asymmetric implementation:
  acceptable (S3 doesn't need help), but means test coverage must explicitly
  exercise both paths, not assume one code path covers all backends.

## 2026-07-11 11:25:00 - Recommendation

### Chosen Approach: Approach 1 (new generic CAS adapter for
Local/GCS/Azure, S3 stays on its existing native `minioObjectStore` path) +
Approach 3 (manual recursive-walk helper for the listing gap, not an interface
change) — explicitly rejecting Approach 2.

**Rationale:**

- **Correctness bar is asymmetric by design, and that's the right call.** S3
  is live in production today with a genuinely correct (real HTTP If-Match)
  conditional-write path (finding #4); nothing about "backend-agnostic" should
  require regressing that to satisfy a uniform abstraction. Local is the
  stated priority and currently has *zero* production traffic on this feature
  (the whole point of the task is to newly unlock it) — a same-process
  mutex + `WriteAtomic` (finding #6, already-shipped) gives genuine
  correctness for the explicitly-stated use case (a single local integration
  test process), without overclaiming multi-process safety nobody asked for.
- **Reuses two already-correct primitives instead of inventing one
  do-everything abstraction.** `minioObjectStore` (S3) and
  `local.Backend.WriteAtomic` (Local) both already exist and are already
  proven; Approach 1 is additive (one new adapter type for the backends that
  don't yet have a correct primitive) rather than a risky wholesale
  replacement of a working system (which is what Approach 2 would require).
- **Tempo's own closest precedent (`VersionedReaderWriter`) is demonstrably
  not good enough to reuse as-is** (finding #5: Local is a complete no-op,
  S3/Azure are racy) — this rules out simply plugging blockpack's registries
  into the existing `backend.VersionedReaderWriter` abstraction without first
  fixing its Local path, which would be strictly more invasive (touching a
  shared interface used by the unrelated user-configurable-overrides feature)
  than adding a new, scoped adapter type inside `vblockpack`.
- **Narrows the actual conditional-write surface correctly** (finding #3):
  only the two registry index files need CAS; L0 writes and read-only
  postings/cube-file lookups need much simpler unconditional wrappers. The
  plan should NOT apply mutex+hash-CAS machinery to `ObjectPutter`/read-only
  call sites — that would be over-engineering relative to what those
  interfaces actually require.

**Implementation Strategy (for PLAN phase):**
1. In blockpack (if any interface changes are needed at all — likely none;
   `ObjectStore`/`ObjectPutter`/`CubeObjectStore` already exist and are
   already backend-agnostic by design, per finding #3's interface bodies).
   This step may be a no-op — confirm in PLAN phase whether any blockpack
   change is required, since the interfaces to satisfy already exist.
2. In tempo's `vblockpack` package, add the new CAS adapter type wrapping
   `backend.RawReader`/`RawWriter`, satisfying `ObjectStore`/`CubeObjectStore`/
   `ObjectPutter`, with the mutex+content-hash+`WriteAtomic`-for-Local design
   from Approach 1. Add the `walkPrefix` recursive-listing helper from
   Approach 3 alongside it (same file, or a small sibling file), satisfying
   the read-only file-store surface (finding #3c).
3. For GCS, branch the adapter's `ConditionalPut` to use
   `WriteVersioned`'s native generation-match instead of content-hash
   emulation (finding #5 — GCS already has a real primitive; use it).
4. Rewrite `tempodb.go`'s two gates (lines 297, 326) to branch on the new
   adapter construction rather than `cfg.Backend == backend.S3`, modeling the
   backend-agnostic condition on the write-gate's existing 2026-07-11
   nil-check-removal precedent (comment at line 289-296) for how this
   codebase phrases "provably non-nil, no defensive re-check" reasoning.
5. Update all 11 function signatures (finding #1's list) to take the new
   adapter (or `rawR`/`rawW` directly) instead of `*s3backend.Config`,
   sequencing the change to avoid a half-migrated state (likely: land the new
   adapter + signature changes together in one changeset per function-group,
   not incrementally across separate PRs, since the S3-only gate is the thing
   holding everything together today).
6. Write the local-backend integration test (frontend RoundTrip → real
   trigger → real backfill, per the prompt's exact spec) against
   `backend.Local`, as the primary new regression test.
7. Mutation-test the new adapter's conflict-detection logic specifically:
   temporarily make `ConditionalPut` skip the hash-compare (always succeed),
   confirm a concurrency test now fails/corrupts, revert, confirm it passes —
   per this session's standing constraint that every real behavioral change
   needs a mutation-tested regression guard.
8. Update `SPEC-CUBE-014`'s S3-specific wording (finding #8's spec-drift
   note) and viusage's equivalent spec text via blockpack's spec-oracle
   workflow, once the abstraction is backend-agnostic in fact.

**Key Decisions:**
- S3 stays on `minioObjectStore`/`viUsageObjectStore` unchanged — **provably
  unchanged**, since Approach 1 never edits those types or their call sites'
  underlying minio calls; only their *construction* (which config/handle gets
  passed in) changes shape, not their internal logic. This directly satisfies
  the prompt's "state clearly whether S3's behavior is provably unchanged
  before recommending deploy" requirement: yes, provided PLAN/EXECUTE
  literally leaves `cubemanager.go`'s and `vi_backfill.go`'s `Get`/
  `ConditionalPut` bodies untouched (verify this explicitly in code review).
- GCS gets its real native primitive (generation-match), not the generic
  emulation — free correctness, no reason to downgrade it to Local's level.
- Azure and the generic Local emulation share the same mutex+content-hash
  code path in this pass; Azure's real `If-Match` wire-up is deferred as a
  documented, explicit future improvement (matches the prompt's stated
  acceptable-minimum: "local-backend-correct + a clean extension point for
  GCS/Azure").
- The recursive-listing gap (finding #7) is solved by a manual walk helper,
  NOT by adding a new method to `backend.RawReader` — keeps the blast radius
  inside `vblockpack`, avoids touching the other 3 backend packages' shared
  interface.

**Risks Identified:**
- **Multi-process Local correctness is not solved** (mutex is in-process
  only) — must be explicitly documented as a known limitation (mirroring
  `client.go`'s existing warning-log convention) so it isn't mistaken for a
  general-purpose multi-replica-safe Local backend. Mitigation: state this
  limitation in the new adapter's doc comment and in `NOTES.md`, and confirm
  the integration test only exercises single-process concurrency.
- **Recursive-listing walk cost is unmeasured** (finding #7/Approach 3's
  main con) — must be sized against the real `<tenant>/indexes/...` and
  `<tenant>/cubes/...` key-depth before committing; if the layout is deeper/
  wider than assumed, this could reproduce the earlier serial-round-trip
  blowup pattern from this same session's `vibuilder.downloadAll`
  investigation, in a new code path. Mitigation: measure actual key-layout
  depth in PLAN phase (read `valueindex`'s and `cube`'s file-layout
  constants) before locking in the walk design; cap recursion depth or fall
  back to a bounded-fanout concurrent walk if depth turns out to be
  non-trivial.
- **Signature-change blast radius across 11 functions** — real mechanical
  risk of a half-migrated state if not sequenced carefully (Key Decision #5
  above); mitigate by landing the adapter and all signature changes in one
  cohesive changeset, gated by `go build`/`go vet` across the whole package
  before considering it done (per this session's standing "trust only real
  build output" constraint).
- **The pre-existing etag=="" unconditional-PUT gap** (finding #4's "one
  pre-existing, non-regressing gap") should be preserved as-is (not silently
  fixed or silently left in a way that diverges between backends) — an
  explicit PLAN-phase decision, not an accident.
- **SPEC-CUBE-014's S3-specific wording** will become stale the moment this
  ships; must be updated via the spec-oracle workflow as part of "done," not
  left to drift (finding #8).

**Open Questions (for PLAN phase to resolve):**
- Does blockpack's `internal/modules/valueindex/` and `internal/modules/cube/`
  actual on-disk key layout make the recursive-listing walk (Approach 3)
  cheap (2-3 levels) or expensive (deep/wide fan-out)? Not measured this pass.
- Is Azure's real conditional-write primitive (`If-Match`, alluded to in its
  own `// TODO` comment, finding #5) cheap enough to wire up in the same pass
  as Local, rather than deferring it? Not investigated this pass (out of
  scope per the prompt's stated priority, but worth a quick PLAN-phase sizing
  check since it may be low-effort given the TODO already identifies the
  mechanism).
- Should the new adapter live entirely inside `tempo/tempodb/encoding/
  vblockpack/` (this session's working assumption, matching where
  `minioObjectStore`/`viUsageObjectStore` already live), or does any part of
  it belong in `tempo/tempodb/backend/` as a shared primitive (e.g. if a
  future feature beyond VI/cube also wants backend-agnostic conditional
  writes)? Leaning toward keeping it `vblockpack`-local for this task (avoids
  touching the shared `backend` package's interfaces at all, per Approach
  3's rationale), but flagging as a placement decision for the planner.
- Exact mapping from the 11 function signatures' current
  `*s3backend.Config` parameter to the new adapter type — a straight 1:1
  swap in most cases, but `newMinioForValueIndex`/`newViBackfillMinioClient`
  (which construct S3-specific minio clients) may need to become
  conditional/backend-specific constructors rather than being deleted
  outright, since S3's read-path (`ConfigureValueIndexQuery`,
  `value_index_query.go`) still needs a real `*minio.Client` for
  `minioVIStore` regardless of this change (finding #3c's read-only surface
  is a separate, still-S3-specific-today concern not explicitly resolved by
  Approach 1 — the prompt's "11 functions" list conflates the CAS-registry
  functions with the read-only-file-store functions; PLAN should split these
  into two tracks: registry CAS adapters (this brainstorm's main subject) vs.
  read-only VI-postings-file-store backend-agnostic-ification (a related but
  distinct, not-yet-fully-scoped piece of work — `minioVIStore`'s List/Get/
  Size/ReadAt would need its own Local/GCS/Azure equivalents, likely simpler
  since no CAS is needed, but not designed in this pass).

## 2026-07-11 11:30:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Root cause / scope confirmed and narrowed:** The S3-only gates in
`tempodb.go` (lines 297, 326, verified accurate) block VI/cube entirely on
non-S3 backends. The actual conditional-write ("ETag emulation") problem is
real but narrower than initially framed — it applies only to two small JSON
registry files (viusage usage-index, cube `index.json`), not to L0 file writes
or read-only VI-postings/cube-file lookups, which need only simple
unconditional wrappers. Tempo already has a same-shaped precedent
(`backend.VersionedReaderWriter`) but its Local/S3/Azure implementations are
explicitly non-atomic/racy and not safe to reuse as-is; Local already has a
correct atomic-write primitive (`local.Backend.WriteAtomic`) that the new
design should build on directly.
**Recommendation:** Approach 1 — a new `vblockpack`-local CAS adapter
(content-hash-as-ETag + path-scoped mutex + `WriteAtomic` for Local, native
generation-match for GCS, same mutex emulation for Azure) wrapping
`backend.RawReader`/`RawWriter`, with S3 explicitly left on its existing,
already-correct `minioObjectStore`/`viUsageObjectStore` path (provably
unchanged, no regression). Combined with Approach 3 (a manual recursive-list
walk helper, not a `backend.RawReader` interface change) to close the
separate flat-recursive-listing gap between S3's native `Recursive: true`
`ListObjects` and `backend.RawReader.List`'s one-level-only semantics.
Explicitly rejected Approach 2 (migrating S3 onto the generic path) as an
unjustified regression risk to a currently-correct, currently-live path.
**Next Phase:** PLAN

Key open item for PLAN to resolve early: split the "11 functions" into two
tracks — (1) the two registry CAS adapters (this brainstorm's core subject,
well-scoped) and (2) the read-only VI-postings-file-store
backend-agnostic-ification (`minioVIStore`'s List/Get/Size/ReadAt equivalents
for Local/GCS/Azure — related, still S3-only today, but not fully designed in
this pass and structurally simpler since it needs no CAS).

Ready for workflow-planner agent to create the detailed implementation plan.

---

# Task: Postgres-backed replacement for viusage/cube usage-tracking + backfill-state registries

## 2026-07-11 12:00:00 - Task Received

Two repos, both worked on directly (no worktrees, per standing convention):
- blockpack: `/home/mdurham/source/blockpack_collection/blockpack` (branch main)
- tempo: `/home/mdurham/source/blockpack_collection/tempo` (branch agentic-tempo)

**Explicit user direction:** "There is an issue on creating a native postgres system to
hold data in a native way, can we review that issue and use that to store the backfill
state and the list of queries?" — user chose "b": treat blockpack issue #467's "DO NOT
IMPLEMENT — design only" constraint as LIFTED for this specific use case.

**Why (root cause confirmed live tonight, not speculation):** Earlier tonight: fixed a
real bug where minio-go's `GetObject` is lazy (the real HTTP GET, and thus the real 404,
only fires on the first Read/ReadAt, NOT on GetObject itself) — `viUsageObjectStore.Get`
and `minioObjectStore.Get` only classified `GetObject`'s OWN error as NoSuchKey, so a
genuinely-missing registry object's real 404 leaked through `io.ReadAll` as a raw,
untranslated minio error instead of `blockpack.ErrNotFound`/`CubeErrNotFound` — meaning
a tenant's registry file could never be created on its first-ever write. Fixed and
deployed (commit f28a670ca, blockpack-f28a670ca-r18 on tempo-dev-test-03).

Once live, VI backfill triggers started firing for the first time ever. But this
immediately exposed a SECOND, distinct, still-unfixed problem: `<tenant>/viusage/
index.json` is ONE JSON file shared across EVERY column being tracked/backfilled for a
tenant (same for `<tenant>/cubes/index.json`). Every concurrent backfill run (one per
triggered column, potentially many columns concurrently, across multiple querier/
query-frontend replicas) persists its own per-block watermark progress via a
conditional-PUT-with-5-retries against that SAME shared file. Real production evidence
from tonight: 32 of 34 real backfill attempts failed within minutes with a 412
Precondition Failed, retry-budget-exhausted conflict.

**Explicit checkpoint (must be honored):** Do NOT `kubectl apply` any new
Cluster/operator/PVC/PDB resource against any real cluster. Do NOT connect to or
provision any real Postgres instance outside of the team's own tests. Land all code,
schema, and manifests as reviewed, ready-to-apply artifacts. Team reports back to the
team lead for an explicit go/no-go before any live infrastructure changes.

Starting brainstorm process (bootstrapped via `lth stats`/`lth prompt`, then verifying
every file the prompt cites against current source, reading blockpack issue #467 in
full, checking tempo's go.mod/vendor for any Postgres client, checking the
tempo-dev-test-03 cluster for the CloudNativePG operator, and consulting blockpack's
spec-oracle convention for viusage/cube invariants).

## 2026-07-11 12:15:00 - Research Findings

### lth guidance consulted (bootstrap)

`lth stats` (59,724 memories, 480,300 edges) and `lth prompt "Postgres-backed registry
to replace S3 JSON conditional-PUT contention, CloudNativePG in-cluster database, Go
database schema design, row-level concurrency vs shared-file conditional writes"`
surfaced: the general principle "synchronize access to shared mutable state, or
eliminate sharing" (directly on point — the recommended design below eliminates sharing
rather than adding a second layer of synchronization on top of it); the
"atomic read-modify-write (GET + conditional POST/PATCH) + automatic retry on conflict"
pattern already implemented by `Registry.updateEntryWithRetry` (below); and a repeated
note that this project's convention is to use `grv` tools for Go file reads/writes —
**not applicable to this brainstorm** (research-only, no code written), but the coder
phase should confirm this tooling convention before editing any `.go` file for this task.

### Existing Registry/Entry/Trigger shape (blockpack, exact, verbatim)

**`viusage/registry.go`** — `ObjectStore` interface (lines 39-42), identical shape
independently re-implemented in **`cube/registry.go`** (lines 37-40):
```go
type ObjectStore interface {
    Get(ctx context.Context, path string) (data []byte, etag string, err error)
    ConditionalPut(ctx context.Context, path string, data []byte, etag string) error
}
```
`Registry.updateEntryWithRetry` (viusage/registry.go:109-162) is the actual mechanic all
mutation goes through: **Load whole blob → linear-scan find entry by (Tenant, ColumnHash,
ColumnType) → mutate in place → re-marshal WHOLE index → `ConditionalPut` whole blob →
on `ErrConflict` retry up to 5x with 50ms-doubling backoff.** Every one of
`recordUse`/`RenewLease`/`UpdateWatermark`/`RecordUseAndMaybeTrigger` (trigger.go:113)
calls this **private** method directly — none of them route through `ObjectStore` a
second time or expose any narrower per-entry primitive. Cube's `registry.go` has the
identical shape independently re-implemented (`Add`/`Remove`/`UpdateWatermarks`, lines
117-256) per **R1** (confirmed via spec-oracle: R1 exists because only one
implementation existed at the time the ruling was made — it explicitly says a
shared-helper decision should wait for "two working implementations," which now exist.
**R1 is not a blanket ban on ever sharing code and does not block a shared Postgres-
backed store.**)

**Entry/BackfillState shape (entry.go:17-85)** — this is the row shape a Postgres
schema must support, one row per (Tenant, ColumnHash, ColumnType):
- `Tenant string`, `ColumnHash string`, `ColumnName string`, `ColumnType string` (key)
- `UseTimestamps []uint64` (bounded ring, `MaxTrackedUses=32`, `omitempty`)
- `Backfill BackfillState` — `LeaseOwnerID string`, `LeaseExpiresAt uint64`,
  `WatermarkSec uint64`, `WindowStartSec/WindowEndSec uint64`, `Triggered bool`,
  `BackfillInProgress bool`, `Done bool`
- `FirstSeenSec uint64`, `CreatedAt uint64`
- `BackfillState.CoversRange(minSec, maxSec)` (entry.go:91-102) is the pure query-time
  coverage predicate — must be preservable against however the new store returns a row.

**`RecordUseAndMaybeTrigger` (trigger.go:102-151)** — the actual hot-path logic: append a
use timestamp, prune to window+`MaxTrackedUses`, then evaluate in ONE pass: `Done` →
never retrigger (R5, permanent); `Triggered && lease valid` → no-op; `Triggered && lease
expired` → crash-self-heal re-acquire (R8); `not yet Triggered && len(UseTimestamps) >=
Threshold` → first-time trigger + lease acquire. **This entire evaluate-and-mutate
function must still run as ONE atomic unit against whatever replaces
`updateEntryWithRetry` — it is exactly the compare-and-swap shape a single `UPDATE ...
WHERE ... RETURNING` (or `INSERT ... ON CONFLICT DO UPDATE`) statement is built for.**

**Correction to this task's own brief, confirmed via a `team-spec-oracle` sub-agent**:
`trigger.go`'s doc comment says `Threshold=1 per explicit team-lead ruling 2026-07-11`,
but the *existing* NOTES.md (NOTE-VIUSAGE-3) still documents `Threshold=3` as an older,
unmeasured default. The code comment is dated today and is presumably the live, current
ruling; NOTES.md appears stale relative to it. **Flagging for the planner — not this
task's scope to fix, but the docs/code are inconsistent right now** (separate from
anything Postgres-related).

### Tempo-side dispatch pattern (tonight's earlier work, exact — see the immediately
preceding brainstorm section above for full detail)

`vi_usage_hook.go:newViUsageObjectStoreForBackend` (lines 175-193) and
`cubemanager.go`'s `ConfigureCubeManager` (lines 123-163, using
`newCubeObjectStoreForBackend` from `rawobjectstore_gcs.go:220-227`) are the **two single
construction points** tempo already has for "which `blockpack.(Cube)ObjectStore`
implementation backs this tenant's registry." `s3cfg != nil` → untouched, original
`viUsageObjectStore`/`minioObjectStore` (real S3 ETag/If-Match); else →
`newObjectStoreForBackend(rawR, rawW)` / `newCubeObjectStoreForBackend(rawR, rawW)`:
probes whether `rawW`'s *concrete type* originates from `tempodb/backend/gcs`
(`isGCSBackend`, rawobjectstore_gcs.go:66-75, checked via `reflect` package-path, NOT a
structural type assertion, because `*azure.Azure` structurally satisfies the same
"versioned" interface but its `WriteVersioned` is a non-atomic emulation) → native
generation-match `WriteVersioned`/`ReadVersioned`; else → `rawCASCore`
(rawobjectstore.go:83-174): content-SHA256-as-etag + a process-local `pathMutexMap`
(one `*sync.Mutex` per key, shared singleton `processCASLocks` across BOTH registries
since their key namespaces never collide) emulating conditional-write for Local/Azure.

**Critical structural fact for the Postgres design**: this dispatch produces an
`ObjectStore`/`CubeObjectStore` value that gets handed to `blockpack.NewRegistry(store,
tenant)` / `blockpack.NewCubeRegistry(store, tenant)` — i.e. today's dispatch only ever
chooses *which blob-shaped store* backs the *same* `Registry` type. **A Postgres backend
that abandons blob semantics (row-per-column) cannot slot into this exact shape** — it
would need `Registry` itself to gain a second, sibling construction path (see
Recommendation below), and the tempo-side dispatch functions would need a new branch
that decides "build a `blockpack.Registry` over a Postgres pool" instead of "build a
`blockpack.ObjectStore` and hand it to the existing constructor."

Also confirmed: `blockpack.CubeObjectStore = cube.ObjectStore` and `CubeObjectPutter =
cube.ObjectPutter` are plain **type aliases** re-exported from `cube_ingest.go:30,62` —
the public API surface for cube's registry interface is defined ONCE, internally, and
merely aliased for external (tempo) use. Any interface-shape change happens in
`internal/modules/cube/registry.go` and `internal/modules/viusage/registry.go` and is
automatically visible through the alias — no separate public copy to keep in sync.

### Postgres/SQL client library: NONE currently vendored in tempo

Checked `go.mod`, `go.sum`, and `vendor/modules.txt` directly:
- No `pgx`, `jackc`, or `lib/pq` anywhere in `go.mod`/`vendor/modules.txt`.
- The one `go.sum` hit for a case-insensitive `pgx` grep was a false positive
  (`go.opentelemetry.io/contrib/otelconf`'s base64 module hash coincidentally contains
  the substring `pGx`) — confirmed by re-reading the exact matched line.
- `github.com/grafana/dskit` (go.mod:25, `v0.0.0-20260427162712-0457a92dacc3`) is
  vendored but does not itself pull in a Postgres driver (dskit is ring/kv-store/runtime
  config, not SQL).
- **A brand-new dependency (pgx v5 is the idiomatic modern choice, or database/sql +
  lib/pq/pgx stdlib shim) will need to be added and vendored** — this is a real,
  reviewable go.mod/go.sum change, not something already available for free.

### CloudNativePG operator: NOT present in the tempo-dev-test-03 cluster

Checked directly, both ways were investigated (not assumed):
- `kubectl get crd | grep -i postgresql` returns only **Crossplane's** SQL provider
  CRDs (`*.postgresql.sql.crossplane.io`, `*.postgresql.sql.m.crossplane.io`,
  `*.sql.crossplane.grafana.net`) — a managed-cloud-SQL provisioning layer (Crossplane
  compositions targeting what is almost certainly a cloud-managed Postgres/CloudSQL
  given the `grafanalabs-dev` GCP artifact registry references seen elsewhere in the
  cluster), **not CloudNativePG**.
- `kubectl get crd | grep -i cnpg` and `kubectl get pods -A | grep -i cnpg` both return
  nothing (exit code 1, no matches).
- Real Postgres pods DO exist in the cluster (`db-o11y` namespace:
  `app-mybooks-postgres-16-...`, `app-mybooks-postgres-17-...`), but these are an
  unrelated playground/test app (`db-o11y-mybooks` image, paired with MySQL/SQL Server
  pods in the same namespace, clearly a multi-DB test harness for a different team's
  product, not general infra usable for this task).
- **Conclusion: installing the CloudNativePG operator is a real, not-yet-done
  infrastructure prerequisite** for issue #467's own proposed architecture. This is a
  bigger, separate lift from the schema/code work and squarely inside the "no live
  infra changes without explicit go/no-go" checkpoint — it cannot be a hidden
  side-effect of "just add Postgres support to blockpack/tempo."

### Existing precedent for a stateful external dependency configured via YAML

`tempodb/config.go:45-79` (`tempodb.Config`) already embeds `Memcached
*memcached.Config` / `Redis *redis.Config` directly as struct fields (`yaml:"memcached"`
/ `yaml:"redis"`), alongside the backend config (`Local`/`GCS`/`S3`/`Azure`). A second,
role-based cache config layer exists too: `modules/cache/config.go`'s `Config` holds
`Caches []CacheConfig`, each `CacheConfig` embedding `*memcached.Config` OR
`*redis.Config` selected by `Role`. `modules/cache/redis/redis.go:12-16` shows the
per-backend `Config` shape: a thin wrapper (`ClientConfig cache.RedisConfig` inline +
a `TTL`) delegating connection-pool/timeout details to a shared `pkg/cache.RedisConfig`.
**This is the concrete shape to mirror for a new `Postgres` config block**: a
`*postgres.Config` field on `tempodb.Config` (or a new sub-package under
`modules/` alongside `cache/`), with connection string/pool-size/timeout fields, wired
into `tempodb.go`'s startup path the same way `ConfigureViUsage`/`ConfigureCubeManager`
already are.

### Test infrastructure precedent for local/ephemeral Postgres: NONE

No `testcontainers-go` anywhere in either repo's `go.mod`/`go.sum` (checked both
blockpack and tempo). Whatever the coder phase picks (testcontainers-go with a Postgres
module, `ory/dockertest`, or a CI-service-container approach with a plain `pgx` pool
pointed at `localhost` in CI) will be a **new** test-infrastructure decision, not an
existing pattern extension. Given the standing environment checkpoint (no live Postgres
provisioning outside team tests), testcontainers-go (spins up + tears down an ephemeral
container per test run, no persistent infra) is the safer default to propose in
planning, but this needs explicit confirmation it's runnable in this repo's CI sandbox
before committing to it.

### Spec-driven modules in scope (consulted via team-spec-oracle, not read directly)

Both `internal/modules/viusage/` and `internal/modules/cube/` have
SPECS.md/NOTES.md/TESTS.md/BENCHMARKS.md and are spec-driven. Consulted via a
`team-spec-oracle` sub-agent per this repo's MCP-server convention (I don't have direct
MCP tool access in this environment, so a spec-oracle agent was spawned to read the spec
files and report back — same net effect as calling `blockpack_search_modules` directly).

Key invariants confirmed: **R1** (independence, not a sharing ban), **R4** (repeated-use
threshold — NOTES.md says Threshold=3, live code comment says Threshold=1 dated today, a
real doc/code inconsistency to flag, not fix, in this task), **R5** (no eviction,
`Triggered` never reverts), **R7** (`CoversRange` watermark semantics, has an
adversarial regression test in TESTS.md), **R8** (lease lifecycle/crash-self-heal),
**R9** (cube's own backfill watermark-persistence gap — since fixed in tempo's
`cube_backfill.go`, task #127, so no longer a live gap). **No existing spec entry
anywhere discusses Postgres, SQL, or replacing the ObjectStore interface** — this task
is breaking genuinely new ground, not contradicting a prior ruling. **`MaxTrackedUses=32`'s
bound is justified by semantics ("a repeated-use threshold in the single digits never
needs more than a few tens of samples"), NOT by S3 blob-size/round-trip cost** (confirmed,
SPEC-VIUSAGE-1) — this directly answers one of the brainstorm's required design
questions: moving to Postgres does **not** remove any stated technical reason for the
ring's bound, because storage cost was never the reason for the bound in the first place.

### blockpack issue #467 — read in full

Title: `[DO NOT IMPLEMENT — design only] value-index catalog: HA in-cluster listing +
metadata query service over object storage`. Proposes CloudNativePG (3 instances,
~1-2 cores/2-4GB each, local-PV storage acceptable since durability comes from WAL
replicas, strict AZ topologySpreadConstraints, PDB, WAL on its own volume, S3 backups)
as the catalog store for value-index FILE metadata (one row per index file:
`file_id/tenant/col_hash/col_type/level/start_sec/end_sec/object_ref/size_bytes/
row_count/min_val/max_val/status/lease_until/enrich_attempts`), populated via a
bucket-notification-driven two-phase (PENDING→enrich→READY) ingestion pipeline plus a
reconciler CronJob, replacing live S3 LIST calls with indexed SQL range queries. Per this
task's explicit brief: only the CloudNativePG infra choice/HA philosophy carries over;
the file-metadata schema and notification/enrichment/reconciler pipeline are explicitly
NOT in scope for viusage/cube (a structurally different problem — usage/backfill state
per column, not per file).

## 2026-07-11 12:30:00 - Design Questions Answered

### Q1: Does the existing ObjectStore/CubeObjectStore shape map onto Postgres?

Investigated two concrete approaches:

**Approach 1a — keep blob semantics, one Postgres row per TENANT.** Implement a new
`ObjectStore`/`CubeObjectStore` adapter in tempo (`postgresObjectStore`) whose `Get`
does `SELECT data, xmin FROM viusage_blob WHERE tenant=$1` (using Postgres's hidden
`xmin` system column, or an explicit `version bigint`, as the "etag") and whose
`ConditionalPut` does `UPDATE viusage_blob SET data=$1 WHERE tenant=$2 AND xmin=$3` (0
rows updated ⇒ `ErrConflict`, mirroring today's 412 check). **This requires ZERO
blockpack code changes** — `Registry` is handed a new `ObjectStore` implementation and
nothing else changes; no new public API surface, no sign-off needed.
**However, this does NOT structurally fix the production bug.** The whole reason 32/34
backfill attempts failed tonight is that *every column's* watermark update targets the
*same shared blob* — swapping the blob's storage medium from S3-JSON to
Postgres-JSONB-in-a-row keeps the identical single-row contention point. Postgres's
local, sub-millisecond compare-and-swap retry is genuinely faster and cheaper than an
S3 round trip with 50ms-doubling backoff, so this alone would likely reduce (not
eliminate) the failure rate under bursty concurrent triggering — but it is a latency
mitigation, not the structural fix the task's own root-cause analysis calls for.

**Approach 1b — genuinely new row-oriented interface, one row per (tenant, colHash,
colType).** This directly matches Entry's real shape and eliminates cross-column
contention entirely: two columns' concurrent `UpdateWatermark` calls touch two disjoint
rows, so they never conflict with each other at all (only truly concurrent updates to
the SAME column's SAME row need Postgres's native MVCC/row-lock, which is the correct,
minimal contention surface — matching the actual concurrency pattern described in the
root cause: "different columns' updates never touch the same row"). This requires
blockpack to grow a genuinely new API surface — but NOT a change to the existing
`ObjectStore` interface itself (see Recommendation below for the specific shape) —
because `Registry.updateEntryWithRetry` is a *private* method today; nothing about the
public `Load`/`RenewLease`/`UpdateWatermark`/`RecordUseAndMaybeTrigger` signatures needs
to change for the row-oriented backend to slot in underneath them.

**Recommendation: Approach 1b.** The whole point of this task is to eliminate the
contention that Approach 1a only makes cheaper to retry through. Given the user's own
root-cause framing ("different columns' updates never touch the same row... Postgres's
native MVCC instead of a hand-rolled ETag-conditional-PUT-with-retry scheme"), Approach
1a would ship something that still occasionally conflicts under enough concurrent
same-tenant traffic, just less often — not a genuine fix.

### Q2: What does "the list of queries" mean?

Ambiguous in the user's own phrasing ("store the backfill state and the list of
queries") and genuinely underdetermined by the codebase — flagging as an **explicit open
question for the team lead to confirm with the user before the planner commits to a
schema**, per this session's own convention for API-surface sign-off. Two readings:

1. **Literal reading**: "the list of queries" = the existing bounded `UseTimestamps`
   ring (32 entries, used only for the R4 threshold check). Confirmed via spec-oracle
   that this bound is semantic, not storage-cost-driven — a Postgres backend does not
   create pressure to unbound it. **Recommendation: migrate `UseTimestamps` as-is,
   bounded, unchanged semantics**, most naturally as either a `bigint[]` column on the
   same per-(tenant,colHash,colType) row, or a small side table if the planner prefers
   avoiding array columns — either way, this is the HOT PATH (`RecordUseAndMaybeTrigger`
   runs on every declined query) and should stay minimal: no new unbounded audit
   capability is indicated by anything in the current spec docs or root-cause writeup.
2. **Expanded reading**: a genuinely NEW, unbounded query-audit-log capability (which
   columns were queried, when, by whom/which tenant, for observability/product
   analytics) that doesn't exist today at all. If this is what's wanted, **recommend
   keeping it as a SEPARATE, new, append-only audit table** (e.g.
   `viusage_query_audit_log(tenant, col_hash, col_type, queried_at, ...)`), NOT unified
   with the bounded ring — the ring's entire design point is "small, fixed-cost,
   read-and-rewritten on every hot-path trigger check"; an unbounded audit table has a
   completely different access pattern (append-mostly, rarely read, no upper bound) and
   mixing the two either bloats the hot-path row or forces the hot path to also write to
   an unbounded table on every call, which is unnecessary I/O for something the trigger
   logic itself never reads.

**Recommendation: do NOT guess between these two readings — surface this explicitly
to the team lead** before the planner finalizes a schema, since the schema shape differs
meaningfully (one small array/JSONB column vs. a whole second table) and this is a
one-sentence ask from the user that a wrong guess would be expensive to unwind after
code is written against it.

### Q3: Migration for real live S3 JSON data on tempo-dev-test-03

Real data exists right now: tenants `11638` and `vulture-tenant` have real
`<tenant>/viusage/index.json` / `<tenant>/cubes/index.json` entries, some with real
in-progress `BackfillState` (non-zero `WatermarkSec`, `BackfillInProgress=true`).
`BackfillState.CoversRange` (entry.go:91-102) defaults to `false` (no coverage) for any
column that's never been `Triggered` — meaning **losing in-progress backfill state is
not a correctness/data-loss risk, only a wasted-work risk**: a column that was
mid-backfill would simply restart from an untriggered state and re-trigger on its next
use (`RecordUseAndMaybeTrigger`'s own R8 self-heal logic already handles "no prior
state" identically to "expired lease," by design). Given:
- this is `tempo-dev-test-03`, not production,
- the task's own writeup already states old data is "mostly re-derivable by triggering
  again,"
- the checkpoint constraint says no live infra changes without an explicit go/no-go
  anyway (so nothing can be migrated live during this brainstorm/plan phase regardless),

**Recommendation: for tempo-dev-test-03 specifically, "start fresh in Postgres, let the
old S3 JSON lapse" is an acceptable, low-risk, explicitly-stated decision** — no
importer needed for THIS environment's cutover. However, **a one-time Go importer (read
S3 `index.json`/`cubes/index.json` per tenant, upsert into the new Postgres rows) should
still be built as a reviewed, ready-to-apply artifact**, because a real production
rollout (if this ever ships beyond dev-test-03) would have tenants with genuinely
expensive, multi-hour/48h-window backfill progress where "just re-trigger" wastes real
compute and delays real coverage — that cost is dev-test-03-acceptable but not
production-acceptable. No dual-write cutover window is recommended: the existing
lease/self-heal design already tolerates "some replica briefly still writes to the old
S3 path while another already reads from Postgres" about as gracefully as it tolerates
a crashed backfill worker today (worst case: one redundant backfill launch, not
corruption) — a dual-write window adds real complexity (two write paths, reconciliation
logic) for a benefit (avoiding a handful of redundant backfill launches) that's smaller
than the complexity cost.

## 2026-07-11 12:40:00 - Approaches Considered (Overall Architecture)

### Approach A: Clean-cut replacement — Postgres becomes the ONLY backend

Delete the S3-JSON-blob `Registry`/`ObjectStore` implementation entirely from
blockpack; delete `viUsageObjectStore`/`minioObjectStore`/`rawObjectStore`/
`gcsObjectStore` and their Cube siblings from tempo; every deployment must run Postgres.

**Pros:**
- Simplest end-state: one code path, one registry implementation, no dispatch branching
  to maintain going forward.
- Matches this project's explicit "no backwards compatibility required" convention
  (memory: blockpack project, blanket API-change permission already granted 2026-07-07).
- No risk of the two implementations silently drifting apart over time.

**Cons:**
- **Blast radius is the entire fleet, not just tempo-dev-test-03.** Every tempo
  deployment using viusage/cube (any backend: S3, GCS, Local, Azure) would need
  Postgres provisioned before it could start at all — a hard, all-or-nothing
  dependency on infrastructure (CloudNativePG operator) that does not exist in ANY
  cluster today (confirmed above), let alone every cluster tempo runs in.
- Cannot be tested/validated incrementally against real production-shaped traffic
  before every other deployment is forced onto it.
- Directly at odds with the explicit checkpoint ("no live infra changes without
  explicit go/no-go") — a clean-cut replacement makes Postgres provisioning a
  hard prerequisite for tempo to run AT ALL, which is a much bigger, harder-to-reverse
  commitment than "an opt-in feature flag."

**Fits existing patterns:** No — directly contradicts the "one factory function per
backend, existing backends untouched" pattern established literally tonight
(`newViUsageObjectStoreForBackend`/`newCubeObjectStoreForBackend`), which exists
specifically so a new backend can be added without risking the ones already live.

### Approach B: Postgres as an ADDITIONAL, opt-in backend (same pattern as tonight)

Add a new branch to `newViUsageObjectStoreForBackend`/`ConfigureCubeManager`'s dispatch
(gated by a new `Postgres *postgres.Config` field, analogous to `s3cfg`/`rawR`/`rawW`
today) that constructs a Postgres-backed `Registry` variant instead of an
`ObjectStore`-backed one. Existing S3/Local/GCS/Azure blob-backed paths are completely
untouched. Deployments opt in via config; tempo-dev-test-03 can flip it on without
affecting any other cluster.

**Pros:**
- Zero risk to the live S3 path (or any other backend) anywhere else — matches this
  session's own established convention exactly.
- Testable/validatable incrementally: dev-test-03 only, real production traffic,
  before any other deployment is ever asked to touch Postgres.
- Directly satisfies the checkpoint: Postgres provisioning becomes an opt-in
  infrastructure addition for specific clusters, not a blocking prerequisite fleet-wide.
- Matches "no backwards compatibility required" without requiring a *forced* migration
  — old JSON-blob code isn't required to be deleted for Postgres support to ship, though
  it CAN be deleted later once every real deployment has migrated (a natural follow-up,
  not a blocker for this task).

**Cons:**
- Two implementations to maintain (JSON-blob + Postgres-row) until/unless the blob path
  is eventually deleted — genuine ongoing cost, though bounded (this is exactly the
  situation Local/GCS/Azure/S3 already are in today, and that dispatch pattern has
  already proven manageable).
- Slightly more code up front (a new `Registry` construction path + dispatch branch)
  than a hard cutover.

**Fits existing patterns:** Yes — directly extends the exact "one factory function per
backend type, others untouched" shape established tonight in
`rawobjectstore.go`/`rawobjectstore_gcs.go`.

### Approach C: Postgres as a blob-in-a-row shim reusing the existing ObjectStore interface (Approach 1a from Q1, elevated to a full architecture option)

Same opt-in dispatch shape as Approach B, but the Postgres adapter implements the
*existing* `ObjectStore`/`CubeObjectStore` interface (one row per tenant, whole JSON blob
in a `jsonb`/`bytea` column, `xmin`-based optimistic conflict detection) instead of a
genuinely new row-per-column interface.

**Pros:**
- Smallest possible code change — zero blockpack public API growth, no sign-off
  needed, ships fastest.
- Still likely reduces (via cheaper/faster local retry vs. S3 round-trip) the observed
  412 failure rate somewhat.

**Cons:**
- **Does not fix the actual root cause.** Per Q1's analysis, this keeps the exact same
  single-shared-row contention point the whole task exists to eliminate — every
  concurrent column update for one tenant still serializes against every other column's
  update for that same tenant. Under exactly the kind of "many columns backfilling
  concurrently across multiple replicas" load that caused tonight's failures, this
  would still exhibit conflicts, just with cheaper retries.
- Ships something that could be mistaken for "the fix" when it structurally isn't,
  risking a false sense of resolution until the next real burst reproduces the same
  failure mode at a smaller (but nonzero) rate.

**Fits existing patterns:** Yes, trivially (reuses `ObjectStore` unchanged) — but "fits
existing patterns" is not the deciding factor here; solving the actual bug is.

## 2026-07-11 12:50:00 - Recommendation

### Chosen Approach: B (Postgres as an additional, opt-in backend) + Q1's Approach 1b (genuinely row-oriented storage underneath Registry, not a blob-in-a-row shim)

**Rationale:**
- Approach B is the only overall-architecture option that satisfies the explicit
  checkpoint (no forced live-infra dependency, no blast radius beyond opted-in
  clusters) while still being a real, structural fix rather than a stopgap — it is
  literally the same shape this session already validated works (tonight's
  Local/GCS/Azure dispatch extension shipped with zero regression to S3, per the
  immediately preceding brainstorm section).
- Row-oriented storage (1b), not blob-in-a-row (1a/Approach C), because the task's own
  root-cause analysis is explicit that the contention is a *sharing* problem (one file,
  many writers) — Approach C keeps the sharing and just makes retries cheaper, which
  does not match "eliminate the shared-file contention entirely" as stated in the task.
- Confirmed via direct interface inspection that this does NOT require changing the
  existing `ObjectStore`/`CubeObjectStore` interface at all — `Registry.
  updateEntryWithRetry` is private, and every public method
  (`Load`/`RenewLease`/`UpdateWatermark`/`RecordUseAndMaybeTrigger`) can be preserved
  byte-identical for callers. Only a NEW, additive construction path is needed.

**Implementation Strategy (high-level, for the planner to detail):**
1. Introduce a new **internal storage abstraction** inside `viusage`/`cube` — e.g. an
   `entryStore` interface (`LoadEntry(ctx, tenant, colHash, colType) (Entry, bool,
   error)` + `UpsertEntry(ctx, tenant, colHash, colType, mutate func(*Entry) error)
   (Entry, error)`, or similar) that `Registry`'s existing public methods delegate to
   internally, INSTEAD OF calling `updateEntryWithRetry`'s blob-specific logic directly.
2. The existing blob+`ObjectStore`+conditional-PUT-retry mechanism becomes ONE
   implementation of this new internal interface (a thin refactor of today's
   `updateEntryWithRetry`, behavior-preserving).
3. A new Postgres-backed implementation (row-per-(tenant,colHash,colType), single
   `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` or `UPDATE ... WHERE <lease/threshold
   precondition> RETURNING` statement for the atomic evaluate-and-mutate step
   `RecordUseAndMaybeTrigger` needs) becomes the second implementation.
4. `Registry` gains a new constructor (e.g. `NewPostgresRegistry(pool, tenant)`) that
   wires the Postgres implementation underneath the SAME `*Registry` type — so every
   existing caller (`RecordUseAndMaybeTrigger`, `RenewLease`, `UpdateWatermark`,
   tempo's `realUsageRecorder`/`cubeManager`) needs zero signature changes.
5. Tempo-side: extend `newViUsageObjectStoreForBackend`/`ConfigureCubeManager`'s
   dispatch with a new branch (gated on a new `Postgres *postgres.Config`, mirroring
   `s3cfg`) that calls `NewPostgresRegistry` instead of `NewRegistry(objectStore, ...)`
   — existing S3/Local/GCS/Azure branches untouched.
6. New `tempodb.Config` field (mirroring `Memcached`/`Redis`'s existing shape,
   `tempodb/config.go:66-76`) for Postgres connection config (DSN/pool
   size/timeouts), wired the same way `ConfigureViUsage` already is from tempodb
   startup.
7. Schema: one table per registry (`viusage_entries`, `cube_entries`), primary key
   `(tenant, col_hash, col_type)`, columns matching `Entry`/`BackfillState`'s fields
   1:1 (see Research Findings above for the exact field list) — NOT #467's file-metadata
   schema (explicitly out of scope per the task brief).
8. Resolve Q2's open question (bounded ring vs. new audit table) with the team lead
   BEFORE finalizing the schema — this materially changes column/table count.
9. Build (but do not run against live infra) a one-time Go importer for the real S3
   JSON data, as a reviewed artifact for eventual production use, while treating
   dev-test-03's own cutover as "start fresh" (per Q3).

**Key Decisions:**
- Preserve `Registry`'s existing public API surface entirely — the new public surface
  is limited to a new constructor + new config types, which is the minimal footprint
  consistent with "still needs explicit sign-off for new public API" while directly
  satisfying the user's own request.
- Reject Approach C (blob-in-a-row) as insufficient given the explicit root-cause
  framing, even though it's the cheapest change — flagged explicitly rather than
  silently picked for speed.
- Reject Approach A (clean-cut, delete blob path) given the CloudNativePG operator does
  not exist yet ANYWHERE in this environment and the checkpoint explicitly forbids
  treating live infra provisioning as a fait accompli.

**Risks Identified:**
- **CloudNativePG operator is not installed anywhere.** This is a real, separate,
  larger infrastructure task (3-instance HA cluster, PDB, AZ spread, WAL volume, S3
  backup target per #467's own acceptance criteria) that must be sequenced BEFORE any
  live Postgres traffic, and is explicitly gated behind the stated go/no-go checkpoint.
  Mitigate: the planner should treat "write the CNPG Cluster manifest as a
  ready-to-apply artifact" and "get explicit go/no-go to actually apply it" as two
  clearly separate plan steps, never conflated.
- **No Postgres client library vendored in tempo today** — adding one (pgx v5
  recommended: modern, actively maintained, native prepared-statement/pooling support
  via `pgxpool`) is a real `go.mod`/`go.sum`/`go mod vendor -e` change requiring the
  usual `replace ../blockpack` revendor discipline this session already follows.
- **Connection pooling/lifecycle is genuinely new for tempo.** Tempo has never held a
  live SQL connection in any process. Needs: pool sizing per process type (querier,
  query-frontend, ingester if applicable, compactor if it also creates cube/viusage
  entries), graceful shutdown (pool `Close()` wired into the same module-service
  Stop path other components use), and — because CNPG's own sizing in #467 assumes
  ~1-2 cores/2-4GB per instance — a real risk that (querier replica count) ×
  (per-process pool size) exceeds Postgres's `max_connections` budget under fleet-wide
  rollout. Mitigate: keep per-process pool size small and bounded (e.g. `pgxpool` with
  a low `MaxConns`, single-digit per process) and flag whether a connection-pooling
  proxy (e.g. pgbouncer, or CNPG's own built-in pooler mode) is needed as an explicit
  planning question, not an assumption either way.
- **Isolation level for the trigger-check-and-lease-acquire operation.** Recommend
  expressing `RecordUseAndMaybeTrigger`'s evaluate-and-mutate step as a SINGLE atomic
  SQL statement (`UPDATE ... WHERE <not Done> AND <not-triggered-or-lease-expired>
  RETURNING *`, or `INSERT ... ON CONFLICT DO UPDATE ... RETURNING *` for the
  create-if-missing case) rather than a multi-statement transaction with
  `SELECT ... FOR UPDATE` — this mirrors today's existing compare-and-swap semantics
  (read-then-conditional-write) with a single round trip and does NOT need
  `SERIALIZABLE` isolation; default `READ COMMITTED` is sufficient because the
  atomicity boundary is the single statement, not a multi-statement transaction. This
  needs explicit review during planning/implementation, not assumed correct without a
  concurrency test (mirroring this session's own "every real behavioral change needs a
  genuine, mutation-tested regression guard" convention) — specifically, a test proving
  N concurrent triggers for the SAME column converge on exactly one winning lease
  acquisition, analogous to `TestRawObjectStore_ConditionalPut_ConcurrentWritersNoLostUpdates`.
- **No local/ephemeral Postgres test pattern exists yet in either repo.** testcontainers-
  go is the most likely fit (matches Go idioms, no persistent infra, teardown per test
  run) but has zero precedent here — needs explicit confirmation it's runnable in this
  repo's CI/sandbox before the planner commits to it as THE test strategy.
- **Doc/code drift already exists independent of this task** (R4 threshold: NOTES.md
  says 3, live code comment says 1, dated today) — not this task's job to fix, but
  worth flagging so the planner doesn't accidentally treat NOTES.md's stale value as
  authoritative when deciding trigger-threshold-adjacent schema defaults.

**Open Questions (for the planner/team lead to resolve, not this brainstorm's job):**
- Q2 above: is "the list of queries" the existing bounded ring (recommended default) or
  a new unbounded audit-log capability? Get explicit confirmation before schema design.
- Exact new public API shape blockpack should expose for `NewPostgresRegistry` (pool
  type — raw `*pgxpool.Pool` vs. a blockpack-owned thin wrapper interface, to avoid
  leaking a third-party pgx type directly into blockpack's public API surface, which
  the existing `api.go`-thin-wrapper convention would likely prefer).
- Whether cube and viusage should share the SAME Postgres table-shape-generation code
  (now that "two working implementations" exist, per NOTE-VIUSAGE-1's own stated
  future-reconsideration condition) or remain fully independent per R1's existing
  practice — R1 does not forbid this, but no explicit ruling exists yet either way.
- pgbouncer/connection-pooler need — real open question, not yet answered by any
  existing precedent in this codebase.
- Whether the importer (Q3) should be a one-shot CLI tool or a startup migration path.

## 2026-07-11 13:00:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Recommendation:** Approach B (Postgres as an additional, opt-in backend, same
factory-dispatch pattern established tonight for Local/GCS/Azure) + row-oriented
storage underneath a new internal storage abstraction inside `Registry` (Q1's Approach
1b), NOT a blob-in-a-row shim (Approach C/1a) and NOT a clean-cut fleet-wide replacement
(Approach A).

**Next Phase:** PLAN

Ready for workflow-planner agent to create detailed implementation plan. Two explicit
open questions (schema shape for "the list of queries"; pgx pool wrapper API shape)
should be resolved with the team lead before the planner finalizes the schema — flagged
above, not resolved here per this brainstorm's non-interactive but honesty-preserving
mandate.

## 2026-07-11 20:55:00 - Task Received

Remove ALL full-block-scan fallback from tempo's vblockpack query paths (search, metrics,
trace-by-id, cube). Two confirmed-in-scope cases:
1. Deployment-level "VI/cube disabled" mode (vr==nil) — permanently allowed a full scan
   today; must be removed entirely. No supported deployment ever scans, for any reason.
2. Per-column/per-cube "no coverage yet" — classified as a routine decline today and
   "still falls back to a scan"; becomes a hard error with "materialized index building"
   text instead, for VI search/metrics and cube metrics.

MCP note: this task also touches blockpack. Checked my toolset for the project's MCP
server tools (`blockpack_search_modules` etc.) — NOT present in my available tools (Read,
Glob, Grep, Bash, Write, Task, TaskList, TaskGet, TaskUpdate only). Per the brainstorm
prompt's own fallback instruction, read blockpack's SPECS.md/NOTES.md-equivalent doc
comments directly via source-code `// NOTE`/`// SPEC-ROOT` tags instead, and say so
explicitly here (done).

No task-list entry of type "brainstorm" exists for this specific task (TaskList only shows
#181 Postgres jobs table and #182 cube-cardinality-gate removal, both separate, explicitly
out-of-scope-for-this-task items per the prompt). Proceeded directly from the team lead's
message + brainstorm-prompt.md rather than claiming a TaskList entry.

## 2026-07-11 21:40:00 - Research Findings

### The prior #481 work already did far more than the shallow grep suggested

The shallow grep in the prompt undersells how much of this is already done. Reading
value_index_query.go, cubequerypath.go, backend_block.go, value_index_structural_query.go,
decline_response.go, slice_errors.go, and blockpack's api.go/reader.go/queryoptions.go in
full (not just grep hits) shows:

- **blockpack.ExecuteTraceMetrics (the metrics scan engine) was already deleted outright**
  in #481 (api.go:556-576, `ExecuteMetricsTraceQL`). When `opts.ValueIndex == nil`
  (vr==nil, case 1) it returns `ErrMetricsValueIndexDisabled` directly — **there is no scan
  fallback left in blockpack for metrics at all**, for either case 1 or case 2. Every
  metrics decline (shape-not-answerable, no-coverage, legacy-block, VI-disabled) is already
  a typed sentinel error, never a scan, all the way through
  `backend_block.go:QueryRange` → `DeclineErrorToHTTPResponse`.
- **Trace-by-id (`FindTraceByID`/`GetTraceByID`) already has zero scan fallback**
  (NOTE-VI-071/072/073, confirmed in blockpack/reader.go:401-505). `scanTraceByID` and
  `getTraceByIDFullScan` (named in the prompt's checklist) **do not exist anywhere in the
  current codebase** — grepped, zero hits. They were already deleted in an earlier issue
  (#473 follow-up). This part of checklist item 1 is stale/hypothetical.
- **Cube's own fallback branch (`tryQueryFromCube`) does not itself scan.** It returns
  `(nil, false, err-or-nil)` on every "cannot answer" path; the caller
  (`backend_block.go:QueryRange`) falls through to the VI/metrics path, which — per the
  point above — never scans either. The package doc comment "Falls back to the full block
  scan on any error or cache miss" (cubequerypath.go:11) is **stale**, describing
  pre-#481 behavior; same for value_index_query.go's several "falls back to a full block
  scan" doc-comment references (lines 6, 80, 214, 227, 390, 516) and rawfilestore.go:113 —
  all describe scan-fallback framing that metrics has already eliminated. These need a doc
  pass regardless of what else changes (doc/code drift, not just a nice-to-have — a future
  reader trusting these comments would materially misunderstand current behavior).

**What this means for scope: metrics and trace-by-id are almost entirely a WORDING/
sentinel-classification problem, not a scan-removal problem.** The real, live, unremoved
full-scan mechanisms are narrower and all in the **search (Fetch)** path plus two
unclassified generic errors in trace-by-id.

### Case 1 (vr==nil) — where a REAL, live, unconditional scan still exists

`backend_block.go:tryIndexFetch`'s vr==nil branch returns via `declineOutcome` (not
`declineOutcomeBounded`), which is **unconditional** `(nil, false, stats, nil)` regardless
of `boundedAuthorized`/limit (value_index_query.go:407-424, explicit R8 doc comment: "the
vr==nil zeroth category is UNCHANGED... KEPT"). Back in `Fetch` (backend_block.go:963-976),
this deliberately leaves `needsBoundedRead = false`, so control falls through to the
**unconditional** `blockpack.QueryTraceQLWithProgram`/`blockpack.QueryTraceQL` call
(lines 1084-1099) — a genuine, real, full block scan. This exact behavior is pinned by a
still-passing regression test named, literally,
**`TestFetch_VIDisabled_StillFullScans_Unchanged_WithLimit`**
(fetch_bounded_dispatch_test.go:225) and its sibling `..._Unchanged` (no limit, line 194).
The structural path has the identical R18-ruled twin branch (backend_block.go:1025-1045,
`case !opts.IndexOnly && getValueIndexQueryReader() == nil`) — same unconditional scan,
same "deliberate no-op, falls through" comment.

This is THE real, unambiguous case-1 finding: **`Fetch`'s search path (filter AND
structural) is the one place a genuinely unconditional, "by design, permanent" full scan
still exists today.** Removing it means: when `getValueIndexQueryReader() == nil`,
`Fetch` must return the new hard error instead of falling through to the final
`QueryTraceQLWithProgram`/`QueryTraceQL` switch — for both the filter branch (line ~963)
and the structural branch (line ~1025).

Also case 1 for cube: `getCubeQueryPath() == nil` (cube feature not configured) simply
skips the cube block silently in `QueryRange` and falls through to VI/metrics — which, per
the finding above, already errors correctly when VI is ALSO nil. **Metrics side of case 1
needs no behavior change**, only decline_response.go wording (see below). Cube's own
"disabled" state has no separate scan risk since it was never itself a scan mechanism.

Also case 1 for trace-by-id: `GetTraceByID`'s `lister == nil` guard
(blockpack/reader.go:431-435) already hard-errors — **but with a bare, un-typed
`fmt.Errorf`**, not one of blockpack's exported sentinels. Traced the whole chain: this
error propagates up through `backend_block.go:FindTraceByID` (line 608,
`fmt.Errorf("GetTraceByID: %w", err)`) into `modules/querier/http.go`'s `handleError`,
which calls `DeclineErrorToHTTPResponse` — which has **no `errors.Is` case for this at
all** (decline_response.go has no trace-by-id entries whatsoever), so it silently falls to
the default 500 branch. **No scan happens (already correct), but the error text is neither
"materialized index building" nor even a coherent 4xx — it's an opaque 500.** This is a
real gap in scope: needs a new typed sentinel from blockpack + a new
`DeclineErrorToHTTPResponse` case.

### Case 2 (per-column/per-cube "no coverage yet") — genuinely ambiguous, needs an explicit decision

`tryIndexFetch`'s OTHER decline branches (build error, no coverage, index declined) route
through `declineOutcomeBounded` (value_index_query.go:426-452):
- `!boundedAuthorized` (no limit present): **already** hard-errors with
  `ErrSearchNoCoverage` — no scan, already matches the task's intent behaviorally. Only the
  message text needs to change to include "materialized index building".
- `boundedAuthorized` (a limit IS present): relays `(nil, false, stats, nil)`, and
  `Fetch` sets `needsBoundedRead = true`, which activates
  `queryOpts.RecentFirstBudget` and calls the SAME
  `QueryTraceQLWithProgram`/`QueryTraceQL` functions — but bounded (Direction=Backward,
  capped at `MaxBlocks=50`/`MaxBytes=64MB`/`MaxDuration=2s`, defaultBoundedRecentFirstPolicy,
  backend_block.go:43-64). **This DOES read real block bytes directly (not the index) when
  there's no coverage** — but the team's own vocabulary (queryoptions.go's
  `RecentFirstBudget` doc comment, and literally the test-file doc comment in
  fetch_bounded_dispatch_test.go:10-11: "routes to F-2's/F-3's bounded newest-first path and
  succeeds (**never scans**, never hard-errors)") explicitly does NOT call this "a scan" —
  it's a deliberate, previously-team-lead-ruled (R2/R7/R9/R17), tested, "honest budgeted
  answer" strategy, distinct from the unconditional exhaustive scan case 1 describes.

  **This is the one place in the whole investigation where the brainstorm-prompt's plain-
  English framing and the codebase's own internal vocabulary genuinely disagree.** The
  prompt's case 2 says: "classified as a routine decline... and STILL falls back to a
  scan... becomes a real error instead of a scan" — which matches this bounded-read
  branch's *externally observable behavior* (touches raw block data instead of the index
  when uncovered) even though the code's own comments insist it "never scans." I read the
  prompt's explicit, blunt framing ("if we dont have indexes OR cubes then return an error...
  that code shouldnt exist") as intentionally overriding the earlier #481 R2/R7/R9/R17
  rulings for this specific branch, not as a term-of-art match. **Flagging this explicitly
  rather than silently picking a side — the planner/team lead should confirm this reading
  before implementation**, because reversing it is large and has a severe secondary
  consequence (next section).

### SEVERE risk if bounded-newest-first is removed: structural search may become non-functional outside slice jobs

Read value_index_structural_query.go in full. `tryStructuralIndexFetch`'s FIRST line:
`if !indexOnly { return nil, false, stats, nil }` (line 53-58) — **the index-driven
structural path is categorically skipped for every normal (non-#487-slice-job) query**,
by an existing, unrelated, already-documented limitation (plan-d.md DT1: multi-file trace
materialization means `ExecuteStructuralFromIndex` has no per-block ownership restriction,
so answering structural queries outside the narrow slice-dispatch model would return
duplicate answers per overlapping block — this is a pre-existing dispatch-safety gate, not
something case 1/2 touches).

Consequence: **every ordinary (non-time-sliced) structural TraceQL query (`{A} >> {B}`,
etc.) today ALWAYS declines the index path** and reaches `backend_block.go`'s
`compiledProgram == nil` branch, which — for a query with a limit — sets
`needsBoundedRead = true` and gets its answer from the bounded-newest-first scan
(backend_block.go:1061-1062, `if blockpack.IsStructuralQuery(query) && boundedAuthorized`).
**If bounded-newest-first is removed, every ordinary structural query with a limit starts
hard-erroring with the new "materialized index building" text — permanently, since DT1's
ownership gate means structural coverage can never come from the index outside a slice job
either.** This would make structural TraceQL search effectively non-functional for normal
(non-slice) dispatch, not just "degraded until backfill catches up" — backfill can never
fix it, because the index-driven structural path is architecturally gated off for this
dispatch shape regardless of coverage. This needs to be called out as a **blocking
question for the team lead**, not silently implemented: either (a) accept this regression
as within the explicitly-accepted interim-availability tradeoff (structural search stops
working outside slice jobs until DT1 is separately resolved), or (b) scope the
bounded-newest-first removal to exclude the structural branch until DT1 lands, or (c)
resolve DT1 first. This is NOT something #481's own review anticipated, since DT1 postdates
it (issue #489).

### Cube metrics case 2

`tryQueryFromCube`'s "not found" branch already returns `ErrCubeWarming` (not a scan) and
fires `maybeCreateCube` async. This is ALREADY error-not-scan behaviorally. The open
question is purely wording: `ErrCubeWarming`'s message ("cube not yet backfilled...retry
shortly") is deliberately distinguished (R1's self-healing story) from a permanent decline
because a retry after backfill will genuinely succeed — unlike case 1's vr==nil, which
never resolves without operator action. Whether this message should also literally contain
"materialized index building" (the user's exact requested text) or keep its more specific,
actionably-different wording is a wording decision for the plan, not a behavior change.

### Cross-cutting risk (checklist item 3): backend_block.go/wal_block.go shared-scan-function risk — CONFIRMED REAL, but scoped narrowly

`wal_block.go`'s `Fetch` (line 292-349+) calls `blockpack.QueryTraceQL` directly and
**unconditionally** on the WAL snapshot — no `tryIndexFetch`, no `vr` check, nothing. This
is entirely correct and MUST NOT be touched: WAL blocks are the live, pre-compaction,
in-memory/local-disk ingest buffer (`walBlock` type) — structurally, permanently
un-indexable (there's no such thing as a value index over data that hasn't been flushed to
a completed block yet). `blockpack.QueryTraceQL`/`QueryTraceQLWithProgram` themselves are
NOT "the scan fallback mechanism" being removed — they're the general-purpose block-query
engine, legitimately used unconditionally by WAL blocks and used (soon-to-be-conditionally,
post-fix) by `backend_block.go`'s completed/indexed blocks. **The fix must be scoped to the
call sites in `backend_block.go`'s `Fetch` (the two `getValueIndexQueryReader() == nil`
branches), never to `QueryTraceQL`/`QueryTraceQLWithProgram` themselves or to
`wal_block.go`.** This is exactly the kind of shared-function risk the checklist warned
about, and it is real, but the actual required change is narrowly scoped once traced end
to end — not a risk that blocks the plan, just a boundary to state explicitly so an
implementer doesn't try to gate the shared query engine itself.

### Multiple independent "decide scan-or-not" sites (checklist's own recurring risk class)

Enumerated every site that currently permits or would permit a scan/bounded-scan for
case 1 or case 2, tracing real control flow (not grep):

1. `value_index_query.go:tryIndexFetch`'s vr==nil branch → `declineOutcome` (filter search,
   case 1) — real unconditional scan reached via backend_block.go Fetch fallthrough.
2. `value_index_structural_query.go:tryStructuralIndexFetch`'s `!indexOnly` guard AND its
   vr==nil branch inside `backend_block.go`'s `case !opts.IndexOnly &&
   getValueIndexQueryReader() == nil` (structural search, case 1) — same mechanism, R18's
   twin of #1.
3. `value_index_query.go:declineOutcomeBounded`'s `boundedAuthorized` branch (filter search,
   case 2) — routes to bounded-newest-first, the ambiguous one above.
4. `backend_block.go`'s inline `blockpack.IsStructuralQuery(query) && boundedAuthorized`
   check (structural search, case 2) — same bounded-newest-first mechanism, structural
   twin of #3, with the SEVERE DT1 consequence above.
5. `blockpack.GetTraceByID`'s `lister == nil` guard (trace-by-id, case 1) — already errors,
   but untyped; needs a sentinel, not a behavior change.
6. `blockpack.getTraceByIDViaIndex`'s `len(keys) == 0` guard (trace-by-id, case 2) — same:
   already errors (NOTE-VI-072), untyped, needs a sentinel.
7. Metrics (`ExecuteMetricsTraceQL`'s `opts.ValueIndex == nil` branch, case 1, and its VI
   decline sentinels, case 2) — already fully converted to errors; wording-only.
8. Cube (`tryQueryFromCube`'s not-found branch) — already error-not-scan; wording-only
   decision on ErrCubeWarming's text.

Sites #1/#2 are unambiguous case-1 fixes. #5/#6 need new blockpack sentinels (typed, not
behavior). #3/#4 are the ambiguous, high-risk bounded-newest-first question. #7/#8 are pure
wording. This enumeration directly answers checklist item 4 ("find them ALL, a grep isn't
enough") — there is no additional undiscovered scan site; the four files named in the
prompt (value_index_query.go, cubequerypath.go, decline_response.go/slice_errors.go/
value_index_structural_query.go/rawfilestore.go) plus backend_block.go/wal_block.go and
blockpack's reader.go/api.go/queryoptions.go together cover the complete blast radius.

### blockpack-side API surface (checklist item 5)

Confirmed by tracing actual return-value contracts, not assuming:

- **New typed sentinel errors needed in blockpack** for `GetTraceByID`'s two currently-bare
  `fmt.Errorf` sites (lister==nil "caller error", and zero-candidate-index-files "coverage
  gap") — both are new exported symbols (e.g. `blockpack.ErrTraceByIDIndexNotConfigured`,
  `blockpack.ErrTraceByIDCoverageGap`), a genuine new public API surface addition requiring
  explicit sign-off per this session's blockpack convention. This is a small, low-risk,
  clearly-scoped addition (two new `var Err... = errors.New(...)` lines plus wrapping them
  at the two call sites instead of `fmt.Errorf(...)`).
- **If (and only if) the bounded-newest-first removal is confirmed in scope**: a much
  larger blockpack-side removal — `RecentFirstBudget` (queryoptions.go, exported public
  type) and every internal consumer (query_traceql.go, internal/modules/executor/stream.go,
  stream_structural.go, collectoptions.go, options.go, recentfirst.go, structuralresult.go,
  structural_funnel_stats.go, api.go's F-3 threading). This is a genuine public-API removal
  (deleting an exported type), and per this session's "no backwards compat but EXPLICIT
  sign-off for public API changes" convention, needs its own explicit go/no-go — distinct
  from and much larger than the sentinel addition above.
- Metrics's existing sentinels (`ErrMetricsValueIndexDisabled`, `ErrMetricsNoCoverage`,
  `ErrMetricsShapeNotAnswerable`, `ErrMetricsLegacyTimeSecZero`) need **no blockpack
  change** — they already exist and already error correctly; only tempo's own
  `decline_response.go` message strings need to change, which is entirely tempo-side.
- Cube's `ErrCubeWarming` and search's `ErrSearchNoCoverage`/`ErrSliceIndexCoverageGap` are
  already tempo-local sentinels (deliberately not blockpack's, per decline_response.go's
  own doc comment on why) — no blockpack change needed there either, purely tempo-side
  message/sentinel-family additions.

**Net: blockpack changes are small and low-risk for cases 1/2's core scope (two new
sentinels), UNLESS the bounded-newest-first question resolves toward "in scope," in which
case blockpack changes become the largest single piece of this task.**

### Doc/code drift found (not blocking, but should be fixed as part of this task's cleanup)

- `decline_response.go:72` claims "There is no longer an explicit opt-out setting
  (2026-07-11...)" for `ErrMetricsValueIndexDisabled` — but `ConfigureValueIndexQuery`/
  `ConfigureValueIndexQueryRaw` (value_index_query.go:84-147) both still explicitly accept
  and handle a nil client/reader (`viQueryReaderPtr = nil`), and `value_index_query.enabled`
  is still referenced as a live config flag in 4 other places in the same file. Read this as
  describing a narrower claim (an explicit operator "off switch" was removed when a backend
  IS configured) rather than "vr==nil can no longer happen" — vr==nil clearly still happens
  today whenever no backend at all is configured, which is exactly case 1. Confirmed this
  reconciles with (and does not contradict) the separate, already-shipped, same-day
  backend-agnostic VI/cube work (Local/GCS/Azure via `ConfigureValueIndexQueryRaw`) — that
  work made VI mandatory-once-configured, not "always configured regardless of backend
  presence." Still, the comment as written is misleading; worth a one-line fix while
  touching this file anyway.
- cubequerypath.go:11's package doc "Falls back to the full block scan on any error or
  cache miss" and value_index_query.go's several "falls back to a full block scan" doc
  references (lines 6, 80, 214, 227, 390, 516) and rawfilestore.go:113 — all stale relative
  to #481's actual ExecuteTraceMetrics deletion. Recommend updating these comments as part
  of this task's own diff (they directly describe the mechanism this task changes).

## 2026-07-11 22:10:00 - Approaches Considered

### Approach A: Minimal — fix only the unambiguous unconditional scans (case-1 search/structural), leave bounded-newest-first alone, wording pass everywhere else

Change only sites #1/#2 above (Fetch's vr==nil branches) to hard-error instead of falling
through to the scan switch. Add the two blockpack trace-by-id sentinels. Update
decline_response.go message text to use "materialized index building" phrasing across the
existing sentinel family (ErrSearchNoCoverage, ErrSliceIndexCoverageGap, ErrCubeWarming,
the metrics F-4 sentinels, the two new trace-by-id sentinels). Leave `RecentFirstBudget`/
bounded-newest-first untouched — case 2's "still falls back to a scan" is interpreted as
already resolved by #481's own vocabulary (it's "bounded," not "a scan").

**Pros:** Small, safe, no cross-repo public API removal, no risk to structural search's
only working path outside slice jobs, ships fast. Directly satisfies the LITERAL "never do
a full scan" (there is no more UNCONDITIONAL/exhaustive scan anywhere) while keeping the
"honest, budgeted, limit-authorized" answer path the prior team-lead ruling deliberately
carved out as not-a-scan.
**Cons:** Does not satisfy the prompt's plainest reading of case 2 ("STILL falls back to a
scan... becomes an error instead") if "falls back to a scan" is read literally
(touches raw block bytes instead of the index) rather than per the codebase's internal
jargon. Risks the team lead judging this an incomplete/hedged implementation of explicitly
confirmed scope.

### Approach B: Full — also remove bounded-newest-first (RecentFirstBudget) entirely, converting every case-2 decline (with or without a limit) into a hard error

Additionally removes `boundedAuthorized`/`needsBoundedRead`/`newBoundedRecentFirstBudget`
from backend_block.go, deletes `RecentFirstBudget` from blockpack's public API and every
consumer (stream.go, stream_structural.go, collectoptions.go, options.go, recentfirst.go,
structuralresult.go, structural_funnel_stats.go, api.go, query_traceql.go), and cleans up
the now-vestigial `DispatchBoundedRecentFirst` frontend Strategy value (search_sharder.go,
metrics_query_range_sharder.go, structural_sharder.go, vcnt_fetch.go, dispatch_events.go —
confirmed search_sharder.go already treats it identically to ordinary block-sharded fanout,
so this cleanup is not itself behavior-changing at the frontend, just dead-code removal).

**Pros:** Matches the prompt's plainest, most literal reading of case 2 and the user's own
blunt framing ("that code shouldn't exist"). Removes an entire strategy family in one pass
rather than leaving a partially-reversed feature. Simplifies backend_block.go materially
(deletes ~150 lines of boundedAuthorized/needsBoundedRead plumbing and its doc comments).
**Cons:** Large, genuinely double-repo blast radius (10+ blockpack files, multiple tempo
files, multiple existing regression tests need deleting/rewriting —
`TestFetch_LowSelectivityNoCoverageLeaf_WithLimit_UsesBoundedRecentFirst_NotScan`,
`TestFetch_SelectiveShapedQuery_PerBlockBuildError_WithLimit_UsesBoundedRecentFirst`,
`TestFetch_ThreeNodeStructuralChain_WithLimit_UsesBounded`, `recentfirst_budget_test.go`,
`structural_dispatch_test.go`'s bounded cases — these test NAMES literally assert the
opposite of the new behavior and must be rewritten, not just left red). Reverses specific,
recent, deliberate team-lead rulings (R2/R7/R9/R17) — should not be done silently. **Makes
ordinary (non-slice-job) structural TraceQL search with a limit permanently error** (the
severe DT1 consequence above) unless DT1 is separately resolved first or the structural
branch is explicitly carved out as an exception.

### Approach C: Full removal of bounded-newest-first, EXCEPT explicitly carve out the structural branch as a documented, temporary exception until DT1 resolves

Same as B for the filter/search path, but leaves `tryStructuralIndexFetch`'s bounded
fallback (`IsStructuralQuery(query) && boundedAuthorized`) alone, with an explicit
NOTE-VI-XXX-style comment tying it to DT1's own resolution as the removal trigger.

**Pros:** Satisfies case 2's literal reading for the (much more common) filter/attribute
search path — the overwhelming majority of real Tempo search traffic — while not silently
breaking structural search's only working non-slice-job path.
**Cons:** A visibly inconsistent rule ("no scan fallback anywhere, except here") that needs
strong justification in the PR description and risks looking like scope creep/hedging if
not explicitly blessed by the team lead. Still carries most of B's blockpack-side blast
radius, since `RecentFirstBudget` itself can't be deleted while the structural branch still
uses it — only the FILTER path's consumption of it goes away, so the actual code-deletion
win vs. B is smaller than it looks (the type and its executor-side plumbing all stay; only
backend_block.go's filter-path call site changes).

## 2026-07-11 22:20:00 - Recommendation

### Chosen Approach: A now, with an explicit escalation path to B/C as a SEPARATE, clearly-labeled follow-on decision

Implement Approach A (the unambiguous, unconditional case-1 scans in Fetch's filter and
structural branches, plus the two new blockpack trace-by-id sentinels, plus a
decline_response.go wording pass unifying every relevant existing sentinel's message to
include "materialized index building" phrasing) as this task's actual deliverable.

**Do NOT silently fold the bounded-newest-first removal into this task's plan.** Present
the case-2/bounded-newest-first question (Approach A vs. B vs. C) to the team lead as an
EXPLICIT, standalone go/no-go decision before the planner writes a single line touching
`RecentFirstBudget`, `boundedAuthorized`, or `needsBoundedRead` — because:
1. The prompt's own wording is genuinely ambiguous against the codebase's own established
   vocabulary (a rare case where "read the actual code" and "read the plain-English
   instruction" point in different directions).
2. The blast radius if "yes, remove it" is 3-5x larger than case 1 alone (10+ additional
   blockpack files, a public API removal needing its own sign-off, several tests that
   assert the OPPOSITE of the new behavior by name).
3. It has a severe, non-obvious secondary consequence (structural search effectively
   stops working outside slice jobs, permanently, due to the unrelated pre-existing DT1
   gate) that the user almost certainly was not aware of when giving the blunt "no scan,
   ever" directive, and that deserves an explicit acknowledgment either way.

**Rationale:** this mirrors #481's own review discipline (per this session's `feedback_
mutation_test_review`-style caution and the checklist's own "the ONE call site that was
top-of-mind is never the only one" warning) — the safest thing to do with a genuinely
ambiguous, high-blast-radius reading is surface it explicitly for a real decision, not
silently pick the smaller-effort reading (A) and call it done, nor silently pick the more
literal reading (B/C) and blow past a severe secondary consequence the user hasn't seen.

**Implementation Strategy (for Approach A, ready for the planner regardless of the case-2
decision):**
1. tempo: `backend_block.go`'s `Fetch`, filter branch (~line 963) — when
   `getValueIndexQueryReader() == nil`, return the new hard error immediately instead of
   falling through to the final `QueryTraceQLWithProgram`/`QueryTraceQL` switch.
2. tempo: `backend_block.go`'s `Fetch`, structural branch (~line 1025-1045,
   `case !opts.IndexOnly && getValueIndexQueryReader() == nil`) — same fix, R18's twin.
3. tempo: introduce one new tempo-local sentinel, e.g. `ErrMaterializedIndexBuilding`
   (slice_errors.go, alongside `ErrSliceIndexCoverageGap`/`ErrSearchNoCoverage`), used by
   both new hard-error sites above. Decide (with the team lead) whether this REPLACES
   `ErrSearchNoCoverage` outright (since both now mean "no index configured/no coverage,
   period") or SITS ALONGSIDE it as case 1's distinct sentinel while `ErrSearchNoCoverage`
   remains case 2's (no-limit) sentinel — recommend the latter (distinct sentinels, unified
   message wording) since case 1 (deployment never configured) and case 2 (configured but
   this specific column/window lacks coverage) are operationally different situations an
   operator would want to distinguish in logs/metrics, even if the HTTP-facing text is
   nearly identical.
4. tempo: `decline_response.go` — add the `ErrMaterializedIndexBuilding` case; update
   `ErrSearchNoCoverage`'s existing message text to include "materialized index building"
   phrasing; leave `ErrCubeWarming`'s distinct "retry shortly" framing as a DELIBERATE,
   noted exception (self-healing case, not "we don't have cubes" in the permanent sense) —
   flag this exact wording choice for team-lead confirmation, don't silently decide it.
5. blockpack: add two new exported sentinels for `GetTraceByID`'s two bare-`fmt.Errorf`
   sites (reader.go:431-435, reader.go:487-492) — needs explicit blockpack public-API
   sign-off per this session's convention, but this is a small, self-contained addition.
   Wire them through tempo's `decline_response.go` with the same "materialized index
   building" wording.
6. Doc cleanup: fix the stale "falls back to a full block scan" comments enumerated above
   (cubequerypath.go, value_index_query.go, rawfilestore.go, decline_response.go's
   "no longer an explicit opt-out" line) in the same diff, since this task's own PR
   description will otherwise contradict the comments it's sitting next to.
7. Mutation-tested regression guards (per this session's own standing convention):
   `TestFetch_VIDisabled_StillFullScans_Unchanged` and
   `TestFetch_VIDisabled_StillFullScans_Unchanged_WithLimit` (fetch_bounded_dispatch_test.go)
   assert the EXACT OPPOSITE of the new behavior by name — these must be rewritten (not
   just deleted) to assert the new hard-error outcome, with a real revert-and-confirm-red
   cycle, not just a green diff. Same for the structural R18 twin case in
   `structural_dispatch_test.go` if a comparable named test exists there (not yet
   confirmed — planner/coder should grep for it before assuming it doesn't).

**Key Decisions:**
- Treat case 1 (Fetch's two unconditional vr==nil scan sites) as unambiguous, in scope, and
  ready to implement without further confirmation.
- Treat case 2's bounded-newest-first branch as requiring an EXPLICIT team-lead decision
  before any blockpack `RecentFirstBudget` code is touched — this is the single most
  consequential open question in this brainstorm.
- Keep metrics/trace-by-id changes to wording + two new small typed sentinels — no scan
  removal needed there, since #481 already fully eliminated metrics' scan engine and
  trace-by-id never had one to begin with (post NOTE-VI-073).
- Never touch `wal_block.go` or the shared `blockpack.QueryTraceQL`/`QueryTraceQLWithProgram`
  functions themselves — they are legitimately used unconditionally by WAL's structurally
  un-indexable live buffer; the fix is scoped to `backend_block.go`'s call sites only.

**Risks Identified:**
- **Accepted, already-noted interim-availability tradeoff** (per the prompt): until the
  separate Postgres jobs-table/fast-backfill pipeline lands, any not-yet-indexed/cubed
  query pattern returns the new hard error with only today's slow, poll-based (10-minute
  default) backfill as the recovery path. User has explicitly accepted this on this
  dev-trial cluster. Recorded for the record only, not re-litigated.
- **NEW risk, not previously flagged anywhere in this session:** if case 2's
  bounded-newest-first removal is approved, ordinary (non-slice-job) structural TraceQL
  search becomes permanently non-functional whenever it declines the index (which is
  ALWAYS, today, for every non-slice-job structural query, per DT1's pre-existing
  dispatch-safety gate) — this is a materially different, likely much worse regression
  than "wait for backfill," since backfill can never fix it. Must be explicitly surfaced
  and either accepted or scoped around (Approach C) before implementation, not discovered
  during review.
- **Doc/code drift risk (lower severity):** several stale "falls back to a full scan"
  comments already exist post-#481; if left unfixed while ALSO adding new, contradictory-
  sounding hard-error behavior right next to them, a future reader (or reviewer) will have
  a materially harder time trusting either the old or new comments. Cheap to fix in the
  same diff; flagged so it's not treated as separately deferrable cleanup.
- **Sentinel-family fragmentation risk:** tempo already has three decline-error "families"
  (tempo-local: ErrSearchNoCoverage/ErrSliceIndexCoverageGap/ErrCubeWarming; blockpack
  metrics: the F-4 sentinels; blockpack structural: ErrStructuralIndexCoverageGap) plus two
  NEW additions this task proposes (ErrMaterializedIndexBuilding, two trace-by-id
  sentinels). Recommend the planner produce one canonical table (sentinel → owning repo →
  HTTP status → exact message text) as part of the plan itself, mirroring #481's own F-4/
  F-10 documentation discipline, so `decline_response.go`'s switch doesn't silently drift
  from this task's own design intent the way `cubequerypath.go`'s stale doc comment already
  drifted from #481's.

**Open Questions (for the team lead, not resolved here):**
- Does case 2 include the bounded-newest-first mechanism (Approach B/C) or not (Approach
  A)? This is the single biggest open question in the whole investigation.
- If bounded-newest-first is removed: accept structural search's permanent non-
  functionality outside slice jobs (until DT1 separately resolves), or carve out an
  explicit structural exception (Approach C), or resolve DT1 first (out of scope /
  much larger, not assessed here)?
- Should `ErrSearchNoCoverage` be replaced outright by the new
  `ErrMaterializedIndexBuilding` sentinel, or kept as a distinct case-2-only sentinel
  alongside it?
- Should `ErrCubeWarming`'s message literally contain "materialized index building" text,
  or keep its more specific "creation triggered, retry shortly" wording as a deliberate,
  noted exception?
- Two new blockpack public sentinels for GetTraceByID (small) — confirmed needed; still
  needs the usual explicit blockpack public-API sign-off per this session's standing
  process, not a rubber-stamp just because the change is small.

## 2026-07-11 22:22:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Recommendation:** Approach A (the two unconditional vr==nil scan sites in `Fetch`, plus
two new blockpack trace-by-id sentinels, plus a decline_response.go wording/sentinel-family
unification pass) as this task's concrete deliverable — with the bounded-newest-first
question (Approach B/C) explicitly escalated to the team lead as a standalone decision
before any of that larger, cross-repo, public-API-removing work is planned or implemented.
**Next Phase:** PLAN, gated on the team lead's answer to the bounded-newest-first question
above — the planner should not need to re-derive any of this investigation, but SHOULD
block on that one open question before finalizing scope.

## 2026-07-11 23:10:00 - Follow-up Task Received: investigate DT1 (issue #489) properly

Team lead's scope decision: resolve DT1 (the per-block ownership gap in
`ExecuteStructuralFromIndex` that gates `tryStructuralIndexFetch` to `indexOnly`-only) as
part of THIS task, so structural queries get real index coverage outside slice jobs and
bounded-newest-first removal becomes safe for structural too, not just an accepted
regression. Investigated properly rather than assuming a single obvious fix shape exists.

**Headline correction to my first-pass finding: DT1 is already resolved, and my earlier
claim that "every ordinary (non-slice-job) structural query always declines the index
path" was WRONG.** I conflated "reachable only through a #487 slice job" with "reachable
only through a narrow, rarely-exercised construct." Digging into `vcnt_fetch.go`'s
`buildQueryPlanFromProgram` and blockpack's `internal/modules/queryplan/queryplan.go`
shows `DispatchTimeSliced` — the dispatch mode structural's already-landed Option B fix
(`structuralTimeSlicedJobsFunc`/`buildStructuralTimeSlicedBackendRequests`,
structural_sharder.go) rides on — is **not** a special opt-in flag reserved for some
separate "#487 feature." It is the ordinary outcome for the *majority* of coverage-eligible
search traffic, filter and structural alike.

### How DT1 actually got resolved (plan-d.md, worktree `blockpack-worktrees/read-path-modernization/.bob/state/plan-d.md`)

Read the full rulings log. Two resolutions were weighed for the real bug (issue #489):
`ExecuteStructuralFromIndex`/`QueryStructuralFromIndex` have no per-block ownership/
`sourceRef` restriction — a structural match's spans can live in ANY block reachable
through the tenant's `backend.Reader`, so dispatching one job per `(block, slice)` pair (the
existing filter/metrics model) would have N overlapping blocks each independently compute
and return the FULL answer for a slice — verified real duplication, not mere redundant
cost, by reading `traceql.anyCombiner.AddMetadata`/`combineSearchResults` (merges by
TraceID across job responses, never discards a "duplicate").

- **(A) output-ownership filter on `ExecuteStructuralFromIndex`** — rejected: every block
  overlapping a slice would still have to compute the FULL answer just to filter down to
  its own shard, baking in universal N× COMPUTATION duplication, defeating #489's own
  performance purpose.
- **(B) — RULED IN, implemented, confirmed live in the current tempo checkout:** a
  structural query's `DispatchTimeSliced` plan dispatches **exactly ONE job per slice, no
  per-block fan-out at all** (`structuralTimeSlicedJobsFunc`, structural_sharder.go:94-140;
  `firstOverlappingBlock` just picks any one nominal HTTP carrier block — the querier never
  reads that block's own data for a structural query). Verified the cross-slice-straddle
  risk this required checking (a trace whose matches split across two slices) was itself
  checked against `combineSearchResults`'s real behavior (ties keep the existing side, no
  double-count) before being wired in.

**This fix is complete and already shipped in the code I read** (`structural_sharder.go`,
`search_sharder.go:235-264`) — DT1, in its original plan-d.md scope, is not outstanding
work.

### The real gate: which structural queries reach DispatchTimeSliced (and thus the real index answer) vs. DispatchBoundedRecentFirst (and thus today's bounded-newest-first scan)

Traced `buildQueryPlanFromProgram` (vcnt_fetch.go:223-357) and
`queryplan.SelectSearchStrategy`/`BuildQueryPlan` (queryplan.go) end to end — this function
is SHARED verbatim between the filter and structural paths (`buildStructuralQueryPlan`,
structural_sharder.go:36-51, tail-calls it with the structural query's left leg). Key
mechanics, confirmed by direct code read, not assumed:

1. `CheckIndexCoverage` gates everything — if it fails, `plan == nil` for either query
   type, dispatch falls to ordinary block-sharded fanout, and `Fetch`'s `opts.IndexOnly`
   is false, so `tryStructuralIndexFetch`'s own `if !indexOnly { return nil, false, stats,
   nil }` guard skips the index path unconditionally. This is genuine case-2 "no coverage
   at all" — correctly in scope for the current task's hard-error conversion.
2. If coverage exists, `SelectSearchStrategy(sel, hasLimit)` is consulted — but its
   `DispatchBlockSharded` return value (Selective+anyLimit, or UnknownSelectivity+noLimit)
   is a **throwaway** in this caller: control falls through unconditionally to
   `BuildQueryPlan`, whose ONLY Strategy gate is `allLeavesResolvable` (already true, since
   step 1 passed) — so `BuildQueryPlan` reports `DispatchTimeSliced` for BOTH of those
   rows. Only `SelectSearchStrategy`'s two `DispatchBoundedRecentFirst` rows (LowSelectivity
   +limit, UnknownSelectivity+limit) short-circuit BEFORE reaching `BuildQueryPlan`
   (vcnt_fetch.go:307-315), and the `LowSelectivity+noLimit` row hard-declines at plan time
   (`ErrPlanTimeLowSelectivityNoLimit`, pre-dispatch, no job ever built).
3. Net: **`DispatchTimeSliced` — and thus a REAL index-driven structural answer via
   `tryStructuralIndexFetch`'s `indexOnly==true` branch — is what a coverage-eligible
   structural query gets whenever it's classified Selective (any limit) or
   UnknownSelectivity-with-no-limit.** Only the narrower `LowSelectivity+limit` /
   `UnknownSelectivity+limit` combination reaches `DispatchBoundedRecentFirst`, which
   dispatches via ordinary per-block fanout (IndexOnly=false, search_sharder.go:291-303 —
   there is no dedicated dispatch branch for this Strategy, it falls through identically to
   `DispatchBlockSharded`/nil) — and it is ONLY in this narrower combination that
   structural's `IsStructuralQuery(query) && boundedAuthorized` bounded-scan branch
   (backend_block.go:1061) is the sole available answer mechanism today.

4. **Bonus, previously-unverified confirmation: under `DispatchTimeSliced`/`indexOnly==true`
   dispatch, structural ALREADY hard-errors on any per-slice decline today** —
   `value_index_structural_query.go`'s `structuralDeclineOutcome` converts EVERY decline
   reason (build error, no leg coverage, genuine per-slice coverage gap despite the
   tenant-wide classification) into `blockpack.ErrStructuralIndexCoverageGap`, never a
   scan or bounded fallback, regardless of cause. **This dispatch mode already matches the
   current task's target behavior exactly — there is nothing left to change here.**

### What this means for "is DT1 the blocker:" it is not — the real remaining gap is shared with the filter path, and is a different problem than DT1

The ONLY place structural still depends on bounded-newest-first is the
`LowSelectivity+limit` / `UnknownSelectivity+limit` classification. Checked
`queryplan.VCNTCostFunc`/`VCNTColumnTotalFunc`/`ClassifyProgramVCNT` (vcnt_cost.go) to
confirm what these classifications actually measure: `Selectivity` is a
count/column-total FRACTION — "what share of this column's live values does this leaf's
equality value match" — genuinely **orthogonal to whether a VI/cube index exists at all**.
`LowSelectivity` means the index DOES have full, real coverage for this leaf; it's just
judged too EXPENSIVE to resolve (matches a large fraction of the column, so the VI lookup
would return/download a huge candidate set). `UnknownSelectivity` means VCNT (a SEPARATE,
coarser cardinality-summary structure used only for cost estimation) has no population
data for this leaf/column — again orthogonal to whether the VI itself has real coverage.

**This directly means the current task's proposed replacement ("materialized index
building" — implying the index doesn't exist yet / is still being backfilled) would be
factually WRONG for this case.** A `LowSelectivity+limit` query's index is fully built and
covers the query completely; DispatchBoundedRecentFirst exists purely as a
performance/cost escape hatch, not a coverage gap. Answers item 5 of the follow-up ask
directly: **yes, something else legitimately relies on bounded-newest-first besides the
no-coverage scenario** — a fully-covered-but-expensive-to-resolve query does too, and it is
NOT structural-specific — the exact same `SelectSearchStrategy` table and the exact same
`DispatchBoundedRecentFirst` dispatch model gate the FILTER path identically (this is the
literal shared function/table both paths consume).

Checked plan-d.md's own explicit finding (recorded during #489's original design,
"L-resolution data-flow trace" section) for whether any bounded/capped index-resolution
primitive exists anywhere in blockpack that a fix here could reuse: **confirmed there is
none** — `vibuilder.BuildSource` resolves every leaf's full candidate set unconditionally
and intersects post-hoc; there is no "resolve a narrowed candidate set, stop early, cap
cost" mode for the value index at all, for filter or structural queries. This was flagged
explicitly in plan-d.md's own ruling 2 refinement as newly-designed-from-scratch work
(D3B) even for the NARROWER "confirm a specific candidate set" case — a genuine "bound the
cost of resolving a low-selectivity leaf in the first place" primitive is a different,
harder, larger problem that D3B doesn't solve either.

### Direct answers to the follow-up task's 5 questions

1. **Why duplicates happen mechanically:** `ExecuteStructuralFromIndex` answers a query's
   entire `[Start,End)` window from VI+TraceGroup data reachable through the tenant's whole
   `backend.Reader`, not scoped to one block's own bytes — dispatching it once per
   overlapping block (the filter path's model) means N blocks each compute and return the
   SAME full answer, and tempo's combiner merges rather than dedupes across job responses.
2. **Actual fix shape:** already decided and shipped — Option B, one job per slice, no
   per-block fan-out (`structuralTimeSlicedJobsFunc`). Not a per-block ownership/dedup pass
   (Option A was explicitly rejected for reintroducing universal N× computation).
3. **Blast radius of the ALREADY-LANDED fix:** `structural_sharder.go` (new file, tempo),
   `search_sharder.go` (dispatch branch), `value_index_structural_query.go` (the `indexOnly`
   gate this dispatch model requires) — all tempo-side; no further blockpack change was
   needed for DT1 itself (`QueryStructuralFromIndex`/`ExecuteStructuralFromIndex` already
   accept a window-scoped, not block-scoped, query — DT1 was a DISPATCH problem, not an
   executor problem).
4. **Honest sizing for what's ACTUALLY still open (not DT1 — the shared
   low/unknown-selectivity-with-limit gap):** this is real, new, cross-cutting design work,
   not a contained fix. It requires inventing a bounded-cost index-resolution primitive
   that does not exist anywhere in blockpack today (confirmed absent, not merely
   unoptimized), touching `internal/modules/vibuilder` (BuildSource's unconditional
   full-resolution behavior) and/or `internal/modules/executor` (structural's own
   leg-resolution calls), PLUS both tempo dispatch paths (search_sharder.go's
   `DispatchBoundedRecentFirst` handling, backend_block.go's
   `boundedAuthorized`/`needsBoundedRead`) if the goal is to retire bounded-newest-first
   for BOTH query types. Rough sizing: **days, not hours** — comparable in scope to a
   scaled-down #487/#489 combined effort (a new plan-time classification path, a new
   querier-side execution mode, a new blockpack-side capped-resolution primitive, its own
   dedicated correctness test suite for "does the cap actually bound cost without
   silently under-reporting" — exactly the kind of property that needs a real regression
   test, mutation-verified, not "looks right"). It is NOT contained, and it is NOT specific
   to structural queries or to this task's stated scope (filter/metrics search decline
   removal) — it would benefit the filter path equally and independently of anything
   structural-specific.
5. **Does DT1 being fixed make bounded-newest-first removal unambiguously safe for
   structural (and filter)?** **No.** DT1's fix already fully covers the dispatch mode
   where bounded-newest-first would otherwise have been structural's fallback for a
   per-block-duplication reason — that mode (`DispatchTimeSliced`) already hard-errors on
   decline today, with no scan/bounded-read involved at all. The mode that STILL depends on
   bounded-newest-first (`DispatchBoundedRecentFirst`, low/unknown-selectivity+limit) is
   untouched by DT1's fix and shares its root cause with the filter path, not with
   structural's per-block ownership problem. Fixing DT1 further (there is nothing left to
   fix in DT1's own scope) does not move this forward at all.

## 2026-07-11 23:25:00 - Revised Recommendation (supersedes the DT1-related caveat in the first pass, does not change Approach A)

**Retract the "severe risk: structural search becomes non-functional outside slice jobs"
framing from my first pass — it was based on an incomplete read of the dispatch code.**
Corrected picture: structural search already gets real, hard-error-on-decline (never
scan) index-driven answers for the Selective/UnknownSelectivity-no-limit majority of
coverage-eligible traffic, via the already-shipped DT1 fix. The genuine remaining
dependency on bounded-newest-first (`LowSelectivity+limit` / `UnknownSelectivity+limit`) is
symmetric across filter AND structural, is a cost/performance escape hatch rather than a
coverage gap, and has NO existing bounded-cost index-resolution alternative to fall back
to in either repo today.

This means the Approach A/B/C framing from my first pass needs one refinement: **the
open question is no longer "should structural be carved out from bounded-newest-first
removal" (Approach C) — DT1 already makes structural's OWN worst case a non-issue.** The
real open question is unchanged in substance but now correctly scoped: **should
`DispatchBoundedRecentFirst` (the low/unknown-selectivity-with-limit escape hatch, shared
identically by filter and structural) be removed**, knowing that doing so safely — i.e.,
without simply making every such query hard-error, including ones over data that IS fully
indexed — requires inventing a genuinely new, unscoped, multi-day, cross-cutting bounded-
cost index-resolution capability that does not exist today. If the team lead wants this
removed WITHOUT that new capability, the honest characterization is "these particular
queries stop being answerable at all until backfill... resolves differently than a
coverage gap" is not quite right either — a `LowSelectivity` query's coverage is real and
complete; removing `DispatchBoundedRecentFirst` without a replacement means these queries
become **permanently** unanswerable via the index (not "wait for backfill" — there's
nothing to backfill; the shape/cost is inherent to the query), which is a materially
different and arguably worse tradeoff than case 1/2's "materialized index building, retry
later" framing implies. Recommend surfacing this distinction explicitly rather than
letting "materialized index building" wording paper over it.

**Recommendation stands: ship Approach A now** (the unconditional vr==nil scan sites in
`Fetch`, unaffected by any of this DT1/selectivity investigation). Treat "retire
`DispatchBoundedRecentFirst`/bounded-newest-first" as its own, separately-scoped,
multi-day follow-up task — not blocked on or unlocked by DT1 (already done), and not
structural-specific (equally a filter-path question). If the team lead wants to proceed
with removing it anyway without the new capability, the plan should use precise wording
for this specific case ("query shape too costly to answer via the index" or similar) rather
than reusing "materialized index building," which would misdescribe a fully-covered
query as if it were an indexing-coverage gap.

## 2026-07-12 00:05:00 - Follow-up Task Received: feasibility of early-stopping/incremental value-index resolution (retiring DispatchBoundedRecentFirst for real)

User's scope confirmed by team lead: no case should ever be "too expensive to serve" via
the index — retire `DispatchBoundedRecentFirst` (filter AND structural) entirely by
building real early-stopping/incremental index resolution (walk entries newest-first, stop
once `limit` matches found) instead of resolving the full candidate set unconditionally.
Critical feasibility question before planning: does the CURRENT value-index file format
support incremental/ordered consumption, or does this need a format change?

**Answer: NO format change is needed. The hard infrastructure for exactly this already
exists in production, built for a different reason (cost/bandwidth, not ordering) — issue
#488/B-4/B-5. What's missing is resolution-loop logic layered on top of it, not a new wire
format.** Read the full on-disk layout (bucketfile.go's own file-layout doc comment),
`DiscoverIndexFiles`/`ParseFilenameV2`/`SortFileMetas` (discovery.go/filename.go), and the
already-shipped ranged/partial-read path (`QueryBucketFileRanged`, bucketquery_ranged.go;
its caller `queryKeysRanged`/`lookupColumn`, vibuilder/builder.go) rather than assuming.

### 1. How entries are actually stored, and whether ordered/incremental reads are possible without materializing everything

Three levels of ordering metadata, ALL already on disk today, none requiring a format change:

- **File level:** the wall-clock time range is embedded directly in the FILENAME itself
  (`L<level>-<wallMinSec>-<wallMaxSec>-<id>.blockpack`, `FormatFilenameV2`/`ParseFilenameV2`,
  filename.go). A caller can sort or filter candidate files by recency from the LIST
  response alone — zero bytes of the file itself need to be read to know its time range.
  `DiscoverIndexFiles` (discovery.go) already parses this and calls `SortFileMetas`, which
  today sorts `(Level ASC, WallMinSec ASC, WallMaxSec ASC)` — freshest COMPACTION LEVEL
  first, but OLDEST-time-first within a level. That specific ordering was chosen for an
  unrelated reason (traverse least-merged files first); newest-first-by-time is a
  different, equally cheap sort key over the exact same already-parsed metadata — a pure
  comparator change (or new sibling sort function, to avoid touching the existing
  Level-first callers), not a discovery mechanism change.
- **Block level, within one file:** each file has a block directory (`BlockDirEntry`,
  bucketfile.go) carrying `MinTimeSec`/`MaxTimeSec` PER BLOCK, readable from a single
  small ranged read (footer, then directory) WITHOUT fetching any block body.
  `QueryBucketFileRanged` (bucketquery_ranged.go) **already reads exactly this way in
  production today**: footer ReadAt → prune whole file → directory ReadAt → prune
  individual blocks by time/value BEFORE fetching their bodies → ranged body ReadAt only
  for surviving blocks. This is the literal "incremental, ordered, partial consumption"
  capability the question asked about, and it already exists, already tested, already the
  live path (`vibuilder.lookupColumn`/`lookupColumnAll` call it via `queryKeysRanged` —
  confirmed by direct code read, not the doc comment alone). What it does NOT do today is
  choose which block to read FIRST for recency — it iterates the directory in on-disk
  array order. Iterating it in `MaxTimeSec`-descending order instead is a sort/reverse over
  the same in-memory `[]BlockDirEntry` slice already fetched — no new I/O shape.
- **Group level, within one decoded block:** `BucketGroup`s are written sorted
  `(TimeSec ASC, CanonicalValue ASC)` (`sortBucketBlock`, bucketfile.go — "canonical
  ordering makes encode deterministic and merge-join compaction possible," i.e. this ASC
  on-disk order is relied on elsewhere and should NOT be changed at encode time). Reading
  a decoded block's `Groups` slice in REVERSE (in memory, after decode) gives newest-first
  within that block at zero cost — the data is already sorted, just the wrong direction for
  this specific consumer; no re-encode needed.

**Net for Q1: yes, ordered/incremental consumption without materializing the full
candidate set is already possible today, at every level (file, block, and — via a trivial
in-memory reversal — group), using metadata that already exists on disk.** The one
genuine architectural mismatch (not a format problem, a concurrency-model problem — see Q2)
is that `queryKeysRanged` (vibuilder/builder.go) fans out to EVERY candidate file/key
CONCURRENTLY today (bounded by `downloadConcurrency`), specifically to hide S3 latency —
the opposite of "process in priority order and stop once satisfied." Retrofitting
early-stop means changing this from all-at-once fan-out to a staged/batched,
priority-ordered fan-out (fetch the newest batch, check the accumulated count, only fetch
the next batch if still short) — a real concurrency-model change, though a well-understood
pattern, not a new one.

### 2. Sizing: resolution-logic-only (confirmed the applicable scenario, given Q1's answer)

Since Q1 confirms no format change is needed, this IS the applicable scenario. Concretely
new work, traced through the real call chain rather than guessed:

- **blockpack, `internal/modules/valueindex`:** a newest-first variant of
  `SortFileMetas` (or a parameter) for file-level ordering; a "stop after N results" budget
  threaded through `QueryBucketFileRanged`'s per-directory-entry loop (bucketquery_ranged.go:88-107)
  — the loop is ALREADY sequential and ALREADY has natural break points per iteration, so
  adding an early-return once a caller-supplied budget is reached is small, localized code;
  reversing in-block group consumption order in `matchGroupsInBlock` (bucketquery.go) or a
  sibling for the ordered case.
- **blockpack, `internal/modules/vibuilder`:** the real design work is here —
  `queryKeysRanged`'s all-at-once `errgroup`+`SetLimit` fan-out (builder.go:661-710+) needs
  restructuring into a staged/batched form: sort keys newest-first, fan out to a bounded
  batch, check the running total against the caller's limit, only proceed to the next batch
  if still short. `lookupColumn`/`lookupColumnAll` and ultimately `BuildSource` need a new
  limit-aware entry point (additive sibling functions, not a signature break, to avoid
  disturbing every existing non-limited caller) that threads a limit down to this batched
  loop and returns as soon as it's satisfied.
- **blockpack, structural path (`internal/modules/executor`, D3/D4/D6):** meaningfully
  HARDER than the filter path, not just "the same pattern twice." Structural resolution
  needs the LEFT leg's candidates (walk anchor) AND, per D3B's own already-documented
  "verify remaining AND-group leaves against candidates" step, real per-candidate
  confirmation against the right leg/tree structure — "stop once the left leg has N
  candidates" does NOT mean "stop, you have N final structural matches," since
  verification can reject some candidates. An early-stopping structural resolver needs its
  own escalation logic (resolve a batch of left-leg candidates newest-first, verify them,
  only fetch more left-leg candidates if verification yields fewer than `limit` confirmed
  matches) — a genuinely new control loop, not a copy of the filter path's.
- **tempo (`tempodb/encoding/vblockpack`, `modules/frontend`):** a new plan.Strategy (or a
  repurposed `DispatchTimeSliced`-like signal) replacing `DispatchBoundedRecentFirst`;
  removal of `backend_block.go`'s `boundedAuthorized`/`needsBoundedRead`/
  `newBoundedRecentFirstBudget` plumbing and the raw-block-scan budget policy; a new querier
  code path that calls the new limit-aware blockpack entry point instead of
  `QueryTraceQLWithProgram`/`QueryTraceQL` with `RecentFirstBudget` set.
- **Tests, both repos:** a genuinely NEW correctness property that has never been tested
  anywhere in either repo — "given a synthetic index with more than `limit` matches spread
  across multiple files/blocks in a KNOWN chronological arrangement, the early-stopping
  path returns the correct NEWEST N matches, not an arbitrary or silently-wrong subset."
  `RecentFirstBudget`'s own existing tests exercise RAW BLOCK read order/budget exhaustion,
  not index-entry chronological correctness — this needs its own dedicated suite,
  mutation-verified per this session's standing convention (reintroduce an
  off-by-one/wrong-direction bug in the new sort/early-stop logic, confirm the new test
  actually goes red, then confirm green after the real fix).

**Rough sizing: filter-path early-stopping (file+block-level batching, new vibuilder entry
point, new tempo dispatch wiring, new test suite) is on the order of 2-4 days of focused
work — genuinely new control-flow and concurrency-model work, not a small tweak, but built
on top of already-existing, already-proven ranged-read infrastructure, which is the main
reason this is NOT week(s)-scale. Structural's own escalation/verification-aware version
is ADDITIONAL work on top of that, not included in the 2-4 day estimate — plan on it being
comparably sized to the filter-path piece by itself, given the new verification-aware
control loop, so a combined, honest estimate for BOTH paths plus tests plus tempo wiring
is closer to 4-7 days, not "a couple of hours."**

### 3. Format-change scenario

Not applicable — Q1 confirmed the current format already carries everything needed
(filename-embedded file time range, per-block directory time range, deterministic on-disk
group sort that's cheaply reversible in memory). Recorded here only per the follow-up
task's own instruction to size this branch explicitly: had the format lacked ANY
recency-ordering metadata at all (e.g. an older hypothetical format with no directory,
requiring a full sequential body scan to find block boundaries), a real format change would
need a new on-disk block directory (a migration/dual-read concern — old files without a
directory would need either a lazy backfill pass or permanent dual-code-path support) and
would reasonably be sized in WEEKS, not days, given blockpack's own file-format-change
discipline (compat story, migration, every existing writer/reader/compactor call site).
This does not apply here — flagged only for completeness per the ask, not as a live risk.

### 4. Is newest-first actually the right/cheapest priority order?

Yes, confirmed directly rather than assumed: BOTH ascending and descending traversal cost
exactly the same at every level examined. File-level ordering is a pure in-memory sort over
already-parsed filename metadata (no I/O either way). Block-level ordering is a
pure in-memory sort/reversal over an already-fetched, already-small directory (no I/O
either way — the actual body ReadAt calls are the only real cost, and they're identical in
count regardless of visitation order). Group-level ordering within a decoded block is a
trivial in-memory reversal of an already-decoded slice. **There is no cheaper alternative
order the storage layout would favor — newest-first is free to implement and matches
users' actual expectations (recent data first) and bounded-newest-first's own existing UX
contract**, so there is no behavior-difference tradeoff to accept here; this is a clean win,
not a compromise.

## 2026-07-12 00:20:00 - Revised recommendation on retiring DispatchBoundedRecentFirst

Given Q1-Q4's findings, I'm upgrading my prior recommendation ("treat this as a vague,
open-ended, multi-day follow-up, don't scope it now") to something more concrete and
actionable: **this is buildable, format-safe, and reasonably well-bounded in size (4-7
days including structural and tests) — it is a real, planned task, not a research
question requiring its own separate brainstorm cycle first.** The planner can size this as
its own phase (mirroring #487/#489's own multi-task-group structure: file/block-level
ordering primitives → vibuilder batched/limited resolution → tempo dispatch rewiring →
structural's verification-aware escalation → removal of the old RecentFirstBudget/
DispatchBoundedRecentFirst machinery → the new mutation-tested correctness suite), rather
than being deferred as "too speculative to plan yet."

**This does NOT change the recommendation for the CURRENT task's own deliverable.** Ship
Approach A (the unconditional vr==nil scan removal in `Fetch`) as this task's own,
smaller, independently-shippable piece — it has no dependency on the
early-stopping-resolution work and shouldn't wait for it. Whether the team lead wants to
sequence the DispatchBoundedRecentFirst retirement as a follow-on PHASE of THIS SAME task
(given the user's now-stated "no case should ever be too expensive to serve" scope) or as
a genuinely separate, subsequently-scheduled task is a sequencing call for the planner/team
lead — both are now real options with a concrete size estimate behind them, which is the
main thing this investigation was blocked on.

## 2026-07-12 01:10:00 - Follow-up Task Received: concrete implementation design for early-stopping resolution

User confirmed (authoritative): every value-index file is stored by time. Team lead asked
me to verify this against actual code (not take it on faith) and design the concrete
change. Verified — and it checks out even more precisely than a general "stored by time"
claim would suggest, at every level. Also found a real, previously-undiscovered
correctness landmine that materially affects sizing — reported honestly below rather than
smoothed over.

### 1. Verifying the claim against real code

Confirmed three independent, compounding facts, each read directly (not inferred):

- **File level:** `SplitIntoBlocks` (bucketmerge.go:143-174, the compaction repartition
  step) explicitly `sort.Slice`s the full merged group list by `(TimeSec ASC, ValueASC)`
  BEFORE chunking it into fixed-size blocks by contiguous slice range (`all[start:end]`).
  This means **block N+1 in the on-disk block directory always covers strictly later time
  than block N** — the directory is chronological by construction, not merely
  chronologically filterable. Iterating a file's block directory in REVERSE array order
  (last entry to first) gives newest-first block traversal at literally zero cost — not
  even a sort, just a reversed loop.
- **L0 (freshest, unmerged) files are single-block** (`writer.go:338-347`,
  `Blocks: []BucketBlock{block}`) — the common, freshest case has no block-ordering
  question at all; the only ordering that matters for an L0 file is the in-block group
  order (see below) and file-level recency (see below).
- **File-level ordering** (already found in the prior feasibility pass, re-confirmed here
  against `filecache.go`'s actual doc comment): `IndexFileCache.FilesForTimeRange`
  "returns the matching keys sorted exactly as `DiscoverIndexFiles` would" — i.e.
  `(Level ASC, WallMinSec ASC, WallMaxSec ASC)`, parsed straight from the filename with zero
  body I/O.
- **In-block group order:** `sortBucketBlock` (bucketfile.go) writes `Groups` sorted
  `(TimeSec ASC, ValueASC)`. Reversing an already-decoded `[]BucketGroup` slice in memory
  gives newest-first at zero cost.

**Verdict: the user's claim checks out, and more precisely than "stored by time" alone
implies — every level (file, block, and in-block group) is chronologically ordered on
disk today, and newest-first traversal is achievable via array reversal or a trivial sort
over already-small, already-in-memory metadata at every level. No new on-disk metadata is
needed anywhere.**

### 2. Concrete design: threading limit + direction through the resolution loop

**vibuilder side (`internal/modules/vibuilder/builder.go`) — the real work:**
- `queryKeysRanged` (builder.go:661+) is the call site that needs the biggest change: it
  currently fans out to EVERY candidate key CONCURRENTLY via `errgroup.SetLimit(downloadConcurrency)`
  with no ordering or early-stop. New design: sort `keys` newest-first first (reverse of
  today's discovery order, or a new discovery variant), then process in small batches
  (e.g. `downloadConcurrency`-sized) SEQUENTIALLY across batches — fan out concurrently
  WITHIN a batch (preserving most of the existing latency-hiding benefit), check the
  running total against the caller's limit AFTER each batch completes, stop before
  starting the next batch once satisfied.
- `QueryBucketFileRanged` (bucketquery_ranged.go:36-109) needs a limit-aware sibling (or an
  additive parameter) that iterates `dir` in REVERSE (see point 1) and returns early once
  the running match count (threaded in from the caller, since one file can satisfy the
  whole limit) is reached.
- `lookupColumn`/`lookupColumnAll` (builder.go:492-593) need a limit-aware sibling that
  threads the limit down into the new batched `queryKeysRanged`.
- `BuildSource` (builder.go:127-249) needs a limit-aware sibling (`BuildSourceBounded` or
  similar) that threads a limit down to `lookupColumn` for the SINGLE-LEAF case (see the
  correctness landmine below for why multi-leaf is not simply "the same, plus a parameter").

**Call sites that currently call `BuildSource` unconditionally and would need the
limit-aware variant threaded through, enumerated by direct grep, not assumed:**
`tempodb/encoding/vblockpack/value_index_query.go`'s `tryIndexFetch` (the filter search
path) and `CheckIndexCoverage`; `value_index_structural_query.go`'s
`tryStructuralIndexFetch`/`tryNegatedStructuralIndexFetch` (both legs); `backend_block.go`'s
metrics path (`BuildValueIndexSourceForMetrics`, a sibling entry point, not `BuildSource`
itself, but the same underlying `lookupColumn`/`queryKeysRanged` machinery — metrics is
`count_over_time`/`rate` only, so its own early-stop question is different: it needs a
COUNT, not a set of matches, so "stop once you have enough" doesn't even apply the same way
— flagging as a separate, likely out-of-scope-for-this-phase question, not silently
assuming it's covered by the same fix).

**Structural side — a genuinely different, and in one respect SMALLER, story:**
`ExecuteStructuralFromIndex`/`evalOneStructuralCandidateTrace` (structural_index.go) turn
out to **already have limit-aware early-stopping**, contrary to what I'd have guessed —
`evalOneStructuralCandidateTrace` (line 322-324) already checks
`opts.Limit > 0 && len(result.Matches) >= opts.Limit` and signals `done=true`, and its
caller's own candidate loop (`ExecoteStructuralFromIndex` line 202-216) already `break`s on
`done`. **This existing mechanism was built for a different reason (bounding
per-candidate materialization/verification cost) but is directly reusable for real
newest-first early-stopping — PROVIDED `candidateTraceIDs` is fed to it in newest-first
order, which it is NOT today** (see the landmine below). So structural's own escalation
loop needs LESS new code than I estimated in the prior pass — the missing piece is
upstream ordering, not a new loop.

### 3. Correctness landmine (found by direct code read, not assumed) — this is the important part

**The SAME landmine appears independently in both the filter path and the structural
path: every point where multiple leaf/candidate sets get combined today sorts by IDENTITY
(span key or TraceID bytes) for algorithmic reasons, discarding whatever time order fed
into it.** This is exactly this session's own recurring "multiple independent places
decide/construct the same kind of thing and can drift" bug class, caught here BEFORE
implementation rather than after.

- **Filter path:** `viEvalNodes`/`viIntersectSorted`/`viUnionSorted`
  (metrics_trace.go:222-299+) implement AND-intersect/OR-union as a streaming merge-join
  over sets sorted by the 24-byte **span key** (TraceID++SpanID), explicitly NOT by time —
  this is a real, load-bearing design choice (NOTE-VI-040/#430) that makes the merge-join
  O(n+m) instead of a hash-map materialization. **This means: even if each individual
  leaf's OWN VILookupResult set were resolved newest-first from vibuilder, the very next
  step (multi-leaf AND/OR combination) re-sorts by identity and throws that order away
  before `QueryTraceQLFromIndex` ever sees it.**
- **Structural path:** `groupVILookupResultsByTrace`/`chooseDiscoverySeed`
  (structural_index.go:329-356) group VI matches into a `map[[16]byte]...` (Go map, no
  order) and then explicitly `sort.Slice`s the resulting TraceID list
  **lexicographically by TraceID bytes** for determinism — again explicitly NOT by time.
  Same underlying problem, independently present.

**Why this matters for correctness, not just efficiency:** for a SINGLE-leaf query
(`{span.foo = "bar"}`), early-stopping the one leaf's own resolution newest-first is
straightforwardly correct — there's nothing to combine against. For a multi-leaf **OR**
query, early-stopping is still tractable via a k-way merge-by-recency across leaves (a
well-understood, correct pattern — resolve each leaf newest-first, merge-sort by time,
stop once the union reaches the limit). For a multi-leaf **AND** query, early-stopping is
genuinely hard: independently truncating each leaf to "its own newest N" and then
intersecting can SILENTLY UNDER-REPORT true matches — a match that's within the true
top-N of the INTERSECTION can easily be outside the top-N of one individual leaf's own
truncated set, if that leaf alone has much lower selectivity than the intersection as a
whole. This is not a hypothetical edge case; it's the general case whenever two leaves have
meaningfully different selectivity. **Confirmed via plan-d.md's own already-recorded
finding (my earlier DT1 investigation) that a "resolve the lead leaf, then confirm the
narrowed candidate set against the remaining leaves" mode does not exist ANYWHERE for
plain filter queries today** — this is exactly the shape needed to correctly early-stop a
multi-leaf AND query (resolve the cheaper/anchor leaf newest-first with early-stop, then
confirm — not independently truncate and intersect — the other leaf(s) against that
already-narrowed candidate set), and it would need to be BUILT for the filter path,
mirroring structural's own D3B pattern rather than copying it (D3B is structural-specific
plumbing, not directly reusable, but the DESIGN PATTERN transfers).

### 4. Removability of `DispatchBoundedRecentFirst` once this lands, and file-by-file sizing

**tempo side, once the vibuilder+structural-ordering work lands:**
- `value_index_query.go`: `declineOutcomeBounded`'s `boundedAuthorized` branch, and
  `tryIndexFetch`'s own `boundedAuthorized` parameter, become dead — replaced by a call
  into the new limit-aware `BuildSource` sibling instead of a decline.
- `backend_block.go`: `boundedAuthorized`, `needsBoundedRead`,
  `newBoundedRecentFirstBudget`, `boundedRecentFirstPolicy` (lines 43-75, 884-976,
  1075-1099) — all removable. The final `switch { case indexAnswered: ...; case
  compiledProgram != nil: QueryTraceQLWithProgram(...); default: QueryTraceQL(...) }` stops
  needing a `RecentFirstBudget`-carrying `queryOpts` variant.
- `value_index_structural_query.go`: once `candidateTraceIDs` ordering is fixed
  upstream, the `IsStructuralQuery(query) && boundedAuthorized` branch (backend_block.go
  line 1061) becomes dead — structural's own already-existing limit-early-stop takes over.
- **Frontend:** `vcnt_fetch.go`'s `DispatchBoundedRecentFirst` branch (line 307-315) and
  `SelectSearchStrategy`'s two `DispatchBoundedRecentFirst` rows (queryplan.go) become
  unreachable/removable — search_sharder.go's fallthrough dispatch (lines 291-303)
  collapses to just the nil/`DispatchBlockSharded` case.
- **blockpack:** `queryoptions.go`'s `RecentFirstBudget` type/field, and every
  consumer enumerated in my first-pass brainstorm (query_traceql.go, stream.go,
  stream_structural.go, collectoptions.go, options.go, recentfirst.go,
  structuralresult.go, structural_funnel_stats.go, api.go's F-3 threading) — fully
  removable, a genuine public-API deletion needing explicit sign-off.

**Confirmed: once single-leaf AND multi-leaf-OR early-stopping land (with multi-leaf-AND
either solved via the new anchor+confirm pattern OR explicitly still declined to the new
"materialized index building"/no-safe-answer error rather than a scan), nothing else
legitimately depends on `DispatchBoundedRecentFirst` — it is fully removable, not just
mostly.**

**Honest, file-level sizing, revised from the prior pass now that the design is concrete:**
- Single-leaf-only early-stopping (vibuilder batching/ordering, new `BuildSource` sibling,
  tempo dispatch wiring for the common case): **~2-3 days**, roughly matching my prior
  estimate.
- Multi-leaf OR (k-way merge-by-recency across leaves): **~1-2 additional days** — a
  clean, well-understood algorithm, but new code and its own test.
- Multi-leaf AND (the anchor-leaf-early-stop + narrowed-confirmation pattern, new for the
  filter path): **~2-3 additional days** — this is the piece I did not have concretely
  scoped in the prior pass; it is real new design, not a copy of an existing pattern,
  though it can lean on the COST-ESTIMATION machinery (VCNT-based selectivity) that
  already exists to choose which leaf is the anchor.
- Structural's own fix (ordering `candidateTraceIDs` by per-trace max recency instead of
  TraceID bytes, in `chooseDiscoverySeed`/`intersectTraceIDSets`) is now smaller than my
  prior estimate, since the escalation loop itself already exists: **~1 day**.
- New mutation-tested correctness suite covering all of the above (see Q5): **~1-2 days**.

**Revised combined estimate: 7-11 days for full correctness across single-leaf, OR, and
AND, plus structural, plus tests — larger than the prior pass's 4-7 days, because the
prior pass had not yet found the AND-query landmine.** A team lead wanting to ship sooner
has a real, honest option: ship single-leaf + OR early-stopping first (killing
`DispatchBoundedRecentFirst` for the majority of real traffic), and keep multi-leaf-AND
queries either on a SEPARATE, narrower, explicitly-labeled decline (not "materialized index
building" — a distinct "query too complex for bounded resolution" reason, since it's
neither a coverage gap nor quite the same low-selectivity-cost story) for a follow-on
phase, rather than blocking the whole removal on solving the hardest sub-case first.

### 5. Test shape for the core correctness property

The property to prove is **not** "the early-stopped result looks plausible" — it is
**"early-stopping resolution returns EXACTLY the same top-N-by-recency result a full
resolution would have produced, just without touching the rest."** Concretely:

- Build a real fixture (via the actual write path — `blockpack.NewWriter`/
  `WriteValueIndexL0`, mirroring task #12's own "MANDATORY new test class... never satisfied
  by a hand-built `VILookupResult`" precedent from the DT1 investigation) with MORE matches
  than `limit`, spread across multiple files AND multiple blocks within a file, at KNOWN,
  distinct timestamps.
- Run the EXISTING full (unbounded) resolution path and record its complete, correct
  match set as the oracle.
- Run the NEW early-stopping path with `limit` set below the true match count.
- Assert the early-stopped result is **exactly** the oracle's top-`limit` entries by
  recency (not a random/arbitrary subset of the right size) — this is the part a
  "looks right" test would miss: an early-stop implementation with a subtly wrong sort
  direction, or that stops one batch too early/late, or that mis-orders across a
  file/block boundary, would still return the RIGHT COUNT of results, just the WRONG ones —
  only an exact-set comparison against a real oracle catches that.
- Mutation-verify per this session's standing convention: flip the sort direction (oldest-
  first instead of newest-first) and confirm the test goes red; revert and confirm green.
  Repeat for an off-by-one in the batch-boundary early-stop check.
- A SEPARATE test for the AND-query landmine specifically: two leaves with deliberately
  different selectivity (one leaf's matches are a strict superset of the true intersection,
  much larger and differently time-distributed), confirming the anchor+confirm design does
  NOT silently drop a true intersection match that would have been outside a naively-
  truncated independent-leaf top-N — this is the test that would have caught the landmine
  itself had it been built with the naive "just add a limit and intersect" approach, so it
  needs to be written from a place of already knowing the failure mode, not discovered
  after the fact.

## 2026-07-12 02:00:00 - Follow-up Task Received: does Phase 5's early-stopping make it safe to relax structural's `!indexOnly` dispatch guard?

Team lead/planner flagged a real distinction to re-examine, not assume: DT1's fix (one job
per slice) is about per-BLOCK dispatch duplication; Phase 5's early-stopping is about
per-CANDIDATE resolution order/cost. Are these actually independent, such that
early-stopping does NOT change whether `!indexOnly` (value_index_structural_query.go:53-58)
can be relaxed for ordinary (non-slice-job) structural dispatch? Investigated concretely,
taking care not to default to caution OR to a convenient "should be fine now" answer
without tracing the actual mechanism.

### Answer: relaxing the guard is NOT safe. The duplication risk is orthogonal to resolution order — confirmed, not assumed, by tracing exactly what each axis controls.

**1. Why per-block dispatch duplication happens, mechanically (re-verified against the real
mechanism, not re-derived from memory):** `ExecuteStructuralFromIndex` has NO per-block or
`sourceRef` output restriction — Option A's multi-file trace materialization (D3) means a
structural match's confirmed spans can legitimately live in ANY block reachable through the
tenant's whole `backend.Reader`. The function does not know or care which block "carries"
the HTTP request; given a window `[minTS, maxTS]`, it always tries to answer that ENTIRE
window. If dispatched one-job-per-block (today's `DispatchBlockSharded`/`backendJobsFunc`
model), N blocks overlapping the query's time range would each independently invoke this
SAME function over the SAME window and each get back the (deterministic, given the same
underlying VI/TraceGroup state) SAME answer — not a partition of it. This is a property of
**how many jobs get created and what window each one answers** — a DISPATCH-layer decision,
made by `search_sharder.go` before any querier-side resolution begins — not a property of
how any one job's resolution algorithm works internally.

**2. Does early-stopping change this calculus? No — traced through explicitly, both
directions:**
- Early-stopping changes ONLY the cost/duration of computing ONE job's answer (stop once
  enough newest-first confirmed matches are found, instead of exhaustively walking every
  candidate). It does not, and structurally cannot, change HOW MANY JOBS get dispatched or
  WHICH WINDOW each job is asked to answer — that decision is made entirely upstream, at
  plan/dispatch time, before the querier (and therefore before any early-stopping logic)
  ever runs. If the guard were relaxed so ordinary per-block dispatch could reach
  `ExecuteStructuralFromIndex`, N blocks would still each independently be asked to answer
  the SAME whole-window query — early-stopping just makes each of those N redundant
  invocations cheaper, not fewer or partitioned. The fundamental defect DT1 exists to
  prevent — N-fold duplicated computation for one logical answer — is unchanged in kind,
  only (possibly) reduced in raw cost-per-duplicate.
- **If anything, early-stopping introduces a NEW, narrower wrinkle that makes relying on
  the combiner's incidental dedup even less safe, not more:** without early-stopping, N
  independent full-resolution invocations over the identical window would produce
  byte-identical, fully-deterministic answers (every VI file/TraceGroup entry visited,
  every legitimate match confirmed) — the combiner's `combineSpansets` tie-break (replace
  only on STRICTLY higher `Matched` count, tie keeps existing) would harmlessly collapse N
  identical responses into one with no data loss, by luck of exact equality, though still
  paying N× the compute cost for nothing (this is the "wasted cost, not necessarily wrong
  data" half of the concern, and matches how plan-d.md's OWN Option-A rejection reasoning
  is phrased — see below). WITH early-stopping, "stop once you have enough" makes each
  job's answer depend on incidental resolution/verification ORDER at the exact
  boundary of the limit (e.g., which VI files a per-tenant `IndexFileCache` happened to
  have cached at the moment each of the N separate jobs ran, or nondeterministic
  goroutine-completion order in D3B's own verification fan-out) — two independent
  invocations over the "same" window are no longer guaranteed to return the exact same
  top-K set at the margin. This does not make the combiner produce WRONG results in any
  new way I can point to concretely (it would just pick whichever job's response happened
  to have the higher `Matched` count, still a valid-if-different top-K), but it removes the
  one property (exact byte-identical N-way duplication) that made the old, already-rejected
  "just let it duplicate, the combiner will sort it out" framing even incidentally safe. It
  is a reason to be MORE cautious about relaxing the guard now, not less.

**3. Re-read plan-d.md's own original rejection of Option A (the per-block ownership
filter) with this specific question in mind — does early-stopping revive it?** Its
rejection reasoning is explicitly a WASTED-COMPUTE argument, not a correctness/wrong-answer
argument: "every block overlapping a slice would still have to compute the FULL structural
answer just to filter down to its own shard, baking in universal N× COMPUTATION
duplication... defeats #489's own performance purpose." Early-stopping makes "compute the
full structural answer" cheaper per invocation, which weakens this specific rejection
reason somewhat — but does NOT eliminate it: N blocks each redundantly resolving the
(now-cheaper) SAME bounded top-K answer, just to keep only their own few owned spans, is
still strictly worse than computing it correctly ONCE — which is exactly Option B's own
insight (one job for the whole window, not one job per block), and that insight has
NOTHING to do with resolution cost at all — it's about not needing to ask the SAME question
N times regardless of how cheap each asking now is. Early-stopping is a reason Option A
became *less bad*, not a reason it becomes the *right* design when Option B (already built,
already proven) is available and unaffected by any of this.

**4. Confirmed: this exact resolution shape is already documented, unprompted, in the
guard's own doc comment** (value_index_structural_query.go:19-26): "...until the
duplication question above is resolved (**either a per-block ownership restriction added to
`ExecuteStructuralFromIndex`, or the frontend's sharder stops fanning structural jobs out
per-block**)..." — the second option named here IS Option B, i.e. exactly DT1's own
already-shipped fix. This means: **the guard is not really an independent "should we
attempt the index" heuristic — it is a proxy for "was this request dispatched via a
one-job-for-the-whole-window model," which today only `structuralTimeSlicedJobsFunc`
provides.** The guard itself does not need to change; what would need to change to
legitimately widen structural's index-driven reach is the SET OF DISPATCH STRATEGIES that
route through that one-job model and set `IndexOnly=true` — not the querier-side check
itself.

**Found, and worth flagging separately: this same doc comment is now stale in one specific
place** — "nothing in the frontend builds a `DispatchTimeSliced` plan for a structural
query yet, so `indexOnly` is never true for one in production today" is **no longer true**.
`structural_sharder.go`/`buildStructuralQueryPlan` (confirmed live and wired into
`search_sharder.go` in my second brainstorm pass) DOES build and dispatch
`DispatchTimeSliced` plans for ordinary, non-#487-labeled structural queries today,
whenever `CheckIndexCoverage` passes and the query classifies `Selective`-any-limit or
`UnknownSelectivity`-no-limit. This comment predates that wiring landing and should be
corrected in the same pass that touches this file for Phase 6, alongside the other stale
"falls back to a full block scan" comments already flagged in my first pass.

### Direct implication for Phase 6's design (this is the part that matters for the plan)

**Do not relax `tryStructuralIndexFetch`'s `!indexOnly` guard.** It is correct and should
stay exactly as-is, with this investigation's reasoning recorded as the reason, not just
caution. What Phase 6 actually needs, and already implicitly requires given my prior
pass's design, is: **whatever new dispatch strategy replaces `DispatchBoundedRecentFirst`
must, for STRUCTURAL queries specifically, route through Option B's one-job/no-per-block-
fanout model (mirroring `structuralTimeSlicedJobsFunc`, setting `IndexOnly=true`) — never
through ordinary per-block fanout — or Phase 6 would silently re-introduce exactly the
duplication defect DT1 was built to eliminate, for a whole new class of queries
(low/unknown-selectivity-with-limit structural search) that never had this problem before
simply because they had no index-driven path to reach it through at all.**

This is a genuine, structural (pun noted) asymmetry between the two query types that Phase
6 must encode explicitly, not something early-stopping resolves for structural the way it
does for filter queries:
- **Filter queries ARE safe to dispatch per-block** in the new early-stopping strategy,
  exactly as `DispatchBoundedRecentFirst` already does today — confirmed in my very first
  pass: `tryIndexFetch` passes `sourceRef` into `QueryTraceQLFromIndex`, which restricts
  each block-job's materialized output to spans physically stored in THAT block, so N
  block-jobs naturally compute a true, disjoint PARTITION of the answer, never the same
  whole answer N times. Early-stopping's per-job resolution cost improvement applies
  cleanly on top of the existing, already-safe per-block dispatch model for this path — no
  special handling needed.
- **Structural queries are NOT safe to dispatch per-block, permanently** — this is an
  inherent property of Option A's multi-file design (D3), completely unrelated to and
  unaffected by early-stopping. Phase 6's dispatch design for the
  low/unknown-selectivity-with-limit case must give structural queries a DIFFERENT
  dispatch treatment than filter queries get in that same classification bucket: one job
  for the whole window (or one job per slice, if width-bounding is still wanted for other
  reasons), never fanned out per block.

Recommend the plan make this explicit as its own numbered design point in Phase 6, with a
direct citation back to this section, so a future implementer skimming "Phase 6: retire
DispatchBoundedRecentFirst" doesn't reflexively apply one dispatch-rewiring pattern
uniformly to both query types.
