## NOTE-VI-016 — Value index consumer service (issue #398)

Date: 2026-06-25

The consumer is the second stage of the value-index pipeline (publisher NOTE-VI-015 #397,
consumer #398, compactor #399). It consumes blockpack `create` events, extracts per-column
entries from the referenced blockpack, accumulates them in per-column in-memory buffers, and
flushes time-windowed L0 value index files to object storage.

### Single-goroutine orchestration, injected dependencies

`Service.Run` owns all mutable state (the per-column buffers and the per-message refcount map),
so no locking is required in the orchestration. The three external dependencies are injected
behind interfaces so the consume→accumulate→flush logic is unit-testable without a real Redis,
blockpack reader, or object store:

- `Consumer` — `Poll`/`Ack`/`Close`. Production impl `RedisConsumer` (Redis Streams consumer
  group); tests use a fake feeding fixed batches.
- `Extractor` — reads a blockpack and yields `[]ColumnEntry`. This abstracts the (heavy) lean
  blockpack reader so the reader integration can evolve independently of the orchestration.
- `ObjectPutter` — `Put(path, data)`. `blockpack.WritableStorage` satisfies it. Exported so
  tempo can supply its object store through the public `valueindexconsumer` subpackage.

### Ack-after-flush via per-message column refcount

At-least-once delivery requires a message be acked only after **all** S3 writes its entries
contributed to have succeeded. A single block can touch multiple configured columns, and each
column flushes independently. We track this with a refcount:

- On ingest, `pendingCol[msgID] = <number of distinct configured columns the block touched>`,
  and each touched column's buffer records the message ID in its `pendingIDs` set.
- On `flushColumn`, every message ID in that buffer's `pendingIDs` is decremented; when a
  message's count reaches zero it is acked. The buffer's `pendingIDs` is then reset.

A message whose block contains no configured column has nothing to flush it with, so it is
acked immediately on ingest. A failed `Put` returns an error before any ack, so the message is
redelivered (Redis Streams PEL) and reprocessed — duplicate entries are removed by the value
index compactor (#399).

### Per-column independent flush (size OR timer)

Each column's buffer flushes independently when:

- its approximate buffered size crosses `MaxColumnBufferBytes` (size-driven, evaluated per
  ingest for the columns that ingest touched), OR
- the `FlushInterval` timer fires (`flushAll`).

A hot column does not block a sparse one. Time-windowed output keeps each L0 file covering a
bounded window, so files are naturally sortable by creation time (xid) — required for the
recency-ordering property the compactor and query path rely on.

`entrySize` is a coarse, monotonic estimate (28 fixed bytes + source-ref + value length); it
only needs to track the buffered footprint to drive the threshold, not match the serialized
size.

### Output key

`<index_prefix>/<col_hash>/L0-<xid>.blockpack` (via `valueindex.FormatFilename(0, valueindex.NewID())`).
Same column → same `<col_hash>` directory, which the compactor lists by. The tenant is encoded
in the storage backend's path scope (one consumer per tenant bucket/prefix), so it is not
repeated in the key.

### Redis Streams details

- Group created with `XGroupCreateMkStream` from `"0"` (read history) so events published
  before the group existed are still consumed. `BUSYGROUP` (group already exists) is tolerated.
- `Poll` uses `XReadGroup` with `>` (new, never-delivered messages) and `Block: PollTimeout`;
  `redis.Nil` (timeout, no messages) maps to an empty slice with nil error so `Run` can fire the
  flush timer between polls.
- Malformed stream entries (missing `action`/`path`) are acked immediately so they are not
  redelivered forever, and skipped.

## NOTE-VI-018 — Shared two-phase extractor + denylist (issue #401)

Date: 2026-06-27

The `Extractor` interface is implementation-injected, and there were two divergent production
implementations of it — the standalone binary (`cmd/value-index-consumer`) and tempo's
in-process module (`cmd/tempo/app/value_index.go`). Both iterated only `block.Columns()` with a
hardcoded value-type switch. For files where the high-value intrinsic columns (`span:name`,
`span:kind`, `span:status`, `span:status_message`, `span:duration`, `resource.service.name`)
live ONLY in the IntrinsicTOC and are absent from the per-block column map, those columns —
the most important ones for tag-value lookups — were silently never indexed.

### One reusable extractor in the public package

`blockpack.ExtractValueIndexEntries(reader, denylist, yield)` (root file
`valueindex_extract.go`) is the single source of truth both implementations now delegate to. It
yields a `blockpack.ValueIndexEntry{ColName, Value, ColType, BlockID, TimeSec}`; each caller maps
that into its own `ColumnEntry` and supplies the `SourceRef`. Living in the root package (which
already exports `Reader`/`WantAll`/`ColumnType`) makes it unit-testable against a real in-memory
blockpack — `valueindex_extract_test.go` writes spans with names/kinds/statuses/service/attribute
and asserts the yielded columns, denylist, per-span TimeSec, and yield-error abort.

### Two phases + dedup (version-robust)

- **Phase 1 — per-block columns.** Each inner block is parsed one at a time (peak memory =
  one block's columns) and every non-denied column it exposes is yielded. In current file
  versions the intrinsic colon-named columns ALSO surface here, so this phase already covers
  most of them. The set of column names actually yielded is recorded.
- **Phase 2 — IntrinsicTOC fallback.** Any non-denied intrinsic column phase 1 did NOT yield
  (TOC-only files) is read straight from the IntrinsicTOC and yielded. Dict-format intrinsics
  are walked via `DictEntries` (each entry carries its own ref list); Flat/XOR/Delta via the
  positionally-aligned value arrays. The per-name dedup set prevents the double-yield that a
  naive "phase 1 = block, phase 2 = all intrinsics" split would cause for columns present in
  both the block map and the TOC.

### Denylist, not allowlist

`DefaultValueIndexDenylist` drops the columns that are useless or redundant as tag values:
the three identity columns (`span:id`, `span:parent_id`, `trace:id` — unique per span) and
`span:start` (high-cardinality timestamp already covered by the time-range index). A nil
denylist selects the default; an empty non-nil denylist indexes everything. `Config.Columns`
is no longer consulted by the production extractors (the orchestration still honours it as an
optional post-filter — empty means "index all", NOTE in service.go).

### Per-span TimeSec

TimeSec is resolved once up front into a packed-key (`blockIdx<<16 | rowIdx`) → seconds map
built from the `span:start` intrinsic (nanoseconds / 1e9), so every yielded entry — intrinsic
or attribute — is stamped with its own span's start second. The prior tempo extractor left
TimeSec at 0; the prior binary extractor had a bug (always used `Uint64Values[0]`).

Back-ref: `valueindex_extract.go`, `valueindex_extract_test.go`,
`cmd/value-index-consumer/main.go`.

### Public re-export (embedder pattern)

`/valueindexconsumer/valueindexconsumer.go` re-exports the minimal API (type aliases + ctor
wrappers), mirroring NOTE-VI-015 / NOTE-370, because tempo cannot import `internal/*`.

Back-refs:
`internal/modules/valueindexconsumer/service.go`,
`internal/modules/valueindexconsumer/redis.go`,
`internal/modules/valueindexconsumer/consumer.go`,
`internal/modules/valueindexconsumer/config.go`,
`valueindexconsumer/valueindexconsumer.go`

## NOTE-VI-019 — Reclaim stale pending messages on startup (issue #403)

Date: 2026-06-27

Redis Streams consumer groups keep a per-consumer pending-entries list (PEL): messages
delivered via `XREADGROUP ... >` but not yet `XACK`ed. When a consumer pod restarts it gets a
fresh `ConsumerName` (`vic-<xid>`) and `XREADGROUP ... >` only ever delivers NEW messages, so
entries left pending under the *previous* instance's name are never redelivered and sit in
limbo. With slow per-message processing (large blocks) this stalls the whole pipeline: spill
files never fill, the flush never fires, and no value index files reach S3.

### Fix: cooperative XAUTOCLAIM scan, drained through Poll

On the first `Poll` calls the consumer scans the group PEL with `XAUTOCLAIM`, claiming entries
idle ≥ `ClaimIdleThreshold` to *itself* and reprocessing them as ordinary messages. The scan is
cursor-driven and cooperative — one `XAUTOCLAIM` step per `Poll` rather than a blocking loop in
the constructor — so startup is not stalled and the flush timer keeps advancing between steps.

- `claimCursor` starts at `"0-0"`; each step advances it to the cursor `XAUTOCLAIM` returns.
- When the cursor wraps back to `"0-0"` (or `XAUTOCLAIM` returns `redis.Nil`), `reclaimDone` is
  set and `Poll` falls through to normal `XREADGROUP ">"` delivery for the rest of the process
  lifetime. No further `XAUTOCLAIM` calls are made.
- A reclaim step that completes the scan with no claimed messages falls through to a blocking
  read *in the same Poll*, so a quiet PEL costs at most one extra round-trip at startup.
- Reclaimed messages are reprocessed exactly like fresh ones; duplicate entries that result
  from at-least-once reprocessing are removed by the value-index compactor (#399), so reclaim
  needs no dedup state of its own. Malformed reclaimed entries are acked-and-skipped via the
  shared `appendParsed` helper, same as in the normal poll path.

### Config: ClaimIdleThreshold

`ClaimIdleThreshold` (YAML `claim_idle_threshold`) defaults to `2 × PollTimeout` — long enough
that a live consumer's own in-flight messages are never stolen mid-processing, short enough that
a dead instance's orphans are reclaimed promptly. `DefaultClaimBatchSize` (500) bounds entries
claimed per cursor step. Wired through tempo's mirror config (`ValueIndexConsumerConfig`) and
`toVICConsumerCfg`.

### streamReader gains XAutoClaim

`*redis.Client` already satisfies the added `XAutoClaim` method; the test fake scripts results
via `claimSteps` (one per call) plus `claimErr`.

Back-refs:
`internal/modules/valueindexconsumer/redis.go`,
`internal/modules/valueindexconsumer/config.go`,
`tempo/tempodb/encoding/common/config.go`,
`tempo/cmd/tempo/app/value_index.go`
