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
- the `FlushInterval` elapsed-time check fires between messages (`flushAll`, see NOTE-VI-020).

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

## NOTE-VI-022 — Spill buffers keyed by (name, colType), not name alone (issue #408)

Date: 2026-06-28

`valueindex.Writer` is single-typed: `NewWriter(colName, colType)` fixes a column
type and every value passed to `AddEntry` must canonical-encode as that type
(`CanonicalValue` returns an error on a type/value mismatch). The consumer's
per-column spill buffer originally keyed `s.buffers` by **column name alone** and
locked the buffer's `colType` to whatever the **first** entry for that name
carried. The spill codec, however, persists each entry's **own** ColType byte and
`readEntry` decodes from that byte — so a buffer could hold a float64 value (its
own byte) while its writer was created for `ColumnTypeString` (the first-seen
type), and `AddEntry` then failed:

    valueindexconsumer: add entry "span.start": valueindex: AddEntry:
    valueindex: ColumnTypeString expects string, got float64

This happens whenever one column name surfaces with more than one column type
across the blocks in a file — e.g. an attribute named `span.start` (the dotted
attribute, distinct from the `span:start` intrinsic) stored as a float64 in one
block and a string in another due to mixed-type spans. It is general: any
mixed-type attribute name triggers it.

### Fix

Key `s.buffers` by `bufferKey{name, colType}`. Distinct types for the same name
now get distinct buffers, each producing a type-consistent L0 index file. As of
NOTE-VI-024 (issue #409) those files also land under distinct `<col_hash>/<type>`
prefixes so they never collide on S3. The per-message ack refcount
(`touched` / `pendingIDs` / `pendingCol`) is tracked per `bufferKey`, so a message
touching one name under two types is acked once, only after **both** buffers
flush.

`writeEntry`/`readEntry` were already correct (each entry self-describes its
ColType); the bug was purely the name-only buffer key feeding a stale `colType`
into `NewWriter`.

Back-refs: `internal/modules/valueindexconsumer/service.go`.

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

## NOTE-VI-020 — Elapsed-time flush check, not ticker-in-select (issue #404)

Date: 2026-06-27

`Service.Run` is single-goroutine: it owns all mutable state (per-column buffers, refcount
map) so the orchestration needs no locking. The original loop drove the timer flush with a
`select { case <-ticker.C: ... default: }` evaluated once per poll iteration, then called
`consumer.Poll` and ran `ingest` for each returned message.

The bug: `ingest()` → `Extractor.Extract()` is synchronous and can take minutes for a large
L1 block. The ticker case is only reachable *between* poll iterations, never while a single
`ingest` is blocked inside `Extract`. So if one message took longer than `FlushInterval`, the
ticker case was perpetually deferred — with a steady stream of long messages the flush timer
could never fire, stranding buffered entries (and their unacked source messages) indefinitely.

The fix (Option C from the issue): drop the ticker entirely and check elapsed wall time after
each batch of ingests:

```go
if s.now().Sub(lastFlush) >= s.cfg.FlushInterval {
    flushAll(); lastFlush = s.now()
}
```

This guarantees a flush happens *between* messages once the interval has elapsed, regardless
of how long any one message took — correctness no longer depends on a select case being
reachable mid-ingest. It does not flush mid-message; the contract is that `FlushInterval` must
exceed a single message's processing time. `DefaultFlushInterval` was raised 5m→15m to safely
clear L1 block processing time so the between-message check fires reliably.

Uses the existing injectable `now func() time.Time` (previously only assigned, never read for
flush timing), so tests can drive flush cadence deterministically with a fake clock instead of
real sleeps. The `ctx.Done()` drain flush is preserved as a leading `ctx.Err()` check at the
top of the loop. NOTE-VI-019's cooperative XAutoClaim reclaim runs inside `Poll`, unaffected.

Back-refs:
`internal/modules/valueindexconsumer/service.go`,
`internal/modules/valueindexconsumer/config.go`

## NOTE-VI-021 — rqlite job-table consumer (issue #405)

Date: 2026-06-27

Adds a pull-based `Consumer` (`RqliteConsumer`) backed by the rqlite job table,
the consumer-side counterpart to the rqlite publisher (blockevents NOTE-VI-021).
This is the queue-transport swap called for in #405; the `Service`
orchestration, `Extractor`, and `ObjectPutter` are unchanged — rqlite replaces
only how messages are delivered and acked.

### Pull, not push — Poll = claim, Ack = delete

The Redis consumer is push-delivered (XREADGROUP). The rqlite consumer pulls:

- **Poll** issues a claim `UPDATE` that flips up to `BatchSize` eligible rows to
  `status='in_progress'`, stamping `worker_id` and `claimed_at`, then reads back
  the rows this worker now owns via `claimedPaths`. The `UPDATE ... WHERE
  file_path IN (SELECT ... ORDER BY inserted_at ASC LIMIT N)` bounds the claim
  and processes oldest-first (FIFO). rqlite's Raft layer serializes concurrent
  workers' claim writes, so two workers never claim the same row — no
  `FOR UPDATE SKIP LOCKED` needed.
- **Ack** `DELETE`s the claimed rows by `file_path` (their `Message.ID`). The
  table is self-trimming; a deleted row is never re-claimed.

The `Message.ID` is the `file_path` itself (the primary key), so Ack deletes by
it directly — no opaque queue token to track.

### Inline stale reclaim — no reaper

The claim predicate also matches rows whose `in_progress` claim is older than
`ClaimIdleThreshold` (`status='in_progress' AND claimed_at < datetime('now',
'-N seconds')`). A dead worker's orphaned claims are picked up by the next live
Poll with no separate reaper process — the rqlite analogue of the Redis
XAUTOCLAIM reclaim (NOTE-VI-019), but simpler because it is one predicate clause
rather than a cursor scan.

### Non-blocking Poll

Poll does not block waiting for rows: an empty table returns an empty slice and
nil error, so the caller's elapsed-time flush check (NOTE-VI-020) still runs.
This matches the Redis consumer's empty-poll contract.

### Testability — claimedPaths abstraction

`gorqlite.QueryResult`'s fields are unexported and its `Next`/`Scan` dereference
an internal connection, so a test fake cannot construct or iterate one. The
`rqliteDB` interface therefore exposes reads through a `claimedPaths` method
returning `[]string` rather than `*gorqlite.QueryResult`. `connAdapter` wraps a
real `*gorqlite.Connection` to satisfy it (translating the SELECT into file
paths); tests substitute a fake that returns scripted paths — full unit coverage
with no real rqlite server.

### Config

`Config.RqliteURL` (YAML `rqlite_url`) is required by `NewRqliteConsumer`,
ignored by the Redis consumer. `BatchSize` and `ClaimIdleThreshold` reuse the
existing defaults. The Redis fields remain so the transport choice is the
operator's; this commit adds the rqlite path without removing the Redis one.

Back-refs:
`internal/modules/valueindexconsumer/rqlite.go`,
`internal/modules/valueindexconsumer/config.go`,
`valueindexconsumer/valueindexconsumer.go`,
`cmd/deadcode/main.go`

## NOTE-VI-023 — Prometheus metrics for the value-index pipeline (issue #407)

Date: 2026-06-28

Both the consumer and the compactor now expose Prometheus metrics, following the cache-layer
precedent (memcache/tieredcache): a `Registerer prometheus.Registerer` field on `Config`
(`yaml:"-"`, injected programmatically by tempo as `prometheus.DefaultRegisterer`), nil = total
no-op. All collectors are built in a `metrics.go` per package via `registerOrReuse`-style
helpers that tolerate `AlreadyRegisteredError`, so multiple consumer/compactor instances (or a
co-located pair) can share one global registry without panicking.

### Nil-safety via nil-receiver no-ops

The service holds a `*consumerMetrics` (resp. `*compactorMetrics`) that is `nil` when
`Config.Registerer` is nil. Every method has a `if m == nil { return }` guard, so the hot path
costs nothing when metrics are disabled and tests need no real registry. This is cleaner than
sprinkling nil checks at each call site.

### Pre-resolved hot-path observers

The extract/flush/run/merge duration histograms and the flush-bytes histogram are stored as
pre-resolved `prometheus.Observer` (the histogram itself, no labels) so observation is a single
`Observe` with zero per-call label allocation — same pattern as memcache `durGetHit` et al.
Native histograms use `NativeHistogramBucketFactor: 1.1`, `MaxBucketNumber: 100`,
`MinResetDuration: 15m`, matching the cache layer.

### `files_total` semantics = one acked message

A file is counted `success` exactly when its message is acked: either immediately on ingest
(no configured column touched) or once all its column buffers have flushed (the ack in
`flushColumn`). An extract failure counts `error` once. This keeps the counter aligned with
"files fully processed and flushed", not "Put calls".

### Pending-jobs / stale-reclaims via optional reporter interfaces

`pending_jobs` (gauge) and `stale_reclaims_total` (counter) are signals the *Consumer impl*
owns, not the Service. Rather than entangle the Redis/rqlite consumers with the Service's
metrics object, the Service queries two **optional** interfaces — `PendingReporter`
(`PendingCount`) and `StaleReclaimReporter` (`StaleReclaimsSince`, delta-and-reset) — once per
Run loop iteration (`observeQueueState`). A consumer that implements neither is a silent no-op;
the gauge simply stays unset. A `PendingCount` error is swallowed (a missing sample beats
crashing the consume loop on a transient backend hiccup). The current Redis/rqlite consumers do
not yet implement these, so wiring the COUNT query into them is a follow-up — the metric plumbing
is in place and the interfaces are unit-tested via a fake reporter.

Back-refs:
`internal/modules/valueindexconsumer/metrics.go`,
`internal/modules/valueindexconsumer/service.go`,
`internal/modules/valueindexconsumer/consumer.go`,
`internal/modules/valueindexconsumer/config.go`

## NOTE-VI-024 — Type-bucketed index paths `<col_hash>/<type>/<file>` (issue #409)

Date: 2026-06-28

NOTE-VI-022 split the in-memory spill buffers by (name, colType) but still wrote
every type's L0 file under the **same** `<tenant>/indexes/<col_hash>/` prefix.
Column names are not unique across types — `span.start` can be a `float64` in one
block and a `string` in another — and the resulting index files have different
value encodings, so a reader cannot tell them apart and the encodings are not
interchangeable. Two type-different files under one prefix is latent S3 corruption.

### Fix

Insert a short, human-readable type segment between the hash and the file:

    <tenant>/indexes/<col_hash>/<type>/L0-<xid>.blockpack

`valueindex.ColTypeName(colType)` (in `valueindex/hash.go`, alongside `ColHash`)
returns the bucket name: `string`, `int64`, `uint64`, `float64`, `bool`, `bytes`,
`uuid`. Range* types map to their **scalar** bucket (`range_float64` → `float64`,
`range_duration` → `int64`, …) because they are indexed as their scalar equivalent
(NOTE-VI-012) — the mapping mirrors `CanonicalValue` exactly. An unindexable type
(VectorF32) returns `""`. `Service.indexKey` now takes the buffer's `colType` and
inserts `ColTypeName(colType)` as the segment.

The compactor needed **no** logic change: `compactTenant` already groups by
`path.Dir(key)`, which under the new layout is `<tenant>/indexes/<col_hash>/<type>`
— exactly the per-(hash, type) grouping required so merges never mix types. `List`
is prefix-based so the extra nesting level is returned naturally, and `mergeLevel`
joins the output filename onto that `colDir`, preserving the `<type>` segment.

No migration: existing index files (if any) can be discarded; the compactor
rewrites from source blocks. The `col_hash` directory still groups all types of a
column together for human browsing.

Back-refs: `internal/modules/valueindex/hash.go` (`ColTypeName`),
`internal/modules/valueindexconsumer/service.go` (`indexKey`).

## NOTE-VI-025 — Buffer spill-file writes through bufio.Writer (issue #412)

Date: 2026-06-28

`writeEntry` wrote each entry's three segments (37-byte fixed header, source-ref
bytes, canonical value bytes) straight to the spill `*os.File`, i.e. up to three
`pwrite` syscalls per entry. For a large L1 block with ~10M spans across ~50
present columns that is hundreds of millions of syscalls per file. pprof showed
~60% of consumer CPU in `internal/runtime/syscall/linux.Syscall6`, all reached via
`writeEntry → os.File.Write`.

### Fix: per-buffer bufio.Writer

`columnBuffer` now carries a `bw *bufio.Writer` (256 KB, `spillWriteBufSize`)
wrapping `file`. `ingest` writes entries to `buf.bw` instead of `buf.file`,
batching them into ~256 KB `pwrite`s — a ~100–1000× syscall reduction depending on
entry size.

### Flush discipline (correctness-critical)

A `bufio.Writer` holds bytes that are not yet on disk, so it MUST be flushed at
every point the file is read or its offset changes:

1. **End of each ingest pass** — every buffer touched by the just-extracted job is
   `bw.Flush()`ed before the next job is claimed. This bounds unwritten data to one
   job's worth and keeps the spill file self-consistent between jobs.
2. **Start of flushColumn** — a defensive `bw.Flush()` before `Seek(0)` + read-back,
   so flushColumn is correct regardless of caller ordering (e.g. the timer-driven
   `flushAll` between Polls).
3. **After truncate+rewind in flushColumn** — `bw.Reset(buf.file)` re-points the
   writer at the rewound file for reuse, discarding any residual bytes (none, post
   flush) and clearing any sticky write error so the next pass starts clean.

### Struct size

Adding the `bw` pointer grew `columnBuffer` from 72 to 80 bytes. Field order
preserved (NOTE-LINT-407): pointers/maps/strings first, the `uint8 colType` +
`bool hasData` still pack into one trailing word — no padding waste.
`TestColumnBufferAlignment` updated to 80.

Back-refs: `internal/modules/valueindexconsumer/service.go`
(`columnBuffer`, `bufferFor`, `ingest`, `flushColumn`, `spillWriteBufSize`).
