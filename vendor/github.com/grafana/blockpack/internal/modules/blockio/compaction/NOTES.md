# compaction — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/blockio/compaction` package.

---

## 1. Native Columnar Copy via AddRow
*Added: 2026-03-05*

**Decision:** `CompactBlocks` copies spans using `Writer.AddRow(block, rowIdx)` rather than
reconstructing OTLP objects and calling `Writer.AddTracesData`.

**Rationale:** Reconstructing OTLP proto objects from column values requires allocating
intermediate struct trees for every span — an O(spans) allocation cost. The native columnar
path copies column values directly from the source block into the destination writer without
any intermediate objects. For typical compaction workloads (millions of spans), this is
significantly more memory-efficient.

**Consequence:** The compaction package depends on `Writer.AddRow`, which requires that the
writer and reader agree on the column schema. New columns added to the writer are automatically
included; columns present in input files but unknown to the writer are silently dropped (this
is the writer's normal handling of unknown columns).

Back-ref: `internal/modules/blockio/compaction/compaction.go:addSpanFromBlock`

---

## 2. Deduplication Key is (trace:id, span:id)
*Added: 2026-03-05*

**Decision:** Spans are deduplicated using a 24-byte composite key: 16-byte `trace:id`
followed by 8-byte `span:id`.

**Rationale:** The combination of trace ID and span ID uniquely identifies a span within
the OpenTelemetry data model. Using both prevents false deduplication when spans from
different traces share the same span ID.

**Alternative considered:** Using a hash of all column values would detect content changes
but requires significantly more CPU and memory. For compaction (merge of redundant replicas),
identity-based deduplication is correct.

**Invariant:** Spans with invalid or missing IDs (non-16-byte trace:id, non-8-byte span:id)
are dropped and counted in `droppedSpans`. See SPECS.md §4.

Back-ref: `internal/modules/blockio/compaction/compaction.go:dedupeKey`

---

## 3. Staging Directory for Transactional Output
*Added: 2026-03-05*

**Decision:** Output files are written to a temporary staging directory and then pushed to
`OutputStorage` only after all content is written. The staging directory is always cleaned
up on return.

**Rationale:** Writing directly to `OutputStorage` (e.g. object storage) would require
either streaming writes (not supported by the `Writer` abstraction) or holding full file
contents in memory. The staging approach:
1. Allows `Writer.Flush` to produce complete, valid files before pushing.
2. Ensures no partial files are pushed if `Writer.Flush` fails.
3. Cleans up staging storage even on error.

**Consequence:** Disk space is required for staging. In environments with constrained local
storage, `Config.StagingDir` should point to a volume with sufficient capacity.

Back-ref: `internal/modules/blockio/compaction/compaction.go:prepareStagingDir`

---

## 4. Output File Rotation on Size Limit
*Added: 2026-03-05*

**Decision:** When `Config.MaxOutputFileSize > 0`, `CompactBlocks` checks
`Writer.CurrentSize()` after each span and rotates to a new output file when the estimate
exceeds the limit.

**Rationale:** Object storage systems (S3, GCS) have per-object size limits and performance
characteristics that favor objects in a bounded size range (typically 100MB–1GB for
sequential scan). Bounded output file sizes make the compaction output predictable.

**Note on estimate accuracy:** `Writer.CurrentSize()` returns an estimate based on
uncompressed column sizes. The actual compressed file may be smaller. The rotation
threshold is therefore a loose upper bound, not a hard limit.

Back-ref: `internal/modules/blockio/compaction/compaction.go:addSpanFromBlock`

---

## NOTE-37: Log File Re-sort by MinHash+Timestamp
*Added: 2026-03-18*

`CompactLogFile` globally re-sorts a log blockpack file by `(minHash[0..3], timestamp)`
to produce tight label-value boundaries per block. The MinHash signature is computed over
all `"key=value"` attribute pairs for each log record.

**Why minHash clustering:** Log files written by ingest have arbitrary row order within
each block. Without re-sorting, the per-block label ranges (min/max service_name, etc.)
span many different label values, making range index pruning ineffective — almost every
block passes the bloom/range check. After re-sorting, blocks contain log records with
similar label sets, so per-block label ranges are tight and queries for a specific
`service_name` can skip the majority of blocks.

**Why timestamp as secondary key:** Within each minHash cluster, timestamp ordering
preserves time-locality. This is important for log queries that filter by time range:
the per-block min/max timestamp bounds stay narrow, enabling block-level time pruning.

**Tradeoff:** The sort is in-memory and O(N) in total rows. `CompactLogFile` is not
suitable for arbitrarily large files; it is intended for moderate-size log segments.

Back-ref: `internal/modules/blockio/compaction/log_compaction.go:CompactLogFile`

---

## NOTE-459: Streaming compaction bounds peak memory to one input block
*Added: 2026-06-21*

`CompactBlocks` previously required the caller to pass every input provider already
materialized (`[]ReaderProvider`). The Tempo callsite downloaded all N input blocks
into memory in parallel before calling it, so peak input-side memory was
`sum(all blocks)` — at L1 ~730 MB/block and `max_input_blocks=4` that is ~3 GB,
causing OOMKills on 20 Gi workers and forcing `max_input_blocks: 2` as a workaround.

`CompactBlocksStreaming` takes `[]ProviderFunc` (lazy factories) instead. It invokes
each factory just-in-time, feeds that single block's spans into the output writer via
`processProvider`, then drops the provider reference before invoking the next factory.
Peak input-side memory is therefore `~max(largest single block)` regardless of input
count. `openAndProcess` is a separate function specifically so the opened provider does
not outlive a single loop iteration: it returns (and the block bytes become GC-eligible)
before the next factory runs.

The only state spanning all inputs is `seenSpans` — the dedup set of 24-byte
`(trace:id, span:id)` keys — which is far smaller than the raw block bytes. Raising the
input-block count grows the dedup set linearly in span count but does not scale peak
memory by block size, so `max_input_blocks` can be raised (8, 16+) without OOM risk.

**Trade-off:** parallel downloads are no longer possible on this path (blocks are
consumed sequentially). Memory safety was prioritized over download parallelism because
OOMKills abort the entire compaction job whereas serial downloads only lengthen it.

`CompactBlocks` is retained as a thin adapter: it wraps each pre-materialized provider in
a trivial factory and delegates to `CompactBlocksStreaming`, keeping one merge/dedup path.

Back-ref: `compaction.go:CompactBlocksStreaming`, `compaction.go:openAndProcess`,
`storage.go:CompactBlocksStreaming`
