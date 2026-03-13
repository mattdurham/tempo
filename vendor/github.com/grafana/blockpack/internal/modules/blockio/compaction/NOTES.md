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
