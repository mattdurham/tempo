# Value Index — Known Problems and Limitations

This file documents known structural problems, performance gaps, and missing capabilities
in the value index. Each entry describes the problem, its impact, and the work required
to fix it. Entries are never deleted — resolved problems are marked **[RESOLVED]** with
a back-reference to the fix.

---

## PROB-VI-001: top-N-by-time queries are slow — current top-K executor takes 20–30s, target < 5s

**Top-N-by-recency queries (e.g. "most recent 20 traces where `status_code = 200`") currently
take 20–30 seconds. The target is under 5 seconds.**

**Impact:**

The executor's existing top-K path (SPEC-STREAM-7/8 in `stream_topk.go`) scans every
blockpack file to find block-level time bounds before applying the predicate. With millions
of span storage files this scan dominates query time. The value index exists to pre-filter
candidate blocks, but currently has no efficient file-discovery mechanism to support
time-ordered traversal.

**Root cause:**

Value index files are written one-per-block-builder-flush. The file count per column is
bounded by compaction (`ValueIndexCompactThresholdFiles = 8`) so steady-state is likely
tens to low-hundreds of files — not millions. The real problem is not file discovery
cost (a single S3 LIST suffices at this scale) but the combination of:

1. No time-ordered traversal: files must be processed newest-first to enable early exit,
   but there is no efficient way to sort files by data recency without opening each one.
2. No intra-file lower-bound seek: `ChunkDirEntry` has `MinTimeSec` but not `MaxTimeSec`,
   so old chunks cannot be skipped when seeking to recent entries within a file.

**Affected query patterns:**

- "Most recent N results matching predicate X" (primary pain point)
- "All results in time range [T1, T2] matching predicate X"

**Unaffected query patterns:**

- Full scans ("all trace IDs where `status_code = 200`") — must read every file anyway.

**Work required to fix:**

Two complementary changes; see NOTE-VI-013 for the architectural decision on file layout.

**Part 1 — Time-windowed files via WarpStream-style buffering (preferred) or aggressive compaction:**

Instead of writing one value index file per block builder flush, the block builder
buffers value index entries in memory and flushes one file per column on a fixed time
window (e.g. every 5 minutes). Compactors then merge these time-windowed files into
larger windows (5m → 30m → 2h → etc.), mirroring how WarpStream handles S3-native
Kafka topic data.

This means:

- Each file covers a known, bounded wall-clock window by construction
- File IDs (xid) already encode creation time and are lexicographically sortable
- A query sorts files by xid descending (newest-first) and stops as soon as N results
  are collected — for common values like `status_code = 200` this is typically 1–2 files
- File count stays small: a 5-minute flush interval produces 12 files/hour, compacted
  aggressively to a handful of files covering recent history
- No manifest, no stub files, no new coordination mechanism required

Alternatively: keep per-flush files but lower `ValueIndexCompactThresholdFiles` and run
compaction more aggressively, achieving the same bounded file count without buffering.

**Part 2 — Add `MaxTimeSec` to `ChunkDirEntry` (see PROB-VI-002):**

Once files are time-windowed, intra-file seeks still need a lower-bound skip to avoid
decompressing old chunks. Adding `MaxTimeSec` to `ChunkDirEntry` enables this.

Back-ref: `internal/modules/valueindex/meta.go:Meta`,
`internal/modules/valueindex/entries.go:ChunkDirEntry`,
`internal/modules/valueindex/compaction.go:CompactFiles`.

---

## PROB-VI-002: Ascending time sort and absent MaxTimeSec prevent efficient recency seeks

**Within a value index file, entries are sorted `(Value ASC, TimeSec ASC)`. The chunk
directory records `MinTimeSec` per chunk but not `MaxTimeSec`. Together, these make it
impossible to seek directly to the most recent entries for a given value without reading
all matching chunks.**

**Impact:**

For a high-cardinality value like `status_code = 200` that appears in every flush,
a single value index file may contain thousands of matching entries spread across many
chunks. To find the 20 most recent, the reader must decode every chunk that contains
the value, collect all matching entries, then take the tail. On a large compacted file
this means unnecessary decompression and allocation of thousands of entries that are
immediately discarded.

**Root cause:**

Two design choices compound each other:

1. **Ascending time sort** (`TimeSec ASC` as the secondary key within a value group):
   the most recent entries are at the end of the value's range, not the beginning. There
   is no way to skip to them without scanning forward.

2. **`ChunkDirEntry` has `MinTimeSec` but no `MaxTimeSec`**: `DecodeChunkRange` can skip
   chunks whose `MinTimeSec` exceeds a ceiling, but cannot skip chunks whose entire time
   range falls below a floor. A "give me entries after T" seek is impossible without
   reading and discarding chunks entirely below T.

**Affected query patterns:**

- "Most recent N results matching predicate X" within a single file
- Any query with a `time >= T` lower bound on a large compacted file

**Unaffected query patterns:**

- Full scans (all entries for a value) — must read every chunk anyway.
- Upper-bounded time queries (`time <= T`) — `MinTimeSec > T` already skips correctly.

**Work required to fix:**

Two independent changes, either of which helps; both together are optimal:

1. **Add `MaxTimeSec uint64` to `ChunkDirEntry`** (wire format change, +8 bytes per
   chunk dir entry). `DecodeChunkRange` can then skip chunks where
   `MaxTimeSec < minTS`, enabling efficient lower-bound seeks. This is a minor wire
   format version bump with backward-compatible fallback (treat missing `MaxTimeSec`
   as `^uint64(0)`).

2. **Store entries in `(Value ASC, TimeSec DESC)` order** within a file (or within
   compacted files only). The most recent entries for a value are then at the front of
   the value's chunk range and can be collected with an early-exit once N results are
   found. This conflicts with the current ascending sort used for deduplication during
   compaction; deduplication would need to be adapted or done in a separate pass.

Back-ref: `internal/modules/valueindex/entries.go:ChunkDirEntry`,
`internal/modules/valueindex/entries.go:DecodeChunkRange`.
