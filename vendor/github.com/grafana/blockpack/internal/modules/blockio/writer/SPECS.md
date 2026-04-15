# Writer Module — Specifications

## SPEC-001: Package Scope and Invariants
*Added: 2026-04-02*

The `writer` package encodes OTLP span/log records into the blockpack columnar format.
A `Writer` is NOT thread-safe — all calls must be serialized by the caller.

**Invariants:**
- `Writer.Flush()` is idempotent with respect to the output stream: it writes exactly one
  complete blockpack file per Flush call and resets internal state for reuse.
- All public API surface is minimal; internal details are unexported.
- The public API is: `New`, `NewWithConfig`, `Flush`, `AddSpan`, `AddLog`, `BlockCount`, `Close`.

---

## SPEC-002: File Format — V5 Footer
*Added: 2026-04-02*

Writers with `VectorDimension > 0` emit a **V5 footer** (46 bytes):

```
version[2] + headerOffset[8] + compactOffset[8] + compactLen[4]
+ intrinsicOffset[8] + intrinsicLen[4] + vectorOffset[8] + vectorLen[4]
```

Writers with `VectorDimension == 0` (default) emit a **V4 footer** (34 bytes), unchanged
from pre-vector behavior.

**Wire format invariant:** V5 is a strict superset of V4 — the first 34 bytes of a V5
footer are identical in layout to V4. Readers detect V5 by total footer size.

---

## SPEC-003: VectorDimension Configuration
*Added: 2026-04-02*

```go
type Config struct {
    VectorDimension int // expected float32 vector dimension (0 = no vector index)
    ...
}
```

**Invariant:** If `VectorDimension > 0`, the writer accumulates embedding vectors from the
`__embedding__` column of each block and writes a VectorIndex section before the footer.

**Invariant:** If a block's `__embedding__` column contains vectors with a dimension that
does not match `VectorDimension`, those vectors are silently skipped (no error, no panic).

---

## SPEC-004: VectorIndex Wire Format
*Added: 2026-04-02*

The VectorIndex section uses the following wire format:

```
magic[4 LE] + version[1] + dim[2 LE] + M[2 LE] + K[2 LE] + num_blocks[4 LE]
file_centroid[dim*4 LE float32]
per block: vector_count[4 LE] + centroid[dim*4 LE float32] + pq_codes[vector_count*M bytes]
codebook[M * K * subvec_dim * 4 LE float32]  (one flat row per subspace)
```

**Invariants:**
- `magic == shared.VectorIndexMagic` (0x56454349, "VECI" in ASCII).
- `version == shared.VectorIndexVersion` (0x01).
- `dim % M == 0`; `subvec_dim = dim / M`.
- `M <= 96` (pqM constant); `K <= 256` (pqK constant).
- `num_blocks` equals the total number of blocks in the file.
- `pq_codes` for a block: one `M`-byte code per vector in block order.
  Each byte `code[m]` is the nearest-centroid index in subspace `m`; always `< K`.

---

## SPEC-005: PQ Training Parameters
*Added: 2026-04-02*

```
maxTrainingSamples = 50_000  // reservoir sample cap
pqM                = 96      // default M (subvectors); may be reduced for small dims
pqK                = 256     // centroids per subspace
```

**Training invariant:** PQ training is performed once per `Flush()` call using reservoir-sampled
vectors (capped at 50,000). The codebook is not written if `effectiveK < 2` (insufficient training
data).

**Memory invariant:** Per-block vectors are stored per-block in `vectorBlockEntry.vectors` and
freed after PQ encoding. The writer never accumulates all file vectors into a single slice.

---

## SPEC-006: Column Encoding Selection
*Added: 2026-04-02*

Each column is encoded with the best-fit encoding selected at flush time:

| Column type | Candidate encodings |
|---|---|
| String / dict | DictEncoding, PrefixEncoding, InlineEncoding |
| Int64 / uint64 | DeltaEncoding, XOREncoding, InlineEncoding |
| Float32 vector | VectorEncoding (flat IEEE 754 LE) |
| Bytes | InlineEncoding |

**Invariant:** The encoding with the smallest serialized size is chosen. All encodings
produce byte-identical output for the same input across writer versions (format stability).

---

## SPEC-007: Block Size and Count Limits
*Added: 2026-04-02*

- `shared.MaxBlocks = 65_535` — maximum blocks per file (uint16 block ID).
- `shared.MaxSpans = 1_000_000` — maximum spans per file.
- `shared.MaxBlockSize = 1_073_741_824` — maximum serialized block size (1 GiB).

**Invariant:** The writer does not enforce these limits internally during `AddSpan`/`AddLog`;
enforcement is the caller's responsibility. Exceeding these limits produces files that may
fail reader validation.

---

## SPEC-11.5: Auto-parsed log body columns
*Added: 2026-04-14*

When a log record body is non-empty, the writer auto-parses it into sparse `log.{key}`
columns via `parseLogBody`:

**Detection heuristic:**
- If the trimmed body starts with `{`, it is parsed as a JSON object (`parseJSONBody`).
- Otherwise, it is parsed as a logfmt string (`parseLogfmtBody`).
- `parseLogBody` returns nil on empty body, parse failure, or JSON non-object (e.g. array).

**Column naming:** Each extracted top-level key `k` becomes a column named `log.{k}`
(via `internLogColName` with prefix `"log."`). The column type is `ColumnTypeRangeString`.

**Silent-failure policy (NOTE-007):** A nil return from `parseLogBody` is silently ignored —
the body column is written as-is and no `log.*` columns are added. No error is surfaced.

**No field cap:** All top-level key-value pairs extracted by `parseLogBody` are stored.
Non-string JSON values are coerced to string via `fmt.Sprint`. Nested objects/arrays are
stored as their `fmt.Sprint` representation (not recursed).

**Invariant:** `log.{key}` columns generated by body auto-parse use `ColumnTypeRangeString`,
distinct from hand-authored log attributes (`ColumnTypeString`). The executor and query
pipeline use this distinction to exclude auto-parsed body fields from `IterateFields`
enumeration (see `span_fields.go:NOTE-ITER-1`) while still resolving them via `GetField`.

Back-ref: `internal/modules/blockio/writer/writer_log_body.go:parseLogBody`,
          `internal/modules/blockio/writer/writer_log.go:362`
