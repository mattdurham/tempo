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
| Float64 | DictEncoding (low cardinality), GorillaFloat64 (high cardinality, NOTE-219) |
| Float32 vector | VectorEncoding (flat IEEE 754 LE) |
| Bytes | InlineEncoding |

**Invariant:** The encoding with the smallest serialized size is chosen. All encodings
produce byte-identical output for the same input across writer versions (format stability).

**Bytes-column selection (NOTE-221, issue #333):** the bytes column path uses a two-tier
data-driven selector (`bytes_cost_select.go`) instead of the legacy name-suffix dispatch
(`isIDColumn`/`isURLColumn`):

1. **Tier 1 — semantic overrides.** `shared.SemanticBytesOverride(name)` pins a family for a small
   allow-list of **intrinsic** columns where the encoding is known a-priori: `trace:id` /
   `log:trace_id` → DeltaDictionary (sorted 16-byte IDs); `span:id` / `span:parent_id` /
   `log:span_id` → XOR (fixed-width IDs with shared high-order bits). User attribute names never
   match. Adding an entry requires a >10% win over the cost path for that column.
2. **Tier 2 — cost-based selection.** `gatherBytesStats` runs a single streaming pass (cap-N
   distinct estimate, common-prefix fold, uniform-length detection, total bytes); the estimators
   `estimateDictBytesCost` / `estimateXORBytesCost` / `estimatePrefixBytesCost` rank the candidate
   families by estimated wire bytes and the cheapest wins.

The demoted name heuristics (`isIDColumn`/`isURLColumn`) break only near-ties — when the runner-up
estimate is within 5% (`bytesCostTiebreakFraction`) of the winner. They can never override a clear
cost winner, so the legacy failure modes (`customer.duration_id`→XOR, `config.file.path`→Prefix)
no longer occur. DeltaDictionary is not a cost candidate (its index-stream win requires sorted +
clustered indexes, established only via the override table). No wire-format change — selection only.

**AllPresent selection (NOTE-AP-001):** when a column is fully present
(`presentCount == nRows`, `nRows > 0`) and `Config.DisableAllPresentEncoding` is false, the
writer emits the AllPresent variant of the chosen dense kind (`shared.AllPresentKindFor`):
Dictionary→15, InlineBytes→16, DeltaUint64→17, RLEIndexes→18, XORBytes→19, PrefixBytes→20,
DeltaDictionary→21. The AllPresent wire format is identical to its base kind except the
presence-RLE segment is omitted (the kind byte signals full presence). Sparse kinds, VectorF32,
and zero-row columns never select an AllPresent variant. Reading is unaffected by the flag —
readers accept both forms (SPECS §9.0).

**Bit-packed DeltaUint64 selection (NOTE-215):** when a uint64 column has been chosen for delta
encoding (per `shouldUseDeltaEncoding`) and `Config.DisableBitPackedDelta` is false, the writer
prefers the bit-packed variant (kind 22, AllPresent kind 23 — SPECS §9.4.1) over the byte-width
form (kind 5/17) when **both** of:

1. **Width savings:** `byte_width*8 − bit_width ≥ bitPackedDeltaMinSavedBits` (4 bits ≈ 12.5% of
   a 1-byte width). Here `byte_width` is the kind-5 byte width (1/2/4/8) and `bit_width` is
   `bits.Len64(maxOffset)`. The win is largest when the offset range falls just above a byte
   boundary (e.g. a ~36-bit range snaps kind 5 to 8 bytes / 64 bits).
2. **Amortization:** `presentCount ≥ bitPackedDeltaMinPresent` (64), so the fixed per-column
   header (`base[8] + bit_width[1] + packed_len[4]`) is small relative to the packed payload.

All-zero offsets (`bit_width == 0`) keep kind 5/17 — that form already stores no payload. The
AllPresent layering (NOTE-AP-001) composes: a fully-present bit-packed column emits kind 23.

These thresholds are deliberately conservative. At the default `defaultMaxBlockSpans = 2000`,
`span:start` columns commonly hit a ~36-bit offset range (≈60 s window in ns), which kind 5
rounds up to 8 bytes — exactly the case condition 1 captures. Reading is unaffected by the flag.

**Per-page DeltaUint64 selection (NOTE-218):** when a uint64 column has been chosen for delta
encoding (per `shouldUseDeltaEncoding`) and `Config.DisablePagedDelta` is false, the writer
prefers the per-page variant (kind 39 — SPECS §9.4.2) over the single-page bit-packed (kind 22)
or byte-width (kind 5) forms when **both** of:

1. **Multi-page:** `presentCount ≥ pagedDeltaMinPages × deltaPageSize` (2 × 1024 = 2048). With
   fewer present rows than two pages there is no per-page region to adapt, so the single-page
   forms are kept.
2. **Density savings:** the simulated sum of per-page packed bits (each page's
   `bits.Len64(page_max_offset) × page_rows`) is at least `1/pagedDeltaMinSavedBitFraction`
   (12.5%) smaller than the column-wide bit-packed payload (`bits.Len64(column_max_offset) ×
   presentCount`). Otherwise the per-page headers (17 B/page) are not worth the saving and the
   writer falls through to kind 22.

This is the per-page generalization of the bit-packing in §9.4.1. Its win is real but
locality-dependent: at the default `defaultMaxBlockSpans = 2000` most blocks span fewer than two
pages and stay on kind 22/5; the per-page form triggers when blocks grow toward `MaxBlockSpans`
and the intra-block timestamp distribution is genuinely bimodal (bursty-then-trickle). Selection
is checked **before** the bit-packed (NOTE-215) and byte-width (kind 5) decisions in
`uint64ColumnBuilder.buildData`. The page-size constant is duplicated in the reader
(`deltaPageSizeReader`) because the wire format derives page boundaries from it rather than
storing per-page row counts — the two MUST stay in sync. Reading is unaffected by the flag.

**Uniform-length XOR selection (NOTE-217):** when a bytes column is chosen for XOR encoding
(per `isIDColumn`) and `Config.DisableUniformBytes` is false, the writer prefers the uniform
variant (kind 24, sparse 25, AllPresent 28 — SPECS §9.5.1) over the variable form (kinds 8/9/19)
when **both** of:

1. **More than one present value:** `presentCount > 1`. A single present value gains nothing —
   its `len[4]` prefix is paid once either way, and the uniform header adds `uniform_len[4]`.
2. **All present values share one non-zero length:** a single pass over present rows checks every
   value's `len(v)` equals the first present value's length, and that length is `> 0`.

Zero-length values, mismatched lengths, or fewer than two present rows fall through to kinds
8/9/19. The check is `uniformValueLen` (`encoding_xor.go`), run once inside `encodeXORBytes` before
building the payload. The uniform payload drops the per-row `val_len[4]` prefix (−33% to −50% wire
bytes for 8/16-byte IDs) and removes the per-row `appendUint32LE` from the encode loop. The
AllPresent layering (NOTE-AP-001) composes: a fully-present uniform column emits kind 28. New kind
IDs are additive — no `enc_version` bump. Reading is unaffected by the flag.

**Gorilla Float64 selection (NOTE-219):** float64 columns route to the Gorilla-XOR variant
(kind 40, AllPresent kind 41 — SPECS §9.8) instead of the Dictionary path (kinds 1/2) when
`Config.DisableGorillaFloat64` is false and **all** of:

1. **Amortization:** `presentCount ≥ gorillaMinPresent` (64), so the fixed per-column header
   (`stream_bit_len[8] + first_value[8]`) is small relative to the packed payload.
2. **High cardinality:** `distinct_present_values > max(gorillaCardinalityFloor (64),
   presentCount / gorillaCardinalityFloorFraction (4))`. This is the **two-population guard**.

Rationale — the float column population splits in two (see NOTE-219):

- **Low-cardinality floats** (HTTP sampling ratios `0.0/0.1/0.5/1.0`, rounded utilization gauges,
  TLS versions encoded as floats): the dictionary has ≤16 entries and Dictionary+RLE gives
  ~1–2 B/val. Gorilla would regress these to ~2–3 B/val. The cardinality guard keeps them on
  Dictionary — this is a deliberate exclusion, **not** an oversight.
- **High-cardinality correlated floats** (NOTE-40 numeric-string-promoted `latency_ms`,
  `duration_seconds`): mostly-distinct, adjacency-correlated. The dictionary provides no real
  dedup and pays ~13 B/val after snappy; Gorilla gives ~2–3 B/val (4–6×).

The decision is **purely data-driven** (distinct count vs present rows), computed in a single pass
over the present values (`float64PresenceAndCardinality`), never name- or type-based — so it
generalizes across the whole float population (raw `ColumnTypeFloat64` and promoted
`ColumnTypeRangeFloat64` alike). Selection runs **before** the Dictionary/sparse decision in
`float64ColumnBuilder.buildData`. There is no sparse (>50% nulls) Gorilla variant — high-cardinality
float columns are overwhelmingly fully present after promotion, and the dense kind handles
interleaved nulls via presence-RLE. The AllPresent layering (NOTE-AP-001) composes: a fully-present
Gorilla column emits kind 41. New kind IDs are additive — no `enc_version` bump. Reading is
unaffected by the flag. **Widening the cardinality guard without re-running the threshold sweep
across traces/logs/metrics risks regressing the low-cardinality population.**

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

---

## SPEC-012: Intrinsic Column Encoding Dispatch
*Added: 2026-04-22*

`encodeColumn` in `intrinsic_accum.go` selects the encoding for each intrinsic accumulator
column based on its type and row count:

| Condition | Encoding |
|---|---|
| `len(c.bytesValues) > 0` AND `len(c.refs) > IntrinsicPageSize` | `encodeXORBytesIntrinsic` → `IntrinsicFormatXORBytes` (Format=0x03) |
| `len(c.bytesValues) > 0` AND `len(c.refs) <= IntrinsicPageSize` | `encodeFlatColumn` → `IntrinsicFormatFlat` |
| `len(c.bytesValues) == 0` AND `len(c.refs) > IntrinsicPageSize` (large uint64 flat column) | `encodeDeltaUint64Intrinsic` → `IntrinsicFormatDeltaUint64` (Format=0x04) |
| `len(c.bytesValues) == 0` AND `len(c.refs) <= IntrinsicPageSize` (small numeric column) | `encodeFlatColumn` → `IntrinsicFormatFlat` |

**Invariant:** The threshold `> IntrinsicPageSize` (strict greater-than) means a column with
exactly `IntrinsicPageSize` rows uses the v1 non-paged flat path, not XOR or DeltaUint64 encoding.

**Invariant:** `encodeXORBytesIntrinsic` requires `len(c.bytesValues) > 0`. If called with
an empty accumulator, it falls back to `encodeFlatColumn`.

**Invariant:** `encodeDeltaUint64Intrinsic` requires `len(c.uint64Values) > 0`. If called with
an empty accumulator, it falls back to `encodeFlatColumn`. Values are sorted ascending before
encoding; all deltas are non-negative. Min/Max stored as 8-byte LE binary (not decimal string).

Back-ref: `internal/modules/blockio/writer/intrinsic_accum.go:encodeColumn`,
          `internal/modules/blockio/writer/intrinsic_accum.go:encodeXORBytesIntrinsic`,
          `internal/modules/blockio/writer/intrinsic_accum.go:encodeDeltaUint64Intrinsic`
