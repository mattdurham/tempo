# Reader Module — Design Notes

## NOTE-001: Lazy Column Decode (Presence-First, On-Demand Full Decode)
*Added: 2026-03-05*

**Problem:** `ParseBlockFromBytes` with a `wantColumns` filter decoded only the predicate
columns eagerly, leaving all remaining columns absent from the block. Callers (executor)
had to issue a second pass via `AddColumnsToBlock` to decode those remaining columns eagerly
before the row loop — decoding ~90 columns per block even when only ~10-15 were accessed.

**Solution:** Replace the two-pass decode with a single-pass lazy model:
1. **Eager pass** — decode `wantColumns` fully (presence + values). Unchanged.
2. **Lazy registration** — for all other columns, call `decodePresenceOnly` (no zstd
   decompression) and store a `rawEncoding` sub-slice into the block's raw bytes. The
   `Column.Present` bitset is populated immediately; value data is deferred.
3. **On-demand full decode** — the first call to any value accessor (`StringValue`,
   `Uint64Value`, etc.) checks `rawEncoding != nil` and calls `decodeNow()`, which performs
   the full zstd decompression and populates all typed fields.

**`decodePresenceOnly` complexity:** O(M/8) where M is span count — scans past compressed
blob length prefixes to reach the uncompressed presence RLE. No zstd involved.

**`Column.IsPresent()` contract:** Always available after `ParseBlockFromBytes` without
triggering `decodeNow()`. Presence is decoded during lazy registration.

**Savings estimate (T9/Q66, 1997 blocks, ~90 non-predicate columns):**
- Old: 1997 × 90 × zstd_decompress ≈ 870ms
- New: 1997 × 90 × presence_only + 1997 × ~15 × zstd_decompress ≈ 40ms + 150ms = 190ms
- Expected saving: ~680ms per query

Back-ref: `internal/modules/blockio/reader/column.go:decodePresenceOnly`,
`internal/modules/blockio/reader/column.go:decodeNow`,
`internal/modules/blockio/reader/block.go:Column`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## NOTE-002: rawEncoding Lifetime Safety
*Added: 2026-03-05*

**Invariant:** `rawEncoding` is a sub-slice into `BlockWithBytes.RawBytes`. It is valid
for the lifetime of the owning `BlockWithBytes`. All lazy decodes (`decodeNow()`) complete
within the same block's row loop before `bwb` goes out of scope.

**internMap safety:** `internMap` is borrowed from `Reader.internStrings`. `ResetInternStrings()`
is called before each block's `ParseBlockFromBytes`. Lazy decodes for a given block complete
before the next `ResetInternStrings()` call, so the borrowed reference is always valid.

**Single-goroutine guarantee:** The scan path is single-goroutine. No locking is required
for `decodeNow()`.

Back-ref: `internal/modules/blockio/reader/column.go:decodeNow`

---

## NOTE-003: Intrinsic Column Decode — Lazy Mirrors Block Column Lazy Decode
*Added: 2026-03-11*

The intrinsic column design follows the same lazy-decode pattern as block columns (see SPEC-002):
- **TOC is parsed eagerly** at `NewReaderFromProvider` time (one I/O for the TOC blob).
  This is analogous to how metadata is parsed eagerly.
- **Column blobs are decoded lazily** on first `GetIntrinsicColumn(name)` call.
  This avoids loading all column data (potentially many MB) when only one column is needed.
- **Results are cached** in `intrinsicDecoded` so repeated calls are O(1) map lookups.

The decode functions (`shared.DecodeTOC`, `shared.DecodeIntrinsicColumnBlob`) live in the
`shared` package (not `writer`) to avoid a circular import: `writer` imports `reader`, so
`reader` cannot import `writer`. Both packages can safely import `shared`.

Back-ref: `internal/modules/blockio/reader/intrinsic_reader.go`,
`internal/modules/blockio/shared/intrinsic_codec.go`
