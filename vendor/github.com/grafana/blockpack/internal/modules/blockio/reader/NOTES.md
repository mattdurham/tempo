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
