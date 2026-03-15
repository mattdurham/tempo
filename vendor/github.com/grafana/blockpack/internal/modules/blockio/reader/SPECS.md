# Reader Module — Specifications

## SPEC-001: Column.IsPresent() — Always Available After ParseBlockFromBytes
*Added: 2026-03-05*

`Column.IsPresent(idx int) bool` returns the correct presence value for any column
returned by `ParseBlockFromBytes`, regardless of whether the column was eagerly decoded
(in `wantColumns`) or lazily registered (not in `wantColumns`).

Lazy registration decodes the presence bitset immediately via `decodePresenceOnly`.
`IsPresent()` MUST NOT trigger a full decode (`decodeNow()`).

Back-ref: `internal/modules/blockio/reader/block.go:IsPresent`

---

## SPEC-002: Column Value Accessors — May Trigger Lazy Decode
*Added: 2026-03-05*

`StringValue`, `Int64Value`, `Uint64Value`, `Float64Value`, `BoolValue`, `BytesValue`
check `rawEncoding != nil` on entry. If set, they call `decodeNow()` to perform the full
decode before accessing typed fields.

**Contract:** After the first call to any value accessor on a lazily-registered column,
subsequent calls return values from the fully-decoded column without re-decoding.

**Error behavior:** If `decodeNow()` encounters a decode error, `rawEncoding` is cleared
and all value accessors return zero/false for all rows (column treated as absent).

Back-ref: `internal/modules/blockio/reader/block.go:StringValue`,
`internal/modules/blockio/reader/column.go:decodeNow`

---

## SPEC-003: ParseBlockFromBytes — All Columns Registered After Return
*Added: 2026-03-05*

After `ParseBlockFromBytes(raw, wantColumns, meta)` returns:
- Columns in `wantColumns` are **eagerly decoded** (presence + values fully available).
- All other columns with `dataLen > 0` are **lazily registered** (presence available,
  values deferred until first accessor call).
- Columns with `dataLen == 0` (trace-level columns) are absent.
- When `wantColumns == nil`, all columns are eagerly decoded (original behavior unchanged).

**NOTE-001:** Lazy registration replaces the old requirement for a second pass via
`AddColumnsToBlock` to access non-predicate columns.

Back-ref: `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`internal/modules/blockio/reader/reader.go:ParseBlockFromBytes`
