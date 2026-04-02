# Code Review: blockpack PR — Code Reuse Issues

Date: 2026-03-29
Branch: blockpack-integration (reviewed against main in /home/matt/source/blockpack-tempo)
Scope: Code reuse, duplication, and missed helper opportunities

---

## Summary

6 distinct reuse issues found across 5 files. None are correctness bugs.
Issue 3 (stale comments) is the highest-priority for reviewers because it now
describes the opposite of the cache's actual eviction behaviour.
Issue 5 (unguarded type assertion) is the highest-priority for runtime safety.

---

## Issue 1 — DUPLICATE: `packKey` helper ignored; inline `<<16` repeated across 5 sites in stream.go

WHAT: `internal/modules/executor/metrics_trace_intrinsic.go:168` already defines a private
`packKey(blockIdx, rowIdx uint16) uint32` helper. The new code in `stream.go` does not use it;
instead it inlines `uint32(ref.BlockIdx)<<16|uint32(ref.RowIdx)` at five separate new sites.
Including pre-existing inlines in `span_fields.go`, `api.go`, and `reader.go`, the same
shift-or pattern appears at approximately 15 distinct source locations.

WHERE:
- Existing helper: `internal/modules/executor/metrics_trace_intrinsic.go:168-170`
- New sites in changed code:
  - `internal/modules/executor/stream.go:919`
  - `internal/modules/executor/stream.go:978`
  - `internal/modules/executor/stream.go:999`
  - `internal/modules/executor/stream.go:1019`
  - `internal/modules/executor/stream.go:1259`
- Also inlined in `intrinsic_ref_index.go:31,46,112,128`, `span_fields.go:180,194,208`,
  `api.go:1144,1168`, `reader.go:262`

WHY: `packKey` is package-private inside `executor`, so callers in `shared` or `api.go` cannot
reach it — but all five new `stream.go` sites could. Inlining makes the bit-width safety
contract (both fields fit in uint16, no overflow) invisible at each call site and creates a
copy-paste surface for future bugs. `RefIndexEntry.Packed` documents the same invariant in a
struct comment rather than enforcing it at a single construction site.

RECOMMENDATION: Promote `packKey` (or rename `PackRef`) to the `shared` package, co-located
with `BlockRef` and `RefIndexEntry`. Use it everywhere a packed ref is constructed. The
`nolint:gosec` annotations can then live in one place.

---

## Issue 2 — DUPLICATE: `overFetch` formula copy-pasted verbatim between two functions in predicates.go

WHAT: The identical multi-condition overFetch branch — including the four-line explanatory
comment — appears twice in the same file.

WHERE:
- `internal/modules/executor/predicates.go:1898-1916` (inside `BlockRefsFromIntrinsicTOC`)
- `internal/modules/executor/predicates.go:1963-1970` (inside `blockRefsFromIntrinsicPartial`)

Both blocks contain:
```go
if len(program.Predicates.Nodes) == 1 {
    overFetch = limit * totalLeaves
} else {
    overFetch = min(min(limit, 50)*10000, 500_000)
}
```
The comment explaining the reasoning is only on the first copy; the second copy is silent.

WHY: Changing the formula (e.g., the 500_000 cap or the 50 clamp) requires two edits.
Both functions have identical `program`, `limit`, and `totalLeaves` shapes, making extraction
trivial.

RECOMMENDATION: Extract `computeOverFetch(limit, nodeCount, totalLeaves int) int`. Both
functions call it after computing `totalLeaves`. Move the explanation comment to the helper.

---

## Issue 3 — STALE COMMENTS: objectcache changed from weak to strong refs but 7 comments still say weak

WHAT: `objectcache/cache.go` was changed in this PR from `weak.Pointer`-backed (GC reclaims
when no strong reference exists) to strong-reference-backed (entries persist until `Clear()` or
process exit). Seven comments across two files still describe the old weak-pointer semantics.

WHERE:
- `internal/modules/blockio/reader/parser.go:17` — "GC-cooperative: entries are reclaimed when no Reader holds a strong reference."
- `internal/modules/blockio/reader/parser.go:24` — "GC-cooperative via weak.Pointer — the GC may reclaim when no Reader holds a strong reference."
- `internal/modules/blockio/reader/parser.go:29` — "fileID+'/intrinsic/'+colName. GC-cooperative via weak.Pointer."
- `internal/modules/blockio/reader/parser.go:34` — "GC-cooperative: allows the GC to reclaim ~45 MB metadata when no Reader is open."
- `internal/modules/blockio/reader/parser.go:44` — "Maps can be addressed with weak.Make by taking &m ..."
- `internal/modules/blockio/reader/intrinsic_reader.go:155` — "NOTE-003: process-level cache uses objectcache.Cache (GC-cooperative weak pointers) instead of sync.Map with strong refs to allow GC to reclaim decoded columns."
- `internal/modules/blockio/reader/reader.go:67,73` — "keeping its weak.Pointer entry valid for the lifetime of this Reader."

WHY: These comments now describe the opposite of actual behaviour. Under strong-reference
semantics the GC will not reclaim decoded columns even when all Reader instances for a file are
closed. Someone debugging memory growth will read these comments and look for strong-reference
leaks, not realising the cache itself is intentionally holding everything alive. The NOTE-003
cross-reference is also now misleading.

RECOMMENDATION: Update all seven comment sites to describe strong-reference semantics, remove
references to `weak.Pointer` and `weak.Make`, and update NOTE-003 in the reader NOTES.md to
document the intentional change and the `GOMEMLIMIT` bounding strategy.

---

## Issue 4 — MISSED REUSE: `buildIntrinsicBytesMap` in api.go not migrated to `LookupRefFast`

WHAT: `buildIntrinsicBytesMap` (`api.go:1158`) constructs a `map[uint32][]byte` over the
entire flat intrinsic column. The `EnsureRefIndex` / `LookupRefFast` machinery introduced in
this PR was designed specifically to replace this pattern, and is already used in `stream.go`
for the timestamp column. The `api.go` fallback paths (trace:id and span:id ID extraction)
were not updated.

WHERE:
- `api.go:1158-1173` — `buildIntrinsicBytesMap` still builds a `map[uint32][]byte`
- `api.go:613,617` — lazy-init path in `streamFilterProgram` calls `buildIntrinsicBytesMap`
- `api.go:1092,1093` — eager path in the log streaming function calls `buildIntrinsicBytesMap`

WHY: `buildIntrinsicBytesMap` allocates a typed map over the full column per file per call.
`EnsureRefIndex` + `LookupRefFast` builds a compact sorted index cached on the column object
itself (via sync.Once inside objectcache), so it is free on subsequent queries for the same
file. Using two different lookup strategies for structurally identical problems increases the
maintenance surface and means the api.go path does not benefit from the cached index.

RECOMMENDATION: Replace `buildIntrinsicBytesMap` with a direct call to `col.EnsureRefIndex()`
followed by `col.LookupRefFast(packed)` at the two lookup sites in `extractIDs`. The `[]byte`
branch of `LookupRefFast` already handles `BytesValues` flat columns.

---

## Issue 5 — UNSAFE ASSERTION: `val.(uint64)` on `LookupRefFast` return, two sites, no fallback

WHAT: `LookupRefFast` returns `(any, bool)`. At two call sites the returned `any` is
immediately type-asserted to `uint64` with no comma-ok guard.

WHERE:
- `internal/modules/executor/stream.go:921` — `pairs = append(pairs, refTS{ref: ref, ts: val.(uint64)})`
- `internal/modules/executor/stream.go:1024` — `ts := val.(uint64)`

WHY: The previous code used `tsMap[key]` where `tsMap` was typed `map[uint32]uint64`, so the
type was statically guaranteed. Switching to `any` return without a defensive assertion makes
the failure mode a runtime panic rather than a compile error. If a future format or bug stores
the timestamp column with a different type (e.g., `int64`), these sites panic instead of
producing an error or skipping the row.

RECOMMENDATION: Either (a) add a typed method `LookupUint64Fast(packedRef uint32) (uint64, bool)`
to `IntrinsicColumn` that avoids `any` boxing entirely, or (b) use `ts, ok := val.(uint64); if !ok { continue }`.
Option (a) is preferred: it eliminates interface-boxing allocation for the uint64, and makes
the type contract visible at the call site.

---

## Issue 6 — API DESIGN: `EnsureRefIndex` must be called before `LookupRefFast`; silent miss if forgotten

WHAT: `LookupRefFast` checks `len(col.refIndex) == 0` and returns `(nil, false)` when the
index has not been built. It does not build the index itself. Every caller is required to call
`EnsureRefIndex` first. Three call sites in `stream.go` do this correctly, but `lookupColumn`
(inside `lookupIntrinsicFields`) calls `EnsureRefIndex` at the top of the helper rather than
letting `LookupRefFast` self-initialise. Forgetting `EnsureRefIndex` at any future call site
produces silent data loss — every lookup returns a miss with no error and no log.

WHERE:
- Required pre-call documented at `internal/modules/blockio/shared/intrinsic_ref_index.go:57`
- Call sites that correctly pre-call: `stream.go:882`, `stream.go:1016`, `stream.go:1258`
- Different pattern inside helper: `stream.go:1257` calls `col.EnsureRefIndex()` before loop

WHY: An "I must be called first" API contract that silently produces wrong results when
violated is fragile. The sync.Once inside `EnsureRefIndex` means calling it inside
`LookupRefFast` costs exactly one atomic load on the hot path — the same cost already paid to
check `len(col.refIndex) == 0`.

RECOMMENDATION: Move the `col.EnsureRefIndex()` call to the top of `LookupRefFast` (before the
nil guard). Remove the explicit pre-calls from all call sites. If the separation is kept for
documentation reasons, at minimum make `LookupRefFast` call `EnsureRefIndex` itself rather than
returning a silent miss.

---

## Non-Issues Noted

- `block.go` sync.Once additions (`decodeOnce`, `presenceOnce`, `denseOnce`): correct, no reuse issue.
- `objectcache/cache.go` weak-to-strong simplification: intentional design change, the implementation is correct.
- `fileLevelBloomReject` added in `stream.go:223`: correctly reuses the existing function from `plan_blocks.go`.
- `collectIntrinsicPlain` boolean removal: reduces code, improves reuse of the intrinsic-lookup path.
