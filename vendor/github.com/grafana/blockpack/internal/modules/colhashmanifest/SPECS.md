# colhashmanifest — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/colhashmanifest` package. It complements `NOTES.md` (design rationale), per
root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-COLMANIFEST-N`, independent
of the `SPEC-VI-N` (per-module-local in each of `valueindex`, `valueindexcompactor`,
`valueindexconsumer`) and `SPEC-VC-N` (per-module-local in each of `valuecounts`,
`valuecountscompactor`) spaces — this module is a genuinely separate, orthogonal concern
(operational-visibility audit manifest, task #216) shared by BOTH the VI and VCNT domains, not
a fifth/third participant in either's own numbering, matching the precedent
`viusage/SPECS.md`'s own ID-convention section documents for the identical situation. IDs are
assigned in ascending order and never reused or renumbered.

---

## SPEC-COLMANIFEST-1: `Entry` / manifest wire format

A per-tenant manifest is a JSON object at `ManifestPath(tenant)` =
`<tenant>/column_manifest/index.json`:

```json
{
  "version": 1,
  "entries": [
    {
      "tenant": "11638",
      "column_hash": "1f3c...  (32 lower-hex chars)",
      "column_name": "span.name",
      "first_seen_by": "vi",
      "first_seen_at_sec": 1752000000,
      "last_seen_at_sec": 1752003600
    }
  ]
}
```

- `column_hash` is `valueindex.ColHash(column_name)`, which is byte-for-byte identical to
  `valuecounts.ColHash(column_name)` (both are `hex(SHA256(column_name)[:16])`) — this package
  does not itself compute or import either hash function; callers already have both the hash
  and the name at their own write boundary and pass them in directly (see NOTES.md
  NOTE-COLMANIFEST-1 for why this package stays uncoupled from both `valueindex` and
  `valuecounts`).
- `first_seen_by` is one of `"vi"`, `"vcnt"`, or `"both"` (`SourceVI`, `SourceVCNT`,
  `SourceBoth`). It upgrades from `"vi"`/`"vcnt"` to `"both"` the first time the OTHER
  pipeline observes the same `(tenant, column_hash)` pair; it never downgrades.
- Entries are keyed by `(tenant, column_hash)`. A given tenant's manifest never has two entries
  with the same `column_hash` — `RecordColumn` locates and updates the existing entry rather
  than appending a duplicate.
- This manifest is **never read by any VI/VCNT read/write/query path**. It exists solely for
  human/tooling consumption (e.g. answering "what columns actually have VI/VCNT data for this
  tenant" when browsing object storage directly, where only opaque hash-named directories are
  otherwise visible).

Back-ref: `internal/modules/colhashmanifest/manifest.go:Entry,manifestIndex,ManifestPath`.

## SPEC-COLMANIFEST-2: `Store` — the minimal, deliberately conditional-PUT-free contract

```go
type Store interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, data []byte) error
}
```

Unlike `internal/modules/cube.ObjectStore` and `internal/modules/viusage.ObjectStore`, `Store`
has **no `ConditionalPut`/ETag contract**, and `Get` does **not** need to distinguish "object
not found" from any other error — `Load` treats every `Get` failure identically as "start from
an empty manifest" (see NOTES.md NOTE-COLMANIFEST-1 for the correctness-stakes argument this
relies on). This means any existing object-storage interface in this codebase that already
exposes ctx-based `Get`/`Put` (e.g. `valuecountscompactor.Store`, `valueindexcompactor.IndexStore`)
satisfies `Store` structurally, with no adapter, since Go allows assigning a superset-method-set
interface value to a narrower interface-typed variable.

Back-ref: `internal/modules/colhashmanifest/manifest.go:Store`.

## SPEC-COLMANIFEST-3: `Load` — best-effort decode, never a hard failure on missing/unreadable state

`Load(ctx, store, tenant) ([]Entry, error)` fetches and decodes `tenant`'s manifest. `Load`
returns `(nil, nil)` — not an error — whenever `store.Get` returns ANY error (not found,
transient, or otherwise) or when the stored object is empty. `Load` returns a real (non-nil)
error only when the object storage returned actual bytes that failed to JSON-decode
(corruption). Callers (including `RecordColumn`) never need special-case "not found" handling.

Back-ref: `internal/modules/colhashmanifest/manifest.go:Load`.

## SPEC-COLMANIFEST-4: `RecordColumn` — write-once-per-genuinely-new-fact contract

`RecordColumn(ctx, store, tenant, colHash, colName, source, nowSec) error`:

1. Rejects any `source` other than `SourceVI`/`SourceVCNT` with an error (does not write).
2. If `(tenant, colHash)` has never been recorded: creates a new `Entry` with
   `FirstSeenBy = source`, `FirstSeenAtSec = LastSeenAtSec = nowSec`, and persists it — exactly
   one `Get` + one `Put`.
3. If `(tenant, colHash)` is already recorded with `FirstSeenBy` equal to `source` OR already
   `SourceBoth`: a **pure no-op** — exactly one `Get`, **zero** `Put` calls. This is the
   "write/update once per colHash when first seen" contract (task #216 design decision 3):
   repeated observations from a source that has already been recorded never re-write the
   manifest.
4. If `(tenant, colHash)` is recorded with the OTHER source: upgrades `FirstSeenBy` to
   `SourceBoth`, bumps `LastSeenAtSec`, and persists — one `Get` + one `Put`. `FirstSeenAtSec`
   is never modified after creation.
5. `RecordColumn` returns the real error from a failed `Put` (or from a corrupt existing
   manifest surfaced by `Load`) rather than swallowing it — swallowing/logging is explicitly
   the CALLER's responsibility (every current call site logs at Warn and discards), never this
   function's, so `RecordColumn` remains independently testable for its own error contract.

No conditional-PUT/ETag/retry-on-conflict machinery: concurrent writers can race and one's
update can be lost under true concurrency. This is an accepted, documented trade-off — see
NOTES.md NOTE-COLMANIFEST-1 — because this manifest is advisory-only and self-heals on the next
observation of the same colHash from any pipeline that later re-touches it (NOTE-COLMANIFEST-1
documents a residual gap for a colHash that is never revisited).

Back-ref: `internal/modules/colhashmanifest/manifest.go:RecordColumn`.

## SPEC-COLMANIFEST-5: `manifestOpTimeout` — every `Store` call is independently bounded, never inherited unbounded from the caller

`Load` and `RecordColumn` wrap every `store.Get`/`store.Put` call in its own
`context.WithTimeout(ctx, manifestOpTimeout)` (3 seconds), derived from whatever `ctx` the
caller supplies. This guarantees that a `Store` implementation which HANGS (as opposed to
erroring quickly) can never block a caller indefinitely: `Load` returns within `manifestOpTimeout`
of a hanging `Get` (treating the resulting `context.DeadlineExceeded` exactly like any other
`Get` error, per SPEC-COLMANIFEST-3); `RecordColumn` returns within roughly `2 x
manifestOpTimeout` in the worst case (one bound for its internal `Load` call, one for its own
`Put` call) of a `Store` that hangs on every call.

This exists because `Load`/`RecordColumn` are invoked synchronously, inline, from
`valueindexconsumer.flushColumn` and `valuecountscompactor.mergeLevel` — both run on their
service's single processing goroutine, using a long-lived top-level `ctx` with no deadline of
its own. Without this bound, a hanging `Store` (e.g. a network partition) would block real VI
flush acking / VCNT compaction indefinitely, contradicting every caller's own "never blocks the
real write path" documentation, which was previously only true against a fast-erroring store —
see NOTES.md NOTE-COLMANIFEST-2 for the full CRITICAL-finding writeup.

Back-ref: `internal/modules/colhashmanifest/manifest.go:manifestOpTimeout,Load,RecordColumn`.

## SPEC-COLMANIFEST-6: Native Postgres Store must be behaviorally identical to the blob-backed implementation, and must still respect manifestOpTimeout under real query contention (issue #506)

*Added: 2026-07-15*

**Invariant:** A native Postgres `Store` implementation (`colhashmanifest.PgStore`) MUST be
behaviorally identical to the blob-backed implementation for `Load`/`RecordColumn` — backend
choice never changes their observable contract (SPEC-COLMANIFEST-3/4). Verified by
`TestRecordColumn_BlobAndPgBackends_IdenticalBehavior` (`pg_blob_differential_test.go`), with
explicit attention to `FirstSeenAtSec` never being mutated by the upgrade-to-`SourceBoth` step, on
either backend.

A Postgres `Store` implementation MUST still respect the caller-derived `manifestOpTimeout`
deadline (SPEC-COLMANIFEST-5) even under REAL query contention (a blocked-on-a-row-lock query),
not just a fast-erroring failure — verified by `TestRecordColumn_PgStore_HangingQueryRespectsCtxTimeout`
(`pg_hanging_query_test.go`), which holds a real, uncommitted transaction locking `RecordColumn`'s
target key and confirms `RecordColumn`'s `Put` call surfaces a real, non-nil, propagated error
within a bounded time (never swallowed — SPEC-COLMANIFEST-4 point 5 — unlike the blob-backed
hanging test's `Get`-swallow path, TEST-COLMANIFEST-15) — mirroring
`TestRecordColumn_HangingStoreDoesNotBlockForever`'s existing discipline for the blob-backed case.

**Back-ref:** `internal/modules/colhashmanifest/pg_store.go`,
`internal/modules/colhashmanifest/pg_blob_differential_test.go:TestRecordColumn_BlobAndPgBackends_
IdenticalBehavior`, `internal/modules/colhashmanifest/pg_hanging_query_test.go:
TestRecordColumn_PgStore_HangingQueryRespectsCtxTimeout`. See NOTE-COLMANIFEST-3. Issue #506.
