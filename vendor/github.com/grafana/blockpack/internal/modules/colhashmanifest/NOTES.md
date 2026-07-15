# colhashmanifest — Design Notes

This document records the design decisions and rationale behind
`internal/modules/colhashmanifest` (task #216: a colHash -> column-name operational-visibility
manifest). Entries are append-only and dated; never delete or rewrite an existing entry — add
an `*Addendum (date):*` note if a decision is later corrected or superseded.

## ID convention

Entries use the module-local prefix `NOTE-COLMANIFEST-N`, independent of the shared `NOTE-VI-N`
space (`valueindex`/`valueindexcompactor`/`valueindexconsumer`/`vibuilder`/`viusage`) and the
shared `NOTE-VC-N` space (`valuecounts`/`valuecountscompactor`). This module is a genuinely
separate, orthogonal concern shared by BOTH domains, matching the precedent
`viusage/NOTES.md`'s own ID-convention section documents for the identical situation.

---

## NOTE-COLMANIFEST-1 — Why this module exists, why it is best-effort-only, and why it deliberately does not import `valueindex` or `valuecounts`

Date: 2026-07-13

**The problem.** Both the value index (VI) and value counts (VCNT) pipelines key their entire
on-disk file layout by `colHash` — a genuine one-way hash
(`hex(SHA256(colName)[:16])`, `valueindex/hash.go:ColHash`, `valuecounts/filename.go:ColHash`)
— with no metadata file anywhere recording which column name produced a given hash. Browsing
object storage directly shows only opaque hash-named directories; there is no way to answer
"what columns actually have VI/VCNT data for this tenant" without already knowing which
candidate names to check. `internal/modules/cube` solved the analogous problem for itself with
`RegistryEntry`/`Registry` (a real, persisted, bulk-loadable registry mapping every hash-derived
`CubeID` back to its full definition) — this module mirrors that pattern's *spirit* (a
JSON-serialized entry per hash, persisted to object storage, loadable in bulk) for VI+VCNT.

**One shared manifest, not two.** `valueindex.ColHash` and `valuecounts.ColHash` are the
IDENTICAL hash function, computed independently by each package (deliberately — see below) —
they could genuinely collide on the same column name. A single manifest keyed by `colHash`
avoids ever having two disagreeing sources of truth for the same hash. `Entry.FirstSeenBy`
(`"vi"` / `"vcnt"` / `"both"`) records which pipeline(s) actually observed each hash, which is
itself useful audit information (e.g. a column indexed by VCNT but never by VI, or vice versa).

**One aggregate per-tenant file, not one file per colHash directory.** Mirrors `cube`'s own
`<tenant>/cubes/index.json` and `viusage`'s own `<tenant>/viusage/index.json` placement
convention — a single small JSON file per tenant, not a proliferation of tiny sidecar files
one per column-hash directory. Placed at `<tenant>/column_manifest/index.json` (top-level,
not nested under either pipeline's `indexPrefix`, since it describes BOTH pipelines and
neither's `indexPrefix` is a natural home for it).

**Why this package does NOT import `internal/modules/valueindex` or
`internal/modules/valuecounts`, unlike `viusage` (which DOES import `valueindex` for its
`ColHash` function).** `valuecountscompactor/service.go`'s `ownsShard` doc comment records an
explicit, deliberate design choice: "`valuecounts.ColHash` and `valueindex.ColHash` are not
compile-time coupled" — the two packages independently reimplement the identical hash function
rather than one importing the other, to avoid coupling the VI and VCNT domains together. This
module respects that same boundary: it is generic, orthogonal infrastructure (a JSON
registry keyed by an opaque string), and never needs to know how `colHash` was computed or
which of the two `ColHash` functions produced it — callers (`valueindexconsumer`,
`valuecountscompactor`) already have `(tenant, colHash, colName)` in hand at their own write
boundary and pass them in directly as plain strings. Creating this as its own sibling package
(rather than adding it to `valueindex` or `valuecounts` directly) keeps that boundary intact
while still giving VI and VCNT ONE shared registry implementation to call into — the best of
both: no new coupling between the two domains, and no risk of two independently-drifting
manifest implementations.

**Why `Store` has no conditional-PUT/ETag contract, unlike `cube.ObjectStore` /
`viusage.ObjectStore`.** Both of those registries require `ConditionalPut`-with-retry because
their read paths depend on the registry's correctness — `cube`'s query router and `viusage`'s
backfill trigger both make real decisions based on registry state, so a lost concurrent update
(or, worse, a misread "not found" destructively overwriting other tenants' entries — see
`cube/registry.go`'s and `viusage/registry.go`'s own extensive doc comments on this exact
historical bug class) is a genuine correctness bug. This manifest is **never consulted by any
read/write/query path** (SPEC-COLMANIFEST-1) — it exists purely for human/tooling
consumption. A lost update under true concurrent writers (rare: this manifest is only mutated
at most twice per `(tenant, colHash)` ever — once to create, once to upgrade to `"both"` — see
SPEC-COLMANIFEST-4) means, in the worst case, an entry is briefly missing or a `FirstSeenBy`
upgrade is briefly delayed; the VERY NEXT observation of that same colHash from ANY pipeline
that later re-touches it re-attempts the same write and self-heals. **This self-heal claim has
a real, accepted gap, not a universal guarantee (found by a 2026-07-13 go-presubmit review):**
in `valuecountscompactor` specifically, different `ShardIndex` replicas own disjoint, stable
colHash sets for the same tenant forever (`ownsShard`), so two shards can race on the same
tenant's manifest file while recording DIFFERENT colHashes, and whichever `Put`s last silently
discards the other's brand-new entry. If that specific colHash is never revisited again (e.g. a
column whose VCNT data is written once and then goes permanently quiet, or a rare tag value with
no future observation to retry the write), there is no future retry to self-heal from and the
entry can permanently miss the manifest. This residual gap is still an accepted trade-off — this
manifest is purely best-effort and never read by any correctness-relevant path — but the
"self-heals" framing above should not be read as "always eventually consistent, no exceptions."
Requiring every caller's object-storage interface to additionally
implement `ConditionalPut` (none of `valueindexconsumer.ObjectPutter`,
`valuecountscompactor.Store`, or `valueindexcompactor.IndexStore` currently do) would be a
real widening of scope for a purely-advisory feature — not worth the cost for this LOW-priority
ticket's stakes. Likewise, `Load` intentionally does NOT distinguish "not found" from any other
`Get` error (SPEC-COLMANIFEST-3) — the precision `cube.ObjectStore.Get`/`viusage.ObjectStore.Get`
require exists specifically to prevent a real Get failure being misread as "empty" and
triggering a destructive unconditional overwrite; here, "misread as empty" merely means this
pass re-derives a slightly incomplete manifest, which is an acceptable, self-healing trade.

**Call sites and the "first seen by blockpack's own write path" caveat for VCNT.** Task #216
requires this to be blockpack-side (not a new tempo-side concern) — the manifest populates
automatically whenever blockpack's own write paths compute a colHash for the first time. For
VI, that is genuinely blockpack's own L0 write:
`internal/modules/valueindexconsumer/service.go:flushColumn`, right after its own
`s.store.Put` succeeds (see `valueindexconsumer/NOTES.md` NOTE-VI-106). For VCNT, blockpack has
NO L0 write path of its own — the root `vcnt.go`'s own doc comment confirms tempo performs the
actual L0 `.vcnt` PUT to S3 using blockpack's helper functions (`EncodeVCNTFile`,
`VCNTObjectKey`, etc.), not blockpack itself. The earliest point blockpack's OWN code ever
touches a given colHash's VCNT data via object storage is therefore compaction
(`internal/modules/valuecountscompactor/service.go:mergeLevel`) — see
`valuecountscompactor/NOTES.md` NOTE-VC-019 for the full accounting of this lag and why it was
accepted rather than requiring new tempo-side code.

**Why `RecordColumn` returns Put errors instead of swallowing them.** "Never fail the real
write" is the CALLER's contract, not this function's — every call site
(`valueindexconsumer.recordManifestEntry`, `valuecountscompactor.recordManifestEntry`) already
logs-and-discards. Keeping `RecordColumn` itself honest about its own errors (rather than
having it silently eat them) keeps this package's own test suite meaningful — a test asserting
"a broken store surfaces an error" would be untestable if this function pre-swallowed
everything.

Back-refs: `internal/modules/colhashmanifest/manifest.go` (`Entry`, `Store`, `Load`,
`RecordColumn`, `ManifestPath`), `internal/modules/valueindexconsumer/service.go:recordManifestEntry`,
`internal/modules/valuecountscompactor/service.go:recordManifestEntry`. See SPECS.md
SPEC-COLMANIFEST-1 through 4, `valuecountscompactor/service.go`'s `ownsShard` doc comment (the
VI/VCNT hash-decoupling precedent this module respects), and `cube/registry.go` /
`viusage/registry.go` (the conditional-PUT pattern this module deliberately does NOT copy, and
why).

---

## NOTE-COLMANIFEST-2 — `manifestOpTimeout`: a hanging Store, not just a failing one, must never block the real VI/VCNT write path

Date: 2026-07-13

**The gap this closes (CRITICAL finding, review-consolidator + go-presubmit-reviewer, task
#216 follow-up):** `Load`/`RecordColumn` originally called `store.Get`/`store.Put` directly on
whatever `ctx` the caller passed in, with no timeout of their own. `flushColumn`
(`valueindexconsumer`) and `mergeLevel` (`valuecountscompactor`) both call `RecordColumn`
synchronously, inline, on their service's single processing goroutine, using a long-lived
top-level `ctx` with no deadline of its own. Every existing test at the time (`TestRecordColumn_
PutErrorPropagates`, `TestFlushColumn_ManifestPutFailureDoesNotFailFlush`, `TestMergeLevel_
ManifestPutFailureDoesNotFailMerge`) only proved tolerance of a FAST error from a misbehaving
`Store` — none exercised a `Store` that HANGS (e.g. a network partition to whatever backs it)
rather than erroring quickly. Against a hanging `Store`, the pre-fix code would block
`flushColumn`/`mergeLevel` forever: `flushColumn` never reaches `resolvePendingAcks`/`Ack`, so
real, already-flushed VI columns stop being acknowledged and the Redis backlog grows
unboundedly; `mergeLevel` never returns, blocking that shard's entire VCNT compaction pass
indefinitely. This directly contradicted this package's and its callers' own repeated "never
blocks the real write path" documentation, which was only ever true against a fast-erroring
store, not a hanging one.

**Fix:** `manifestOpTimeout` (3 seconds) bounds every `Store` call this package issues with its
own independent `context.WithTimeout`, derived from whatever `ctx` the caller supplies rather
than inherited unbounded — one wrap around `Load`'s `Get` call, one wrap around `RecordColumn`'s
`Put` call. The worst case for a single `RecordColumn` call that both creates a new entry AND
hits a hanging store on every call is now bounded at ~2× `manifestOpTimeout` (one timeout for
`Load`'s `Get`, one for the subsequent `Put`) instead of unbounded. A timeout expiry surfaces to
`Load` as an ordinary `Get` error (swallowed exactly like any other `Get` failure, per
SPEC-COLMANIFEST-3) and to `RecordColumn`'s caller as an ordinary `Put` error (logged at `Warn`
and discarded by every current call site, per SPEC-COLMANIFEST-4 point 5) — no new error type,
no new caller-visible contract change beyond the bound itself.

**Regression test:** `TestRecordColumn_HangingStoreDoesNotBlockForever` (`manifest_test.go`)
constructs a `Store` whose `Get`/`Put` block on `<-ctx.Done()` rather than returning a fast
error, and asserts `RecordColumn` still returns within a bounded time (confirmed red before this
fix — it hung against the test's own 10s bound — and green after, returning in ~2×
`manifestOpTimeout`). Mirrored at both hook call sites:
`TestFlushColumn_HangingManifestStoreDoesNotBlockFlush` (`valueindexconsumer/manifest_hook_
test.go`) and `TestMergeLevel_HangingManifestStoreDoesNotBlockMerge`
(`valuecountscompactor/manifest_hook_test.go`).

Back-refs: `internal/modules/colhashmanifest/manifest.go:manifestOpTimeout,Load,RecordColumn`.
See SPECS.md SPEC-COLMANIFEST-5.

## NOTE-COLMANIFEST-3 — Native Postgres Store: generic key/blob table chosen over a normalized per-Entry table (issue #506)

Date: 2026-07-15

**Decision:** `pg_store.go` adds `PgStore{pool}` / `NewPgStore(pool)`, a fresh (not ported) native
Postgres-backed implementation of `Store` (SPEC-COLMANIFEST-2), backed by a generic key/blob table
(`column_manifest_blobs`: `key TEXT PRIMARY KEY, data BYTEA, updated_at BIGINT`) — deliberately NOT
a normalized table with one row per `Entry`. `Get` = `SELECT data WHERE key=$1`; `Put` = `INSERT
... ON CONFLICT(key) DO UPDATE`. No transaction, no advisory lock, no `SELECT ... FOR UPDATE`.

**Why key/blob, not normalized (spec-oracle-506's Open Decision D1 review, approved before
implementation):**
1. `Store`'s own contract (SPEC-COLMANIFEST-2) is ALREADY blob-shaped — `Get(ctx,key)([]byte,error)`/
   `Put(ctx,key,data)error` operating on one opaque key (`ManifestPath(tenant)`,
   SPEC-COLMANIFEST-1) holding one whole JSON-encoded manifest, not one row per `Entry`. A
   key/blob table maps 1:1 onto `Store`'s own two methods with ZERO additional logic in
   `pg_store.go` — `RecordColumn`/`Load`'s entire business logic (the idempotent-per-colHash
   create/no-op/upgrade-to-`SourceBoth` state machine, SPEC-COLMANIFEST-4) keeps running
   byte-for-byte identically regardless of backend, since `manifest.go` only ever calls
   `Store.Get`/`Store.Put`.
2. A normalized table would have forced `pg_store.go` to reimplement SPEC-COLMANIFEST-4's decision
   table as SQL, AND reimplement the JSON blob's encode/decode round-trip (splitting one
   `Put(wholeBlob)` into N per-entry row upserts, reassembling N rows into one blob on `Get`) —
   real, avoidable duplication of already-tested Go logic, and a genuine behavioral-drift risk
   between backends (no SPECS.md anywhere in this codebase permits backend-dependent behavioral
   divergence).
3. No query-by-subfield need exists to justify normalization's usual benefit — this manifest is
   never consulted by any correctness-relevant read/write/query path (SPEC-COLMANIFEST-1), only
   ever loaded whole per tenant.
4. `PgStore.Get` on a missing key returns a real, non-nil, wrapped error (never `(nil, nil)`) —
   matching `Store`'s own doc-comment expectation of a genuine error signal for a miss, even
   though `Load` (SPEC-COLMANIFEST-3) treats ANY `Get` error identically as "empty manifest" one
   layer up, so this is behaviorally equivalent to the blob backend either way from `Load`'s
   perspective.

**No row-locking/ETag ceremony, by design:** matches NOTE-COLMANIFEST-1's own rationale exactly —
this manifest is advisory-only, never consulted by any correctness-relevant path, and already
tolerates lost updates under true concurrency on the blob backend. Giving the Postgres backend
row-lock/ETag rigor cube/viusage's own `EntryStore` implementations need would over-engineer a
LOW-priority, best-effort feature against its own already-reasoned simplicity choice.

**Verified end-to-end** by `TestRecordColumn_BlobAndPgBackends_IdenticalBehavior`
(`pg_blob_differential_test.go`) and `TestRecordColumn_PgStore_HangingQueryRespectsCtxTimeout`
(`pg_hanging_query_test.go`, the required manifestOpTimeout-under-real-contention regression test
spec-oracle-506's review called for) — see SPEC-COLMANIFEST-6.

**Back-refs:** `internal/modules/colhashmanifest/pg_store.go:PgStore,NewPgStore,Get,Put,
ApplySchema`, `internal/modules/colhashmanifest/schema.sql`,
`column_manifest.go:NewPgColumnManifestStore,ApplyColumnManifestSchema` (root, first-ever
colhashmanifest root re-export). Tests: `pg_store_test.go`, `pg_blob_differential_test.go`,
`pg_hanging_query_test.go`. See SPEC-COLMANIFEST-6. Issue #506.
