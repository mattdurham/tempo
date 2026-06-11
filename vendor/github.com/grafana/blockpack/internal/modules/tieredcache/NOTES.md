# NOTES: tieredcache

## NOTE-TC-001: New Package vs. Extending chaincache

`chaincache` implements depth-wise tiering: fastest-first ordering for the same key space.
`tieredcache` implements width-wise routing: key-space partitioning across two independent
sub-caches. These are orthogonal composition axes. Mixing both concerns in one package would
blur the responsibility boundary and make each harder to reason about independently.

## NOTE-TC-002: isBlockDataKey Uses Byte-Scan Not Regexp

Regexp compilation (even cached via `sync.Once`) involves a mutex under high concurrency.
The block key pattern (`/block/` followed by decimal digits) is simple enough that a byte-scan
loop is exact, zero-alloc, and O(len(key)) with no lock contention. For keys ≤ 200 bytes
(realistic for any blockpack key), this is strictly cheaper than regexp at scale.

## NOTE-TC-003: strings.LastIndex for /block/ Segment

`strings.LastIndex` (not `strings.Contains` or `strings.Index`) is used to find the `/block/`
segment. This ensures that a fileID containing `/block/` as an internal path component is not
misclassified. Only the trailing segment determines routing. For example:
- `"s3://bucket/block/data/footer"` — last `/block/` tail is `"data/footer"` (not digits → false)
- `"s3://bucket/key/block/42"` — last `/block/` tail is `"42"` (all digits → true)

## NOTE-TC-004: Section Routing via SectionCache Methods (not key classification)
*Updated: 2026-05-06*

TypedTieredCache implements sectioncache.SectionCache via typed method dispatch — there is
no string key parsing at query time. Each method (GetOrFetchFooter, GetOrFetchBloom, etc.)
routes directly to the correct sub-cache. This replaces the old string-based classifyCacheKey
approach that was present in the binary TieredCache and is no longer used.

Back-ref: typed.go

## NOTE-TC-005: TieredCache Preserved for Backward Compat; TypedTieredCache is Preferred

**Date:** 2026-05-05

`TieredCache` (binary router) is exported as `blockpack.TieredCache` via the public API
(`api.go`). Removing or renaming it would be a breaking API change. It is preserved unchanged.

`TypedTieredCache` is additive — it does not replace `TieredCache`; it adds a richer N-way
router alongside it. Migration is opt-in:
- Old: `tieredcache.New(meta, data)`
- New: `tieredcache.NewTypedTieredCache(tieredcache.DefaultTypedConfig(mem, disk))`

Callers that pass `TieredCache` via `Options.Cache` continue to get the binary routing path
wrapped in `sectioncache.FilecacheAdapter` (no regression from prior behavior).

Back-ref: `internal/modules/tieredcache/typed.go:DefaultTypedConfig`

## NOTE-TC-006: Hybrid Typed-Section Routing — Why Two Interfaces on TypedTieredCache

**Date:** 2026-05-05

`TypedTieredCache` implements both `sectioncache.SectionCache` and `filecache.Cache`.

**Rationale:**
- `SectionCache` enables direct typed routing (no key parsing) when the reader detects it via
  type assertion on `opts.Cache`. This is the primary performance path.
**Decision:** `TypedTieredCache` implements `sectioncache.SectionCache` only. The `filecache.Cache`
compat methods were removed; direct typed routing via `sectioncache.SectionCache` is the only path.

**Alternative rejected:** Adding `Options.SectionCache` field to reader options would expose an
internal interface in the public API, which violates the minimal public API contract.

Back-ref: `internal/modules/tieredcache/typed.go:TypedTieredCache`

## NOTE-TC-007: bloom Filter Sub-Cache Budget

**Date:** 2026-05-05

Bloom filters can reach 15 MiB per file. The `Bloom` sub-cache in `TypedTieredCache` must
have a budget of at least `2 × maxBloomSize × maxConcurrentFiles` to avoid evicting active
bloom entries. At 15 MiB max and 20 concurrent files, that's 600 MiB — which may exceed
typical container memory budgets.

Operators with large bloom filters should provide a dedicated, larger `MemoryCache` for bloom
via `TypedConfig{Bloom: bigMemCache, ...}` rather than relying on `DefaultTypedConfig`.

Back-ref: `internal/modules/tieredcache/typed.go:DefaultTypedConfig`

## NOTE-179: GetMultiV8Section / PutV8Section batched per-column fetch

`GetMultiV8Section` batch-fetches V8 per-column blobs that share one (tocType, subType) routing,
returning a map keyed by the input names and reporting `false` when the routed sub-cache does not
support batch fetch (so the caller falls back to per-name `GetOrFetchV8Section`). `PutV8Section`
writes back the names that missed the batch under the same key scheme. Together they let the
block-read path issue one pipelined memcache request per block instead of one connection-dialing
Get per column (~28% of querier CPU in `gomemcache.(*Client).dial`).

Back-ref: `internal/modules/tieredcache/typed.go:GetMultiV8Section`

## NOTE-188: Index-Aligned Batch Re-Keying — No Reverse-Lookup Map
*Added: 2026-06-11*

**Problem:** `GetMultiV8Section` and `GetMultiV8SectionMixed` each built a reverse-lookup
map (`keyToName` / `keyToReq`: full-cache-key -> input name/V8SectionKey) purely to re-key
the `GetMulti` result back from full cache-key strings to the caller's input identifiers.
These methods run once per block per query on the warm read path (NOTE-185/179: query-frontend
shards one block per querier call, so a heavy metrics query over hundreds of blocks calls them
hundreds of times), so the per-batch map allocation + hashing is pure GC pressure on the
universal hot path.

**Fix:** the `keys` slice is already built index-aligned with the inputs (`keys[i]` is the full
cache key for `names[i]` / `reqs[i]`). So instead of populating a reverse map and iterating the
result map, walk the inputs and probe `hits[keys[i]]` directly. The output map is byte-identical
(same name/key -> blob entries) but one map allocation per batch is eliminated and the result is
built with a single ordered pass over the (small) input slice rather than a range over the result
map plus a map lookup.

**Correctness:** `keys[i]` is computed from input `i` in the same loop, so the (key, input) pairing
is exact; a missing key simply isn't in `hits` and is skipped, identical to the old "found" guard.
No behavior change for hits, misses, batch-unsupported (nil,false,nil), or error paths.

**Verification:** `go build ./...` clean; tieredcache + reader suites green under `-race`.

Back-ref: `internal/modules/tieredcache/typed.go:GetMultiV8Section`,
`internal/modules/tieredcache/typed.go:GetMultiV8SectionMixed`
