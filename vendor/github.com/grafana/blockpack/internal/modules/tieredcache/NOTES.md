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

## NOTE-TC-004: Key Classification Stability

The block data key format (`fileID+"/block/"+blockIdx`) is generated in exactly one place in
the reader (`reader.go:ReadGroup`, SPEC-011). Any new block-data key format must be classified
in `tieredcache_test.go:TestIsBlockDataKey` (for TieredCache binary router) and
`typed_test.go:TestClassifyCacheKey` (for TypedTieredCache N-way router) before merging.
These tests serve as the classification registry, ensuring routing is reviewed explicitly for
every new key format. Failing to update the classifier causes the key to route to
`SectionTypeOther` (metadata fallback) — not a panic, but a mis-routing that defeats the
purpose of typed cache budgets.

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
