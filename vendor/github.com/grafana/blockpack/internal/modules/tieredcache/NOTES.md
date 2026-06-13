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

## NOTE-189: Fast V8 Section Key Build — strconv Concatenation, No fmt.Sprintf
*Added: 2026-06-11*

**Problem:** the four V8-section cache key builds in `TypedTieredCache`
(`GetOrFetchV8Section`, `GetMultiV8Section`, `GetMultiV8SectionMixed`, `PutV8Section`)
all used `fmt.Sprintf("%s\x00v8\x00%d\x00%d\x00%s", fileID, tocType, subType, name)`.
The two batch methods build one such key per wanted column, and the batch path itself
runs once per block per query on the warm read path (NOTE-185/179: query-frontend shards
one block per querier call, so a heavy metrics query over hundreds of blocks builds tens
of thousands of these keys). `fmt.Sprintf` boxes the two `uint32` args into `interface{}`,
runs the format-verb scanner via reflection, and copies its internal `[]byte` to a string —
all avoidable for a fixed concatenation.

**Fix:** route all four builds through `sectioncache.V8SectionKeyFast`, which uses
`strconv.Itoa` + a pre-sized `strings.Builder` to produce a byte-identical key in one
allocation with no reflection or interface boxing.

**Correctness:** `strconv.Itoa(int(x))` for the two `uint32` values is identical to `%d`
for all values in range (block sub-type / toc-type are small non-negative ints), and the
literal separators are byte-for-byte the same. Output is byte-identical to the prior
`fmt.Sprintf`, so cache keys are unchanged and warm hits are preserved.

**Verification:** `go build ./...` clean; tieredcache + reader + sectioncache + executor
suites green under `-race`.

Back-ref: `internal/modules/sectioncache/keys.go:V8SectionKeyFast`,
`internal/modules/tieredcache/typed.go` (4 key builds).

## NOTE-197 — batch per-file intrinsic column fetch (GetMultiIntrinsic)

A metrics/search query touches several intrinsic columns per file (span:start + every
predicate-leaf column + each group-by column). Each was previously resolved by its own
`GetOrFetchIntrinsic`, i.e. one memcache round-trip (and under pool pressure one connection
acquisition) per column. A querier CPU profile (2026-06-11) is dominated by kernel networking
(`__nft_rbtree_lookup` ~23%, `sch_direct_xmit`/`veth_xmit`/`nf_hook_slow` tx softirq path) on
these round-trips — not by decode. `GetMultiIntrinsic` collapses the per-file intrinsic fan-out
into a single pipelined `GetMulti` (same lever NOTE-179/185 applied to V8 block columns). It
re-keys hits by input name without a reverse-lookup map (NOTE-188 pattern) and returns
`(nil,false,nil)` when the intrinsic sub-cache lacks batch support so callers fall back to
per-name fetches. Misses fall through to the existing per-name path, so the result is identical.

**Verification:** `go build ./...` clean; tieredcache + reader + executor suites green under `-race`.

Back-ref: `internal/modules/tieredcache/typed.go:GetMultiIntrinsic`,
`internal/modules/blockio/reader/intrinsic_reader.go:PrefetchIntrinsicColumns`.

---

## NOTE-337: deduplicate the V8-section batch-fetch body and PutV8Section routing

`GetMultiV8Section` (`[]string` column names) and `GetMultiV8SectionMixed` (`[]V8SectionKey`)
were ~39 lines each of structurally identical code: route by subType, bail if the tier is not a
`sectionBatchGetter`, build `keys []string`, `GetMulti`, then re-key the hits back onto the
input slice by index (NOTE-188). The only difference was the per-request identity type used to
build the key and key the result map.

Extracted `batchGetV8Section[K comparable](t, subType, reqs []K, keyFor func(K) string)` — a
generic helper parameterized by the key-builder. Both public methods now reduce to a one-line
call passing their respective `keyFor` closure. Behaviour is byte-for-byte preserved (same
routing, same index-aligned re-key, same `observeSection` accounting).

`PutV8Section` also carried a verbatim copy of `routeV8`'s subType→sub-cache switch (minus the
metric-index return). It now calls `routeV8` and discards the metric index.

**Verification:** new tests `TestPutV8Section_RoutesViaRouteV8` (all four routing classes land
in the same tier `routeV8` picks) and `TestGetMultiV8SectionMixed_BatchHitAndMiss` /
`_Empty` / `_NoBatchSupport` exercise the V8SectionKey-keyed path through the shared helper.
tieredcache suite green under `-race`; `make precommit` fully green. Pure structural
deduplication, no behaviour change. Back-ref: `internal/modules/tieredcache/typed.go:batchGetV8Section`.
