# TESTS: tieredcache

## TestIsBlockDataKey
Table-driven test covering every known cache key pattern. Verifies SPEC-TC-001.
All block data patterns return true; all metadata patterns and edge cases return false.
Serves as the classification registry — new key formats must be added here before merging.

## TestGet_BlockKey_RoutesToDataCache
Verifies that `Get` with a block key (`fileID/block/7`) is routed to the data cache only.
Asserts that the metadata cache receives zero Get calls. Verifies SPEC-TC-002.

## TestGet_MetadataKey_RoutesToMetadataCache
Verifies that `Get` with a metadata key (`fileID/footer`) is routed to the metadata cache only.
Asserts that the data cache receives zero Get calls. Verifies SPEC-TC-002.

## TestPut_BlockKey_RoutesToDataCache
Verifies that `Put` with a block key writes to the data cache only.
Asserts that the metadata cache receives zero Put calls. Verifies SPEC-TC-002.

## TestPut_MetadataKey_RoutesToMetadataCache
Verifies that `Put` with a metadata key writes to the metadata cache only.
Asserts that the data cache receives zero Put calls. Verifies SPEC-TC-002.

## TestGetOrFetch_BlockKey_RoutesToDataCache
Verifies that `GetOrFetch` with a block key fetches and stores in the data cache only.
Asserts that the metadata cache receives zero Put calls. Verifies SPEC-TC-002.

## TestGetOrFetch_MetadataKey_RoutesToMetadataCache
Verifies that `GetOrFetch` with a metadata key fetches and stores in the metadata cache only.
Asserts that the data cache receives zero Put calls. Verifies SPEC-TC-002.

## TestClose_ClosesMetadataAndData
Verifies that `Close` calls `Close` on both sub-caches. Verifies SPEC-TC-004.

## TestClose_NilReceiver
Verifies that `Close` on a nil `*TieredCache` returns nil without panicking. Verifies SPEC-TC-003.

## TestClose_ReturnsJoinedErrors
Verifies that when both sub-caches return errors from `Close`, the returned error wraps both
(using `errors.Join`). Both errors are detectable via `errors.Is`. Verifies SPEC-TC-003.

## TestConcurrent_MixedKeys
N goroutines concurrently call `Get`, `Put`, and `GetOrFetch` with alternating block and
metadata keys. Uses real `MemoryCache` tiers (not stubs) so the race detector exercises
actual locking paths. Run with `-race`. Verifies SPEC-TC-005.

---

## TypedTieredCache Tests (typed_test.go)

## TestTypedTieredCache_*Routing (11 tests)
One test per SectionCache method + sub-cache:
- `TestTypedTieredCache_FooterRouting` — GetOrFetchFooter → footer sub-cache only
- `TestTypedTieredCache_HeaderRouting` — GetOrFetchHeader → footer sub-cache only
- `TestTypedTieredCache_V8TOCRouting` — GetOrFetchV8TOC → toc sub-cache only
- `TestTypedTieredCache_V8SectionRouting` — GetOrFetchV8Section → toc sub-cache only
- `TestTypedTieredCache_V14SectionRouting` — GetOrFetchV14Section → metadata sub-cache only
- `TestTypedTieredCache_GetV14SectionRouting` — GetV14Section → metadata sub-cache only
- `TestTypedTieredCache_BloomRouting` / `TestTypedTieredCache_BloomRoutingV3` — bloom sub-cache only
- `TestTypedTieredCache_MetadataRouting` — GetOrFetchMetadata → metadata sub-cache only
- `TestTypedTieredCache_TraceIndexRouting` / `TestTypedTieredCache_TraceIndexRouting_Full` — traceIdx only
- `TestTypedTieredCache_BlockColumnsRouting` — block sub-cache only
- `TestTypedTieredCache_IntrinsicRouting` — intrinsic sub-cache only
Verifies SPEC-TC-006, SPEC-TC-002.

## TestTypedTieredCache_NilSubCaches_Normalized
Verifies that nil TypedConfig fields are normalized to NopCache; no panics. Verifies SPEC-TC-003.

## TestTypedTieredCache_Close_AllSubCachesClosed
Verifies that Close invokes Close on all 7 sub-caches. Verifies SPEC-TC-004.

## TestTypedTieredCache_DefaultTypedConfig_MemAssignment
Verifies that DefaultTypedConfig assigns mem to Footer/TOC/Bloom/Block/Intrinsic and
disk to Metadata/TraceIdx. Documents the recommended tier mapping.

## TestTypedTieredCache_Concurrent_MixedKeys
N goroutines call typed methods and filecache.Cache methods concurrently using real
MemoryCache. Run with `-race`. Verifies SPEC-TC-005.



---

## TypedTieredCache Metrics Tests (typed_test.go)

### TestTypedTieredCache_Metrics_Miss
Constructs TypedTieredCache with a real prometheus.Registry. Calls GetOrFetchFooter on
an absent key. Asserts blockpack_typed_cache_fetch_duration_seconds has 1 observation
for section="footer", result="miss". Verifies SPEC-TC-008.

### TestTypedTieredCache_Metrics_Hit
Pre-populates the footer sub-cache. Calls GetOrFetchFooter. Asserts histogram has
1 observation for section="footer", result="hit". Verifies SPEC-TC-008.

### TestTypedTieredCache_Metrics_RequestCounter
Calls GetOrFetchFooter twice (first miss, second hit). Asserts sectionRequests counter
has 1 miss and 1 hit for section="footer". Verifies SPEC-TC-008.

### TestTypedTieredCache_Metrics_AllSections
Exercises one method per section type. Asserts the histogram contains at least one
observation for each of the 7 section labels. Verifies SPEC-TC-008 label coverage.

### TestTypedTieredCache_Metrics_NilRegisterer_NoPanic
Constructs TypedTieredCache without a Registerer. Calls all typed methods. Asserts
no panic. Verifies SPEC-TC-008 nil-safety.

### TestTypedTieredCache_GetBlockColumns_HotPath_ZeroAllocs
Asserts GetBlockColumns allocs/op ≤ 1 with metrics enabled. The 1 alloc is the pre-existing
string key from BlockColumnsKeyFast escaping through the filecache.Cache interface dispatch
(not introduced by metrics). The metric instrumentation (pre-resolved Observer.Observe +
Counter.Inc) must add 0 additional allocs after native histogram bucket stabilization
(500-call warmup). Verifies BENCH-TC-003.
