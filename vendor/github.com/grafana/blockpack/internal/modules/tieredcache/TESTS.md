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
