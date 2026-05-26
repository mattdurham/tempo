# SPECS: tieredcache

## SPEC-TC-001: Section-Typed Dispatch

`TypedTieredCache` routes each cache operation to exactly one of 7 dedicated sub-caches
based on the called `sectioncache.SectionCache` method (no key parsing required at runtime).
The mapping:

| Method(s) | SectionType | Sub-cache field |
|---|---|---|
| `GetOrFetchFooter`, `GetOrFetchHeader` | `SectionTypeFooter` | `footer` |
| `GetOrFetchV8TOC`, `GetOrFetchV8Section` | `SectionTypeTOC` | `toc` |
| `GetOrFetchBloom` | `SectionTypeBloom` | `bloom` |
| `GetOrFetchMetadata`, `GetOrFetchV14Section`, `GetV14Section` | `SectionTypeMetadata` | `metadata` |
| `GetOrFetchTraceIndex` | `SectionTypeTraceIdx` | `traceIdx` |
| `GetBlockColumns`, `CacheBlockColumns` | `SectionTypeBlockData` | `block` |
| `GetOrFetchIntrinsic` | `SectionTypeIntrinsic` | `intrinsic` |

**SPEC-TC-001 (legacy, binary router):** The original `TieredCache` uses `isBlockDataKey` for
binary routing: block data keys → `dataCache`; all other keys → `metadataCache`. This
behavior is preserved unchanged for `TieredCache`. `TypedTieredCache` supersedes it with
N-way typed dispatch.

Back-ref: `internal/modules/tieredcache/typed.go:TypedTieredCache`,
`internal/modules/tieredcache/tieredcache.go:isBlockDataKey`

## SPEC-TC-002: Routing Contract

- Each `sectioncache.SectionCache` method maps to exactly one sub-cache.
- No operation routes to two sub-caches simultaneously.
- `TieredCache` (binary): block keys → `dataCache`; all others → `metadataCache`.
- `TypedTieredCache` (N-way typed dispatch): 7 dedicated sub-caches, one per section type.

Back-ref: `internal/modules/tieredcache/typed.go:TypedTieredCache`

## SPEC-TC-003: Nil Receiver Safety

`(*TieredCache).Close()` and `(*TypedTieredCache).Close()` are safe to call on nil
receivers and return nil.
All other methods require a non-nil receiver and non-nil sub-caches. Use `filecache.NopCache`
for a disabled tier. `NewTypedTieredCache` normalizes nil TypedConfig fields to `NopCache`.

## SPEC-TC-004: Shared Tier Close Safety

If the same cache instance is passed to multiple TypedConfig fields (e.g.
`DefaultTypedConfig(mem, disk)` assigns the same `mem` to 5 fields), `TypedTieredCache.Close`
deduplicates by pointer: each unique sub-cache instance is closed exactly once.
Both `MemoryCache.Close()` and `FileCache.Close()` are documented no-ops; the deduplication
is a belt-and-suspenders safety measure.

Back-ref: `internal/modules/tieredcache/typed.go:TypedTieredCache.Close`

## SPEC-TC-005: Concurrent Safety

`TieredCache` and `TypedTieredCache` are safe for concurrent use. Thread safety is fully
delegated to the sub-caches; neither implementation holds mutable state after construction.

## SPEC-TC-006: TypedTieredCache Implements sectioncache.SectionCache Only

`TypedTieredCache` implements `sectioncache.SectionCache` (direct typed routing, no key
parsing). Pass it as `Options.Cache` in reader options; the reader uses it directly.

The `filecache.Cache` compatibility methods have been removed. Callers holding a
`TypedTieredCache` as a `filecache.Cache` are no longer supported — migrate to
`sectioncache.SectionCache` directly.

Back-ref: `internal/modules/tieredcache/typed.go:TypedTieredCache`

## SPEC-TC-007: SectionCache Interface Reference

`TypedTieredCache` satisfies `sectioncache.SectionCache` (one method per section type).
Compile-time assertion: `var _ sectioncache.SectionCache = (*TypedTieredCache)(nil)` in
`typed_test.go`.

Back-ref: `internal/modules/sectioncache/sectioncache.go:SectionCache`

## SPEC-TC-008: TypedTieredCache Metrics Contract

When `TypedConfig.Registerer` is non-nil, `NewTypedTieredCache` registers:

- `blockpack_typed_cache_requests_total` (CounterVec, labels: `section`, `result`)
  — incremented on every GetOrFetch* / Get* call. `result` ∈ {"hit", "miss", "error"}.
- `blockpack_typed_cache_fetch_duration_seconds` (native HistogramVec, labels: `section`, `result`)
  — observes wall-clock duration from method entry to return.
  `NativeHistogramBucketFactor=1.1`, `NativeHistogramMaxBucketNumber=100`.

`section` label values: "footer", "toc", "bloom", "metadata", "traceIdx", "block", "intrinsic".

When `Registerer` is nil, no metrics are registered and all observe calls are no-ops.
Passing the same non-nil `Registerer` to multiple `NewTypedTieredCache` calls is safe:
`AlreadyRegisteredError` is handled by returning the previously registered collector.

`CacheBlockColumns` (Put-only) is not instrumented. Only Get/GetOrFetch operations are metered.

All label combinations are pre-resolved into `sectionCounters [numSections][3]prometheus.Counter`
and `sectionObs [numSections][3]prometheus.Observer` at construction time. The hot path
(observeSection) uses pre-resolved values only — no `WithLabelValues` map lookup at
call time, 0 additional heap allocations from the metric path.

Back-ref: `internal/modules/tieredcache/typed.go:NewTypedTieredCache`
