# SPECS: tieredcache

## SPEC-TC-001: Key Classification

Block data keys match the pattern `*/block/<decimal-integer>` where:
- `/block/` is the final `/block/` segment in the key (using `strings.LastIndex`)
- `<decimal-integer>` is one or more ASCII decimal digits (`0`–`9`) with no other characters

All other keys are metadata keys. This includes footer, header, metadata, section,
trace-index, and any V8 null-byte-separated column keys.

The block data key format is generated in exactly one place: `internal/modules/blockio/reader/reader.go:ReadGroup`
(the `fileID+"/block/"+strconv.Itoa(blockIdx)` pattern, SPEC-011). Any new block-data
key format added to the reader or parser MUST be added to `TestIsBlockDataKey` before merging.

## SPEC-TC-002: Routing Contract

- Block data keys (`isBlockDataKey` returns true) route exclusively to `dataCache`.
- All other keys route exclusively to `metadataCache`.
- No key routes to both caches simultaneously.

## SPEC-TC-003: Nil Receiver Safety

`(*TieredCache).Close()` is safe to call on a nil receiver and returns nil.
All other methods (`Get`, `Put`, `GetOrFetch`) require a non-nil receiver and non-nil
sub-caches. Use `filecache.NopCache` for a disabled tier.

## SPEC-TC-004: Shared Tier Close Safety

If the same `MemoryCache` or `FileCache` instance is passed to both sub-chains, calling
`TieredCache.Close()` may invoke `Close()` on those instances twice. This is safe because
both `MemoryCache.Close()` and `FileCache.Close()` are documented no-ops.

`MemCache.Close()` closes TCP connections. `OpenMemCache` always creates an independent
`*gomemcache.Client` per call; separate `MemCache` instances are recommended for metadata
and data to avoid shared TCP connection teardown.

## SPEC-TC-005: Concurrent Safety

`TieredCache` is safe for concurrent use. Thread safety is fully delegated to the
sub-caches; `TieredCache` itself holds no mutable state after construction.
