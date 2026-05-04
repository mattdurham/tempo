# BENCHMARKS: tieredcache

## BENCH-TC-001: Get Routing Overhead

**Target:** `TieredCache.Get` overhead vs. direct sub-cache call < 100 ns.

The routing path is: one `strings.LastIndex` call + a short digit scan + one virtual
dispatch. For keys ≤ 200 bytes this is O(len(key)) but practically < 50 ns at 1 GHz.

```
BenchmarkTieredCache_Get_BlockKey    N ops    ~15 ns/op
BenchmarkTieredCache_Get_MetaKey     N ops    ~10 ns/op
```

## BENCH-TC-002: isBlockDataKey Throughput

**Target:** Throughput ≥ 100M keys/sec for realistic key lengths (50–150 bytes).

The byte-scan in `isBlockDataKey` touches at most `len(key)` bytes. For a block key
like `"s3://bucket/long/path/file.bp/block/9999"` (~45 bytes) the scan terminates
after 4 digits. For metadata keys the scan terminates at the first non-digit byte.

```
BenchmarkIsBlockDataKey_BlockKey     N ops    ~8 ns/op    (short tail scan)
BenchmarkIsBlockDataKey_MetaKey      N ops    ~20 ns/op   (LastIndex scan + immediate bail)
```
