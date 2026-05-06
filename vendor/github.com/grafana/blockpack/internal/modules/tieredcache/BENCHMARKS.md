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

## BENCH-TC-003: TypedTieredCache Method Dispatch Overhead

**Target:** 0 additional allocs/op from metric instrumentation over baseline. Baseline
has 1 alloc/op from `BlockColumnsKeyFast` string escaping through `filecache.Cache`
interface dispatch (pre-existing, not introduced by metrics). Wall-clock overhead for
metric path < 20 ns/op (pre-resolved Observer.Observe + Counter.Inc, atomic ops only).

**Implemented:** `BenchmarkTypedTieredCache_GetBlockColumns_HotPath_ZeroAlloc`
in `internal/modules/tieredcache/typed_test.go`.

**Notes:**
- First 1000 iterations warm up native histogram bucket array (initial allocs expected).
- After warmup, `Observe()` on a pre-resolved `prometheus.Observer` is 0-alloc
  (atomic float64 operations only; no map lookup).
- `time.Now()` / `time.Since()` are stack-only operations (no heap alloc).
- Pre-resolved `sectionCounters [numSections][3]prometheus.Counter` means `Counter.Inc()`
  is also 0-alloc (direct atomic increment).
- Companion deterministic test: `TestTypedTieredCache_GetBlockColumns_HotPath_ZeroAllocs`
  (uses `testing.AllocsPerRun` to compare with vs without Registerer).

**Observed (Apple M1 Max):** ~135–145 ns/op, 24 B/op, 1 allocs/op (string key only).
