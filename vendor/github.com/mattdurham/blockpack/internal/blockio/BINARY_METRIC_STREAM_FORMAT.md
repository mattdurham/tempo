# Binary Metric Stream Format with Group Key Deduplication and Timestamp Optimization

## Overview

The binary metric stream format reduces metric stream block size by ~77.7% (from 108 KB to 23.5 KB per stream) through:
1. **Binary encoding** instead of JSON
2. **Group key deduplication** - store group keys once, reference by index
3. **Timestamp optimization** - store start_time and step_millis once, calculate bucket times

## Size Reduction Results

### Single Stream (40 services × 60 buckets = 2400 aggregates)
- **COUNT/SUM/MIN/MAX**: 108 KB → 23.93 KB → 23.47 KB (77.7% reduction with timestamp optimization)
- **AVG**: 108 KB → 45.6 KB → 45.12 KB (58.2% reduction)

### Multiple Streams (3 streams)
- **Total**: 324 KB → 73.5 KB → 72.0 KB (77.8% reduction)

## Binary Format Specification

### Stream Header
```
- stream_count (4 bytes)
For each stream:
  - stream_id_len (2 bytes)
  - stream_id (N bytes)
  - query_len (4 bytes)
  - query (N bytes)
  - step_size_nanos (8 bytes)
  - bucket_count (4 bytes)
```

### Time Range Section (NEW - Optimized)
```
- start_time (8 bytes)                # Timestamp of first bucket (nanoseconds)
- step_millis (4 bytes)               # Step size in milliseconds
```

### Group Key Section
```
- group_key_count (4 bytes)          # Number of unique GROUP BY keys
For each unique group key:
  - field_count (1 byte)              # Number of fields in GroupKey
  For each field:
    - value_type (1 byte)             # 0=string, 1=int64, 2=float64, 3=bool
    - value_len (2 bytes) [strings]   # Length for string values
    - value (N bytes)                 # Actual value
```

### Bucket Data Section
```
For each bucket (60 times):
  # Bucket time is IMPLICIT: bucket_time = start_time + (i * step_millis * 1_000_000)
  For each group key (by index order):
    - value_count (1 byte)            # Number of aggregate values present
    For each value:
      - value_type (1 byte)           # 1=Count, 2=Sum, 3=Min, 4=Max, 5=P50, 6=P95, 7=P99
      - value (8 bytes)               # Always float64/int64
```

## Key Design Decisions

### 1. Timestamp Optimization (NEW)
**Problem**: Each bucket stored its own timestamp (8 bytes), repeated 60 times.
- 60 buckets × 8 bytes = 480 bytes per stream wasted on redundant timestamps

**Solution**: Store start_time (8 bytes) and step_millis (4 bytes) once, calculate bucket times.
- Bucket time formula: `bucket_time = start_time + (i * step_millis * 1_000_000)`
- Using uint32 for step_millis supports up to ~49 days (4,294,967 seconds)
- Savings: 480 - 12 = **468 bytes per stream** (1.9% additional reduction)

This is a common optimization in time-series databases (Prometheus, VictoriaMetrics, etc.)

### 2. Group Key Deduplication
**Problem**: With JSON format, service names were repeated 60 times (once per bucket).
- Example: `"service-1"` appears in all 60 buckets = 60 × ~10 bytes = 600 bytes per service
- For 40 services: 40 × 600 = 24,000 bytes just for repeated keys

**Solution**: Store unique group keys once, reference by index.
- Store `"service-1"` once = 10 bytes
- Reference by index in each bucket = 0 bytes (implicit from ordering)
- Savings per service: ~590 bytes × 40 services = ~23.6 KB

### 3. Binary Encoding
**Benefits**:
- No JSON overhead (quotes, commas, field names)
- Fixed-width integers instead of variable-length strings
- Type information stored as 1 byte instead of JSON type inference

**Example**: COUNT aggregate for one service in one bucket
- JSON: `"service-1":{"k":["service-1"],"c":1000}` = ~40 bytes
- Binary: value_count(1) + type(1) + value(8) = 10 bytes (excluding shared group key)

### 4. Sparse Data Handling
**Design**: Each bucket writes data for ALL group keys (in index order), even if no data exists.
- If data exists: value_count > 0, followed by type+value pairs
- If no data: value_count = 0

**Alternative Considered**: Use bitmask to indicate which group keys have data.
- Would save ~1 byte per empty slot
- Adds complexity for marginal gains (most buckets have data for all services)
- Current approach is simpler and more predictable

### 5. Aggregate Value Types
Store only the values needed for each aggregate function:
- COUNT/RATE: Just Count (1 value)
- AVG: Sum + Count (2 values)
- SUM: Just Sum (1 value)
- MIN: Just Min (1 value)
- MAX: Just Max (1 value)
- QUANTILE: Specific quantile (1 value)

This selective storage further reduces size.

## Implementation Files

### Writer (`writer.go`)
- `GroupKeyIndex`: Struct for tracking unique group keys
- `BinaryAggValue`: Struct for aggregate values
- `buildGroupKeyIndex()`: Builds deduplication index
- `writeGroupKey()`: Writes group key in binary format
- `writeGroupKeyValue()`: Writes individual group key field
- `getAggregateValues()`: Extracts values based on aggregate function
- `writeBinaryAggregates()`: Main binary serialization function
- `buildMetricStreamBlocks()`: Updated to use binary format

### Reader (`reader.go`)
- `ReadMetricStreamBlocks()`: Returns offset/length of metric stream blocks
- `GetMetricStreamBlocksRaw()`: Returns raw metric stream block bytes
- Full deserialization to be implemented later

## Testing

### Test Files
1. `writer_binary_aggregate_test.go`:
   - `TestBinaryAggregateSizeReduction`: Verifies 75%+ size reduction
   - `TestBinaryAggregateMultipleStreams`: Tests multiple streams
   - `TestBinaryAggregateGroupKeyDeduplication`: Verifies deduplication logic
   - `BenchmarkBinaryAggregateSerialization`: Performance benchmark

2. `reader_metric_stream_test.go`:
   - `TestReaderMetricStreamBlocks`: Verifies reader can access metric stream blocks

### Test Results
```
TestBinaryAggregateSizeReduction/COUNT_query:  24.50 KB (77.3% reduction)
TestBinaryAggregateSizeReduction/AVG_query:    45.59 KB (57.8% reduction)
TestBinaryAggregateSizeReduction/SUM_query:    24.50 KB (77.3% reduction)
TestBinaryAggregateMultipleStreams:            73.46 KB (77.3% reduction)
```

## Benchmark Results
```
BenchmarkBinaryAggregateSerialization-24
    1330 ops          894479 ns/op        609968 B/op        22235 allocs/op
```

## Future Optimizations

### 1. Bitmask for Sparse Data
Instead of `value_count` per group key, use a bitmask:
```
- group_key_bitmask (ceil(group_key_count / 8) bytes)  # 1 bit per group key
- For each set bit: write values
```
Savings: ~1 byte per empty slot, but most buckets are dense.

### 2. ~~Delta Encoding for Bucket Times~~ (IMPLEMENTED)
~~Store first bucket time, then deltas~~
**COMPLETED**: We now store start_time + step_millis, eliminating per-bucket timestamps entirely.
Achieved savings: 468 bytes per stream (1.9% additional reduction)

### 3. Varint Encoding for Counts
Use variable-length integers for counts instead of fixed 8 bytes:
```
- Small counts (< 128): 1 byte
- Medium counts (< 16384): 2 bytes
- Large counts: more bytes as needed
```
Potential savings depend on data distribution.

### 4. Compression
Apply general-purpose compression (e.g., Snappy, LZ4) to entire metric stream block.
- Pro: Simple, effective for repetitive data
- Con: Requires decompression before access

## Version Compatibility

- **Format Version**: v9 (required - includes group key deduplication and timestamp optimization)
- **Supported Versions**: Only v9 is supported - older format versions are rejected
- **Forward Compatibility**: Readers check version and will handle future v10+ formats if introduced
- **Timestamp Optimization**: Readers must parse start_time and step_millis

## Usage Example

### Writing
```go
w := NewWriter(100)

// Add metric stream
spec := &sql.QuerySpec{
    Aggregate: sql.AggregateSpec{
        Function: "COUNT",
        GroupBy:  []string{"resource.service.name"},
    },
}

streamDef := &MetricStreamDef{
    StreamID:      "my-stream",
    Query:         "count spans by service",
    Spec:          spec,
    StepSizeNanos: 60 * time.Second.Nanoseconds(),
}

w.AddMetricStream(streamDef)

// Write spans...
w.AddTracesData(tracesData)

// Flush to bytes
fileBytes, err := w.Flush()
```

### Reading
```go
reader, err := NewReader(fileBytes)

// Get metric stream block metadata
offset, length, err := reader.ReadMetricStreamBlocks()

// Get raw bytes (for now)
aggBytes, err := reader.GetMetricStreamBlocksRaw()

// TODO: Full deserialization coming soon
```

## Performance Characteristics

### Write Performance
- Group key indexing: O(buckets × aggregates) = O(60 × 40) = O(2400)
- Binary serialization: Linear in size of output
- Memory: ~600 KB per stream for working data structures

### Read Performance (when implemented)
- Parse header: O(1)
- Parse group keys: O(unique_group_keys) = O(40)
- Parse bucket: O(group_keys × values_per_aggregate) = O(40 × 1-2)
- Total: Linear in data size, very fast

### Space Efficiency
- Per aggregate (COUNT): ~10 bytes (vs ~40 bytes JSON)
- Per stream header: ~50-100 bytes
- Per stream time range: 12 bytes (start_time + step_millis, stored once)
- Per group key: ~10-20 bytes (stored once)
- Per bucket header: 0 bytes (timestamps are now implicit!)
- Total: ~10 bytes per aggregate + small fixed overhead (468 bytes less per stream vs v2)
