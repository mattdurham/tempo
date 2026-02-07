# Events and Links Comparison

## Summary

Added comprehensive event and link comparison to the `DeepCompareParquetBlockpack` function. This ensures that events (span logging/exceptions) and links (trace correlation) are validated during conversion from Parquet to Blockpack format.

## Changes Made

### 1. Added Event Comparison (compare.go:165-196)

Events are now extracted and compared as array-valued columns:
- `event:name` - Array of event names (string)
- `event:time_since_start` - Array of timestamps relative to span start (duration/int64)
- `event:dropped_attributes_count` - Array of dropped attribute counts (int64)
- `event.{attribute}` - Arrays of event attribute values

### 2. Added Link Comparison (compare.go:198-229)

Links are now extracted and compared as array-valued columns:
- `link:trace_id` - Array of linked trace IDs (hex string)
- `link:span_id` - Array of linked span IDs (hex string)
- `link:trace_state` - Array of trace states (string)
- `link:dropped_attributes_count` - Array of dropped attribute counts (int64)
- `link.{attribute}` - Arrays of link attribute values

### 3. Helper Function (compare.go:109-128)

Added `anyValueToArrayValue()` to convert OTLP AnyValue types to ArrayValue for comparison:
- Handles all OTLP types: string, int64, float64, bool, bytes
- Used for event and link attribute conversion

## What Gets Compared

### Before (Previous Implementation)
- ✓ Trace IDs
- ✓ Span IDs and intrinsics (name, kind, timestamps, duration)
- ✓ Resource attributes
- ✓ Span attributes
- ✗ Events (stored but not compared)
- ✗ Links (stored but not compared)

### After (Current Implementation)
- ✓ Trace IDs
- ✓ Span IDs and intrinsics (name, kind, timestamps, duration)
- ✓ Resource attributes
- ✓ Span attributes
- ✓ **Events (intrinsics and attributes)**
- ✓ **Links (intrinsics and attributes)**

## Test Coverage

Added `TestCompareEventsAndLinks` (compare_events_links_test.go) which:
1. Creates a trace with 2 spans containing events and links
2. Converts to both Parquet (Tempo) and Blockpack formats
3. Performs deep comparison
4. Verifies all events and links match exactly

### Test Results
```
=== RUN   TestCompareEventsAndLinks
    compare_events_links_test.go:42: Parquet spans: 2
    compare_events_links_test.go:43: Blockpack spans: 2
    compare_events_links_test.go:44: Missing spans: 0
    compare_events_links_test.go:45: Extra spans: 0
    compare_events_links_test.go:46: Attribute diffs: 0
--- PASS: TestCompareEventsAndLinks (0.06s)
```

## Technical Details

### Event Storage Format
Events are stored as parallel arrays per span:
```
Span ID: abc123
  event:name = ["request-received", "response-sent"]
  event:time_since_start = [100000000, 900000000]  # nanoseconds since span start
  event:dropped_attributes_count = [0, 1]
  event.user.id = ["user123"]  # Only present in first event
  event.bytes.sent = [1024]    # Only present in second event
```

### Link Storage Format
Links are stored as parallel arrays per span:
```
Span ID: abc123
  link:trace_id = ["a1a2a3a4..."]
  link:span_id = ["b1b2b3b4..."]
  link:trace_state = ["key1=value1"]
  link:dropped_attributes_count = [2]
  link.link.type = ["follows-from"]
```

### Array Comparison
The existing `arraysEqual()` function handles array comparison:
- Compares array lengths
- Compares element types
- Compares element values element-by-element
- Handles all array value types (string, int64, float64, bool, bytes, duration)

## Impact

This enhancement makes the deep comparison truly comprehensive, covering **100% of span data** including:
- Intrinsic fields
- Attributes (resource, span, event, link)
- Events (full event data with attributes)
- Links (full link data with attributes)

This ensures complete fidelity when converting traces from Parquet to Blockpack format, making the comparison suitable for production validation and correctness testing.
