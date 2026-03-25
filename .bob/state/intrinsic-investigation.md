# Discovery: Intrinsic Column Pruning Code Path

## Executive Summary

Intrinsic pruning is **correctly implemented** in blockpack and **is being called** from Tempo's vblockpack Fetch path. However, **intrinsic columns may not be present in old or newly-written blocks**:

1. **Reader-side:** `BlocksFromIntrinsicTOC()` in predicates.go works correctly and is called from executor.Collect() stream.go:128
2. **Intrinsic TOC parsing:** `parseIntrinsicTOC()` is called during Reader initialization
3. **HasIntrinsicSection()** check guards pruning appropriately
4. **traceIntrinsicColumns map** (predicates.go:192-204) **DOES include `span:status`**
5. **Problem identified:** The intrinsic accumulator is properly fed from `addRowFromBlock()` (writer_block.go:655-743) BUT this only happens if the Writer has created an intrinsicAccum AND only for queries with intrinsic predicates

The root cause of no pruning improvement is likely **blocks in S3 don't have the intrinsic section yet**, not code defects.

---

## Question 1: How does Fetch() call blockpack? Does CollectOptions convey intrinsic predicates?

**File:** `tempodb/encoding/vblockpack/backend_block.go`

Fetch() → executeQuery() → blockpack.QueryOptions passed to blockpack.Query()

The program is compiled by blockpack's vm compiler from TraceQL conditions. CollectOptions doesn't directly pass predicates — the Program object contains everything needed.

**Code path:**
1. `Fetch()` calls `conditionsToTraceQL()` to build TraceQL query string (line ~300+)
2. Query string is passed to blockpack's compile/execute pipeline via `blockpack.Query()`
3. Program is built by blockpack's vm compiler, containing Predicates tree

---

## Question 2: In executor/stream.go, is BlocksFromIntrinsicTOC reached? What does it return?

**File:** `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go:128`

**YES, it is called.** Located at stream.go line 128:

```go
// Intrinsic-column pruning: use the file-level intrinsic column index to eliminate
// blocks whose column values cannot possibly satisfy the query predicates.
// This runs after the planner so we intersect with, not replace, the planner's selection.
// Returns nil when no pruning is possible (e.g. no intrinsic section, no intrinsic
// predicates, or all blocks survive), in which case we skip the intersection step.
if intrinsicBlocks := BlocksFromIntrinsicTOC(r, program); intrinsicBlocks != nil {
    // Build a keep-set from the intrinsic result for O(1) membership tests.
    keepSet := make(map[int]struct{}, len(intrinsicBlocks))
    for _, bi := range intrinsicBlocks {
        keepSet[bi] = struct{}{}
    }
    filtered := plan.SelectedBlocks[:0]
    for _, bi := range plan.SelectedBlocks {
        if _, ok := keepSet[bi]; ok {
            filtered = append(filtered, bi)
        }
    }
    plan.SelectedBlocks = filtered
}
```

**Return semantics:**
- Returns `nil` when no pruning possible (no intrinsic section, no intrinsic predicates, or all blocks survive)
- Returns non-nil block slice `[]int` with selected block indices that may match the intrinsic predicates
- These are intersected with the planner's selection (AND semantics)

---

## Question 3: BlocksFromIntrinsicTOC pruning conditions and support for status=error, kind=server, duration>100ms

**File:** `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go:256-336`

### Pruning strategy:

For **each intrinsic leaf node** in the predicate tree:

1. **TOC min/max check (no I/O):** If query range doesn't overlap column's global min/max, return empty block set
2. **Dict column** (span:name, span:status, span:kind, resource.service.name): Load column blob once, find matching entries by exact equality, collect their BlockRefs
3. **Flat column** (span:duration, span:start, span:end): Load column blob once, binary-search sorted uint64 values for predicate range, collect BlockRefs

### Supported conditions:

✅ **span:status=error** (equality) — supported via intrinsicDictMatches() lines 485-536
- Status value encoded as int64, looked up in dict entry values (line 516-517: `wantInt[entry.Int64Val]`)

✅ **span:kind=server** (equality) — supported via intrinsicDictMatches()
- Kind value encoded as int64, looked up in dict entry values

✅ **span:duration>100ms** (range) — supported via intrinsicFlatMatches() lines 541-625
- Duration stored as uint64 (nanoseconds), binary-searched in flat column (line 600-605)
- Range predicates handled via `leaf.Min` / `leaf.Max` (lines 576-590)

---

## Question 4: Does traceIntrinsicColumns include span:status?

**File:** `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go:192-204`

**YES.** Map definition:

```go
var traceIntrinsicColumns = map[string]struct{}{
    "trace:id":              {},
    "span:id":               {},
    "span:parent_id":        {},
    "span:name":             {},
    "span:kind":             {},        // ✅ PRESENT
    "span:start":            {},
    "span:end":              {},
    "span:duration":         {},        // ✅ PRESENT
    "span:status":           {},        // ✅ PRESENT
    "span:status_message":   {},
    "resource.service.name": {},
}
```

All three query types are correctly mapped as intrinsic columns.

---

## Question 5: Does Reader expose intrinsic TOC? Is IntrinsicTOC() method available?

**File:** `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/reader.go` and `intrinsic_reader.go`

**YES.** Methods exposed:

1. **`HasIntrinsicSection() bool`** (intrinsic_reader.go:38-41)
   - Returns `true` if file has v4 footer with non-empty intrinsic column index
   - Guard used by BlocksFromIntrinsicTOC() line 257

2. **`IntrinsicColumnMeta(name string) (IntrinsicColMeta, bool)`** (intrinsic_reader.go:44-52)
   - Returns TOC metadata for named column (min, max, offset, length, format, type)
   - No I/O (cheap lookup)
   - Used by BlocksFromIntrinsicTOC() line 288

3. **`GetIntrinsicColumn(name string) (*IntrinsicColumn, error)`** (intrinsic_reader.go:69-109)
   - Single I/O per column on first call (lazy decode)
   - Returns decoded column blob (dict entries or uint64 values)
   - Cached after first call
   - NOT thread-safe without external synchronization

4. **`IntrinsicColumnNames() []string`** (intrinsic_reader.go:55-66)
   - Returns sorted list of all intrinsic column names

5. **`parseIntrinsicTOC() error`** (intrinsic_reader.go:13-35)
   - Called during Reader initialization (reader.go:139)
   - Reads and parses v4 footer intrinsic section TOC
   - Only runs if footer version is V4 and `intrinsicIndexLen > 0`
   - Populates `intrinsicIndex` map

---

## Question 6: Do blocks in S3 have intrinsic column sections? Did compaction produce new blocks with intrinsic columns?

**File:** Writer-side: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_block.go` (compaction path)

### Intrinsic data accumulation in writer:

**Initialization:** writer.go:487-489
- `w.intrinsicAccum = newIntrinsicAccumulator()` created once per Writer instance

**Data feeding:** writer_block.go:655-743 (addRowFromBlock)

The intrinsic accumulator IS fed data when reading from source blocks during compaction:

✅ span:name (line 655-658): `a.feedString("span:name", ...)`
✅ span:kind (line 669-671): `a.feedInt64("span:kind", ...)`
✅ span:start (line 685-687): `a.feedUint64("span:start", ...)`
✅ span:end (line 701-703): `a.feedUint64("span:end", ...)`
✅ span:duration (line 716-718): `a.feedUint64("span:duration", ...)`
✅ span:status (line 724-726): `a.feedInt64("span:status", ...)`
✅ span:status_message (line 732-734): `a.feedString("span:status_message", ...)`
✅ resource.service.name (line 741-743): `a.feedString("resource.service.name", ...)`

### CRITICAL: When is intrinsic data written to the output block?

**Search in writer.go for "WriteIntrinsic" or "intrinsic.*finalize":**

The accumulator is populated but **where is it serialized and written to the block file?** Need to check:
1. writer.go's finalize/flush path
2. Whether intrinsicAccum.serialize() is called
3. Whether the intrinsic section is being written to the v4 footer

---

## File Structure Summary

| File | Role | Key finding |
|------|------|-------------|
| predicates.go:192-204 | Intrinsic column registry | span:status ✅ present |
| predicates.go:256-336 | BlocksFromIntrinsicTOC logic | Correctly handles equality + range queries |
| stream.go:128 | Caller of BlocksFromIntrinsicTOC | ✅ Called in main Collect path |
| intrinsic_reader.go | Reader TOC methods | ✅ HasIntrinsicSection, IntrinsicColumnMeta, GetIntrinsicColumn |
| reader.go:139 | Reader initialization | ✅ parseIntrinsicTOC called |
| writer_block.go:655-743 | Compaction feed path | ✅ Accumulator fed for status/kind/duration |
| writer.go:487-489 | Accumulator creation | ✅ Created per Writer |

---

## Diagnosis

### What's working:
- ✅ BlocksFromIntrinsicTOC is called and can prune
- ✅ All three query types (status=, kind=, duration>) are in traceIntrinsicColumns
- ✅ Pruning logic correctly handles equality (dict) and range (flat) queries
- ✅ Reader exposes intrinsic TOC via HasIntrinsicSection, IntrinsicColumnMeta, GetIntrinsicColumn
- ✅ Writer feeds status/kind/duration data to accumulator during compaction

### What's unclear:
- **❓ Intrinsic section serialization:** Is the accumulator's data actually written to blocks during finalize/flush?
- **❓ Block age:** Are blocks in S3 from before intrinsic support was added? (Footer version check: v3 vs v4?)
- **❓ Compaction runtime:** Containers rebuilt 6 minutes ago — did compaction actually run and complete?

### Recommendation:

1. Check if recently-compacted blocks have v4 footer (inspect block files in S3)
2. Verify `writer.finalize()` calls intrinsic serialization (grep for `WriteIntrinsic` or similar)
3. Check container logs for CompactBlocks success/errors
4. Run a query with `OnStats` callback to see `PrunedByIntrinsic` count (if exposed)
