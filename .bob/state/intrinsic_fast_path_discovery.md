# Discovery: Intrinsic-Only Query Fast Path

## Executive Summary

The blockpack query pipeline has clear separation points for injecting an intrinsic-only fast path that bypasses full block I/O. The intrinsic columns (span:id, span:name, span:duration, span:status, span:kind, trace:id, resource.service.name) are already indexed in a separate section and can be filtered efficiently without decoding full blocks. The fast path should be added at the `executor/stream.go:Collect()` level, after block pruning but before block I/O, by detecting intrinsic-only programs and loading/filtering intrinsic column data directly.

---

## Type Definitions & Interfaces

### MatchedRow (executor/stream.go:59-63)
```go
type MatchedRow struct {
	Block    *modules_reader.Block  // parsed block containing the span
	BlockIdx int                     // block index in the file
	RowIdx   int                     // row (span) index within the block
}
```
**Purpose**: Represents a single matched span row. Returned by `Collect()` and `CollectTopK()`. The callback in the query pipeline extracts fields from these using `modulesSpanFieldsAdapter`.

**File**: `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`

---

### SpanFieldsProvider Interface (shared/provider.go:8-11)
```go
type SpanFieldsProvider interface {
	GetField(name string) (any, bool)
	IterateFields(fn func(name string, value any) bool)
}
```
**Purpose**: Lazy-loading interface for span attributes. Two implementations:
1. `modulesSpanFieldsAdapter` (blockio/span_fields.go:15-126) — reads from block columns on demand
2. `materializedSpanFields` (api.go:381-396) — heap-allocated map used by `SpanMatch.Clone()`

**Key methods**:
- `GetField(name)`: Returns typed value for named attribute (e.g., "span:duration", "resource.service.name", "service.name" with fallback)
- `IterateFields(fn)`: Calls fn for every present attribute; stops if fn returns false

**File**: `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/provider.go`

---

### SpanMatch (api.go:354-360)
```go
type SpanMatch struct {
	Fields  SpanFieldsProvider  // lazy provider for attributes
	TraceID string              // hex-encoded trace:id
	SpanID  string              // hex-encoded span:id
}
```
**Purpose**: Public API result type holding one matched span. Fields is safe to retain after query returns (via `Clone()`). For structural queries, Fields is nil.

**File**: `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/api.go`

---

## Intrinsic Column Types

### IntrinsicColumn (shared/types.go:84-97)
```go
type IntrinsicColumn struct {
	// For flat columns (IntrinsicFormatFlat):
	Uint64Values []uint64      // non-nil for ColumnTypeUint64 (span:duration, span:start, span:end)
	BytesValues  [][]byte      // non-nil for ColumnTypeBytes (trace:id, span:id)
	BlockRefs    []BlockRef    // parallel to Uint64Values / BytesValues — maps row values back to (blockIdx, rowIdx)

	// For dict columns (IntrinsicFormatDict):
	DictEntries []IntrinsicDictEntry  // one entry per distinct value

	Name   string       // column name (e.g. "span:name", "span:duration")
	Count  uint32       // total number of rows stored (present rows only)
	Type   ColumnType   // ColumnTypeUint64, ColumnTypeBytes, ColumnTypeString, ColumnTypeInt64
	Format uint8        // IntrinsicFormatFlat or IntrinsicFormatDict
}
```
**Purpose**: Decoded intrinsic column blob. The BlockRefs array is the critical link: it maps each column value back to its source (blockIdx, rowIdx) pair.

**File**: `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go`

---

### IntrinsicDictEntry (shared/types.go:99-104)
```go
type IntrinsicDictEntry struct {
	BlockRefs []BlockRef  // all (blockIdx, rowIdx) pairs with this value
	Value     string      // string representation (for Int64: decimal string)
	Int64Val  int64       // set for Int64 type
}
```
**Purpose**: One dictionary entry. Maps a single distinct value to all block references containing it.

---

### BlockRef (shared/types.go:77-82)
```go
type BlockRef struct {
	BlockIdx uint16  // block index in the file
	RowIdx   uint16  // row (span) index within the block
}
```
**Purpose**: Lightweight (blockIdx, rowIdx) pair. Used in intrinsic columns to link values back to their source spans. Limited to uint16 because typical blocks have <65K rows.

---

### IntrinsicColMeta (shared/types.go:106-119)
```go
type IntrinsicColMeta struct {
	Name   string       // column name (e.g. "span:duration")
	Min    string       // encoded lower boundary (8-byte LE for numeric, raw string for string)
	Max    string       // encoded upper boundary
	Offset uint64       // absolute file offset
	Length uint32       // byte length (snappy-compressed)
	Count  uint32       // total rows
	Type   ColumnType   // data type
	Format uint8        // IntrinsicFormatFlat or IntrinsicFormatDict
}
```
**Purpose**: Table-of-contents entry for one intrinsic column. Used for min/max pruning without loading the column blob.

---

## How Results Are Assembled from Block Data

### From MatchedRow to SpanMatch Flow (api.go:744-785)

**Function**: `streamFilterProgram()` (api.go:744-785)

**Steps**:
1. **Collect rows**: Call `ex.Collect(r, program, collectOpts)` → returns `[]MatchedRow`
2. **For each MatchedRow**:
   ```go
   fields := modules_blockio.NewSpanFieldsAdapter(row.Block, row.RowIdx)
   traceIDHex, spanIDHex := extractIDs(row.Block, row.RowIdx)
   match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
   ```
3. **Callback**: Call `fn(match, true)` to emit result
4. **Clone**: Caller may call `match.Clone()` to materialize fields into a map

**Key insight**: Field access is **lazy**. The `SpanFieldsProvider` (modulesSpanFieldsAdapter) reads from block columns on-demand via `GetField()`. This is critical for the fast path: we must ensure the intrinsic columns are available and correct.

---

### NewSpanFieldsAdapter (blockio/span_fields.go:23-25)
```go
func NewSpanFieldsAdapter(block *modules_reader.Block, rowIdx int) modules_shared.SpanFieldsProvider {
	return &modulesSpanFieldsAdapter{block: block, rowIdx: rowIdx}
}
```

**Implementation** (blockio/span_fields.go:15-126):
- `GetField(name)`: Lookup column by exact name or unscoped fallback (resource./span. prefixes)
- `IterateFields(fn)`: Iterate all block columns, call fn for each present value

---

## Intrinsic Column Detection & Pruning

### ProgramIsIntrinsicOnly (executor/predicates.go:219-236)
```go
func ProgramIsIntrinsicOnly(program *vm.Program) bool {
	wantCols := ProgramWantColumns(program)
	if wantCols == nil {
		return false // match-all queries are not intrinsic-only
	}
	for col := range wantCols {
		_, inTrace := traceIntrinsicColumns[col]
		_, inLog := logIntrinsicColumns[col]
		if !inTrace && !inLog {
			return false
		}
	}
	return true
}
```

**Intrinsic columns** (executor/predicates.go:192-217):
```go
var traceIntrinsicColumns = map[string]struct{}{
	"trace:id":              {},
	"span:id":               {},
	"span:parent_id":        {},
	"span:name":             {},
	"span:kind":             {},
	"span:start":            {},
	"span:end":              {},
	"span:duration":         {},
	"span:status":           {},
	"span:status_message":   {},
	"resource.service.name": {},
}
```

**Returns**: `true` if program only references columns from this set and has actual predicates (not match-all).

---

### BlocksFromIntrinsicTOC (executor/predicates.go:238-336)

**Purpose**: Existing block-level pruning using intrinsic column index.

**Steps** (lines 256-336):
1. Load intrinsic TOC (table of contents) from reader
2. For each intrinsic-only predicate leaf:
   - Check TOC min/max overlap (line 295) — no I/O
   - Load column blob (line 301) — **single I/O per column**
   - Filter by predicate type:
     - **Dict columns**: `intrinsicDictMatches()` (line 309) — exact equality only
     - **Flat columns**: `intrinsicFlatMatches()` (line 311) — range/equality via binary search
   - Return matching block indices
3. Intersect block sets across all intrinsic predicates (AND semantics)
4. Return `nil` if no pruning (all blocks still selected)

**Key functions** (executor/predicates.go:482-625):

#### intrinsicDictMatches (lines 482-536)
- Filter dict column entries by exact equality
- Collect all BlockRefs from matching entries
- Return sorted block index list

#### intrinsicFlatMatches (lines 538-625)
- Binary search sorted Uint64Values for range [lo, hi]
- Collect BlockRefs from matching values
- Handle both equality and range predicates

**File**: `/home/matt/source/tempo-mrd/vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`

---

## Collector Callback Pipeline

### spanMatchFn Type (api.go:398-400)
```go
type spanMatchFn func(match *SpanMatch, more bool) bool
```
**Purpose**: Internal callback signature. `more=true` emits a result, `more=false` signals end-of-results.

**Usage**:
1. `QueryTraceQL()` creates a collector: `collector := func(match *SpanMatch, more bool) bool { ... }`
2. Passes it to query streamers (streamFilterProgram, streamStructuralQuery, streamPipelineQuery)
3. Streamers call `fn(match, true)` for each result, `fn(nil, false)` at end

---

## Current Query Execution Flow (Collect Path)

**Function**: `stream.go:Collect()` (lines 73-265)

```
1. Validate program (line 82)
2. Compute wantColumns for two-pass decode (lines 100-114)
3. Create planner and build predicates (lines 116-121)
4. **[INTRINSIC PRUNING]** BlocksFromIntrinsicTOC(r, program) (lines 128-141)
   — Prunes blocks by intrinsic column values
5. Apply sub-file sharding if needed (lines 143-155)
6. Partition selected blocks into coalesced I/O groups (line 181)
7. **[BLOCK I/O LOOP]** For each selected block:
   a. ReadGroup() — fetch ~8MB coalesced block bytes from storage
   b. ParseBlockFromBytes() — first pass: decode predicate columns only
   c. ColumnPredicate(provider) — evaluate VM predicate against decoded columns
   d. If matches: ParseBlockFromBytes() — second pass: decode result columns
   e. streamSortedRows() — sort by timestamp (if needed), enforce limit
   f. Append to results
8. Return results ([]MatchedRow)
```

**Bottleneck**: Step 7 requires full block I/O even for intrinsic-only queries. The fast path should:
- Detect `ProgramIsIntrinsicOnly(program) == true`
- Load intrinsic columns (already loaded by BlocksFromIntrinsicTOC in step 4)
- Build MatchedRow results directly from BlockRef values
- Skip steps 7a-7e entirely

---

## Proposed Fast Path Integration Point

### Option A: Early return in stream.Collect() (PREFERRED)
**File**: `executor/stream.go`
**Location**: After line 141 (after intrinsic pruning), before line 145 (sub-file sharding)

**Logic**:
```go
if ProgramIsIntrinsicOnly(program) {
	intrinsicCols := loadIntrinsicColumnsForProgram(r, program)
	rows := buildMatchedRowsFromIntrinsicColumns(intrinsicCols, program, opts)
	return rows, nil
}
```

**Advantages**:
- Single point of injection
- Reuses BlocksFromIntrinsicTOC pruning already done
- No modification to external interfaces
- Skips entire coalesced I/O machinery

**Implementation requirements**:
1. Load intrinsic columns (reuse reader.GetIntrinsicColumn() calls)
2. Evaluate predicates against intrinsic data (reuse intrinsicDictMatches/intrinsicFlatMatches logic)
3. Construct MatchedRow slices from BlockRef values
4. Apply sorting/limit just like regular path

---

### Option B: Separate fast-path function in api.go
**File**: `api.go`
**Location**: In streamFilterProgram() (line 745), before call to ex.Collect()

**Disadvantage**: Requires changes to public API; violates CLAUDE.md principle of minimal public surface.

---

## Field Access for Intrinsic-Only Results

### Challenge
A MatchedRow must have a populated `Block` pointer to support lazy field access via `modulesSpanFieldsAdapter`. However, in the intrinsic-only fast path, we don't parse full blocks.

### Solutions

**Solution 1: Create synthetic Block with intrinsic columns only** (FEASIBLE)
- Construct a `modules_reader.Block` struct containing only intrinsic columns
- Block.GetColumn() will return the intrinsic column values
- modulesSpanFieldsAdapter.GetField() will work for intrinsic columns
- Non-intrinsic GetField() calls return (nil, false) — acceptable since query is intrinsic-only

**Solution 2: Create a custom SpanFieldsProvider** (ALTERNATIVE)
- Implement the SpanFieldsProvider interface backed by intrinsic column data
- Embed in MatchedRow via a wrapper
- Problem: MatchedRow.Block is a required field; would need to change MatchedRow struct

**Solution 3: Materialize intrinsic values into materializedSpanFields immediately** (SIMPLEST)
- For each matched row, collect its intrinsic field values from the IntrinsicColumn data
- Create a map[string]any with pre-materialized values
- Wrap in materializedSpanFields
- Create MatchedRow with this as Fields, and a synthetic/nil Block
- Problem: Changes MatchedRow to allow nil Block; requires API changes

---

## Recommended Implementation Strategy

### Step 1: Extract Common Predicate Evaluation Logic
Create helper functions in `executor/predicates.go` (already exist but not exported):
- `intrinsicDictMatches(col, leaf) []int` — already exists (line 482)
- `intrinsicFlatMatches(col, leaf) []int` — already exists (line 538)

**Action**: Export these (rename to remove 'intrinsic' prefix if needed, or leave private and call from fast path in stream.go)

### Step 2: Implement Intrinsic-Only Result Building
In `executor/stream.go`, add function:
```go
func (e *Executor) collectFromIntrinsicColumns(
	r *Reader,
	program *vm.Program,
	opts CollectOptions,
) ([]MatchedRow, error)
```

**Logic**:
1. Call BlocksFromIntrinsicTOC() to get candidate blocks
2. For each intrinsic predicate, load column blob and filter
3. Collect BlockRef values from matching entries
4. Convert BlockRef values to MatchedRow (requires synthetic Block or custom adapter)
5. Apply sorting by timestamp (if needed)
6. Apply limit
7. Return results

### Step 3: Integrate into Collect() Path
In `stream.go:Collect()` (line 142), add early return:
```go
if intrinsicBlocks != nil && ProgramIsIntrinsicOnly(program) {
	return e.collectFromIntrinsicColumns(r, program, opts)
}
```

### Step 4: Handle Block/Fields Construction
For each MatchedRow result, ensure Fields can resolve intrinsic column lookups:

**Option A (Recommended)**: Construct a minimal Block struct
```go
syntheticBlock := &modules_reader.Block{}
// Load intrinsic columns and register them in syntheticBlock
syntheticBlock.SetColumn(colName, colData)
```

**Option B**: Create wrapper that bridges intrinsic column data to MatchedRow.Block

---

## Type & Interface Summary for Implementation

| Type | Location | Purpose |
|------|----------|---------|
| `MatchedRow` | `executor/stream.go:59` | Output struct with Block, BlockIdx, RowIdx |
| `SpanFieldsProvider` | `shared/provider.go:8` | Interface: GetField, IterateFields |
| `IntrinsicColumn` | `shared/types.go:84` | Decoded column blob with BlockRefs |
| `BlockRef` | `shared/types.go:77` | (blockIdx, rowIdx) pair |
| `IntrinsicDictEntry` | `shared/types.go:99` | Dict entry with BlockRefs |
| `SpanMatch` | `api.go:354` | Public result type |

| Function | Location | Purpose |
|----------|----------|---------|
| `ProgramIsIntrinsicOnly()` | `executor/predicates.go:219` | Detect intrinsic-only program |
| `BlocksFromIntrinsicTOC()` | `executor/predicates.go:238` | Prune blocks by intrinsic columns |
| `intrinsicDictMatches()` | `executor/predicates.go:482` | Filter dict column entries |
| `intrinsicFlatMatches()` | `executor/predicates.go:538` | Binary-search flat column values |
| `NewSpanFieldsAdapter()` | `blockio/span_fields.go:23` | Create lazy field provider from block row |
| `streamFilterProgram()` | `api.go:744` | Execute filter query via Collect |

---

## Key Design Constraints

1. **BlockRef limits**: uint16 means max 65,536 rows per block (typical: 2,000-10,000)
2. **Intrinsic-only definition**: Program must reference NO non-intrinsic columns
3. **Predicate types supported**: Equality (dict), Range/Equality (flat); NOT regex on intrinsic columns
4. **No OR semantics**: BlocksFromIntrinsicTOC skips OR nodes for safety
5. **Block Fields must work**: Even in fast path, MatchedRow.Block must support GetColumn() for lazy access

---

## Files to Modify

| File | Changes |
|------|---------|
| `executor/stream.go` | Add collectFromIntrinsicColumns(); add early return in Collect() |
| `executor/predicates.go` | May need to export intrinsicDictMatches/intrinsicFlatMatches if used from stream.go |
| `blockio/span_fields.go` | Possibly: create adapter for synthetic/intrinsic-only blocks |
| `shared/types.go` | No changes needed (types already defined) |
| `api.go` | No changes needed (streamFilterProgram already compatible) |

---

## Summary

The intrinsic-only fast path opportunity lies in:
1. **Detecting** queries that only reference intrinsic columns via `ProgramIsIntrinsicOnly()`
2. **Loading** intrinsic column blobs (single I/O per column, not full block I/O)
3. **Filtering** using existing `intrinsicDictMatches()` and `intrinsicFlatMatches()` logic
4. **Building** MatchedRow results from BlockRef values without parsing full block data
5. **Integrating** as an early return in `executor.Collect()` before the coalesced I/O loop

The main challenge is ensuring MatchedRow.Block supports field access for intrinsic columns even without full block decode. This can be solved by creating a minimal Block struct populated only with intrinsic column data, which is sufficient since the query is intrinsic-only and won't access non-intrinsic fields.
