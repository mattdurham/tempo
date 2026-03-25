# Bug Investigation: Blockpack Returns 0 Results for `{resource.service.name="loki-querier"}`

## Issue Summary

Blockpack returns 0 results for queries about "loki-querier" service while Parquet returns 20 results. Both are loaded with data containing loki-querier traces.

**Query:** `{resource.service.name="loki-querier"}`
**Also fails:** `{resource.service.name=~"loki.*"}` (regex query)
**Timestamps:** `start=1767243600 end=2147483647`
**Test Results** (2025-03-15):
- Parquet: `{resource.service.name="loki-querier"}` → 20 traces
- Blockpack: `{resource.service.name="loki-querier"}` → 0 traces
- Parquet: `{resource.service.name=~"loki.*"}` → 20 traces  
- Blockpack: `{resource.service.name=~"loki.*"}` → 0 traces

**Other services work fine:**
- Both return 20 traces for `{resource.service.name="grafana"}`
- Blockpack can find "CockroachDB", "consumer", and many other services with equality queries

## Data Verification

Service names in blockpack (from `{}` match-all query, limit=10000):
- CockroachDB (4238 traces)
- auth_api (1746 traces)
- adaptive-profiles-data-plane (1244 traces)
- consumer (948 traces)
- gme-block-builder (941 traces)
- [42 other services listed]
- **NOT FOUND: loki-querier**

Service names in parquet (from `{}` match-all query):
- Same services as blockpack
- PLUS: loki-querier (20 traces)

**Conclusion:** Blockpack blocks do NOT contain loki-querier traces, but parquet blocks do. The containers have different data loaded.

## Code Path Analysis

### Query Flow (TraceQL with `q` parameter)

When `/api/search?q=%7Bresource.service.name%3D%22loki-querier%22%7D` is called:

1. **HTTP Handler** → `ParseSearchRequest()`: Populates `SearchRequest.Query` with the TraceQL string
2. **Querier** (`modules/querier/querier.go:650-671`):
   - Detects `IsTraceQLQuery(req.SearchReq)` returns true
   - Strips hint suffix to get `queryForBackend = "{resource.service.name=\"loki-querier\"}"`
   - Injects via `WithOriginalTraceQLQuery(ctx, queryForBackend, mostRecent)`
   - **Calls Fetch() path** (not Search())
3. **Backend Block** → `tempodb/encoding/vblockpack/backend_block.go:305-369`:
   - `Fetch()` retrieves original query from context at line 310-312
   - Passes query to `blockpack.QueryTraceQL(r, query, opts)` at line 362
   - Correctly sets `MostRecent: common.TraceQLMostRecent(ctx)` at line 364

### Code Evidence: Implementation is Correct

**File:** `modules/querier/querier.go:650-671`
```go
if api.IsTraceQLQuery(req.SearchReq) {
    queryForBackend := req.SearchReq.Query
    mostRecent := strings.Contains(queryForBackend, "most_recent=true")
    if idx := strings.Index(queryForBackend, " with ("); idx != -1 {
        queryForBackend = queryForBackend[:idx]
    }
    ctx = common.WithOriginalTraceQLQuery(ctx, queryForBackend, mostRecent)
    // ...Fetch is called
```

**File:** `tempodb/encoding/vblockpack/backend_block.go:305-312`
```go
func (b *blockpackBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
    query := conditionsToTraceQL(req.Conditions, req.AllConditions)
    if orig, ok := common.OriginalTraceQLQuery(ctx); ok {
        query = orig  // Original TraceQL query is used
    }
```

## Root Cause: Blockpack Data Content

The issue is **NOT a code bug in Tempo's vblockpack integration**. The Tempo code correctly:
- Receives and injects TraceQL queries into context
- Passes the query to blockpack.QueryTraceQL()
- Handles the most_recent hint properly

The issue is that **blockpack blocks do not contain loki-querier traces**, while parquet blocks do.

### Why This Matters

This is a **data consistency issue between the two storage backends**:
- Parquet was loaded with complete production trace data including loki-querier
- Blockpack was loaded with incomplete data (missing loki-querier traces)

### Resolution

**To fix the discrepancy:**

1. **Reload blockpack with complete data:**
   ```bash
   cd /home/matt/source/tempo-mrd/.docker
   make down           # Stop containers
   make clean-volumes  # Wipe all data
   make up             # Start fresh
   make reload         # Load traces from ~/utils/dev_traces/3/
   ```

2. **Verify both have same data:**
   ```bash
   # Should find loki-querier in blockpack now
   curl -s "http://localhost:13201/api/search?q=%7Bresource.service.name%3D%22loki-querier%22%7D" | jq '.traces | length'
   # Should return 20 (matching parquet)
   ```

## Blockpack Code Quality

The vblockpack integration layer is well-implemented:
- **Query injection:** Context properly carries original TraceQL query from Querier to Block
- **Condition conversion:** Fallback `conditionsToTraceQL()` correctly handles non-TraceQL paths
- **Hint handling:** `most_recent` flag extracted and passed to blockpack.QueryTraceQL()
- **Column mapping:** `attributeToTraceQLName()` correctly handles resource scope attributes

**No changes needed to Tempo code** — the issue is purely in the test data setup.

## Files Analyzed

| File | Lines | Purpose |
|------|-------|---------|
| `modules/querier/querier.go` | 650-671 | Query routing: detects TraceQL and injects into context |
| `tempodb/encoding/vblockpack/backend_block.go` | 305-369 | Fetch implementation: uses original query and calls blockpack.QueryTraceQL |
| `tempodb/encoding/vblockpack/backend_block.go` | 773-846 | conditionsToTraceQL: fallback for non-TraceQL paths |
| `tempodb/encoding/vblockpack/backend_block.go` | 849-878 | attributeToTraceQLName: maps attributes to TraceQL column names |
| `tempodb/encoding/common/interfaces.go` | 26 | Block interface defining Fetch/Search signatures |
| `pkg/traceql/engine.go` | 49-177 | ExecuteSearch: compiles TraceQL and orchestrates Fetch |

## Summary

**Issue:** Blockpack returns 0 results for loki-querier queries
**Root Cause:** Blockpack blocks don't contain loki-querier traces (data loading issue, not code bug)
**Status:** No Tempo code changes needed
**Resolution:** Reload blockpack with complete trace data matching parquet

The vblockpack integration correctly handles TraceQL queries and passes them to blockpack without modification.
