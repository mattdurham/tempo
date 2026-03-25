# Tempo Limit Flow Investigation - Complete Documentation

## Investigation Goal

Trace exactly how `searchReq.Limit` flows from the frontend/querier down to where `Fetch` is called on a backend block.

## Key Finding

**`searchReq.Limit` is available at the querier level but is NEVER passed to the storage layer.** The limit is completely lost between `Querier.SearchBlock()` and `block.Fetch()`.

## Documents Created

### 1. **limit_flow_discovery.md** (13K)
Comprehensive overview of the complete flow with diagrams and tables.

**Sections:**
- Summary of the findings
- Complete flow diagram showing where limit is and isn't used
- Key files and signatures
- Critical findings with code examples
- Data flow summary
- Code paths by query type

**Read this first** for the big picture.

---

### 2. **limit_flow_code_trace.md** (8.2K)
Step-by-step execution trace showing exactly what happens at each layer.

**Sections:**
- Step 1: Frontend Request → Querier.SearchBlock
- Step 2: Querier.SearchBlock creates wrapper fetcher
- Step 3: Engine.ExecuteSearch processes query
- Step 4: FetchSpansRequest object (no Limit field)
- Step 5: SpansetFetcherWrapper pass-through
- Step 6: TempoDB Reader.Fetch calls block.Fetch
- Step 7: Block.Fetch materializes all spansets
- Comparison with Ingester Path (where limit DOES work)
- Summary table of what flows through vs what's lost

**Read this** to understand the actual execution flow.

---

### 3. **limit_qa.md** (11K)
Direct answers to your specific questions with exact code.

**Answers:**
1. SearchBlock function and tempodb calls
2. Tempodb.Search method and engine.ExecuteSearch calls
3. Whether TraceSampler/SpanSampler are set from Limit
4. Whether there's a wrapper enforcing limit
5. Whether TraceSampler is ever set in engine

**Read this** to get specific answers with code references.

---

### 4. **function_signatures.md** (7.7K)
Exact function signatures from the codebase with full declarations.

**Contains:**
- Querier.SearchBlock signature
- Engine.ExecuteSearch signature
- SpansetFetcher interface
- FetchSpansRequest structure
- FetchSpansResponse structure
- SpansetFetcherWrapper implementation
- Reader.Fetch (TempoDB interface)
- ReaderWriter.Fetch (implementation)
- BackendBlock.Fetch (interface)
- PostProcessIngesterSearchResults

**Use this** as a reference for exact signatures and type definitions.

---

## Quick Reference - The Core Issue

### What Gets Lost

```
Querier.SearchBlock()
  req.SearchReq.Limit = 100  ✓ Available

Engine.ExecuteSearch()
  searchReq.Limit = 100  ✓ Available
  BUT: Creates FetchSpansRequest with NO Limit field

Block.Fetch()
  req (FetchSpansRequest) has NO Limit  ✗ LOST
```

### The Problem

| Location | Limit Available? | Limit Used? |
|----------|------------------|------------|
| Querier.SearchBlock | Yes | NO |
| Engine.ExecuteSearch | Yes | NO |
| FetchSpansRequest | NO | N/A |
| SpansetFetcherWrapper | NO | N/A |
| TempoDB Reader.Fetch | NO | N/A |
| Block.Fetch | NO | N/A |

### Where Limit IS Used

Only in `postProcessIngesterSearchResults()` for ingester searches:

```go
if req.Limit != 0 && int(req.Limit) < len(response.Traces) {
    response.Traces = response.Traces[:req.Limit]
}
```

This is AFTER all results are collected from ingesters, not at the storage layer.

---

## File Locations (Absolute Paths)

| File | Purpose |
|------|---------|
| `/home/matt/source/tempo-mrd/modules/querier/querier.go:616-657` | Querier.SearchBlock entry point |
| `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:49-139` | Engine.ExecuteSearch core logic |
| `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:81-113` | FetchSpansRequest definition |
| `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:312-324` | SpansetFetcherWrapper implementation |
| `/home/matt/source/tempo-mrd/tempodb/tempodb.go:541-550` | TempoDB Reader.Fetch |
| `/home/matt/source/tempo-mrd/tempodb/tempodb.go:429-437` | TempoDB Reader.Search |
| `/home/matt/source/tempo-mrd/modules/querier/querier.go:841-876` | postProcessIngesterSearchResults |
| `/home/matt/source/tempo-mrd/pkg/tempopb/tempo.proto` | SearchRequest protobuf with Limit field |

---

## Key Code Snippets

### 1. Where Limit Is Available But Not Used

**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:648-653`

```go
if api.IsTraceQLQuery(req.SearchReq) {
    fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
        return q.store.Fetch(ctx, meta, req, opts)
    })

    return q.engine.ExecuteSearch(ctx, req.SearchReq, fetcher, q.limits.UnsafeQueryHints(tenantID))
    // NOTE: req.SearchReq.Limit is available but NOT extracted or passed forward
}
```

### 2. Where FetchSpansRequest is Created (No Limit Field)

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:53, 85-86`

```go
rootExpr, _, _, _, fetchSpansRequest, err := Compile(searchReq.Query)
// ...
fetchSpansRequest.StartTimeUnixNanos = unixSecToNano(searchReq.Start)
fetchSpansRequest.EndTimeUnixNanos = unixSecToNano(searchReq.End)
// searchReq.Limit is NOT copied
```

### 3. Simple Pass-Through Wrapper (No Limit Logic)

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:322-324`

```go
func (s SpansetFetcherWrapper) Fetch(ctx context.Context, request FetchSpansRequest) (FetchSpansResponse, error) {
    return s.f(ctx, request)  // PASS-THROUGH
}
```

### 4. Only Place Where Limit IS Enforced

**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:871-873`

```go
if req.Limit != 0 && int(req.Limit) < len(response.Traces) {
    response.Traces = response.Traces[:req.Limit]
}
```

This is in `postProcessIngesterSearchResults()` after all ingester results are collected.

---

## Questions Answered

### Q1: Read modules/querier/querier.go around line 616 - show me the SearchBlock function
**See:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:616-657`
- Full function shown in limit_qa.md and function_signatures.md
- Creates SpansetFetcherWrapper with no limit logic

### Q2: Read tempodb/tempodb.go - find the Search method and how it calls engine.ExecuteSearch
**See:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:429-437, 541-550`
- `Search()` method shown (non-TraceQL path)
- `Fetch()` method shown (TraceQL path)
- **NOTE:** Engine.ExecuteSearch is called in querier.go, NOT in tempodb.go
- TempoDB.Fetch is called BY ExecuteSearch, not the reverse

### Q3: Look for any place where TraceSampler or SpanSampler are set based on searchReq.Limit
**Result:** Not found anywhere in the codebase
- FetchSpansRequest.TraceSampler - never populated
- FetchSpansRequest.SpanSampler - never populated
- searchReq.Limit - never read in engine or storage layer

### Q4: Look for how searchReq.Limit is used - is there a wrapper SpansetFetcher that honors the limit?
**Result:** NO
- SpansetFetcherWrapper is a simple pass-through (storage.go:322-324)
- No limit enforcement anywhere in the fetcher chain
- Only place limit is enforced is in postProcessIngesterSearchResults()

### Q5: Read pkg/traceql/engine.go around line 49-130 - confirm whether TraceSampler is ever set
**Result:** NO
- TraceSampler never set in ExecuteSearch
- SpanSampler never set in ExecuteSearch
- searchReq.Limit never read in ExecuteSearch
- Only searchReq.SpansPerSpanSet is used (line 118)

---

## Reading Order Recommendation

**For complete understanding:**
1. Start with **limit_flow_discovery.md** (overview with diagrams)
2. Read **limit_qa.md** (specific answers to your questions)
3. Review **limit_flow_code_trace.md** (step-by-step execution)
4. Use **function_signatures.md** (as reference for exact signatures)

**For quick lookup:**
- Use **function_signatures.md** to find exact declarations
- Use **limit_qa.md** to find answers to specific questions
- Use **limit_flow_discovery.md** for comprehensive overview

---

## Summary

The `searchReq.Limit` field **exists** in the request at the querier level, but it is **never extracted** and **never passed** to the storage layer. The storage layer (block.Fetch) receives a FetchSpansRequest with **no Limit field**, so it must materialize ALL matching spansets.

The limit is only enforced for ingester results via post-processing in `postProcessIngesterSearchResults()`, which is called AFTER all results are collected.

**For TraceQL block searches, there is currently NO limit enforcement at the storage layer.**
