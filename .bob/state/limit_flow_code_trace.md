# Limit Flow - Actual Code Trace

## Complete Execution Path for TraceQL Block Search

### Step 1: Frontend Request → Querier.SearchBlock

**Source:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:616`

The request object contains:
```
SearchBlockRequest{
  SearchReq: SearchRequest{
    Limit: 100,           // <-- User's limit
    Query: "{ .http.method = \"GET\" }",
    Start: 1234567890,
    End:   1234567950,
    SpansPerSpanSet: 10,
  },
  BlockID: "abc-123-...",
  Version: "vblockpack",
  // ... other fields
}
```

### Step 2: Querier.SearchBlock - Create Wrapper Fetcher

**Source:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:648-653`

```go
if api.IsTraceQLQuery(req.SearchReq) {
    // Wrap the store.Fetch call
    fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
        return q.store.Fetch(ctx, meta, req, opts)
    })

    // Execute with the fetcher
    // req.SearchReq.Limit is available here
    // but it's NEVER EXTRACTED or PASSED FORWARD
    return q.engine.ExecuteSearch(ctx, req.SearchReq, fetcher, q.limits.UnsafeQueryHints(tenantID))
}
```

**Limit Status:** Available in `req.SearchReq.Limit` but not used

### Step 3: Engine.ExecuteSearch - Process Query

**Source:** `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:49-130`

```go
func (e *Engine) ExecuteSearch(ctx context.Context, searchReq *tempopb.SearchRequest, spanSetFetcher SpansetFetcher, allowUnsafeQueryHints bool) (*tempopb.SearchResponse, error) {
    ctx, span := tracer.Start(ctx, "traceql.Engine.ExecuteSearch")
    defer span.End()

    // Parse query into AST
    rootExpr, _, _, _, fetchSpansRequest, err := Compile(searchReq.Query)
    if err != nil {
        return nil, err
    }

    // ... handle various hints ...

    // Create the FetchSpansRequest - THIS IS KEY
    fetchSpansRequest.StartTimeUnixNanos = unixSecToNano(searchReq.Start)
    fetchSpansRequest.EndTimeUnixNanos = unixSecToNano(searchReq.End)

    // Only SpansPerSpanSet is extracted from searchReq
    // searchReq.Limit is completely IGNORED

    span.SetAttributes(attribute.String("pipeline", rootExpr.Pipeline.String()))
    span.SetAttributes(attribute.String("fetchSpansRequest", fmt.Sprint(fetchSpansRequest)))

    // ... setup SecondPass ...

    // Call the fetcher with FetchSpansRequest (no Limit field)
    fetchSpansResponse, err := spanSetFetcher.Fetch(ctx, *fetchSpansRequest)
    // ...
}
```

**Limit Status:** Not read from `searchReq`. Not added to `fetchSpansRequest`.

### Step 4: The FetchSpansRequest Object - No Limit Field

**Source:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:81-113`

```go
type FetchSpansRequest struct {
    StartTimeUnixNanos uint64
    EndTimeUnixNanos   uint64
    Conditions         []Condition

    // Hints
    AllConditions bool

    // Sampling - NEVER POPULATED
    TraceSampler Sampler
    SpanSampler  Sampler

    // SecondPass filtering
    SecondPass           SecondPassFn
    SecondPassConditions []Condition
    SecondPassSelectAll  bool

    // NOTE: NO Limit FIELD EXISTS
}
```

This structure is passed to `block.Fetch()` with no limit information.

### Step 5: SpansetFetcherWrapper.Fetch - Pass Through

**Source:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:322-324`

```go
func (s SpansetFetcherWrapper) Fetch(ctx context.Context, request FetchSpansRequest) (FetchSpansResponse, error) {
    // Simple pass-through, no limit enforcement
    return s.f(ctx, request)
}
```

The function passed to this wrapper is:
```go
func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
    return q.store.Fetch(ctx, meta, req, opts)
}
```

No limit enforcement here either.

### Step 6: TempoDB Reader.Fetch - Call Block.Fetch

**Source:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:541-550`

```go
func (rw *readerWriter) Fetch(ctx context.Context, meta *backend.BlockMeta, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
    block, err := encoding.OpenBlock(meta, rw.r)
    if err != nil {
        return traceql.FetchSpansResponse{}, err
    }

    rw.cfg.Search.ApplyToOptions(&opts)
    return block.Fetch(ctx, req, opts)  // req has no Limit
}
```

Calls the block's Fetch method with `req` (no limit) and `opts`.

### Step 7: Block.Fetch - Storage Layer (e.g., vblockpack)

**Source:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go`

```go
func (b *backendBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
    // ... implementation details ...
    // req has no Limit field
    // Must materialize ALL matching spansets
}
```

The backend block materializes **ALL** matching spansets. No limit is enforced here.

## Comparison: Ingester Path (Where Limit DOES Work)

**Source:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:357-369`

```go
func (q *Querier) SearchRecent(ctx context.Context, req *tempopb.SearchRequest) (*tempopb.SearchResponse, error) {
    if _, err := validation.ExtractValidTenantID(ctx); err != nil {
        return nil, fmt.Errorf("error extracting org id in Querier.SearchRecent: %w", err)
    }

    results, err := q.forLiveStoreRing(ctx, func(ctx context.Context, client tempopb.QuerierClient) (any, error) {
        return client.SearchRecent(ctx, req)  // req contains Limit
    })
    if err != nil {
        return nil, fmt.Errorf("error querying live-stores in Querier.SearchRecent: %w", err)
    }

    // req is passed to postProcessIngesterSearchResults
    return q.postProcessIngesterSearchResults(req, results), nil
}
```

Then:

```go
func (q *Querier) postProcessIngesterSearchResults(req *tempopb.SearchRequest, results []any) *tempopb.SearchResponse {
    response := &tempopb.SearchResponse{
        Metrics: &tempopb.SearchMetrics{},
    }

    traces := map[string]*tempopb.TraceSearchMetadata{}

    for _, result := range results {
        sr := result.(*tempopb.SearchResponse)
        for _, t := range sr.Traces {
            if _, ok := traces[t.TraceID]; !ok {
                traces[t.TraceID] = t
            }
        }
        if sr.Metrics != nil {
            response.Metrics.InspectedBytes += sr.Metrics.InspectedBytes
            response.Metrics.InspectedTraces += sr.Metrics.InspectedTraces
        }
    }

    for _, t := range traces {
        response.Traces = append(response.Traces, t)
    }

    // Sort and limit results
    sort.Slice(response.Traces, func(i, j int) bool {
        return response.Traces[i].StartTimeUnixNano > response.Traces[j].StartTimeUnixNano
    })

    // HERE IS WHERE LIMIT IS ENFORCED
    if req.Limit != 0 && int(req.Limit) < len(response.Traces) {
        response.Traces = response.Traces[:req.Limit]
    }

    return response
}
```

## Summary of What Happens to Limit

| Component | Input | Has Limit? | Output | Limit Applied? |
|-----------|-------|-----------|--------|-----------------|
| Frontend | SearchRequest(Limit=100) | Yes | SearchBlockRequest | Pass through |
| SearchBlock() | req.SearchReq.Limit | Yes | req to ExecuteSearch | **NO** - not extracted |
| ExecuteSearch() | searchReq.Limit | Yes | fetchSpansRequest | **NO** - not copied |
| FetchSpansRequest | (field) | **NO** | (to Fetch) | N/A |
| SpansetFetcherWrapper | request | **NO** | (to store.Fetch) | N/A |
| tempodb.Fetch | req | **NO** | (to block.Fetch) | N/A |
| Block.Fetch | req | **NO** | FetchSpansResponse | **NO** - no limit field |
| SearchBlock result | SearchResponse | (no limit applied) | (to caller) | **NO** |

## What DOES Flow Through

✅ Query (TraceQL conditions extracted)
✅ Time range (Start/End → UnixNanos)
✅ SpansPerSpanSet (used to limit spans within each spanset)

❌ Limit (completely lost)
❌ TraceSampler (never set)
❌ SpanSampler (never set)

## Why This Matters for Block Pack

When a TraceQL search hits a block:
1. Block must materialize ALL matching spansets
2. No limit is enforced at the storage layer
3. No sampling is applied
4. The caller gets back ALL results

If you need to enforce limit, you must:
- Add a Limit field to FetchSpansRequest, OR
- Wrap the fetcher to enforce limit, OR
- Post-process results (like ingester path does)
