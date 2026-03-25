# Discovery: searchReq.Limit Flow from Frontend to Block.Fetch

## Summary

The `searchReq.Limit` field flows from the frontend through the querier and into the response, but **does not** reach the `block.Fetch()` call in TraceQL searches. The limit is applied as a post-processing step in `postProcessIngesterSearchResults()` for ingester results, and in `SearchBlock()` for direct block searches. No wrapper fetcher applies the limit before `block.Fetch()` is called.

## Complete Flow Diagram

```
Frontend (with Limit in SearchRequest)
  ↓
Querier.SearchBlock(ctx, req *tempopb.SearchBlockRequest)
  ├─ req.SearchReq contains Limit field
  ├─ For TraceQL queries: creates SpansetFetcherWrapper
  │  └─ SpansetFetcherWrapper is a simple pass-through wrapper
  │     (no limit enforcement)
  ├─ Calls q.engine.ExecuteSearch(ctx, req.SearchReq, fetcher, hints)
  │  └─ Does NOT pass Limit to fetcher
  └─ Returns result directly to caller
     (Limit is never applied in this path)

For Ingester Results (SearchRecent path):
Querier.SearchRecent(ctx, req *tempopb.SearchRequest)
  ├─ req.Limit is preserved
  └─ Calls postProcessIngesterSearchResults(req, results)
     └─ **THIS IS WHERE LIMIT IS APPLIED** (line 871-873)
        if req.Limit != 0 && int(req.Limit) < len(response.Traces) {
          response.Traces = response.Traces[:req.Limit]
        }
```

## Key Files & Signatures

### 1. SearchBlockRequest with Limit

**File:** `/home/matt/source/tempo-mrd/pkg/tempopb/tempo.proto`

```protobuf
message SearchRequest {
  map<string, string> Tags = 1;
  uint32 MinDurationMs = 2;
  uint32 MaxDurationMs = 3;
  uint32 Limit = 4;                    // <-- LIMIT FIELD
  uint32 start = 5;
  uint32 end = 6;
  string Query = 8;                    // TraceQL query
  uint32 SpansPerSpanSet = 9;
  google.protobuf.Timestamp RF1After = 10;
}

message SearchBlockRequest {
  SearchRequest searchReq = 1;         // <-- Limit is inside here
  string blockID = 2;
  uint32 startPage = 3;
  uint32 pagesToSearch = 4;
  uint32 indexPageSize = 6;
  uint32 totalRecords = 7;
  string version = 9;
  uint64 size = 10;
}
```

### 2. Querier.SearchBlock - Entry Point for Block Searches

**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:616-657`

```go
func (q *Querier) SearchBlock(ctx context.Context, req *tempopb.SearchBlockRequest) (*tempopb.SearchResponse, error) {
	tenantID, err := validation.ExtractValidTenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("error extracting org id in Querier.BackendSearch: %w", err)
	}

	blockID, err := backend.ParseUUID(req.BlockID)
	if err != nil {
		return nil, err
	}

	dc, err := backend.DedicatedColumnsFromTempopb(req.DedicatedColumns)
	if err != nil {
		return nil, err
	}

	meta := &backend.BlockMeta{
		Version:          req.Version,
		TenantID:         tenantID,
		Size_:            req.Size_,
		IndexPageSize:    req.IndexPageSize,
		TotalRecords:     req.TotalRecords,
		BlockID:          blockID,
		FooterSize:       req.FooterSize,
		DedicatedColumns: dc,
	}

	opts := common.DefaultSearchOptions()
	opts.StartPage = int(req.StartPage)
	opts.TotalPages = int(req.PagesToSearch)
	opts.MaxBytes = q.limits.MaxBytesPerTrace(tenantID)

	if api.IsTraceQLQuery(req.SearchReq) {
		// Create a wrapper fetcher that calls block.Fetch
		fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
			return q.store.Fetch(ctx, meta, req, opts)
		})

		// Call engine with the fetcher
		// NOTE: req.SearchReq.Limit is available here but NOT passed to ExecuteSearch
		return q.engine.ExecuteSearch(ctx, req.SearchReq, fetcher, q.limits.UnsafeQueryHints(tenantID))
	}

	return q.store.Search(ctx, meta, req.SearchReq, opts)
}
```

**Key Observation:** `req.SearchReq.Limit` is available in the request but is **not used** in the TraceQL path. It's simply ignored.

### 3. SpansetFetcherWrapper - Simple Pass-Through

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:312-324`

```go
type SpansetFetcherWrapper struct {
	f func(ctx context.Context, req FetchSpansRequest) (FetchSpansResponse, error)
}

var _ = (SpansetFetcher)(&SpansetFetcherWrapper{})

func NewSpansetFetcherWrapper(f func(ctx context.Context, req FetchSpansRequest) (FetchSpansResponse, error)) SpansetFetcher {
	return SpansetFetcherWrapper{f}
}

func (s SpansetFetcherWrapper) Fetch(ctx context.Context, request FetchSpansRequest) (FetchSpansResponse, error) {
	return s.f(ctx, request)  // <-- PASS-THROUGH: no limit enforcement
}
```

**No limit handling here** - it's just a thin wrapper.

### 4. Engine.ExecuteSearch - Core Query Execution

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:49-139`

```go
func (e *Engine) ExecuteSearch(ctx context.Context, searchReq *tempopb.SearchRequest, spanSetFetcher SpansetFetcher, allowUnsafeQueryHints bool) (*tempopb.SearchResponse, error) {
	ctx, span := tracer.Start(ctx, "traceql.Engine.ExecuteSearch")
	defer span.End()

	rootExpr, _, _, _, fetchSpansRequest, err := Compile(searchReq.Query)
	if err != nil {
		return nil, err
	}

	// ... debug hints handling ...

	var mostRecent, ok bool
	if mostRecent, ok = rootExpr.Hints.GetBool(HintMostRecent, allowUnsafeQueryHints); !ok {
		mostRecent = false
	}

	if rootExpr.IsNoop() {
		return &tempopb.SearchResponse{
			Traces:  nil,
			Metrics: &tempopb.SearchMetrics{},
		}, nil
	}

	fetchSpansRequest.StartTimeUnixNanos = unixSecToNano(searchReq.Start)
	fetchSpansRequest.EndTimeUnixNanos = unixSecToNano(searchReq.End)

	span.SetAttributes(attribute.String("pipeline", rootExpr.Pipeline.String()))
	span.SetAttributes(attribute.String("fetchSpansRequest", fmt.Sprint(fetchSpansRequest)))

	// calculate search meta conditions
	meta := SearchMetaConditionsWithout(fetchSpansRequest.Conditions, fetchSpansRequest.AllConditions)
	fetchSpansRequest.SecondPassConditions = append(fetchSpansRequest.SecondPassConditions, meta...)

	spansetsEvaluated := 0
	// set up the expression evaluation as a filter to reduce data pulled
	fetchSpansRequest.SecondPass = func(inSS *Spanset) ([]*Spanset, error) {
		if len(inSS.Spans) == 0 {
			return nil, nil
		}

		evalSS, err := rootExpr.Pipeline.evaluate([]*Spanset{inSS})
		if err != nil {
			span.RecordError(err, trace.WithAttributes(attribute.String("msg", "pipeline.evaluate")))
			return nil, err
		}

		spansetsEvaluated++
		if len(evalSS) == 0 {
			return nil, nil
		}

		// reduce all evalSS to their max length to reduce meta data lookups
		for i := range evalSS {
			l := len(evalSS[i].Spans)
			evalSS[i].AddAttribute(attributeMatched, NewStaticInt(l))

			spansPerSpanSet := int(searchReq.SpansPerSpanSet)  // <-- Uses searchReq.SpansPerSpanSet
			if spansPerSpanSet == 0 {
				spansPerSpanSet = DefaultSpansPerSpanSet
			}
			if l > spansPerSpanSet {
				evalSS[i].Spans = evalSS[i].Spans[:spansPerSpanSet]
			}
		}

		return evalSS, nil
	}

	fetchSpansResponse, err := spanSetFetcher.Fetch(ctx, *fetchSpansRequest)
	if err != nil {
		if errors.Is(err, util.ErrUnsupported) {
			return &tempopb.SearchResponse{
				Traces:  nil,
				Metrics: &tempopb.SearchMetrics{},
			}, nil
		}
		return nil, err
	}
	// ... continues with result processing ...
}
```

**Key Observations:**
- `searchReq` is available but only `SpansPerSpanSet` (line 118) is used from it
- `searchReq.Limit` is **never read**
- `TraceSampler` and `SpanSampler` in `FetchSpansRequest` are **never set** from the request
- The fetcher is called with `fetchSpansRequest` which doesn't contain limit info

### 5. FetchSpansRequest Structure (No Limit Field)

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:81-113`

```go
type FetchSpansRequest struct {
	StartTimeUnixNanos uint64
	EndTimeUnixNanos   uint64
	Conditions         []Condition

	// Hints
	AllConditions bool

	// Sampling
	TraceSampler Sampler  // <-- These are never set from searchReq.Limit
	SpanSampler  Sampler

	// SecondPass filtering
	SecondPass           SecondPassFn
	SecondPassConditions []Condition
	SecondPassSelectAll  bool
}
```

**NO LIMIT FIELD** exists in FetchSpansRequest.

### 6. TempoDB Reader.Fetch - Backend Block Fetch

**File:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:541-550`

```go
// Fetch only uses rw.r which has caching enabled
func (rw *readerWriter) Fetch(ctx context.Context, meta *backend.BlockMeta, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	block, err := encoding.OpenBlock(meta, rw.r)
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}

	rw.cfg.Search.ApplyToOptions(&opts)
	return block.Fetch(ctx, req, opts)  // <-- Called with req (no limit)
}
```

The `req` parameter has no limit info.

### 7. Post-Processing for Ingester Results (Where Limit IS Applied)

**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:841-876`

```go
func (q *Querier) postProcessIngesterSearchResults(req *tempopb.SearchRequest, results []any) *tempopb.SearchResponse {
	response := &tempopb.SearchResponse{
		Metrics: &tempopb.SearchMetrics{},
	}

	traces := map[string]*tempopb.TraceSearchMetadata{}

	for _, result := range results {
		sr := result.(*tempopb.SearchResponse)

		for _, t := range sr.Traces {
			// Just simply take first result for each trace
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
	if req.Limit != 0 && int(req.Limit) < len(response.Traces) {
		response.Traces = response.Traces[:req.Limit]  // <-- LIMIT ENFORCED HERE
	}

	return response
}
```

**THIS IS THE ONLY PLACE WHERE searchReq.Limit IS ENFORCED** (for ingester results).

### 8. QueryFrontend or HTTP Handler Entry Point

The `SearchRequest` with `Limit` comes from the frontend. The SearchBlock request is created by the frontend when distributing block searches.

## Critical Findings

### 1. No Limit Enforcement for TraceQL Block Searches
- `Querier.SearchBlock()` for TraceQL queries calls `ExecuteSearch()`
- **Limit is available in `req.SearchReq` but never used**
- `FetchSpansRequest` sent to `block.Fetch()` has **no Limit field**
- `SpansetFetcherWrapper` is a simple pass-through with **no limit enforcement**

### 2. Limit Only Applied to Ingester Results
- `Querier.SearchRecent()` calls `postProcessIngesterSearchResults()`
- **ONLY HERE** is `req.Limit` read and enforced (lines 871-873)
- This is **after** all results are collected from ingesters

### 3. No Sampling Setup
- `FetchSpansRequest.TraceSampler` and `FetchSpansRequest.SpanSampler` exist but are **never populated**
- In `ExecuteSearch()`, only `searchReq.SpansPerSpanSet` is used (line 118)
- `searchReq.Limit` is completely ignored

### 4. What Fields ARE Passed Through
From `searchReq` to the execution layer:
- `SpansPerSpanSet` (used to limit spans per spanset)
- Time range: `Start` and `End` (converted to `UnixNanos`)
- Everything else is extracted into TraceQL conditions

Limit is **not passed through**.

## Data Flow Summary

```
SearchBlockRequest.SearchReq.Limit
  └─ AVAILABLE in Querier.SearchBlock()
     └─ Passed to Engine.ExecuteSearch(ctx, req.SearchReq, ...)
        └─ NEVER READ in ExecuteSearch()
           └─ NOT PASSED to FetchSpansRequest
              └─ NOT PASSED to block.Fetch()
                 └─ LOST

SearchRequest.Limit (for ingesters)
  └─ AVAILABLE in Querier.SearchRecent()
     └─ Passed to postProcessIngesterSearchResults()
        └─ ENFORCED via slice truncation (lines 871-873)
           └─ LIMIT APPLIED TO RESPONSE
```

## Code Paths by Query Type

| Query Type | Entry Point | Limit Handling |
|-----------|-------------|-----------------|
| TraceQL Block Search | `SearchBlock()` | **NOT HANDLED** - ignored completely |
| Legacy Block Search | `SearchBlock()` (non-TraceQL path) | May use `req.SearchReq.Limit` via `Search()` method (needs verification) |
| Ingester Search (Recent) | `SearchRecent()` | **HANDLED** in `postProcessIngesterSearchResults()` |
| Tag Searches | Various | Not relevant to span limit |

## Recommendations for Implementation

If you need to enforce `Limit` on TraceQL block searches, you have several options:

1. **Option A: Wrapper Fetcher** - Create a limiting wrapper around `SpansetFetcherWrapper`
2. **Option B: Engine Level** - Modify `ExecuteSearch()` to accept and enforce limit
3. **Option C: FetchSpansRequest** - Add a `Limit` field to `FetchSpansRequest`
4. **Option D: Post-Process** - Apply limit after engine returns (similar to ingester flow)

Current code suggests **Option D** is the existing pattern, but block search results go directly to the caller without this post-processing.
