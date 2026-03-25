# Direct Answers to Your Questions

## Q1: Read SearchBlock and how it calls into tempodb

**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:616-657`

**Complete Function:**
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
		fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
			return q.store.Fetch(ctx, meta, req, opts)
		})

		return q.engine.ExecuteSearch(ctx, req.SearchReq, fetcher, q.limits.UnsafeQueryHints(tenantID))
	}

	return q.store.Search(ctx, meta, req.SearchReq, opts)
}
```

**How it calls tempodb:**
- For TraceQL: Via `q.store.Fetch(ctx, meta, req, opts)` wrapped in the fetcher
- For non-TraceQL: Via `q.store.Search(ctx, meta, req.SearchReq, opts)`

**Where Limit is:** `req.SearchReq.Limit` is available but **not used** in the TraceQL path.

---

## Q2: Read tempodb.go - Find Search method and show how it calls engine.ExecuteSearch

**File:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:429-437`

```go
// Search the given block.  This method takes the pre-loaded block meta instead of a block ID, which
// eliminates a read per search request.
func (rw *readerWriter) Search(ctx context.Context, meta *backend.BlockMeta, req *tempopb.SearchRequest, opts common.SearchOptions) (*tempopb.SearchResponse, error) {
	block, err := encoding.OpenBlock(meta, rw.r)
	if err != nil {
		return nil, err
	}

	rw.cfg.Search.ApplyToOptions(&opts)
	return block.Search(ctx, req, opts)
}
```

And the **Fetch method** (used by TraceQL path):

**File:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:541-550`

```go
// Fetch only uses rw.r which has caching enabled
func (rw *readerWriter) Fetch(ctx context.Context, meta *backend.BlockMeta, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	block, err := encoding.OpenBlock(meta, rw.r)
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}

	rw.cfg.Search.ApplyToOptions(&opts)
	return block.Fetch(ctx, req, opts)
}
```

**What is passed for spanSetFetcher:**
- `Fetch` receives a `traceql.FetchSpansRequest` (no Limit field)
- This gets passed to `block.Fetch()`
- The block materializes all matching spansets
- No limit is enforced

**Engine.ExecuteSearch is NOT called here in tempodb.** ExecuteSearch is called in the querier (line 653 of querier.go).

---

## Q3: Look for TraceSampler or SpanSampler set based on searchReq.Limit

**Search Result:** Not found. Neither TraceSampler nor SpanSampler are ever set based on searchReq.Limit.

Where they're defined:

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:81-113`

```go
type FetchSpansRequest struct {
	StartTimeUnixNanos uint64
	EndTimeUnixNanos   uint64
	Conditions         []Condition

	// Hints
	AllConditions bool

	// Sampling
	// There are two methods of sampling: trace-level samples a subset of traces,
	// either skipping them or returning them in full.  Span-level samples a
	// subset of spans within each trace, still returning a spanset for each
	// matching trace. Only one will be applied at a time.  These fields are
	// optional and there is no negative effect on the results if they are not
	// honored at the storage layer.
	TraceSampler Sampler    // <-- NEVER SET
	SpanSampler  Sampler    // <-- NEVER SET

	// SecondPassFn and Conditions allow a caller to retrieve one set of data
	// in the first pass, filter using the SecondPassFn callback and then
	// request a different set of data in the second pass.
	SecondPass           SecondPassFn
	SecondPassConditions []Condition
	SecondPassSelectAll  bool
}
```

**Where they should be set (but aren't):**
- In `Engine.ExecuteSearch()` - where `searchReq.Limit` is available but ignored
- Nowhere in the codebase currently sets these samplers from `searchReq.Limit`

---

## Q4: Is there a wrapper SpansetFetcher that honors the limit before calling block.Fetch?

**Answer: NO. There is no such wrapper.**

The SpansetFetcherWrapper is:

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
	return s.f(ctx, request)  // <-- SIMPLE PASS-THROUGH, NO LIMIT LOGIC
}
```

This is just a thin wrapper that:
1. Takes a function that does `q.store.Fetch(ctx, meta, req, opts)`
2. Returns that function's result directly
3. **Does NOT enforce any limit**

---

## Q5: Read engine.go around line 49-130 - confirm whether TraceSampler is ever set

**File:** `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:49-139`

```go
func (e *Engine) ExecuteSearch(ctx context.Context, searchReq *tempopb.SearchRequest, spanSetFetcher SpansetFetcher, allowUnsafeQueryHints bool) (*tempopb.SearchResponse, error) {
	ctx, span := tracer.Start(ctx, "traceql.Engine.ExecuteSearch")
	defer span.End()

	rootExpr, _, _, _, fetchSpansRequest, err := Compile(searchReq.Query)
	if err != nil {
		return nil, err
	}

	// Check for performance testing hints
	if returnIn, ok := rootExpr.Hints.GetDuration(HintDebugReturnIn, allowUnsafeQueryHints); ok {
		var stdDev time.Duration
		if stdDevDuration, ok := rootExpr.Hints.GetDuration(HintDebugStdDev, allowUnsafeQueryHints); ok {
			stdDev = stdDevDuration
		}
		simulateLatency(returnIn, stdDev)

		var probability float64
		if p, ok := rootExpr.Hints.GetFloat(HintDebugDataFactor, allowUnsafeQueryHints); ok {
			probability = p
		}
		return generateFakeSearchResponse(probability), nil
	}

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

	// calculate search meta conditions.
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

			spansPerSpanSet := int(searchReq.SpansPerSpanSet)
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
```

**Findings:**
- `TraceSampler` is **NEVER set** in ExecuteSearch
- `SpanSampler` is **NEVER set** in ExecuteSearch
- `searchReq.Limit` is **NEVER read** in ExecuteSearch
- The only field from `searchReq` that is used is `SpansPerSpanSet` (line 118)
- The `fetchSpansRequest` is passed to the fetcher with NO limit or sampling info

---

## Summary Table

| Question | Answer | Location |
|----------|--------|----------|
| SearchBlock entry point? | Yes, at line 616 of querier.go | `/home/matt/source/tempo-mrd/modules/querier/querier.go:616` |
| How does it call tempodb? | Via `q.store.Fetch()` for TraceQL, wrapped in SpansetFetcherWrapper | Line 649-651 of querier.go |
| Search method in tempodb? | `Search()` for non-TraceQL, `Fetch()` for TraceQL | `/home/matt/source/tempo-mrd/tempodb/tempodb.go:429, 541` |
| How does tempodb call engine.ExecuteSearch? | It doesn't - ExecuteSearch is called in the querier, not tempodb | querier.go:653 |
| TraceSampler/SpanSampler set from Limit? | **NO** - never set from `searchReq.Limit` | Not found anywhere |
| Wrapper enforces limit? | **NO** - SpansetFetcherWrapper is a simple pass-through | `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:322` |
| TraceSampler set in engine? | **NO** - never set, never read | `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:49-139` |
| Where IS limit enforced? | Only in `postProcessIngesterSearchResults()` for ingester results | `/home/matt/source/tempo-mrd/modules/querier/querier.go:871` |

---

## The Core Issue

**`searchReq.Limit` is available at the querier level but is never passed down to the storage layer.**

Flow:
```
Querier.SearchBlock()
  ├─ Has: req.SearchReq.Limit = 100
  ├─ Creates: SpansetFetcherWrapper (no limit logic)
  └─ Calls: engine.ExecuteSearch(ctx, req.SearchReq, fetcher, hints)
      ├─ Has: searchReq.Limit = 100
      ├─ Creates: FetchSpansRequest (NO Limit FIELD)
      └─ Calls: fetcher.Fetch(ctx, fetchSpansRequest)
          └─ Calls: q.store.Fetch(ctx, meta, req, opts)
              └─ Calls: block.Fetch(ctx, req, opts)
                  └─ NO LIMIT INFO AVAILABLE
```

The limit information is lost between step 1 and step 3.
