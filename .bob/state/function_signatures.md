# Function Signatures - Exact Declarations

## Entry Points

### Querier.SearchBlock
**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:616`

```go
func (q *Querier) SearchBlock(
    ctx context.Context,
    req *tempopb.SearchBlockRequest,
) (*tempopb.SearchResponse, error)
```

**Input req contains:**
```protobuf
message SearchBlockRequest {
  SearchRequest searchReq = 1;  // <- Contains Limit field
  string blockID = 2;
  uint32 startPage = 3;
  uint32 pagesToSearch = 4;
  uint32 indexPageSize = 6;
  uint32 totalRecords = 7;
  string version = 9;
  uint64 size = 10;
}

message SearchRequest {
  map<string, string> Tags = 1;
  uint32 MinDurationMs = 2;
  uint32 MaxDurationMs = 3;
  uint32 Limit = 4;  // <- THIS IS THE LIMIT
  uint32 start = 5;
  uint32 end = 6;
  string Query = 8;
  uint32 SpansPerSpanSet = 9;
  google.protobuf.Timestamp RF1After = 10;
}
```

---

## Engine Execution

### Engine.ExecuteSearch
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/engine.go:49`

```go
func (e *Engine) ExecuteSearch(
    ctx context.Context,
    searchReq *tempopb.SearchRequest,  // <- Has Limit, but never used
    spanSetFetcher SpansetFetcher,     // <- Interface below
    allowUnsafeQueryHints bool,
) (*tempopb.SearchResponse, error)
```

**What it reads from searchReq:**
- `searchReq.Start` (converted to StartTimeUnixNanos)
- `searchReq.End` (converted to EndTimeUnixNanos)
- `searchReq.SpansPerSpanSet` (used in SecondPass)
- Query hints
- **NOT: searchReq.Limit**

---

## Storage Layer - Fetcher Interface

### SpansetFetcher Interface
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:252-254`

```go
type SpansetFetcher interface {
    Fetch(
        context.Context,
        FetchSpansRequest,  // <- NO Limit FIELD
    ) (FetchSpansResponse, error)
}
```

### FetchSpansRequest Structure
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:81-113`

```go
type FetchSpansRequest struct {
    StartTimeUnixNanos uint64
    EndTimeUnixNanos   uint64
    Conditions         []Condition

    // Hints
    AllConditions bool

    // Sampling
    TraceSampler Sampler  // <- NOT SET FROM searchReq.Limit
    SpanSampler  Sampler  // <- NOT SET FROM searchReq.Limit

    // SecondPass filtering
    SecondPass           SecondPassFn
    SecondPassConditions []Condition
    SecondPassSelectAll  bool

    // NOTE: NO Limit FIELD
}
```

### Sampler Type (for reference)
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go`

```go
type Sampler interface {
    // Interface definition not shown, but it's for sampling traces or spans
}
```

### FetchSpansResponse
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:246-250`

```go
type FetchSpansResponse struct {
    Results SpansetIterator  // <- Iterator over Spansets
    Bytes   func() uint64    // <- Callback for bytes read
}
```

---

## Wrapper Implementation

### SpansetFetcherWrapper
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:312-324`

```go
type SpansetFetcherWrapper struct {
    f func(ctx context.Context, req FetchSpansRequest) (FetchSpansResponse, error)
}

func NewSpansetFetcherWrapper(
    f func(ctx context.Context, req FetchSpansRequest) (FetchSpansResponse, error),
) SpansetFetcher {
    return SpansetFetcherWrapper{f}
}

func (s SpansetFetcherWrapper) Fetch(
    ctx context.Context,
    request FetchSpansRequest,
) (FetchSpansResponse, error) {
    return s.f(ctx, request)  // PASS-THROUGH, NO MODIFICATIONS
}
```

**In Querier.SearchBlock, it's created as:**
```go
fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
    return q.store.Fetch(ctx, meta, req, opts)  // <- Direct call, no limit enforcement
})
```

---

## TempoDB Layer

### Reader.Fetch (TempoDB Reader interface)
**File:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:94`

```go
Fetch(
    ctx context.Context,
    meta *backend.BlockMeta,
    req traceql.FetchSpansRequest,  // <- NO LIMIT FIELD
    opts common.SearchOptions,
) (traceql.FetchSpansResponse, error)
```

### ReaderWriter.Fetch (implementation)
**File:** `/home/matt/source/tempo-mrd/tempodb/tempodb.go:541-550`

```go
func (rw *readerWriter) Fetch(
    ctx context.Context,
    meta *backend.BlockMeta,
    req traceql.FetchSpansRequest,  // <- NO LIMIT FIELD
    opts common.SearchOptions,
) (traceql.FetchSpansResponse, error) {
    block, err := encoding.OpenBlock(meta, rw.r)
    if err != nil {
        return traceql.FetchSpansResponse{}, err
    }

    rw.cfg.Search.ApplyToOptions(&opts)
    return block.Fetch(ctx, req, opts)
}
```

---

## Backend Block Layer

### BackendBlock.Fetch (Interface)
**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/common/block.go`

```go
type BackendBlock interface {
    Fetch(
        ctx context.Context,
        req traceql.FetchSpansRequest,  // <- NO LIMIT FIELD
        opts SearchOptions,
    ) (traceql.FetchSpansResponse, error)
    // ... other methods ...
}
```

### Example Implementation (vblockpack)
**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go`

```go
func (b *backendBlock) Fetch(
    ctx context.Context,
    req traceql.FetchSpansRequest,  // <- NO LIMIT FIELD
    opts common.SearchOptions,
) (traceql.FetchSpansResponse, error) {
    // Implementation must materialize ALL matching spansets
    // No limit information available
}
```

---

## Post-Processing (Ingester Path Only)

### Querier.SearchRecent
**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:357`

```go
func (q *Querier) SearchRecent(
    ctx context.Context,
    req *tempopb.SearchRequest,  // <- Contains Limit
) (*tempopb.SearchResponse, error)
```

### Querier.postProcessIngesterSearchResults
**File:** `/home/matt/source/tempo-mrd/modules/querier/querier.go:841`

```go
func (q *Querier) postProcessIngesterSearchResults(
    req *tempopb.SearchRequest,  // <- Limit is HERE
    results []any,
) *tempopb.SearchResponse
```

**This is the ONLY place where Limit is enforced:**
```go
if req.Limit != 0 && int(req.Limit) < len(response.Traces) {
    response.Traces = response.Traces[:req.Limit]
}
```

---

## Key Type Definitions

### Spanset
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:204-222`

```go
type Spanset struct {
    // these fields are actually used by the engine to evaluate queries
    Scalar Static
    Spans  []Span

    TraceID            []byte
    RootSpanName       string
    RootServiceName    string
    StartTimeUnixNanos uint64
    DurationNanos      uint64
    ServiceStats       map[string]ServiceStats
    Attributes         []*SpansetAttribute

    ReleaseFn func(*Spanset)
}
```

### SpansetIterator
**File:** `/home/matt/source/tempo-mrd/pkg/traceql/storage.go:241-244`

```go
type SpansetIterator interface {
    Next(context.Context) (*Spanset, error)
    Close()
}
```

---

## Call Chain Summary

```
SearchBlock(req: SearchBlockRequest)
  req.SearchReq.Limit = available but NOT USED

-> ExecuteSearch(searchReq: SearchRequest, fetcher: SpansetFetcher)
  searchReq.Limit = available but NOT USED
  Creates FetchSpansRequest with NO Limit field

-> fetcher.Fetch(req: FetchSpansRequest)
  req has NO Limit field

-> SpansetFetcherWrapper.Fetch(request: FetchSpansRequest)
  PASS-THROUGH: return s.f(ctx, request)

-> q.store.Fetch(ctx, meta, req, opts)
  req has NO Limit field

-> block.Fetch(ctx, req, opts)
  req has NO Limit field
  MUST materialize ALL matching spansets
```

**Result: LIMIT COMPLETELY LOST**

Compare with ingester path:

```
SearchRecent(req: SearchRequest)
  req.Limit = available

-> multiple ingesters return results

-> postProcessIngesterSearchResults(req, results)
  req.Limit = available and USED
  Enforces: response.Traces = response.Traces[:req.Limit]
```

**Result: LIMIT ENFORCED**
