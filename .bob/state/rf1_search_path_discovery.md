# Discovery: RF=1 Block Search Path Investigation

**Goal**: Understand how RF=1 backend blocks are routed through the frontend and querier, and whether `blockpackBlock.Fetch` is called for them.

## Key Finding

**Both RF=0 and RF=1 backend blocks take the SAME search path through `SearchBlock` RPC, which calls `tempodb.Fetch` on the querier side.** The `rf1FilterFn` only filters which blocks are selected, not how they are queried.

---

## 1. Frontend Block Selection (RF=1 Filtering)

### Location: `/home/matt/source/tempo-mrd/modules/frontend/util.go:29-38`

```go
func rf1FilterFn(rf1After time.Time) func(m *backend.BlockMeta) bool {
	return func(m *backend.BlockMeta) bool {
		if rf1After.IsZero() {
			return m.ReplicationFactor == backend.DefaultReplicationFactor
		}

		return (m.ReplicationFactor == backend.DefaultReplicationFactor && m.StartTime.Before(rf1After)) ||
			(m.ReplicationFactor == backend.MetricsGeneratorReplicationFactor && m.StartTime.After(rf1After))
	}
}
```

### Constants: `/home/matt/source/tempo-mrd/tempodb/backend/block_meta.go:135-137`

```go
DefaultReplicationFactor          = 0 // Replication factor for blocks from the ingester. This is the default value to indicate RF3.
MetricsGeneratorReplicationFactor = 1
LiveStoreReplicationFactor        = MetricsGeneratorReplicationFactor
```

### How It Works

**When `rf1After` is set to a timestamp (e.g., `1970-01-01T00:00:00Z`):**

The filter returns blocks matching this condition:
```
(RF==0 AND StartTime < rf1After) OR (RF==1 AND StartTime > rf1After)
```

This means:
- **RF=0 blocks** (old backend blocks): Selected if their start time is BEFORE the cutoff
- **RF=1 blocks** (generator/recent blocks): Selected if their start time is AFTER the cutoff

**Example from test** (`TestRF1After` in `/home/matt/source/tempo-mrd/modules/frontend/search_sharder_test.go:1149-1179`):

With `rf1After=90 seconds` and blocks at times 100 and 200:
- Block RF=0, StartTime=100 → `100 < 90` = NO ❌
- Block RF=1, StartTime=100 → `100 > 90` = YES ✓
- Block RF=0, StartTime=200 → `200 < 90` = NO ❌
- Block RF=1, StartTime=200 → `200 > 90` = YES ✓

Result: Only RF=1 blocks are selected (2 blocks)

---

## 2. Backend Request Building (Single Code Path)

### Location: `/home/matt/source/tempo-mrd/modules/frontend/search_sharder.go:304-354`

Function `buildBackendRequests()` is called for ALL selected blocks (both RF=0 and RF=1):

```go
func buildBackendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest, firstShardIdx int, blockIter func(shardIterFn, jobIterFn), reqCh chan<- pipeline.Request, errFn func(error)) {
	defer close(reqCh)

	blockIter(nil, func(m *backend.BlockMeta, shard, startPage, pages int) {
		blockID := m.BlockID.String()

		// Build SearchBlockRequest (same for RF=0 and RF=1)
		pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
			r, err = api.BuildSearchBlockRequest(r, &tempopb.SearchBlockRequest{
				BlockID:       blockID,
				StartPage:     uint32(startPage),
				PagesToSearch: uint32(pages),
				IndexPageSize: m.IndexPageSize,
				TotalRecords:  m.TotalRecords,
				Version:       m.Version,
				Size_:         m.Size_,
				FooterSize:    m.FooterSize,
			}, dedColsJSON)
			return r, err
		})

		// Send to reqCh for querier processing
		select {
		case reqCh <- pipelineR:
		case <-ctx.Done():
			return
		}
	})
}
```

### Key Point
- **No replication factor check** at this stage
- **No separate code path** for RF=1 blocks
- Both RF=0 and RF=1 blocks receive `SearchBlockRequest`

---

## 3. Block Selection Flow

### Location: `/home/matt/source/tempo-mrd/modules/frontend/search_sharder.go:136-183`

The `backendRequests()` function filters blocks using `rf1FilterFn`:

```go
func (s *asyncSearchSharder) backendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest, resp *combiner.SearchJobResponse, reqCh chan<- pipeline.Request, errFn func(error)) {

	// Lines 157-163: Apply RF filtering
	rf1After := searchReq.RF1After
	if rf1After.IsZero() {
		rf1After = s.cfg.RF1After
	}

	blocks := blockMetasForSearch(s.reader.BlockMetas(tenantID), startT, endT, rf1FilterFn(rf1After))

	// Then buildBackendRequests() sends all filtered blocks to querier via SearchBlockRequest
	go func() {
		buildBackendRequests(ctx, tenantID, parent, searchReq, firstShardIdx, blockIter, reqCh, errFn)
	}()
}
```

### Helper Function: `/home/matt/source/tempo-mrd/modules/frontend/frontend.go:371-394`

```go
func blockMetasForSearch(allBlocks []*backend.BlockMeta, start, end time.Time, filterFn func(m *backend.BlockMeta) bool) []*backend.BlockMeta {
	blocks := make([]*backend.BlockMeta, 0, len(allBlocks)/50)
	for _, m := range allBlocks {
		if !m.StartTime.After(end) && !m.EndTime.Before(start) && filterFn(m) {
			blocks = append(blocks, m)
		}
	}
	// Sort backwards in time
	sort.Slice(blocks, func(i, j int) bool {
		if !blocks[i].EndTime.Equal(blocks[j].EndTime) {
			return blocks[i].EndTime.After(blocks[j].EndTime)
		}
		return blocks[i].BlockID.String() < blocks[j].BlockID.String()
	})
	return blocks
}
```

---

## 4. Querier Side: SearchBlock RPC Handler

### Location: `/home/matt/source/tempo-mrd/modules/querier/querier.go:615-657`

```go
// SearchBlock searches the specified subset of the block for the passed tags.
func (q *Querier) SearchBlock(ctx context.Context, req *tempopb.SearchBlockRequest) (*tempopb.SearchResponse, error) {
	tenantID, err := validation.ExtractValidTenantID(ctx)
	if err != nil {
		return nil, err
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
		// LINE 649-651: THE CRITICAL CALL
		fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
			return q.store.Fetch(ctx, meta, req, opts)  // <-- CALLS tempodb.Fetch()
		})

		return q.engine.ExecuteSearch(ctx, req.SearchReq, fetcher, q.limits.UnsafeQueryHints(tenantID))
	}

	return q.store.Search(ctx, meta, req.SearchReq, opts)
}
```

### Key Observation
- **No replication factor check** in `SearchBlock`
- **Both RF=0 and RF=1 blocks call** `q.store.Fetch()`
- The `store` is the same `storage.Store` instance for all blocks

---

## 5. Query Path for RF=1 Blocks

### Handlers: `/home/matt/source/tempo-mrd/modules/querier/http.go:130-170`

```go
func (q *Querier) SearchHandler(w http.ResponseWriter, r *http.Request) {
	isSearchBlock := api.IsSearchBlock(r)

	if !isSearchBlock {
		// Ingester/recent data path
		req, err := api.ParseSearchRequest(r)
		resp, err = q.SearchRecent(ctx, req)
	} else {
		// Backend block path (for both RF=0 and RF=1)
		req, err := api.ParseSearchBlockRequest(r)
		span.SetAttributes(attribute.String("SearchRequestBlock", req.String()))

		resp, err = q.SearchBlock(ctx, req)  // <-- SAME HANDLER
		if err != nil {
			handleError(w, err)
			return
		}
	}
}
```

---

## 6. Storage Store Implementation

### Location: `/home/matt/source/tempo-mrd/modules/querier/querier.go:56-116`

```go
type Querier struct {
	services.Service

	cfg Config

	liveStorePool *ring_client.Pool
	partitionRing *ring.PartitionInstanceRing

	engine *traceql.Engine
	store  storage.Store  // <-- Single storage.Store for all backends
	limits overrides.Interface

	externalClient *external.Client

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}
```

The querier has:
- A single `store` of type `storage.Store` (tempodb)
- A separate `liveStorePool` for querying recent live data

**But RF=1 blocks don't bypass the store** — they go through the standard `SearchBlock` path which calls `store.Fetch()` or `store.Search()`.

---

## 7. No Live-Store Bypass for RF=1 Backend Blocks

### Evidence

**Search in querier for replication factor checks:**

```bash
$ grep -rn "ReplicationFactor" modules/querier/
# Result: No matches
```

The querier **does not check replication factor** to decide routing. RF=1 blocks are still backend blocks, not live-store data. They go through the standard `SearchBlock` path.

---

## 8. Metrics Query Path (Different from Search)

For metrics queries (not search), there IS a separate path for RF=1 blocks:

### Location: `/home/matt/source/tempo-mrd/modules/frontend/metrics_query_range_sharder.go:182-251`

```go
func (s *queryRangeSharder) backendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq tempopb.QueryRangeRequest, cutoff time.Time, targetBytesPerRequest int, reqCh chan pipeline.Request, jobMetadata *combiner.QueryRangeJobResponse) {

	// Line 205-207: ONLY queries RF=1 blocks
	blocks := blockMetasForSearch(s.reader.BlockMetas(tenantID), start, end, func(m *backend.BlockMeta) bool {
		return m.ReplicationFactor == backend.MetricsGeneratorReplicationFactor
	})

	if len(blocks) == 0 {
		close(reqCh)
		return
	}

	// Builds QueryRangeRequest (not SearchBlockRequest)
	s.buildBackendRequests(ctx, tenantID, parent, backendReq, firstShardIdx, blockIter, reqCh, getExemplarsForBlock)
}
```

**But this is for metrics queries**, not trace search. It includes block metadata fields like:
- `BlockID`
- `StartPage`
- `PagesToSearch`
- `Version`

These are passed to the querier as part of the `QueryRangeRequest`.

---

## Answers to Your Questions

### Q1: Where is `rf1FilterFn` used?
**A:** In `/home/matt/source/tempo-mrd/modules/frontend/search_sharder.go:163` — called during `backendRequests()` to filter which blocks are selected for backend querying.

### Q2: Does it separate blocks into two different querier paths?
**A:** No. Both RF=0 and RF=1 blocks are sent to the same `SearchBlock` RPC handler. The filtering only decides which blocks to query, not how they are queried.

### Q3: What happens to RF=1 backend blocks vs RF=0 blocks?
**A:** Both receive `SearchBlockRequest` and are handled identically by the querier's `SearchBlock()` method.

### Q4: Are RF=1 backend blocks queried via a different API?
**A:** No. Both use the standard `SearchBlock` RPC (not a live-store query). The live-store data is queried via `SearchRecent()` in the ingester path, not backend blocks.

### Q5: Are there separate sharder types for RF=0 vs RF=1?
**A:** No. The `asyncSearchSharder` handles both. For metrics queries, there's a `queryRangeSharder`, but it still uses the same request pattern.

### Q6: What RPC does the frontend make for RF=1 backend blocks?
**A:** `SearchBlock` (via `api.BuildSearchBlockRequest`), same as RF=0 blocks.

### Q7: Does `tempodb.Fetch` get called for RF=1 blocks?
**A:** **YES**. In `/home/matt/source/tempo-mrd/modules/querier/querier.go:650`, all `SearchBlock` requests (whether the block is RF=0 or RF=1) call `q.store.Fetch()`, which invokes `tempodb.Fetch()` on the backend block.

---

## Summary

**The `rf1FilterFn` only controls block SELECTION, not block QUERYING.**

- **RF=1 filtering**: Temporal cutoff-based selection in the frontend
- **RF=1 querying**: Same `SearchBlock` RPC path as RF=0 blocks
- **tempodb.Fetch called**: YES, for both RF=0 and RF=1 backend blocks
- **Replication factor never checked**: In querier or store layer

If blockpack blocks (RF=1) are not being fetched, the issue is likely in:
1. The block selection filter logic (rf1FilterFn)
2. The BlockMeta metadata construction (missing ReplicationFactor or StartTime)
3. The vblockpack backend block's Fetch implementation itself

Not in the routing logic.
