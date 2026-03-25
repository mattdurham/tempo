# Discovery: Parquet Reader State Caching vs Blockpack

## Executive Summary

Parquet encoding (vparquet3/4/5) **does NOT cache reader state** at the block level. Each request (FindTraceByID, Search, Fetch) creates a fresh `parquet.File` object from scratch. Blockpack, conversely, creates fresh readers per request but leverages blockpack's **DefaultProvider range caching** for repeated reads within a single request (e.g., footer, header, indexes read multiple times are cached in memory).

Both encodings pool span/spanset/event/link objects to avoid allocation overhead, but neither maintains persistent reader state across requests.

---

## Part 1: Parquet Reader Lifecycle (vparquet5, representative of 3/4)

### Block Structure

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block.go` (lines 19-27)

```go
type backendBlock struct {
	meta *backend.BlockMeta
	r    backend.Reader

	openMtx sync.Mutex
}
```

**Key observation:** Only `meta`, `r` (backend.Reader), and `openMtx` are stored. **No parquet.File object is cached.** The `openMtx` mutex protects concurrent calls to `openForSearch`, but does NOT serialize reader creation — multiple concurrent requests each get their own `parquet.File` instance.

### Reader Creation: openForSearch()

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_search.go` (lines 56-90)

```go
func (b *backendBlock) openForSearch(ctx context.Context, opts common.SearchOptions) (*parquet.File, *BackendReaderAt, error) {
	b.openMtx.Lock()
	defer b.openMtx.Unlock()

	// TODO: ctx is also cached when we cache backendReaderAt, not ideal but leaving it as is for now
	backendReaderAt := NewBackendReaderAt(ctx, b.r, DataFileName, b.meta)

	schema, _, _ := SchemaWithDynamicChanges(b.meta.DedicatedColumns)

	o := []parquet.FileOption{
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(true),
		parquet.FileReadMode(parquet.ReadModeAsync),
		parquet.FileSchema(schema),
	}

	readBufferSize := opts.ReadBufferSize
	if readBufferSize <= 0 {
		readBufferSize = parquet.DefaultFileConfig().ReadBufferSize
	}
	o = append(o, parquet.ReadBufferSize(readBufferSize))

	// cached reader
	cachedReaderAt := newCachedReaderAt(backendReaderAt, readBufferSize, int64(b.meta.Size_), b.meta.FooterSize)

	_, span := tracer.Start(ctx, "parquet.OpenFile")
	defer span.End()
	pf, err := parquet.OpenFile(cachedReaderAt, int64(b.meta.Size_), o...)

	return pf, backendReaderAt, err
}
```

**Critical insight:**
- **New BackendReaderAt created per call** — not reused
- **New cachedReaderAt created per call** — not cached at block level
- **New parquet.File created per call** — stateless between requests
- `openMtx` is just a mutex guard but doesn't prevent concurrent `openForSearch` calls that each get different `parquet.File` instances

### Request Flow Example: FindTraceByID

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_findtracebyid.go` (lines 103-144)

```go
func (b *backendBlock) FindTraceByID(ctx context.Context, traceID common.ID, opts common.SearchOptions) (_ *tempopb.TraceByIDResponse, err error) {
	derivedCtx, span := tracer.Start(ctx, "parquet.backendBlock.FindTraceByID", ...)
	defer span.End()

	found, err := b.checkBloom(derivedCtx, traceID)
	if err != nil || !found {
		return nil, err
	}

	ok, rowGroup, err := b.checkIndex(derivedCtx, traceID)
	if err != nil || !ok {
		return nil, err
	}

	pf, rr, err := b.openForSearch(derivedCtx, opts)  // Creates NEW parquet.File
	if err != nil {
		return nil, fmt.Errorf("unexpected error opening parquet file: %w", err)
	}

	foundTrace, err := findTraceByID(derivedCtx, traceID, b.meta, pf, rowGroup)  // Uses pf

	result := &tempopb.TraceByIDResponse{
		Trace:   foundTrace,
		Metrics: &tempopb.TraceByIDMetrics{},
	}
	bytesRead := rr.BytesRead()
	result.Metrics.InspectedBytes += bytesRead
	span.SetAttributes(attribute.Int64("inspectedBytes", int64(bytesRead)))

	return result, err
}
```

**Observation:** `pf` is a local variable, not stored on the block. It's discarded after the request completes. The parquet reader is NOT kept open.

### Request Flow Example: Search

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_search.go` (lines 92-119)

```go
func (b *backendBlock) Search(ctx context.Context, req *tempopb.SearchRequest, opts common.SearchOptions) (_ *tempopb.SearchResponse, err error) {
	derivedCtx, span := tracer.Start(ctx, "parquet.backendBlock.Search", ...)
	defer span.End()

	pf, rr, err := b.openForSearch(derivedCtx, opts)  // Creates NEW parquet.File
	if err != nil {
		return nil, fmt.Errorf("unexpected error opening parquet file: %w", err)
	}
	defer func() { span.SetAttributes(attribute.Int64("inspectedBytes", int64(rr.BytesRead()))) }()

	rgs := rowGroupsFromFile(pf, opts)
	results, err := searchParquetFile(derivedCtx, pf, req, rgs, b.meta.DedicatedColumns)
	if err != nil {
		return nil, err
	}
	results.Metrics.InspectedBytes += rr.BytesRead()
	results.Metrics.InspectedTraces += uint32(b.meta.TotalObjects)

	return results, nil
}
```

Same pattern: fresh `parquet.File` per request.

### Request Flow Example: Fetch

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_traceql.go` (lines 1032-1056)

```go
func (b *backendBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	err := checkConditions(req.Conditions)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("conditions invalid: %w", err)
	}

	coalesceConditions(&req)

	pf, rr, err := b.openForSearch(ctx, opts)  // Creates NEW parquet.File
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}

	rgs := rowGroupsFromFile(pf, opts)

	iter, err := fetch(ctx, req, pf, rgs, b.meta.DedicatedColumns)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("creating fetch iter: %w", err)
	}

	return traceql.FetchSpansResponse{
		Results: iter,
		Bytes:   func() uint64 { return rr.BytesRead() },
	}, nil
}
```

Pattern: new `parquet.File` per request.

---

## Part 2: Parquet Caching Strategy — Per-Request, Not Block-Level

### 1. BackendReaderAt: Per-Request Byte-Range Tracking

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/readers.go` (lines 22-55)

```go
type BackendReaderAt struct {
	ctx  context.Context
	r    backend.Reader
	name string
	meta *backend.BlockMeta

	bytesRead atomic.Uint64
}

func NewBackendReaderAt(ctx context.Context, r backend.Reader, name string, meta *backend.BlockMeta) *BackendReaderAt {
	return &BackendReaderAt{ctx, r, name, meta, atomic.Uint64{}}
}

func (b *BackendReaderAt) ReadAtWithCache(p []byte, off int64, role cache.Role) (int, error) {
	err := b.r.ReadRange(b.ctx, b.name, (uuid.UUID)(b.meta.BlockID), b.meta.TenantID, uint64(off), p, &backend.CacheInfo{
		Role: role,
		Meta: b.meta,
	})
	if isFatalError(err) {
		return 0, err
	}
	b.bytesRead.Add(uint64(len(p)))
	return len(p), err
}
```

**Purpose:** Wraps `backend.Reader` calls with **byte-range metrics tracking** and cache role hints (`cache.RoleParquetFooter`, `cache.RoleParquetPage`, etc.). But this is **NOT a cache** — it delegates to the backend reader (likely S3 or filesystem), which may have its own caching layer.

### 2. cachedReaderAt: Per-Request Parquet Metadata Caching

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/readers.go` (lines 64-122)

```go
type cachedReaderAt struct {
	r             cacheReaderAt
	cachedObjects map[int64]cachedObjectRecord // storing offsets and length of objects we want to cache

	readerSize int64
	footerSize uint32

	maxPageSize int
}

func newCachedReaderAt(r cacheReaderAt, maxPageSize int, size int64, footerSize uint32) *cachedReaderAt {
	return &cachedReaderAt{r, map[int64]cachedObjectRecord{}, size, footerSize, maxPageSize}
}

// called by parquet-go in OpenFile() to set offset and length of footer section
func (r *cachedReaderAt) SetFooterSection(offset, length int64) {
	r.cachedObjects[offset] = cachedObjectRecord{length, cache.RoleParquetFooter}
}

// called by parquet-go in OpenFile() to set offset and length of column indexes
func (r *cachedReaderAt) SetColumnIndexSection(offset, length int64) {
	r.cachedObjects[offset] = cachedObjectRecord{length, cache.RoleParquetColumnIdx}
}

// called by parquet-go in OpenFile() to set offset and length of offset index section
func (r *cachedReaderAt) SetOffsetIndexSection(offset, length int64) {
	r.cachedObjects[offset] = cachedObjectRecord{length, cache.RoleParquetOffsetIdx}
}

func (r *cachedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 4 && off == 0 {
		// Magic header
		return copy(p, []byte("PAR1")), nil
	}

	if len(p) == 8 && off == r.readerSize-8 && r.footerSize > 0 {
		// Magic footer
		binary.LittleEndian.PutUint32(p, r.footerSize)
		copy(p[4:8], []byte("PAR1"))
		return 8, nil
	}

	// check if the offset and length is stored as a special object
	rec, ok := r.cachedObjects[off]
	if ok && rec.length == int64(len(p)) {
		return r.r.ReadAtWithCache(p, off, rec.role)
	}

	if len(p) <= r.maxPageSize {
		return r.r.ReadAtWithCache(p, off, cache.RoleParquetPage)
	}

	return r.r.ReadAtWithCache(p, off, cache.RoleNone)
}
```

**Purpose:**
- Records which byte ranges are footer, column index, offset index (called by parquet-go during `OpenFile`)
- Routes reads to the backend with the **correct cache role hint**
- The actual caching (in-memory storage) is **delegated to the backend.Reader** (e.g., distributed cache like Memcached) — NOT done locally
- **New instance per request** — `cachedObjects` map is rebuilt each time `newCachedReaderAt` is called

### 3. Span/Spanset/Event/Link Object Pools

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_traceql.go` (lines 24-45)

```go
var (
	// Pool capacities are tuned to the typical number of entries collected at each level.
	// Starting with sufficient capacity avoids makeslice calls when collectInternal
	// appends column values into the IteratorResult.Entries slice.
	pqSpanPool            = parquetquery.NewResultPool(16) // spans collect ~10-20 column values
	pqSpansetPool         = parquetquery.NewResultPool(4)
	pqTracePool           = parquetquery.NewResultPool(4)
	pqAttrPool            = parquetquery.NewResultPool(4)  // key + value + type = ~4 entries
	pqEventPool           = parquetquery.NewResultPool(4)
	pqLinkPool            = parquetquery.NewResultPool(4)
	pqInstrumentationPool = parquetquery.NewResultPool(4)

	intervalMapper15Seconds   = traceql.NewIntervalMapper(...)
	intervalMapper60Seconds   = traceql.NewIntervalMapper(...)
	intervalMapper300Seconds  = traceql.NewIntervalMapper(...)
	intervalMapper3600Seconds = traceql.NewIntervalMapper(...)
)

type span struct {
	spanAttrs            []attrVal
	resourceAttrs        []attrVal
	// ... more fields
}

var spanPool = sync.Pool{
	New: func() interface{} {
		return &span{
			spanAttrs:        make([]attrVal, 0, 16),
			resourceAttrs:    make([]attrVal, 0, 16),
			// ...
		}
	},
}

var spansetPool = sync.Pool{}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &event{attrs: make([]attrVal, 0, 4)}
	},
}

var linkPool = sync.Pool{
	New: func() interface{} {
		return &link{attrs: make([]attrVal, 0, 4)}
	},
}
```

**Purpose:** **Object pooling to avoid allocations**, not reader caching. Reused across all requests to the block. These are **allocation pools**, not **state caches**.

### 4. Row Pool (Compactor)

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/compactor.go`

```go
var pool = newRowPool(1_000_000)
```

**Purpose:** Reuses row buffer objects during compaction. Also allocation pooling, not reader state caching.

---

## Part 3: Blockpack Reader Caching Strategy

### Block Structure

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go` (lines 60-71)

```go
type blockpackBlock struct {
	meta   *backend.BlockMeta
	reader backend.Reader
}

func newBackendBlock(meta *backend.BlockMeta, r backend.Reader) *blockpackBlock {
	return &blockpackBlock{
		meta:   meta,
		reader: r,
	}
}
```

**Key difference from parquet:** Only stores `meta` and `reader`. **No blockpack reader object cached either.**

### Per-Request Reader Provider with Range Caching

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go` (lines 22-85)

```go
type tempoReaderProvider struct {
	reader    backend.Reader
	tenantID  string
	blockID   uuid.UUID
	knownSize int64 // populated from BlockMeta.Size_; avoids a full S3 download
}

func (p *tempoReaderProvider) Size() (int64, error) {
	if p.knownSize > 0 {
		return p.knownSize, nil
	}
	// Fallback: stream to get size (expensive — always populate knownSize at construction time).
	rc, size, err := p.reader.StreamReader(context.Background(), DataFileName, p.blockID, p.tenantID)
	if err != nil {
		return 0, err
	}
	rc.Close()
	return size, nil
}

func (p *tempoReaderProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	err := p.reader.ReadRange(context.Background(), DataFileName, p.blockID, p.tenantID, uint64(off), buf, nil)
	if err != nil {
		size, sizeErr := p.Size()
		if sizeErr == nil && off >= size {
			return 0, io.EOF
		}
		return 0, err
	}
	return len(buf), nil
}

func (b *blockpackBlock) newReaderProvider() *blockpack.DefaultProvider {
	raw := &tempoReaderProvider{
		reader:    b.reader,
		tenantID:  b.meta.TenantID,
		blockID:   uuid.UUID(b.meta.BlockID),
		knownSize: int64(b.meta.Size_),
	}
	return blockpack.NewDefaultProvider(raw)
}
```

**Key features:**
- `tempoReaderProvider` is **stateless** — delegates all reads to `backend.Reader`
- `blockpack.NewDefaultProvider(raw)` wraps the provider with **per-request range caching**
  - **Comment (line 74-76):** "The DefaultProvider wraps tempoReaderProvider with per-request range caching: repeated reads of the same byte range (footer, header, index) are served from memory without extra I/O."
  - This is **blockpack library's own caching**, not Tempo's
- `knownSize` optimization: avoids redundant S3 calls to get file size (takes size from BlockMeta)

### Request Flow Example: FindTraceByID

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go` (lines 92-125)

```go
func (b *blockpackBlock) FindTraceByID(_ context.Context, id common.ID, _ common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
	if len(id) != 16 {
		return nil, fmt.Errorf("trace ID must be 16 bytes, got %d", len(id))
	}

	r, err := blockpack.NewLeanReaderFromProvider(b.newReaderProvider())  // Creates NEW provider (with range cache)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack reader: %w", err)
	}

	traceIDHex := hex.EncodeToString(id)
	var matches []blockpack.SpanMatch
	if err := blockpack.GetTraceByID(r, traceIDHex, func(match *blockpack.SpanMatch, more bool) bool {
		if !more {
			return false
		}
		matches = append(matches, match.Clone())
		return true
	}); err != nil {
		return nil, fmt.Errorf("GetTraceByID: %w", err)
	}

	if len(matches) == 0 {
		return nil, nil
	}

	trace, err := reconstructTrace(id, matches)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct trace: %w", err)
	}

	return &tempopb.TraceByIDResponse{Trace: trace}, nil
}
```

**Blockpack difference:**
- Creates a **LeanReader** from the provider (minimal I/O path)
- The **DefaultProvider's range cache** (in blockpack library) caches repeated reads within the LeanReader's lifetime
- After the callback completes, the provider/reader is discarded (no block-level cache)

### Request Flow Example: Fetch

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go` (lines 270-390)

```go
func (b *blockpackBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	query := conditionsToTraceQL(req.Conditions, req.AllConditions)

	r, err := blockpack.NewReaderFromProvider(b.newReaderProvider())  // Creates NEW provider (with range cache)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("vblockpack Fetch: open reader: %w", err)
	}

	// ... rest of implementation uses r
}
```

Same pattern: fresh provider/reader per request.

---

## Part 4: Key Differences

| Aspect | Parquet (vparquet5) | Blockpack (vblockpack) |
|--------|------------------|------------------------|
| **Block struct fields** | `meta`, `r`, `openMtx` | `meta`, `reader` |
| **Reader cached?** | No — new `parquet.File` per request | No — new `blockpack.Reader` per request |
| **Per-request caching** | `cachedReaderAt` tracks footer/index/column-index metadata, delegates to backend | `blockpack.DefaultProvider` (blockpack library) range-caches footer/index reads |
| **Where cache lives** | Backend reader (S3/distributed cache) via cache role hints | Blockpack library's DefaultProvider (in-memory, per-request) |
| **Mutex usage** | `openMtx` in block struct — protects `openForSearch` | None — provider is stateless |
| **Bloom filter caching** | Fetched per block.checkBloom() — cached at backend level | Not checked in FindTraceByID (blockpack does internally?) |
| **Index caching** | `checkIndex()` fetches and unmarshals index per request | Blockpack library handles internally |
| **Object pooling** | Yes — span, spanset, event, link pools (`sync.Pool`) | Not visible in backend_block (blockpack library may use) |
| **LeanReader vs FullReader** | Uses full parquet.File (async read mode) | Uses LeanReader for minimal I/O; FullReader for queries |

---

## Part 5: Lean Reader Pattern (Blockpack Advantage)

Blockpack has a **LeanReader** concept (`blockpack.NewLeanReaderFromProvider`) used in `FindTraceByID`:

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go` (line 98)

```go
r, err := blockpack.NewLeanReaderFromProvider(b.newReaderProvider())
```

**What is LeanReader?**
- A minimal reader that only reads the blockpack index/metadata, not all column data
- Used for finding a trace by ID — fetches only the requested trace's spans
- Part of blockpack's public API (from `github.com/grafana/blockpack`)
- More I/O-efficient than opening a full parquet.File which requires reading all column structures

**Parquet equivalent:** Would be to skip bloom/page-index and use row-group binary search:

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_findtracebyid.go` (lines 154-231)

```go
// If no index then fallback to binary searching the rowgroups.
if rowGroup == -1 {
	var (
		numRowGroups = len(pf.RowGroups())
		buf          = make(parquet.Row, 1)
		err          error
	)

	// Cache of row group bounds
	rowGroupMins := make([]common.ID, numRowGroups+1)
	// ... binary search logic
}
```

But parquet still needs to `parquet.OpenFile(cachedReaderAt, ...)` upfront to get the row groups, which reads the full footer and schema.

---

## Part 6: Bloom Filter Handling

### Parquet: Bloom filters read per query

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_findtracebyid.go` (lines 36-63)

```go
func (b *backendBlock) checkBloom(ctx context.Context, id common.ID) (found bool, err error) {
	// ... tracer setup

	shardKey := common.ShardKeyForTraceID(id, int(b.meta.BloomShardCount))
	nameBloom := common.BloomName(shardKey)
	span.SetAttributes(attribute.String("bloom", nameBloom))

	bloomBytes, err := b.r.Read(derivedCtx, nameBloom, (uuid.UUID)(b.meta.BlockID), b.meta.TenantID, &backend.CacheInfo{
		Meta: b.meta,
		Role: cache.RoleBloom,
	})
	if err != nil {
		return false, fmt.Errorf("error retrieving bloom %s (%s, %s): %w", nameBloom, b.meta.TenantID, b.meta.BlockID, err)
	}

	filter := &bloom.BloomFilter{}
	_, err = filter.ReadFrom(bytes.NewReader(bloomBytes))
	if err != nil {
		return false, fmt.Errorf("error parsing bloom (%s, %s): %w", b.meta.TenantID, b.meta.BlockID, err)
	}

	return filter.Test(id), nil
}
```

**Caching:** Delegated to `backend.Reader` with `cache.RoleBloom` hint. Each query fetches from backend (which may be cached there).

### Blockpack: No bloom check in FindTraceByID

**File:** `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go` (lines 92-125)

No `checkBloom` call. Blockpack's `blockpack.GetTraceByID` likely has its own bloom filter internally.

---

## Part 7: Summary: No Block-Level Reader Caching

**Both parquet and blockpack follow this pattern:**

1. **Block struct** holds only metadata and backend reader — **no stateful cache**
2. **Per-request lifecycle:**
   - Create fresh reader/provider
   - Apply range caching (blockpack.DefaultProvider or backend reader)
   - Conduct the query
   - Discard reader
3. **Object pooling** for frequently allocated types (spans, spans, events, links) to reduce GC pressure
4. **No "lean reader" equivalent in parquet** — always opens full `parquet.File` which is heavier

### Why not cache readers?

1. **Concurrency safety** — parquet.File and blockpack.Reader may not be safe for concurrent use
2. **Memory efficiency** — readers hold file handles; caching would increase memory usage
3. **Simplicity** — fresh reader per request is easier to reason about
4. **Backend caching** — the backend.Reader itself (S3, local FS) may cache blocks, roles, pages
5. **Per-request optimization** — blockpack.DefaultProvider range-caches within a request, which is where most repeated reads happen (footer/index read multiple times)

---

## Key Files Reference

| Path | Purpose |
|------|---------|
| `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block.go` | backendBlock struct (no reader caching) |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_search.go:57` | `openForSearch()` — creates fresh parquet.File per call |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_findtracebyid.go:103` | `FindTraceByID()` — uses fresh parquet.File |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/block_traceql.go:1032` | `Fetch()` — uses fresh parquet.File |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vparquet5/readers.go` | BackendReaderAt, cachedReaderAt (per-request wrappers) |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go:60` | blockpackBlock struct (no reader caching) |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go:77` | `newReaderProvider()` — creates fresh provider per call |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go:92` | `FindTraceByID()` — uses LeanReader (minimal I/O) |
| `/home/matt/source/tempo-mrd/tempodb/encoding/vblockpack/backend_block.go:270` | `Fetch()` — uses full Reader |
