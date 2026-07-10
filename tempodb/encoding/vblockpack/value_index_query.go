package vblockpack

// value_index_query.go — querier-side index-driven query wiring (blockpack issue
// #461). When configured, blockpackBlock.Fetch / QueryRange build a value-index
// source (discover + download + per-leaf predicate) and try the index path before
// falling back to a full block scan.
//
// Authoritative-index contract (SPEC-ROOT-019, NOTE-VI-047 / NOTE-VI-078): for a
// query shape the index CAN answer, its result is complete and correct — there is
// no speculative "index answered but a scan is cheaper" fallback. Fallback to a
// full scan is reserved for ROUTINE DECLINES only (a query shape the index
// architecturally cannot answer: an unindexable/negation leaf, a non-filter query,
// or no coverage). An index/data INCONSISTENCY — the index had coverage and matched
// spans but named a block/page the data file cannot resolve — is index corruption:
// it now FAILS the query (NOTE-VI-078, issue #481) instead of being masked by a
// silent scan, mirroring the trace-by-id path's NOTE-VI-071 posture.
//
// The reader is a process-level singleton, configured once at querier startup via
// ConfigureValueIndexQuery, mirroring the embedder/cache singletons. When it is
// nil (the default, value_index_query.enabled=false) every query path is
// byte-identical to before — the index path is simply skipped.

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	minio "github.com/minio/minio-go/v7"
)

// valueIndexStore is the read surface viQueryReader needs from its backing object store:
// List+Get (blockpack.LookupStore, trace-by-id) and Size+ReadAt (blockpack.ValueIndexFileStore,
// search/metrics). minioVIStore is the only production implementation; tests substitute an
// in-memory fake (see enableValueIndexForTest) to exercise the now-mandatory index path
// (NOTE-VI-073 removed GetTraceByID's no-lister scan fallback) without a live object store.
type valueIndexStore interface {
	blockpack.LookupStore
	blockpack.ValueIndexFileStore
}

// viQueryReader holds the process-level state needed to answer queries from the
// value index. The querier serves many tenants, so the file-listing cache is
// per-tenant (issue #462's IndexFileCache bakes in the tenant); reader lazily
// builds and memoises one cache per tenant. The object store is shared — it
// addresses objects by full key, which already embeds the tenant.
type viQueryReader struct {
	store       valueIndexStore
	caches      map[string]*blockpack.IndexFileCache
	indexPrefix string
	ttl         time.Duration
	mu          sync.Mutex
}

// cacheFor returns the per-tenant file-listing cache, creating (and starting the
// background refresh for) it on first use.
func (vr *viQueryReader) cacheFor(tenant string) *blockpack.IndexFileCache {
	vr.mu.Lock()
	defer vr.mu.Unlock()
	if c, ok := vr.caches[tenant]; ok {
		return c
	}
	c := blockpack.NewIndexFileCache(vr.store, tenant, vr.indexPrefix, vr.ttl)
	c.Background(context.Background())
	vr.caches[tenant] = c
	return c
}

var (
	viQueryReaderMu  sync.RWMutex
	viQueryReaderPtr *viQueryReader
)

// ConfigureValueIndexQuery installs the process-level index-driven query reader.
// Call once at querier startup. A nil client or disabled config leaves the reader unset.
// Search/metrics (tryIndexFetch) still fall back to a full block scan when unset; trace-by-id
// (FindTraceByID) does not — it requires the index unconditionally (NOTE-VI-073).
// contentCacheBytes bounds the trace-by-ID index-file content cache (blockpack
// issue #475); zero disables it, leaving the raw store (byte-identical to before).
func ConfigureValueIndexQuery(client *minio.Client, bucket, indexPrefix string, ttl time.Duration, contentCacheBytes int64) {
	viQueryReaderMu.Lock()
	defer viQueryReaderMu.Unlock()

	if client == nil {
		viQueryReaderPtr = nil
		return
	}
	if indexPrefix == "" {
		indexPrefix = "indexes"
	}
	// blockpack issue #475: the content cache is the direct mitigation for the
	// N-way redundant fetch that caused the production timeout, so it is on by
	// default. A negative value explicitly disables it (raw store, byte-identical
	// to before); zero takes the default budget.
	switch {
	case contentCacheBytes < 0:
		contentCacheBytes = 0
	case contentCacheBytes == 0:
		contentCacheBytes = defaultContentCacheBytes
	}
	// Wrap the raw store with the content cache + singleflight dedup for the
	// trace-by-ID Get path (blockpack issue #475). newCachingStore returns the raw
	// store unwrapped when contentCacheBytes <= 0.
	store := newCachingStore(&minioVIStore{client: client, bucket: bucket}, contentCacheBytes)
	viQueryReaderPtr = &viQueryReader{
		store:       store,
		caches:      make(map[string]*blockpack.IndexFileCache),
		indexPrefix: indexPrefix,
		ttl:         ttl,
	}
}

// ConfigureValueIndexQueryForTest installs store (satisfying both blockpack.LookupStore and
// blockpack.ValueIndexFileStore — the same two interfaces minioVIStore implements) as the
// process-level index-driven query reader, for tests in OTHER packages that need genuine
// CheckIndexCoverage/tryIndexFetch behavior against a real reader without a live minio client or
// S3 test harness (e.g. modules/frontend's metrics-sharder end-to-end tests, holistic-review
// Issue 2/B — proving DispatchTimeSliced is actually reachable through the real compile path,
// not just via a hand-constructed QueryPlan). Mirrors this package's own (unexported)
// withVIQueryReader test helper 1:1; exposed here only because that helper cannot be called
// across a package boundary. A nil store disables the reader, matching ConfigureValueIndexQuery's
// own nil-client contract. Returns a restore function the caller MUST defer to reset prior
// process-level state — this is a shared package-level singleton, not per-instance state.
func ConfigureValueIndexQueryForTest(store interface {
	blockpack.LookupStore
	blockpack.ValueIndexFileStore
}, indexPrefix string) (restore func()) {
	viQueryReaderMu.Lock()
	prev := viQueryReaderPtr
	if store == nil {
		viQueryReaderPtr = nil
	} else {
		viQueryReaderPtr = &viQueryReader{
			store:       store,
			caches:      make(map[string]*blockpack.IndexFileCache),
			indexPrefix: indexPrefix,
			ttl:         time.Minute,
		}
	}
	viQueryReaderMu.Unlock()
	return func() {
		viQueryReaderMu.Lock()
		viQueryReaderPtr = prev
		viQueryReaderMu.Unlock()
	}
}

// getValueIndexQueryReader returns the configured reader, or nil when the
// index-driven path is disabled.
func getValueIndexQueryReader() *viQueryReader {
	viQueryReaderMu.RLock()
	defer viQueryReaderMu.RUnlock()
	return viQueryReaderPtr
}

// indexFetchStats records the observable I/O of one index-path attempt so the
// querier can attach it to its OTel span and log line (issue #465). It is
// populated whether or not the index answered the query — a declined attempt that
// still downloaded files reports the bytes it spent before falling back.
type indexFetchStats struct {
	// FilesRead is the number of value-index files downloaded building the source.
	FilesRead int
	// BytesRead is the total byte size of those files.
	BytesRead int64
	// Hits is the number of span entries the per-column predicates matched.
	Hits int
	// Used is true when the index path answered the query (the caller skipped the
	// full block scan).
	Used bool
}

// tryIndexFetch attempts to answer a compiled filter program from the value index.
// It returns three distinct outcomes, mirroring the authoritative contract the
// blockpack executor now enforces (SPEC-ROOT-019, NOTE-VI-047 / NOTE-VI-078):
//
//   - (matches, true, stats, nil) — the index fully answered the query for this
//     block. The caller uses the result and does NOT scan.
//   - (nil, false, stats, nil) — a ROUTINE DECLINE: the index genuinely cannot
//     answer this query shape (index disabled, no coverage for a leaf, an
//     unindexable/negation predicate, a non-filter query, or a build-time
//     coverage miss). The caller falls back to a correct full block scan. This is
//     a documented, still-standing exception, not an error.
//   - (nil, false, stats, err) — an INDEX/DATA INCONSISTENCY: the index HAD
//     coverage and produced matches, but one names a block/page absent from the
//     data file. Because the value index is authoritative for the columns it
//     covers, this is index corruption, not a routine miss. The caller must FAIL
//     the query with this error rather than masking it with a silent scan — the
//     silent-scan-masks-corruption anti-pattern the trace-by-id path eliminated in
//     NOTE-VI-071. blockpack.QueryTraceQLFromIndex already surfaces this as a
//     non-nil error; tryIndexFetch now propagates it up through Fetch instead of
//     logging-and-scanning (NOTE-VI-078, issue #481).
//
// indexOnly (issue #487) narrows this contract for a time-slice job: a slice job's
// narrowed [Start, End) window means a full block scan is not a safe fallback the
// way it is for a normal query (a scan ignores the slice boundary at the per-span
// level and can double-count or over-fetch across overlapping slice jobs). When
// indexOnly is true, every ROUTINE DECLINE outcome above is converted into
// (nil, false, stats, ErrSliceIndexCoverageGap) instead of (nil, false, stats, nil)
// — the caller must fail the query, never fall through to a scan. This reuses the
// SAME outcome-3 (typed error) propagation shape NOTE-VI-078 already established;
// no new error-return convention is introduced. indexOnly has no effect on the
// other two outcomes: a full index answer is still used as-is, and a genuine
// index/data inconsistency still returns its own distinct error either way.
//
// The returned indexFetchStats captures the download I/O of this attempt
// regardless of outcome (issue #465).
// CheckIndexCoverage reports whether the configured value-index query reader (issue #461) has
// build-time coverage to attempt answering prog from the index over [minSec, maxSec] for
// tenant — issue #487's frontend-side query-planning need for a block-independent proxy of
// tryIndexFetch's OWN first decline gate (`if !ok { ...decline...}` right after
// BuildValueIndexSource, above). It is a REUSE of that exact call, not a new coverage check:
// same singleton reader, same per-tenant IndexFileCache, same blockpack.BuildValueIndexSource
// entry point tryIndexFetch itself calls.
//
// Returns false whenever the index-driven query path is disabled (viQueryReaderPtr nil, e.g.
// value_index_query.enabled=false) or BuildValueIndexSource itself errors — both mirror
// tryIndexFetch's own ROUTINE DECLINE handling for those identical conditions.
//
// FIXED (T5b/#162, was a KNOWN LIMITATION under T5/#155): BuildValueIndexSource's own ok
// return means "at least one leaf has a SHAPE the value index can represent at all"
// (equality/range/regex), not "every leaf has an indexable shape" — a genuinely
// empty-but-queried column is itself valid coverage (NOTE-VI-033: a covered-but-empty lookup is
// an authoritative "zero matches" answer, not a decline), so BuildValueIndexSource's ok alone is
// a query-SHAPE completeness gap, not a data-presence one: a query mixing one indexable leaf
// (equality/range/regex) with one leaf the index architecturally cannot represent (multi-value
// OR, negation, a RequirePresent-only leaf) would report true from ok alone, because BuildSource
// only requires ONE leaf in the whole program to have a valid shape. blockpack.AllLeavesIndexable
// (issue #487 T5b) closes exactly that gap — it aggregates the SAME per-leaf shape rule ALL, not
// ANY, and this function now requires BOTH: BuildValueIndexSource's ok (build-time
// availability/no-error) AND blockpack.AllLeavesIndexable(prog) (every leaf's shape is
// representable). Still no second, independently-invented coverage rule: AllLeavesIndexable
// itself reuses the exact same vibuilder.LeafIndexable decision buildPredicate makes for real
// index-source construction (blockpack-side, see AllLeavesIndexable's own doc comment).
//
// tryIndexFetch's SECOND decline gate — QueryTraceQLFromIndex's own indexOK, which enforces
// per-query-shape completeness against a specific block's actual data — still requires a
// *blockpack.Reader over one specific block and therefore still cannot be evaluated at the
// frontend's block-independent plan time; this function remains a reuse of tryIndexFetch's FIRST
// gate only, now tightened with the shape-completeness half tryIndexFetch's own executor-level
// check would otherwise catch downstream.
func CheckIndexCoverage(ctx context.Context, tenant string, prog *blockpack.Program, minSec, maxSec uint64) bool {
	if !blockpack.AllLeavesIndexable(prog) {
		return false
	}
	vr := getValueIndexQueryReader()
	if vr == nil {
		return false
	}
	cache := vr.cacheFor(tenant)
	watermarks := watermarksForOrNil(ctx, tenant)
	_, ok, err := blockpack.BuildValueIndexSource(ctx, cache, vr.store, prog, minSec, maxSec, watermarks)
	if err != nil {
		return false
	}
	return ok
}

// boundedAuthorized (issue #481 part 3, F-7, R7, LOCAL-DERIVATION contract ruled by R17 —
// supersedes an earlier plan-strategy-threaded design that never had a wire path from the
// frontend's QueryPlan.Strategy to this per-block call, and would have required protobuf
// generator surgery unavailable in this environment) is set true by the caller (Fetch, F-8)
// exactly when a limit is present on this query and it is not a #487 slice job — the SAME
// hasLimit predicate blockpack.SelectSearchStrategy already applies frontend-side (F-6),
// re-evaluated locally rather than threaded over the wire, so there is only ever one
// implementation of "does this query have a limit". A slice job (indexOnly) always passes
// false regardless of limit — R11-AMENDED's absolute priority. See declineOutcomeBounded's
// doc comment for the full outcome table.
func (b *blockpackBlock) tryIndexFetch(
	ctx context.Context,
	r *blockpack.Reader,
	prog *blockpack.Program,
	query string,
	opts blockpack.QueryOptions,
	indexOnly bool,
	boundedAuthorized bool,
) ([]blockpack.SpanMatch, bool, indexFetchStats, error) {
	var stats indexFetchStats
	vr := getValueIndexQueryReader()
	if vr == nil {
		// R8: the vr==nil zeroth category is UNCHANGED and untouched by boundedAuthorized —
		// see declineOutcome's own doc comment.
		return declineOutcome(indexOnly, stats)
	}

	// Derive the query's second-granularity window. A zero bound means "unbounded"
	// in QueryOptions; map that to the full uint64 range so discovery includes all
	// files (the per-file time filter then prunes by wall clock).
	minSec, maxSec := nanoWindowToSec(opts.StartNano, opts.EndNano)

	cache := vr.cacheFor(b.meta.TenantID)
	watermarks := watermarksForOrNil(ctx, b.meta.TenantID)
	src, ok, err := blockpack.BuildValueIndexSource(ctx, cache, vr.store, prog, minSec, maxSec, watermarks)
	if err != nil {
		// A build-time error is an object-store/discovery failure (List error,
		// corrupt-file decode). This is not the authoritative "index named a block
		// absent from the data file" inconsistency — the source has not yet
		// produced any span match — so it remains a routine decline (F-7: hard error
		// unless boundedAuthorized, or ErrSliceIndexCoverageGap under indexOnly).
		level.Warn(util_log.Logger).Log("msg", "vblockpack: index fetch: build source error",
			"block", b.meta.BlockID, "err", err)
		return declineOutcomeBounded(indexOnly, boundedAuthorized, stats)
	}
	if !ok {
		level.Info(util_log.Logger).Log("msg", "vblockpack: index fetch: no coverage",
			"block", b.meta.BlockID, "tenant", b.meta.TenantID, "minSec", minSec, "maxSec", maxSec)
		// #496/B1: every leaf's shape was unindexable (R3's permanent-decline case) --
		// recordUsageForDeclinedQuery's own Indexable check correctly never records
		// this, but it is still called here for the OTHER, indexable-but-uncovered
		// leaves that a mixed query might contain alongside the unindexable one.
		recordUsageForDeclinedQuery(ctx, b.meta.TenantID, prog, dedicatedColumnSet(b.meta.DedicatedColumns), time.Now())
		return declineOutcomeBounded(indexOnly, boundedAuthorized, stats)
	}
	level.Info(util_log.Logger).Log("msg", "vblockpack: index fetch: coverage found",
		"block", b.meta.BlockID, "tenant", b.meta.TenantID, "files", src.Stats().FilesRead)
	// Capture the build-time I/O even if the query later declines: those bytes were
	// spent and are worth reporting (issue #465).
	bs := src.Stats()
	stats.FilesRead = bs.FilesRead
	stats.BytesRead = bs.BytesRead
	stats.Hits = bs.Hits
	// #496/B1: NOTE-VI-033's "Add even when empty" contract means ok=true here only
	// means "at least one leaf has an indexable shape" -- it does NOT mean any VI
	// files were actually ever discovered for that leaf's column (see
	// backend_block.go's QueryRange for the fuller explanation of this same gate).
	// bs.FilesRead == 0 is the real "genuinely missing index" signal.
	if bs.FilesRead == 0 {
		recordUsageForDeclinedQuery(ctx, b.meta.TenantID, prog, dedicatedColumnSet(b.meta.DedicatedColumns), time.Now())
	}

	sourceRef := blockObjectKey(b.meta.TenantID, b.meta.BlockID.String())
	// The value index is authoritative for the columns it covers (blockpack
	// NOTE-VI-047/SPEC-ROOT-019): QueryTraceQLFromIndex does not speculatively
	// decline a large-but-correct result set. A non-nil err signals an index/data
	// inconsistency (the index named a block/page absent from the data file) — the
	// index HAD coverage and produced matches, so this is index corruption, not a
	// routine "cannot answer" miss.
	//
	// NOTE-VI-078 (issue #481): this error is now PROPAGATED to the caller so Fetch
	// FAILS the query, instead of being logged-and-masked by a silent full scan.
	// Masking corruption behind a scan is exactly the anti-pattern the trace-by-id
	// path eliminated in NOTE-VI-071 — an authoritative index that names data the
	// file cannot resolve must be observable as an error, not quietly worked around.
	// The remaining routine declines (indexOK=false, err=nil) still fall back to a
	// correct scan below (or fail with ErrSliceIndexCoverageGap under indexOnly,
	// issue #487); only genuine index/data skew becomes THIS hard error either way.
	matches, indexOK, err := blockpack.QueryTraceQLFromIndex(
		ctx, r, src, query, sourceRef, opts,
	)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "vblockpack: index fetch: index/data inconsistency, failing query (NOTE-VI-078)",
			"block", b.meta.BlockID, "tenant", b.meta.TenantID, "err", err)
		return nil, false, stats, err
	}
	if !indexOK {
		return declineOutcomeBounded(indexOnly, boundedAuthorized, stats)
	}
	stats.Used = true
	return matches, true, stats, nil
}

// declineOutcome returns tryIndexFetch's ROUTINE DECLINE outcome: today's
// (nil, false, stats, nil) when indexOnly is false, or the #487 slice-mode
// (nil, false, stats, ErrSliceIndexCoverageGap) when indexOnly is true — a slice
// job must fail rather than let its caller fall through to an unsafe full scan.
//
// R8 (issue #481 part 3): this is the UNCHANGED, UNCONDITIONAL decline path reserved for the
// vr == nil zeroth category ONLY (value_index_query.enabled=false — the index-driven path is
// disabled entirely for this querier, mirroring trace-by-id's already-settled "no index
// provided → scan is the only correct path, KEPT" category). It is deliberately NOT threaded
// through declineOutcomeBounded's boundedAuthorized gate below — vr==nil is a config-level
// absence of any index signal, never a per-query/per-block routine decline, so it is exempt from
// R7's backstop by design (F-8 keeps calling this same path for vr==nil, unchanged).
func declineOutcome(indexOnly bool, stats indexFetchStats) ([]blockpack.SpanMatch, bool, indexFetchStats, error) {
	if indexOnly {
		return nil, false, stats, ErrSliceIndexCoverageGap
	}
	return nil, false, stats, nil
}

// declineOutcomeBounded is tryIndexFetch's ROUTINE DECLINE outcome for every decline site EXCEPT
// the vr==nil zeroth category (issue #481 part 3, F-7, team-lead rulings R7/R17): boundedAuthorized
// is Fetch's LOCAL derivation (R17) from whether this query carries a limit — the frontend's
// plan-time gate (buildQueryPlanFromProgram, R6) is authoritative for query-SHAPE-driven
// rejection, but per-block declines a coarse, tenant-level VCNT classification couldn't predict
// (a query classified Selective at plan time can still decline on an individual block) are this
// function's concern, independent of the frontend's Strategy choice.
//
//   - indexOnly (unchanged, takes priority): ErrSliceIndexCoverageGap — a #487 slice job's
//     narrowed window has no safe scan fallback either way, regardless of boundedAuthorized.
//   - !indexOnly && boundedAuthorized (a limit is present on this query): (nil, false, stats,
//     nil) — relayed unchanged so Fetch's caller (F-8) routes to the bounded path instead of a
//     scan. Bounded-with-a-limit is an honest, budgeted answer by construction (R2), never a
//     wrong one, regardless of what selectivity class the frontend assigned this query.
//   - !indexOnly && !boundedAuthorized (no limit present): the search decline hard-errors
//     DIRECTLY — never an implicit scan. Absent a limit, there is no safe way to bound the read,
//     matching R6's framing that an unauthorized decline must never silently fall back to
//     "silently scan," the #481 anti-pattern this phase eliminates.
func declineOutcomeBounded(indexOnly, boundedAuthorized bool, stats indexFetchStats) ([]blockpack.SpanMatch, bool, indexFetchStats, error) {
	if indexOnly {
		return nil, false, stats, ErrSliceIndexCoverageGap
	}
	if !boundedAuthorized {
		return nil, false, stats, ErrSearchNoCoverage
	}
	return nil, false, stats, nil
}

// floorToMinuteSec floors sec down to the same 60-second (minute) alignment as
// blockpack's write-side TimeSec truncation. This is a mandatory, coordinated
// invariant, not an independent optimization: if the query side disagrees with the
// write side's granularity, a span whose minute-floored TimeSec falls before a
// non-aligned query minSec is silently dropped by LookupValue's exact inclusion
// filter (the search/metrics path), or DiscoverIndexFiles reports zero covering
// files entirely (the trace-by-id path, where a coverage gap is now a hard error —
// NOTE-VI-072/NOTE-VI-073 removed the scan fallback that used to mask this).
// TestNanoWindowToSec_MinuteFloorAlignsWithWriteSide and
// TestFindTraceByID_QueryWindowIsMinuteFloored are the regression tests for the two
// call sites (search/metrics and trace-by-id respectively).
func floorToMinuteSec(sec uint64) uint64 {
	return sec / secondsPerMinute * secondsPerMinute
}

// nanoWindowToSec converts a [startNano, endNano] window to whole seconds for
// value-index file discovery. A zero bound is treated as unbounded.
//
// maxSec needs no equivalent widening to minSec's floor: flooring only ever
// reduces TimeSec relative to the true span second, so TimeSec <= true_span_sec <=
// maxSec holds regardless of alignment (proven algebraically, not just asserted).
func nanoWindowToSec(startNano, endNano uint64) (uint64, uint64) {
	const nanosPerSec = 1_000_000_000
	minSec := uint64(0)
	if startNano > 0 {
		minSec = floorToMinuteSec(startNano / nanosPerSec)
	}
	maxSec := ^uint64(0)
	if endNano > 0 {
		maxSec = endNano / nanosPerSec
	}
	return minSec, maxSec
}

// minioVIStore satisfies blockpack.Lister (List), blockpack.ValueIndexFileStore
// (Size + ReadAt), and blockpack.LookupStore (List + Get) over a minio client. The
// search/metrics path uses Lister+ValueIndexFileStore; the trace-by-ID path (issue
// #468) uses LookupStore. The value index lives in the same bucket as the trace
// blocks.
type minioVIStore struct {
	client *minio.Client
	bucket string
}

// List returns the full object keys under prefix.
func (s *minioVIStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// mapNotFound translates a minio "no such key" / HTTP 404 into
// blockpack.ErrValueIndexFileNotFound so the builder's downloadAll can recognise a
// retention/compaction race (a file the listing cache still names but object
// storage has already deleted) and skip the file instead of failing the index
// build (blockpack issue #399 point 5). All other errors pass through unchanged so
// transient failures still abort and fall back to a correct full scan.
func mapNotFound(err error) error {
	if err == nil {
		return nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == 404 || resp.Code == "NoSuchKey" {
		return blockpack.ErrValueIndexFileNotFound
	}
	return err
}

// Get fetches the full bytes of the object at key, satisfying the fetch half of
// blockpack.LookupStore (the trace-by-ID index lookup surface, blockpack issue
// #468). Trace-by-ID index files are a single column directory's merged postings —
// small enough to read whole into memory, the same assumption Size/ReadAt make for
// the search/metrics path. A 404 maps to blockpack.ErrValueIndexFileNotFound so a
// retention/compaction race (a key the listing cache still names but object storage
// has already deleted) is treated as a benign miss, not a lookup error.
func (s *minioVIStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapNotFound(err)
	}
	defer func() { _ = obj.Close() }()
	// minio's GetObject is lazy: a 404 surfaces on the first read, not on the
	// GetObject call, so map the read error too.
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, mapNotFound(err)
	}
	return data, nil
}

// Size returns the byte length of the object at key.
func (s *minioVIStore) Size(key string) (int64, error) {
	info, err := s.client.StatObject(context.Background(), s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, mapNotFound(err)
	}
	return info.Size, nil
}

// ReadAt fills p from the object at key starting at off, following io.ReaderAt
// semantics. Value-index files are small (one column directory's merged postings),
// so a ranged GET per call is acceptable; the file-listing cache already removes
// the per-query LIST cost (issue #462).
func (s *minioVIStore) ReadAt(key string, p []byte, off int64) (int, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+int64(len(p))-1); err != nil {
		return 0, err
	}
	obj, err := s.client.GetObject(context.Background(), s.bucket, key, opts)
	if err != nil {
		return 0, mapNotFound(err)
	}
	defer func() { _ = obj.Close() }()
	// minio's GetObject is lazy: a 404 surfaces on the first read, not on the
	// GetObject call, so map the read error too.
	n, err := io.ReadFull(obj, p)
	return n, mapNotFound(err)
}
