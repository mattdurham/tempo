package tieredcache

// NOTE: TypedTieredCache implements sectioncache.SectionCache (direct typed routing,
// no key parsing). Pass it directly as Options.Cache; no filecache.Cache wrapper needed.

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/grafana/blockpack/internal/modules/sectioncache"
)

// SectionType classifies a section for sub-cache routing in TypedTieredCache.
type SectionType uint8

const (
	// SectionTypeFooter covers /footer/* and /header.
	SectionTypeFooter SectionType = iota + 1
	// SectionTypeTOC covers /v8/toc/dec and null-byte V8 column/index blobs.
	SectionTypeTOC
	// SectionTypeBloom covers /compact-header (all variants).
	SectionTypeBloom
	// SectionTypeMetadata covers /metadata/dec and /v14/sec/* generic sections.
	SectionTypeMetadata
	// SectionTypeTraceIdx covers /compact-trace-index and /compact.
	SectionTypeTraceIdx
	// SectionTypeBlockData covers /block/<decimal-integer>.
	SectionTypeBlockData
	// SectionTypeOther is reserved for future extensibility; no sub-cache is currently
	// assigned to this type. Internal use only — value is part of the TypedConfig API.
	SectionTypeOther
)

// TypedConfig holds one filecache.Cache per section type.
// Nil fields are normalized to filecache.NopCache by NewTypedTieredCache.
//
// Migration: replace tieredcache.New(meta, data) with
// tieredcache.NewTypedTieredCache(tieredcache.DefaultTypedConfig(mem, disk)).
// TieredCache (binary router) is preserved for backward compat but TypedTieredCache
// is the preferred implementation for new deployments.

// Footer caches footer variants (/footer/*) and header blobs (/header).
// Typically a small MemoryCache (footers are 18–58 bytes; effectively infinite budget).

// TOC caches V8 ToC descriptor blobs (/v8/toc/dec) and V8 column section blobs.
// Typically a medium MemoryCache (blobs are 10–50 KB each).

// Bloom caches compact-header blobs (all variants: /compact-header, /v14/compact-header).
// Each blob contains the bloom filter AND the block table for the file; actual stored
// size can exceed 15 MiB per file for large files with many blocks.
// Size this budget to at least 2 × maxBlobSize × maxConcurrentFiles to avoid
// evicting active entries during concurrent FindTraceByID workloads.

// Metadata caches /metadata/dec blobs and V14 generic sections (/v14/sec/*).
// Typically a disk-backed FileCache (~45 MB per entry; tolerate disk round-trip).

// TraceIdx caches trace index sections (/compact-trace-index, /compact).
// Typically a disk-backed FileCache (~50 MB per entry; accessed only on bloom hit).

// Block caches raw block column bytes (/block/<N>).
// Should be a MemoryCache: block reads are latency-critical (SPEC-ROOT-015).

// Registerer is an optional Prometheus registerer.
// When non-nil, TypedTieredCache metrics are registered on construction:
//   - blockpack_typed_cache_requests_total (labels: section, result)
//   - blockpack_typed_cache_fetch_duration_seconds (labels: section, result)
//
// Nil means no metrics. Consistent with filecache.Config.Registerer.

// DefaultTypedConfig returns a TypedConfig with the recommended tier mapping:
//   - hot: Footer, TOC, Bloom, Block (low-latency, high-reuse small blobs)
//   - warm: Metadata, TraceIdx (large blobs; disk round-trip acceptable)
//
// #466 removed the in-process memory tier, so both slots are typically a disk FileCache
// and/or a remote MemCache chain rather than an in-process LRU.
func DefaultTypedConfig(hot, warm filecache.Cache) TypedConfig {
	return TypedConfig{
		Footer:   hot,
		TOC:      hot,
		Bloom:    hot,
		Metadata: warm,
		TraceIdx: warm,
		Block:    hot,
	}
}

// TwoTierTypedConfig returns a TypedConfig that splits caching across two remote caches:
//   - meta: Footer, TOC, Bloom, Metadata, TraceIdx — small, high-reuse entries
//     that benefit from a shared cache with low eviction pressure (e.g. memcached-01).
//   - page: Block — large column-page blobs where a separate cache avoids evicting
//     the small metadata entries (e.g. memcached-blockpack-page-01).
//
// This mapping matches the tempo-dev-test-03 cache topology where memcached-01 is wired
// for metadata and memcached-blockpack-page-01 for page data.
func TwoTierTypedConfig(meta, page filecache.Cache) TypedConfig {
	return TypedConfig{
		Footer:   meta,
		TOC:      meta,
		Bloom:    meta,
		Metadata: meta,
		TraceIdx: meta,
		Block:    page,
	}
}

// Section index constants for sectionObs/sectionCounters arrays.
const (
	numSections = 6 // footer, toc, bloom, metadata, traceIdx, block

	idxFooter   = 0
	idxTOC      = 1
	idxBloom    = 2
	idxMetadata = 3
	idxTraceIdx = 4
	idxBlock    = 5

	idxHit   = 0
	idxMiss  = 1
	idxError = 2
)

// NOTE-LINT-407: constants for the Prometheus section/result label values (goconst).
const (
	sectionFooter   = "footer"
	sectionTOC      = "toc"
	sectionBloom    = "bloom"
	sectionMetadata = "metadata"
	sectionTraceIdx = "traceIdx"
	sectionBlock    = "block"

	resultHit   = "hit"
	resultMiss  = "miss"
	resultError = "error"

	labelSection = "section"
	labelResult  = "result"
)

// sectionLabel maps section index to the Prometheus label value.
var sectionLabel = [numSections]string{
	idxFooter:   sectionFooter,
	idxTOC:      sectionTOC,
	idxBloom:    sectionBloom,
	idxMetadata: sectionMetadata,
	idxTraceIdx: sectionTraceIdx,
	idxBlock:    sectionBlock,
}

var resultLabel = [3]string{idxHit: resultHit, idxMiss: resultMiss, idxError: resultError}

// TypedTieredCache routes cache operations to one of seven sub-caches based on
// section type. It implements sectioncache.SectionCache (typed method dispatch, no key
// parsing). Pass it directly as Options.Cache in reader.Options.
//
// Safe for concurrent use. Thread safety is delegated to sub-caches.
//
// SPEC-TC-002: each key maps to exactly one sub-cache; no key routes to two simultaneously.
// SPEC-TC-005: thread safety fully delegated to sub-caches; no mutable state post-construction.

// footer handles both footer blobs (/footer/*) and the fixed-size header blob (/header).

// sectionRequests is a pre-registered CounterVec (labels: section, result).
// nil when no Registerer was provided.

// Pre-resolved counters indexed by [sectionIdx][resultIdx]. Zero-alloc hot path.
// Nil entries when no Registerer was provided.

// Pre-resolved histogram observers indexed by [sectionIdx][resultIdx].
// Nil entries when no Registerer was provided.

// typedRegisterOrReuseCounter registers a CounterVec with the given Registerer.
// If the metric is already registered (AlreadyRegisteredError), it returns the
// previously registered collector instead of panicking.
func typedRegisterOrReuseCounter(
	reg prometheus.Registerer,
	cv *prometheus.CounterVec,
) *prometheus.CounterVec {
	if err := reg.Register(cv); err == nil {
		return cv
	} else if are := (prometheus.AlreadyRegisteredError{}); errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
			return existing
		}
	}
	return cv
}

// typedRegisterOrReuseHistogram registers a HistogramVec with the given Registerer.
// If the metric is already registered (AlreadyRegisteredError), it returns the
// previously registered collector instead of panicking.
func typedRegisterOrReuseHistogram(
	reg prometheus.Registerer,
	hv *prometheus.HistogramVec,
) *prometheus.HistogramVec {
	if err := reg.Register(hv); err == nil {
		return hv
	} else if are := (prometheus.AlreadyRegisteredError{}); errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
			return existing
		}
	}
	return hv
}

// NewTypedTieredCache constructs a TypedTieredCache from cfg.
// Nil sub-cache fields are normalized to filecache.NopCache.
func NewTypedTieredCache(cfg TypedConfig) *TypedTieredCache {
	nop := filecache.NopCache
	normalize := func(c filecache.Cache) filecache.Cache {
		if c == nil {
			return nop
		}
		return c
	}
	t := &TypedTieredCache{
		footer:   normalize(cfg.Footer),
		toc:      normalize(cfg.TOC),
		bloom:    normalize(cfg.Bloom),
		metadata: normalize(cfg.Metadata),
		traceIdx: normalize(cfg.TraceIdx),
		block:    normalize(cfg.Block),
	}

	if cfg.Registerer != nil {
		t.sectionRequests = typedRegisterOrReuseCounter(
			cfg.Registerer,
			prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "blockpack_typed_cache_requests_total",
				Help: "Total cache requests by section type and result (hit/miss).",
			}, []string{labelSection, labelResult}),
		)

		h := typedRegisterOrReuseHistogram(
			cfg.Registerer,
			prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Name:                            "blockpack_typed_cache_fetch_duration_seconds",
				Help:                            "Duration of TypedTieredCache GetOrFetch operations by section type and result.",
				NativeHistogramBucketFactor:     1.1,
				NativeHistogramMaxBucketNumber:  100,
				NativeHistogramMinResetDuration: 15 * time.Minute,
			}, []string{labelSection, labelResult}),
		)

		// Pre-resolve all 7×3 label combinations for 0-alloc hot path.
		for i := range numSections {
			for j, result := range resultLabel {
				t.sectionCounters[i][j] = t.sectionRequests.WithLabelValues(sectionLabel[i], result)
				t.sectionObs[i][j] = h.WithLabelValues(sectionLabel[i], result)
			}
		}
	}

	return t
}

// ---------------------------------------------------------------------------
// Metrics helper
// ---------------------------------------------------------------------------

// observeSection records a cache operation for the given section index.
// start is the time.Time captured at method entry (zero if metrics are disabled).
// fetchCalled is true when the fetch function was invoked (cache miss).
// err is the error returned by the operation (if any).
//
// When sectionRequests is nil (no Registerer configured), this is a no-op.
//
// Note: under singleflight, only the goroutine that actually called fetch() sets
// fetchCalled=true. Other goroutines that waited for the same in-flight request
// see fetchCalled=false (result="hit" from their perspective). This is correct:
// they did not call the underlying fetch — the data was available to them.
func (t *TypedTieredCache) observeSection(
	sectionIdx int,
	start time.Time,
	fetchCalled bool,
	err error,
) {
	if t.sectionRequests == nil {
		return
	}
	resultIdx := idxHit
	if err != nil {
		resultIdx = idxError
	} else if fetchCalled {
		resultIdx = idxMiss
	}
	if ctr := t.sectionCounters[sectionIdx][resultIdx]; ctr != nil {
		ctr.Inc()
	}
	if obs := t.sectionObs[sectionIdx][resultIdx]; obs != nil {
		obs.Observe(time.Since(start).Seconds())
	}
}

// ---------------------------------------------------------------------------
// sectioncache.SectionCache methods
// ---------------------------------------------------------------------------

// GetOrFetchFooter fetches or caches a footer blob.
func (t *TypedTieredCache) GetOrFetchFooter(
	fileID, variant string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	wrapped := func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	}
	var val []byte
	var err error
	if variant == "" {
		val, err = t.footer.GetOrFetch(fileID+"/footer", wrapped)
	} else {
		val, err = t.footer.GetOrFetch(fileID+"/footer"+variant, wrapped)
	}
	t.observeSection(idxFooter, start, fetchCalled, err)
	return val, err
}

// GetOrFetchHeader fetches or caches a header blob.
func (t *TypedTieredCache) GetOrFetchHeader(
	fileID string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	val, err := t.footer.GetOrFetch(fileID+"/header", func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	t.observeSection(idxFooter, start, fetchCalled, err)
	return val, err
}

// GetOrFetchV8TOC fetches or caches the V8 ToC descriptor blob.
func (t *TypedTieredCache) GetOrFetchV8TOC(
	fileID string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	val, err := t.toc.GetOrFetch(fileID+"/v8/toc/dec", func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	t.observeSection(idxTOC, start, fetchCalled, err)
	return val, err
}

// GetOrFetchV8Section fetches or caches a V8 per-column or per-index blob.
// NOTE-422: routes uniformly to the toc tier (routeV8) — v2 lean files carry only
// the block index and value-index sections; the retired bloom/trace subtypes that
// once routed to the bloom tier can no longer appear in any file.
func (t *TypedTieredCache) GetOrFetchV8Section(
	fileID string,
	tocType, subType uint32,
	name string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	key := sectioncache.V8SectionKeyFast(fileID, tocType, subType, name)
	fetchCalled := false
	// NOTE-422: v2 lean files carry only the block index and value-index sections in
	// their ToC (bloom/trace/traceChunked/KLL/IntrinsicTOC/SpanTree were all removed at
	// the v2 cutover). Every subType reaching this path routes to the toc tier; the old
	// ToCSubTypeBloom/ToCSubTypeTrace bloom-tier branch was dead and has been removed.
	cache, sectionIdx := t.routeV8()
	val, err := cache.GetOrFetch(key, func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	t.observeSection(sectionIdx, start, fetchCalled, err)
	return val, err
}

// sectionBatchGetter is the optional interface a sub-cache tier may implement to
// fetch many keys in one round-trip (NOTE-179). chaincache.ChainedCache provides
// it; nil/non-batch tiers fall back to per-key GetOrFetchV8Section.
type sectionBatchGetter interface {
	GetMulti(keys []string) (map[string][]byte, error)
}

// GetMultiV8Section batch-fetches V8 per-column/per-index blobs that all share the
// same (tocType, subType) routing, returning a map keyed by the input names. Names
// absent from the result missed the cache and must be fetched + Put by the caller.
// Returns (nil, false, nil) when the routed sub-cache does not support batch fetch,
// signaling the caller to fall back to per-name GetOrFetchV8Section.
//
// NOTE-179: collapses the per-column section-cache fan-out (one GetOrFetch, and
// thus one memcache connection acquisition, per column) into a single pipelined
// GetMulti — the dominant ~28%-of-CPU (*Client).dial cost on the warm read path.
func (t *TypedTieredCache) GetMultiV8Section(
	fileID string,
	tocType, subType uint32,
	names []string,
) (map[string][]byte, bool, error) {
	return batchGetV8Section(t, subType, names, func(name string) string {
		return sectioncache.V8SectionKeyFast(fileID, tocType, subType, name)
	})
}

// batchGetV8Section is the shared body of GetMultiV8Section / GetMultiV8SectionMixed
// (NOTE-337): route by subType, bail if the tier is not a sectionBatchGetter, build the
// cache keys via keyFor, GetMulti, then re-key the hits back onto the input slice by
// index (NOTE-188 — keys[i] is the full key for reqs[i], so no reverse-lookup map).
// K is the caller's per-request identity (column name, or V8SectionKey) that the
// returned map is keyed by.
func batchGetV8Section[K comparable](
	t *TypedTieredCache,
	subType uint32,
	reqs []K,
	keyFor func(K) string,
) (map[K][]byte, bool, error) {
	sub, sectionIdx := t.routeV8()
	bg, ok := sub.(sectionBatchGetter)
	if !ok {
		return nil, false, nil
	}

	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	keys := make([]string, len(reqs))
	for i, rq := range reqs {
		keys[i] = keyFor(rq)
	}
	hits, err := bg.GetMulti(keys)
	if err != nil {
		t.observeSection(sectionIdx, start, false, err)
		return nil, true, err
	}
	out := make(map[K][]byte, len(hits))
	for i, rq := range reqs {
		if val, found := hits[keys[i]]; found {
			out[rq] = val
		}
	}
	// Record one batched-fetch observation; per-key hit/miss accounting is
	// approximate at the batch level (the section metrics are coarse counters).
	t.observeSection(sectionIdx, start, false, nil)
	return out, true, nil
}

// routeV8 returns the sub-cache tier and metric index used by every V8-section
// method. NOTE-422: v2 lean files carry only the block index and value-index
// sections in their ToC; the retired bloom/trace subtypes are the only ones that
// ever routed to the bloom tier, and no file can emit them, so every V8 section
// now routes uniformly to the toc tier. Routing no longer depends on subType.
func (t *TypedTieredCache) routeV8() (filecache.Cache, int) {
	return t.toc, idxTOC
}

// GetMultiV8SectionMixed batch-fetches V8 section blobs whose keys may span
// different tocTypes, returning a map keyed by each requested V8SectionKey.
// Keys absent from the result missed the cache. Returns (nil, false, nil) when
// the sub-cache does not support batch fetch, signaling the caller to fall back
// to per-key GetOrFetchV8Section.
//
// All requested keys MUST route to the same sub-cache (same subType class); the
// caller groups them. NOTE-185: collapses the warm block-read path's two
// sequential memcache round-trips — Phase-1 ToC GetOrFetch followed by Phase-2
// column GetMulti — into ONE pipelined request. The column section keys are
// derivable from blockIdx + wanted column NAMES without first decoding the ToC,
// so the ToC key and every wanted column key can be requested together; the ToC
// blob in the result still supplies the offsets that place each column blob in
// the assembled buffer.
func (t *TypedTieredCache) GetMultiV8SectionMixed(
	fileID string,
	reqs []shared.V8SectionKey,
) (map[shared.V8SectionKey][]byte, bool, error) {
	if len(reqs) == 0 {
		return nil, true, nil
	}
	return batchGetV8Section(t, reqs[0].SubType, reqs, func(rq shared.V8SectionKey) string {
		return sectioncache.V8SectionKeyFast(fileID, rq.TocType, rq.SubType, rq.Name)
	})
}

// PutV8Section stores a single V8 per-column/per-index blob under the same key
// scheme used by GetOrFetchV8Section / GetMultiV8Section, so a batch caller can
// write back the names that missed the batch fetch (NOTE-179).
func (t *TypedTieredCache) PutV8Section(
	fileID string,
	tocType, subType uint32,
	name string,
	value []byte,
) error {
	sub, _ := t.routeV8()
	key := sectioncache.V8SectionKeyFast(fileID, tocType, subType, name)
	return sub.Put(key, value)
}

// sectionBatchPutter is the optional interface a sub-cache tier may implement to
// store many keys in one round-trip (NOTE-441). chaincache.ChainedCache provides
// it; nil/non-batch tiers fall back to per-key PutV8Section.
type sectionBatchPutter interface {
	PutMulti(items map[string][]byte) error
}

// PutMultiV8Section writes back many V8 per-column/per-index blobs that all share
// the same (tocType, subType) routing in ONE batched round-trip when the sub-cache
// supports it, returning false when it does not (the caller then falls back to
// per-name PutV8Section). NOTE-441: the write-side analog of GetMultiV8Section —
// the V8 cold-miss path resolves the columns that miss the batch GET, then writes
// them all back here in a single funneled SetMulti (sequential writes reusing one
// pooled connection) instead of one independent Set (one connection acquisition) per column.
func (t *TypedTieredCache) PutMultiV8Section(
	fileID string,
	tocType, subType uint32,
	values map[string][]byte,
) (bool, error) {
	if len(values) == 0 {
		return true, nil
	}
	sub, _ := t.routeV8()
	bp, ok := sub.(sectionBatchPutter)
	if !ok {
		return false, nil
	}
	keyed := make(map[string][]byte, len(values))
	for name, value := range values {
		keyed[sectioncache.V8SectionKeyFast(fileID, tocType, subType, name)] = value
	}
	return true, bp.PutMulti(keyed)
}

// GetOrFetchV14Section fetches or caches a V14 decompressed generic section blob.
func (t *TypedTieredCache) GetOrFetchV14Section(
	fileID string,
	sectionType uint8,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	key := fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, sectionType)
	fetchCalled := false
	val, err := t.metadata.GetOrFetch(key, func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	t.observeSection(idxMetadata, start, fetchCalled, err)
	return val, err
}

// GetV14Section probes the metadata cache without calling a fetch function.
func (t *TypedTieredCache) GetV14Section(fileID string, sectionType uint8) ([]byte, bool, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	key := fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, sectionType)
	val, ok, err := t.metadata.Get(key)
	t.observeSection(idxMetadata, start, !ok && err == nil, err)
	return val, ok, err
}

// GetOrFetchBloom fetches or caches a bloom / compact-header blob.
func (t *TypedTieredCache) GetOrFetchBloom(
	fileID string,
	isV14 bool,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	wrapped := func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	}
	var val []byte
	var err error
	if isV14 {
		val, err = t.bloom.GetOrFetch(fileID+"/v14/compact-header", wrapped)
	} else {
		val, err = t.bloom.GetOrFetch(fileID+"/compact-header", wrapped)
	}
	t.observeSection(idxBloom, start, fetchCalled, err)
	return val, err
}

// GetOrFetchMetadata fetches or caches the decoded metadata blob.
func (t *TypedTieredCache) GetOrFetchMetadata(
	fileID string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	val, err := t.metadata.GetOrFetch(fileID+"/metadata/dec", func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	t.observeSection(idxMetadata, start, fetchCalled, err)
	return val, err
}

// GetOrFetchTraceIndex fetches or caches a trace index blob.
func (t *TypedTieredCache) GetOrFetchTraceIndex(
	fileID string,
	full bool,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	wrapped := func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	}
	var val []byte
	var err error
	if full {
		val, err = t.traceIdx.GetOrFetch(fileID+"/compact", wrapped)
	} else {
		val, err = t.traceIdx.GetOrFetch(fileID+"/compact-trace-index", wrapped)
	}
	t.observeSection(idxTraceIdx, start, fetchCalled, err)
	return val, err
}

// GetBlockColumns returns cached block column bytes for blockIdx.
func (t *TypedTieredCache) GetBlockColumns(fileID string, blockIdx int) ([]byte, bool, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	// BlockColumnsKeyFast uses string concatenation + strconv.Itoa (faster than fmt.Sprintf on hot path).
	val, ok, err := t.block.Get(sectioncache.BlockColumnsKeyFast(fileID, blockIdx))
	t.observeSection(idxBlock, start, !ok && err == nil, err)
	return val, ok, err
}

// CacheBlockColumns stores block column bytes for blockIdx.
func (t *TypedTieredCache) CacheBlockColumns(fileID string, blockIdx int, data []byte) error {
	return t.block.Put(sectioncache.BlockColumnsKeyFast(fileID, blockIdx), data)
}

// Close closes all sub-caches. Deduplicates by pointer to avoid double-closing
// shared instances (e.g. DefaultTypedConfig assigns the same mem to 5 fields).
// Safe to call on a nil *TypedTieredCache.
//
// SPEC-TC-004: each unique sub-cache instance is closed exactly once.
func (t *TypedTieredCache) Close() error {
	if t == nil {
		return nil
	}
	all := []filecache.Cache{t.footer, t.toc, t.bloom, t.metadata, t.traceIdx, t.block}
	seen := make(map[filecache.Cache]struct{}, len(all))
	var errs []error
	for _, c := range all {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
