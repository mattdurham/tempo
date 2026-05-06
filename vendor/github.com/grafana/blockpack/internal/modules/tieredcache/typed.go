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
	// SectionTypeIntrinsic covers /intrinsic/*.
	SectionTypeIntrinsic
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
type TypedConfig struct {
	// Footer caches footer variants (/footer/*) and header blobs (/header).
	// Typically a small MemoryCache (footers are 18–58 bytes; effectively infinite budget).
	Footer filecache.Cache
	// TOC caches V8 ToC descriptor blobs (/v8/toc/dec) and V8 column section blobs.
	// Typically a medium MemoryCache (blobs are 10–50 KB each).
	TOC filecache.Cache
	// Bloom caches compact-header blobs (all variants: /compact-header, /v14/compact-header).
	// Each blob contains the bloom filter AND the block table for the file; actual stored
	// size can exceed 15 MiB per file for large files with many blocks.
	// Size this budget to at least 2 × maxBlobSize × maxConcurrentFiles to avoid
	// evicting active entries during concurrent FindTraceByID workloads.
	Bloom filecache.Cache
	// Metadata caches /metadata/dec blobs and V14 generic sections (/v14/sec/*).
	// Typically a disk-backed FileCache (~45 MB per entry; tolerate disk round-trip).
	Metadata filecache.Cache
	// TraceIdx caches trace index sections (/compact-trace-index, /compact).
	// Typically a disk-backed FileCache (~50 MB per entry; accessed only on bloom hit).
	TraceIdx filecache.Cache
	// Block caches raw block column bytes (/block/<N>).
	// Should be a MemoryCache: block reads are latency-critical (SPEC-ROOT-015).
	Block filecache.Cache
	// Intrinsic caches intrinsic per-column blobs (/intrinsic/<name>).
	// Typically a MemoryCache (variable size; isolated to avoid evicting footer/bloom).
	Intrinsic filecache.Cache
	// Registerer is an optional Prometheus registerer.
	// When non-nil, TypedTieredCache metrics are registered on construction:
	//   - blockpack_typed_cache_requests_total (labels: section, result)
	//   - blockpack_typed_cache_fetch_duration_seconds (labels: section, result)
	//
	// Nil means no metrics. Consistent with filecache.Config.Registerer.
	Registerer prometheus.Registerer
}

// DefaultTypedConfig returns a TypedConfig with the recommended tier mapping:
//   - mem: Footer, TOC, Bloom, Block, Intrinsic (low-latency, high-reuse)
//   - disk: Metadata, TraceIdx (large blobs; disk round-trip acceptable)
//
// IMPORTANT: Bloom compact-header blobs can exceed 15 MiB per file. Size the mem budget
// to at least 2 × maxBlobSize × maxConcurrentFiles to avoid evicting active entries.
func DefaultTypedConfig(mem, disk filecache.Cache) TypedConfig {
	return TypedConfig{
		Footer:    mem,
		TOC:       mem,
		Bloom:     mem,
		Metadata:  disk,
		TraceIdx:  disk,
		Block:     mem,
		Intrinsic: mem,
	}
}

// Section index constants for sectionObs/sectionCounters arrays.
const (
	numSections = 7 // footer, toc, bloom, metadata, traceIdx, block, intrinsic

	idxFooter    = 0
	idxTOC       = 1
	idxBloom     = 2
	idxMetadata  = 3
	idxTraceIdx  = 4
	idxBlock     = 5
	idxIntrinsic = 6

	idxHit   = 0
	idxMiss  = 1
	idxError = 2
)

// sectionLabel maps section index to the Prometheus label value.
var sectionLabel = [numSections]string{
	idxFooter:    "footer",
	idxTOC:       "toc",
	idxBloom:     "bloom",
	idxMetadata:  "metadata",
	idxTraceIdx:  "traceIdx",
	idxBlock:     "block",
	idxIntrinsic: "intrinsic",
}

var resultLabel = [3]string{idxHit: "hit", idxMiss: "miss", idxError: "error"}

// TypedTieredCache routes cache operations to one of seven sub-caches based on
// section type. It implements sectioncache.SectionCache (typed method dispatch, no key
// parsing). Pass it directly as Options.Cache in reader.Options.
//
// Safe for concurrent use. Thread safety is delegated to sub-caches.
//
// SPEC-TC-002: each key maps to exactly one sub-cache; no key routes to two simultaneously.
// SPEC-TC-005: thread safety fully delegated to sub-caches; no mutable state post-construction.
type TypedTieredCache struct {
	// footer handles both footer blobs (/footer/*) and the fixed-size header blob (/header).
	footer    filecache.Cache
	toc       filecache.Cache
	bloom     filecache.Cache
	metadata  filecache.Cache
	traceIdx  filecache.Cache
	block     filecache.Cache
	intrinsic filecache.Cache
	// sectionRequests is a pre-registered CounterVec (labels: section, result).
	// nil when no Registerer was provided.
	sectionRequests *prometheus.CounterVec
	// Pre-resolved counters indexed by [sectionIdx][resultIdx]. Zero-alloc hot path.
	// Nil entries when no Registerer was provided.
	sectionCounters [numSections][3]prometheus.Counter
	// Pre-resolved histogram observers indexed by [sectionIdx][resultIdx].
	// Nil entries when no Registerer was provided.
	sectionObs [numSections][3]prometheus.Observer
}

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
		footer:    normalize(cfg.Footer),
		toc:       normalize(cfg.TOC),
		bloom:     normalize(cfg.Bloom),
		metadata:  normalize(cfg.Metadata),
		traceIdx:  normalize(cfg.TraceIdx),
		block:     normalize(cfg.Block),
		intrinsic: normalize(cfg.Intrinsic),
	}

	if cfg.Registerer != nil {
		t.sectionRequests = typedRegisterOrReuseCounter(
			cfg.Registerer,
			prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "blockpack_typed_cache_requests_total",
				Help: "Total cache requests by section type and result (hit/miss).",
			}, []string{"section", "result"}),
		)

		h := typedRegisterOrReuseHistogram(
			cfg.Registerer,
			prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Name:                            "blockpack_typed_cache_fetch_duration_seconds",
				Help:                            "Duration of TypedTieredCache GetOrFetch operations by section type and result.",
				NativeHistogramBucketFactor:     1.1,
				NativeHistogramMaxBucketNumber:  100,
				NativeHistogramMinResetDuration: 15 * time.Minute,
			}, []string{"section", "result"}),
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
// Routes to the appropriate sub-cache based on subType:
//   - ToCSubTypeBloom, ToCSubTypeTrace → bloom (compact/trace bloom data)
//   - ToCSubTypeIntrinsic              → intrinsic
//   - all others                       → toc
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
	key := fmt.Sprintf("%s\x00v8\x00%d\x00%d\x00%s", fileID, tocType, subType, name)
	fetchCalled := false
	var val []byte
	var err error
	var sectionIdx int
	switch subType {
	case shared.ToCSubTypeBloom, shared.ToCSubTypeTrace:
		sectionIdx = idxBloom
		val, err = t.bloom.GetOrFetch(key, func() ([]byte, error) {
			fetchCalled = true
			return fetch()
		})
	case shared.ToCSubTypeIntrinsic:
		sectionIdx = idxIntrinsic
		val, err = t.intrinsic.GetOrFetch(key, func() ([]byte, error) {
			fetchCalled = true
			return fetch()
		})
	default:
		sectionIdx = idxTOC
		val, err = t.toc.GetOrFetch(key, func() ([]byte, error) {
			fetchCalled = true
			return fetch()
		})
	}
	t.observeSection(sectionIdx, start, fetchCalled, err)
	return val, err
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

// GetOrFetchIntrinsic fetches or caches an intrinsic per-column blob.
func (t *TypedTieredCache) GetOrFetchIntrinsic(
	fileID, name string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	var start time.Time
	if t.sectionRequests != nil {
		start = time.Now()
	}
	fetchCalled := false
	val, err := t.intrinsic.GetOrFetch(sectioncache.IntrinsicKey(fileID, name), func() ([]byte, error) {
		fetchCalled = true
		return fetch()
	})
	t.observeSection(idxIntrinsic, start, fetchCalled, err)
	return val, err
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
	all := []filecache.Cache{t.footer, t.toc, t.bloom, t.metadata, t.traceIdx, t.block, t.intrinsic}
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
