// Package sectioncache defines the typed cache interface used by the blockpack reader.
// Each method corresponds to a distinct blockpack section type, enabling independent
// cache routing, eviction budgets, and storage tiers per section type.
//
// NOTE: SectionCache is the internal interface replacing filecache.Cache on the Reader
// struct. It enables direct typed routing (no key parsing) when the reader detects an
// implementation via type assertion on opts.Cache. FilecacheAdapter bridges any
// filecache.Cache to SectionCache by reconstructing the legacy key format.
package sectioncache

import (
	"fmt"

	"github.com/grafana/blockpack/internal/modules/filecache"
)

// SectionCache is the typed cache interface used internally by Reader.
// Implementations must be safe for concurrent use.
// A nil SectionCache is NOT safe; use NopSectionCache as a no-op stand-in.
//
// SPEC-RDR-012: Reader.cache must always be a non-nil SectionCache; the Reader
// constructor normalizes nil to NopSectionCache.
// SPEC-TC-007: TypedTieredCache satisfies this interface; compile-time assertion in typed_test.go.
type SectionCache interface {
	// GetOrFetchFooter fetches or returns a cached footer blob.
	// variant is "/v78", "/v6", "/v5", "/v4", or "" (V3 bare key).
	// Adapter key: fileID + "/footer" + variant
	// Returns (nil, nil) if fetch returns (nil, nil) — treated as a miss (not stored).
	// err is non-nil only if the underlying store fails independently of fetch.
	GetOrFetchFooter(fileID, variant string, fetch func() ([]byte, error)) ([]byte, error)

	// GetOrFetchHeader fetches or returns a cached header blob (fixed 22-byte, V13+).
	// Adapter key: fileID + "/header"
	GetOrFetchHeader(fileID string, fetch func() ([]byte, error)) ([]byte, error)

	// GetOrFetchV8TOC fetches or returns a cached V8 unified ToC descriptor blob.
	// Adapter key: fileID + "/v8/toc/dec"
	GetOrFetchV8TOC(fileID string, fetch func() ([]byte, error)) ([]byte, error)

	// GetOrFetchV8Section fetches or returns a cached V8 per-column or per-index blob.
	// Adapter key: fmt.Sprintf("%s\x00v8\x00%d\x00%d\x00%s", fileID, tocType, subType, name)
	GetOrFetchV8Section(
		fileID string,
		tocType, subType uint32,
		name string,
		fetch func() ([]byte, error),
	) ([]byte, error)

	// GetOrFetchV14Section fetches or returns a cached V14 decompressed generic section blob.
	// Adapter key: fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, sectionType)
	GetOrFetchV14Section(
		fileID string,
		sectionType uint8,
		fetch func() ([]byte, error),
	) ([]byte, error)

	// GetV14Section is a non-fetch probe (opportunistic check before provider read).
	// Returns (nil, false, nil) on miss; does NOT call a fetch function.
	// Adapter key: fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, sectionType)
	GetV14Section(fileID string, sectionType uint8) ([]byte, bool, error)

	// GetOrFetchBloom fetches or returns a cached bloom / compact-header section.
	//   isV14=true  → fileID + "/v14/compact-header"
	//   isV14=false → fileID + "/compact-header"
	GetOrFetchBloom(fileID string, isV14 bool, fetch func() ([]byte, error)) ([]byte, error)

	// GetOrFetchMetadata fetches or returns a cached decoded metadata blob.
	// Adapter key: fileID + "/metadata/dec"
	GetOrFetchMetadata(fileID string, fetch func() ([]byte, error)) ([]byte, error)

	// GetOrFetchTraceIndex fetches or returns a cached trace index section.
	//   full=true  → fileID + "/compact"
	//   full=false → fileID + "/compact-trace-index"
	GetOrFetchTraceIndex(fileID string, full bool, fetch func() ([]byte, error)) ([]byte, error)

	// GetBlockColumns returns cached block column bytes for blockIdx.
	// Returns (nil, false, nil) on a miss. err is non-nil only if the store fails.
	// Adapter key: fileID + "/block/" + blockIdx (see sectioncache.BlockColumnsKey)
	GetBlockColumns(fileID string, blockIdx int) ([]byte, bool, error)

	// CacheBlockColumns stores block column bytes for blockIdx.
	// Errors are non-fatal — callers may discard them. A cache write failure does not
	// affect correctness; the next read will re-fetch from the provider.
	// Adapter key: fileID + "/block/" + blockIdx (see sectioncache.BlockColumnsKey)
	CacheBlockColumns(fileID string, blockIdx int, data []byte) error

	// GetOrFetchIntrinsic fetches or returns a cached intrinsic per-column blob.
	// Adapter key: fmt.Sprintf("%s/intrinsic/%s", fileID, name)
	GetOrFetchIntrinsic(fileID, name string, fetch func() ([]byte, error)) ([]byte, error)

	// Close releases any resources held by the cache.
	Close() error
}

// nopSectionCache is a SectionCache that never stores anything. All GetOrFetch
// calls invoke their fetch functions; Get returns (nil, false, nil); Cache is a no-op.
type nopSectionCache struct{}

// NopSectionCache is a SectionCache that never stores anything and always
// calls the fetch function. Use it as a drop-in when no cache is desired.
var NopSectionCache SectionCache = nopSectionCache{}

func (nopSectionCache) GetOrFetchFooter(_ string, _ string, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetOrFetchHeader(_ string, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetOrFetchV8TOC(_ string, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetOrFetchV8Section(
	_ string, _, _ uint32, _ string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetOrFetchV14Section(
	_ string, _ uint8,
	fetch func() ([]byte, error),
) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetV14Section(_ string, _ uint8) ([]byte, bool, error) {
	return nil, false, nil
}

func (nopSectionCache) GetOrFetchBloom(_ string, _ bool, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetOrFetchMetadata(_ string, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetOrFetchTraceIndex(_ string, _ bool, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) GetBlockColumns(_ string, _ int) ([]byte, bool, error) {
	return nil, false, nil
}

func (nopSectionCache) CacheBlockColumns(_ string, _ int, _ []byte) error {
	return nil
}

func (nopSectionCache) GetOrFetchIntrinsic(_ string, _ string, fetch func() ([]byte, error)) ([]byte, error) {
	return fetch()
}

func (nopSectionCache) Close() error {
	return nil
}

// FilecacheAdapter wraps a filecache.Cache to implement SectionCache.
// It reconstructs the legacy key format for each typed method call.
// This is the backward-compat bridge for callers using filecache.Cache.
type FilecacheAdapter struct {
	cache filecache.Cache
}

// NewFilecacheAdapter wraps c in a FilecacheAdapter.
// If c is nil, filecache.NopCache is used.
func NewFilecacheAdapter(c filecache.Cache) *FilecacheAdapter {
	if c == nil {
		c = filecache.NopCache
	}
	return &FilecacheAdapter{cache: c}
}

// GetOrFetchFooter delegates to the underlying cache using the legacy key
// fileID + "/footer" + variant. If variant is empty, the V3 bare key is used.
func (a *FilecacheAdapter) GetOrFetchFooter(
	fileID, variant string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	if variant == "" {
		return a.cache.GetOrFetch(fileID+"/footer", fetch)
	}
	return a.cache.GetOrFetch(fileID+"/footer"+variant, fetch)
}

// GetOrFetchHeader delegates using the legacy key fileID + "/header".
func (a *FilecacheAdapter) GetOrFetchHeader(
	fileID string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	return a.cache.GetOrFetch(fileID+"/header", fetch)
}

// GetOrFetchV8TOC delegates using the legacy key fileID + "/v8/toc/dec".
func (a *FilecacheAdapter) GetOrFetchV8TOC(
	fileID string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	return a.cache.GetOrFetch(fileID+"/v8/toc/dec", fetch)
}

// GetOrFetchV8Section delegates using the null-byte legacy key format.
func (a *FilecacheAdapter) GetOrFetchV8Section(
	fileID string,
	tocType, subType uint32,
	name string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	key := fmt.Sprintf("%s\x00v8\x00%d\x00%d\x00%s", fileID, tocType, subType, name)
	return a.cache.GetOrFetch(key, fetch)
}

// GetOrFetchV14Section delegates using the hex-format legacy key.
func (a *FilecacheAdapter) GetOrFetchV14Section(
	fileID string,
	sectionType uint8,
	fetch func() ([]byte, error),
) ([]byte, error) {
	key := fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, sectionType)
	return a.cache.GetOrFetch(key, fetch)
}

// GetV14Section probes the underlying cache without calling a fetch function.
func (a *FilecacheAdapter) GetV14Section(fileID string, sectionType uint8) ([]byte, bool, error) {
	key := fmt.Sprintf("%s/v14/sec/%02x/dec", fileID, sectionType)
	return a.cache.Get(key)
}

// GetOrFetchBloom delegates using the appropriate compact-header key variant.
func (a *FilecacheAdapter) GetOrFetchBloom(
	fileID string,
	isV14 bool,
	fetch func() ([]byte, error),
) ([]byte, error) {
	if isV14 {
		return a.cache.GetOrFetch(fileID+"/v14/compact-header", fetch)
	}
	return a.cache.GetOrFetch(fileID+"/compact-header", fetch)
}

// GetOrFetchMetadata delegates using the legacy key fileID + "/metadata/dec".
func (a *FilecacheAdapter) GetOrFetchMetadata(
	fileID string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	return a.cache.GetOrFetch(fileID+"/metadata/dec", fetch)
}

// GetOrFetchTraceIndex delegates using the appropriate trace index key variant.
func (a *FilecacheAdapter) GetOrFetchTraceIndex(
	fileID string,
	full bool,
	fetch func() ([]byte, error),
) ([]byte, error) {
	if full {
		return a.cache.GetOrFetch(fileID+"/compact", fetch)
	}
	return a.cache.GetOrFetch(fileID+"/compact-trace-index", fetch)
}

// GetBlockColumns probes the underlying cache for block column bytes.
func (a *FilecacheAdapter) GetBlockColumns(fileID string, blockIdx int) ([]byte, bool, error) {
	return a.cache.Get(BlockColumnsKey(fileID, blockIdx))
}

// CacheBlockColumns stores block column bytes in the underlying cache.
func (a *FilecacheAdapter) CacheBlockColumns(fileID string, blockIdx int, data []byte) error {
	return a.cache.Put(BlockColumnsKey(fileID, blockIdx), data)
}

// GetOrFetchIntrinsic delegates using the intrinsic legacy key format.
func (a *FilecacheAdapter) GetOrFetchIntrinsic(
	fileID, name string,
	fetch func() ([]byte, error),
) ([]byte, error) {
	return a.cache.GetOrFetch(IntrinsicKey(fileID, name), fetch)
}

// Close delegates to the underlying cache's Close method.
func (a *FilecacheAdapter) Close() error {
	return a.cache.Close()
}
