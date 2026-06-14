package blockpack

// reader.go — public type aliases, provider interfaces, cache configuration,
// Reader/Writer constructors, GetTraceByID, and file layout analysis.
// These are the core I/O primitives that storage backends and integrations build on.

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"runtime"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_chaincache "github.com/grafana/blockpack/internal/modules/chaincache"
	modules_filecache "github.com/grafana/blockpack/internal/modules/filecache"
	modules_memcache "github.com/grafana/blockpack/internal/modules/memcache"
	modules_memorycache "github.com/grafana/blockpack/internal/modules/memorycache"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	modules_sectioncache "github.com/grafana/blockpack/internal/modules/sectioncache"
	modules_tieredcache "github.com/grafana/blockpack/internal/modules/tieredcache"
	vm "github.com/grafana/blockpack/internal/vm"
	"golang.org/x/sync/errgroup"
)

// AGENT: Reader types - these provide access to blockpack data.
// Do not expose any internal reader implementation details.

// WantColumns controls which columns ParseBlockFromBytes eagerly decodes.
// Use WantAll() to load all columns, or WantOnly(cols) for query-plan-driven selection.
type WantColumns = modules_reader.WantColumns

// WantAll returns a WantColumns that eagerly decodes every column.
// Used by tempo's backend_block.go via the blockpack public API.
func WantAll() WantColumns { return modules_reader.WantAll() }

// WantOnly returns a WantColumns that eagerly decodes only the named columns.
// Used by tempo's backend_block.go via the blockpack public API.
func WantOnly(cols map[string]struct{}) WantColumns { return modules_reader.WantOnly(cols) }

// Reader reads modules-format blockpack files and provides query execution.
// This is a thin type alias for the internal modules reader.
type Reader = modules_reader.Reader

// Writer encodes OTLP spans into the modules blockpack format.
// This is a thin type alias for the internal modules writer.
type Writer = modules_blockio.Writer

// Block represents a decoded block of spans.
type Block = modules_reader.Block

// Column represents a decoded column.
type Column = modules_reader.Column

// DataType represents the type of data being read for caching optimization.
type DataType = modules_rw.DataType

// DataType constants for read optimization hints.
// Priority order (highest → lowest): Footer ≈ Header > Metadata ≈ TraceBloomFilter >
// TimestampIndex > Block. Caching layers use this ordering for eviction decisions.
const (
	DataTypeUnknown          = modules_rw.DataTypeUnknown
	DataTypeFooter           = modules_rw.DataTypeFooter
	DataTypeHeader           = modules_rw.DataTypeHeader
	DataTypeMetadata         = modules_rw.DataTypeMetadata
	DataTypeTraceBloomFilter = modules_rw.DataTypeTraceBloomFilter
	DataTypeTimestampIndex   = modules_rw.DataTypeTimestampIndex
	DataTypeBlock            = modules_rw.DataTypeBlock
)

// SharedLRUCache is a byte-bounded, priority-tiered LRU cache shared across readers.
// Higher-priority data types (Footer, Header, TraceBloomFilter) survive cache pressure
// from block reads and are evicted last.
type SharedLRUCache = modules_rw.SharedLRUCache

// NewSharedLRUCache creates a SharedLRUCache with the given total byte capacity.
// The cache is safe for concurrent use and can be shared across multiple readers.
func NewSharedLRUCache(maxBytes int64) *SharedLRUCache {
	return modules_rw.NewSharedLRUCache(maxBytes)
}

// NewSharedLRUProvider wraps underlying with a caching layer backed by a shared LRU cache.
// readerID uniquely identifies this reader within the cache (e.g. file path or object key).
// The returned provider satisfies ReaderProvider and is safe for concurrent use.
func NewSharedLRUProvider(underlying ReaderProvider, readerID string, cache *SharedLRUCache) ReaderProvider {
	return modules_rw.NewSharedLRUProvider(underlying, readerID, cache)
}

// Cache is the common interface for all blockpack cache tiers.
// Implementations: FileCache (disk), MemoryCache (in-process), MemCache (remote),
// ChainedCache (multi-tier). Use NewChainedCache to compose tiers:
//
//	chain := NewChainedCache(memCache, diskCache, remoteCache)
type Cache = modules_filecache.Cache

// FileCache is a disk-backed, size-bounded byte cache for blockpack file sections
// (footer, header, metadata, blocks). It deduplicates concurrent fetches for the
// same key so that many goroutines opening the same file share a single I/O.
// A nil *FileCache is safe to use; all operations become pass-throughs.
type FileCache = modules_filecache.FileCache

// FileCacheConfig configures a disk-backed FileCache.

// Registerer is an optional Prometheus registerer.
// When non-nil, cache metrics are registered and incremented on cache operations.

// Path is the directory path used for cache storage.

// MaxBytes is the maximum total bytes stored on disk.
// Oldest entries (FIFO) are evicted when the limit is exceeded.

// Enabled controls whether the cache is active.
// When false, OpenFileCache returns (nil, nil) and readers skip all caching.

// OpenFileCache opens (or creates) a FileCache with the given configuration.
// Returns (nil, nil) when cfg.Enabled is false.
// The caller must call FileCache.Close() when done.
func OpenFileCache(cfg FileCacheConfig) (*FileCache, error) {
	return modules_filecache.Open(modules_filecache.Config{
		Enabled:    cfg.Enabled,
		MaxBytes:   cfg.MaxBytes,
		Path:       cfg.Path,
		Registerer: cfg.Registerer,
	})
}

// MemoryCache is a byte-bounded in-process LRU cache that implements Cache.
// It is intended as the fastest tier in a multi-tier chain:
// MemoryCache → FileCache → MemCache.
type MemoryCache = modules_memorycache.MemoryCache

// MemoryCacheConfig configures an in-process MemoryCache.

// Registerer is an optional Prometheus registerer.
// When non-nil, cache metrics are registered and incremented on cache operations.

// MaxBytes is the maximum total bytes the cache may hold.
// Required and must be positive.

// NewMemoryCache creates an in-process LRU cache with the given byte capacity.
func NewMemoryCache(cfg MemoryCacheConfig) (*MemoryCache, error) {
	return modules_memorycache.New(modules_memorycache.Config{
		MaxBytes:   cfg.MaxBytes,
		Registerer: cfg.Registerer,
	})
}

// MemCache is a remote memcache-backed cache that implements Cache.
// It is intended as the outermost (largest) tier in a multi-tier chain.
// Keys are hashed with SHA-256 before transmission, so arbitrary-length
// blockpack cache keys are always valid memcache keys.
type MemCache = modules_memcache.MemCache

// MemCacheConfig configures a remote MemCache.

// Registerer is an optional Prometheus registerer.
// When non-nil, cache metrics are registered and incremented on cache operations.

// TierLabel overrides the "tier" Prometheus label (default "remote").
// Set distinct values when opening multiple MemCache instances with the
// same Registerer to avoid label collisions.

// Servers is the list of memcache server addresses (host:port).

// Expiration is the TTL in seconds for stored items. 0 = no expiration.

// Enabled controls whether the cache is active.
// When false, OpenMemCache returns (nil, nil).

// OpenMemCache creates a MemCache connecting to the configured servers.
// Returns (nil, nil) when cfg.Enabled is false.
// The caller must call MemCache.Close() when done.
func OpenMemCache(cfg MemCacheConfig) (*MemCache, error) {
	return modules_memcache.Open(modules_memcache.Config{
		Servers:    cfg.Servers,
		Expiration: cfg.Expiration,
		Enabled:    cfg.Enabled,
		Registerer: cfg.Registerer,
		TierLabel:  cfg.TierLabel,
	})
}

// ChainedCache is a multi-tier Cache that searches tiers in order and writes
// fetched values back to faster tiers on a hit. Use NewChainedCache to build one.
type ChainedCache = modules_chaincache.ChainedCache

// NewChainedCache creates a ChainedCache from the given tiers ordered fastest-first.
// Recommended order: MemoryCache → FileCache → MemCache.
//
// Example:
//
//	mem, _ := blockpack.NewMemoryCache(blockpack.MemoryCacheConfig{MaxBytes: 100 << 20})
//	disk, _ := blockpack.OpenFileCache(blockpack.FileCacheConfig{Path: "/tmp/bpcache", MaxBytes: 1 << 30, Enabled: true})
//	remote, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Servers: []string{"localhost:11211"}, Enabled: true})
//	chain := blockpack.NewChainedCache(mem, disk, remote)
//	reader, _ := blockpack.NewReaderWithCache(provider, fileID, chain)
func NewChainedCache(tiers ...Cache) *ChainedCache {
	return modules_chaincache.New(tiers...)
}

// TypedConfig holds one cache per section type for TypedTieredCache.
// Use DefaultTypedConfig for the recommended tier mapping, or set each field individually.
type TypedConfig = modules_tieredcache.TypedConfig

// TypedTieredCache routes cache operations to one of seven sub-caches based on
// section type (Footer, TOC, Bloom, Metadata, TraceIdx, Block, Intrinsic).
// It implements sectioncache.SectionCache via typed method dispatch — no key parsing.
// Prefer this over the deprecated NewTieredCache binary router.
type TypedTieredCache = modules_tieredcache.TypedTieredCache

// DefaultTypedConfig returns a TypedConfig with the recommended tier mapping:
//   - mem: Footer, TOC, Bloom, Block, Intrinsic (low-latency, high-reuse small blobs)
//   - disk: Metadata, TraceIdx (large blobs; disk round-trip acceptable)
//
// Example:
//
//	mem, _  := blockpack.NewMemoryCache(blockpack.MemoryCacheConfig{MaxBytes: 256 << 20})
//	disk, _ := blockpack.OpenFileCache(blockpack.FileCacheConfig{Path: "/var/cache/bp", MaxBytes: 10 << 30, Enabled: true})
//	tiered  := blockpack.NewTypedTieredCache(blockpack.DefaultTypedConfig(mem, disk))
//	reader, _ := blockpack.NewReaderWithCache(provider, fileID, tiered)
func DefaultTypedConfig(mem, disk Cache) TypedConfig {
	return modules_tieredcache.DefaultTypedConfig(mem, disk)
}

// TwoTierTypedConfig returns a TypedConfig that splits caching across two remote caches:
//   - meta: Footer, TOC, Bloom, Metadata, TraceIdx, Intrinsic — small, high-reuse entries
//     that benefit from a shared cache with low eviction pressure (e.g. memcached-01).
//   - page: Block — large column-page blobs cached separately to prevent small metadata
//     entries from being evicted by large page data (e.g. memcached-blockpack-page-01).
//
// Example:
//
//	meta, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Addresses: []string{"memcached-01:11211"}})
//	page, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Addresses: []string{"memcached-blockpack-page-01:11211"}})
//	tiered  := blockpack.NewTypedTieredCache(blockpack.TwoTierTypedConfig(meta, page))
func TwoTierTypedConfig(meta, page Cache) TypedConfig {
	return modules_tieredcache.TwoTierTypedConfig(meta, page)
}

// NewTypedTieredCache constructs a TypedTieredCache from cfg.
// Nil sub-cache fields are normalized to NopCache.
func NewTypedTieredCache(cfg TypedConfig) *TypedTieredCache {
	return modules_tieredcache.NewTypedTieredCache(cfg)
}

// SectionCache is the typed cache interface used internally by Reader.
// TypedTieredCache implements this interface. Pass a *TypedTieredCache directly
// to NewReaderWithSectionCache / NewLeanReaderWithSectionCache to bypass
// the FilecacheAdapter wrapping layer used by NewReaderWithCache.
type SectionCache = modules_sectioncache.SectionCache

// NewReaderWithSectionCache creates a Reader using a SectionCache directly.
// Use this when passing a *TypedTieredCache to avoid the FilecacheAdapter wrapper.
func NewReaderWithSectionCache(provider ReaderProvider, fileID string, sc SectionCache) (*Reader, error) {
	return modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewLeanReaderWithSectionCache creates a lean Reader using a SectionCache directly.
// Use this when passing a *TypedTieredCache to avoid the FilecacheAdapter wrapper.
func NewLeanReaderWithSectionCache(provider ReaderProvider, fileID string, sc SectionCache) (*Reader, error) {
	return modules_reader.NewLeanReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// Signal type constants for blockpack file discrimination.
// SignalTypeLog is returned by Reader.SignalType() for log blockpack files.
// SignalTypeTrace is the default for trace blockpack files (version < 12).
const (
	SignalTypeTrace = modules_shared.SignalTypeTrace
	SignalTypeLog   = modules_shared.SignalTypeLog
)

// ReaderProvider supplies random access to blockpack data.
// Implementations can use files, memory, cloud storage, etc.
type ReaderProvider = modules_rw.ReaderProvider

// CloseableReaderProvider extends ReaderProvider with resource cleanup.
// Implementations that hold open file descriptors or network connections
// should implement Close to release them.

// AGENT: Reader constructors - minimal set needed for creating readers.

// NewReaderFromProvider creates a modules-format reader from a ReaderProvider.
func NewReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	return modules_reader.NewReaderFromProvider(provider)
}

// NewReaderWithCache creates a Reader that caches footer, header, metadata, and block
// reads using the provided Cache. fileID must uniquely identify the file within the
// cache namespace — typically the file path or object storage key.
// A nil cache falls back to uncached reads. cache may be any Cache implementation:
// FileCache, MemoryCache, MemCache, or a ChainedCache.
func NewReaderWithCache(provider ReaderProvider, fileID string, cache Cache) (*Reader, error) {
	var sc modules_sectioncache.SectionCache
	if cache != nil {
		sc = modules_sectioncache.NewFilecacheAdapter(cache)
	}
	return modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewLeanReaderFromProvider creates a lean Reader optimized for GetTraceByID workloads.
// V13 files: 2 I/Os (footer + compact trace index).
// V14 files: ≥3 I/Os on open (footer + section directory + block_index); remaining sections
// are loaded lazily. Trace index deferred to first bloom hit (+1 I/O). See TestLeanReader_ThreeIO.
// Falls back to NewReaderFromProvider for files without a compact trace index (v3 footer).
func NewLeanReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	return modules_reader.NewLeanReaderFromProvider(provider)
}

// NewLeanReaderWithCache creates a lean Reader with caching. Uses the same lean path
// as NewLeanReaderFromProvider (version-dependent I/O count; see its doc for details)
// but caches footer and section reads. fileID must uniquely identify the file within the cache namespace.
// cache may be any Cache implementation: FileCache, MemoryCache, MemCache, or ChainedCache.
func NewLeanReaderWithCache(provider ReaderProvider, fileID string, cache Cache) (*Reader, error) {
	var sc modules_sectioncache.SectionCache
	if cache != nil {
		sc = modules_sectioncache.NewFilecacheAdapter(cache)
	}
	return modules_reader.NewLeanReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewReaderForProgram returns the optimal Reader for the given program.
// Programs that require column data (column predicates, streaming, vector scoring)
// get a full reader. All other programs — including nil — get a lean reader
// that reads only the compact trace index on bloom hit.
func NewReaderForProgram(prog *vm.Program, provider ReaderProvider, fileID string, cache Cache) (*Reader, error) {
	if prog.NeedsColumnData() {
		return NewReaderWithCache(provider, fileID, cache)
	}
	return NewLeanReaderWithCache(provider, fileID, cache)
}

// GetTraceByID looks up all spans for the given trace ID and returns them.
// traceIDHex must be a 32-character hex string (16 bytes); upper or lower case is accepted.
// Returns an empty slice (not an error) when the trace is not found.
// Use NewLeanReaderFromProvider for the lowest-I/O path.
func GetTraceByID(r *Reader, traceIDHex string) (results []SpanMatch, err error) {
	if r == nil {
		return nil, fmt.Errorf("GetTraceByID: reader cannot be nil")
	}

	if len(traceIDHex) != 32 {
		return nil, fmt.Errorf("GetTraceByID: traceIDHex must be 32 hex chars, got %d", len(traceIDHex))
	}

	traceIDBytes, decErr := hex.DecodeString(traceIDHex)
	if decErr != nil {
		return nil, fmt.Errorf("GetTraceByID: invalid trace ID hex: %w", decErr)
	}

	var traceID [16]byte
	copy(traceID[:], traceIDBytes)

	entries := r.TraceEntries(traceID)
	if len(entries) == 0 {
		return nil, nil
	}
	blockIDs := make([]int, len(entries))
	for i, e := range entries {
		blockIDs[i] = e.BlockID
	}

	// NOTE-293 (Lever B): resolve matching rows and span IDs from the block payloads that are
	// fetched (and decoded with WantAll) anyway. Each block carries per-row trace:id and
	// span:id columns, so the whole-file intrinsic trace:id and span:id columns — whose size
	// scales with the file's total span count, not the looked-up trace — no longer need to be
	// read on this path. The intrinsic columns are loaded only as a lazy fallback for blocks
	// whose payload lacks these columns (e.g. files written without per-block identity).
	rawMap := make(map[int][]byte, len(entries))
	for _, group := range r.CoalescedGroups(blockIDs) {
		groupRaw, fetchErr := r.ReadGroup(group)
		if fetchErr != nil {
			return nil, fmt.Errorf("GetTraceByID: read group: %w", fetchErr)
		}
		for bi, raw := range groupRaw {
			rawMap[bi] = raw
		}
	}

	// NOTE-291: parse each matching span-block concurrently (see parseMatchingBlocks).
	parsedBlocks, parseErr := parseMatchingBlocks(r, entries, rawMap)
	if parseErr != nil {
		return nil, parseErr
	}

	// rowsByBlock maps blockID → matching rowIdxs, populated from each block's own trace:id
	// column. Blocks lacking that column are recorded for the intrinsic fallback below.
	rowsByBlock := make(map[int][]int, len(entries))
	var blocksNeedingIntrinsic map[int]bool
	for i, entry := range entries {
		traceIDCol := parsedBlocks[i].Block.GetColumn("trace:id")
		if traceIDCol == nil {
			if blocksNeedingIntrinsic == nil {
				blocksNeedingIntrinsic = make(map[int]bool, len(entries))
			}
			blocksNeedingIntrinsic[entry.BlockID] = true
			continue
		}
		for rowIdx := range parsedBlocks[i].Block.SpanCount() {
			if v, ok := traceIDCol.BytesValue(rowIdx); ok && bytes.Equal(v, traceID[:]) {
				rowsByBlock[entry.BlockID] = append(rowsByBlock[entry.BlockID], rowIdx)
			}
		}
	}

	// Lazy fallback: only files whose blocks lack a trace:id column pay the whole-file
	// intrinsic reads. For current files (per-block identity present) this is never taken.
	var spanIDByRef map[uint32][]byte
	if len(blocksNeedingIntrinsic) > 0 {
		fallbackRows, fbErr := intrinsicFallbackRows(r, traceID, blocksNeedingIntrinsic)
		if fbErr != nil {
			return nil, fbErr
		}
		for bid, rows := range fallbackRows {
			rowsByBlock[bid] = append(rowsByBlock[bid], rows...)
		}
		// span:id for fallback blocks also comes from the intrinsic section.
		spanIDByRef = buildIntrinsicBytesMapForRows(r, "span:id", fallbackRows)
	}

	traceIDStr := hex.EncodeToString(traceID[:])
	for i, entry := range entries {
		bwb := parsedBlocks[i]
		for _, rowIdx := range rowsByBlock[entry.BlockID] {
			fields := modules_blockio.NewSpanFieldsAdapterWithReader(
				bwb.Block,
				r,
				entry.BlockID,
				rowIdx,
				nil,
			)
			// span:id comes from the block column; fall back to the intrinsic map only for
			// blocks resolved via the intrinsic fallback above.
			spanIDStr := ""
			if col := bwb.Block.GetColumn("span:id"); col != nil {
				if v, ok := col.BytesValue(rowIdx); ok {
					spanIDStr = hex.EncodeToString(v)
				}
			}
			if spanIDStr == "" && spanIDByRef != nil {
				key := uint32(entry.BlockID)<<16 | uint32(rowIdx) //nolint:gosec // bounded values
				if v, ok := spanIDByRef[key]; ok {
					spanIDStr = hex.EncodeToString(v)
				}
			}
			match := SpanMatch{
				Fields:  fields,
				TraceID: traceIDStr,
				SpanID:  spanIDStr,
			}
			results = append(results, match.Clone())
			modules_blockio.ReleaseSpanFieldsAdapter(fields)
		}
	}

	return results, nil
}

// intrinsicFallbackRows loads the whole-file intrinsic trace:id column and returns the rows
// matching traceID, scoped to wantBlocks. It is the NOTE-293 fallback for blocks whose payload
// lacks a per-block trace:id column; current files never reach it.
func intrinsicFallbackRows(r *Reader, traceID [16]byte, wantBlocks map[int]bool) (map[int][]int, error) {
	if tocErr := r.EnsureIntrinsicTOC(); tocErr != nil {
		return nil, fmt.Errorf("GetTraceByID: load intrinsic TOC: %w", tocErr)
	}
	col, traceColErr := r.GetIntrinsicColumn("trace:id")
	if traceColErr != nil {
		return nil, fmt.Errorf("GetTraceByID: load intrinsic trace:id: %w", traceColErr)
	}
	rows := make(map[int][]int)
	if col == nil {
		return rows, nil
	}
	for i, ref := range col.BlockRefs {
		if !wantBlocks[int(ref.BlockIdx)] {
			continue
		}
		if i < len(col.BytesValues) && bytes.Equal(col.BytesValues[i], traceID[:]) {
			rows[int(ref.BlockIdx)] = append(rows[int(ref.BlockIdx)], int(ref.RowIdx))
		}
	}
	return rows, nil
}

// parseMatchingBlocks decodes each matching span-block concurrently (NOTE-291). Each
// ParseBlockFromBytes call decodes an independent input blob (rawMap[entry.BlockID]) into
// an independent output Block — there is no shared mutable state between calls
// (ParseBlockFromBytes allocates a per-call intern map and reads the Reader's
// pre-decoded/pre-compressed lookups under their own mutexes). A trace that spans N
// span-blocks within one file previously paid N×decode serially; decoding them in parallel
// reduces that to ~max.
//
// Results are returned in a slice indexed by entry position so the caller's row-emission
// loop stays sequential and order-preserving. The fan-out is bounded by
// traceByIDParseConcurrency so a single trace-by-ID call cannot saturate every core and
// starve concurrent metrics queries on the same querier. Each goroutine writes only its own
// slot, and all writes happen-before the gParse.Wait() return — no data race.
func parseMatchingBlocks(
	r *Reader,
	entries []modules_reader.TraceEntry,
	rawMap map[int][]byte,
) ([]*modules_reader.BlockWithBytes, error) {
	parsedBlocks := make([]*modules_reader.BlockWithBytes, len(entries))
	var gParse errgroup.Group
	gParse.SetLimit(traceByIDParseConcurrency())
	for i, entry := range entries {
		i, entry := i, entry
		raw, ok := rawMap[entry.BlockID]
		if !ok {
			return nil, fmt.Errorf("GetTraceByID: block %d missing from coalesced read", entry.BlockID)
		}
		gParse.Go(func() error {
			bwb, blockErr := r.ParseBlockFromBytes(raw, modules_reader.WantAll(), r.BlockMeta(entry.BlockID))
			if blockErr != nil {
				return fmt.Errorf("GetTraceByID: block %d: %w", entry.BlockID, blockErr)
			}
			parsedBlocks[i] = bwb
			return nil
		})
	}
	if waitErr := gParse.Wait(); waitErr != nil {
		return nil, waitErr
	}
	return parsedBlocks, nil
}

// traceByIDParseConcurrency bounds the number of span-blocks GetTraceByID decodes
// concurrently (NOTE-291). Trace-by-ID is an interactive, relatively rare lookup that
// shares the querier with background metrics scans, so the per-trace decode fan-out is
// capped to a small fraction of available cores. The cap scales with GOMAXPROCS but is
// clamped to [2, 4]: 2 guarantees within-file parallelism even on small queriers, and 4
// caps the CPU a single trace-by-ID call can claim so it cannot starve concurrent
// metrics queries. A trace spanning fewer blocks than the cap simply uses fewer workers.
func traceByIDParseConcurrency() int {
	const (
		minLimit = 2
		maxLimit = 4
	)
	n := runtime.GOMAXPROCS(0) / 2
	if n < minLimit {
		n = minLimit
	}
	if n > maxLimit {
		n = maxLimit
	}
	return n
}

// AGENT: Writer constructors - minimal set needed for creating writers.

// DedicatedColumn describes an attribute column to be stored in the intrinsic section
// in addition to the standard block columns. Dedicated columns enable the zero-block-read
// fast path for metrics queries that filter or group-by on them.
// Name must be the full blockpack column name including prefix, e.g. "span.http.method".
type DedicatedColumn = modules_blockio.DedicatedColumn

// WriterConfig configures a blockpack Writer.
// It is a type alias for the internal WriterConfig.
type WriterConfig = modules_blockio.WriterConfig

// NewWriter creates a streaming modules-format blockpack writer that writes to output.
// maxSpansPerBlock controls block granularity (0 uses the default of 2000).
func NewWriter(output io.Writer, maxSpansPerBlock int) (*Writer, error) {
	return modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  output,
		MaxBlockSpans: maxSpansPerBlock,
	})
}

// NewWriterWithConfig creates a Writer with full configuration control.
// Use this when VectorDimension or other advanced settings are needed.
func NewWriterWithConfig(cfg WriterConfig) (*Writer, error) {
	return modules_blockio.NewWriterWithConfig(cfg)
}

// FileLayoutReport describes the byte-level structure of a blockpack file.
type FileLayoutReport = modules_reader.FileLayoutReport

// FileLayoutSection describes one contiguous byte range in a blockpack file.
type FileLayoutSection = modules_reader.FileLayoutSection

// AnalyzeFileLayout inspects every byte in a blockpack file and returns a
// JSON-serializable report organized by section. Each entry carries its
// absolute offset, on-disk (compressed) size, column name, column type, and
// encoding kind for column data blobs.
//
// The returned Sections slice is sorted by Offset ascending and satisfies the
// invariant: sum(section.CompressedSize) == FileLayoutReport.FileSize.
func AnalyzeFileLayout(r *Reader) (report *FileLayoutReport, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			report = nil
			err = fmt.Errorf("internal error in AnalyzeFileLayout: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("AnalyzeFileLayout: reader cannot be nil")
	}

	return r.FileLayout()
}

// ClearReaderCaches resets all process-level reader caches (metadata, sketch, intrinsic).
// Intended for test isolation.
func ClearReaderCaches() {
	modules_reader.ClearCaches()
}
