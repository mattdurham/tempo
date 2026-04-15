package blockpack

// reader.go — public type aliases, provider interfaces, cache configuration,
// Reader/Writer constructors, GetTraceByID, and file layout analysis.
// These are the core I/O primitives that storage backends and integrations build on.

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/prometheus/client_golang/prometheus"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_chaincache "github.com/grafana/blockpack/internal/modules/chaincache"
	modules_filecache "github.com/grafana/blockpack/internal/modules/filecache"
	modules_memcache "github.com/grafana/blockpack/internal/modules/memcache"
	modules_memorycache "github.com/grafana/blockpack/internal/modules/memorycache"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	vm "github.com/grafana/blockpack/internal/vm"
)

// AGENT: Reader types - these provide access to blockpack data.
// Do not expose any internal reader implementation details.

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
type FileCacheConfig struct {
	// Registerer is an optional Prometheus registerer.
	// When non-nil, cache metrics are registered and incremented on cache operations.
	Registerer prometheus.Registerer
	// Path is the directory path used for cache storage.
	Path string

	// MaxBytes is the maximum total bytes stored on disk.
	// Oldest entries (FIFO) are evicted when the limit is exceeded.
	MaxBytes int64

	// Enabled controls whether the cache is active.
	// When false, OpenFileCache returns (nil, nil) and readers skip all caching.
	Enabled bool
}

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
type MemoryCacheConfig struct {
	// Registerer is an optional Prometheus registerer.
	// When non-nil, cache metrics are registered and incremented on cache operations.
	Registerer prometheus.Registerer
	// MaxBytes is the maximum total bytes the cache may hold.
	// Required and must be positive.
	MaxBytes int64
}

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
type MemCacheConfig struct {
	// Registerer is an optional Prometheus registerer.
	// When non-nil, cache metrics are registered and incremented on cache operations.
	Registerer prometheus.Registerer
	// Servers is the list of memcache server addresses (host:port).
	Servers []string

	// Expiration is the TTL in seconds for stored items. 0 = no expiration.
	Expiration int32

	// Enabled controls whether the cache is active.
	// When false, OpenMemCache returns (nil, nil).
	Enabled bool
}

// OpenMemCache creates a MemCache connecting to the configured servers.
// Returns (nil, nil) when cfg.Enabled is false.
// The caller must call MemCache.Close() when done.
func OpenMemCache(cfg MemCacheConfig) (*MemCache, error) {
	return modules_memcache.Open(modules_memcache.Config{
		Servers:    cfg.Servers,
		Expiration: cfg.Expiration,
		Enabled:    cfg.Enabled,
		Registerer: cfg.Registerer,
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
type CloseableReaderProvider interface {
	ReaderProvider
	Close() error
}

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
	return modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  cache,
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
	return modules_reader.NewLeanReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  cache,
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
	// matchingBlockSet is built here and reused below to filter intrinsic column scans.
	// This prevents O(all-blocks) intrinsic column traversal when the trace exists in
	// only a small fraction of blocks.
	matchingBlockSet := make(map[int]bool, len(entries))
	for i, e := range entries {
		blockIDs[i] = e.BlockID
		matchingBlockSet[e.BlockID] = true
	}
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

	// Ensure the intrinsic TOC is loaded — lean readers skip this at open time.
	// Required because trace:id is now stored exclusively in the intrinsic section.
	if err := r.EnsureIntrinsicTOC(); err != nil {
		return nil, fmt.Errorf("GetTraceByID: load intrinsic TOC: %w", err)
	}

	// Build intrinsic trace:id lookup to find (blockID, rowIdx) pairs.
	// After dual-storage removal, trace:id is only in the intrinsic section.
	intrinsicTraceCol, traceColErr := r.GetIntrinsicColumn("trace:id")
	if traceColErr != nil {
		return nil, fmt.Errorf("GetTraceByID: load intrinsic trace:id: %w", traceColErr)
	}

	// rowsByBlock maps blockID → []rowIdx for rows matching traceID.
	// Only entries in matchingBlockSet are considered, scoping the scan to relevant blocks.
	rowsByBlock := make(map[int][]int)
	if intrinsicTraceCol != nil {
		for i, ref := range intrinsicTraceCol.BlockRefs {
			if !matchingBlockSet[int(ref.BlockIdx)] {
				continue // skip blocks that don't contain this trace
			}
			if i < len(intrinsicTraceCol.BytesValues) && bytes.Equal(intrinsicTraceCol.BytesValues[i], traceID[:]) {
				rowsByBlock[int(ref.BlockIdx)] = append(rowsByBlock[int(ref.BlockIdx)], int(ref.RowIdx))
			}
		}
	}

	// Fall back to scanning trace:id block columns for legacy files that still have them.
	// This handles files written before the dual-storage removal.
	useLegacyScan := len(rowsByBlock) == 0 && intrinsicTraceCol == nil

	// Pre-build intrinsic span:id map, scoped to rows in rowsByBlock.
	// Using the filtered variant avoids scanning span:id entries for all blocks
	// when only a small subset of blocks match the trace.
	spanIDByRef := buildIntrinsicBytesMapForRows(r, "span:id", rowsByBlock)

	for _, entry := range entries {
		raw, ok := rawMap[entry.BlockID]
		if !ok {
			return nil, fmt.Errorf("GetTraceByID: block %d missing from coalesced read", entry.BlockID)
		}
		bwb, blockErr := r.ParseBlockFromBytes(raw, nil, r.BlockMeta(entry.BlockID))
		if blockErr != nil {
			return nil, fmt.Errorf("GetTraceByID: block %d: %w", entry.BlockID, blockErr)
		}

		var matchingRows []int
		if useLegacyScan {
			// Legacy path: scan trace:id block column for matching rows.
			traceIDCol := bwb.Block.GetColumn("trace:id")
			if traceIDCol == nil {
				continue
			}
			for rowIdx := range bwb.Block.SpanCount() {
				v, ok2 := traceIDCol.BytesValue(rowIdx)
				if ok2 && bytes.Equal(v, traceID[:]) {
					matchingRows = append(matchingRows, rowIdx)
				}
			}
		} else {
			matchingRows = rowsByBlock[entry.BlockID]
		}

		for _, rowIdx := range matchingRows {
			fields := modules_blockio.NewSpanFieldsAdapterWithReader(bwb.Block, r, entry.BlockID, rowIdx)
			// trace:id is known; extract span:id via intrinsic fallback map.
			traceIDStr := hex.EncodeToString(traceID[:])
			spanIDStr := ""
			key := uint32(entry.BlockID)<<16 | uint32(rowIdx) //nolint:gosec // bounded values
			if v, ok2 := spanIDByRef[key]; ok2 {
				spanIDStr = hex.EncodeToString(v)
			} else if col := bwb.Block.GetColumn("span:id"); col != nil {
				if v, ok2 := col.BytesValue(rowIdx); ok2 {
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

// AGENT: Writer constructors - minimal set needed for creating writers.

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
