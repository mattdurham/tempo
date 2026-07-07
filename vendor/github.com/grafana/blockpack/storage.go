package blockpack

// storage.go — Storage interface, file/MinIO backends, BlockMeta, FilePlan,
// compaction, and OTLP proto conversion. These are the persistence and
// planning primitives used by query orchestrators and ingest pipelines.

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	minio "github.com/minio/minio-go/v7"

	modules_compaction "github.com/grafana/blockpack/internal/modules/blockio/compaction"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	modules_sectioncache "github.com/grafana/blockpack/internal/modules/sectioncache"
	"github.com/grafana/blockpack/internal/s3provider"
)

// AGENT: Storage interfaces - minimal abstraction for storage backends.

// Storage provides access to blockpack files for query execution.
// It exposes object storage primitives: Size and ReadAt.
type Storage interface {
	// Size returns the total size of the object at the given path.
	Size(path string) (int64, error)

	// ReadAt reads len(p) bytes from the object at path starting at offset off.
	//
	// The dataType parameter provides a hint about the type of data being read
	// (footer, header, metadata, block, index, or compact). Storage implementations
	// can use this hint to optimize caching strategies, TTLs, or read-ahead behavior.
	// Simple implementations that don't perform caching can ignore this parameter.
	//
	// Implementations MUST follow io.ReaderAt semantics for correct interoperability:
	//   - off must be non-negative. Return an error (e.g., os.ErrInvalid) if off < 0.
	//   - If off >= Size(path), ReadAt must return (0, io.EOF).
	//   - Any short read (where n < len(p)) MUST return a non-nil error. At end
	//     of file that error MUST be io.EOF.
	//   - ReadAt may return (len(p), nil) only when the buffer is completely filled.
	//
	// It returns the number of bytes read and any error encountered.
	ReadAt(path string, p []byte, off int64, dataType DataType) (int, error)
}

// WritableStorage extends Storage with write and delete capability.

// Put writes data to the given path, creating or overwriting the file.

// Delete removes the file at the given path.

func (p *storageReaderProvider) Size() (int64, error) {
	return p.storage.Size(p.path)
}

func (p *storageReaderProvider) ReadAt(buf []byte, off int64, dataType modules_rw.DataType) (int, error) {
	return p.storage.ReadAt(p.path, buf, off, dataType)
}

// NewFileStorage creates a filesystem-based WritableStorage rooted at baseDir.
func NewFileStorage(baseDir string) WritableStorage {
	return &folderStorageWrapper{baseDir: baseDir}
}

// folderStorageWrapper implements WritableStorage using the local filesystem.

// safePath joins baseDir and path and verifies the result stays within baseDir.
// It returns an error for absolute paths, paths with ".." components that escape
// the base directory, or any path that resolves outside baseDir after cleaning.
func (w *folderStorageWrapper) safePath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("storage: path must be relative, got %q", path)
	}
	full := filepath.Join(w.baseDir, path)
	// filepath.Join already cleans the result; verify it is rooted at baseDir.
	base := filepath.Clean(w.baseDir)
	if full != base && !strings.HasPrefix(full, base+string(filepath.Separator)) {
		return "", fmt.Errorf("storage: path %q escapes base directory", path)
	}
	return full, nil
}

func (w *folderStorageWrapper) Size(path string) (int64, error) {
	full, err := w.safePath(path)
	if err != nil {
		return 0, err
	}
	fi, err := os.Stat(full)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (w *folderStorageWrapper) ReadAt(path string, p []byte, off int64, _ DataType) (int, error) {
	full, err := w.safePath(path)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(full) //nolint:gosec // path validated by safePath
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	return f.ReadAt(p, off)
}

func (w *folderStorageWrapper) Put(path string, data []byte) error {
	full, err := w.safePath(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o600) //nolint:gosec // path validated by safePath
}

func (w *folderStorageWrapper) Delete(path string) error {
	full, err := w.safePath(path)
	if err != nil {
		return err
	}
	return os.RemoveAll(full)
}

// AGENT: S3 / MinIO storage provider.

// MinIOProvider is a ReaderProvider backed by a single MinIO/S3 object.
// Construct with NewMinIOProvider and pass to NewReaderFromProvider.
// All methods are safe for concurrent use.
type MinIOProvider = s3provider.MinIOProvider

// NewMinIOProvider returns a ReaderProvider that reads a blockpack file from
// the given MinIO/S3-compatible object storage.
//
// client must be a connected *minio.Client.
// bucket is the bucket name; object is the full object path within that bucket.
//
// Size() is cached after the first call — StatObject is issued at most once.
// Each ReadAt call issues an independent HTTP range request.
func NewMinIOProvider(client *minio.Client, bucket, object string) *MinIOProvider {
	return s3provider.NewMinIOProvider(client, bucket, object)
}

// AGENT: Block metadata API - provides file-level metadata for query planning.

// BlockMeta contains metadata about a blockpack file for block selection.
// Tempo uses this information to determine which files to query based on time range overlap.

// Earliest span start time (unix nanos)
// Latest span start time (unix nanos)
// Total number of spans across all blocks
// Total number of unique traces (from trace index)
// Number of blocks in the file
// File size in bytes

// GetBlockMeta returns metadata about a blockpack file including time range,
// span/trace counts, and file size. This is used by Tempo for block selection
// to determine which files to query based on time range overlap.
//
// The returned metadata includes:
//   - MinStartNanos: Earliest span start time in the file (unix nanoseconds)
//   - MaxStartNanos: Latest span start time in the file (unix nanoseconds)
//   - TotalSpans: Total number of spans across all blocks
//   - TotalTraces: Number of unique traces (from trace index), consistent with Tempo's TotalObjects
//   - BlockCount: Number of blocks in the file
//   - Size: File size in bytes
//
// This function reads only the file header and block index metadata (typically <1KB),
// making it suitable for frequent calls during query planning.
func GetBlockMeta(path string, storage Storage) (meta *BlockMeta, err error) {
	return GetBlockMetaWithCache(path, storage, nil)
}

// GetBlockMetaWithCache is like GetBlockMeta but caches footer, header, and metadata
// reads in cache. Pass nil for cache to disable caching (equivalent to GetBlockMeta).
// path is used as the fileID within the cache namespace.
func GetBlockMetaWithCache(path string, storage Storage, cache *FileCache) (meta *BlockMeta, err error) {
	defer func() {
		if r := recover(); r != nil {
			meta = nil
			err = fmt.Errorf("internal error in GetBlockMeta: %v", r)
		}
	}()

	provider := &storageReaderProvider{storage: storage, path: path}
	var sc modules_sectioncache.SectionCache
	if cache != nil {
		sc = modules_sectioncache.NewFilecacheAdapter(cache)
	}
	r, readerErr := modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: path,
	})
	if readerErr != nil {
		return nil, fmt.Errorf("open blockpack: %w", readerErr)
	}

	size, sizeErr := storage.Size(path)
	if sizeErr != nil {
		return nil, fmt.Errorf("get file size: %w", sizeErr)
	}

	minStart := ^uint64(0)
	var maxStart uint64
	var totalSpans int
	for i := range r.BlockCount() {
		bm := r.BlockMeta(i)
		if bm.MinStart < minStart {
			minStart = bm.MinStart
		}
		if bm.MaxStart > maxStart {
			maxStart = bm.MaxStart
		}
		totalSpans += int(bm.SpanCount)
	}
	if r.BlockCount() == 0 {
		minStart = 0
	}

	return &BlockMeta{
		MinStartNanos: minStart,
		MaxStartNanos: maxStart,
		TotalSpans:    totalSpans,
		TotalTraces:   r.TraceCount(),
		BlockCount:    r.BlockCount(),
		Size:          size,
	}, nil
}

// fileEntry holds a path, its resolved metadata, and whether metadata resolution failed.

// FilePlan holds an ordered list of blockpack files with their resolved metadata,
// intended for use by query planners that need file-level priority ordering.

// PlanFiles resolves BlockMeta for each path and returns a FilePlan.
// Paths that fail metadata resolution are retained in the plan with failed=true
// and a zero BlockMeta; the error is logged via slog. A nil or empty paths slice
// returns an empty FilePlan. cache may be nil.
func PlanFiles(paths []string, storage Storage, cache *FileCache) *FilePlan {
	if len(paths) == 0 {
		return &FilePlan{}
	}

	entries := make([]fileEntry, len(paths))
	for i, p := range paths {
		meta, err := GetBlockMetaWithCache(p, storage, cache)
		if err != nil {
			slog.Error("PlanFiles: failed to get block meta", "path", p, "error", err)
			entries[i] = fileEntry{path: p, failed: true}
			continue
		}
		entries[i] = fileEntry{path: p, meta: *meta}
	}

	return &FilePlan{files: entries}
}

// Between returns the paths of files whose time range overlaps [minNanos, maxNanos],
// sorted newest-first (by MaxStartNanos descending). Files that failed metadata
// resolution are always included and sorted after all non-failed files. Returns nil
// when no files qualify.
func (p *FilePlan) Between(minNanos, maxNanos uint64) []string {
	if p == nil || len(p.files) == 0 {
		return nil
	}

	type candidate struct {
		path     string
		maxStart uint64
		failed   bool
	}

	var candidates []candidate
	for _, fe := range p.files {
		if fe.failed {
			candidates = append(candidates, candidate{path: fe.path, failed: true})
			continue
		}
		// Files with unknown time (both 0) are conservatively included, matching
		// the block-level planner behavior in queryplanner/planner.go.
		unknownTime := fe.meta.MinStartNanos == 0 && fe.meta.MaxStartNanos == 0
		if unknownTime || (fe.meta.MaxStartNanos >= minNanos && fe.meta.MinStartNanos <= maxNanos) {
			candidates = append(candidates, candidate{path: fe.path, maxStart: fe.meta.MaxStartNanos})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	slices.SortFunc(candidates, func(ci, cj candidate) int {
		// Failed files sort after non-failed
		if ci.failed != cj.failed {
			if !ci.failed {
				return -1
			}
			return 1
		}
		// Both failed: stable by path
		if ci.failed {
			return cmp.Compare(ci.path, cj.path)
		}
		// Both non-failed: newest first; tiebreak by path
		if ci.maxStart != cj.maxStart {
			return cmp.Compare(cj.maxStart, ci.maxStart) // descending
		}
		return cmp.Compare(ci.path, cj.path)
	})

	paths := make([]string, len(candidates))
	for i, c := range candidates {
		paths[i] = c.path
	}

	return paths
}

// AGENT: Conversion functions - convert from other formats into blockpack.

// AGENT: Compaction - merge and deduplicate multiple blockpack files.

// CompactionConfig configures a CompactBlocks operation.
type CompactionConfig = modules_compaction.Config

// CompactBlocks reads spans from multiple blockpack providers, deduplicates them,
// and writes compacted output files to output.
//
// providers are the blockpack sources to merge; they are read sequentially.
// cfg controls staging directory, output file size limits, and spans per block.
// output receives the compacted files via its Put method.
//
// Returns the relative paths of all output files written to output, and the count of
// spans dropped due to genuine (trace:id, span:id) duplication.
//
// DEV-ONLY HAND-PATCH (holistic-review Fix 3, 2026-07-07): this vendored copy predates a
// blockpack change surfacing droppedSpans through this wrapper instead of discarding it.
// Hand-patched here, mirroring the A-Tempo-1 pattern, so tempo's non-vendor call sites can
// already consume the new 3-value return before the real revendor lands. Minimal — will be
// replaced wholesale at the next `go mod vendor`.
func CompactBlocks(
	ctx context.Context,
	providers []ReaderProvider,
	cfg CompactionConfig,
	output WritableStorage,
) ([]string, int64, error) {
	return modules_compaction.CompactBlocks(ctx, providers, cfg, output)
}

// CompactionProviderFunc lazily opens a single input blockpack provider on demand.
// See CompactBlocksStreaming.
type CompactionProviderFunc = modules_compaction.ProviderFunc

// CompactBlocksStreaming is the memory-bounded variant of CompactBlocks: instead of
// taking all input providers already materialized, it opens each provider via its
// CompactionProviderFunc just-in-time, feeds its spans into the output, then releases
// the provider before opening the next. Peak input-side memory is ~max(largest single
// block) rather than sum(all blocks), so the number of input blocks can be raised
// without proportionally growing peak memory. The dedup set of (trace:id, span:id)
// keys is the only state that spans all inputs.
//
// providers are opened sequentially in slice order; each closure should download/open
// exactly one block and must not capture references that pin earlier blocks in memory.
//
// Returns the relative paths of all output files written to output, and the count of
// spans dropped due to genuine (trace:id, span:id) duplication.
//
// DEV-ONLY HAND-PATCH (holistic-review Fix 3, 2026-07-07): see CompactBlocks above.
func CompactBlocksStreaming(
	ctx context.Context,
	providers []CompactionProviderFunc,
	cfg CompactionConfig,
	output WritableStorage,
) ([]string, int64, error) {
	return modules_compaction.CompactBlocksStreaming(ctx, providers, cfg, output)
}
