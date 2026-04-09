package blockpack

// storage.go — Storage interface, file/MinIO backends, BlockMeta, FilePlan,
// compaction, and OTLP proto conversion. These are the persistence and
// planning primitives used by query orchestrators and ingest pipelines.

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	minio "github.com/minio/minio-go/v7"

	modules_compaction "github.com/grafana/blockpack/internal/modules/blockio/compaction"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/otlpconvert"
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
type WritableStorage interface {
	Storage
	// Put writes data to the given path, creating or overwriting the file.
	Put(path string, data []byte) error
	// Delete removes the file at the given path.
	Delete(path string) error
}

// storageReaderProvider adapts a Storage + path to modules_rw.ReaderProvider.
type storageReaderProvider struct {
	storage Storage
	path    string
}

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
type folderStorageWrapper struct {
	baseDir string
}

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
type BlockMeta struct {
	MinStartNanos uint64 // Earliest span start time (unix nanos)
	MaxStartNanos uint64 // Latest span start time (unix nanos)
	TotalSpans    int    // Total number of spans across all blocks
	TotalTraces   int    // Total number of unique traces (from trace index)
	BlockCount    int    // Number of blocks in the file
	Size          int64  // File size in bytes
}

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
	r, readerErr := modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  cache,
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
type fileEntry struct {
	path   string
	meta   BlockMeta
	failed bool
}

// FilePlan holds an ordered list of blockpack files with their resolved metadata,
// intended for use by query planners that need file-level priority ordering.
type FilePlan struct {
	files []fileEntry
}

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

// ConvertProtoToBlockpack reads an OTLP protobuf-encoded TracesData file and writes
// blockpack-formatted trace data to output.
// The input file must contain a single wire-encoded tracev1.TracesData protobuf message.
// maxSpansPerBlock controls block granularity (0 uses the default of 2000).
func ConvertProtoToBlockpack(inputPath string, output io.Writer, maxSpansPerBlock int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error in ConvertProtoToBlockpack: %v", r)
		}
	}()
	return otlpconvert.ConvertFromProtoFile(inputPath, output, maxSpansPerBlock)
}

// ConvertLogsProtoToBlockpack reads an OTLP protobuf-encoded LogsData file and writes
// a blockpack log file to output.
// The input file must contain a single wire-encoded logsv1.LogsData protobuf message.
// maxRecordsPerBlock controls block granularity (0 uses the default of 2000).
func ConvertLogsProtoToBlockpack(inputPath string, output io.Writer, maxRecordsPerBlock int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error in ConvertLogsProtoToBlockpack: %v", r)
		}
	}()
	return otlpconvert.ConvertLogsProtoFile(inputPath, output, maxRecordsPerBlock)
}

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
// Returns the relative paths of all output files written to output.
func CompactBlocks(
	ctx context.Context,
	providers []ReaderProvider,
	cfg CompactionConfig,
	output WritableStorage,
) ([]string, error) {
	converted := make([]modules_rw.ReaderProvider, len(providers))
	for i, p := range providers {
		converted[i] = p
	}
	paths, _, err := modules_compaction.CompactBlocks(ctx, converted, cfg, output)
	return paths, err
}
