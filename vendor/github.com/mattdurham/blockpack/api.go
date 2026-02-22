// Package blockpack provides a minimal public API for blockpack.
//
// AGENT: This public API must remain minimal and focused. Before adding any new
// public functions, types, or interfaces, you MUST ask the user for explicit
// permission. The design goal is to keep the API surface as small as possible
// and hide all implementation details in internal/ packages.
//
// The public API intentionally exposes ONLY:
//   - Query execution functions (TraceQL and SQL)
//   - Reader interface and basic types
//   - Provider interfaces for storage abstraction
//
// Everything else is internal implementation detail.
package blockpack

import (
	"context"
	"fmt"
	"io"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/blockio/compaction"
	"github.com/mattdurham/blockpack/internal/blockio/reader"
	"github.com/mattdurham/blockpack/internal/executor"
	"github.com/mattdurham/blockpack/internal/sql"
	"github.com/mattdurham/blockpack/internal/tempoapi"
)

// AGENT: Reader types - these provide access to blockpack data.
// Do not expose any internal reader implementation details.

// Reader reads blockpack files and provides query execution.
// This is a thin wrapper around internal implementation.
type Reader = reader.Reader

// Writer writes blockpack files from trace data.
// This is a thin wrapper around internal implementation.
type Writer = blockio.Writer

// Block represents a decoded block of spans.
type Block = reader.Block

// Column represents a decoded column.
type Column = reader.Column

// DataType represents the type of data being read for caching optimization.
type DataType = blockio.DataType

// DataType constants for read optimization hints
const (
	DataTypeFooter   = blockio.DataTypeFooter
	DataTypeMetadata = blockio.DataTypeMetadata
	DataTypeColumn   = blockio.DataTypeColumn
	DataTypeRow      = blockio.DataTypeRow
	DataTypeIndex    = blockio.DataTypeIndex
	DataTypeUnknown  = blockio.DataTypeUnknown
)

// ReaderProvider supplies random access to blockpack data.
// Implementations can use files, memory, cloud storage, etc.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}

// CloseableReaderProvider extends ReaderProvider with resource cleanup.
type CloseableReaderProvider interface {
	ReaderProvider
	Close() error
}

// AGENT: Reader constructors - minimal set needed for creating readers.

// NewReaderFromProvider creates a reader from a ReaderProvider.
func NewReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	// Wrap the public ReaderProvider to match internal shared.ReaderProvider
	wrappedProvider := &readerProviderAdapter{provider: provider}
	return reader.NewReaderFromProvider(wrappedProvider)
}

// readerProviderAdapter adapts the public ReaderProvider interface to shared.ReaderProvider
type readerProviderAdapter struct {
	provider ReaderProvider
}

func (a *readerProviderAdapter) Size() (int64, error) {
	return a.provider.Size()
}

func (a *readerProviderAdapter) ReadAt(p []byte, off int64, dataType reader.DataType) (int, error) {
	// Convert DataType from reader to public API DataType
	return a.provider.ReadAt(p, off, dataType)
}

// AGENT: Writer constructors - minimal set needed for creating writers.

// NewWriter creates a streaming blockpack writer that writes to the given output.
// maxSpansPerBlock controls block granularity (0 uses the default of 2000).
func NewWriter(output io.Writer, maxSpansPerBlock int) (*Writer, error) {
	return blockio.NewWriterWithConfig(blockio.WriterConfig{
		OutputStream:  output,
		MaxBlockSpans: maxSpansPerBlock,
	})
}

// AGENT: Query execution - this is the main public API for querying.
// Keep this minimal - just TraceQL and SQL query functions.

// QueryOptions configures query execution.
type QueryOptions = executor.QueryOptions

// QueryResult represents the results of a query.
type QueryResult = executor.BlockpackResult

// SpanMatch represents a single span that matched the query.
type SpanMatch = executor.BlockpackSpanMatch

// ExecuteTraceQL executes a TraceQL filter query against the provided storage.
// This is the primary query interface - it accepts a TraceQL filter string and returns results.
//
// The path parameter specifies the blockpack file to query.
// For structural queries (using >>, >, ~, <<, <, !~ operators), use ExecuteTraceQLStructural.
// For metrics queries, use ExecuteTraceQLMetrics.
func ExecuteTraceQL(
	path string,
	traceqlQuery string,
	storage Storage,
	opts QueryOptions,
) (result *QueryResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteTraceQL: %v", r)
		}
	}()

	// Compile TraceQL filter query to VM program
	program, err := sql.CompileTraceQL(traceqlQuery)
	if err != nil {
		return nil, fmt.Errorf("compile TraceQL: %w", err)
	}

	// Create executor with storage adapter
	exec := executor.NewBlockpackExecutor(&storageAdapter{storage: storage})

	// Execute query using VM program
	return exec.ExecuteQuery(path, program, nil, opts)
}

// ExecuteSQL executes a SQL query against the provided storage.
// This is the primary SQL query interface.
//
// The path parameter specifies the blockpack file to query.
func ExecuteSQL(path string, sqlQuery string, storage Storage, opts QueryOptions) (result *QueryResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteSQL: %v", r)
		}
	}()

	// Parse SQL query
	parsedSQL, err := sql.ParseSQL(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	// Compile SQL to VM program
	program, err := sql.CompileToProgram(parsedSQL)
	if err != nil {
		return nil, fmt.Errorf("compile SQL: %w", err)
	}

	// Create executor with storage adapter
	exec := executor.NewBlockpackExecutor(&storageAdapter{storage: storage})

	// Execute the query
	return exec.ExecuteQuery(path, program, nil, opts)
}

// AGENT: Trace lookup API - provides indexed trace access for Tempo compatibility.

// FindTraceByID looks up a trace by its hex-encoded ID using the trace block index.
// Returns the complete OTLP Trace with spans grouped by resource and scope.
// Returns nil if the trace is not found in the file.
// Returns an error if the trace ID format is invalid or the file cannot be read.
//
// This function uses the trace block index for O(log N) lookup performance,
// making it suitable for serving trace requests from Grafana or other clients.
// The trace ID should be a 32-character hex string (16 bytes).
//
// The returned Trace follows the OpenTelemetry Protocol (OTLP) format with spans
// properly grouped into ResourceSpans by (resource.service.name, scope.name, scope.version).
func FindTraceByID(path string, traceIDHex string, storage Storage) (trace *tempopb.Trace, err error) {
	defer func() {
		if r := recover(); r != nil {
			trace = nil
			err = fmt.Errorf("internal error in FindTraceByID: %v", r)
		}
	}()

	// Use storage adapter that implements the internal Storage interface
	adapter := &storageAdapter{storage: storage}
	return tempoapi.FindTraceByID(traceIDHex, path, adapter)
}

// AGENT: Tag discovery API - provides attribute name and value enumeration for Tempo/Grafana autocomplete.

// SearchTags returns attribute names available in the blockpack file.
// The scope parameter filters results:
//   - "resource" or "Resource": only resource attributes (starting with "resource.")
//   - "span" or "Span": only span attributes (starting with "span." or "span:")
//   - "" (empty string): all attributes
//
// This function powers Grafana's attribute name autocomplete by extracting names from
// the dedicated column index. The returned list is sorted and deduplicated.
//
// Attribute names follow blockpack's canonical naming conventions:
//   - Resource attributes: resource.service.name, resource.cluster.name, etc.
//   - Span intrinsics: span:name, span:id, span:status, span:duration, etc.
//   - Span attributes: span.http.method, span.db.statement, etc.
//   - Trace intrinsics: trace:id
func SearchTags(path string, scope string, storage Storage) (tags []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			tags = nil
			err = fmt.Errorf("internal error in SearchTags: %v", r)
		}
	}()

	adapter := &storageAdapter{storage: storage}
	return tempoapi.SearchTags(path, scope, adapter)
}

// SearchTagValues returns distinct values for the given attribute name.
// Values are limited to 10,000 entries to prevent excessive memory usage
// on high-cardinality attributes like trace IDs or request IDs.
//
// This function powers Grafana's attribute value autocomplete. It uses the dedicated
// column index for fast O(1) lookup when available. The tag name is normalized
// to canonical form before lookup:
//   - "service.name" -> "resource.service.name"
//   - "service_name" -> "resource.service.name"
//
// If the attribute has more than 10,000 distinct values, only the first 10,000
// (sorted) are returned. If the attribute is not indexed or doesn't exist in the
// file, an empty slice is returned (not an error).
//
// The returned values are sorted alphabetically.
func SearchTagValues(path string, tag string, storage Storage) (tags []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			tags = nil
			err = fmt.Errorf("internal error in SearchTagValues: %v", r)
		}
	}()

	adapter := &storageAdapter{storage: storage}
	return tempoapi.SearchTagValues(path, tag, adapter)
}

// BlockMeta contains metadata about a blockpack file for block selection.
// Tempo uses this information to determine which files to query based on time range overlap.
type BlockMeta = tempoapi.BlockMeta

// GetBlockMeta returns metadata about a blockpack file including time range,
// span count, and file size. This is used by Tempo for block selection
// to determine which files to query based on time range overlap.
//
// The returned metadata includes:
//   - MinStartNanos: Earliest span start time in the file (unix nanoseconds)
//   - MaxStartNanos: Latest span start time in the file (unix nanoseconds)
//   - TotalSpans: Total number of spans across all blocks
//   - BlockCount: Number of blocks in the file
//   - Size: File size in bytes
//
// This function reads only the file header and block index metadata (typically <1KB),
// making it suitable for frequent calls during query planning.
func GetBlockMeta(path string, storage Storage) (meta *BlockMeta, err error) {
	defer func() {
		if r := recover(); r != nil {
			meta = nil
			err = fmt.Errorf("internal error in GetBlockMeta: %v", r)
		}
	}()

	adapter := &storageAdapter{storage: storage}
	return tempoapi.GetBlockMeta(path, adapter)
}

// AGENT: Storage interfaces - minimal abstraction for storage backends.

// Storage provides access to blockpack files for query execution.
// It exposes object storage primitives: Size and ReadAt.
type Storage interface {
	// Size returns the total size of the object at the given path.
	Size(path string) (int64, error)

	// ReadAt reads len(p) bytes from the object at path starting at offset off.
	//
	// The dataType parameter provides a hint about the type of data being read
	// (footer, metadata, column, row, index, or unknown). Storage implementations
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

// WritableStorage extends Storage with write capability.
// Used by CompactBlocks to push compacted output files to a storage backend.
type WritableStorage interface {
	Storage
	// Put writes data to the given path, creating or overwriting the file.
	Put(path string, data []byte) error
}

// CompactionConfig configures the compaction operation.
type CompactionConfig struct {
	// StagingDir is a local directory used to stage compacted output files
	// before they are pushed to outputStorage. Must be writable.
	// If empty, os.TempDir() is used.
	StagingDir string
	// MaxOutputFileSize is the approximate maximum size in bytes of each output
	// file. Size is estimated using the writer's internal heuristic (spans *
	// 2048 bytes) rather than actual serialized bytes, so output files may
	// exceed this value for spans with many attributes. A new output file is
	// started when the estimate crosses this threshold while buffering spans.
	// Zero means no size limit.
	MaxOutputFileSize int64

	// MaxSpansPerBlock controls how many spans are written per block in the
	// output files. Defaults to 2000 if zero.
	MaxSpansPerBlock int
}

// CompactBlocks merges and re-packs spans from the provided blockpack providers
// into new output files, stages them locally, then copies to outputStorage.
// Returns the relative paths of all output files written to outputStorage.
//
// If config.MaxOutputFileSize > 0, a new output file is started whenever the
// writer's estimated size (spans * 2048 bytes) crosses that threshold. This
// is an approximation — actual output file size may differ.
//
// The ctx parameter controls cancellation. CompactBlocks checks ctx.Err()
// before processing each provider.
func CompactBlocks(
	ctx context.Context,
	providers []ReaderProvider,
	config CompactionConfig,
	outputStorage WritableStorage,
) (outputPaths []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			outputPaths = nil
			err = fmt.Errorf("internal error in CompactBlocks: %v", r)
		}
	}()

	if outputStorage == nil {
		return nil, fmt.Errorf("outputStorage cannot be nil")
	}

	internalProviders := make([]blockio.ReaderProvider, len(providers))
	for i, p := range providers {
		internalProviders[i] = &readerProviderAdapter{provider: p}
	}

	return compaction.CompactBlocks(ctx, internalProviders, compaction.Config{
		MaxOutputFileSize: config.MaxOutputFileSize,
		MaxSpansPerBlock:  config.MaxSpansPerBlock,
		StagingDir:        config.StagingDir,
	}, outputStorage)
}

// storageAdapter adapts our Storage interface to the internal executor's FileStorage interface.
// It wraps Storage (which has ReadAt) and provides both Get and GetProvider methods.
type storageAdapter struct {
	storage Storage
}

// Get implements the legacy FileStorage interface by reading through ReadAt.
// This method is not used by the executor (which uses GetProvider internally),
// but is required to satisfy the FileStorage interface.
func (a *storageAdapter) Get(path string) ([]byte, error) {
	size, err := a.storage.Size(path)
	if err != nil {
		return nil, err
	}

	// Validate size can fit in int
	if size < 0 {
		return nil, fmt.Errorf("invalid size: %d", size)
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("size %d exceeds maximum buffer size", size)
	}

	data := make([]byte, int(size))
	n, err := a.storage.ReadAt(path, data, 0, DataTypeUnknown)
	if err != nil {
		// Allow io.EOF only if we read the full buffer. Any other error,
		// or io.EOF with a short read, indicates a size mismatch or truncation.
		if err == io.EOF && n == len(data) {
			return data, nil
		}
		return nil, err
	}
	if n != len(data) {
		return nil, fmt.Errorf("short read: expected %d bytes, got %d", len(data), n)
	}
	return data, nil
}

// GetProvider returns a ReaderProvider for the given path.
// If the underlying Storage implements GetProvider directly, it uses that for efficiency.
// Otherwise, it wraps Storage.ReadAt/Size in a provider.
// Returns blockio.ReaderProvider to satisfy executor.ProviderStorage interface.
func (a *storageAdapter) GetProvider(path string) (blockio.ReaderProvider, error) {
	// Optimization: If storage implements GetProvider directly (e.g., for efficient
	// file handle reuse), use that instead of wrapping ReadAt calls.
	if provStorage, ok := a.storage.(interface {
		GetProvider(string) (blockio.ReaderProvider, error)
	}); ok {
		return provStorage.GetProvider(path)
	}

	// Fallback: wrap the Storage interface ReadAt/Size calls
	return &storageReaderProvider{storage: a.storage, path: path}, nil
}

// storageReaderProvider implements ReaderProvider by delegating to Storage's ReadAt.
type storageReaderProvider struct {
	storage Storage
	path    string
}

func (p *storageReaderProvider) Size() (int64, error) {
	return p.storage.Size(p.path)
}

func (p *storageReaderProvider) ReadAt(buf []byte, off int64, dataType DataType) (int, error) {
	return p.storage.ReadAt(p.path, buf, off, dataType)
}

// NewFileStorage creates a new filesystem-based storage that implements the Storage interface.
// It wraps executor.FolderStorage to provide Size and ReadAt methods.
func NewFileStorage(baseDir string) Storage {
	return &folderStorageWrapper{fs: executor.NewFolderStorage(baseDir)}
}

// folderStorageWrapper wraps FolderStorage to satisfy our public Storage interface.
// It provides Size, ReadAt, and GetProvider methods by delegating to FolderStorage.
type folderStorageWrapper struct {
	fs *executor.FolderStorage
}

// GetProvider returns a provider directly from FolderStorage for efficient file handle reuse.
// This avoids the overhead of opening/closing the file on every ReadAt/Size call.
func (w *folderStorageWrapper) GetProvider(path string) (blockio.ReaderProvider, error) {
	return w.fs.GetProvider(path)
}

// Put writes data to the given path within the storage's base directory.
func (w *folderStorageWrapper) Put(path string, data []byte) error {
	return w.fs.Put(path, data)
}

func (w *folderStorageWrapper) Size(path string) (int64, error) {
	provider, err := w.fs.GetProvider(path)
	if err != nil {
		return 0, err
	}
	// Close the provider if it's closeable to avoid resource leaks
	if closeable, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			_ = closeable.Close() // Ignore close error in defer
		}()
	}
	return provider.Size()
}

func (w *folderStorageWrapper) ReadAt(path string, p []byte, off int64, dataType DataType) (int, error) {
	provider, err := w.fs.GetProvider(path)
	if err != nil {
		return 0, err
	}
	// Close the provider if it's closeable to avoid resource leaks
	if closeable, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			_ = closeable.Close() // Ignore close error in defer
		}()
	}
	return provider.ReadAt(p, off, dataType)
}
