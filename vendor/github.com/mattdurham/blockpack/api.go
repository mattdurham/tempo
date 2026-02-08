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
	"fmt"
	"io"

	"github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/executor"
	"github.com/mattdurham/blockpack/internal/sql"
)

// AGENT: Reader types - these provide access to blockpack data.
// Do not expose any internal reader implementation details.

// Reader reads blockpack files and provides query execution.
// This is a thin wrapper around internal implementation.
type Reader = blockio.Reader

// Writer writes blockpack files from trace data.
// This is a thin wrapper around internal implementation.
type Writer = blockio.Writer

// Block represents a decoded block of spans.
type Block = blockio.Block

// Column represents a decoded column.
type Column = blockio.Column

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
	return blockio.NewReaderFromProvider(provider)
}

// AGENT: Writer constructors - minimal set needed for creating writers.

// NewWriter creates a new blockpack writer with the specified maximum spans per block.
func NewWriter(maxSpansPerBlock int) *Writer {
	return blockio.NewWriter(maxSpansPerBlock)
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
func ExecuteTraceQL(path string, traceqlQuery string, storage Storage, opts QueryOptions) (*QueryResult, error) {
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
func ExecuteSQL(path string, sqlQuery string, storage Storage, opts QueryOptions) (*QueryResult, error) {
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
