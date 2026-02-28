package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// DataType is a hint passed to ReaderProvider.ReadAt for caching layers.
type DataType string

// DataType constants for read-type hints.
const (
	DataTypeFooter   DataType = "footer"
	DataTypeHeader   DataType = "header"
	DataTypeMetadata DataType = "metadata"
	DataTypeBlock    DataType = "block"
	DataTypeIndex    DataType = "index"
	DataTypeCompact  DataType = "compact"
)

// ReaderProvider is the storage backend interface.
// All implementations must be safe for concurrent use.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}

// SpanFieldsProvider gives access to all attributes for a single span row.
// GetField returns the typed value of a named attribute.
// IterateFields calls fn for every present attribute, stopping if fn returns false.
type SpanFieldsProvider interface {
	GetField(name string) (any, bool)
	IterateFields(fn func(name string, value any) bool)
}
