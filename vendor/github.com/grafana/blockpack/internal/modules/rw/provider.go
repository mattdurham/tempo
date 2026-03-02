// Package rw provides the storage abstraction layer for blockpack.
// It defines the ReaderProvider interface, DataType hints, and standard
// provider compositions (tracking, caching, default).
package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// DataType is a hint passed to ReaderProvider.ReadAt for caching layers.
// Implementations may ignore it; it is used by caching layers for future segmentation.
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
// Size returns the total size of the storage in bytes.
// ReadAt is pread-style: offset-based and safe for concurrent use.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}
