// Package rw provides the storage abstraction layer for blockpack.
// It defines the ReaderProvider interface, DataType hints, and standard
// provider compositions (tracking, caching, default).
package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// DataType is a hint passed to ReaderProvider.ReadAt identifying the kind of data being read.
// Caching layers use it to determine eviction priority; other implementations may ignore it.
// Higher-priority types survive cache pressure longer than lower-priority ones.
//
// Priority order (highest → lowest): Footer ≈ Header > Metadata ≈ TraceBloomFilter >
// TimestampIndex > Block.
type DataType uint8

// DataType constants ordered by eviction priority (lower numeric value = higher priority).
const (
	DataTypeUnknown          DataType = iota // 0 – unknown; treated as lowest priority
	DataTypeFooter                           // 1 – file footer; highest priority, never evicted first
	DataTypeHeader                           // 2 – file header; same tier as footer
	DataTypeMetadata                         // 3 – block metadata section
	DataTypeTraceBloomFilter                 // 4 – compact trace-ID bloom filter + lookup index
	DataTypeTimestampIndex                   // 5 – per-file timestamp index
	DataTypeBlock                            // 6 – span block payload; lowest priority, evicted first
)

// ReaderProvider is the storage backend interface.
// All implementations must be safe for concurrent use.
// Size returns the total size of the storage in bytes.
// ReadAt is pread-style: offset-based and safe for concurrent use.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}
