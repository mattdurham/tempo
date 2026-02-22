// Package reader provides high-level block reading functionality for the
// blockio package, including block decoding, column extraction, and metadata parsing.
//
// This package was split from the monolithic blockio package during cleanup to
// improve maintainability and establish clear boundaries between read and write paths.
//
// # Core Types
//
// Block represents a decoded block with columns and metadata:
//   - Columns map column names to typed values
//   - Metadata provides block-level information
//   - GetColumn safely retrieves specific columns
//
// SliceReader provides efficient multi-column reading:
//   - ReadSpans reads all columns for multiple rows
//   - ReadColumn reads a single column across rows
//   - Reuses buffers to minimize allocations
//
// # Reading Workflow
//
// 1. Open a block file or data stream
// 2. Parse metadata to understand block structure
// 3. Query which blocks contain desired data
// 4. Read specific columns from selected blocks
// 5. Decode columns into typed values
//
// # Column Types
//
// The reader supports multiple column encoding formats:
//   - Plain: Uncompressed raw values
//   - Dictionary: Deduplicated values with indexes
//   - Delta: Delta-encoded integers
//   - RLE: Run-length encoded values
//   - Bitset: Dense boolean arrays
//
// All decoders are in the decoders subpackage for organization.
//
// # Usage Example
//
//	// Read a block from storage
//	data, err := os.ReadFile("block-001.bp")
//	if err != nil {
//	    return err
//	}
//
//	// Create slice reader for efficient access
//	sr := reader.NewSliceReader(data)
//
//	// Read specific columns
//	spans, err := sr.ReadSpans([]string{"service.name", "duration"})
//	if err != nil {
//	    return err
//	}
//
//	// Process decoded spans
//	for _, span := range spans {
//	    service := span.GetString("service.name")
//	    duration := span.GetUint64("duration")
//	    fmt.Printf("%s: %d ms\n", service, duration)
//	}
//
// # Performance Considerations
//
// Block reading is optimized for query patterns:
//   - Lazy loading: Only read requested columns
//   - Batch operations: Read multiple rows efficiently
//   - Buffer reuse: Minimize allocations in hot paths
//   - Bloom filters: Skip blocks without required columns
//   - Coalescing: Combine adjacent I/O operations
//
// # Decoders Subpackage
//
// The decoders/ subdirectory contains column decoding implementations:
//   - delta.go: Delta encoding for integers
//   - dictionary.go: Dictionary encoding for strings
//   - rle.go: Run-length encoding
//   - bitset.go: Boolean bitsets
//
// This organization addresses the BOT comment: "these decodes are ripe to move
// to their own folder" and improves code discoverability.
//
// # Thread Safety
//
// SliceReader instances are NOT thread-safe. Create separate readers for
// concurrent access or use external synchronization. The underlying data
// is immutable and safe for concurrent reading with separate reader instances.
//
// # Error Handling
//
// Reader functions return errors for:
//   - Corrupted block data (invalid magic, checksum failures)
//   - Unsupported format versions
//   - Missing required columns
//   - Invalid column encodings
//
// Always check errors and handle corruption gracefully in production systems.
package reader
