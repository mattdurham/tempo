// Package writer provides functionality for building blockpack files.
//
// The Writer type handles span ingestion, column building, block
// serialization, and metric stream support. It supports both in-memory
// and streaming modes for flexible output.
//
// This package was split from the monolithic blockio package during cleanup to
// improve maintainability and establish clear boundaries between read and write paths.
//
// # Core Types
//
// Writer is the main entry point for building blockpack files:
//   - AddTracesData ingests OTLP trace data
//   - Flush serializes all buffered data to bytes
//   - Supports configurable block sizes
//
// WriterConfig controls Writer behavior:
//   - MaxBlockSpans sets the target spans per block
//   - OutputStream enables streaming mode
//
// # Writing Workflow
//
// 1. Create a Writer with NewWriter or NewWriterWithConfig
// 2. Add spans via AddTracesData
// 3. Call Flush to serialize the blockpack
// 4. The result is a complete blockpack byte slice (or written to stream)
//
// # Thread Safety
//
// Writer instances are NOT thread-safe. All methods must be called from a
// single goroutine. Concurrent calls will cause data races and may result
// in panics or corrupted data.
package writer
