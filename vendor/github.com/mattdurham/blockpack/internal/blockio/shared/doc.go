// Package shared contains types and utilities shared between blockio writer and reader packages.
//
// # Architecture
//
// This package serves as the single source of truth for:
//   - Wire format types (BlockIndexEntry, ColumnIndexEntry, AttributeStats)
//   - Data provider interfaces (ReaderProvider) and wrappers
//   - Configuration types for caching, memcache, and tracking
//
// # Provider Wrappers
//
// Provider wrappers implement the decorator pattern for ReaderProvider:
//   - TrackingReaderProvider: Tracks I/O metrics (bytes read, operations)
//   - CachingReaderProvider: LRU cache for read data
//   - DataAwareCachingProvider: Region-aware cache with access pattern tracking
//   - MemcacheReaderProvider: Distributed cache using memcache protocol
//
// These wrappers can be stacked in any order, allowing flexible provider chains.
//
// # Type Ownership
//
// - shared/: Cross-package types and provider utilities
// - blockio/: Write operations and block encoding
// - reader/: Read operations and block decoding
// - format_exports.go: Public API surface (uses type aliases to shared/)
//
// # Design Principles
//
// - No circular dependencies: reader and blockio both import shared, but not each other
// - Type aliases preserve backward compatibility
// - Provider wrappers are composable and stateless where possible
package shared //nolint:revive
