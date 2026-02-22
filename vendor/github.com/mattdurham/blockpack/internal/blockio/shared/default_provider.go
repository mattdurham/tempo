package shared

import (
	"fmt"
	"time"
)

// DefaultProvider is the standard provider stack for a single blockpack file read session.
// It layers two capabilities on top of any underlying storage provider:
//
//  1. RangeCachingProvider: serves repeated or sub-range reads from in-memory cache,
//     avoiding redundant IO to underlying storage.
//  2. TrackingReaderProvider: counts only the IO operations that reach actual storage
//     (i.e. cache misses), giving accurate IOPS and bytes-read metrics.
//
// Chain (outer to inner): DefaultProvider → RangeCaching → Tracking → underlying
//
// Use NewDefaultProvider as the single place to build the standard provider stack.
// Pass your storage-specific ReaderProvider (filesystem, S3, etc.) as the underlying argument.
type DefaultProvider struct {
	cache   *RangeCachingProvider
	tracker *TrackingReaderProvider
}

// NewDefaultProvider creates a DefaultProvider wrapping the given storage provider.
func NewDefaultProvider(underlying ReaderProvider) (*DefaultProvider, error) {
	if underlying == nil {
		return nil, fmt.Errorf("underlying provider cannot be nil")
	}
	tracker, err := NewTrackingReaderProvider(underlying)
	if err != nil {
		return nil, fmt.Errorf("create tracking provider: %w", err)
	}
	cache, err := NewRangeCachingProvider(tracker)
	if err != nil {
		return nil, fmt.Errorf("create range caching provider: %w", err)
	}
	return &DefaultProvider{cache: cache, tracker: tracker}, nil
}

// NewDefaultProviderWithLatency creates a DefaultProvider with artificial per-IO latency.
// The latency is applied at the tracking layer (below the cache) to simulate object storage
// first-byte latency such as S3 (~10–20 ms per operation). Cache hits incur no latency.
func NewDefaultProviderWithLatency(underlying ReaderProvider, latency time.Duration) (*DefaultProvider, error) {
	if underlying == nil {
		return nil, fmt.Errorf("underlying provider cannot be nil")
	}
	tracker, err := NewTrackingReaderProviderWithLatency(underlying, latency)
	if err != nil {
		return nil, fmt.Errorf("create tracking provider: %w", err)
	}
	cache, err := NewRangeCachingProvider(tracker)
	if err != nil {
		return nil, fmt.Errorf("create range caching provider: %w", err)
	}
	return &DefaultProvider{cache: cache, tracker: tracker}, nil
}

// Size returns the total size of the underlying file.
func (d *DefaultProvider) Size() (int64, error) {
	return d.cache.Size()
}

// ReadAt reads bytes, serving from the in-memory range cache when possible.
// Only cache misses reach the underlying storage and are counted by the tracker.
func (d *DefaultProvider) ReadAt(p []byte, off int64, dataType DataType) (int, error) {
	return d.cache.ReadAt(p, off, dataType)
}

// BytesRead returns total bytes read from underlying storage (cache misses only).
func (d *DefaultProvider) BytesRead() int64 {
	return d.tracker.BytesRead()
}

// IOOperations returns the number of ReadAt calls that reached underlying storage.
func (d *DefaultProvider) IOOperations() int64 {
	return d.tracker.IOOperations()
}

// BytesPerIO returns average bytes per real storage IO operation.
func (d *DefaultProvider) BytesPerIO() float64 {
	return d.tracker.BytesPerIO()
}

// Reset resets all tracking counters to zero.
func (d *DefaultProvider) Reset() {
	d.tracker.Reset()
}
