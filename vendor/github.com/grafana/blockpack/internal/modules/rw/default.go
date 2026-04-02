package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import "time"

// latencyProvider injects artificial latency before each read (for testing).
type latencyProvider struct {
	underlying ReaderProvider
	latency    time.Duration
}

func (l *latencyProvider) Size() (int64, error) {
	return l.underlying.Size()
}

func (l *latencyProvider) ReadAt(p []byte, off int64, dt DataType) (int, error) {
	time.Sleep(l.latency)
	return l.underlying.ReadAt(p, off, dt)
}

// DefaultProvider is the standard composition:
//
//	outer (cache):   *RangeCachingProvider
//	inner (tracker): *TrackingReaderProvider
//	innermost:       user storage
//
// Holds the tracker separately for stats access.
// All methods are safe for concurrent use.
type DefaultProvider struct {
	cache   *RangeCachingProvider
	tracker *TrackingReaderProvider
}

// NewDefaultProvider creates a DefaultProvider wrapping underlying.
func NewDefaultProvider(underlying ReaderProvider) *DefaultProvider {
	tracker := NewTrackingReaderProvider(underlying)
	cache := NewRangeCachingProvider(tracker)
	return &DefaultProvider{cache: cache, tracker: tracker}
}

// NewDefaultProviderWithLatency creates a DefaultProvider with an injected
// per-read latency for testing.
func NewDefaultProviderWithLatency(underlying ReaderProvider, latency time.Duration) *DefaultProvider {
	lp := &latencyProvider{underlying: underlying, latency: latency}
	return NewDefaultProvider(lp)
}

// Size delegates to the cache layer.
func (d *DefaultProvider) Size() (int64, error) {
	return d.cache.Size()
}

// ReadAt delegates to the cache layer.
func (d *DefaultProvider) ReadAt(p []byte, off int64, dt DataType) (int, error) {
	return d.cache.ReadAt(p, off, dt)
}

// IOOps returns the number of actual I/O operations performed.
func (d *DefaultProvider) IOOps() int64 { return d.tracker.IOOps() }

// BytesRead returns the total bytes read from storage.
func (d *DefaultProvider) BytesRead() int64 { return d.tracker.BytesRead() }

// Reset zeroes the I/O counters by delegating to the inner tracker.
// Useful for isolating metrics between query phases.
func (d *DefaultProvider) Reset() { d.tracker.Reset() }
