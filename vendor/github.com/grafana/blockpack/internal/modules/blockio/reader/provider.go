package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TrackingReaderProvider wraps a ReaderProvider and counts I/O calls and bytes.
type TrackingReaderProvider struct {
	underlying shared.ReaderProvider
	ioOps      atomic.Int64
	bytesRead  atomic.Int64
}

// NewTrackingReaderProvider wraps underlying with I/O tracking.
func NewTrackingReaderProvider(underlying shared.ReaderProvider) *TrackingReaderProvider {
	return &TrackingReaderProvider{underlying: underlying}
}

// Size delegates to the underlying provider.
func (t *TrackingReaderProvider) Size() (int64, error) {
	return t.underlying.Size()
}

// ReadAt records the I/O operation and delegates to the underlying provider.
func (t *TrackingReaderProvider) ReadAt(p []byte, off int64, dt shared.DataType) (int, error) {
	t.ioOps.Add(1)
	n, err := t.underlying.ReadAt(p, off, dt)
	t.bytesRead.Add(int64(n))
	return n, err
}

// IOOps returns the number of ReadAt calls made.
func (t *TrackingReaderProvider) IOOps() int64 { return t.ioOps.Load() }

// BytesRead returns the total bytes read.
func (t *TrackingReaderProvider) BytesRead() int64 { return t.bytesRead.Load() }

// Reset zeroes the counters.
func (t *TrackingReaderProvider) Reset() {
	t.ioOps.Store(0)
	t.bytesRead.Store(0)
}

// cachedRange is one cached contiguous byte range.
type cachedRange struct {
	data   []byte
	offset int64
}

// RangeCachingProvider wraps a ReaderProvider with sub-range caching.
// If a requested range is fully contained within a cached range, the bytes are
// served from memory without a new I/O call.
type RangeCachingProvider struct {
	underlying shared.ReaderProvider
	cache      []cachedRange
	mu         sync.RWMutex
}

// NewRangeCachingProvider wraps underlying with range caching.
func NewRangeCachingProvider(underlying shared.ReaderProvider) *RangeCachingProvider {
	return &RangeCachingProvider{underlying: underlying}
}

// Size delegates to the underlying provider.
func (c *RangeCachingProvider) Size() (int64, error) {
	return c.underlying.Size()
}

// ReadAt checks whether [off, off+len(p)) is covered by a cached range and
// serves from cache if so.  Otherwise it issues a real read and stores the
// result in the cache.
func (c *RangeCachingProvider) ReadAt(p []byte, off int64, dt shared.DataType) (int, error) {
	end := off + int64(len(p))

	c.mu.RLock()
	for _, cr := range c.cache {
		crEnd := cr.offset + int64(len(cr.data))
		if off >= cr.offset && end <= crEnd {
			n := copy(p, cr.data[off-cr.offset:])
			c.mu.RUnlock()
			return n, nil
		}
	}
	c.mu.RUnlock()

	buf := make([]byte, len(p))
	n, err := c.underlying.ReadAt(buf, off, dt)
	if err != nil {
		return n, err
	}

	copy(p, buf[:n])

	c.mu.Lock()
	c.cache = append(c.cache, cachedRange{offset: off, data: buf[:n]})
	c.mu.Unlock()

	return n, nil
}

// latencyProvider injects artificial latency before each read (for testing).
type latencyProvider struct {
	underlying shared.ReaderProvider
	latency    time.Duration
}

func (l *latencyProvider) Size() (int64, error) {
	return l.underlying.Size()
}

func (l *latencyProvider) ReadAt(p []byte, off int64, dt shared.DataType) (int, error) {
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
type DefaultProvider struct {
	cache   *RangeCachingProvider
	tracker *TrackingReaderProvider
}

// NewDefaultProvider creates a DefaultProvider wrapping underlying.
func NewDefaultProvider(underlying shared.ReaderProvider) *DefaultProvider {
	tracker := NewTrackingReaderProvider(underlying)
	cache := NewRangeCachingProvider(tracker)
	return &DefaultProvider{cache: cache, tracker: tracker}
}

// NewDefaultProviderWithLatency creates a DefaultProvider with an injected
// per-read latency for testing.
func NewDefaultProviderWithLatency(underlying shared.ReaderProvider, latency time.Duration) *DefaultProvider {
	lp := &latencyProvider{underlying: underlying, latency: latency}
	return NewDefaultProvider(lp)
}

// Size delegates to the cache layer.
func (d *DefaultProvider) Size() (int64, error) {
	return d.cache.Size()
}

// ReadAt delegates to the cache layer.
func (d *DefaultProvider) ReadAt(p []byte, off int64, dt shared.DataType) (int, error) {
	return d.cache.ReadAt(p, off, dt)
}

// IOOps returns the number of actual I/O operations performed.
func (d *DefaultProvider) IOOps() int64 { return d.tracker.IOOps() }

// BytesRead returns the total bytes read from storage.
func (d *DefaultProvider) BytesRead() int64 { return d.tracker.BytesRead() }
