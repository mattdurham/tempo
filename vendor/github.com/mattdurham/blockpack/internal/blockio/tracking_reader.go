package blockio

import (
	"sync/atomic"
	"time"
)

// TrackingReaderProvider wraps a ReaderProvider and tracks total bytes read and I/O operations
type TrackingReaderProvider struct {
	underlying   ReaderProvider
	bytesRead    int64
	ioOperations int64 // Number of ReadAt calls (important for object storage chattiness)
	latency      int64 // Artificial latency per I/O operation in nanoseconds (for S3 simulation)
}

// NewTrackingReaderProvider wraps a ReaderProvider with byte and I/O operation tracking
func NewTrackingReaderProvider(underlying ReaderProvider) *TrackingReaderProvider {
	return &TrackingReaderProvider{
		underlying:   underlying,
		bytesRead:    0,
		ioOperations: 0,
		latency:      0,
	}
}

// NewTrackingReaderProviderWithLatency wraps a ReaderProvider with tracking and artificial latency
// latency simulates object storage first-byte latency (typically 10-20ms for S3)
func NewTrackingReaderProviderWithLatency(underlying ReaderProvider, latency time.Duration) *TrackingReaderProvider {
	return &TrackingReaderProvider{
		underlying:   underlying,
		bytesRead:    0,
		ioOperations: 0,
		latency:      int64(latency),
	}
}

func (t *TrackingReaderProvider) Size() (int64, error) {
	return t.underlying.Size()
}

func (t *TrackingReaderProvider) ReadAt(p []byte, off int64, dataType DataType) (int, error) {
	// Simulate object storage latency (e.g., S3 first-byte latency)
	if latency := atomic.LoadInt64(&t.latency); latency > 0 {
		time.Sleep(time.Duration(latency))
	}

	n, err := t.underlying.ReadAt(p, off, dataType)
	// Track I/O operation count regardless of bytes read
	// This is important for object storage where each operation has latency/cost
	atomic.AddInt64(&t.ioOperations, 1)
	if n > 0 {
		atomic.AddInt64(&t.bytesRead, int64(n))
	}
	return n, err
}

// BytesRead returns the total bytes read through this provider
func (t *TrackingReaderProvider) BytesRead() int64 {
	return atomic.LoadInt64(&t.bytesRead)
}

// IOOperations returns the number of ReadAt calls made
// This is critical for understanding object storage chattiness
func (t *TrackingReaderProvider) IOOperations() int64 {
	return atomic.LoadInt64(&t.ioOperations)
}

// BytesPerIO returns the average bytes read per I/O operation
// Higher values indicate more efficient I/O (fewer, larger reads)
func (t *TrackingReaderProvider) BytesPerIO() float64 {
	ops := atomic.LoadInt64(&t.ioOperations)
	if ops == 0 {
		return 0
	}
	bytes := atomic.LoadInt64(&t.bytesRead)
	return float64(bytes) / float64(ops)
}

// Reset resets all counters to zero
func (t *TrackingReaderProvider) Reset() {
	atomic.StoreInt64(&t.bytesRead, 0)
	atomic.StoreInt64(&t.ioOperations, 0)
}
