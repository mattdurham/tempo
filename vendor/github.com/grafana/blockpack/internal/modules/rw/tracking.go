package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import "sync/atomic"

// TrackingReaderProvider wraps a ReaderProvider and counts I/O calls and bytes.
// All methods are safe for concurrent use.
type TrackingReaderProvider struct {
	underlying ReaderProvider
	ioOps      atomic.Int64
	bytesRead  atomic.Int64
}

// NewTrackingReaderProvider wraps underlying with I/O tracking.
func NewTrackingReaderProvider(underlying ReaderProvider) *TrackingReaderProvider {
	return &TrackingReaderProvider{underlying: underlying}
}

// Size delegates to the underlying provider.
func (t *TrackingReaderProvider) Size() (int64, error) {
	return t.underlying.Size()
}

// ReadAt records the I/O operation and delegates to the underlying provider.
func (t *TrackingReaderProvider) ReadAt(p []byte, off int64, dt DataType) (int, error) {
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
