package rw

import "sync/atomic"

// TrackingReaderProvider is a blockpack data type.
type TrackingReaderProvider struct {
	underlying ReaderProvider
	ioOps      atomic.Int64
	bytesRead  atomic.Int64
}
