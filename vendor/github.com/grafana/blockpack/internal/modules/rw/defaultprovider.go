package rw

// DefaultProvider is a blockpack data type.
type DefaultProvider struct {
	cache   *RangeCachingProvider
	tracker *TrackingReaderProvider
}
