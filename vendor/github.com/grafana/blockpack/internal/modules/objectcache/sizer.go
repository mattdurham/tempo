package objectcache

// Sizer is a blockpack data type.
type Sizer interface {
	SizeBytes() int64
}
