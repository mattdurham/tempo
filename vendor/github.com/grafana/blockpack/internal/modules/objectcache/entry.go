package objectcache

type entry[V any] struct {
	val        *V
	prev, next *entry[V]
	key        string
	sizeBytes  int64
}
