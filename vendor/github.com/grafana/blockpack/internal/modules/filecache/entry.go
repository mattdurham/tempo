package filecache

type entry struct {
	filename string
	key      string
	order    uint64
	size     int64
}
