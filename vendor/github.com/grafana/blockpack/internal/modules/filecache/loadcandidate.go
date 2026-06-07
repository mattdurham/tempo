package filecache

type loadCandidate struct {
	path  string
	key   string
	size  int64
	mtime int64
}
