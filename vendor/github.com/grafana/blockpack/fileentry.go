package blockpack

type fileEntry struct {
	path   string
	meta   BlockMeta
	failed bool
}
