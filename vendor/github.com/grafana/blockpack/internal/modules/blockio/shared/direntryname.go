package shared

// DirEntryName is a blockpack data type.
type DirEntryName struct {
	Name          string
	Offset        uint64
	CompressedLen uint32
}
