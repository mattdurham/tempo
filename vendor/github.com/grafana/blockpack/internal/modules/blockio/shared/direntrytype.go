package shared

// DirEntryType is a blockpack data type.
type DirEntryType struct {
	Offset        uint64
	CompressedLen uint32
	SectionType   uint8
}
