package shared

// SectionDirectory is a blockpack data type.
type SectionDirectory struct {
	TypeEntries map[uint8]DirEntryType
	NameEntries map[string]DirEntryName
	SignalType  uint8
}
