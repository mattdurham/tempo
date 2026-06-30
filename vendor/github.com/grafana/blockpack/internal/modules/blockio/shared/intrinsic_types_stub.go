package shared

// Stub types retained for API compatibility after IntrinsicTOC removal (#433/#436).
// These types have no fields or methods; callers receive nil/zero values from all stubs.

// IntrinsicColumn is a stub. The IntrinsicTOC was removed in #433.
type IntrinsicColumn struct {
	Name         string
	BlockRefs    []BlockRef
	BytesValues  [][]byte
	Uint64Values []uint64
	Int64Values  []int64
	DictEntries  []IntrinsicDictEntry
	Count        int
	Format       uint8
	Type         ColumnType
}

// SizeBytes satisfies objectcache.Sizer.
func (col *IntrinsicColumn) SizeBytes() int64 { return 0 }

// IntrinsicColMeta is a stub. The IntrinsicTOC was removed in #433.
type IntrinsicColMeta struct {
	Name   string
	Min    string
	Max    string
	Offset uint64
	Length uint32
	Count  uint32
	Type   ColumnType
	Format uint8
}

// IntrinsicDictEntry is a stub. The IntrinsicTOC was removed in #433.
type IntrinsicDictEntry struct {
	Value     string
	BlockRefs []BlockRef
	Int64Val  int64
}

// DecodedPage is a stub. The streaming intrinsic scan path was removed in #433.
type DecodedPage struct{}

// PageStats is a stub. The streaming intrinsic scan path was removed in #433.
type PageStats struct{}

// IsIntrinsicColumn always returns false after IntrinsicTOC removal (#436).
// All fields are now regular block columns.
func IsIntrinsicColumn(_ string) bool { return false }

// MakeNoZeroBytes returns an uninitialized byte slice of size n.
// After unzeroed_alloc.go was deleted (#436), this simple fallback is sufficient.
func MakeNoZeroBytes(n int) []byte {
	return make([]byte, n)
}

// SemanticBytesEncoding identifies the dense base encoding kind for semantic overrides.
// After #436 (intrinsic/attribute distinction removal), all overrides return None.
type SemanticBytesEncoding uint8

const (
	// SemanticBytesNone means no override.
	SemanticBytesNone SemanticBytesEncoding = iota
	// SemanticBytesDeltaDictionary is unused after #436.
	SemanticBytesDeltaDictionary
	// SemanticBytesXOR is unused after #436.
	SemanticBytesXOR
	// SemanticBytesPrefix is unused after #436.
	SemanticBytesPrefix
	// SemanticBytesOverrideStub is unused after #436.
	SemanticBytesOverrideStub
)

// SemanticBytesOverride always returns SemanticBytesNone after #436.
// All columns now use the cost-based encoding selector.
func SemanticBytesOverride(_ string) SemanticBytesEncoding { return SemanticBytesNone }

// BlockRef for intrinsic column scans (old API compatibility).
// After #436, these functions always return nil since IntrinsicTOC is gone.

// ScanDictColumnRefsWithBloom is a stub. IntrinsicTOC removed in #433.
func ScanDictColumnRefsWithBloom(
	_ []byte,
	_ func(valueBytes []byte, int64Val int64, isInt64 bool) bool,
	_ [][]byte,
	_ int,
) []BlockRef {
	return nil
}

// ScanFlatColumnRefs is a stub. IntrinsicTOC removed in #433.
func ScanFlatColumnRefs(_ []byte, _, _ uint64, _, _ bool, _ int) []BlockRef { return nil }

// LookupRefFastUint64 is a stub. IntrinsicTOC removed in #433.
func (col *IntrinsicColumn) LookupRefFastUint64(_ uint32) (uint64, bool) { return 0, false }

// LookupRefFastString is a stub. IntrinsicTOC removed in #433.
func (col *IntrinsicColumn) LookupRefFastString(_ uint32) (string, bool) { return "", false }

// LookupRefFastInt64 is a stub. IntrinsicTOC removed in #433.
func (col *IntrinsicColumn) LookupRefFastInt64(_ uint32) (int64, bool) { return 0, false }

// EnsureRefIndex is a stub. IntrinsicTOC removed in #433.
func (col *IntrinsicColumn) EnsureRefIndex() {}

// EnsureBlockRefs is a stub. IntrinsicTOC removed in #433.
func (col *IntrinsicColumn) EnsureBlockRefs() {}

// ScanFlatColumnTopKRefs is a stub. IntrinsicTOC removed in #433.
func ScanFlatColumnTopKRefs(_ []byte, _ int, _ bool) []BlockRef { return nil }

// ScanFlatColumnRefsFiltered is a stub. IntrinsicTOC removed in #433.
func ScanFlatColumnRefsFiltered(_ []byte, _ bool, _ int, _ func(BlockRef) bool) []BlockRef {
	return nil
}

// LookupRefFastBytes is a stub. IntrinsicTOC removed in #433.
func (col *IntrinsicColumn) LookupRefFastBytes(_ uint32) ([]byte, bool) { return nil, false }
