// Package reader implements the blockpack file reader.
package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/hex"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Column holds a decoded column ready for query evaluation.
type Column struct {
	internMap map[string]string // borrowed from Reader.internStrings for lazy decode
	Name      string

	// Dictionary fields — MUST be heap-allocated (never arena) per NOTES §10.
	StringDict  []string
	StringIdx   []uint32
	Int64Dict   []int64
	Int64Idx    []uint32
	Uint64Dict  []uint64
	Uint64Idx   []uint32
	Float64Dict []float64
	Float64Idx  []uint32
	BoolDict    []uint8
	BoolIdx     []uint32
	BytesDict   [][]byte
	BytesIdx    []uint32

	// Inline bytes (kinds 3/4) — no dictionary.
	BytesInline [][]byte

	// Presence bitset — MAY be arena-allocated.
	Present []byte

	// NOTE-001: Lazy decode fields — rawEncoding is a sub-slice of the block's RawBytes.
	// Non-nil means this column has not been fully decoded yet (presence-only path).
	// rawEncoding is valid for the lifetime of the owning BlockWithBytes (bwb = BlockWithBytes:
	// the struct that pairs a decoded Block with its raw byte slice kept alive for lazy access).
	// All lazy decodes complete within the same block's row loop before bwb goes out of scope.
	rawEncoding []byte

	// sparseDictIdx holds the raw sparse dict indexes before the dense Idx slice is built.
	// Non-nil means expandDenseIdx() has not been called yet (lazy dense expansion).
	// Set by decodeDictKind2Sparse / decodeRLEIndexes; cleared after first value access.
	// NOTE-PERF-1: sparse dict columns (kind 2 / kind 7 RLE) defer the O(spanCount)
	// expandSparseIndexes allocation until the column is first accessed, avoiding
	// allocation for columns that are decoded but never read (e.g. early block exit).
	sparseDictIdx []uint32

	// Total span count this column covers (including nulls).
	SpanCount int
	Type      shared.ColumnType
}

// IsDecoded reports whether this column's values have been fully decoded.
// NOTE-001: returns false when rawEncoding is non-nil (column is lazily registered and not yet decoded).
// Callers use this to skip columns not in wantColumns — touching any value accessor on an
// un-decoded column triggers decodeNow (zstd decompression), which must be avoided for
// columns that were registered lazily but never requested.
func (c *Column) IsDecoded() bool { return c.rawEncoding == nil }

// EnsureDecoded triggers full decode if this column was lazily registered.
// Per-row value accessors (StringValue, Int64Value, etc.) call decodeNow automatically,
// so EnsureDecoded is only needed by callers that access the underlying slices directly
// (StringDict, StringIdx, etc.), bypassing the per-row path.
// NOTE-026: required by scanStringDictFloat before iterating StringDict.
func (c *Column) EnsureDecoded() {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
}

// expandDenseIdx builds the dense Idx slice from sparseDictIdx + Present on first access.
// Called automatically by StringValue, Int64Value, etc. when sparseDictIdx is set.
// NOTE-PERF-1: deferred from decode time to first-access time.
func (c *Column) expandDenseIdx() {
	if c.sparseDictIdx == nil {
		return
	}
	dense := expandSparseIndexes(c.sparseDictIdx, c.Present, c.SpanCount)
	c.sparseDictIdx = nil // release sparse slice — no longer needed
	assignDictIdx(c, dense)
}

// IsPresent reports whether span at idx has a value.
// NOTE-002: lazy presence decode — if rawEncoding is set and Present is nil, the
// presence bitmap has not been decoded yet. Decode it now from rawEncoding.
// rawEncoding remains non-nil after this call so decodeNow can still do the full decode.
// If rawEncoding is nil and Present is nil, the column has no presence bitmap (all present).
func (c *Column) IsPresent(idx int) bool {
	if c.Present == nil {
		if c.rawEncoding != nil {
			present, err := decodePresenceOnly(c.rawEncoding, c.SpanCount)
			if err != nil {
				// Mark column as fully-absent so retry returns false consistently.
				c.Present = []byte{}
				c.rawEncoding = nil
				c.internMap = nil
				return false
			}
			c.Present = present
		} else {
			return true // no presence bitmap = all spans present
		}
	}

	return shared.IsPresent(c.Present, idx)
}

// StringValue returns the string value at idx and whether it is present.
// For ColumnTypeUUID columns, the 16-byte binary value is formatted as a UUID string
// (e.g. "213085fc-b15b-45fc-8fa0-d448d4a246be"), preserving the original string representation.
// NOTE-001: triggers lazy decode on first call if rawEncoding is set.
func (c *Column) StringValue(idx int) (string, bool) {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return "", false
	}

	if c.Type == shared.ColumnTypeUUID {
		return c.uuidStringValue(idx)
	}

	if len(c.StringIdx) > idx {
		di := int(c.StringIdx[idx])
		if di < len(c.StringDict) {
			return c.StringDict[di], true
		}
	}

	return "", false
}

// uuidStringValue formats a 16-byte UUID from BytesDict/BytesInline as an RFC 4122 UUID string.
func (c *Column) uuidStringValue(idx int) (string, bool) {
	var b []byte

	if c.BytesInline != nil {
		if idx < len(c.BytesInline) {
			b = c.BytesInline[idx]
		}
	} else if len(c.BytesIdx) > idx {
		di := int(c.BytesIdx[idx])
		if di < len(c.BytesDict) {
			b = c.BytesDict[di]
		}
	}

	if len(b) != 16 {
		return "", false
	}

	// Format as RFC 4122 UUID without heap allocation: encode into a fixed
	// 36-byte buffer (8-4-4-4-12 hex groups separated by hyphens).
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:]), true
}

// StringValues returns all string values as a flat slice of length SpanCount,
// building it in one pass over StringIdx/StringDict. Absent rows are represented
// as "". Used by the regex scan path to avoid per-row dictionary indirection.
// NOTE-015: batch string extraction — see executor/NOTES.md NOTE-015.
func (c *Column) StringValues() []string {
	out := make([]string, c.SpanCount)
	for i := range c.SpanCount {
		if v, ok := c.StringValue(i); ok {
			out[i] = v
		}
	}
	return out
}

// Int64Value returns the int64 value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if rawEncoding is set.
func (c *Column) Int64Value(idx int) (int64, bool) {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Int64Idx) > idx {
		di := int(c.Int64Idx[idx])
		if di < len(c.Int64Dict) {
			return c.Int64Dict[di], true
		}
	}

	return 0, false
}

// Uint64Value returns the uint64 value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if rawEncoding is set.
func (c *Column) Uint64Value(idx int) (uint64, bool) {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Uint64Idx) > idx {
		di := int(c.Uint64Idx[idx])
		if di < len(c.Uint64Dict) {
			return c.Uint64Dict[di], true
		}
	}

	return 0, false
}

// Float64Value returns the float64 value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if rawEncoding is set.
func (c *Column) Float64Value(idx int) (float64, bool) {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return 0, false
	}

	if len(c.Float64Idx) > idx {
		di := int(c.Float64Idx[idx])
		if di < len(c.Float64Dict) {
			return c.Float64Dict[di], true
		}
	}

	return 0, false
}

// BoolValue returns the bool value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if rawEncoding is set.
func (c *Column) BoolValue(idx int) (bool, bool) {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return false, false
	}

	if len(c.BoolIdx) > idx {
		di := int(c.BoolIdx[idx])
		if di < len(c.BoolDict) {
			return c.BoolDict[di] != 0, true
		}
	}

	return false, false
}

// BytesValue returns the bytes value at idx and whether it is present.
// NOTE-001: triggers lazy decode on first call if rawEncoding is set.
func (c *Column) BytesValue(idx int) ([]byte, bool) {
	if c.rawEncoding != nil {
		c.decodeNow()
	}
	c.expandDenseIdx()
	if !c.IsPresent(idx) {
		return nil, false
	}

	if c.BytesInline != nil {
		if idx < len(c.BytesInline) {
			return c.BytesInline[idx], true
		}

		return nil, false
	}

	if len(c.BytesIdx) > idx {
		di := int(c.BytesIdx[idx])
		if di < len(c.BytesDict) {
			return c.BytesDict[di], true
		}
	}

	return nil, false
}

// ColIterEntry is a single entry in the pre-computed deduplicated column iteration list.
// Built once by BuildIterFields after all columns are registered.
// NOTE-049: Pre-computed column iteration order eliminates the per-span seen-map alloc.
type ColIterEntry struct {
	Col  *Column
	Name string
}

// Block holds decoded columns for a single block.
type Block struct {
	columns map[shared.ColumnKey]*Column
	// nameIndex maps column name → best-type Column for O(1) GetColumn lookups.
	// Built by buildNameIndex after all columns are registered. When multiple typed
	// variants exist for the same name, the lowest ColumnType value wins (stable tie-breaker).
	nameIndex map[string]*Column
	// NOTE-002: lazyColumnStore is the arena-like backing store for lazily-registered
	// Column structs. One slice allocation replaces N individual *Column allocations.
	// Pointers into this slice (stored in columns map) are stable because the slice
	// is sized to exact capacity before any appends — no reallocation ever occurs.
	lazyColumnStore []Column
	// iterFields is the pre-computed deduplicated column iteration list, built by
	// BuildIterFields. When non-nil, IterateFields uses this slice directly — zero allocs.
	// NOTE-049: see blockio/NOTES.md §49.
	iterFields []ColIterEntry
	meta       shared.BlockMeta
	spanCount  int
}

// NewBlockForParsing creates a Block with an empty columns map, for use with AddColumnsToBlock.
// Call buildNameIndex after all columns have been added.
func NewBlockForParsing(meta shared.BlockMeta) *Block {
	return &Block{
		columns:   make(map[shared.ColumnKey]*Column),
		meta:      meta,
		spanCount: int(meta.SpanCount),
	}
}

// buildNameIndex builds the nameIndex from the current columns map.
// Must be called after all columns have been registered (post-parse or post-AddColumnsToBlock).
func (b *Block) buildNameIndex() {
	b.nameIndex = make(map[string]*Column, len(b.columns))
	for k, col := range b.columns {
		if prev, ok := b.nameIndex[k.Name]; !ok || k.Type < prev.Type {
			b.nameIndex[k.Name] = col
		}
	}
}

// BuildIterFields pre-computes a deduplicated column iteration slice for use by
// modulesSpanFieldsAdapter.IterateFields. Called once after all columns are added.
// After this call, IterateFields on any adapter for this block is allocation-free.
// NOTE-049: Eliminates the per-span make(map[string]struct{}) in IterateFields.
// Idempotent — calling twice rebuilds from the current column set.
func (b *Block) BuildIterFields() {
	seen := make(map[string]struct{}, len(b.columns))
	entries := make([]ColIterEntry, 0, len(b.columns))
	for key := range b.columns {
		// NOTE-ITER-1: skip body-parsed auto-columns; they are not original attributes.
		if key.Type == shared.ColumnTypeRangeString {
			continue
		}
		if _, already := seen[key.Name]; already {
			continue
		}
		seen[key.Name] = struct{}{}
		// Use GetColumn for a stable tie-breaking column when multiple type variants
		// share the same name — matches the semantics of direct GetColumn calls.
		col := b.GetColumn(key.Name)
		if col == nil {
			continue
		}
		entries = append(entries, ColIterEntry{Col: col, Name: key.Name})
	}
	b.iterFields = entries
}

// IterFields returns the pre-computed deduplicated column list built by BuildIterFields.
// Returns nil if BuildIterFields has not been called.
func (b *Block) IterFields() []ColIterEntry { return b.iterFields }

// SpanCount returns the number of spans in the block.
func (b *Block) SpanCount() int { return b.spanCount }

// GetColumn returns the column with the given name, using the lowest ColumnType
// value as a stable tie-breaker when multiple typed variants exist.
// When a name has only one type variant (the common case) this is equivalent to
// a typed lookup. Use GetColumnByType for precise (name, type) access.
func (b *Block) GetColumn(name string) *Column {
	return b.nameIndex[name]
}

// GetColumnByType returns the column with the exact (name, type) combination, or nil.
func (b *Block) GetColumnByType(name string, typ shared.ColumnType) *Column {
	return b.columns[shared.ColumnKey{Name: name, Type: typ}]
}

// GetAllColumns returns all columns with the given name across all types.
// Returns nil if no column with that name exists.
func (b *Block) GetAllColumns(name string) []*Column {
	var result []*Column
	for k, col := range b.columns {
		if k.Name == name {
			result = append(result, col)
		}
	}
	return result
}

// Columns returns the full column map keyed by (name, type).
func (b *Block) Columns() map[shared.ColumnKey]*Column { return b.columns }

// Meta returns the block metadata.
func (b *Block) Meta() shared.BlockMeta { return b.meta }

// BlockWithBytes bundles a decoded Block with its raw bytes for AddColumnsToBlock.
type BlockWithBytes struct {
	Block    *Block
	RawBytes []byte
}
